// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use memmap::Mmap;

use core::store::directory::{Directory, FSDirectory, FilterDirectory};
use core::store::io::{FSIndexOutput, IndexInput, MmapIndexInput, ReadOnlySource};
use core::store::IOContext;
use error::Result;

#[derive(Default, Clone, Debug)]
struct CacheStat {
    // Number of time the cache prevents to call `mmap`
    hit: usize,
    // Number of time call `mmap`
    // as no entry was in the cache.
    miss_empty: usize,
    // Number of time calling `mmap` when the entry in the cache was evinced.
    miss_weak: usize,
}

struct MmapCache {
    stat: CacheStat,
    cache: HashMap<PathBuf, Weak<Mmap>>,
    purge_weak_limit: usize,
}

impl Default for MmapCache {
    fn default() -> MmapCache {
        const STARTING_PURGE_WEAK_LIMIT: usize = 1_000;
        MmapCache {
            stat: CacheStat::default(),
            cache: HashMap::new(),
            purge_weak_limit: STARTING_PURGE_WEAK_LIMIT,
        }
    }
}

impl MmapCache {
    fn cleanup(&mut self) {
        let previous_cache_size = self.cache.len();

        let mut new_cache = HashMap::new();
        ::std::mem::swap(&mut new_cache, &mut self.cache);
        self.cache = new_cache
            .into_iter()
            .filter(|&(_, ref weak_ref)| weak_ref.upgrade().is_some())
            .collect();
        if self.cache.len() == previous_cache_size {
            self.purge_weak_limit *= 2;
        }
    }

    fn get_mmap(&mut self, full_path: &PathBuf) -> Result<Option<Arc<Mmap>>> {
        // if we exceed this limit, then we go through the weak
        // and remove those that are obsolete.
        if self.cache.len() > self.purge_weak_limit {
            self.cleanup();
        }

        match self.cache.entry(full_path.clone()) {
            HashMapEntry::Occupied(mut occupied) => {
                if let Some(mmap) = occupied.get().upgrade() {
                    self.stat.hit += 1;
                    Ok(Some(Arc::clone(&mmap)))
                } else {
                    // The entry exists but the weak ref has been destroyed.
                    self.stat.miss_weak += 1;
                    if let Some(mmap) = MmapIndexInput::mmap(&full_path, 0, 0)? {
                        occupied.insert(Arc::downgrade(&mmap));
                        Ok(Some(mmap))
                    } else {
                        Ok(None)
                    }
                }
            }

            HashMapEntry::Vacant(vacant) => {
                self.stat.miss_empty += 1;
                if let Some(mmap) = MmapIndexInput::mmap(&full_path, 0, 0)? {
                    vacant.insert(Arc::downgrade(&mmap));
                    Ok(Some(mmap))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// File-based `@link Directory` implementation that uses
/// mmap for reading, and `FSDirectory.FSIndexOutput` for writing.
///
/// *NOTE*: memory mapping uses up a portion of the
/// virtual memory address space in your process equal to the
/// size of the file being mapped.  Before using this class,
/// be sure your have plenty of virtual address space, e.g. by
/// using a 64 bit JRE, or a 32 bit JRE with indexes that are
/// guaranteed to fit within the address space.
/// On 32 bit platforms also consult `#MMapDirectory(Path, LockFactory, int)`
/// if you have problems with mmap failing because of fragmented
/// address space. If you get an OutOfMemoryException, it is recommended
/// to reduce the chunk size, until it works.
///
/// This will consume additional transient disk usage: on Windows,
/// attempts to delete or overwrite the files will result in an
/// exception; on other platforms, which typically have a &quot;delete on
/// last close&quot; semantics, while such operations will succeed, the bytes
/// are still consuming space on disk.  For many applications this
/// limitation is not a problem (e.g. if you have plenty of disk space,
/// and you don't rely on overwriting files on Windows) but it's still
/// an important limitation to be aware of.
///
/// This class supplies the workaround mentioned in the bug report
/// (see `#setUseUnmap`), which may fail on
/// non-Oracle/OpenJDK JVMs. It forcefully unmaps the buffer on close by using
/// an undocumented internal cleanup functionality. If
/// `#UNMAP_SUPPORTED` is <code>true</code>, the workaround
/// will be automatically enabled (with no guarantees; if you discover
/// any problems, you can disable it).
///
/// *NOTE:* Accessing this class either directly or
/// indirectly from a thread while it's interrupted can close the
/// underlying channel immediately if at the same time the thread is
/// blocked on IO. The channel will remain closed and subsequent access
/// to `MMapDirectory` will throw a `ClosedChannelException`. If
/// your application uses either `Thread#interrupt()` or
/// `Future#cancel(boolean)` you should use the legacy `RAFDirectory`
/// from the Lucene `misc` module in favor of `MMapDirectory`.
///
/// See [Blog post about MMapDirectory](http://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html)
pub struct MmapDirectory {
    directory: FSDirectory,
    pub preload: bool,
    mmap_cache: Arc<Mutex<MmapCache>>,
}

impl MmapDirectory {
    pub fn new<T: AsRef<Path>>(directory: &T) -> Result<MmapDirectory> {
        let directory = FSDirectory::new(directory)?;
        Ok(MmapDirectory {
            directory,
            preload: false,
            mmap_cache: Arc::new(Mutex::new(MmapCache::default())),
        })
    }
}

impl FilterDirectory for MmapDirectory {
    type Dir = FSDirectory;

    #[inline]
    fn dir(&self) -> &Self::Dir {
        &self.directory
    }
}

impl Directory for MmapDirectory {
    type IndexOutput = FSIndexOutput;
    type TempOutput = FSIndexOutput;

    fn create_output(&self, name: &str, context: &IOContext) -> Result<Self::IndexOutput> {
        self.directory.create_output(name, context)
    }

    fn open_input(&self, name: &str, _ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        let full_path = self.directory.resolve(name);
        let mut mmap_cache = self.mmap_cache.lock()?;
        let boxed = mmap_cache
            .get_mmap(&full_path)?
            .map(ReadOnlySource::from)
            .map(MmapIndexInput::from)
            .unwrap();
        Ok(Box::new(boxed))
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        self.directory.create_temp_output(prefix, suffix, ctx)
    }
}

impl fmt::Display for MmapDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MmapDirectory({})", self.directory)
    }
}
