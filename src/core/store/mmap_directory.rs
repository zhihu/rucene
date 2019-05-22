use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use memmap::Mmap;

use core::store::fs_index_output::FSIndexOutput;
use core::store::lock::LockFactory;
use core::store::{Directory, FSDirectory, IOContext};
use core::store::{IndexInput, MmapIndexInput, ReadOnlySource};
use error::Result;

#[derive(Default, Clone, Debug)]
pub struct CacheStat {
    // Number of time the cache prevents to call `mmap`
    hit: usize,
    // Number of time call `mmap`
    // as no entry was in the cache.
    miss_empty: usize,
    // Number of time calling `mmap` when the entry in the cache was evinced.
    miss_weak: usize,
}

pub struct MmapCache {
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

pub struct MmapDirectory<LF: LockFactory> {
    directory: FSDirectory<LF>,
    pub preload: bool,
    mmap_cache: Arc<Mutex<MmapCache>>,
}

impl<LF: LockFactory> MmapDirectory<LF> {
    pub fn new<T: AsRef<Path>>(
        directory: &T,
        lock_factory: LF,
        _max_chunk_size: u32,
    ) -> Result<MmapDirectory<LF>> {
        let directory = FSDirectory::new(directory, lock_factory)?;
        Ok(MmapDirectory {
            directory,
            preload: false,
            mmap_cache: Arc::new(Mutex::new(MmapCache::default())),
        })
    }
}

impl<LF: LockFactory> Directory for MmapDirectory<LF> {
    type LK = LF::LK;
    type IndexOutput = FSIndexOutput;
    type TempOutput = FSIndexOutput;

    fn list_all(&self) -> Result<Vec<String>> {
        self.directory.list_all()
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        self.directory.file_length(name)
    }

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

    fn obtain_lock(&self, name: &str) -> Result<Self::LK> {
        self.directory.obtain_lock(name)
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        self.directory.create_temp_output(prefix, suffix, ctx)
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        self.directory.delete_file(name)
    }

    fn sync(&self, name: &HashSet<String>) -> Result<()> {
        self.directory.sync(name)
    }

    /// Ensure that directory metadata, such as recent file renames, are made durable.
    fn sync_meta_data(&self) -> Result<()> {
        self.directory.sync_meta_data()
    }

    fn rename(&self, source: &str, dest: &str) -> Result<()> {
        self.directory.rename(source, dest)
    }

    fn resolve(&self, name: &str) -> PathBuf {
        self.directory.resolve(name)
    }
}

// unsafe impl<LF: LockFactory> Send for MmapDirectory<LF> {}
// unsafe impl<LF: LockFactory> Sync for MmapDirectory<LF> {}

impl<LF: LockFactory> fmt::Display for MmapDirectory<LF> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MmapDirectory({})", self.directory)
    }
}
