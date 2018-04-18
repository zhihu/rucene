use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use memmap::Mmap;

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

pub struct MmapDirectory {
    directory: FSDirectory,
    pub preload: bool,
    mmap_cache: Arc<Mutex<MmapCache>>,
}

impl MmapDirectory {
    pub fn new<T: AsRef<Path>>(directory: &T, _max_chunk_size: u32) -> Result<MmapDirectory> {
        let directory = FSDirectory::new(directory)?;
        Ok(MmapDirectory {
            directory,
            preload: false,
            mmap_cache: Arc::new(Mutex::new(MmapCache::default())),
        })
    }
}

impl Directory for MmapDirectory {
    fn list_all(&self) -> Result<Vec<String>> {
        self.directory.list_all()
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        self.directory.file_length(name)
    }

    fn open_input(&self, name: &str, _ctx: &IOContext) -> Result<Box<IndexInput>> {
        let full_path = self.directory.resolve(name);
        let mut mmap_cache = self.mmap_cache.lock()?;

        let boxed = mmap_cache
            .get_mmap(&full_path)?
            .map(ReadOnlySource::from)
            .map(MmapIndexInput::from)
            .unwrap();
        Ok(Box::new(boxed))
    }
}

impl Drop for MmapDirectory {
    fn drop(&mut self) {}
}

impl fmt::Display for MmapDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MmapDirectory({})", self.directory)
    }
}
