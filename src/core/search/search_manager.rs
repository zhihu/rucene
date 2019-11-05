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

use core::{
    codec::Codec,
    index::merge::{MergePolicy, MergeScheduler},
    index::reader::{IndexReader, StandardDirectoryReader},
    index::writer::IndexWriter,
    search::searcher::IndexSearcher,
    store::directory::Directory,
};

use std::ops::Deref;
use std::{
    mem,
    sync::{Arc, Mutex, MutexGuard},
};

use error::{
    ErrorKind::{AlreadyClosed, IllegalState},
    Result,
};

/// Utility class to safely share `IndexSearcher` instances across multiple
/// threads, while periodically reopening. This class ensures each searcher is
/// closed only once all threads have finished using it.
///
///
/// In addition you should periodically call {@link #maybeRefresh}. While it's
/// possible to call this just before running each query, this is discouraged
/// since it penalizes the unlucky queries that need to refresh. It's better to use
/// a separate background thread, that periodically calls {@link #maybeRefresh}. Finally,
/// be sure to call {@link #close} once you are done
pub struct SearcherManager<C: Codec, T, SF: SearcherFactory<C>> {
    searcher_factory: SF,
    pub manager_base: ReferenceManagerBase<SF::Searcher>,
    refresh_listener: Option<T>,
}

impl<C: Codec, T, SF: SearcherFactory<C>> SearcherManager<C, T, SF> {
    pub fn from_writer<D, MS, MP>(
        writer: &IndexWriter<D, C, MS, MP>,
        apply_all_deletes: bool,
        write_all_deletes: bool,
        searcher_factory: SF,
        refresh_listener: Option<T>,
    ) -> Result<Self>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let reader = writer.get_reader(apply_all_deletes, write_all_deletes)?;
        Self::new(reader, searcher_factory, refresh_listener)
    }

    pub fn new<D, MS, MP>(
        reader: StandardDirectoryReader<D, C, MS, MP>,
        searcher_factory: SF,
        refresh_listener: Option<T>,
    ) -> Result<Self>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let current = searcher_factory.new_searcher(Arc::new(reader))?;
        let manager_base = ReferenceManagerBase::new(Arc::new(current));
        Ok(SearcherManager {
            searcher_factory,
            manager_base,
            refresh_listener,
        })
    }
}

impl<C, T, SF, RL> ReferenceManager<SF::Searcher, RL> for SearcherManager<C, T, SF>
where
    C: Codec,
    T: Deref<Target = RL>,
    SF: SearcherFactory<C>,
    RL: RefreshListener,
{
    fn base(&self) -> &ReferenceManagerBase<SF::Searcher> {
        &self.manager_base
    }

    fn refresh_listener(&self) -> Option<&RL> {
        self.refresh_listener.as_ref().map(|r| r.deref())
    }

    fn dec_ref(&self, _reference: &SF::Searcher) -> Result<()> {
        // reference.reader().dec_ref()
        Ok(())
    }

    fn refresh_if_needed(
        &self,
        reference_to_refresh: &Arc<SF::Searcher>,
    ) -> Result<Option<Arc<SF::Searcher>>> {
        if let Some(reader) = reference_to_refresh.reader().refresh()? {
            self.searcher_factory
                .new_searcher(Arc::from(reader))
                .map(|s| Some(Arc::new(s)))
        } else {
            Ok(None)
        }
    }

    fn try_inc_ref(&self, _reference: &Arc<SF::Searcher>) -> Result<bool> {
        // reference.reader().inc_ref()
        Ok(true)
    }

    fn ref_count(&self, _reference: &SF::Searcher) -> u32 {
        // TODO ?
        1
    }
}

/// Factory used by `SearcherManager` to create new `IndexSearcher` impls.
pub trait SearcherFactory<C: Codec> {
    type Searcher: IndexSearcher<C>;
    /// Returns a new IndexSearcher over the given reader.
    fn new_searcher(&self, reader: Arc<dyn IndexReader<Codec = C>>) -> Result<Self::Searcher>;
}

pub struct ReferenceManagerBase<T: ?Sized> {
    lock: Mutex<()>,
    refresh_lock: Mutex<()>,
    current: Option<Arc<T>>,
}

impl<T: ?Sized> ReferenceManagerBase<T> {
    pub fn new(current: Arc<T>) -> Self {
        Self {
            lock: Mutex::new(()),
            refresh_lock: Mutex::new(()),
            current: Some(current),
        }
    }
    fn ensure_open(&self) -> Result<()> {
        if self.current.is_none() {
            bail!(AlreadyClosed("this ReferenceManager is closed".into()));
        }
        Ok(())
    }
}

/// Utility class to safely share instances of a certain type across multiple
/// threads, while periodically refreshing them. This class ensures each
/// reference is closed only once all threads have finished using it. It is
/// recommended to consult the documentation of {@link ReferenceManager}
/// implementations for their {@link #maybeRefresh()} semantics.
pub trait ReferenceManager<T: ?Sized, RL: RefreshListener> {
    fn base(&self) -> &ReferenceManagerBase<T>;

    fn refresh_listener(&self) -> Option<&RL>;

    fn _swap_reference(&self, new_reference: Option<Arc<T>>, _l: &MutexGuard<()>) -> Result<()> {
        // let _l = base.lock.lock().unwrap();
        // we use base.lock to make sure this operation is safe
        let base_ptr =
            self.base() as *const ReferenceManagerBase<T> as *mut ReferenceManagerBase<T>;
        unsafe {
            if let Some(old_ref) = mem::replace(&mut (*base_ptr).current, new_reference) {
                self.release(old_ref.as_ref())?;
            }
        }
        Ok(())
    }

    /// Decrement reference counting on the given reference.
    fn dec_ref(&self, reference: &T) -> Result<()>;

    /// Refresh the given reference if needed. Returns `None` if no refresh was needed,
    /// otherwise a new refreshed reference
    fn refresh_if_needed(&self, reference_to_refresh: &Arc<T>) -> Result<Option<Arc<T>>>;

    /// Try to increment reference counting on the given reference. Return true if
    /// the operation was successful.
    fn try_inc_ref(&self, reference: &Arc<T>) -> Result<bool>;

    /// Obtain the current reference. You must match every call to acquire with one
    /// call to `release`; it's best to do so in a finally clause, and set the
    /// reference to None to prevent accidental usage after it has been released.
    fn acquire(&self) -> Result<Arc<T>> {
        loop {
            if let Some(ref cur) = self.base().current {
                if self.try_inc_ref(cur)? {
                    return Ok(Arc::clone(cur));
                }
            // TODO double check for ref count
            } else {
                bail!(IllegalState(
                    "this ReferenceManager is closed - this is likely a bug when the reference \
                     count is modified outside of the ReferenceManager"
                        .into()
                ));
            }
        }
    }

    /// Closes this ReferenceManager to prevent future {@link #acquire() acquiring}. A
    /// reference manager should be closed if the reference to the managed resource
    /// should be disposed or the application using the {@link ReferenceManager}
    /// is shutting down. The managed resource might not be released immediately,
    /// if the {@link ReferenceManager} user is holding on to a previously
    /// {@link #acquire() acquired} reference. The resource will be released once
    /// when the last reference is {@link #release(Object) released}. Those
    /// references can still be used as if the manager was still active.
    ///
    /// Applications should not {@link #acquire() acquire} new references from this
    /// manager once this method has been called. {@link #acquire() Acquiring} a
    /// resource on a closed {@link ReferenceManager} will throw an
    /// {@link AlreadyClosedException}.
    fn close(&self) -> Result<()> {
        let _l = self.base().lock.lock().unwrap();
        if self.base().current.is_some() {
            // make sure we can call this more than once
            // closeable javadoc says:
            // if this is already closed then invoking this method has no effect.
            self._swap_reference(None, &_l)?;
            self.after_close()?;
        }
        Ok(())
    }

    /// Returns the current reference count of the given reference
    fn ref_count(&self, reference: &T) -> u32;

    /// Called after close(), so subclass can free any resources.
    fn after_close(&self) -> Result<()> {
        Ok(())
    }

    fn _do_maybe_refresh(&self, l: &MutexGuard<()>) -> Result<()> {
        // let _l = self.base().refresh_lock.lock()?;

        let mut refreshed = false;
        let reference = self.acquire()?;
        self.notify_refresh_listeners_before()?;
        let res = self._exec_maybe_refresh(&reference, &mut refreshed, l);
        if let Err(e) = self.release(reference.as_ref()) {
            error!("release old reference failed by: {:?}", e);
        }
        if let Err(e) = self.notify_refresh_listeners_refreshed(refreshed) {
            error!("notify refresh listeners refreshed: {:?}", e);
        }
        if res.is_ok() {
            self.after_maybe_refresh()?;
        }
        res
    }

    fn _exec_maybe_refresh(
        &self,
        reference: &Arc<T>,
        refreshed: &mut bool,
        l: &MutexGuard<()>,
    ) -> Result<()> {
        if let Some(new_reference) = self.refresh_if_needed(reference)? {
            debug_assert_ne!(
                new_reference.as_ref() as *const T,
                reference.as_ref() as *const T
            );
            self._do_swap(new_reference, l)?;
            *refreshed = true;
        }
        Ok(())
    }

    fn _do_swap(&self, reference: Arc<T>, l: &MutexGuard<()>) -> Result<()> {
        let res = self._swap_reference(Some(Arc::clone(&reference)), l);
        if res.is_err() {
            let _ = self.release(reference.as_ref());
        }
        res
    }

    /// You must call this (or {@link #maybeRefreshBlocking()}), periodically, if
    /// you want that {@link #acquire()} will return refreshed instances.
    ///
    /// <b>Threads</b>: it's fine for more than one thread to call this at once.
    /// Only the first thread will attempt the refresh; subsequent threads will see
    /// that another thread is already handling refresh and will return
    /// immediately. Note that this means if another thread is already refreshing
    /// then subsequent threads will return right away without waiting for the
    /// refresh to complete.
    ///
    /// If this method returns true it means the calling thread either refreshed or
    /// that there were no changes to refresh. If it returns false it means another
    /// thread is currently refreshing.
    fn maybe_refresh(&self) -> Result<bool> {
        self.base().ensure_open()?;
        // Ensure only 1 thread does refresh at once; other threads just return immediately:
        if let Ok(guard) = self.base().refresh_lock.try_lock() {
            self._do_maybe_refresh(&guard)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn maybe_refresh_blocking(&self) -> Result<()> {
        let l = self.base().refresh_lock.lock()?;
        self._do_maybe_refresh(&l)
    }

    fn after_maybe_refresh(&self) -> Result<()> {
        Ok(())
    }

    fn release(&self, reference: &T) -> Result<()> {
        self.dec_ref(reference)
    }

    fn notify_refresh_listeners_before(&self) -> Result<()> {
        if let Some(rl) = self.refresh_listener() {
            rl.before_refresh()
        } else {
            Ok(())
        }
    }

    fn notify_refresh_listeners_refreshed(&self, refreshed: bool) -> Result<()> {
        if let Some(rl) = self.refresh_listener() {
            rl.after_refresh(refreshed)
        } else {
            Ok(())
        }
    }
}

pub trait RefreshListener {
    /// Called right before a refresh attempt starts.
    fn before_refresh(&self) -> Result<()>;

    /// Called after the attempted refresh; if the refresh did open a new
    /// reference then didRefresh will be true and `acquire()` is guareeteed
    /// to return the new reference
    fn after_refresh(&self, refreshed: bool) -> Result<()>;
}
