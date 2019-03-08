use std::{mem,
          sync::{Arc, Mutex, MutexGuard}};

use error::Result;

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
            bail!("this ReferenceManager is closed");
        }
        Ok(())
    }
}

/// Utility class to safely share instances of a certain type across multiple
/// threads, while periodically refreshing them. This class ensures each
/// reference is closed only once all threads have finished using it. It is
/// recommended to consult the documentation of {@link ReferenceManager}
/// implementations for their {@link #maybeRefresh()} semantics.
pub trait ReferenceManager<T: ?Sized> {
    fn base(&self) -> &ReferenceManagerBase<T>;

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
                bail!("this ReferenceManager is closed");
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

        let reference = self.acquire()?;
        let res = self._exec_maybe_refresh(&reference, l);
        let _ = self.release(reference.as_ref());
        if res.is_ok() {
            self.after_maybe_refresh()?;
        }
        res
    }

    fn _exec_maybe_refresh(&self, reference: &Arc<T>, l: &MutexGuard<()>) -> Result<()> {
        if let Some(new_reference) = self.refresh_if_needed(reference)? {
            debug_assert_ne!(
                new_reference.as_ref() as *const T,
                reference.as_ref() as *const T
            );
            self._do_swap(new_reference, l)?;
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
        let l = self.base().refresh_lock.lock().unwrap();
        self._do_maybe_refresh(&l)
    }

    fn after_maybe_refresh(&self) -> Result<()> {
        Ok(())
    }

    fn release(&self, reference: &T) -> Result<()> {
        self.dec_ref(reference)
    }
}
