use core::{
    codec::Codec,
    index::{
        merge_policy::MergePolicy, merge_scheduler::MergeScheduler, IndexReader, IndexWriter,
        StandardDirectoryReader,
    },
    search::searcher::IndexSearcher,
    store::Directory,
    util::{ReferenceManager, ReferenceManagerBase, RefreshListener},
};

use error::Result;

use std::ops::Deref;
use std::sync::Arc;

/// Utility class to safely share {@link IndexSearcher} instances across multiple
/// threads, while periodically reopening. This class ensures each searcher is
/// closed only once all threads have finished using it.
///
/// <p>
/// Use {@link #acquire} to obtain the current searcher, and {@link #release} to
/// release it, like this:
///
/// <pre class="prettyprint">
/// IndexSearcher s = manager.acquire();
/// try {
///   // Do searching, doc retrieval, etc. with s
/// } finally {
///   manager.release(s);
/// }
/// // Do not use s after this!
/// s = null;
/// </pre>
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
    pub fn from_writer<D: Directory, MS: MergeScheduler, MP: MergePolicy>(
        writer: &IndexWriter<D, C, MS, MP>,
        apply_all_deletes: bool,
        write_all_deletes: bool,
        searcher_factory: SF,
        refresh_listener: Option<T>,
    ) -> Result<Self> {
        let reader = writer.get_reader(apply_all_deletes, write_all_deletes)?;
        Self::new(reader, searcher_factory, refresh_listener)
    }

    pub fn new<D: Directory, MS: MergeScheduler, MP: MergePolicy>(
        reader: StandardDirectoryReader<D, C, MS, MP>,
        searcher_factory: SF,
        refresh_listener: Option<T>,
    ) -> Result<Self> {
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
        //        if let Some(r) = reference_to_refresh
        //            .reader()
        //            .as_any()
        //            .downcast_ref::<StandardDirectoryReader<D, MS, MP>>()
        //        {
        //            if let Some(new_reader) = r.open_if_changed(None)? {
        //                self.searcher_factory
        //                    .new_searcher(Arc::new(new_reader))
        //                    .map(|s| Some(Arc::new(s)))
        //            } else {
        //                Ok(None)
        //            }
        //        } else {
        //            unreachable!()
        //        }
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

pub trait SearcherFactory<C: Codec> {
    type Searcher: IndexSearcher<C>;
    fn new_searcher(&self, reader: Arc<IndexReader<Codec = C>>) -> Result<Self::Searcher>;
}
