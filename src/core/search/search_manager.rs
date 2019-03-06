use core::{index::{IndexReader, IndexWriter, StandardDirectoryReader},
           search::searcher::{DefaultIndexSearcher, DefaultSimilarityProducer, IndexSearcher},
           util::{ReferenceManager, ReferenceManagerBase}};

use error::Result;

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
///
pub struct SearcherManager {
    searcher_factory: Box<SearcherFactory>,
    manager_base: ReferenceManagerBase<ManagedIndexSearcher>,
}

impl SearcherManager {
    pub fn from_writer(
        writer: &IndexWriter,
        apply_all_deletes: bool,
        write_all_deletes: bool,
        searcher_factory: Box<SearcherFactory>,
    ) -> Result<Self> {
        let reader = writer.get_reader(apply_all_deletes, write_all_deletes)?;
        Self::new(reader, searcher_factory)
    }

    pub fn new(
        reader: StandardDirectoryReader,
        searcher_factory: Box<SearcherFactory>,
    ) -> Result<Self> {
        let current = searcher_factory.new_searcher(Arc::new(reader))?;
        let manager_base = ReferenceManagerBase::new(Arc::new(current));
        Ok(SearcherManager {
            searcher_factory,
            manager_base,
        })
    }
}

impl ReferenceManager<ManagedIndexSearcher> for SearcherManager {
    fn base(&self) -> &ReferenceManagerBase<ManagedIndexSearcher> {
        &self.manager_base
    }

    fn dec_ref(&self, reference: &ManagedIndexSearcher) -> Result<()> {
        // reference.reader().dec_ref()
        Ok(())
    }

    fn refresh_if_needed(
        &self,
        reference_to_refresh: &Arc<ManagedIndexSearcher>,
    ) -> Result<Option<Arc<ManagedIndexSearcher>>> {
        if let Some(r) = reference_to_refresh
            .reader()
            .as_any()
            .downcast_ref::<StandardDirectoryReader>()
        {
            if let Some(new_reader) = r.open_if_changed(None)? {
                self.searcher_factory
                    .new_searcher(Arc::new(new_reader))
                    .map(|s| Some(Arc::new(s)))
            } else {
                Ok(None)
            }
        } else {
            unreachable!()
        }
    }

    fn try_inc_ref(&self, reference: &Arc<ManagedIndexSearcher>) -> Result<bool> {
        // reference.reader().inc_ref()
        Ok(true)
    }

    fn ref_count(&self, reference: &ManagedIndexSearcher) -> u32 {
        // TODO ?
        1
    }
}

pub type ManagedIndexSearcher = DefaultIndexSearcher<Arc<IndexReader>, DefaultSimilarityProducer>;

pub trait SearcherFactory {
    fn new_searcher(&self, reader: Arc<IndexReader>) -> Result<ManagedIndexSearcher>;
}
