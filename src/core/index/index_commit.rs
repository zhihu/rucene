use core::index::IndexWriter;
use core::index::StandardDirectoryReader;
use core::store::Directory;
use error::Result;

use std::collections::{HashMap, HashSet};

/// Expert: represents a single commit into an index as seen by the
/// {@link IndexDeletionPolicy} or {@link IndexReader}.
///
/// Changes to the content of an index are made visible
/// only after the writer who made that change commits by
/// writing a new segments file
/// (`segments_N</code`). This point in time, when the
/// action of writing of a new segments file to the directory
/// is completed, is an index commit.
///
/// Each index commit point has a unique segments file
/// associated with it. The segments file associated with a
/// later index commit point would have a larger N.
///
// TODO: this is now a poor name, because this class also represents a
// point-in-time view from an NRT reader
pub trait IndexCommit {
    /// Get the segments file (`segments_N`) associated with this commit point
    fn segments_file_name(&self) -> &str;

    /// Returns all index files referenced by this commit point.
    fn file_names(&self) -> Result<&HashSet<String>>;

    /// Return the `Directory` for the index
    fn directory(&self) -> &Directory;

    /// Delete this commit point.  This only applies when using
    /// the commit point in the context of IndexWriter's
    /// IndexDeletionPolicy.
    ///
    /// Upon calling this, the writer is notified that this commit
    /// point should be deleted.
    ///
    /// Decision that a commit-point should be deleted is taken by the
    /// `IndexDeletionPolicy` in effect and therefore this should only
    /// be called by its `IndexDeletionPolicy#onInit on_init()` or
    /// `IndexDeletionPolicy#onCommit on_commit()` methods.
    fn delete(&mut self) -> Result<()>;

    fn delete1(&mut self, writer: &IndexWriter) -> Result<()> {
        unimplemented!()
    }

    /// Returns true if this commit should be deleted; this is only
    /// used by `IndexWriter` after invoking the `IndexDeletionPolicy`
    fn is_deleted(&self) -> bool;

    /// Returns number of segments referenced by this commit.
    fn segment_count(&self) -> usize;

    /// Returns the generation (the _N in segments_N) for this IndexCommit
    fn generation(&self) -> i64;

    /// Returns user_data, previously passed to
    /// `IndexWriter::set_live_commit_data()` for this commit.
    /// Map is (String -> String)
    fn user_data(&self) -> &HashMap<String, String>;

    /// package-private API for IndexWriter to init from a commit-point pulled from
    /// an NRT or non-NRT reader.
    fn reader(&self) -> Option<&StandardDirectoryReader> {
        None
    }
}
