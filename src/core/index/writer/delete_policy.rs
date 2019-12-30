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

use core::index::writer::index_file_deleter::CommitPoint;
use error::Result;

/// Expert: policy for deletion of stale `IndexCommit index commits`.
///
/// Implement this interface, and pass it to one
/// of the `IndexWriter` or `IndexReader`
/// constructors, to customize when older
/// `IndexCommit point-in-time commits`
/// are deleted from the index directory.  The default deletion policy
/// is `KeepOnlyLastCommitDeletionPolicy`, which always
/// removes old commits as soon as a new commit is done (this
/// matches the behavior before 2.2).
///
/// One expected use case for this (and the reason why it
/// was first created) is to work around problems with an
/// index directory accessed via filesystems like NFS because
/// NFS does not provide the "delete on last close" semantics
/// that Lucene's "point in time" search normally relies on.
/// By implementing a custom deletion policy, such as "a
/// commit is only removed once it has been stale for more
/// than X minutes", you can give your readers time to
/// refresh to the new commit before `IndexWriter`
/// removes the old commits.  Note that doing so will
/// increase the storage requirements of the index.  See <a
/// target="top"
/// href="http://issues.apache.org/jira/browse/LUCENE-710">LUCENE-710</a>
/// for details.
///
/// Implementers of sub-classes should make sure that `#clone()`
/// returns an independent instance able to work with any other `IndexWriter`
/// or `Directory` instance.
pub trait IndexDeletionPolicy {
    /// This is called once when a writer is first
    /// instantiated to give the policy a chance to remove old
    /// commit points.
    ///
    /// The writer locates all index commits present in the
    /// index directory and calls this method.  The policy may
    /// choose to delete some of the commit points, doing so by
    /// calling method `IndexCommit#delete delete()`
    /// of `IndexCommit`.
    ///
    /// <u>Note:</u> the last CommitPoint is the most recent one,
    /// i.e. the "front index state". Be careful not to delete it,
    /// unless you know for sure what you are doing, and unless
    /// you can afford to lose the index content while doing that.
    ///
    /// @param commits List of current
    /// `IndexCommit point-in-time commits`,
    ///  sorted by age (the 0th one is the oldest commit).
    ///  Note that for a new index this method is invoked with
    ///  an empty list.
    fn on_init(&self, commits: Vec<&mut CommitPoint>) -> Result<()>;

    /// This is called each time the writer completed a commit.
    /// This gives the policy a chance to remove old commit points
    /// with each commit.
    ///
    /// The policy may now choose to delete old commit points
    /// by calling method `IndexCommit#delete delete()`
    /// of `IndexCommit`.
    ///
    /// This method is only called when `IndexWriter#commit`
    /// or `IndexWriter#close` is called, or possibly not at all
    /// if the `IndexWriter#rollback` is called.
    ///
    /// Note: the last CommitPoint is the most recent one,
    /// i.e. the "front index state". Be careful not to delete it,
    /// unless you know for sure what you are doing, and unless
    /// you can afford to lose the index content while doing that.
    ///  
    /// @param commits List of `IndexCommit`,
    ///  sorted by age (the 0th one is the oldest commit).
    fn on_commit(&self, commits: Vec<&mut CommitPoint>) -> Result<()>;
}

#[derive(Default)]
pub struct KeepOnlyLastCommitDeletionPolicy;

impl KeepOnlyLastCommitDeletionPolicy {
    pub fn on_init(&self, commits: Vec<&mut CommitPoint>) -> Result<()> {
        self.on_commit(commits)
    }

    pub fn on_commit(&self, mut commits: Vec<&mut CommitPoint>) -> Result<()> {
        commits.pop();
        for commit in commits {
            commit.delete()?;
        }
        Ok(())
    }
}
