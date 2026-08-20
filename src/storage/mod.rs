pub mod file;
pub mod memory;

pub use file::FileStorage;
pub use file::FileStorageError;
pub use memory::MemoryStorage;

use crate::core::types::LogEntry;
use crate::core::types::LogIndex;
use crate::core::types::NodeId;
use crate::core::types::Snapshot;
use crate::core::types::SuffixDisposition;
use crate::core::types::Term;

/// The complete persisted state, as read back at startup.
pub struct LoadedState<Cmd> {
    /// Highest term this node has seen. Zero on a fresh node.
    pub current_term: Term,
    /// Candidate this node voted for in `current_term`, if any.
    pub voted_for: Option<NodeId>,
    /// Most recent snapshot, if one was ever installed.
    pub snapshot: Option<Snapshot>,
    /// Entries after the snapshot boundary, or from index 1 if there is no snapshot.
    pub entries: Vec<LogEntry<Cmd>>,
}

/// Durable sink for the state Raft requires to survive a restart: `currentTerm`,
/// `votedFor`, and the log (section 5.1).
///
/// Implementations are deliberately passive. `Node` owns the in-memory log and
/// states exactly what changed through `truncate_from`, `append`, and
/// `install_snapshot`. Storage never re-derives a diff by comparing lengths or
/// terms, because a length-only comparison misses a conflict overwrite that
/// replaces entries without changing the log length.
pub trait Storage<Cmd> {
    type Error;

    /// Reads the full persisted state. Called once, before the node starts.
    fn load(&self) -> Result<LoadedState<Cmd>, Self::Error>;

    /// Records the term and vote. Must be durable before returning (section 5.1):
    /// a node that acknowledges a vote and then forgets it can elect two leaders
    /// in one term.
    fn set_meta(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<(), Self::Error>;

    /// Removes every entry at or after `index`. Inclusive of `index` itself.
    /// No-op when `index` is past the end of the log.
    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// Appends `entries` to the tail.
    ///
    /// The caller guarantees `entries` is exactly the suffix following what is
    /// already durable. No conflict detection happens here.
    fn append(&mut self, entries: &[LogEntry<Cmd>]) -> Result<(), Self::Error>;

    /// Persists `snapshot` and brings the log into the state `disposition`
    /// names: `Retain` drops every entry at or below `snapshot.meta.last_index`
    /// and keeps the rest, `Discard` drops the entire log.
    ///
    /// The snapshot must reach durable storage before the log changes. In the
    /// reverse order, a crash between the two steps leaves neither the entries
    /// nor the snapshot that replaced them, losing committed state.
    ///
    /// The two must also recover as one unit. An implementation that persists
    /// the snapshot and then dies before applying `Discard` would otherwise come
    /// back with the new snapshot beside a suffix the caller had already
    /// invalidated, which is exactly the divergence section 7 forbids.
    fn install_snapshot(
        &mut self,
        snapshot: &Snapshot,
        disposition: SuffixDisposition,
    ) -> Result<(), Self::Error>;
}
