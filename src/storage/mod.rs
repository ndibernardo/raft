pub mod file;
pub mod memory;

pub use file::FileStorage;
pub use file::FileStorageError;
pub use memory::MemoryStorage;

use crate::core::types::LogEntry;
use crate::core::types::LogIndex;
use crate::core::types::NodeId;
use crate::core::types::Snapshot;
use crate::core::types::Term;

/// Everything read back at startup. Four unrelated fields is past the point
/// where positional tuple access is honest — name them.
pub struct LoadedState<Cmd> {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub snapshot: Option<Snapshot>,
    /// Entries after the snapshot boundary (or from index 1 if no snapshot).
    pub entries: Vec<LogEntry<Cmd>>,
}

/// §5.1: currentTerm, votedFor, and log on stable storage. A dumb durable sink —
/// `Node` owns the log and tells storage exactly what changed; storage never
/// re-derives a diff from lengths or terms, which would silently drop a
/// same-length conflict overwrite.
pub trait Storage<Cmd> {
    type Error;

    /// Full persisted state, read once at startup.
    fn load(&self) -> Result<LoadedState<Cmd>, Self::Error>;

    /// Must be durable before returning (§5.1).
    fn set_meta(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<(), Self::Error>;

    /// Inclusive: the entry at `index` is also removed. No-op if `index` is past the end.
    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// Appends to the tail. Caller guarantees `entries` is exactly the suffix that
    /// follows what's already durable — no conflict detection happens here.
    fn append(&mut self, entries: &[LogEntry<Cmd>]) -> Result<(), Self::Error>;

    /// Persists the snapshot durably, then drops all log entries at or below
    /// `snapshot.meta.last_index`. Must write the snapshot before touching the
    /// log — the reverse order loses committed state on a crash between the two.
    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error>;
}
