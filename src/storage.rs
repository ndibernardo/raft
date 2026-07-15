use crate::types::Log;
use crate::types::LogEntry;
use crate::types::LogIndex;
use crate::types::NodeId;
use crate::types::Term;

/// Term, vote, and log as read back from durable storage on startup.
pub type LoadedState<Cmd> = (Term, Option<NodeId>, Vec<LogEntry<Cmd>>);

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
}

/// In-memory storage. Legitimate as a standalone backend (single-process clusters,
/// tests) — not durable across restarts by construction, which is the point.
pub struct MemoryStorage<Cmd> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log<Cmd>,
}

impl<Cmd> MemoryStorage<Cmd> {
    pub fn new() -> Self {
        Self {
            current_term: Term::default(),
            voted_for: None,
            log: Log::new(),
        }
    }
}

impl<Cmd> Default for MemoryStorage<Cmd> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Cmd: Clone> Storage<Cmd> for MemoryStorage<Cmd> {
    type Error = std::convert::Infallible;

    fn load(&self) -> Result<LoadedState<Cmd>, Self::Error> {
        Ok((
            self.current_term,
            self.voted_for,
            self.log.iter().cloned().collect(),
        ))
    }

    fn set_meta(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<(), Self::Error> {
        self.current_term = term;
        self.voted_for = voted_for;
        Ok(())
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.log.truncate_from(index);
        Ok(())
    }

    fn append(&mut self, entries: &[LogEntry<Cmd>]) -> Result<(), Self::Error> {
        for entry in entries {
            self.log.append(entry.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogPayload;

    #[test]
    fn term_and_vote_round_trip_through_storage() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        let (term, voted_for, _) = storage.load().unwrap();
        assert_eq!(term, Term::default());
        assert_eq!(voted_for, None);

        storage
            .set_meta(Term::from(5), Some(NodeId::from(3)))
            .unwrap();

        let (term, voted_for, _) = storage.load().unwrap();
        assert_eq!(term, Term::from(5));
        assert_eq!(voted_for, Some(NodeId::from(3)));
    }

    #[test]
    fn appended_entries_are_readable_after_load() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        storage
            .append(&[
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET name=miles".to_string()),
                },
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET counter=1".to_string()),
                },
            ])
            .unwrap();

        let (_, _, entries) = storage.load().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].payload,
            LogPayload::Command("SET name=miles".to_string())
        );
        assert_eq!(
            entries[1].payload,
            LogPayload::Command("SET counter=1".to_string())
        );
    }

    #[test]
    fn truncate_from_removes_entries_from_index_inclusive() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        storage
            .append(&[
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET name=miles".to_string()),
                },
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET counter=1".to_string()),
                },
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET price=100".to_string()),
                },
            ])
            .unwrap();

        storage.truncate_from(LogIndex::from(2)).unwrap();

        let (_, _, entries) = storage.load().unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn truncate_then_append_replaces_the_tail() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        storage
            .append(&[
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET name=miles".to_string()),
                },
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET status=pending".to_string()),
                },
            ])
            .unwrap();

        storage.truncate_from(LogIndex::from(2)).unwrap();
        storage
            .append(&[LogEntry {
                term: Term::from(2),
                payload: LogPayload::Command("SET status=active".to_string()),
            }])
            .unwrap();

        let (_, _, entries) = storage.load().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[1].payload,
            LogPayload::Command("SET status=active".to_string())
        );
    }
}
