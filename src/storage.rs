use crate::types::Log;
use crate::types::LogEntry;
use crate::types::LogIndex;
use crate::types::NodeId;
use crate::types::Term;

/// §5.1: currentTerm, votedFor, and log on stable storage. Implementations must flush to
/// durable media before returning from any write — persisting after responding violates safety.
pub trait Storage<Cmd> {
    type Error;

    fn current_term(&self) -> Result<Term, Self::Error>;

    /// Must be durable before returning (§5.1).
    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error>;

    fn voted_for(&self) -> Result<Option<NodeId>, Self::Error>;

    /// Must be durable before returning (§5.1).
    fn set_voted_for(&mut self, candidate: Option<NodeId>) -> Result<(), Self::Error>;

    fn last_log_index(&self) -> Result<LogIndex, Self::Error>;

    /// Index 0 returns Some(Term::default()); out-of-bounds returns None.
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>, Self::Error>;

    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry<Cmd>>, Self::Error>;

    fn entries_from(&self, start: LogIndex) -> Result<Vec<LogEntry<Cmd>>, Self::Error>;

    /// Returns the index of the appended entry.
    fn append(&mut self, entry: LogEntry<Cmd>) -> Result<LogIndex, Self::Error>;

    /// Inclusive: the entry at index is also removed.
    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// On conflict (same index, different term), truncates and replaces per §5.3.
    fn append_entries(
        &mut self,
        prev_log_index: LogIndex,
        entries: Vec<LogEntry<Cmd>>,
    ) -> Result<(), Self::Error>;
}

/// In-memory storage for testing.
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

    fn current_term(&self) -> Result<Term, Self::Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error> {
        self.current_term = term;
        Ok(())
    }

    fn voted_for(&self) -> Result<Option<NodeId>, Self::Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, candidate: Option<NodeId>) -> Result<(), Self::Error> {
        self.voted_for = candidate;
        Ok(())
    }

    fn last_log_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.log.last_index())
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>, Self::Error> {
        Ok(self.log.term_at(index))
    }

    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry<Cmd>>, Self::Error> {
        Ok(self.log.entry(index).cloned())
    }

    fn entries_from(&self, start: LogIndex) -> Result<Vec<LogEntry<Cmd>>, Self::Error> {
        Ok(self.log.suffix_from(start).to_vec())
    }

    fn append(&mut self, entry: LogEntry<Cmd>) -> Result<LogIndex, Self::Error> {
        Ok(self.log.append(entry))
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.log.truncate_from(index);
        Ok(())
    }

    fn append_entries(
        &mut self,
        prev_log_index: LogIndex,
        entries: Vec<LogEntry<Cmd>>,
    ) -> Result<(), Self::Error> {
        self.log.merge(prev_log_index, entries);
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

        assert_eq!(storage.current_term().unwrap(), Term::default());
        assert_eq!(storage.voted_for().unwrap(), None);

        storage.set_current_term(Term::from(5)).unwrap();
        storage.set_voted_for(Some(NodeId::from(3))).unwrap();

        assert_eq!(storage.current_term().unwrap(), Term::from(5));
        assert_eq!(storage.voted_for().unwrap(), Some(NodeId::from(3)));
    }

    #[test]
    fn appended_entry_is_readable_by_index() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        let idx = storage
            .append(LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET name=miles".to_string()),
            })
            .unwrap();
        assert_eq!(idx, LogIndex::from(1));

        let idx = storage
            .append(LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET counter=1".to_string()),
            })
            .unwrap();
        assert_eq!(idx, LogIndex::from(2));

        assert_eq!(storage.last_log_index().unwrap(), LogIndex::from(2));
        assert_eq!(
            storage.term_at(LogIndex::from(1)).unwrap(),
            Some(Term::from(1))
        );
        assert_eq!(
            storage.entry(LogIndex::from(1)).unwrap().map(|e| e.payload),
            Some(LogPayload::Command("SET name=miles".to_string()))
        );
    }

    #[test]
    fn truncate_from_removes_entries_from_index_inclusive() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        storage
            .append(LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET name=miles".to_string()),
            })
            .unwrap();
        storage
            .append(LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET counter=1".to_string()),
            })
            .unwrap();
        storage
            .append(LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET price=100".to_string()),
            })
            .unwrap();

        storage.truncate_from(LogIndex::from(2)).unwrap();

        assert_eq!(storage.last_log_index().unwrap(), LogIndex::from(1));
    }

    #[test]
    fn append_entries_replaces_conflicting_entry_and_trims_tail() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        storage
            .append(LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET name=miles".to_string()),
            })
            .unwrap();
        storage
            .append(LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET status=pending".to_string()),
            })
            .unwrap();

        storage
            .append_entries(
                LogIndex::from(1),
                vec![LogEntry {
                    term: Term::from(2),
                    payload: LogPayload::Command("SET status=active".to_string()),
                }],
            )
            .unwrap();

        assert_eq!(storage.last_log_index().unwrap(), LogIndex::from(2));
        assert_eq!(
            storage.entry(LogIndex::from(2)).unwrap().map(|e| e.payload),
            Some(LogPayload::Command("SET status=active".to_string()))
        );
    }
}
