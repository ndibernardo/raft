use crate::core::types::Log;
use crate::core::types::LogEntry;
use crate::core::types::LogIndex;
use crate::core::types::NodeId;
use crate::core::types::Snapshot;
use crate::core::types::Term;
use crate::storage::LoadedState;
use crate::storage::Storage;

/// In-memory storage. Legitimate as a standalone backend (single-process clusters,
/// tests) — not durable across restarts by construction, which is the point.
pub struct MemoryStorage<Cmd> {
    current_term: Term,
    voted_for: Option<NodeId>,
    snapshot: Option<Snapshot>,
    log: Log<Cmd>,
}

impl<Cmd> MemoryStorage<Cmd> {
    pub fn new() -> Self {
        Self {
            current_term: Term::default(),
            voted_for: None,
            snapshot: None,
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
        Ok(LoadedState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            snapshot: self.snapshot.clone(),
            entries: self.log.iter().cloned().collect(),
        })
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

    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        self.log
            .compact_through(snapshot.meta.last_index, snapshot.meta.last_term);
        self.snapshot = Some(snapshot.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::LogPayload;

    #[test]
    fn term_and_vote_round_trip_through_storage() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        let loaded = storage.load().unwrap();
        assert_eq!(loaded.current_term, Term::default());
        assert_eq!(loaded.voted_for, None);

        storage
            .set_meta(Term::from(5), Some(NodeId::from(3)))
            .unwrap();

        let loaded = storage.load().unwrap();
        assert_eq!(loaded.current_term, Term::from(5));
        assert_eq!(loaded.voted_for, Some(NodeId::from(3)));
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

        let entries = storage.load().unwrap().entries;
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

        let entries = storage.load().unwrap().entries;
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

        let entries = storage.load().unwrap().entries;
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[1].payload,
            LogPayload::Command("SET status=active".to_string())
        );
    }

    fn test_snapshot(last_index: u64, last_term: u64, bytes: Vec<u8>) -> Snapshot {
        use std::collections::HashMap;

        use crate::core::types::ClusterConfig;
        use crate::core::types::SnapshotData;
        use crate::core::types::SnapshotMeta;

        let members = HashMap::from([(NodeId::from(1), "127.0.0.1:9001".parse().unwrap())]);
        Snapshot {
            meta: SnapshotMeta {
                last_index: LogIndex::from(last_index),
                last_term: Term::from(last_term),
                config: ClusterConfig::new(members).unwrap(),
            },
            data: SnapshotData::new(bytes),
        }
    }

    #[test]
    fn install_snapshot_drops_covered_log_prefix() {
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

        storage
            .install_snapshot(&test_snapshot(1, 1, vec![9]))
            .unwrap();

        let entries = storage.load().unwrap().entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].payload,
            LogPayload::Command("SET status=pending".to_string())
        );
    }

    #[test]
    fn install_snapshot_is_readable_via_load() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();
        storage
            .append(&[LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET name=miles".to_string()),
            }])
            .unwrap();

        let snapshot = test_snapshot(1, 1, vec![1, 2, 3]);
        storage.install_snapshot(&snapshot).unwrap();

        let loaded = storage.load().unwrap();
        assert_eq!(loaded.snapshot, Some(snapshot));
        assert!(loaded.entries.is_empty());
    }
}
