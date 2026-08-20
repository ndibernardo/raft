use crate::core::types::Log;
use crate::core::types::LogEntry;
use crate::core::types::LogIndex;
use crate::core::types::NodeId;
use crate::core::types::Snapshot;
use crate::core::types::SuffixDisposition;
use crate::core::types::Term;
use crate::storage::LoadedState;
use crate::storage::Storage;

/// Storage that keeps everything in memory and nothing on disk.
///
/// Suitable for single-process clusters and for tests, where losing state on
/// restart is the intended behaviour rather than a limitation. A cluster that
/// must survive a restart needs `FileStorage`.
pub struct MemoryStorage<Cmd> {
    current_term: Term,
    voted_for: Option<NodeId>,
    snapshot: Option<Snapshot>,
    log: Log<Cmd>,
}

impl<Cmd> MemoryStorage<Cmd> {
    /// Empty storage: term 0, no vote, no snapshot, no entries.
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

    /// Applies both halves in one call, so there is no window in which the
    /// snapshot is recorded and the log is not. Nothing here survives a restart
    /// anyway, but keeping the semantics identical to `FileStorage` is what lets
    /// the two back the same tests.
    fn install_snapshot(
        &mut self,
        snapshot: &Snapshot,
        disposition: SuffixDisposition,
    ) -> Result<(), Self::Error> {
        match disposition {
            SuffixDisposition::Retain => self
                .log
                .compact_through(snapshot.meta.last_index, snapshot.meta.last_term),
            SuffixDisposition::Discard => self
                .log
                .reset_to_snapshot(snapshot.meta.last_index, snapshot.meta.last_term),
        }
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
            .install_snapshot(&test_snapshot(1, 1, vec![9]), SuffixDisposition::Retain)
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
        storage
            .install_snapshot(&snapshot, SuffixDisposition::Retain)
            .unwrap();

        let loaded = storage.load().unwrap();
        assert_eq!(loaded.snapshot, Some(snapshot));
        assert!(loaded.entries.is_empty());
    }

    #[test]
    fn install_snapshot_with_discard_drops_entries_above_the_boundary() {
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
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("SET region=eu-west-1".to_string()),
                },
            ])
            .unwrap();

        // The boundary sits at index 2 and the entry above it is valid only if
        // the local log agreed there. Discard says it did not.
        storage
            .install_snapshot(&test_snapshot(2, 9, vec![4, 2]), SuffixDisposition::Discard)
            .unwrap();

        let loaded = storage.load().unwrap();
        assert!(loaded.entries.is_empty());
    }
}
