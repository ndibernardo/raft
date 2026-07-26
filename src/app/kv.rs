use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::app::runtime::StateMachine;
use crate::core::types::SnapshotData;

/// An operation against the store, as replicated through the log.
///
/// `Get` is included so a read is ordered against writes by going through
/// consensus like any other command, at the cost of a full round trip.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
}

/// What applying a `KvCommand` produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvResult {
    /// A write completed. `Delete` reports this whether or not the key existed.
    Ok,
    /// A read completed, with `None` when the key is absent.
    Value(Option<String>),
}

/// In-memory key-value store, the example state machine driven by the log.
#[derive(Default)]
pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Applies one command and returns its result. Deterministic, as every
    /// replica must reach the same state from the same sequence of commands.
    pub fn apply(&mut self, command: KvCommand) -> KvResult {
        match command {
            KvCommand::Get { key } => KvResult::Value(self.data.get(&key).cloned()),
            KvCommand::Set { key, value } => {
                self.data.insert(key, value);
                KvResult::Ok
            }
            KvCommand::Delete { key } => {
                self.data.remove(&key);
                KvResult::Ok
            }
        }
    }
}

/// Serialization format for a `KvStore` snapshot.
///
/// Separate from `KvStore` so the domain type carries no derive of its own, and
/// the on-disk format can change without changing the store.
#[derive(Serialize, Deserialize)]
struct KvSnapshotDto {
    entries: HashMap<String, String>,
}

/// Failure to serialize or deserialize a `KvStore` snapshot.
#[derive(Debug, thiserror::Error)]
#[error("kv store snapshot error: {0}")]
pub struct KvSnapshotError(#[from] serde_json::Error);

impl StateMachine<KvCommand> for KvStore {
    type Output = KvResult;
    type SnapshotError = KvSnapshotError;

    fn apply(&mut self, command: KvCommand) -> Self::Output {
        KvStore::apply(self, command)
    }

    fn snapshot(&self) -> Result<SnapshotData, Self::SnapshotError> {
        let dto = KvSnapshotDto {
            entries: self.data.clone(),
        };
        let bytes = serde_json::to_vec(&dto)?;
        Ok(SnapshotData::new(bytes))
    }

    fn restore(&mut self, data: &SnapshotData) -> Result<(), Self::SnapshotError> {
        let dto: KvSnapshotDto = serde_json::from_slice(data.as_bytes())?;
        self.data = dto.entries;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_stored_value_after_set() {
        let mut store = KvStore::new();

        store.apply(KvCommand::Set {
            key: "username".to_string(),
            value: "miles".to_string(),
        });

        let result = store.apply(KvCommand::Get {
            key: "username".to_string(),
        });

        assert_eq!(result, KvResult::Value(Some("miles".to_string())));
    }

    #[test]
    fn get_returns_none_for_absent_key() {
        let mut store = KvStore::new();

        let result = store.apply(KvCommand::Get {
            key: "username".to_string(),
        });

        assert_eq!(result, KvResult::Value(None));
    }

    #[test]
    fn delete_removes_key_and_subsequent_get_returns_none() {
        let mut store = KvStore::new();

        store.apply(KvCommand::Set {
            key: "username".to_string(),
            value: "miles".to_string(),
        });
        store.apply(KvCommand::Delete {
            key: "username".to_string(),
        });

        let result = store.apply(KvCommand::Get {
            key: "username".to_string(),
        });

        assert_eq!(result, KvResult::Value(None));
    }

    #[test]
    fn delete_on_absent_key_returns_ok() {
        let mut store = KvStore::new();

        let result = store.apply(KvCommand::Delete {
            key: "username".to_string(),
        });

        assert_eq!(result, KvResult::Ok);
    }

    #[test]
    fn snapshot_then_restore_round_trips_store_contents() {
        let mut store = KvStore::new();
        store.apply(KvCommand::Set {
            key: "username".to_string(),
            value: "miles".to_string(),
        });
        store.apply(KvCommand::Set {
            key: "status".to_string(),
            value: "active".to_string(),
        });

        let data = store.snapshot().unwrap();
        let mut restored = KvStore::new();
        restored.restore(&data).unwrap();

        assert_eq!(
            restored.apply(KvCommand::Get {
                key: "username".to_string()
            }),
            KvResult::Value(Some("miles".to_string()))
        );
        assert_eq!(
            restored.apply(KvCommand::Get {
                key: "status".to_string()
            }),
            KvResult::Value(Some("active".to_string()))
        );
    }

    #[test]
    fn restore_replaces_existing_state_not_merges() {
        let mut source = KvStore::new();
        source.apply(KvCommand::Set {
            key: "region".to_string(),
            value: "eu-west-1".to_string(),
        });
        let data = source.snapshot().unwrap();

        let mut target = KvStore::new();
        target.apply(KvCommand::Set {
            key: "username".to_string(),
            value: "miles".to_string(),
        });
        target.restore(&data).unwrap();

        assert_eq!(
            target.apply(KvCommand::Get {
                key: "username".to_string()
            }),
            KvResult::Value(None),
            "restore must replace state wholesale, not merge into it"
        );
        assert_eq!(
            target.apply(KvCommand::Get {
                key: "region".to_string()
            }),
            KvResult::Value(Some("eu-west-1".to_string()))
        );
    }

    #[test]
    fn restore_rejects_corrupt_bytes() {
        let mut store = KvStore::new();
        let corrupt = SnapshotData::new(b"not valid json".to_vec());

        assert!(store.restore(&corrupt).is_err());
    }
}
