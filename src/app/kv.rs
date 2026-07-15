use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::app::runtime::StateMachine;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvResult {
    Ok,
    Value(Option<String>),
}

/// In-memory key-value store.
#[derive(Default)]
pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

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

impl StateMachine<KvCommand> for KvStore {
    type Output = KvResult;

    fn apply(&mut self, command: KvCommand) -> Self::Output {
        KvStore::apply(self, command)
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
}
