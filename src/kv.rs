use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::runtime::StateMachine;

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
    fn set_and_get() {
        let mut store = KvStore::new();

        store.apply(KvCommand::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        });

        let result = store.apply(KvCommand::Get {
            key: "foo".to_string(),
        });

        assert_eq!(result, KvResult::Value(Some("bar".to_string())));
    }

    #[test]
    fn get_missing_key() {
        let mut store = KvStore::new();

        let result = store.apply(KvCommand::Get {
            key: "missing".to_string(),
        });

        assert_eq!(result, KvResult::Value(None));
    }

    #[test]
    fn delete() {
        let mut store = KvStore::new();

        store.apply(KvCommand::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        });
        store.apply(KvCommand::Delete {
            key: "foo".to_string(),
        });

        let result = store.apply(KvCommand::Get {
            key: "foo".to_string(),
        });

        assert_eq!(result, KvResult::Value(None));
    }
}
