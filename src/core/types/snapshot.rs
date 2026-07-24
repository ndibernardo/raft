use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use super::config::ClusterConfig;
use super::primitives::LogIndex;
use super::primitives::Term;

/// Point in the log a snapshot covers, plus the config active at that point.
/// The config must ride along: the compacted prefix may hold the active
/// `ConfigChange` entry, and a restarting node needs it before replaying the log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_index: LogIndex,
    pub last_term: Term,
    pub config: ClusterConfig,
}

/// A complete snapshot: metadata + opaque serialized state machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: SnapshotData,
}

/// Serialized state machine bytes. Newtype so it can't be confused with any
/// other byte buffer; core never inspects it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotData(Vec<u8>);

impl SnapshotData {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for SnapshotData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SnapshotData(<{} bytes>)", self.0.len())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::core::types::NodeId;

    fn single_member_config() -> ClusterConfig {
        let members = HashMap::from([(NodeId::from(1), "127.0.0.1:9001".parse().unwrap())]);
        ClusterConfig::new(members).unwrap()
    }

    #[test]
    fn snapshot_data_round_trips_bytes() {
        let data = SnapshotData::new(vec![1, 2, 3]);
        assert_eq!(data.as_bytes(), &[1, 2, 3]);
        assert_eq!(data.into_bytes(), vec![1, 2, 3]);
    }

    #[test]
    fn snapshot_data_debug_hides_raw_bytes() {
        let data = SnapshotData::new(vec![0; 42]);
        assert_eq!(format!("{data:?}"), "SnapshotData(<42 bytes>)");
    }

    #[test]
    fn snapshot_round_trips_through_serde_json() {
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_index: LogIndex::from(7),
                last_term: Term::from(2),
                config: single_member_config(),
            },
            data: SnapshotData::new(vec![9, 9, 9]),
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let restored: Snapshot = serde_json::from_str(&json).unwrap();

        assert_eq!(restored, snapshot);
    }
}
