use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use super::config::ClusterConfig;
use super::primitives::LogIndex;
use super::primitives::Term;

/// The point in the log a snapshot covers, plus the cluster configuration
/// active at that point.
///
/// The configuration travels with the snapshot because the compacted prefix may
/// contain the `ConfigChange` entry that established it. Without it, a node
/// restarting from this snapshot would not know its own membership before it
/// starts replaying the surviving log entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Index of the last entry folded into the snapshot. Entries at or below it
    /// are discarded from the log.
    pub last_index: LogIndex,
    /// Term of the entry at `last_index`, used for the log matching check.
    pub last_term: Term,
    /// Membership in force as of `last_index`.
    pub config: ClusterConfig,
}

/// Metadata plus the serialized state machine it describes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: SnapshotData,
}

/// What happens to the log entries above a snapshot boundary when the snapshot
/// is installed (section 7).
///
/// The receiver of an `InstallSnapshot` decides this once, by comparing its own
/// term at the boundary. Storage is told the answer rather than re-deriving it,
/// because after the install the entry that carried the deciding term is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuffixDisposition {
    /// The local entry at the boundary carried the snapshot's index and term,
    /// so the Log Matching Property makes every entry above it valid.
    Retain,
    /// The boundary conflicts with the local log, or lies past its end, so the
    /// whole log is invalid and goes.
    Discard,
}

/// Serialized state machine bytes.
///
/// A newtype rather than a bare `Vec<u8>` so it cannot be swapped with any other
/// buffer at a call site. The core module treats the contents as opaque; only
/// the `StateMachine` implementation knows how to interpret them.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotData(Vec<u8>);

impl SnapshotData {
    /// Wraps already-serialized state machine bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Borrows the serialized bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the wrapper and yields the serialized bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

/// Prints the length instead of the contents, so that logging a `Snapshot` does
/// not dump the entire state machine into the trace output.
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
