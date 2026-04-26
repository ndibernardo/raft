use std::collections::HashMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use super::primitives::NodeId;

/// Complete cluster membership: every voting member's ID mapped to its Raft RPC address.
///
/// Stored verbatim in the log as `LogPayload::ConfigChange` so any node can reconstruct
/// the full membership history purely from its log after a crash.
///
/// A `ConfigChange` entry takes effect immediately when appended — not when committed.
/// This is the single-server-changes safety rule (dissertation §4.1): changing one
/// member at a time guarantees any majority of the old config and any majority of the
/// new config overlap, so two independent leaders cannot form.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub members: HashMap<NodeId, SocketAddr>,
}

impl ClusterConfig {
    pub fn new(members: HashMap<NodeId, SocketAddr>) -> Self {
        Self { members }
    }

    /// All member IDs except `self_id`.
    pub fn peer_ids(&self, self_id: NodeId) -> Vec<NodeId> {
        self.members
            .keys()
            .copied()
            .filter(|&id| id != self_id)
            .collect()
    }

    pub fn size(&self) -> usize {
        self.members.len()
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.members.contains_key(&id)
    }
}
