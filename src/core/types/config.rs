use std::collections::HashMap;
use std::net::SocketAddr;

use serde::Deserialize;
use serde::Serialize;

use super::primitives::NodeId;

/// Why a `ClusterConfig` could not be constructed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// An empty config is representable but poisons quorum math (size 0 → majority 1
    /// with zero voters), so it's rejected at construction instead.
    #[error("cluster config must have at least one member")]
    Empty,
}

/// Complete cluster membership: every voting member's ID mapped to its Raft RPC address.
/// Always non-empty — enforced at construction, so quorum math never divides by a
/// membership of size zero.
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
    members: HashMap<NodeId, SocketAddr>,
}

impl ClusterConfig {
    /// Returns `Err(ConfigError::Empty)` if `members` is empty.
    pub fn new(members: HashMap<NodeId, SocketAddr>) -> Result<Self, ConfigError> {
        if members.is_empty() {
            return Err(ConfigError::Empty);
        }
        Ok(Self { members })
    }

    /// Derives the next config with `id` added (or its address updated). Adding a
    /// member to an already-valid config can never produce an empty one.
    pub fn with_member(&self, id: NodeId, addr: SocketAddr) -> Self {
        let mut members = self.members.clone();
        members.insert(id, addr);
        Self { members }
    }

    /// Derives the next config with `id` removed. Fails if `id` was the last member.
    pub fn without_member(&self, id: NodeId) -> Result<Self, ConfigError> {
        let mut members = self.members.clone();
        members.remove(&id);
        Self::new(members)
    }

    /// All member IDs except `self_id`.
    pub fn peer_ids(&self, self_id: NodeId) -> Vec<NodeId> {
        self.members
            .keys()
            .copied()
            .filter(|&id| id != self_id)
            .collect()
    }

    /// Every (id, address) pair in the config.
    pub fn members(&self) -> impl Iterator<Item = (NodeId, SocketAddr)> + '_ {
        self.members.iter().map(|(&id, &addr)| (id, addr))
    }

    pub fn size(&self) -> usize {
        self.members.len()
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.members.contains_key(&id)
    }

    /// Whether `id` currently holds a voting seat — a removed or spoofed
    /// `NodeId` must not be silently treated as `false` where the caller needs
    /// to branch on cluster membership as a domain outcome.
    pub fn membership_of(&self, id: NodeId) -> Membership {
        if self.contains(id) {
            Membership::Member
        } else {
            Membership::NonMember
        }
    }
}

/// Whether a `NodeId` holds a voting seat in a `ClusterConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Membership {
    Member,
    NonMember,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn single_member_config() -> ClusterConfig {
        ClusterConfig::new(HashMap::from([(NodeId::from(1), addr(9001))])).unwrap()
    }

    #[test]
    fn new_rejects_empty_members() {
        assert_eq!(ClusterConfig::new(HashMap::new()), Err(ConfigError::Empty));
    }

    #[test]
    fn new_accepts_non_empty_members() {
        let members = HashMap::from([(NodeId::from(1), addr(9001))]);
        assert!(ClusterConfig::new(members).is_ok());
    }

    #[test]
    fn with_member_adds_a_new_member() {
        let config = single_member_config();
        let grown = config.with_member(NodeId::from(2), addr(9002));

        assert_eq!(grown.size(), 2);
        assert!(grown.contains(NodeId::from(2)));
    }

    #[test]
    fn without_member_removes_an_existing_member() {
        let config = single_member_config().with_member(NodeId::from(2), addr(9002));
        let shrunk = config.without_member(NodeId::from(2)).unwrap();

        assert_eq!(shrunk.size(), 1);
        assert!(!shrunk.contains(NodeId::from(2)));
    }

    /// The invariant this whole type exists to enforce: an empty config would
    /// poison quorum math (majority of zero voters), so it must be unrepresentable.
    #[test]
    fn without_member_rejects_removing_the_last_member() {
        let config = single_member_config();
        assert_eq!(
            config.without_member(NodeId::from(1)),
            Err(ConfigError::Empty)
        );
    }
}
