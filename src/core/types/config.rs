use std::collections::HashMap;
use std::net::SocketAddr;

use serde::Deserialize;
use serde::Serialize;

use super::primitives::NodeId;

/// Why a `ClusterConfig` could not be constructed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// A membership of size zero breaks quorum arithmetic: the majority
    /// threshold computes to 1 while no node can ever vote, so no entry commits.
    #[error("cluster config must have at least one member")]
    Empty,
}

/// Complete cluster membership: every voting member's ID mapped to its Raft RPC
/// address.
///
/// Construction rejects an empty membership, so quorum arithmetic never runs
/// against zero voters.
///
/// A configuration is stored verbatim in the log as `LogPayload::ConfigChange`,
/// which lets any node reconstruct the entire membership history from its log
/// alone after a crash.
///
/// A `ConfigChange` entry takes effect as soon as it is appended, not when it
/// commits. That is the single-server-change rule of dissertation section 4.1:
/// because only one member is added or removed at a time, any majority of the
/// old configuration intersects any majority of the new one, so the two
/// configurations cannot elect separate leaders.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    members: HashMap<NodeId, SocketAddr>,
}

impl ClusterConfig {
    /// Builds a configuration from a membership map.
    ///
    /// # Errors
    /// `ConfigError::Empty` when `members` is empty.
    pub fn new(members: HashMap<NodeId, SocketAddr>) -> Result<Self, ConfigError> {
        if members.is_empty() {
            return Err(ConfigError::Empty);
        }
        Ok(Self { members })
    }

    /// Derives the next configuration with `id` added, or its address updated if
    /// it is already a member. Infallible, because growing a non-empty
    /// membership cannot empty it.
    pub fn with_member(&self, id: NodeId, addr: SocketAddr) -> Self {
        let mut members = self.members.clone();
        members.insert(id, addr);
        Self { members }
    }

    /// Derives the next configuration with `id` removed. Removing an absent `id`
    /// yields an equivalent configuration.
    ///
    /// # Errors
    /// `ConfigError::Empty` when `id` was the only remaining member.
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

    /// Every identifier and address pair in the configuration, in unspecified order.
    pub fn members(&self) -> impl Iterator<Item = (NodeId, SocketAddr)> + '_ {
        self.members.iter().map(|(&id, &addr)| (id, addr))
    }

    /// Number of voting members. Always at least 1.
    pub fn size(&self) -> usize {
        self.members.len()
    }

    /// Whether `id` is a voting member.
    pub fn contains(&self, id: NodeId) -> bool {
        self.members.contains_key(&id)
    }

    /// Whether `id` holds a voting seat, as a named outcome.
    ///
    /// Callers that branch on membership use this rather than `contains`, so
    /// that a removed or unknown `NodeId` reads as an explicit `NonMember`
    /// decision instead of an anonymous `false`.
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

    /// The invariant the type exists to enforce: an empty configuration would
    /// make a majority of zero voters representable, so it must be rejected.
    #[test]
    fn without_member_rejects_removing_the_last_member() {
        let config = single_member_config();
        assert_eq!(
            config.without_member(NodeId::from(1)),
            Err(ConfigError::Empty)
        );
    }
}
