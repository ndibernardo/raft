use crate::types::{LogIndex, NodeId};
use std::collections::{HashMap, HashSet};

/// §5.1: followers are passive — they issue no requests, only respond to RPCs from
/// leaders and candidates. If a follower receives no communication, it starts an election.
pub struct Follower {
    pub leader_id: Option<NodeId>,
}

impl Follower {
    pub fn new() -> Self {
        Self { leader_id: None }
    }

    pub fn set_leader(&mut self, leader_id: NodeId) {
        self.leader_id = Some(leader_id);
    }
}

impl Default for Follower {
    fn default() -> Self {
        Self::new()
    }
}

/// §5.2: a candidate requests votes from peers to win an election. It votes for itself
/// and wins if it receives votes from a majority of servers in the full cluster.
pub struct Candidate {
    votes_received: HashSet<NodeId>,
}

impl Candidate {
    pub fn new(self_id: NodeId) -> Self {
        Self {
            votes_received: HashSet::from([self_id]),
        }
    }

    pub fn record_vote(&mut self, from: NodeId) {
        self.votes_received.insert(from);
    }

    // §5.2: a candidate wins the election if it receives votes from a majority
    // of the servers in the full cluster (⌊N/2⌋ + 1 out of N servers).
    pub fn has_majority(&self, cluster_size: usize) -> bool {
        let majority = cluster_size / 2 + 1;
        self.votes_received.len() >= majority
    }
}

/// §5.3, Figure 2, Volatile state on leaders (reinitialized after election).
/// The leader maintains next_index and match_index for each follower to track replication.
pub struct Leader {
    next_index: HashMap<NodeId, LogIndex>,  // next log index to send to each server
    match_index: HashMap<NodeId, LogIndex>, // highest log index known to be replicated
}

impl Leader {
    // nextIndex initialized to leader last log index + 1 (optimistic).
    // matchIndex initialized to 0 (conservative, increases monotonically).
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        Self {
            next_index: peers.iter().map(|&p| (p, last_log_index.next())).collect(),
            match_index: peers.iter().map(|&p| (p, LogIndex::default())).collect(),
        }
    }

    pub fn next_index_for(&self, peer: NodeId) -> Option<LogIndex> {
        self.next_index.get(&peer).copied()
    }

    /// For commit calculation: includes all followers, not the leader's own index.
    pub fn match_indices(&self) -> impl Iterator<Item = LogIndex> + '_ {
        self.match_index.values().copied()
    }

    /// §5.3: advance both indices together — next_index is always match_index + 1.
    pub fn record_success(&mut self, from: NodeId, match_index: LogIndex) {
        self.match_index.insert(from, match_index);
        self.next_index.insert(from, match_index.next());
    }

    /// §5.3: back off one position so the next AppendEntries probes earlier in the log.
    pub fn record_failure(&mut self, from: NodeId) {
        if let Some(&current) = self.next_index.get(&from)
            && let Some(prev) = current.prev()
        {
            self.next_index.insert(from, prev);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_new_initializes_correctly() {
        let peers = vec![NodeId::from(1), NodeId::from(2), NodeId::from(3)];
        let leader = Leader::new(&peers, LogIndex::from(5));

        assert_eq!(leader.next_index_for(NodeId::from(1)), Some(LogIndex::from(6)));
        assert_eq!(leader.next_index_for(NodeId::from(2)), Some(LogIndex::from(6)));
        assert_eq!(leader.next_index_for(NodeId::from(3)), Some(LogIndex::from(6)));

        let match_indices: Vec<_> = leader.match_indices().collect();
        assert_eq!(match_indices.len(), 3);
        assert!(match_indices.iter().all(|&idx| idx == LogIndex::default()));
    }

    #[test]
    fn next_index_for_returns_none_for_unknown_peer() {
        let peers = vec![NodeId::from(1)];
        let leader = Leader::new(&peers, LogIndex::from(0));

        assert_eq!(leader.next_index_for(NodeId::from(99)), None);
    }

    #[test]
    fn record_success_updates_both_indices() {
        let peers = vec![NodeId::from(1), NodeId::from(2)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_success(NodeId::from(1), LogIndex::from(5));

        let match_indices: Vec<_> = leader.match_indices().collect();
        assert!(match_indices.contains(&LogIndex::from(5)));
        assert_eq!(leader.next_index_for(NodeId::from(1)), Some(LogIndex::from(6)));
    }

    #[test]
    fn record_success_updates_only_target_peer() {
        let peers = vec![NodeId::from(1), NodeId::from(2)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_success(NodeId::from(1), LogIndex::from(5));

        // peer 2 should remain unchanged
        assert_eq!(leader.next_index_for(NodeId::from(2)), Some(LogIndex::from(1)));
    }

    #[test]
    fn record_failure_decrements_next_index() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(10));

        assert_eq!(leader.next_index_for(NodeId::from(1)), Some(LogIndex::from(11)));

        leader.record_failure(NodeId::from(1));
        assert_eq!(leader.next_index_for(NodeId::from(1)), Some(LogIndex::from(10)));

        leader.record_failure(NodeId::from(1));
        assert_eq!(leader.next_index_for(NodeId::from(1)), Some(LogIndex::from(9)));
    }

    #[test]
    fn record_failure_does_not_decrement_below_zero() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_failure(NodeId::from(1));
        assert_eq!(leader.next_index_for(NodeId::from(1)), Some(LogIndex::from(0)));

        leader.record_failure(NodeId::from(1));
        assert_eq!(leader.next_index_for(NodeId::from(1)), Some(LogIndex::from(0)));
    }

    #[test]
    fn record_failure_for_unknown_peer_does_nothing() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(5));

        leader.record_failure(NodeId::from(99));

        // Should not panic or create new entry
        assert_eq!(leader.next_index_for(NodeId::from(99)), None);
    }

    #[test]
    fn match_indices_returns_all_values() {
        let peers = vec![NodeId::from(1), NodeId::from(2), NodeId::from(3)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_success(NodeId::from(1), LogIndex::from(3));
        leader.record_success(NodeId::from(2), LogIndex::from(5));
        leader.record_success(NodeId::from(3), LogIndex::from(4));

        let mut match_indices: Vec<_> = leader.match_indices().collect();
        match_indices.sort();

        assert_eq!(match_indices, vec![
            LogIndex::from(3),
            LogIndex::from(4),
            LogIndex::from(5),
        ]);
    }

    #[test]
    fn record_success_for_new_peer_creates_entry() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_success(NodeId::from(99), LogIndex::from(10));

        assert_eq!(leader.next_index_for(NodeId::from(99)), Some(LogIndex::from(11)));
        let match_indices: Vec<_> = leader.match_indices().collect();
        assert!(match_indices.contains(&LogIndex::from(10)));
    }

    #[test]
    fn candidate_counts_self_vote_on_creation() {
        let candidate = Candidate::new(NodeId::from(1));
        assert!(candidate.has_majority(1));
    }

    #[test]
    fn candidate_reaches_majority_with_enough_grants() {
        let mut candidate = Candidate::new(NodeId::from(1));
        candidate.record_vote(NodeId::from(2));
        assert!(candidate.has_majority(3));
    }

    #[test]
    fn candidate_does_not_count_duplicate_grants_from_same_peer() {
        let mut candidate = Candidate::new(NodeId::from(1));
        candidate.record_vote(NodeId::from(2));
        candidate.record_vote(NodeId::from(2));
        // 3 grants from only 2 distinct peers must not reach majority in a 5-node cluster
        assert!(!candidate.has_majority(5));
    }
}
