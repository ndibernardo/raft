use std::collections::HashMap;
use std::collections::HashSet;

use crate::core::types::LogIndex;
use crate::core::types::NodeId;

/// Per-role state of a follower (section 5.1).
///
/// Followers are passive: they issue no requests and only respond to leaders and
/// candidates. The single piece of state worth keeping is the current leader, so
/// that a client can be redirected instead of merely refused.
#[derive(Debug)]
pub struct Follower {
    leader_id: Option<NodeId>,
}

impl Follower {
    /// A follower that has not yet heard from a leader in this term.
    pub fn new() -> Self {
        Self { leader_id: None }
    }

    /// The leader of the current term, if one has been observed.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Records the leader named by an accepted AppendEntries or InstallSnapshot.
    pub fn set_leader(&mut self, leader_id: NodeId) {
        self.leader_id = Some(leader_id);
    }
}

impl Default for Follower {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-role state of a candidate (section 5.2): the set of servers that have
/// granted it a vote in the current term.
///
/// A set rather than a counter, because a peer may resend its response and a
/// duplicated grant would otherwise inflate the tally into a false majority.
#[derive(Debug)]
pub struct Candidate {
    votes_received: HashSet<NodeId>,
}

impl Candidate {
    /// A candidate that has already voted for itself, as section 5.2 requires.
    pub fn new(self_id: NodeId) -> Self {
        Self {
            votes_received: HashSet::from([self_id]),
        }
    }

    /// Records a granted vote. Idempotent per peer.
    pub fn record_vote(&mut self, from: NodeId) {
        self.votes_received.insert(from);
    }

    /// Whether the granted votes amount to a strict majority of `cluster_size`,
    /// which is `cluster_size / 2 + 1` servers (section 5.2).
    pub fn has_majority(&self, cluster_size: usize) -> bool {
        let majority = cluster_size / 2 + 1;
        self.votes_received.len() >= majority
    }
}

/// Per-peer replication state of a leader (section 5.3, Figure 2). Volatile:
/// rebuilt from scratch on every election.
#[derive(Debug)]
pub struct Leader {
    /// Next index the leader will send to each peer. A guess, corrected by
    /// backing off on rejection.
    next_index: HashMap<NodeId, LogIndex>,
    /// Highest index known to be replicated on each peer. Only ever raised by an
    /// acknowledgement, so it is safe to use for the commit decision.
    match_index: HashMap<NodeId, LogIndex>,
}

impl Leader {
    /// Initializes replication state for `peers`.
    ///
    /// `next_index` starts one past the leader's own log, the optimistic guess
    /// that peers are already caught up. `match_index` starts at 0, the
    /// pessimistic assumption that nothing is confirmed replicated yet. Guessing
    /// the other way around in either field would let the leader commit an entry
    /// no follower actually holds.
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        Self {
            next_index: peers.iter().map(|&p| (p, last_log_index.next())).collect(),
            match_index: peers.iter().map(|&p| (p, LogIndex::default())).collect(),
        }
    }

    /// Next index to send to `peer`, or `None` if `peer` is not tracked.
    pub fn next_index_for(&self, peer: NodeId) -> Option<LogIndex> {
        self.next_index.get(&peer).copied()
    }

    /// Highest index known replicated on `peer`, or `None` if `peer` is not
    /// tracked.
    ///
    /// The commit calculation asks per configuration member rather than reading
    /// the whole map, so an untracked member counts as nothing replicated
    /// instead of silently shrinking the quorum it is measured against.
    pub fn match_index_for(&self, peer: NodeId) -> Option<LogIndex> {
        self.match_index.get(&peer).copied()
    }

    /// Records an acknowledgement from `from` up to `match_index`, advancing
    /// `next_index` to one past it (section 5.3).
    ///
    /// Raises the stored value only. A leader resends a snapshot on every
    /// heartbeat until it is acknowledged, so a delayed duplicate for an older
    /// boundary can arrive after a newer, higher acknowledgement. Applying it
    /// would rewind `match_index` and, with it, the commit index.
    pub fn record_success(&mut self, from: NodeId, match_index: LogIndex) {
        let current = self.match_index.entry(from).or_default();
        if match_index > *current {
            *current = match_index;
            self.next_index.insert(from, match_index.next());
        }
    }

    /// Backs `next_index` off by one position after a rejected AppendEntries, so
    /// the next attempt probes earlier in the log until the two logs agree
    /// (section 5.3). Floors at index 0 and ignores untracked peers.
    pub fn record_failure(&mut self, from: NodeId) {
        if let Some(&current) = self.next_index.get(&from)
            && let Some(prev) = current.prev()
        {
            self.next_index.insert(from, prev);
        }
    }

    /// All peer IDs currently tracked by this leader.
    pub fn tracked_peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.next_index.keys().copied()
    }

    /// Registers a peer added by a configuration change.
    ///
    /// Idempotent: a repeated call for a known peer leaves its already-advanced
    /// indices alone rather than resetting replication progress.
    pub fn add_peer(&mut self, peer: NodeId, last_log_index: LogIndex) {
        self.next_index
            .entry(peer)
            .or_insert_with(|| last_log_index.next());
        self.match_index.entry(peer).or_default();
    }

    /// Deregisters a peer removed by a configuration change. Its match index
    /// stops counting toward quorum from the next commit calculation onward.
    pub fn remove_peer(&mut self, peer: NodeId) {
        self.next_index.remove(&peer);
        self.match_index.remove(&peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_new_initializes_correctly() {
        let peers = vec![NodeId::from(1), NodeId::from(2), NodeId::from(3)];
        let leader = Leader::new(&peers, LogIndex::from(5));

        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(6))
        );
        assert_eq!(
            leader.next_index_for(NodeId::from(2)),
            Some(LogIndex::from(6))
        );
        assert_eq!(
            leader.next_index_for(NodeId::from(3)),
            Some(LogIndex::from(6))
        );

        for peer in [1, 2, 3] {
            assert_eq!(
                leader.match_index_for(NodeId::from(peer)),
                Some(LogIndex::default())
            );
        }
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

        assert_eq!(
            leader.match_index_for(NodeId::from(1)),
            Some(LogIndex::from(5))
        );
        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(6))
        );
    }

    #[test]
    fn record_success_updates_only_target_peer() {
        let peers = vec![NodeId::from(1), NodeId::from(2)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_success(NodeId::from(1), LogIndex::from(5));

        // Peer 2 keeps the value it was initialized with.
        assert_eq!(
            leader.next_index_for(NodeId::from(2)),
            Some(LogIndex::from(1))
        );
    }

    #[test]
    fn record_success_ignores_a_late_duplicate_that_would_regress_indices() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        // The peer acknowledges through index 8 via ordinary AppendEntries.
        leader.record_success(NodeId::from(1), LogIndex::from(8));
        // A duplicate InstallSnapshotResponse for an earlier boundary then
        // arrives late, because the leader resends the snapshot every heartbeat
        // until it is acknowledged.
        leader.record_success(NodeId::from(1), LogIndex::from(3));

        assert_eq!(
            leader.match_index_for(NodeId::from(1)),
            Some(LogIndex::from(8)),
            "match_index must not regress from a stale duplicate response"
        );
        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(9))
        );
    }

    #[test]
    fn record_failure_decrements_next_index() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(10));

        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(11))
        );

        leader.record_failure(NodeId::from(1));
        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(10))
        );

        leader.record_failure(NodeId::from(1));
        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(9))
        );
    }

    #[test]
    fn record_failure_does_not_decrement_below_zero() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_failure(NodeId::from(1));
        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(0))
        );

        leader.record_failure(NodeId::from(1));
        assert_eq!(
            leader.next_index_for(NodeId::from(1)),
            Some(LogIndex::from(0))
        );
    }

    #[test]
    fn record_failure_for_unknown_peer_does_nothing() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(5));

        leader.record_failure(NodeId::from(99));

        // An untracked peer must not be silently added to the quorum set.
        assert_eq!(leader.next_index_for(NodeId::from(99)), None);
    }

    #[test]
    fn match_indices_returns_all_values() {
        let peers = vec![NodeId::from(1), NodeId::from(2), NodeId::from(3)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_success(NodeId::from(1), LogIndex::from(3));
        leader.record_success(NodeId::from(2), LogIndex::from(5));
        leader.record_success(NodeId::from(3), LogIndex::from(4));

        assert_eq!(
            leader.match_index_for(NodeId::from(1)),
            Some(LogIndex::from(3))
        );
        assert_eq!(
            leader.match_index_for(NodeId::from(2)),
            Some(LogIndex::from(5))
        );
        assert_eq!(
            leader.match_index_for(NodeId::from(3)),
            Some(LogIndex::from(4))
        );
    }

    #[test]
    fn record_success_for_new_peer_creates_entry() {
        let peers = vec![NodeId::from(1)];
        let mut leader = Leader::new(&peers, LogIndex::from(0));

        leader.record_success(NodeId::from(99), LogIndex::from(10));

        assert_eq!(
            leader.next_index_for(NodeId::from(99)),
            Some(LogIndex::from(11))
        );
        assert_eq!(
            leader.match_index_for(NodeId::from(99)),
            Some(LogIndex::from(10))
        );
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
        // Three grants from two distinct servers is not a majority of five.
        assert!(!candidate.has_majority(5));
    }
}
