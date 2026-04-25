use tracing::{debug, info};

use crate::command::Command;
use crate::state::{Candidate, Follower, Leader};
use crate::storage::Storage;
use crate::types::{
    AppendEntries, AppendEntriesResponse, LogEntry, LogIndex, LogPayload, Message, NodeId,
    RequestVote, RequestVoteResponse, Term,
};

/// §Figure 2: current_term, voted_for, log. Must be written to durable storage before
/// responding to any RPC — persisting after responding violates Raft's safety guarantees.
pub struct PersistentState<Cmd> {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry<Cmd>>,
}

impl<Cmd: Clone> PersistentState<Cmd> {
    pub fn load<S: Storage<Cmd>>(storage: &S) -> Result<Self, S::Error> {
        Ok(Self {
            current_term: storage.current_term()?,
            voted_for: storage.voted_for()?,
            log: storage.entries_from(LogIndex::from(1))?,
        })
    }

    pub fn save<S: Storage<Cmd>>(&self, storage: &mut S) -> Result<(), S::Error> {
        storage.set_current_term(self.current_term)?;
        storage.set_voted_for(self.voted_for)?;

        let stored_len = storage.last_log_index()?;
        let current_len = LogIndex::from_length(self.log.len());

        if stored_len > current_len {
            storage.truncate_from(current_len.next())?;
        }

        for (idx, entry) in self.log.iter().enumerate() {
            let log_index = LogIndex::from((idx + 1) as u64);
            if log_index > stored_len {
                storage.append(entry.clone())?;
            }
        }

        Ok(())
    }
}

/// §Figure 2: commit_index and last_applied. Reset to zero on every restart — not persisted.
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

/// A committed entry ready to apply to the state machine.
#[derive(Debug, PartialEq, Eq)]
pub struct Applied<'a, Cmd> {
    pub index: LogIndex,
    pub command: &'a Cmd,
}

pub struct Node<Cmd> {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub persistent: PersistentState<Cmd>,
    pub volatile: VolatileState,
    pub role: Role,
}

impl<Cmd: Clone> Node<Cmd> {
    /// Starts as follower with no known leader (§5.1 initial state).
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            peers,
            persistent: PersistentState {
                current_term: Term::default(),
                voted_for: None,
                log: Vec::new(),
            },
            volatile: VolatileState {
                commit_index: LogIndex::default(),
                last_applied: LogIndex::default(),
            },
            role: Role::Follower(Follower::new()),
        }
    }

    /// Crash recovery: restore term, vote, and log from durable storage, restart as follower.
    pub fn from_storage<S: Storage<Cmd>>(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: &S,
    ) -> Result<Self, S::Error> {
        let persistent = PersistentState::load(storage)?;
        Ok(Self {
            id,
            peers,
            persistent,
            volatile: VolatileState {
                commit_index: LogIndex::default(),
                last_applied: LogIndex::default(),
            },
            role: Role::Follower(Follower::new()),
        })
    }

    /// Must be called before responding to any RPC (§5.1 durability requirement).
    pub fn save<S: Storage<Cmd>>(&self, storage: &mut S) -> Result<(), S::Error> {
        self.persistent.save(storage)
    }

    fn last_log_index(&self) -> LogIndex {
        LogIndex::from_length(self.persistent.log.len())
    }

    /// Convert to follower state. Only resets voted_for when moving to a newer term —
    /// stepping down within the same term (e.g. candidate hearing from current leader)
    /// must preserve the existing vote to uphold the at-most-one-vote-per-term invariant.
    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        if term > self.persistent.current_term {
            self.persistent.current_term = term;
            self.persistent.voted_for = None;
        }
        let mut follower = Follower::new();
        if let Some(id) = leader_id {
            follower.set_leader(id);
            info!(node = %self.id, term = %self.persistent.current_term, leader = %id, "became follower");
        } else {
            info!(node = %self.id, term = %self.persistent.current_term, "stepped down to follower");
        }
        self.role = Role::Follower(follower);
    }

    fn last_log_term(&self) -> Term {
        self.persistent
            .log
            .last()
            .map_or(Term::default(), |entry| entry.term)
    }

    /// Leaders ignore; followers and candidates start a new election.
    pub fn election_timeout(&mut self) -> Vec<Command<Cmd>> {
        match &self.role {
            Role::Leader(_) => Vec::new(),
            Role::Follower(_) | Role::Candidate(_) => self.start_election(),
        }
    }

    fn start_election(&mut self) -> Vec<Command<Cmd>> {
        self.persistent.current_term = self.persistent.current_term.increment();
        self.persistent.voted_for = Some(self.id);
        self.role = Role::Candidate(Candidate::new(self.id));
        info!(node = %self.id, term = %self.persistent.current_term, "election started");

        // Single node cluster: already have majority with own vote.
        let cluster_size = self.peers.len() + 1;
        if cluster_size == 1 {
            return self.become_leader();
        }

        let request = RequestVote {
            term: self.persistent.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        let mut commands = Vec::new();
        for &peer in &self.peers {
            commands.push(Command::Send {
                to: peer,
                message: Message::RequestVote(request.clone()),
            });
        }
        commands.push(Command::ResetElectionTimer);
        commands
    }

    /// §Figure 2 RequestVote RPC — receiver implementation.
    pub fn handle_request_vote(&mut self, from: NodeId, req: RequestVote) -> Vec<Command<Cmd>> {
        let mut reset_timer = false;
        if req.term > self.persistent.current_term {
            self.become_follower(req.term, None);
            reset_timer = true;
        }

        let vote_granted = self.should_grant_vote(&req);
        if vote_granted {
            self.persistent.voted_for = Some(req.candidate_id);
            reset_timer = true;
        }

        let mut commands = Vec::new();
        if reset_timer {
            commands.push(Command::ResetElectionTimer);
        }
        commands.push(Command::Send {
            to: from,
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: self.persistent.current_term,
                vote_granted,
            }),
        });
        commands
    }

    /// Grant vote only if all three conditions hold. Figure 2, RequestVote RPC §1–2:
    /// (1) candidate's term is current, (2) we haven't voted for someone else this term,
    /// (3) candidate's log is at least as up-to-date as ours (§5.4.1).
    fn should_grant_vote(&self, req: &RequestVote) -> bool {
        let term_ok = req.term >= self.persistent.current_term;
        let vote_ok = match self.persistent.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };
        let log_ok = self.is_log_up_to_date(req.last_log_term, req.last_log_index);

        term_ok && vote_ok && log_ok
    }

    /// §5.4.1: compare last-entry term first, then index.
    fn is_log_up_to_date(&self, candidate_term: Term, candidate_index: LogIndex) -> bool {
        (candidate_term, candidate_index) >= (self.last_log_term(), self.last_log_index())
    }

    /// Step down on higher term; become leader on majority.
    pub fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        resp: RequestVoteResponse,
    ) -> Vec<Command<Cmd>> {
        if resp.term < self.persistent.current_term {
            return Vec::new();
        }
        if resp.term > self.persistent.current_term {
            self.become_follower(resp.term, None);
            return vec![Command::ResetElectionTimer];
        }

        let dominated = match &mut self.role {
            Role::Candidate(candidate) => {
                if resp.vote_granted {
                    candidate.record_vote(from);
                }
                candidate.has_majority(self.peers.len() + 1)
            }
            Role::Follower(_) | Role::Leader(_) => return Vec::new(),
        };

        if dominated {
            self.become_leader()
        } else {
            Vec::new()
        }
    }

    /// §8: no-op entry commits prior-term entries indirectly via Log Matching, avoiding the
    /// Figure 8 anomaly where a leader cannot directly commit entries from previous terms.
    fn become_leader(&mut self) -> Vec<Command<Cmd>> {
        self.persistent.log.push(LogEntry {
            term: self.persistent.current_term,
            payload: LogPayload::NoOp,
        });
        self.role = Role::Leader(Leader::new(&self.peers, self.last_log_index()));
        info!(node = %self.id, term = %self.persistent.current_term, "became leader");
        self.send_heartbeats()
    }

    /// Leaders replicate and reset the heartbeat timer; non-leaders are no-ops.
    pub fn heartbeat_timeout(&mut self) -> Vec<Command<Cmd>> {
        match &self.role {
            Role::Leader(_) => self.send_heartbeats(),
            _ => Vec::new(),
        }
    }

    // §5.3: prev_log_index/term lets the follower detect a gap before accepting new entries.
    fn send_heartbeats(&self) -> Vec<Command<Cmd>> {
        let Role::Leader(leader) = &self.role else {
            return Vec::new();
        };

        let mut commands = Vec::new();

        for &peer in &self.peers {
            let next_index = leader
                .next_index_for(peer)
                .unwrap_or_else(|| self.last_log_index().next());

            let prev_log_index = next_index.prev().unwrap_or_default();
            let prev_log_term = self.term_at(prev_log_index);

            let entries = self.entries_from(next_index);

            commands.push(Command::Send {
                to: peer,
                message: Message::AppendEntries(AppendEntries {
                    term: self.persistent.current_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.volatile.commit_index,
                }),
            });
        }

        commands.push(Command::ResetHeartbeatTimer);
        commands
    }

    fn term_at(&self, index: LogIndex) -> Term {
        match index.to_array_index() {
            None => Term::default(),
            Some(idx) => self
                .persistent
                .log
                .get(idx)
                .map(|e| e.term)
                .unwrap_or_default(),
        }
    }

    fn entries_from(&self, start: LogIndex) -> Vec<LogEntry<Cmd>> {
        match start.to_array_index() {
            None => self.persistent.log.clone(),
            Some(idx) => self.persistent.log.get(idx..).unwrap_or_default().to_vec(),
        }
    }

    /// §Figure 2 AppendEntries RPC — receiver implementation.
    pub fn handle_append_entries(
        &mut self,
        from: NodeId,
        req: AppendEntries<Cmd>,
    ) -> Vec<Command<Cmd>> {
        if req.term > self.persistent.current_term {
            self.become_follower(req.term, Some(req.leader_id));
        }
        if req.term < self.persistent.current_term {
            return vec![Command::Send {
                to: from,
                message: Message::AppendEntriesResponse(AppendEntriesResponse::Rejected {
                    term: self.persistent.current_term,
                }),
            }];
        }

        // §5.2: a candidate that hears from a leader in its own term must step down.
        // voted_for is kept — we already voted for ourselves this term and that is valid.
        if matches!(self.role, Role::Candidate(_)) {
            self.become_follower(req.term, Some(req.leader_id));
        }

        // Valid AppendEntries from current leader.
        if let Role::Follower(follower) = &mut self.role {
            follower.set_leader(req.leader_id);
        }

        let mut commands = vec![Command::ResetElectionTimer];

        if !self.check_log_consistency(req.prev_log_index, req.prev_log_term) {
            commands.push(Command::Send {
                to: from,
                message: Message::AppendEntriesResponse(AppendEntriesResponse::Rejected {
                    term: self.persistent.current_term,
                }),
            });
            return commands;
        }

        self.append_entries(req.prev_log_index, req.entries);

        if req.leader_commit > self.volatile.commit_index {
            self.volatile.commit_index = std::cmp::min(req.leader_commit, self.last_log_index());
        }

        commands.push(Command::Send {
            to: from,
            message: Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
                term: self.persistent.current_term,
                match_index: self.last_log_index(),
            }),
        });

        commands
    }

    /// §5.3 Log Matching: reject if prev_log_index/term don't match our log (Figure 2 §2).
    fn check_log_consistency(&self, prev_log_index: LogIndex, prev_log_term: Term) -> bool {
        match prev_log_index.to_array_index() {
            // Index 0 is the implicit sentinel: term must also be 0.
            None => prev_log_term == Term::default(),
            Some(idx) => self
                .persistent
                .log
                .get(idx)
                .is_some_and(|entry| entry.term == prev_log_term),
        }
    }

    /// Non-leaders and stale terms are no-ops; leaders update replication progress.
    pub fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        resp: AppendEntriesResponse,
    ) -> Vec<Command<Cmd>> {
        let term = resp.term();
        if term > self.persistent.current_term {
            self.become_follower(term, None);
            return vec![Command::ResetElectionTimer];
        }
        if term < self.persistent.current_term {
            return Vec::new();
        }

        let accepted = match (&mut self.role, resp) {
            (Role::Leader(leader), AppendEntriesResponse::Accepted { match_index, .. }) => {
                leader.record_success(from, match_index);
                true
            }
            (Role::Leader(leader), AppendEntriesResponse::Rejected { .. }) => {
                leader.record_failure(from);
                false
            }
            _ => false,
        };

        if accepted {
            self.advance_commit_index();
        }

        Vec::new()
    }

    /// Returns the assigned log index. None if not leader — caller must redirect.
    pub fn submit_command(&mut self, command: Cmd) -> Option<LogIndex> {
        if !matches!(self.role, Role::Leader(_)) {
            return None;
        }

        let entry = LogEntry {
            term: self.persistent.current_term,
            payload: LogPayload::Command(command),
        };
        self.persistent.log.push(entry);

        Some(self.last_log_index())
    }

    /// Figure 2, Rules for Servers (Leaders): if there exists N > commitIndex such that a
    /// majority of matchIndex[i] >= N and log[N].term == currentTerm, set commitIndex = N.
    /// §5.4.2, Figure 8: a leader may only commit entries from its current term directly;
    /// earlier entries are committed indirectly by the Log Matching Property.
    fn advance_commit_index(&mut self) {
        let Role::Leader(leader) = &self.role else {
            return;
        };

        // Collect match indices including leader's own implicit match.
        let mut match_indices: Vec<LogIndex> = leader.match_indices().collect();
        match_indices.push(self.last_log_index());
        match_indices.sort();

        // Majority position: with N nodes, need (N/2 + 1) replicas.
        // Sorted ascending, so match_indices[len/2] is the median.
        let majority_pos = match_indices.len() / 2;
        let majority_index = match_indices[majority_pos];

        // Only commit if entry is from current term (Figure 8 safety).
        let has_higher_index = majority_index > self.volatile.commit_index;
        let is_current_term = majority_index
            .to_array_index()
            .and_then(|idx| self.persistent.log.get(idx))
            .is_some_and(|e| e.term == self.persistent.current_term);

        if has_higher_index && is_current_term {
            self.volatile.commit_index = majority_index;
            debug!(node = %self.id, commit_index = %majority_index, "commit index advanced");
        }
    }

    /// True when commit_index has advanced past last_applied.
    pub fn has_pending_applies(&self) -> bool {
        self.volatile.commit_index > self.volatile.last_applied
    }

    /// §5.3: no-op entries advance last_applied but are not returned to the caller.
    pub fn take_entry_to_apply(&mut self) -> Option<Applied<'_, Cmd>> {
        loop {
            if self.volatile.last_applied >= self.volatile.commit_index {
                return None;
            }

            self.volatile.last_applied = self.volatile.last_applied.next();
            let index = self.volatile.last_applied;

            let idx = index.to_array_index()?;
            let entry = self.persistent.log.get(idx)?;

            match &entry.payload {
                LogPayload::NoOp => {}
                LogPayload::Command(command) => return Some(Applied { index, command }),
            }
        }
    }

    /// §5.3 Figure 2 AppendEntries §3–5: truncates on conflict (same index, different term).
    fn append_entries(&mut self, prev_log_index: LogIndex, entries: Vec<LogEntry<Cmd>>) {
        let mut insert_index = prev_log_index.next();

        for entry in entries {
            match insert_index.to_array_index() {
                Some(idx) if idx < self.persistent.log.len() => {
                    if self.persistent.log[idx].term != entry.term {
                        self.persistent.log.truncate(idx);
                        self.persistent.log.push(entry);
                    }
                }
                _ => {
                    self.persistent.log.push(entry);
                }
            }
            insert_index = insert_index.next();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64, peers: &[u64]) -> Node<String> {
        Node::new(
            NodeId::from(id),
            peers.iter().map(|&p| NodeId::from(p)).collect(),
        )
    }

    fn is_follower(node: &Node<String>) -> bool {
        matches!(node.role, Role::Follower(_))
    }

    fn is_candidate(node: &Node<String>) -> bool {
        matches!(node.role, Role::Candidate(_))
    }

    fn is_leader(node: &Node<String>) -> bool {
        matches!(node.role, Role::Leader(_))
    }

    fn extract_vote_granted(cmds: &[Command<String>]) -> bool {
        cmds.iter()
            .find_map(|c| match c {
                Command::Send {
                    message: Message::RequestVoteResponse(r),
                    ..
                } => Some(r.vote_granted),
                _ => None,
            })
            .unwrap()
    }

    fn extract_append_success(cmds: &[Command<String>]) -> bool {
        cmds.iter()
            .find_map(|c| match c {
                Command::Send {
                    message: Message::AppendEntriesResponse(r),
                    ..
                } => Some(matches!(r, AppendEntriesResponse::Accepted { .. })),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn new_node_is_follower() {
        let n = node(1, &[2, 3]);
        assert!(is_follower(&n));
        assert_eq!(n.persistent.current_term, Term::default());
        assert_eq!(n.persistent.voted_for, None);
    }

    #[test]
    fn election_timeout_starts_election() {
        let mut n = node(1, &[2, 3]);
        let commands = n.election_timeout();

        assert!(is_candidate(&n));
        assert_eq!(n.persistent.current_term, Term::from(1));
        assert_eq!(n.persistent.voted_for, Some(NodeId::from(1)));

        let send_count = commands
            .iter()
            .filter(|c| matches!(c, Command::Send { .. }))
            .count();
        assert_eq!(send_count, 2);
    }

    #[test]
    fn candidate_becomes_leader_with_majority() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();

        let resp = RequestVoteResponse {
            term: Term::from(1),
            vote_granted: true,
        };
        n.handle_request_vote_response(NodeId::from(2), resp);

        assert!(is_leader(&n));
    }

    #[test]
    fn candidate_stays_candidate_without_majority() {
        let mut n = node(1, &[2, 3, 4, 5]);
        n.election_timeout();

        let resp = RequestVoteResponse {
            term: Term::from(1),
            vote_granted: true,
        };
        n.handle_request_vote_response(NodeId::from(2), resp);

        assert!(is_candidate(&n));
    }

    #[test]
    fn node_rejects_vote_if_already_voted() {
        let mut n = node(1, &[2, 3]);

        let req1 = RequestVote {
            term: Term::from(1),
            candidate_id: NodeId::from(2),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(2), req1);
        assert!(extract_vote_granted(&cmds));

        let req2 = RequestVote {
            term: Term::from(1),
            candidate_id: NodeId::from(3),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(3), req2);
        assert!(!extract_vote_granted(&cmds));
    }

    #[test]
    fn node_grants_vote_in_new_term() {
        let mut n = node(1, &[2, 3]);
        n.persistent.current_term = Term::from(1);
        n.persistent.voted_for = Some(NodeId::from(2));

        let req = RequestVote {
            term: Term::from(2),
            candidate_id: NodeId::from(3),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(3), req);
        assert!(extract_vote_granted(&cmds));
    }

    #[test]
    fn node_rejects_vote_with_stale_log() {
        let mut n = node(1, &[2, 3]);
        n.persistent.log.push(LogEntry {
            term: Term::from(2),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });

        let req = RequestVote {
            term: Term::from(2),
            candidate_id: NodeId::from(2),
            last_log_index: LogIndex::default(),
            last_log_term: Term::default(),
        };
        let cmds = n.handle_request_vote(NodeId::from(2), req);
        assert!(!extract_vote_granted(&cmds));
    }

    #[test]
    fn append_entries_resets_election_timer() {
        let mut n = node(1, &[2, 3]);

        let req = AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        let cmds = n.handle_append_entries(NodeId::from(2), req);

        assert!(cmds
            .iter()
            .any(|c| matches!(c, Command::ResetElectionTimer)));
    }

    #[test]
    fn append_entries_rejects_stale_term() {
        let mut n = node(1, &[2, 3]);
        n.persistent.current_term = Term::from(5);

        let req = AppendEntries {
            term: Term::from(3),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        let cmds = n.handle_append_entries(NodeId::from(2), req);
        assert!(!extract_append_success(&cmds));
    }

    #[test]
    fn append_entries_appends_new_entries() {
        let mut n = node(1, &[2, 3]);

        let req = AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("a".to_string()),
                },
                LogEntry {
                    term: Term::from(1),
                    payload: LogPayload::Command("b".to_string()),
                },
            ],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

        assert_eq!(n.persistent.log.len(), 2);
    }

    #[test]
    fn append_entries_truncates_on_conflict() {
        let mut n = node(1, &[2, 3]);
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        });

        let req = AppendEntries {
            term: Term::from(2),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::from(1),
            prev_log_term: Term::from(1),
            entries: vec![LogEntry {
                term: Term::from(2),
                payload: LogPayload::Command("SET status=active".to_string()),
            }],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

        assert_eq!(n.persistent.log.len(), 2);
        assert_eq!(n.persistent.log[1].payload, LogPayload::Command("SET status=active".to_string()));
        assert_eq!(n.persistent.log[1].term, Term::from(2));
    }

    #[test]
    fn higher_term_converts_to_follower() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        assert!(is_candidate(&n));

        let req = AppendEntries {
            term: Term::from(5),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

        assert!(is_follower(&n));
        assert_eq!(n.persistent.current_term, Term::from(5));
    }

    #[test]
    fn leader_advances_commit_index_on_majority() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );
        assert!(is_leader(&n));

        // Submit a command (no-op is at index 1, command at index 2).
        let index = n.submit_command("SET counter=1".to_string());
        assert_eq!(index, Some(LogIndex::from(2)));

        // Simulate successful replication of no-op to one follower.
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(1),
            },
        );

        // No-op at index 1 (current term) achieves majority → commit_index advances.
        assert_eq!(n.volatile.commit_index, LogIndex::from(1));
    }

    #[test]
    fn leader_decrements_next_index_on_failure() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );
        assert!(is_leader(&n));

        // Add entries to leader's log.
        n.submit_command("SET name=miles".to_string());
        n.submit_command("SET counter=1".to_string());

        let Role::Leader(leader) = &n.role else {
            panic!("expected leader");
        };
        let initial_next = leader.next_index_for(NodeId::from(2)).unwrap();

        // Simulate failed replication.
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse::Rejected {
                term: Term::from(1),
            },
        );

        let Role::Leader(leader) = &n.role else {
            panic!("expected leader");
        };
        let new_next = leader.next_index_for(NodeId::from(2)).unwrap();

        assert!(new_next < initial_next);
    }

    #[test]
    fn submit_command_fails_on_non_leader() {
        let mut n = node(1, &[2, 3]);
        assert!(n.submit_command("SET counter=1".to_string()).is_none());

        n.election_timeout();
        assert!(n.submit_command("SET counter=1".to_string()).is_none());
    }

    #[test]
    fn leader_does_not_commit_entries_from_previous_term() {
        use crate::state::Leader as LeaderState;

        // Build a leader in term 2 that has an old entry at index 1 (term 1) but no
        // current-term entry yet.  This replicates the Figure 8 scenario: the leader
        // must not advance commitIndex to an entry whose term < currentTerm even when
        // it has majority replication — only a current-term entry may trigger the commit.
        let mut n = node(1, &[2, 3]);
        n.persistent.current_term = Term::from(2);
        n.persistent.voted_for = Some(NodeId::from(1));
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        // Set up leader state with next_index starting after the existing entry.
        n.role = Role::Leader(LeaderState::new(&n.peers, LogIndex::from(1)));

        // Peer 2 reports it has replicated the old entry.
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse::Accepted {
                term: Term::from(2),
                match_index: LogIndex::from(1),
            },
        );

        // Must not commit: entry at index 1 is from term 1, not currentTerm 2.
        assert_eq!(n.volatile.commit_index, LogIndex::default());
    }

    #[test]
    fn take_entry_to_apply_returns_committed_entries() {
        let mut n = node(1, &[2, 3]);

        // Add entries to log.
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET counter=1".to_string()),
        });

        // Commit first entry.
        n.volatile.commit_index = LogIndex::from(1);

        assert!(n.has_pending_applies());
        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(1));
        assert_eq!(applied.command, &"SET name=miles".to_string());
        assert!(!n.has_pending_applies());
        assert!(n.take_entry_to_apply().is_none());

        // Commit second entry.
        n.volatile.commit_index = LogIndex::from(2);

        assert!(n.has_pending_applies());
        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(2));
        assert_eq!(applied.command, &"SET counter=1".to_string());
        assert!(!n.has_pending_applies());
    }

    #[test]
    fn take_entry_to_apply_advances_last_applied() {
        let mut n = node(1, &[2, 3]);

        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET counter=1".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(1);

        assert_eq!(n.volatile.last_applied, LogIndex::default());
        n.take_entry_to_apply();
        assert_eq!(n.volatile.last_applied, LogIndex::from(1));
    }

    #[test]
    fn become_leader_appends_noop_entry() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );

        assert!(is_leader(&n));
        assert_eq!(n.persistent.log.len(), 1);
        assert!(matches!(n.persistent.log[0].payload, LogPayload::NoOp));
        assert_eq!(n.persistent.log[0].term, Term::from(1));
    }

    #[test]
    fn take_entry_to_apply_skips_noop() {
        let mut n = node(1, &[2, 3]);

        // no-op at index 1, real command at index 2.
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::NoOp,
        });
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET price=100".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(2);

        // First call must skip the no-op and return the real command.
        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(2));
        assert_eq!(applied.command, &"SET price=100".to_string());
        assert_eq!(n.volatile.last_applied, LogIndex::from(2));
        assert!(n.take_entry_to_apply().is_none());
    }

    #[test]
    fn candidate_steps_down_on_same_term_append_entries() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout(); // term 1, voted for self
        assert!(is_candidate(&n));
        assert_eq!(n.persistent.voted_for, Some(NodeId::from(1)));

        let req = AppendEntries {
            term: Term::from(1), // same term — leader already elected
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

        // must revert to follower but keep the vote (§5.2)
        assert!(is_follower(&n));
        assert_eq!(n.persistent.voted_for, Some(NodeId::from(1)));
    }

    #[test]
    fn save_and_load_persistent_state() {
        use crate::storage::MemoryStorage;

        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            },
        );
        n.submit_command("SET name=miles".to_string());
        n.submit_command("SET counter=1".to_string());

        let mut storage = MemoryStorage::new();
        n.save(&mut storage).unwrap();

        let restored: Node<String> =
            Node::from_storage(NodeId::from(1), vec![NodeId::from(2), NodeId::from(3)], &storage)
                .unwrap();

        assert_eq!(
            restored.persistent.current_term,
            n.persistent.current_term
        );
        assert_eq!(restored.persistent.voted_for, n.persistent.voted_for);
        assert_eq!(restored.persistent.log.len(), n.persistent.log.len());
        assert!(is_follower(&restored));
    }

    #[test]
    fn stale_vote_response_is_ignored() {
        let mut n = node(1, &[2, 3]);
        n.persistent.current_term = Term::from(3);

        let cmds = n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse { term: Term::from(1), vote_granted: true },
        );

        assert!(cmds.is_empty());
        assert!(is_follower(&n));
    }

    #[test]
    fn vote_response_is_ignored_when_not_candidate() {
        // same-term response so term guards don't fire; role check must reject
        let mut n = node(1, &[2, 3]);
        n.persistent.current_term = Term::from(1);

        let cmds = n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse { term: Term::from(1), vote_granted: true },
        );

        assert!(cmds.is_empty());
        assert!(is_follower(&n));
    }

    #[test]
    fn append_entries_rejects_on_prev_term_mismatch() {
        let mut n = node(1, &[2, 3]);
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET counter=1".to_string()),
        });

        let req = AppendEntries {
            term: Term::from(2),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::from(1),
            prev_log_term: Term::from(2), // mismatches our term 1 at index 1
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        let cmds = n.handle_append_entries(NodeId::from(2), req);

        assert!(!extract_append_success(&cmds));
    }

    #[test]
    fn follower_advances_commit_index_from_leader_commit() {
        let mut n = node(1, &[2, 3]);
        n.persistent.log.push(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET counter=1".to_string()),
        });

        let req = AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::from(1),
            prev_log_term: Term::from(1),
            entries: vec![],
            leader_commit: LogIndex::from(1),
        };
        n.handle_append_entries(NodeId::from(2), req);

        assert_eq!(n.volatile.commit_index, LogIndex::from(1));
    }
}
