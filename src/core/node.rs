use std::collections::HashSet;

use tracing::debug;
use tracing::info;

use crate::core::command::Command;
use crate::core::state::Candidate;
use crate::core::state::Follower;
use crate::core::state::Leader;
use crate::core::types::AppendEntries;
use crate::core::types::AppendEntriesResponse;
use crate::core::types::ClusterConfig;
use crate::core::types::InstallSnapshot;
use crate::core::types::InstallSnapshotResponse;
use crate::core::types::Log;
use crate::core::types::LogEntry;
use crate::core::types::LogIndex;
use crate::core::types::LogPayload;
use crate::core::types::Membership;
use crate::core::types::MergeOutcome;
use crate::core::types::Message;
use crate::core::types::NodeId;
use crate::core::types::RequestVote;
use crate::core::types::RequestVoteResponse;
use crate::core::types::Snapshot;
use crate::core::types::SnapshotData;
use crate::core::types::SnapshotMeta;
use crate::core::types::Term;
use crate::core::types::TermLookup;
use crate::core::types::Vote;
use crate::storage::Storage;

/// Why `Node::submit_command` refused a client command: this node isn't the leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("not the leader")]
pub struct NotLeaderError {
    /// Best-known current leader, if this node has heard from one — the client can retry there.
    pub leader_hint: Option<NodeId>,
}

/// Why `Node::propose_config_change` refused a membership change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    #[error("not the leader")]
    NotLeader { leader_hint: Option<NodeId> },
    /// Single-server changes (§4.1) allow at most one uncommitted config change at a time.
    #[error("a config change is already pending")]
    ConfigChangePending,
}

/// Why `Node::compact_to_snapshot` refused to compact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompactError {
    /// `last_applied` is already at or below the current snapshot boundary —
    /// nothing has been applied since the last compaction.
    #[error("nothing to compact: last_applied is at or below the snapshot boundary")]
    NothingToCompact,
}

/// §Figure 2: current_term, voted_for, log. Must be written to durable storage before
/// responding to any RPC — persisting after responding violates Raft's safety guarantees.
///
/// `Node` is the single owner of the log; `save` never re-derives what changed by
/// diffing against storage, which would silently drop a same-length conflict
/// overwrite. Every mutation records precisely what happened — `set_term`/
/// `set_voted_for` flag the meta dirty, `append_entry`/`merge_entries` record the
/// exact truncation point and appended suffix — and `save` just replays that
/// record to storage, then clears it.
#[derive(Debug)]
pub struct PersistentState<Cmd> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log<Cmd>,
    meta_dirty: bool,
    truncated_from: Option<LogIndex>,
    pending_append: Vec<LogEntry<Cmd>>,
    /// Recorded by `compact_to_snapshot`/`handle_install_snapshot`, replayed by
    /// `save` as `storage.install_snapshot(...)`, then cleared — same
    /// record-then-replay pattern as `truncated_from`/`pending_append`.
    pending_snapshot_install: Option<Snapshot>,
    /// The most recently installed snapshot, kept for the leader send path
    /// (building `InstallSnapshot` RPCs) — unlike `pending_snapshot_install`,
    /// this persists for the node's lifetime, not just until the next `save`.
    latest_snapshot: Option<Snapshot>,
}

impl<Cmd: Clone> PersistentState<Cmd> {
    fn new() -> Self {
        Self {
            current_term: Term::default(),
            voted_for: None,
            log: Log::new(),
            meta_dirty: false,
            truncated_from: None,
            pending_append: Vec::new(),
            pending_snapshot_install: None,
            latest_snapshot: None,
        }
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    pub fn log(&self) -> &Log<Cmd> {
        &self.log
    }

    pub fn latest_snapshot(&self) -> Option<&Snapshot> {
        self.latest_snapshot.as_ref()
    }

    fn set_term(&mut self, term: Term) {
        if term != self.current_term {
            self.current_term = term;
            self.meta_dirty = true;
        }
    }

    fn set_voted_for(&mut self, candidate: Option<NodeId>) {
        if candidate != self.voted_for {
            self.voted_for = candidate;
            self.meta_dirty = true;
        }
    }

    /// Leader-side: append one entry to the tail. Returns its assigned index.
    fn append_entry(&mut self, entry: LogEntry<Cmd>) -> LogIndex {
        let index = self.log.append(entry.clone());
        self.pending_append.push(entry);
        index
    }

    /// Follower-side §5.3 conflict resolution — delegates to `Log::merge` and
    /// records exactly what it did so `save` can tell storage the same thing.
    fn merge_entries(&mut self, prev_index: LogIndex, entries: Vec<LogEntry<Cmd>>) -> MergeOutcome {
        let before_len = self.log.len();
        let outcome = self.log.merge(prev_index, entries);

        match outcome {
            MergeOutcome::Truncated { from } => {
                self.truncated_from = Some(from);
                self.pending_append = self.log.suffix_from(from).to_vec();
            }
            MergeOutcome::Appended => {
                let new_start = LogIndex::from_length(before_len).next();
                self.pending_append
                    .extend(self.log.suffix_from(new_start).iter().cloned());
            }
        }

        outcome
    }

    pub fn load<S: Storage<Cmd>>(storage: &S) -> Result<Self, S::Error> {
        let loaded = storage.load()?;
        // A persisted snapshot fixes the log's index offset — `from_entries`
        // would otherwise treat the already-reconciled suffix as starting at
        // index 1, corrupting every index arithmetic downstream.
        let log = match &loaded.snapshot {
            Some(snapshot) => Log::from_snapshot_and_suffix(
                snapshot.meta.last_index,
                snapshot.meta.last_term,
                loaded.entries,
            ),
            None => Log::from_entries(loaded.entries),
        };
        Ok(Self {
            current_term: loaded.current_term,
            voted_for: loaded.voted_for,
            log,
            meta_dirty: false,
            truncated_from: None,
            pending_append: Vec::new(),
            pending_snapshot_install: None,
            latest_snapshot: loaded.snapshot,
        })
    }

    pub fn save<S: Storage<Cmd>>(&mut self, storage: &mut S) -> Result<(), S::Error> {
        if self.meta_dirty {
            storage.set_meta(self.current_term, self.voted_for)?;
            self.meta_dirty = false;
        }
        if let Some(snapshot) = self.pending_snapshot_install.take() {
            storage.install_snapshot(&snapshot)?;
        }
        if let Some(from) = self.truncated_from.take() {
            storage.truncate_from(from)?;
        }
        if !self.pending_append.is_empty() {
            storage.append(&self.pending_append)?;
            self.pending_append.clear();
        }
        Ok(())
    }

    /// Discard-case `InstallSnapshot`: wipes the local log and records the
    /// truncation so `save` clears any stale suffix in storage. Storage-side
    /// `install_snapshot` only runs `compact_through`, which keeps entries
    /// above the boundary — without this, a suffix discarded here would
    /// survive on disk and diverge from the in-memory log after restart.
    fn discard_to_snapshot(&mut self, last_index: LogIndex, last_term: Term) {
        self.log.reset_to_snapshot(last_index, last_term);
        self.truncated_from = Some(last_index.next());
    }
}

/// §Figure 2: commit_index and last_applied. Reset to zero on every restart — not persisted.
#[derive(Debug)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

#[derive(Debug)]
pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

/// A committed entry ready to apply to the state machine.
#[derive(Debug, PartialEq, Eq)]
pub struct Applied<'a, Cmd> {
    pub index: LogIndex,
    /// Term of the entry actually committed at `index` — not necessarily the term the
    /// submitting client saw. Callers key pending responses on `(term, index)` so a
    /// later leader's unrelated entry landing at the same index can never be mistaken
    /// for the original submission.
    pub term: Term,
    pub command: &'a Cmd,
}

#[derive(Debug)]
pub struct Node<Cmd> {
    id: NodeId,
    /// Current effective cluster config (from latest ConfigChange in log, or initial_config).
    config: ClusterConfig,
    /// Config at startup — used as the floor when rescanning the log after a truncation
    /// removes all ConfigChange entries.
    initial_config: ClusterConfig,
    persistent: PersistentState<Cmd>,
    volatile: VolatileState,
    role: Role,
    /// Configs applied on append (before commit). Drained by Runtime → Transport.
    pending_config_changes: Vec<ClusterConfig>,
    /// Configs that have just been committed. Drained by Server to resolve HTTP requests.
    pending_committed_config_changes: Vec<(LogIndex, ClusterConfig)>,
    /// A snapshot installed via `handle_install_snapshot`, awaiting the
    /// Runtime's `state_machine.restore()` call. Drained by `take_snapshot_to_restore`.
    pending_snapshot_restore: Option<Snapshot>,
}

impl<Cmd: Clone> Node<Cmd> {
    /// Starts as follower with no known leader (§5.1 initial state).
    pub fn new(id: NodeId, config: ClusterConfig) -> Self {
        Self {
            id,
            initial_config: config.clone(),
            config,
            persistent: PersistentState::new(),
            volatile: VolatileState {
                commit_index: LogIndex::default(),
                last_applied: LogIndex::default(),
            },
            role: Role::Follower(Follower::new()),
            pending_config_changes: Vec::new(),
            pending_committed_config_changes: Vec::new(),
            pending_snapshot_restore: None,
        }
    }

    /// Crash recovery: restore term, vote, and log from durable storage, restart as follower.
    /// Derives the current config from the latest ConfigChange entry in the restored log.
    pub fn from_storage<S: Storage<Cmd>>(
        id: NodeId,
        initial_config: ClusterConfig,
        storage: &S,
    ) -> Result<Self, S::Error> {
        let persistent = PersistentState::load(storage)?;
        // A restored snapshot already covers everything up to its boundary — starting
        // at zero would make take_entry_to_apply replay committed entries the snapshot
        // (and the state machine restored from it) already reflect, double-applying them.
        let boundary = persistent.log().snapshot_last_index();
        let mut node = Self {
            id,
            config: initial_config.clone(),
            initial_config,
            persistent,
            volatile: VolatileState {
                commit_index: boundary,
                last_applied: boundary,
            },
            role: Role::Follower(Follower::new()),
            pending_config_changes: Vec::new(),
            pending_committed_config_changes: Vec::new(),
            pending_snapshot_restore: None,
        };
        // Replay the latest config from the restored log so peers and transport
        // are updated correctly on restart.
        node.apply_latest_config_from_log();
        Ok(node)
    }

    /// Must be called before responding to any RPC (§5.1 durability requirement).
    pub fn save<S: Storage<Cmd>>(&mut self, storage: &mut S) -> Result<(), S::Error> {
        self.persistent.save(storage)
    }

    /// This node's identifier.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Current role: follower, candidate, or leader.
    pub fn role(&self) -> &Role {
        &self.role
    }

    /// Current effective cluster config.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Read-only view of term/vote/log — mutation is internal to `Node`.
    pub fn persistent(&self) -> &PersistentState<Cmd> {
        &self.persistent
    }

    /// Read-only view of commit_index/last_applied — mutation is internal to `Node`.
    pub fn volatile(&self) -> &VolatileState {
        &self.volatile
    }

    /// All member IDs except this node's own — derived from `config` on demand rather
    /// than cached, so it can never desynchronize from it.
    fn peer_ids(&self) -> Vec<NodeId> {
        self.config.peer_ids(self.id)
    }

    fn last_log_index(&self) -> LogIndex {
        self.persistent.log.last_index()
    }

    /// Convert to follower state. Only resets voted_for when moving to a newer term —
    /// stepping down within the same term (e.g. candidate hearing from current leader)
    /// must preserve the existing vote to uphold the at-most-one-vote-per-term invariant.
    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        if term > self.persistent.current_term {
            self.persistent.set_term(term);
            self.persistent.set_voted_for(None);
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
        self.persistent.log.last_term()
    }

    /// Apply a new cluster config: updates peers, leader tracking maps, and emits a
    /// pending notification so the transport is updated before the next heartbeat.
    fn set_config(&mut self, config: ClusterConfig) {
        let old_peers: HashSet<NodeId> = self.peer_ids().into_iter().collect();
        let new_peers: HashSet<NodeId> = config.peer_ids(self.id).into_iter().collect();
        let last = self.persistent.log.last_index();

        if let Role::Leader(leader) = &mut self.role {
            for &peer in &new_peers {
                if !old_peers.contains(&peer) {
                    leader.add_peer(peer, last);
                }
            }
            for &old_peer in &old_peers {
                if !new_peers.contains(&old_peer) {
                    leader.remove_peer(old_peer);
                }
            }
        }

        self.config = config.clone();
        self.pending_config_changes.push(config);
    }

    /// Scan the log backward for the latest ConfigChange entry and apply it.
    /// Called after a truncation (which may have removed a previously active config)
    /// and on startup (to replay the config from a recovered log). Falls back to the
    /// snapshot's config (if any) before `initial_config` — a compacted prefix may
    /// have discarded the only ConfigChange entry that ever existed.
    fn apply_latest_config_from_log(&mut self) {
        let latest = self.persistent.log.iter().rev().find_map(|e| {
            if let LogPayload::ConfigChange(c) = &e.payload {
                Some(c.clone())
            } else {
                None
            }
        });
        let config = latest
            .or_else(|| {
                self.persistent
                    .latest_snapshot
                    .as_ref()
                    .map(|s| s.meta.config.clone())
            })
            .unwrap_or_else(|| self.initial_config.clone());
        if config != self.config {
            self.set_config(config);
        }
    }

    /// Leaders ignore; followers and candidates start a new election.
    pub fn election_timeout(&mut self) -> Vec<Command<Cmd>> {
        match &self.role {
            Role::Leader(_) => Vec::new(),
            Role::Follower(_) | Role::Candidate(_) => self.start_election(),
        }
    }

    fn start_election(&mut self) -> Vec<Command<Cmd>> {
        self.persistent
            .set_term(self.persistent.current_term.next());
        self.persistent.set_voted_for(Some(self.id));
        self.role = Role::Candidate(Candidate::new(self.id));
        info!(node = %self.id, term = %self.persistent.current_term, "election started");

        let peers = self.peer_ids();

        // Single node cluster: already have majority with own vote.
        let cluster_size = peers.len() + 1;
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
        for &peer in &peers {
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

        let vote = if self.should_grant_vote(&req) {
            self.persistent.set_voted_for(Some(req.candidate_id));
            reset_timer = true;
            info!(node = %self.id, term = %self.persistent.current_term, candidate = %req.candidate_id, "vote granted");
            Vote::Granted
        } else {
            debug!(node = %self.id, term = %self.persistent.current_term, candidate = %req.candidate_id, "vote denied");
            Vote::Denied
        };

        let mut commands = Vec::new();
        if reset_timer {
            commands.push(Command::ResetElectionTimer);
        }
        commands.push(Command::Send {
            to: from,
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: self.persistent.current_term,
                vote,
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

        // A vote from outside the current config must not count toward the majority —
        // transport has no authentication, and a removed member's stale response must
        // not be able to hand out an election win.
        let membership = self.config.membership_of(from);
        let cluster_size = self.peer_ids().len() + 1;
        let dominated = match &mut self.role {
            Role::Candidate(candidate) => {
                match (resp.vote, membership) {
                    (Vote::Granted, Membership::Member) => {
                        candidate.record_vote(from);
                        debug!(node = %self.id, term = %self.persistent.current_term, from = %from, "vote received");
                    }
                    (Vote::Granted, Membership::NonMember)
                    | (Vote::Denied, Membership::Member)
                    | (Vote::Denied, Membership::NonMember) => {}
                }
                candidate.has_majority(cluster_size)
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
        self.persistent.append_entry(LogEntry {
            term: self.persistent.current_term,
            payload: LogPayload::NoOp,
        });
        self.role = Role::Leader(Leader::new(&self.peer_ids(), self.last_log_index()));
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

        // Collect tracked peers upfront to avoid a borrow-checker conflict: the iterator
        // borrows `leader` (which borrows `self.role`), but the loop body also needs `&self`
        // to call term_at / entries_from.
        let peers: Vec<NodeId> = leader.tracked_peers().collect();
        let mut commands = Vec::new();

        for peer in peers {
            let next_index = leader
                .next_index_for(peer)
                .unwrap_or_else(|| self.last_log_index().next());

            // The entry at next_index - 1 has already been compacted away: an
            // AppendEntries here could never carry a valid prev_log_index, so
            // the peer needs the whole compacted prefix via InstallSnapshot instead.
            if next_index <= self.persistent.log.snapshot_last_index()
                && let Some(snapshot) = &self.persistent.latest_snapshot
            {
                commands.push(Command::Send {
                    to: peer,
                    message: Message::InstallSnapshot(InstallSnapshot {
                        term: self.persistent.current_term,
                        leader_id: self.id,
                        snapshot: snapshot.clone(),
                    }),
                });
                continue;
            }

            let prev_log_index = next_index.prev().unwrap_or_default();
            let prev_log_term = self.term_at(prev_log_index);

            let entries = self.entries_from(next_index);

            if !entries.is_empty() {
                debug!(node = %self.id, peer = %peer, count = entries.len(), prev_index = %prev_log_index, "replicating entries");
            }
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
        match self.persistent.log.term_at(index) {
            TermLookup::Known(term) => term,
            // send_heartbeats routes any next_index at or below the snapshot boundary
            // to InstallSnapshot instead, so prev_log_index here is always above the
            // boundary — Compacted cannot occur. BeyondEnd falls back defensively.
            TermLookup::Compacted | TermLookup::BeyondEnd => Term::default(),
        }
    }

    fn entries_from(&self, start: LogIndex) -> Vec<LogEntry<Cmd>> {
        self.persistent.log.suffix_from(start).to_vec()
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
            debug!(node = %self.id, our_term = %self.persistent.current_term, their_term = %req.term, leader = %from, "append entries rejected: stale term");
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
            debug!(node = %self.id, leader = %from, prev_index = %req.prev_log_index, prev_term = %req.prev_log_term, "append entries rejected: log inconsistency");
            commands.push(Command::Send {
                to: from,
                message: Message::AppendEntriesResponse(AppendEntriesResponse::Rejected {
                    term: self.persistent.current_term,
                }),
            });
            return commands;
        }

        let entry_count = req.entries.len();
        self.append_entries(req.prev_log_index, req.entries);
        if entry_count > 0 {
            debug!(node = %self.id, leader = %from, count = entry_count, match_index = %self.last_log_index(), "entries appended");
        }

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
        self.persistent.log.matches(prev_log_index, prev_log_term)
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
                debug!(node = %self.id, peer = %from, "replication rejected, decrementing next_index");
                false
            }
            _ => false,
        };

        if accepted {
            self.advance_commit_index();
        }

        Vec::new()
    }

    /// Best-known current leader, for redirecting a client that reached the wrong node.
    /// `None` for a leader (self is the answer) or a candidate (no leader known this term).
    pub fn leader_hint(&self) -> Option<NodeId> {
        match &self.role {
            Role::Follower(follower) => follower.leader_id(),
            Role::Candidate(_) | Role::Leader(_) => None,
        }
    }

    /// Appends a command to the log. Errors if this node isn't the leader.
    pub fn submit_command(&mut self, command: Cmd) -> Result<LogIndex, NotLeaderError> {
        if !matches!(self.role, Role::Leader(_)) {
            return Err(NotLeaderError {
                leader_hint: self.leader_hint(),
            });
        }

        let entry = LogEntry {
            term: self.persistent.current_term,
            payload: LogPayload::Command(command),
        };
        let index = self.persistent.append_entry(entry);
        debug!(node = %self.id, term = %self.persistent.current_term, index = %index, "command appended to log");
        Ok(index)
    }

    /// Propose a membership change. Returns the log index of the ConfigChange entry.
    ///
    /// The new config takes effect immediately on append (dissertation §4.1).
    ///
    /// # Errors
    /// `SubmitError::NotLeader` — this node isn't the leader.
    /// `SubmitError::ConfigChangePending` — another change is already uncommitted;
    /// single-server changes (§4.1) allow at most one in flight.
    pub fn propose_config_change(
        &mut self,
        config: ClusterConfig,
    ) -> Result<LogIndex, SubmitError> {
        if !matches!(self.role, Role::Leader(_)) {
            return Err(SubmitError::NotLeader {
                leader_hint: self.leader_hint(),
            });
        }
        if self.has_pending_config_change() {
            debug!(node = %self.id, "config change rejected: another change is pending");
            return Err(SubmitError::ConfigChangePending);
        }
        let entry = LogEntry {
            term: self.persistent.current_term,
            payload: LogPayload::ConfigChange(config.clone()),
        };
        let index = self.persistent.append_entry(entry);
        // Takes effect immediately on append — quorum calculations use the new config
        // before the entry is committed.
        self.set_config(config);
        info!(node = %self.id, term = %self.persistent.current_term, index = %index, members = self.config.size(), "config change proposed");
        Ok(index)
    }

    /// Returns true if there is a ConfigChange entry in the uncommitted suffix.
    /// Used to enforce the one-change-at-a-time rule of single-server changes.
    fn has_pending_config_change(&self) -> bool {
        let committed = self.volatile.commit_index;
        self.persistent.log.iter().enumerate().any(|(i, e)| {
            let index = LogIndex::from_length(i + 1);
            index > committed && matches!(e.payload, LogPayload::ConfigChange(_))
        })
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

        // Only commit if entry is from current term (Figure 8 safety). Uses
        // term_at (not entry()) so a majority_index sitting exactly at the
        // snapshot boundary — its entry gone via compaction — is still checked
        // correctly via the boundary's preserved term.
        let has_higher_index = majority_index > self.volatile.commit_index;
        let is_current_term = matches!(
            self.persistent.log.term_at(majority_index),
            TermLookup::Known(term) if term == self.persistent.current_term
        );

        if has_higher_index && is_current_term {
            self.volatile.commit_index = majority_index;
            debug!(node = %self.id, commit_index = %majority_index, "commit index advanced");
        }
    }

    /// True when commit_index has advanced past last_applied.
    pub fn has_pending_applies(&self) -> bool {
        self.volatile.commit_index > self.volatile.last_applied
    }

    /// §5.3: no-op and config-change entries advance last_applied but are not returned
    /// to the caller. Config changes are buffered in pending_committed_config_changes.
    pub fn take_entry_to_apply(&mut self) -> Option<Applied<'_, Cmd>> {
        loop {
            if self.volatile.last_applied >= self.volatile.commit_index {
                return None;
            }

            self.volatile.last_applied = self.volatile.last_applied.next();
            let index = self.volatile.last_applied;

            let entry = self.persistent.log.entry(index)?;

            match &entry.payload {
                LogPayload::NoOp => {}
                LogPayload::ConfigChange(cfg) => {
                    // Already applied to self.config on append.
                    // Buffer the commit notification for Server to resolve HTTP requests.
                    let cfg = cfg.clone();
                    self.pending_committed_config_changes.push((index, cfg));
                }
                LogPayload::Command(command) => {
                    return Some(Applied {
                        index,
                        term: entry.term,
                        command,
                    });
                }
            }
        }
    }

    /// Newly applied (appended, not necessarily committed) configs.
    /// Runtime passes these to Transport so RPCs to new peers can be sent immediately.
    pub fn take_config_changes(&mut self) -> Vec<ClusterConfig> {
        std::mem::take(&mut self.pending_config_changes)
    }

    /// Committed config changes with their log index.
    /// Server uses these to resolve pending membership HTTP requests.
    pub fn take_committed_config_changes(&mut self) -> Vec<(LogIndex, ClusterConfig)> {
        std::mem::take(&mut self.pending_committed_config_changes)
    }

    /// §5.3 Figure 2 AppendEntries §3–5: truncates on conflict (same index, different term).
    /// Calls apply_latest_config_from_log if a truncation occurred or a ConfigChange was added,
    /// because a truncation may have removed a previously active config.
    fn append_entries(&mut self, prev_log_index: LogIndex, entries: Vec<LogEntry<Cmd>>) {
        let has_config_entry = entries
            .iter()
            .any(|e| matches!(e.payload, LogPayload::ConfigChange(_)));

        let outcome = self.persistent.merge_entries(prev_log_index, entries);
        if let MergeOutcome::Truncated { .. } = outcome {
            debug!(node = %self.id, prev_index = %prev_log_index, "log truncated on conflict");
        }

        // A truncation may have removed a previously active ConfigChange; a newly
        // appended ConfigChange must also trigger a rescan to become the active config.
        if matches!(outcome, MergeOutcome::Truncated { .. }) || has_config_entry {
            self.apply_latest_config_from_log();
        }
    }

    /// The cluster config active as of `index`, without reading past it. `self.config`
    /// reflects config changes as of the log tail (changes take effect on append, not
    /// commit — dissertation §4.1), so it cannot be used directly whenever a later
    /// ConfigChange exists beyond `index`; this rescans the prefix in that case.
    fn config_as_of(&self, index: LogIndex) -> ClusterConfig {
        let has_later_config_change = self
            .persistent
            .log
            .suffix_from(index.next())
            .iter()
            .any(|e| matches!(e.payload, LogPayload::ConfigChange(_)));

        if !has_later_config_change {
            return self.config.clone();
        }

        let first = self.persistent.log.first_index();
        self.persistent
            .log
            .iter()
            .enumerate()
            .map(|(i, e)| (first.advance_by(i as u64), e))
            .take_while(|(entry_index, _)| *entry_index <= index)
            .filter_map(|(_, e)| match &e.payload {
                LogPayload::ConfigChange(c) => Some(c.clone()),
                _ => None,
            })
            .last()
            .or_else(|| {
                self.persistent
                    .latest_snapshot
                    .as_ref()
                    .map(|s| s.meta.config.clone())
            })
            .unwrap_or_else(|| self.initial_config.clone())
    }

    /// Compacts the log through `last_applied`, using the serialized state machine
    /// data the caller (Runtime, which owns the state machine) supplies.
    ///
    /// # Errors
    /// `CompactError::NothingToCompact` — nothing has been applied since the last
    /// compaction.
    pub fn compact_to_snapshot(&mut self, data: SnapshotData) -> Result<Snapshot, CompactError> {
        let last_index = self.volatile.last_applied;
        if last_index <= self.persistent.log.snapshot_last_index() {
            return Err(CompactError::NothingToCompact);
        }
        let last_term = self.term_at(last_index);
        let config = self.config_as_of(last_index);

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_index,
                last_term,
                config,
            },
            data,
        };

        self.persistent.log.compact_through(last_index, last_term);
        self.persistent.pending_snapshot_install = Some(snapshot.clone());
        self.persistent.latest_snapshot = Some(snapshot.clone());
        info!(node = %self.id, last_index = %last_index, "log compacted to snapshot");

        Ok(snapshot)
    }

    /// Snapshot buffered by `handle_install_snapshot`, awaiting the Runtime's
    /// `state_machine.restore()` call. Drained so a re-poll doesn't restore twice.
    pub fn take_snapshot_to_restore(&mut self) -> Option<Snapshot> {
        self.pending_snapshot_restore.take()
    }

    /// §7 InstallSnapshot RPC — receiver implementation (single-message variant,
    /// no offset/done chunking).
    pub fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        req: InstallSnapshot,
    ) -> Vec<Command<Cmd>> {
        if req.term < self.persistent.current_term {
            debug!(node = %self.id, our_term = %self.persistent.current_term, their_term = %req.term, leader = %from, "install snapshot rejected: stale term");
            return vec![Command::Send {
                to: from,
                message: Message::InstallSnapshotResponse(InstallSnapshotResponse::Rejected {
                    term: self.persistent.current_term,
                }),
            }];
        }

        if req.term > self.persistent.current_term {
            self.become_follower(req.term, Some(req.leader_id));
        }
        // §5.2: a candidate that hears from a leader in its own term must step down.
        if matches!(self.role, Role::Candidate(_)) {
            self.become_follower(req.term, Some(req.leader_id));
        }
        if let Role::Follower(follower) = &mut self.role {
            follower.set_leader(req.leader_id);
        }

        let mut commands = vec![Command::ResetElectionTimer];

        let last_index = req.snapshot.meta.last_index;
        let last_term = req.snapshot.meta.last_term;

        // Already have this committed — idempotent retry handling, nothing to do.
        if last_index <= self.volatile.commit_index {
            commands.push(Command::Send {
                to: from,
                message: Message::InstallSnapshotResponse(InstallSnapshotResponse::Installed {
                    term: self.persistent.current_term,
                    last_index,
                }),
            });
            return commands;
        }

        // Retain-suffix case: our log already agrees with the snapshot boundary,
        // so anything after it is still valid and must not be discarded.
        let retains_suffix = matches!(
            self.persistent.log.term_at(last_index),
            TermLookup::Known(term) if term == last_term
        );
        if retains_suffix {
            self.persistent.log.compact_through(last_index, last_term);
        } else {
            self.persistent.discard_to_snapshot(last_index, last_term);
        }

        // last_applied jumps straight to the boundary — cases above must never
        // re-apply log entries; any surviving suffix beyond commit_index applies
        // through the normal take_entry_to_apply path afterward.
        self.volatile.commit_index = std::cmp::max(self.volatile.commit_index, last_index);
        self.volatile.last_applied = last_index;
        self.set_config(req.snapshot.meta.config.clone());

        self.persistent.pending_snapshot_install = Some(req.snapshot.clone());
        self.persistent.latest_snapshot = Some(req.snapshot.clone());
        self.pending_snapshot_restore = Some(req.snapshot);
        info!(node = %self.id, leader = %from, last_index = %last_index, "snapshot installed");

        commands.push(Command::Send {
            to: from,
            message: Message::InstallSnapshotResponse(InstallSnapshotResponse::Installed {
                term: self.persistent.current_term,
                last_index,
            }),
        });
        commands
    }

    /// Non-leaders and stale terms are no-ops; leaders in-term advance replication
    /// progress and may complete a quorum. A `Rejected` response only ever means a
    /// stale term — there is no lower boundary to decrement to below a snapshot.
    pub fn handle_install_snapshot_response(
        &mut self,
        from: NodeId,
        resp: InstallSnapshotResponse,
    ) -> Vec<Command<Cmd>> {
        let term = resp.term();
        if term > self.persistent.current_term {
            self.become_follower(term, None);
            return vec![Command::ResetElectionTimer];
        }
        if term < self.persistent.current_term {
            return Vec::new();
        }

        if let (Role::Leader(leader), InstallSnapshotResponse::Installed { last_index, .. }) =
            (&mut self.role, resp)
        {
            leader.record_success(from, last_index);
            self.advance_commit_index();
        }

        Vec::new()
    }
}

#[cfg(test)]
impl<Cmd: Clone> Node<Cmd> {
    fn force_term(&mut self, term: Term) {
        self.persistent.set_term(term);
    }

    fn force_vote(&mut self, candidate: NodeId) {
        self.persistent.set_voted_for(Some(candidate));
    }

    fn push_entry(&mut self, entry: LogEntry<Cmd>) {
        self.persistent.append_entry(entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addr(id: u64) -> std::net::SocketAddr {
        format!("127.0.0.1:{}", 9000 + id).parse().unwrap()
    }

    fn test_config(id: u64, peer_ids: &[u64]) -> ClusterConfig {
        let members = std::iter::once(id)
            .chain(peer_ids.iter().copied())
            .map(|i| (NodeId::from(i), test_addr(i)))
            .collect();
        ClusterConfig::new(members).unwrap()
    }

    fn node(id: u64, peers: &[u64]) -> Node<String> {
        Node::new(NodeId::from(id), test_config(id, peers))
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
                } => Some(r.vote == Vote::Granted),
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
            vote: Vote::Granted,
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
            vote: Vote::Granted,
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
        n.force_term(Term::from(1));
        n.force_vote(NodeId::from(2));

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
        n.push_entry(LogEntry {
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

        assert!(
            cmds.iter()
                .any(|c| matches!(c, Command::ResetElectionTimer))
        );
    }

    #[test]
    fn append_entries_rejects_stale_term() {
        let mut n = node(1, &[2, 3]);
        n.force_term(Term::from(5));

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
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.push_entry(LogEntry {
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
        let entry = n.persistent.log.entry(LogIndex::from(2)).unwrap();
        assert_eq!(
            entry.payload,
            LogPayload::Command("SET status=active".to_string())
        );
        assert_eq!(entry.term, Term::from(2));
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
                vote: Vote::Granted,
            },
        );
        assert!(is_leader(&n));

        // Submit a command (no-op is at index 1, command at index 2).
        let index = n.submit_command("SET counter=1".to_string());
        assert_eq!(index, Ok(LogIndex::from(2)));

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
                vote: Vote::Granted,
            },
        );
        assert!(is_leader(&n));

        // Add entries to leader's log.
        n.submit_command("SET name=miles".to_string()).unwrap();
        n.submit_command("SET counter=1".to_string()).unwrap();

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
        assert!(n.submit_command("SET counter=1".to_string()).is_err());

        n.election_timeout();
        assert!(n.submit_command("SET counter=1".to_string()).is_err());
    }

    /// A follower that has heard from a leader must surface it as a redirect hint
    /// so the client can retry there instead of guessing.
    #[test]
    fn submit_command_on_follower_returns_known_leader_hint() {
        let mut n = node(1, &[2, 3]);
        let req = AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);
        assert!(is_follower(&n));

        let result = n.submit_command("SET counter=1".to_string());
        assert_eq!(
            result,
            Err(NotLeaderError {
                leader_hint: Some(NodeId::from(2))
            })
        );
    }

    /// A candidate has no leader to redirect to this term.
    #[test]
    fn submit_command_on_candidate_returns_no_leader_hint() {
        let mut n = node(1, &[2, 3, 4, 5]);
        n.election_timeout();
        assert!(is_candidate(&n));

        let result = n.submit_command("SET counter=1".to_string());
        assert_eq!(result, Err(NotLeaderError { leader_hint: None }));
    }

    #[test]
    fn leader_does_not_commit_entries_from_previous_term() {
        use crate::core::state::Leader as LeaderState;

        let mut n = node(1, &[2, 3]);
        n.force_term(Term::from(2));
        n.force_vote(NodeId::from(1));
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.role = Role::Leader(LeaderState::new(&n.peer_ids(), LogIndex::from(1)));

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

        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET counter=1".to_string()),
        });

        n.volatile.commit_index = LogIndex::from(1);

        assert!(n.has_pending_applies());
        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(1));
        assert_eq!(applied.command, &"SET name=miles".to_string());
        assert!(!n.has_pending_applies());
        assert!(n.take_entry_to_apply().is_none());

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

        n.push_entry(LogEntry {
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
                vote: Vote::Granted,
            },
        );

        assert!(is_leader(&n));
        assert_eq!(n.persistent.log.len(), 1);
        let entry = n.persistent.log.entry(LogIndex::from(1)).unwrap();
        assert!(matches!(entry.payload, LogPayload::NoOp));
        assert_eq!(entry.term, Term::from(1));
    }

    #[test]
    fn take_entry_to_apply_skips_noop() {
        let mut n = node(1, &[2, 3]);

        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::NoOp,
        });
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET price=100".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(2);

        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(2));
        assert_eq!(applied.command, &"SET price=100".to_string());
        assert_eq!(n.volatile.last_applied, LogIndex::from(2));
        assert!(n.take_entry_to_apply().is_none());
    }

    #[test]
    fn candidate_steps_down_on_same_term_append_entries() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        assert!(is_candidate(&n));
        assert_eq!(n.persistent.voted_for, Some(NodeId::from(1)));

        let req = AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::default(),
            prev_log_term: Term::default(),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        n.handle_append_entries(NodeId::from(2), req);

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
                vote: Vote::Granted,
            },
        );
        n.submit_command("SET name=miles".to_string()).unwrap();
        n.submit_command("SET counter=1".to_string()).unwrap();

        let mut storage = MemoryStorage::new();
        n.save(&mut storage).unwrap();

        let restored: Node<String> =
            Node::from_storage(NodeId::from(1), test_config(1, &[2, 3]), &storage).unwrap();

        assert_eq!(
            restored.persistent.current_term(),
            n.persistent.current_term()
        );
        assert_eq!(restored.persistent.voted_for(), n.persistent.voted_for());
        assert_eq!(restored.persistent.log().len(), n.persistent.log().len());
        assert!(is_follower(&restored));
    }

    /// A same-length conflict overwrite must be durable. Length-only
    /// comparison in `save()` would see stored_len == current_len and write nothing,
    /// silently leaving the deposed entry `B` on disk instead of the new entry `C`.
    #[test]
    fn save_persists_same_length_conflict_overwrite() {
        use crate::storage::MemoryStorage;

        let mut storage: MemoryStorage<String> = MemoryStorage::new();
        let mut persistent: PersistentState<String> = PersistentState::new();
        persistent.set_term(Term::from(1));
        persistent.set_voted_for(Some(NodeId::from(1)));
        persistent.append_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        persistent.append_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        });
        persistent.save(&mut storage).unwrap();

        // New leader (term 2) overwrites entry 2 in place — log stays the same length.
        persistent.set_term(Term::from(2));
        persistent.merge_entries(
            LogIndex::from(1),
            vec![LogEntry {
                term: Term::from(2),
                payload: LogPayload::Command("SET status=active".to_string()),
            }],
        );
        persistent.save(&mut storage).unwrap();

        let reloaded: PersistentState<String> = PersistentState::load(&storage).unwrap();
        assert_eq!(
            reloaded.log, persistent.log,
            "disk log must reflect the conflict overwrite"
        );
    }

    /// Growing case: conflict resolution both truncates and appends new
    /// entries beyond the old stored length. Length-only append-loop would skip the
    /// overwritten entry, producing a frankenstein log mixing old and new entries.
    #[test]
    fn save_persists_conflict_overwrite_that_also_grows_the_log() {
        use crate::storage::MemoryStorage;

        let mut storage: MemoryStorage<String> = MemoryStorage::new();
        let mut persistent: PersistentState<String> = PersistentState::new();
        persistent.set_term(Term::from(1));
        persistent.set_voted_for(Some(NodeId::from(1)));
        persistent.append_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        persistent.append_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        });
        persistent.save(&mut storage).unwrap();

        persistent.set_term(Term::from(2));
        persistent.merge_entries(
            LogIndex::from(1),
            vec![
                LogEntry {
                    term: Term::from(2),
                    payload: LogPayload::Command("SET status=active".to_string()),
                },
                LogEntry {
                    term: Term::from(2),
                    payload: LogPayload::Command("SET region=eu-west-1".to_string()),
                },
            ],
        );
        persistent.save(&mut storage).unwrap();

        let reloaded: PersistentState<String> = PersistentState::load(&storage).unwrap();
        assert_eq!(reloaded.log, persistent.log);
    }

    #[test]
    fn stale_vote_response_is_ignored() {
        let mut n = node(1, &[2, 3]);
        n.force_term(Term::from(3));

        let cmds = n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );

        assert!(cmds.is_empty());
        assert!(is_follower(&n));
    }

    #[test]
    fn vote_response_is_ignored_when_not_candidate() {
        let mut n = node(1, &[2, 3]);
        n.force_term(Term::from(1));

        let cmds = n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );

        assert!(cmds.is_empty());
        assert!(is_follower(&n));
    }

    /// A vote from a node outside the current config (e.g. a removed
    /// member, or a spoofed envelope — transport has no authentication) must not count
    /// toward the majority `has_majority` computes over `config` size.
    #[test]
    fn vote_from_non_member_does_not_count_toward_majority() {
        let mut n = node(1, &[2, 3]); // config = {1, 2, 3}, majority = 2, self-vote = 1
        n.election_timeout();
        assert!(is_candidate(&n));

        let cmds = n.handle_request_vote_response(
            NodeId::from(99),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );

        assert!(
            is_candidate(&n),
            "a vote from a non-member must not be enough to win the election"
        );
        assert!(cmds.is_empty());
    }

    #[test]
    fn append_entries_rejects_on_prev_term_mismatch() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET counter=1".to_string()),
        });

        let req = AppendEntries {
            term: Term::from(2),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::from(1),
            prev_log_term: Term::from(2),
            entries: vec![],
            leader_commit: LogIndex::default(),
        };
        let cmds = n.handle_append_entries(NodeId::from(2), req);

        assert!(!extract_append_success(&cmds));
    }

    #[test]
    fn follower_advances_commit_index_from_leader_commit() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
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

    #[test]
    fn propose_config_change_fails_on_non_leader() {
        let mut n = node(1, &[2, 3]);
        let new_config = test_config(1, &[2, 3, 4]);
        assert!(matches!(
            n.propose_config_change(new_config),
            Err(SubmitError::NotLeader { .. })
        ));
    }

    #[test]
    fn propose_config_change_appends_entry_and_updates_peers() {
        let mut n = node(1, &[2, 3]);
        // Become leader.
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );
        assert!(is_leader(&n));

        let new_config = test_config(1, &[2, 3, 4]);
        let index = n.propose_config_change(new_config);
        assert!(index.is_ok());

        // Peers now include node 4.
        assert!(n.peer_ids().contains(&NodeId::from(4)));
        // A ConfigChange entry was appended after the no-op.
        assert!(matches!(
            n.persistent.log.last().unwrap().payload,
            LogPayload::ConfigChange(_)
        ));
    }

    #[test]
    fn second_config_change_rejected_while_first_uncommitted() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );

        let config_a = test_config(1, &[2, 3, 4]);
        let config_b = test_config(1, &[2, 3, 4, 5]);

        assert!(n.propose_config_change(config_a).is_ok());
        assert_eq!(
            n.propose_config_change(config_b),
            Err(SubmitError::ConfigChangePending),
            "second change must be rejected as pending, not conflated with not-leader"
        );
    }

    #[test]
    fn take_entry_to_apply_skips_config_change_and_buffers_committed() {
        let mut n = node(1, &[2, 3]);
        let new_config = test_config(1, &[2, 3, 4]);

        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(new_config.clone()),
        });
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET city=amsterdam".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(2);

        // First call must skip the ConfigChange and return the Command.
        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(2));
        assert_eq!(applied.command, &"SET city=amsterdam".to_string());

        // The committed config change must be buffered.
        let committed = n.take_committed_config_changes();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0, LogIndex::from(1));
        assert_eq!(committed[0].1, new_config);
    }

    fn make_leader(id: u64, peers: &[u64]) -> Node<String> {
        let mut n = node(id, peers);
        n.election_timeout();
        for &peer in peers {
            n.handle_request_vote_response(
                NodeId::from(peer),
                RequestVoteResponse {
                    term: Term::from(1),
                    vote: Vote::Granted,
                },
            );
        }
        n
    }

    fn test_snapshot(last_index: u64, last_term: u64, config: ClusterConfig) -> Snapshot {
        Snapshot {
            meta: SnapshotMeta {
                last_index: LogIndex::from(last_index),
                last_term: Term::from(last_term),
                config,
            },
            data: SnapshotData::new(vec![1, 2, 3]),
        }
    }

    #[test]
    fn compact_to_snapshot_errors_when_nothing_new_to_compact() {
        let mut n = node(1, &[2, 3]);

        let result = n.compact_to_snapshot(SnapshotData::new(vec![1]));

        assert_eq!(result.unwrap_err(), CompactError::NothingToCompact);
    }

    #[test]
    fn compact_to_snapshot_compacts_log_through_last_applied() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(2);
        n.volatile.last_applied = LogIndex::from(2);

        let snapshot = n.compact_to_snapshot(SnapshotData::new(vec![42])).unwrap();

        assert_eq!(snapshot.meta.last_index, LogIndex::from(2));
        assert_eq!(snapshot.meta.last_term, Term::from(1));
        assert_eq!(
            snapshot.meta.config, n.config,
            "no later ConfigChange exists, so the current active config is used directly"
        );
        assert_eq!(n.persistent.log.snapshot_last_index(), LogIndex::from(2));
        assert!(n.persistent.log.is_empty());
        assert_eq!(n.persistent.latest_snapshot, Some(snapshot));
    }

    #[test]
    fn compact_to_snapshot_picks_config_change_at_or_before_boundary_not_a_later_one() {
        let mut n = node(1, &[2, 3]);
        let earlier_config = test_config(1, &[2, 3]);
        let later_config = test_config(1, &[2, 3, 4]);

        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(earlier_config.clone()),
        }); // index 1
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        }); // index 2, the compaction boundary
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(later_config),
        }); // index 3, beyond the boundary — must not be picked

        n.volatile.commit_index = LogIndex::from(2);
        n.volatile.last_applied = LogIndex::from(2);

        let snapshot = n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();

        assert_eq!(snapshot.meta.config, earlier_config);
    }

    #[test]
    fn compact_to_snapshot_falls_back_to_previous_snapshot_config_when_retained_prefix_has_none() {
        let mut n = node(1, &[2, 3]);
        let grown_config = test_config(1, &[2, 3, 4]);

        // C1 compacted into a first snapshot — the retained log no longer holds it.
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(grown_config.clone()),
        }); // index 1
        n.set_config(grown_config.clone()); // config changes take effect on append, §4.1
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        }); // index 2, first compaction boundary
        n.volatile.commit_index = LogIndex::from(2);
        n.volatile.last_applied = LogIndex::from(2);
        let first_snapshot = n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();
        assert_eq!(first_snapshot.meta.config, grown_config);

        // C2 appended but not yet applied when the second compaction fires — the
        // retained prefix (just index 3) carries no ConfigChange at all.
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        }); // index 3, second compaction boundary
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(test_config(1, &[2, 3, 4, 5])),
        }); // index 4, beyond the boundary — must not be picked
        n.volatile.commit_index = LogIndex::from(3);
        n.volatile.last_applied = LogIndex::from(3);

        let second_snapshot = n.compact_to_snapshot(SnapshotData::new(vec![2])).unwrap();

        assert_eq!(
            second_snapshot.meta.config, grown_config,
            "must fall back to the previous snapshot's config, not initial_config, \
             when the only ConfigChange in the retained prefix was already compacted away"
        );
    }

    #[test]
    fn leader_sends_install_snapshot_to_lagging_peer_and_append_entries_to_caught_up_peer() {
        let mut n = make_leader(1, &[2, 3]);
        n.submit_command("SET name=miles".to_string()).unwrap(); // index 2
        n.submit_command("SET status=pending".to_string()).unwrap(); // index 3

        // Peer 3 catches up to index 3; peer 2 is left behind at its initial next_index.
        n.handle_append_entries_response(
            NodeId::from(3),
            AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(3),
            },
        );

        n.volatile.commit_index = LogIndex::from(3);
        n.volatile.last_applied = LogIndex::from(3);
        n.compact_to_snapshot(SnapshotData::new(vec![7])).unwrap();

        let commands = n.heartbeat_timeout();

        let sent_to_2 = commands
            .iter()
            .find_map(|c| match c {
                Command::Send { to, message } if *to == NodeId::from(2) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(
            matches!(sent_to_2, Message::InstallSnapshot(_)),
            "peer behind the compacted boundary must receive InstallSnapshot, got {sent_to_2:?}"
        );

        let sent_to_3 = commands
            .iter()
            .find_map(|c| match c {
                Command::Send { to, message } if *to == NodeId::from(3) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(
            matches!(sent_to_3, Message::AppendEntries(_)),
            "caught-up peer must keep receiving AppendEntries, got {sent_to_3:?}"
        );
    }

    #[test]
    fn install_snapshot_response_advances_match_index_and_can_advance_commit_index() {
        let mut n = make_leader(1, &[2, 3]);
        n.submit_command("SET name=miles".to_string()).unwrap(); // index 2

        n.handle_install_snapshot_response(
            NodeId::from(2),
            InstallSnapshotResponse::Installed {
                term: Term::from(1),
                last_index: LogIndex::from(2),
            },
        );

        let Role::Leader(leader) = n.role() else {
            panic!("expected leader");
        };
        assert_eq!(
            leader.next_index_for(NodeId::from(2)),
            Some(LogIndex::from(3))
        );
        assert_eq!(n.volatile.commit_index, LogIndex::from(2));
    }

    #[test]
    fn install_snapshot_response_with_higher_term_steps_leader_down() {
        let mut n = make_leader(1, &[2, 3]);

        let cmds = n.handle_install_snapshot_response(
            NodeId::from(2),
            InstallSnapshotResponse::Rejected {
                term: Term::from(99),
            },
        );

        assert!(is_follower(&n));
        assert_eq!(n.persistent.current_term, Term::from(99));
        assert!(
            cmds.iter()
                .any(|c| matches!(c, Command::ResetElectionTimer))
        );
    }

    #[test]
    fn follower_rejects_stale_term_install_snapshot() {
        let mut n = node(1, &[2, 3]);
        n.force_term(Term::from(5));

        let req = InstallSnapshot {
            term: Term::from(3),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(1, 1, test_config(1, &[2, 3])),
        };
        let cmds = n.handle_install_snapshot(NodeId::from(2), req);

        let resp = cmds
            .iter()
            .find_map(|c| match c {
                Command::Send {
                    message: Message::InstallSnapshotResponse(r),
                    ..
                } => Some(r),
                _ => None,
            })
            .unwrap();
        assert!(
            matches!(resp, InstallSnapshotResponse::Rejected { term } if *term == Term::from(5))
        );
    }

    #[test]
    fn candidate_steps_down_on_in_term_install_snapshot() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        assert!(is_candidate(&n));

        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(1, 1, test_config(1, &[2, 3])),
        };
        n.handle_install_snapshot(NodeId::from(2), req);

        assert!(is_follower(&n));
    }

    #[test]
    fn follower_ignores_snapshot_at_or_below_commit_index() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(1);

        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(1, 1, test_config(1, &[2, 3])),
        };
        let cmds = n.handle_install_snapshot(NodeId::from(2), req);

        assert_eq!(
            n.persistent.log.len(),
            1,
            "already-committed state untouched"
        );
        assert!(n.pending_snapshot_restore.is_none());

        let resp = cmds
            .iter()
            .find_map(|c| match c {
                Command::Send {
                    message: Message::InstallSnapshotResponse(r),
                    ..
                } => Some(r),
                _ => None,
            })
            .unwrap();
        assert!(
            matches!(resp, InstallSnapshotResponse::Installed { last_index, .. } if *last_index == LogIndex::from(1))
        );
    }

    #[test]
    fn follower_retains_suffix_when_boundary_entry_matches() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        }); // index 1
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        }); // index 2
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET region=eu-west-1".to_string()),
        }); // index 3

        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(2, 1, test_config(1, &[2, 3])), // term matches our entry at index 2
        };
        n.handle_install_snapshot(NodeId::from(2), req);

        assert_eq!(n.persistent.log.snapshot_last_index(), LogIndex::from(2));
        assert_eq!(n.persistent.log.len(), 1);
        assert_eq!(
            n.persistent.log.entry(LogIndex::from(3)).unwrap().payload,
            LogPayload::Command("SET region=eu-west-1".to_string())
        );
        assert_eq!(n.volatile.last_applied, LogIndex::from(2));
        assert!(n.pending_snapshot_restore.is_some());
    }

    #[test]
    fn follower_discards_conflicting_log_wholesale_on_install_snapshot() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        }); // index 1
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        }); // index 2, term 1 — conflicts with the snapshot's claimed term 9

        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(2, 9, test_config(1, &[2, 3])),
        };
        n.handle_install_snapshot(NodeId::from(2), req);

        assert!(n.persistent.log.is_empty());
        assert_eq!(n.persistent.log.snapshot_last_index(), LogIndex::from(2));
        assert_eq!(n.persistent.log.snapshot_last_term(), Term::from(9));
    }

    #[test]
    fn follower_adopts_snapshot_config_on_install_snapshot() {
        let mut n = node(1, &[2, 3]);
        let grown_config = test_config(1, &[2, 3, 4]);

        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(1, 1, grown_config.clone()),
        };
        n.handle_install_snapshot(NodeId::from(2), req);

        assert_eq!(n.config, grown_config);
    }

    #[test]
    fn take_snapshot_to_restore_drains_pending_snapshot() {
        let mut n = node(1, &[2, 3]);
        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(1, 1, test_config(1, &[2, 3])),
        };
        n.handle_install_snapshot(NodeId::from(2), req);

        assert!(n.take_snapshot_to_restore().is_some());
        assert!(n.take_snapshot_to_restore().is_none());
    }

    #[test]
    fn save_persists_snapshot_install_before_appends() {
        use crate::storage::LoadedState;
        use crate::storage::MemoryStorage;

        #[derive(Default)]
        struct RecordingStorage {
            calls: Vec<&'static str>,
            inner: MemoryStorage<String>,
        }

        impl Storage<String> for RecordingStorage {
            type Error = std::convert::Infallible;

            fn load(&self) -> Result<LoadedState<String>, Self::Error> {
                self.inner.load()
            }

            fn set_meta(
                &mut self,
                term: Term,
                voted_for: Option<NodeId>,
            ) -> Result<(), Self::Error> {
                self.calls.push("set_meta");
                self.inner.set_meta(term, voted_for)
            }

            fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error> {
                self.calls.push("truncate_from");
                self.inner.truncate_from(index)
            }

            fn append(&mut self, entries: &[LogEntry<String>]) -> Result<(), Self::Error> {
                self.calls.push("append");
                self.inner.append(entries)
            }

            fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
                self.calls.push("install_snapshot");
                self.inner.install_snapshot(snapshot)
            }
        }

        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(1);
        n.volatile.last_applied = LogIndex::from(1);
        n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();
        // A new entry appended after compaction means save() has both a pending
        // snapshot install and a pending append to persist in the same call.
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        });

        let mut storage = RecordingStorage::default();
        n.save(&mut storage).unwrap();

        let install_pos = storage
            .calls
            .iter()
            .position(|&c| c == "install_snapshot")
            .expect("install_snapshot must be called");
        let append_pos = storage
            .calls
            .iter()
            .position(|&c| c == "append")
            .expect("append must be called");
        assert!(
            install_pos < append_pos,
            "snapshot must be installed before the new suffix is appended: {:?}",
            storage.calls
        );
    }

    #[test]
    fn election_safety_after_full_compaction_uses_snapshot_boundary_for_vote_comparison() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(3),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.volatile.commit_index = LogIndex::from(1);
        n.volatile.last_applied = LogIndex::from(1);
        n.compact_to_snapshot(SnapshotData::new(vec![9])).unwrap();

        assert!(n.persistent.log.is_empty());
        assert_eq!(n.persistent.log.snapshot_last_index(), LogIndex::from(1));
        assert_eq!(n.persistent.log.snapshot_last_term(), Term::from(3));

        // A candidate whose log ends at a lower term than our snapshot boundary
        // must be denied, even though our in-memory `entries` vec is now empty.
        let stale_log_request = RequestVote {
            term: Term::from(4),
            candidate_id: NodeId::from(2),
            last_log_index: LogIndex::from(1),
            last_log_term: Term::from(2),
        };
        let cmds = n.handle_request_vote(NodeId::from(2), stale_log_request);
        assert!(!extract_vote_granted(&cmds));

        // A candidate at least as up to date as the snapshot boundary must be granted.
        let caught_up_request = RequestVote {
            term: Term::from(5),
            candidate_id: NodeId::from(3),
            last_log_index: LogIndex::from(1),
            last_log_term: Term::from(3),
        };
        let cmds = n.handle_request_vote(NodeId::from(3), caught_up_request);
        assert!(extract_vote_granted(&cmds));
    }
}
