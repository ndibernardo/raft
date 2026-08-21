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
use crate::core::types::SuffixDisposition;
use crate::core::types::Term;
use crate::core::types::TermLookup;
use crate::core::types::Vote;
use crate::storage::Storage;

/// Why `Node::submit_command` refused a client command: this node is not the leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("not the leader")]
pub struct NotLeaderError {
    /// The leader this node last heard from, so the client can retry there
    /// instead of polling the cluster at random.
    pub leader_hint: Option<NodeId>,
}

/// Why `Node::propose_config_change` refused a membership change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    #[error("not the leader")]
    NotLeader { leader_hint: Option<NodeId> },
    /// An earlier `ConfigChange` is still uncommitted. The single-server-change
    /// rule of dissertation section 4.1 permits only one in flight, because
    /// overlapping changes can produce two disjoint majorities.
    #[error("a config change is already pending")]
    ConfigChangePending,
}

/// Why `Node::compact_to_snapshot` refused to compact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompactError {
    /// `last_applied` is at or below the current snapshot boundary, so nothing
    /// has been applied since the last compaction.
    #[error("nothing to compact: last_applied is at or below the snapshot boundary")]
    NothingToCompact,
}

/// A snapshot install recorded in memory and awaiting replay to storage.
///
/// The disposition travels with the snapshot because storage cannot re-derive
/// it: by the time it runs, the local entry whose term decided the question is
/// already inside the compacted prefix.
#[derive(Clone, Debug)]
struct PendingSnapshotInstall {
    snapshot: Snapshot,
    disposition: SuffixDisposition,
}

/// The state Figure 2 requires on stable storage: `current_term`, `voted_for`,
/// and the log. All of it must reach durable storage before this node responds
/// to any RPC, since a response is a promise the node must still honour after a
/// crash.
///
/// `Node` is the single owner of the log, and `save` never reconstructs a diff
/// by comparing against storage. A length comparison in particular would miss a
/// conflict overwrite that replaces entries without changing the log length.
/// Instead every mutation records exactly what it did (`set_term` and
/// `set_voted_for` mark the metadata dirty; `append_entry` and `merge_entries`
/// record the truncation point and the appended suffix), and `save` replays that
/// record to storage and clears it.
#[derive(Debug)]
pub struct PersistentState<Cmd> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log<Cmd>,
    /// Set when the term or vote changed and storage has not seen it yet.
    meta_dirty: bool,
    /// Index a conflict truncated the log at, pending replay to storage.
    truncated_from: Option<LogIndex>,
    /// Entries appended since the last `save`, in index order.
    pending_append: Vec<LogEntry<Cmd>>,
    /// Snapshot recorded by `compact_to_snapshot` or `handle_install_snapshot`,
    /// replayed by `save` as `storage.install_snapshot` and then cleared. Same
    /// record-then-replay pattern as `truncated_from` and `pending_append`.
    pending_snapshot_install: Option<PendingSnapshotInstall>,
    /// The most recent snapshot, retained for the leader send path that builds
    /// `InstallSnapshot` RPCs. Unlike `pending_snapshot_install` it lives for the
    /// node's lifetime rather than until the next `save`.
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

    /// Highest term this node has seen.
    pub fn current_term(&self) -> Term {
        self.current_term
    }

    /// Candidate this node voted for in `current_term`, if any.
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// The replicated log.
    pub fn log(&self) -> &Log<Cmd> {
        &self.log
    }

    /// The most recent snapshot, or `None` if the log was never compacted and no
    /// leader ever installed one.
    pub fn latest_snapshot(&self) -> Option<&Snapshot> {
        self.latest_snapshot.as_ref()
    }

    /// Records a new term, marking the metadata dirty only on an actual change
    /// so an unchanged term does not force a needless durable write.
    fn set_term(&mut self, term: Term) {
        if term != self.current_term {
            self.current_term = term;
            self.meta_dirty = true;
        }
    }

    /// Records a vote, marking the metadata dirty only on an actual change.
    fn set_voted_for(&mut self, candidate: Option<NodeId>) {
        if candidate != self.voted_for {
            self.voted_for = candidate;
            self.meta_dirty = true;
        }
    }

    /// Leader-side append of one entry to the tail. Returns its assigned index.
    fn append_entry(&mut self, entry: LogEntry<Cmd>) -> LogIndex {
        let index = self.log.append(entry.clone());
        self.pending_append.push(entry);
        index
    }

    /// Follower-side conflict resolution (section 5.3). Delegates to
    /// `Log::merge` and records the resulting truncation point and suffix, so
    /// `save` can hand storage the identical change.
    fn merge_entries(&mut self, prev_index: LogIndex, entries: Vec<LogEntry<Cmd>>) -> MergeOutcome {
        let outcome = self.log.merge(prev_index, entries);

        match outcome {
            MergeOutcome::Unchanged => {}
            MergeOutcome::Truncated { from } => {
                self.truncated_from = Some(from);
                self.pending_append = self.log.suffix_from(from).to_vec();
            }
            // The merge names the absolute index it started writing at. Deriving
            // it from the retained length instead would name an index still held
            // by an older entry once compaction has moved the log's start, and
            // storage would receive that entry a second time.
            MergeOutcome::Appended { from } => {
                self.pending_append
                    .extend(self.log.suffix_from(from).iter().cloned());
            }
        }

        outcome
    }

    /// Reads term, vote, snapshot, and log back from durable storage.
    ///
    /// # Errors
    /// Whatever the storage backend reports.
    pub fn load<S: Storage<Cmd>>(storage: &S) -> Result<Self, S::Error> {
        let loaded = storage.load()?;
        // A persisted snapshot fixes the log's index offset. `from_entries`
        // would place the surviving suffix at index 1 instead, shifting every
        // index in the log by the size of the compacted prefix.
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

    /// Replays every recorded change to storage and clears the record.
    ///
    /// The order is fixed: metadata, then snapshot install, then truncation,
    /// then append. A truncation applied before its snapshot, or an append
    /// before the truncation that makes room for it, would leave storage
    /// holding entries the in-memory log has already discarded.
    ///
    /// # Errors
    /// Whatever the storage backend reports. The uncompleted part of the record
    /// is retained, so a later `save` can retry it. Each record is cleared only
    /// after the call that consumed it returned success, since a record dropped
    /// ahead of the write it describes can never be replayed.
    pub fn save<S: Storage<Cmd>>(&mut self, storage: &mut S) -> Result<(), S::Error> {
        if self.meta_dirty {
            storage.set_meta(self.current_term, self.voted_for)?;
            self.meta_dirty = false;
        }
        if let Some(pending) = &self.pending_snapshot_install {
            storage.install_snapshot(&pending.snapshot, pending.disposition)?;
            self.pending_snapshot_install = None;
        }
        if let Some(from) = self.truncated_from {
            storage.truncate_from(from)?;
            self.truncated_from = None;
        }
        if !self.pending_append.is_empty() {
            storage.append(&self.pending_append)?;
            self.pending_append.clear();
        }
        Ok(())
    }

    /// Applies `snapshot` to the in-memory log and records it, with the
    /// disposition, for `save` to replay as a single storage call.
    ///
    /// A `Discard` supersedes every earlier log mutation still pending, so the
    /// recorded truncation and suffix go with it. Replaying them after the
    /// install would put entries back into storage that this node has just
    /// declared invalid.
    fn install_snapshot(&mut self, snapshot: Snapshot, disposition: SuffixDisposition) {
        let last_index = snapshot.meta.last_index;
        let last_term = snapshot.meta.last_term;
        match disposition {
            SuffixDisposition::Retain => self.log.compact_through(last_index, last_term),
            SuffixDisposition::Discard => {
                self.log.reset_to_snapshot(last_index, last_term);
                self.truncated_from = None;
                self.pending_append.clear();
            }
        }
        self.latest_snapshot = Some(snapshot.clone());
        self.pending_snapshot_install = Some(PendingSnapshotInstall {
            snapshot,
            disposition,
        });
    }
}

/// The volatile state of Figure 2. Not persisted: both fields restart at the
/// snapshot boundary, or at zero when there is no snapshot, and are rebuilt by
/// replaying the log.
#[derive(Debug)]
pub struct VolatileState {
    /// Highest index known to be committed, meaning replicated on a majority.
    pub commit_index: LogIndex,
    /// Highest index handed to the state machine.
    pub last_applied: LogIndex,
}

/// The role a node currently occupies, holding the state specific to it.
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
    /// Term of the entry that actually committed at `index`, which is not
    /// necessarily the term the submitting client observed. Callers key pending
    /// client responses on the pair of term and index, so an unrelated entry
    /// written at the same index by a later leader cannot be mistaken for the
    /// original submission.
    pub term: Term,
    pub command: &'a Cmd,
}

/// A single Raft server, implemented as a pure state machine.
///
/// Every entry point takes an event (a timeout, an incoming message, a client
/// submission), mutates this node's state, and returns the `Command`s the driver
/// must carry out. Nothing here performs I/O or reads a clock, which is what
/// makes the whole protocol deterministically testable.
#[derive(Debug)]
pub struct Node<Cmd> {
    id: NodeId,
    /// Cluster configuration currently in force, taken from the latest
    /// `ConfigChange` in the log, or from `initial_config` when there is none.
    config: ClusterConfig,
    /// Configuration this node booted with. Serves as the fallback when a
    /// rescan finds no `ConfigChange` in the log and no snapshot to fall back
    /// to, which happens after a truncation removes the only one there was.
    initial_config: ClusterConfig,
    persistent: PersistentState<Cmd>,
    volatile: VolatileState,
    role: Role,
    /// Configurations that took effect on append, before commit. The runtime
    /// drains these and passes them to the transport, so RPCs can reach a newly
    /// added peer immediately.
    pending_config_changes: Vec<ClusterConfig>,
    /// Configurations that have just committed. The server drains these to
    /// resolve the client requests that proposed them.
    pending_committed_config_changes: Vec<(LogIndex, ClusterConfig)>,
    /// A snapshot accepted by `handle_install_snapshot` and awaiting the
    /// runtime's call to `StateMachine::restore`. Drained by
    /// `take_snapshot_to_restore`.
    pending_snapshot_restore: Option<Snapshot>,
}

impl<Cmd: Clone> Node<Cmd> {
    /// A fresh node at term 0, starting as a follower with no known leader, as
    /// section 5.1 requires.
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

    /// Restores a node after a crash: term, vote, and log come from durable
    /// storage, and the role restarts as follower. The active configuration is
    /// derived from the restored log rather than from `initial_config`.
    ///
    /// # Errors
    /// Whatever the storage backend reports while loading.
    pub fn from_storage<S: Storage<Cmd>>(
        id: NodeId,
        initial_config: ClusterConfig,
        storage: &S,
    ) -> Result<Self, S::Error> {
        let persistent = PersistentState::load(storage)?;
        // A restored snapshot already covers everything up to its boundary. From
        // zero, take_entry_to_apply would replay entries the restored state
        // machine already reflects, applying them a second time.
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
        // The restored log may carry a membership this node was unaware of when
        // it booted, so the peer set has to be rebuilt from it.
        node.apply_latest_config_from_log();
        Ok(node)
    }

    /// Flushes pending state to durable storage. The driver must call this
    /// before it sends any of the returned commands, per the durability
    /// requirement of section 5.1.
    ///
    /// # Errors
    /// Whatever the storage backend reports.
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

    /// Cluster configuration currently in force.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Read-only view of term, vote, and log. Mutation is internal to `Node`.
    pub fn persistent(&self) -> &PersistentState<Cmd> {
        &self.persistent
    }

    /// Read-only view of the commit and apply positions. Mutation is internal
    /// to `Node`.
    pub fn volatile(&self) -> &VolatileState {
        &self.volatile
    }

    /// Every member except this node. Derived from `config` on each call rather
    /// than cached, so it cannot drift out of step with a membership change.
    fn peer_ids(&self) -> Vec<NodeId> {
        self.config.peer_ids(self.id)
    }

    fn last_log_index(&self) -> LogIndex {
        self.persistent.log.last_index()
    }

    /// Steps down to follower in `term`, optionally recording the leader.
    ///
    /// The vote is cleared only when moving to a strictly newer term. Stepping
    /// down within the current term, as a candidate does when it hears from the
    /// leader of that term, has to keep the existing vote: this node already
    /// voted for itself, and forgetting that would let it vote a second time in
    /// the same term.
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

    /// Adopts `config` as the active membership.
    ///
    /// Adds and removes peers in the leader's replication maps to match, and
    /// queues the configuration for the transport so a newly added peer is
    /// reachable before the next heartbeat goes out.
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

        // The quorum is computed from the tracked peers, so removing one lowers
        // the bar. An index that the smaller membership already replicated is
        // committed the moment the change takes effect, without waiting for the
        // next response. Does nothing unless this node is the leader.
        self.advance_commit_index();
    }

    /// Scans the log backward for the newest `ConfigChange` and adopts it.
    ///
    /// Called after a truncation, which may have removed the entry that
    /// established the active configuration, and at startup to recover the
    /// configuration from a restored log.
    ///
    /// The fallback order is the log, then the snapshot's configuration, then
    /// `initial_config`. The snapshot has to come before `initial_config`
    /// because compaction may have discarded the only `ConfigChange` entry that
    /// ever existed, and reverting to the boot configuration would resurrect a
    /// membership the cluster has already moved past.
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

    /// Handles the election timeout firing. Followers and candidates start a new
    /// election; a leader has nothing to do, since it is the reason the timeout
    /// should not have fired elsewhere.
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

        // In a single-node cluster the candidate's own vote is already a
        // majority, and there is no peer to wait for.
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

    /// Receiver implementation of the RequestVote RPC (Figure 2).
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

    /// Whether to grant the vote. All three conditions of the RequestVote
    /// receiver in Figure 2 must hold: the candidate's term is not stale, this
    /// node has not already voted for a different candidate in that term, and
    /// the candidate's log is at least as up to date as this one (section
    /// 5.4.1).
    fn should_grant_vote(&self, req: &RequestVote) -> bool {
        let term_ok = req.term >= self.persistent.current_term;
        let vote_ok = match self.persistent.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };
        let log_ok = self.is_log_up_to_date(req.last_log_term, req.last_log_index);

        term_ok && vote_ok && log_ok
    }

    /// The up-to-date comparison of section 5.4.1: the later last-entry term
    /// wins, and the longer log breaks a tie on equal terms.
    fn is_log_up_to_date(&self, candidate_term: Term, candidate_index: LogIndex) -> bool {
        (candidate_term, candidate_index) >= (self.last_log_term(), self.last_log_index())
    }

    /// Handles a RequestVote response: steps down on a higher term, and becomes
    /// leader once the granted votes form a majority.
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

        // A vote from a server outside the current configuration must not count.
        // The transport is unauthenticated, so a removed member, or anything
        // able to spoof one, could otherwise supply the deciding vote.
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

    /// Assumes leadership: appends a no-op entry, initializes replication state,
    /// and sends the first round of heartbeats.
    ///
    /// The no-op exists because of the Figure 8 anomaly (section 8). A leader
    /// may not commit an entry from an earlier term by counting replicas, so
    /// entries inherited from a predecessor would otherwise stay uncommitted
    /// until new traffic arrives. Committing the no-op, which belongs to the
    /// current term, commits everything before it through the Log Matching
    /// Property.
    fn become_leader(&mut self) -> Vec<Command<Cmd>> {
        self.persistent.append_entry(LogEntry {
            term: self.persistent.current_term,
            payload: LogPayload::NoOp,
        });
        self.role = Role::Leader(Leader::new(&self.peer_ids(), self.last_log_index()));
        info!(node = %self.id, term = %self.persistent.current_term, "became leader");
        // The local log is one of the quorum's votes, so an append can be a
        // majority on its own. In a single-member cluster it always is, and no
        // peer response will ever arrive to trigger the commit decision later.
        self.advance_commit_index();
        self.send_heartbeats()
    }

    /// Handles the heartbeat interval firing. Leaders replicate and restart the
    /// timer; every other role ignores it.
    pub fn heartbeat_timeout(&mut self) -> Vec<Command<Cmd>> {
        match &self.role {
            Role::Leader(_) => self.send_heartbeats(),
            _ => Vec::new(),
        }
    }

    /// Builds one replication message per tracked peer.
    ///
    /// Each peer gets an AppendEntries carrying everything from its `next_index`
    /// onward, prefixed with the index and term immediately before it so the
    /// follower can detect a gap before accepting anything (section 5.3). A peer
    /// whose `next_index` has already been compacted away gets an
    /// InstallSnapshot instead. With nothing to replicate, the AppendEntries is
    /// empty and serves as the heartbeat.
    fn send_heartbeats(&self) -> Vec<Command<Cmd>> {
        let Role::Leader(leader) = &self.role else {
            return Vec::new();
        };

        // Collected up front because the iterator borrows `leader`, and so
        // `self.role`, while the loop body needs `&self` for term_at and
        // entries_from.
        let peers: Vec<NodeId> = leader.tracked_peers().collect();
        let mut commands = Vec::new();

        for peer in peers {
            let next_index = leader
                .next_index_for(peer)
                .unwrap_or_else(|| self.last_log_index().next());

            // The entry preceding next_index has been compacted away, so no
            // AppendEntries could carry a prev_log_index this leader can still
            // prove. The peer needs the compacted prefix as a snapshot.
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

    /// Term at `index`, collapsing the unavailable cases to term 0.
    fn term_at(&self, index: LogIndex) -> Term {
        match self.persistent.log.term_at(index) {
            TermLookup::Known(term) => term,
            // send_heartbeats routes any peer whose next_index sits at or below
            // the snapshot boundary to InstallSnapshot, so a compacted index
            // cannot reach this point. Assert the invariant rather than trust it.
            TermLookup::Compacted => {
                debug_assert!(
                    false,
                    "term_at called with a compacted index; send_heartbeats must \
                     route this peer to InstallSnapshot instead"
                );
                Term::default()
            }
            // Reachable, for instance when probing one position past the
            // leader's own tail. Term 0 never matches a real entry, so the
            // follower correctly rejects the consistency check.
            TermLookup::BeyondEnd => Term::default(),
        }
    }

    fn entries_from(&self, start: LogIndex) -> Vec<LogEntry<Cmd>> {
        self.persistent.log.suffix_from(start).to_vec()
    }

    /// Receiver implementation of the AppendEntries RPC (Figure 2).
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

        // A candidate that hears from a leader of its own term concedes the
        // election (section 5.2). The vote survives the step-down, since this
        // node's vote for itself in this term remains valid.
        if matches!(self.role, Role::Candidate(_)) {
            self.become_follower(req.term, Some(req.leader_id));
        }

        // The request is from the current leader, so record it for redirection.
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

    /// Rule 2 of the AppendEntries receiver in Figure 2: whether this log holds
    /// `prev_log_term` at `prev_log_index` (section 5.3, Log Matching).
    fn check_log_consistency(&self, prev_log_index: LogIndex, prev_log_term: Term) -> bool {
        self.persistent.log.matches(prev_log_index, prev_log_term)
    }

    /// Handles an AppendEntries response. A leader advances or backs off the
    /// peer's replication state and recomputes the commit index; a stale term or
    /// any other role is ignored.
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

    /// The leader to redirect a misdirected client to.
    ///
    /// `None` on a leader, where this node is the answer, and on a candidate,
    /// which has not yet heard from a leader in its term.
    pub fn leader_hint(&self) -> Option<NodeId> {
        match &self.role {
            Role::Follower(follower) => follower.leader_id(),
            Role::Candidate(_) | Role::Leader(_) => None,
        }
    }

    /// Appends a client command to the log and returns the index it landed at.
    /// The entry is not yet committed; the caller learns that through
    /// `take_entry_to_apply`.
    ///
    /// # Errors
    /// `NotLeaderError` when this node is not the leader, carrying the leader
    /// hint to retry against.
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
        self.advance_commit_index();
        Ok(index)
    }

    /// Proposes a membership change and returns the index of the `ConfigChange`
    /// entry.
    ///
    /// The new configuration governs quorum decisions from the moment it is
    /// appended, before it commits (dissertation section 4.1).
    ///
    /// # Errors
    /// `SubmitError::NotLeader` when this node is not the leader.
    /// `SubmitError::ConfigChangePending` when an earlier change is still
    /// uncommitted, which the single-server-change rule forbids.
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
        // Quorum calculations must use the new membership from this point on. A
        // leader that kept counting the old majority while the new one is live
        // could commit an entry the new configuration never agreed to.
        self.set_config(config);
        info!(node = %self.id, term = %self.persistent.current_term, index = %index, members = self.config.size(), "config change proposed");
        Ok(index)
    }

    /// Whether the uncommitted suffix holds a `ConfigChange`. Enforces the
    /// one-change-at-a-time rule of single-server membership changes.
    fn has_pending_config_change(&self) -> bool {
        // Ask the log for the uncommitted range rather than deriving absolute
        // indexes from iterator positions: after compaction the first retained
        // entry is not index 1, and a pending change would read as committed.
        self.persistent
            .log
            .suffix_from(self.volatile.commit_index.next())
            .iter()
            .any(|e| matches!(e.payload, LogPayload::ConfigChange(_)))
    }

    /// Recomputes the commit index from the leader's replication state.
    ///
    /// Figure 2, Rules for Servers (Leaders): if some index N above
    /// `commit_index` is matched by a majority and the entry at N belongs to the
    /// current term, commit through N. The term condition is the Figure 8
    /// restriction of section 5.4.2: an entry from an earlier term may not be
    /// committed by replica count, because a later leader could still overwrite
    /// it. Such entries commit indirectly once a current-term entry above them
    /// commits.
    fn advance_commit_index(&mut self) {
        let Role::Leader(leader) = &self.role else {
            return;
        };

        // The leader stores every entry it has appended, so its own log position
        // counts toward the quorum alongside the tracked peers.
        let mut match_indices: Vec<LogIndex> = leader.match_indices().collect();
        match_indices.push(self.last_log_index());
        match_indices.sort();

        // Sorted ascending, the element at position p is matched by the len - p
        // servers at or above it. A majority of len is len / 2 + 1, so the
        // highest index a majority has reached sits at (len - 1) / 2. Using
        // len / 2 would count exactly half of an even-sized cluster as a
        // majority and commit an entry one server short of quorum.
        let majority_pos = (match_indices.len() - 1) / 2;
        let majority_index = match_indices[majority_pos];

        // term_at rather than entry(), so that a majority index landing exactly
        // on the snapshot boundary is still checked against the term the
        // boundary preserved after its entry was compacted away.
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

    /// Whether any committed entry has yet to be applied.
    pub fn has_pending_applies(&self) -> bool {
        self.volatile.commit_index > self.volatile.last_applied
    }

    /// Advances `last_applied` to the next committed command and returns it, or
    /// `None` once everything committed has been applied.
    ///
    /// No-op and `ConfigChange` entries advance `last_applied` without being
    /// returned, since the state machine has no use for them. A committed
    /// `ConfigChange` is buffered for `take_committed_config_changes` instead.
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
                    // The membership itself took effect back when the entry was
                    // appended. Only the commit notification is outstanding, so
                    // the server can answer the request that proposed it.
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

    /// Drains the configurations that took effect on append, whether or not they
    /// have committed. The runtime forwards these to the transport so RPCs can
    /// reach a newly added peer straight away.
    pub fn take_config_changes(&mut self) -> Vec<ClusterConfig> {
        std::mem::take(&mut self.pending_config_changes)
    }

    /// Drains the configurations that have committed, paired with the index of
    /// the entry that carried them. The server uses these to resolve the
    /// membership requests waiting on them.
    pub fn take_committed_config_changes(&mut self) -> Vec<(LogIndex, ClusterConfig)> {
        std::mem::take(&mut self.pending_committed_config_changes)
    }

    /// Grafts a leader's entries onto the log, truncating on conflict, per rules
    /// 3 to 5 of the AppendEntries receiver in Figure 2 (section 5.3).
    fn append_entries(&mut self, prev_log_index: LogIndex, entries: Vec<LogEntry<Cmd>>) {
        let has_config_entry = entries
            .iter()
            .any(|e| matches!(e.payload, LogPayload::ConfigChange(_)));

        let outcome = self.persistent.merge_entries(prev_log_index, entries);
        if let MergeOutcome::Truncated { .. } = outcome {
            debug!(node = %self.id, prev_index = %prev_log_index, "log truncated on conflict");
        }

        // A truncation may have removed the entry that established the active
        // membership, and a newly arrived ConfigChange has to become active. The
        // rescan resolves both cases against the same fallback order.
        if matches!(outcome, MergeOutcome::Truncated { .. }) || has_config_entry {
            self.apply_latest_config_from_log();
        }
    }

    /// The membership in force at `index`, ignoring anything appended after it.
    ///
    /// `self.config` tracks the log tail, because a change takes effect on
    /// append rather than on commit (dissertation section 4.1). It is therefore
    /// wrong for a snapshot taken at `index` whenever a later `ConfigChange`
    /// exists, and the prefix has to be rescanned in that case.
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

    /// Compacts the log through `last_applied` and returns the resulting
    /// snapshot.
    ///
    /// The caller supplies `data`, since the runtime owns the state machine and
    /// this node cannot serialize it. Compacting through `last_applied` rather
    /// than `commit_index` is what makes the snapshot self-consistent: those
    /// entries are already reflected in the bytes handed in.
    ///
    /// # Errors
    /// `CompactError::NothingToCompact` when nothing has been applied since the
    /// previous compaction.
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

        // Local compaction never conflicts with its own log, so the suffix
        // above the boundary is always valid.
        self.persistent
            .install_snapshot(snapshot.clone(), SuffixDisposition::Retain);
        info!(node = %self.id, last_index = %last_index, "log compacted to snapshot");

        Ok(snapshot)
    }

    /// Takes the snapshot buffered by `handle_install_snapshot` so the runtime
    /// can hand it to `StateMachine::restore`. Draining rather than borrowing
    /// means a second poll cannot restore the same snapshot twice.
    pub fn take_snapshot_to_restore(&mut self) -> Option<Snapshot> {
        self.pending_snapshot_restore.take()
    }

    /// Receiver implementation of the InstallSnapshot RPC (section 7), in the
    /// single-message variant without offset and done chunking.
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
        // A candidate that hears from a leader of its own term concedes the
        // election (section 5.2).
        if matches!(self.role, Role::Candidate(_)) {
            self.become_follower(req.term, Some(req.leader_id));
        }
        if let Role::Follower(follower) = &mut self.role {
            follower.set_leader(req.leader_id);
        }

        let mut commands = vec![Command::ResetElectionTimer];

        let last_index = req.snapshot.meta.last_index;
        let last_term = req.snapshot.meta.last_term;

        // The snapshot is already covered by what this node has committed. The
        // leader resends until acknowledged, so answering positively rather than
        // reinstalling keeps the RPC idempotent.
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

        // A local entry carrying the snapshot's index and term makes everything
        // above it valid by the Log Matching Property, so the suffix survives.
        // Any other answer, including a boundary past the end of the log, means
        // the local log conflicts and section 7 discards all of it.
        let disposition = match self.persistent.log.term_at(last_index) {
            TermLookup::Known(term) if term == last_term => SuffixDisposition::Retain,
            TermLookup::Known(_) | TermLookup::Compacted | TermLookup::BeyondEnd => {
                SuffixDisposition::Discard
            }
        };

        // The restored state machine already reflects everything through the
        // boundary, so last_applied jumps there rather than replaying it. A
        // surviving suffix above commit_index still applies later through
        // take_entry_to_apply.
        self.volatile.commit_index = std::cmp::max(self.volatile.commit_index, last_index);
        self.volatile.last_applied = last_index;

        // The rescan below falls back to the snapshot's configuration, so
        // latest_snapshot has to be the new one first. In the discard branch the
        // log is now empty and that fallback is the only source; in the retain
        // branch a surviving ConfigChange still wins, being effective on append.
        // Either way, resolving against the previous snapshot would restore a
        // membership the cluster has already left behind.
        self.persistent
            .install_snapshot(req.snapshot.clone(), disposition);
        self.apply_latest_config_from_log();
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

    /// Handles an InstallSnapshot response. A leader in the response's term
    /// advances the peer's replication state, which may complete a quorum;
    /// stale terms and other roles are ignored.
    ///
    /// There is no back-off branch. A rejection can only mean a stale term,
    /// since a snapshot carries the whole compacted prefix and leaves no earlier
    /// position to probe.
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
    use proptest::prelude::*;

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

        // The no-op at index 1 belongs to the current term, so a majority match
        // there is enough to advance the commit index.
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
    fn single_node_leader_commits_its_noop_on_election() {
        let mut n = node(1, &[]);

        n.election_timeout();

        assert!(is_leader(&n), "a lone member is its own majority");
        assert_eq!(n.last_log_index(), LogIndex::from(1));
        assert_eq!(
            n.volatile.commit_index,
            LogIndex::from(1),
            "no peer will ever acknowledge, so the local append is the quorum"
        );
    }

    #[test]
    fn single_node_leader_commits_and_applies_a_submitted_command() {
        let mut n = node(1, &[]);
        n.election_timeout();

        let index = n.submit_command("SET price=1250".to_string()).unwrap();

        assert_eq!(index, LogIndex::from(2));
        assert_eq!(n.volatile.commit_index, LogIndex::from(2));

        let applied = n.take_entry_to_apply().unwrap();
        assert_eq!(applied.index, LogIndex::from(2));
        assert_eq!(applied.command, &"SET price=1250".to_string());
    }

    #[test]
    fn single_node_leader_commits_its_own_config_change() {
        let mut n = node(1, &[]);
        n.election_timeout();

        let index = n.propose_config_change(test_config(1, &[])).unwrap();

        assert_eq!(index, LogIndex::from(2));
        assert_eq!(n.volatile.commit_index, LogIndex::from(2));
        assert!(
            n.take_entry_to_apply().is_none(),
            "a ConfigChange is not a state-machine command"
        );
        assert_eq!(
            n.take_committed_config_changes(),
            vec![(LogIndex::from(2), test_config(1, &[]))]
        );
    }

    #[test]
    fn single_node_leader_can_compact_the_entries_it_committed_alone() {
        let mut n = node(1, &[]);
        n.election_timeout();
        n.submit_command("SET price=1250".to_string()).unwrap();
        while n.take_entry_to_apply().is_some() {}

        let snapshot = n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();

        assert_eq!(snapshot.meta.last_index, LogIndex::from(2));
        assert_eq!(n.persistent.log.snapshot_last_index(), LogIndex::from(2));
    }

    #[test]
    fn multi_node_leader_does_not_commit_its_noop_before_any_peer_acknowledges() {
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
        assert_eq!(
            n.volatile.commit_index,
            LogIndex::default(),
            "two of three logs still lack the no-op"
        );
    }

    #[test]
    fn multi_node_leader_does_not_commit_a_submitted_command_before_replication() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );

        n.submit_command("SET price=1250".to_string()).unwrap();

        assert_eq!(n.volatile.commit_index, LogIndex::default());
    }

    #[test]
    fn shrinking_the_configuration_commits_what_the_smaller_quorum_already_holds() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );
        n.submit_command("SET price=1250".to_string()).unwrap();
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(2),
            },
        );
        assert_eq!(n.volatile.commit_index, LogIndex::from(2));

        // Node 3 never replicated anything. Dropping it makes {1, 2} the whole
        // cluster, and both already hold index 2, so the ConfigChange at index 3
        // commits on the two logs that carry it.
        let index = n.propose_config_change(test_config(1, &[2])).unwrap();
        assert_eq!(index, LogIndex::from(3));
        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(3),
            },
        );

        assert_eq!(n.volatile.commit_index, LogIndex::from(3));
    }

    #[test]
    fn adopting_a_configuration_on_a_follower_never_advances_the_commit_index() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET price=1250".to_string()),
        });

        n.set_config(test_config(1, &[2]));

        assert!(is_follower(&n));
        assert_eq!(
            n.volatile.commit_index,
            LogIndex::default(),
            "only a leader may decide a commit index from replication state"
        );
    }

    #[test]
    fn two_node_leader_needs_both_logs_to_commit() {
        let mut n = node(1, &[2]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );

        assert_eq!(
            n.volatile.commit_index,
            LogIndex::default(),
            "a majority of two is two; the leader's own log is not enough"
        );

        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(1),
            },
        );

        assert_eq!(n.volatile.commit_index, LogIndex::from(1));
    }

    #[test]
    fn four_node_leader_needs_three_logs_to_commit() {
        let mut n = node(1, &[2, 3, 4]);
        n.election_timeout();
        for voter in [2, 3] {
            n.handle_request_vote_response(
                NodeId::from(voter),
                RequestVoteResponse {
                    term: Term::from(1),
                    vote: Vote::Granted,
                },
            );
        }
        assert!(is_leader(&n));

        n.handle_append_entries_response(
            NodeId::from(2),
            AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(1),
            },
        );

        assert_eq!(
            n.volatile.commit_index,
            LogIndex::default(),
            "two of four logs is half, not a majority of four"
        );

        n.handle_append_entries_response(
            NodeId::from(3),
            AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(1),
            },
        );

        assert_eq!(n.volatile.commit_index, LogIndex::from(1));
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

        // A term 2 leader overwrites entry 2 in place, leaving the log length
        // unchanged. Storage cannot detect this by comparing lengths.
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

    /// A compacted log's retained length is smaller than its last index. Deriving
    /// the first appended index from that length names an index that is still
    /// retained, so `save` hands storage a suffix beginning one or more entries
    /// too early and the entry is persisted twice.
    #[test]
    fn save_after_compaction_does_not_duplicate_a_retained_entry() {
        use crate::storage::MemoryStorage;

        let mut storage: MemoryStorage<String> = MemoryStorage::new();
        let mut persistent = compacted_state_with_one_retained_entry(&mut storage);

        persistent.merge_entries(
            LogIndex::from(2),
            vec![LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET region=eu-west-1".to_string()),
            }],
        );
        persistent.save(&mut storage).unwrap();

        let reloaded: PersistentState<String> = PersistentState::load(&storage).unwrap();
        assert_eq!(reloaded.log, persistent.log);
    }

    /// The same accounting failure, checked across a real reopen so that the
    /// duplicate cannot be masked by in-memory state the backend still holds.
    #[test]
    fn file_storage_reopen_after_compaction_does_not_duplicate_a_retained_entry() {
        use crate::storage::FileStorage;

        let dir = tempfile::tempdir().expect("tempdir");
        let mut storage: FileStorage<String> = FileStorage::open(dir.path()).expect("open storage");
        let mut persistent = compacted_state_with_one_retained_entry(&mut storage);

        persistent.merge_entries(
            LogIndex::from(2),
            vec![LogEntry {
                term: Term::from(1),
                payload: LogPayload::Command("SET region=eu-west-1".to_string()),
            }],
        );
        persistent.save(&mut storage).unwrap();
        drop(storage);

        let reopened: FileStorage<String> = FileStorage::open(dir.path()).expect("reopen storage");
        let reloaded: PersistentState<String> = PersistentState::load(&reopened).unwrap();
        assert_eq!(reloaded.log, persistent.log);
    }

    /// Two entries appended and made durable, then compacted through index 1, so
    /// that the log retains exactly entry 2 above a snapshot boundary at 1.
    fn compacted_state_with_one_retained_entry<S: Storage<String>>(
        storage: &mut S,
    ) -> PersistentState<String>
    where
        S::Error: std::fmt::Debug,
    {
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
        persistent.save(storage).unwrap();

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_index: LogIndex::from(1),
                last_term: Term::from(1),
                config: test_config(1, &[2, 3]),
            },
            data: SnapshotData::new(b"name=miles".to_vec()),
        };
        persistent.install_snapshot(snapshot, SuffixDisposition::Retain);
        persistent.save(storage).unwrap();

        persistent
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

    /// A vote from a server outside the current configuration, whether a removed
    /// member or a spoofed envelope on the unauthenticated transport, must not
    /// count toward the majority that `has_majority` computes over the
    /// configuration size.
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
    fn second_config_change_rejected_when_compaction_moved_the_log_start() {
        let mut n = node(1, &[2, 3]);
        n.election_timeout();
        n.handle_request_vote_response(
            NodeId::from(2),
            RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            },
        );
        // The leader no-op at index 1 commits, applies, and is compacted away,
        // so the first retained entry no longer sits at index 1.
        n.volatile.commit_index = LogIndex::from(1);
        n.volatile.last_applied = LogIndex::from(1);
        n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();

        let config_a = test_config(1, &[2, 3, 4]);
        let config_b = test_config(1, &[2, 3, 4, 5]);

        let first = n.propose_config_change(config_a).unwrap();
        assert_eq!(first, LogIndex::from(2));

        assert_eq!(
            n.propose_config_change(config_b),
            Err(SubmitError::ConfigChangePending),
            "the uncommitted change at index 2 is the first retained entry; \
             reading its position as index 1 would make it look committed"
        );
    }

    proptest! {
        /// A `ConfigChange` is pending exactly when its absolute index is above
        /// the commit index, whatever compaction did to the retained range.
        #[test]
        fn pending_config_change_follows_absolute_index_across_compaction_boundaries(
            length in 4usize..10,
            config_slot in 1usize..10,
            first_boundary in 1usize..10,
            boundary_gap in 1usize..4,
            commit_gap in 0usize..4,
        ) {
            let config_index = config_slot.min(length);
            let first = first_boundary.min(length - 2);
            let second = (first + boundary_gap).min(length - 1);
            let commit = (second + commit_gap).min(length);

            let mut n = node(1, &[2, 3]);
            for i in 1..=length {
                let payload = if i == config_index {
                    LogPayload::ConfigChange(test_config(1, &[2, 3, 4]))
                } else {
                    LogPayload::Command(format!("SET counter={i}"))
                };
                n.push_entry(LogEntry { term: Term::from(1), payload });
            }
            n.volatile.commit_index = LogIndex::from(commit as u64);

            // Two compactions, so the retained range starts well above index 1
            // and a position-derived index would be wrong by two offsets.
            n.volatile.last_applied = LogIndex::from(first as u64);
            n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();
            n.volatile.last_applied = LogIndex::from(second as u64);
            n.compact_to_snapshot(SnapshotData::new(vec![2])).unwrap();

            prop_assert_eq!(n.has_pending_config_change(), config_index > commit);
        }
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
        }); // index 3, beyond the boundary, so it must not be picked

        n.volatile.commit_index = LogIndex::from(2);
        n.volatile.last_applied = LogIndex::from(2);

        let snapshot = n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();

        assert_eq!(snapshot.meta.config, earlier_config);
    }

    #[test]
    fn compact_to_snapshot_falls_back_to_previous_snapshot_config_when_retained_prefix_has_none() {
        let mut n = node(1, &[2, 3]);
        let grown_config = test_config(1, &[2, 3, 4]);

        // The first configuration is compacted into a snapshot, so the retained
        // log no longer holds it.
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(grown_config.clone()),
        }); // index 1
        n.set_config(grown_config.clone()); // effective on append, section 4.1
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        }); // index 2, first compaction boundary
        n.volatile.commit_index = LogIndex::from(2);
        n.volatile.last_applied = LogIndex::from(2);
        let first_snapshot = n.compact_to_snapshot(SnapshotData::new(vec![1])).unwrap();
        assert_eq!(first_snapshot.meta.config, grown_config);

        // The second configuration is appended but not yet applied when the
        // second compaction fires, so the retained prefix, index 3 alone,
        // carries no ConfigChange.
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        }); // index 3, second compaction boundary
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(test_config(1, &[2, 3, 4, 5])),
        }); // index 4, beyond the boundary, so it must not be picked
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
        }); // index 2, term 1, conflicting with the snapshot's term 9

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

    /// A boundary past the end of the local log leaves nothing to compare terms
    /// against, so section 7 treats it like a conflict and clears the log.
    #[test]
    fn follower_discards_log_when_snapshot_boundary_is_past_its_end() {
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        }); // index 1

        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(5, 3, test_config(1, &[2, 3])),
        };
        n.handle_install_snapshot(NodeId::from(2), req);

        assert!(n.persistent.log.is_empty());
        assert_eq!(n.persistent.log.snapshot_last_index(), LogIndex::from(5));
        assert_eq!(n.persistent.log.snapshot_last_term(), Term::from(3));
    }

    /// The durable log must end up where the in-memory one did. A discard that
    /// reached only memory would come back at the next restart.
    #[test]
    fn save_clears_storage_log_after_a_conflicting_install() {
        use crate::storage::MemoryStorage;

        let mut storage: MemoryStorage<String> = MemoryStorage::new();
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        });
        n.save(&mut storage).expect("save appends");

        n.handle_install_snapshot(
            NodeId::from(2),
            InstallSnapshot {
                term: Term::from(1),
                leader_id: NodeId::from(2),
                snapshot: test_snapshot(2, 9, test_config(1, &[2, 3])),
            },
        );
        n.save(&mut storage).expect("save installs");

        let loaded = storage.load().expect("load");
        assert!(loaded.entries.is_empty());
        assert_eq!(
            loaded.snapshot.map(|snap| snap.meta.last_term),
            Some(Term::from(9))
        );
    }

    /// A failed install stays recorded. Dropping it would leave the in-memory
    /// log compacted and storage still holding the prefix, with nothing left to
    /// replay the difference.
    #[test]
    fn save_keeps_the_recorded_install_when_storage_rejects_it() {
        use crate::storage::LoadedState;

        #[derive(Debug, PartialEq, Eq)]
        struct StorageOffline;

        struct FailingStorage {
            install_attempts: usize,
        }

        impl Storage<String> for FailingStorage {
            type Error = StorageOffline;

            fn load(&self) -> Result<LoadedState<String>, Self::Error> {
                Err(StorageOffline)
            }

            fn set_meta(
                &mut self,
                _term: Term,
                _voted_for: Option<NodeId>,
            ) -> Result<(), Self::Error> {
                Ok(())
            }

            fn truncate_from(&mut self, _index: LogIndex) -> Result<(), Self::Error> {
                Ok(())
            }

            fn append(&mut self, _entries: &[LogEntry<String>]) -> Result<(), Self::Error> {
                Ok(())
            }

            fn install_snapshot(
                &mut self,
                _snapshot: &Snapshot,
                _disposition: SuffixDisposition,
            ) -> Result<(), Self::Error> {
                self.install_attempts += 1;
                Err(StorageOffline)
            }
        }

        let mut storage = FailingStorage {
            install_attempts: 0,
        };
        let mut n = node(1, &[2, 3]);
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        });
        n.handle_install_snapshot(
            NodeId::from(2),
            InstallSnapshot {
                term: Term::from(1),
                leader_id: NodeId::from(2),
                snapshot: test_snapshot(2, 9, test_config(1, &[2, 3])),
            },
        );

        assert_eq!(n.save(&mut storage), Err(StorageOffline));
        assert_eq!(n.save(&mut storage), Err(StorageOffline));
        assert_eq!(storage.install_attempts, 2, "the install must be retried");
    }

    #[test]
    fn follower_retains_suffix_config_change_over_snapshots_boundary_time_config() {
        let mut n = node(1, &[2, 3]);
        let boundary_time_config = test_config(1, &[2, 3]);
        let grown_config = test_config(1, &[2, 3, 4]);

        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET name=miles".to_string()),
        }); // index 1
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command("SET status=pending".to_string()),
        }); // index 2, the snapshot boundary
        n.push_entry(LogEntry {
            term: Term::from(1),
            payload: LogPayload::ConfigChange(grown_config.clone()),
        }); // index 3, beyond the boundary and effective on append (section 4.1)

        let req = InstallSnapshot {
            term: Term::from(1),
            leader_id: NodeId::from(2),
            snapshot: test_snapshot(2, 1, boundary_time_config), // term matches our entry at index 2
        };
        n.handle_install_snapshot(NodeId::from(2), req);

        assert_eq!(n.persistent.log.snapshot_last_index(), LogIndex::from(2));
        assert_eq!(
            n.config(),
            &grown_config,
            "surviving suffix's ConfigChange must win over the snapshot's boundary-time config"
        );
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

            fn install_snapshot(
                &mut self,
                snapshot: &Snapshot,
                disposition: SuffixDisposition,
            ) -> Result<(), Self::Error> {
                self.calls.push("install_snapshot");
                self.inner.install_snapshot(snapshot, disposition)
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
