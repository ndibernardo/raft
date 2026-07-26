use std::num::NonZeroUsize;
use std::time::Duration;
use std::time::Instant;

use rand::Rng;

use crate::core::command::Command;
use crate::core::node::CompactError;
use crate::core::node::Node;
use crate::core::node::NotLeaderError;
use crate::core::node::Role;
use crate::core::node::SubmitError;
use crate::core::types::ClusterConfig;
use crate::core::types::LogIndex;
use crate::core::types::Message;
use crate::core::types::NodeId;
use crate::core::types::SnapshotData;
use crate::core::types::Term;
use crate::storage::Storage;

/// The application state that committed commands are applied to.
///
/// Implementations must be deterministic: every replica applies the same
/// commands in the same order and has to arrive at the same state, or a snapshot
/// taken on one node will not reconstruct the same state on another.
pub trait StateMachine<Cmd> {
    type Output;
    type SnapshotError: std::error::Error;

    /// Applies one committed command and returns whatever the client should see.
    fn apply(&mut self, command: Cmd) -> Self::Output;

    /// Serializes the current state.
    ///
    /// The result must reflect exactly the commands applied so far, no more and
    /// no fewer, because it is paired with a log index that claims precisely
    /// that.
    fn snapshot(&self) -> Result<SnapshotData, Self::SnapshotError>;

    /// Replaces the state wholesale with the contents of `data`. Not a merge:
    /// anything the snapshot does not contain is gone afterward.
    fn restore(&mut self, data: &SnapshotData) -> Result<(), Self::SnapshotError>;
}

/// Something that has happened and requires the node to act.
pub enum Event<Cmd> {
    ElectionTimeout,
    HeartbeatTimeout,
    Message { from: NodeId, message: Message<Cmd> },
}

/// Timer durations governing elections and replication.
pub struct TimerConfig {
    /// Base time a follower waits without hearing from a leader before standing
    /// for election. The actual wait is randomized within this value and twice
    /// it.
    pub election_timeout: Duration,
    /// How often a leader replicates. Must be comfortably below
    /// `election_timeout`, or followers will time out under a healthy leader.
    pub heartbeat_interval: Duration,
}

impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(100),
        }
    }
}

/// When the runtime compacts the log on its own. Evaluated after every batch of
/// applies.
#[derive(Debug, Default, Clone, Copy)]
pub struct SnapshotPolicy {
    /// Compact once this many entries have been applied since the last snapshot.
    /// `None` leaves compaction entirely to the caller.
    pub compact_threshold: Option<NonZeroUsize>,
}

/// Why a `Runtime` operation failed. The two sources are the durable storage
/// backend and the state machine's own serialization.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError<StErr, SmErr> {
    #[error("storage error: {0}")]
    Storage(StErr),
    #[error("state machine error: {0}")]
    StateMachine(SmErr),
}

/// Shorthand for a `Runtime` result. Every fallible method fails with a storage
/// error or a state machine error and nothing else.
type RuntimeResult<T, StErr, SmErr> = Result<T, RuntimeError<StErr, SmErr>>;

/// Drives a `Node`, supplying the three things it deliberately lacks: a clock,
/// durable storage, and a state machine.
///
/// The node decides what should happen; the runtime makes it happen, in the
/// order Raft requires. Sending the resulting messages is still the caller's
/// job, which keeps the transport out of this layer.
pub struct Runtime<Cmd, S: StateMachine<Cmd>, St> {
    node: Node<Cmd>,
    state_machine: S,
    storage: St,
    config: TimerConfig,
    snapshot_policy: SnapshotPolicy,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    /// Outputs from applying committed entries, in log order. Drained by
    /// `take_outputs`.
    pending_outputs: Vec<(Term, LogIndex, S::Output)>,
    /// Set when the most recent `handle` demoted this node from leader. Drained
    /// by `take_stepped_down`.
    stepped_down: bool,
}

impl<Cmd: Clone, S: StateMachine<Cmd>, St: Storage<Cmd>> Runtime<Cmd, S, St> {
    /// Wraps `node` with the clock, storage, and state machine it needs.
    pub fn new(
        node: Node<Cmd>,
        state_machine: S,
        storage: St,
        config: TimerConfig,
        snapshot_policy: SnapshotPolicy,
    ) -> Self {
        let now = Instant::now();
        // Randomized between one and two election timeouts, exactly as
        // Command::ResetElectionTimer does. A fixed initial deadline would make
        // every node in a fresh cluster stand for election at the same instant,
        // splitting the first vote every time.
        let jitter_ms = rand::rng().random_range(0..config.election_timeout.as_millis() as u64);
        let election_deadline = now + config.election_timeout + Duration::from_millis(jitter_ms);
        Self {
            node,
            state_machine,
            storage,
            snapshot_policy,
            election_deadline,
            heartbeat_deadline: now + config.heartbeat_interval,
            config,
            pending_outputs: Vec::new(),
            stepped_down: false,
        }
    }

    /// Rebuilds a runtime from durable storage, restarting as a follower
    /// (section 5.1).
    ///
    /// A persisted snapshot is restored into the state machine before anything
    /// else, since the log no longer contains the entries it covers. Committed
    /// entries above the snapshot boundary are replayed later, as the leader
    /// drives this node forward with AppendEntries.
    ///
    /// # Errors
    /// `RuntimeError::Storage` if the persisted state cannot be read.
    /// `RuntimeError::StateMachine` if the snapshot cannot be restored.
    pub fn from_storage(
        id: NodeId,
        initial_config: ClusterConfig,
        mut state_machine: S,
        storage: St,
        config: TimerConfig,
        snapshot_policy: SnapshotPolicy,
    ) -> RuntimeResult<Self, St::Error, S::SnapshotError> {
        let node =
            Node::from_storage(id, initial_config, &storage).map_err(RuntimeError::Storage)?;
        if let Some(snapshot) = node.persistent().latest_snapshot() {
            state_machine
                .restore(&snapshot.data)
                .map_err(RuntimeError::StateMachine)?;
        }
        Ok(Self::new(
            node,
            state_machine,
            storage,
            config,
            snapshot_policy,
        ))
    }

    /// The wrapped node.
    pub fn node(&self) -> &Node<Cmd> {
        &self.node
    }

    /// The state machine, for serving reads.
    pub fn state_machine(&self) -> &S {
        &self.state_machine
    }

    /// Mutable access to the state machine. Bypassing `apply` with this leaves
    /// the replicas divergent, since the change is in no log.
    pub fn state_machine_mut(&mut self) -> &mut S {
        &mut self.state_machine
    }

    /// Consumes the runtime and hands back its storage, so the same storage can
    /// be fed to `from_storage` to simulate a crash and restart.
    pub fn into_storage(self) -> St {
        self.storage
    }

    /// Processes one event and returns the messages the caller must send.
    ///
    /// State is durable by the time this returns, and not before. The caller
    /// must therefore not transmit anything until it does, since section 5.1
    /// forbids responding to an RPC ahead of the state that response promises.
    ///
    /// # Errors
    /// `RuntimeError::Storage` if persisting fails.
    /// `RuntimeError::StateMachine` if restoring a snapshot or taking one fails.
    pub fn handle(
        &mut self,
        event: Event<Cmd>,
    ) -> RuntimeResult<Vec<Command<Cmd>>, St::Error, S::SnapshotError> {
        let was_leader = matches!(self.node.role(), Role::Leader(_));

        let commands = match event {
            Event::ElectionTimeout => self.node.election_timeout(),
            Event::HeartbeatTimeout => self.node.heartbeat_timeout(),
            Event::Message { from, message } => self.handle_message(from, message),
        };

        if was_leader && !matches!(self.node.role(), Role::Leader(_)) {
            self.stepped_down = true;
        }

        self.process_commands(&commands);
        self.node
            .save(&mut self.storage)
            .map_err(RuntimeError::Storage)?;
        self.restore_snapshot_if_pending()?;
        self.apply_committed();
        self.maybe_compact()?;

        Ok(commands)
    }

    /// Feeds a snapshot accepted by `handle_install_snapshot` to the state
    /// machine.
    ///
    /// A failed restore propagates rather than being retried quietly. The state
    /// machine may be half replaced at that point, and continuing to apply
    /// entries on top of it would diverge from the rest of the cluster.
    fn restore_snapshot_if_pending(&mut self) -> RuntimeResult<(), St::Error, S::SnapshotError> {
        if let Some(snapshot) = self.node.take_snapshot_to_restore() {
            self.state_machine
                .restore(&snapshot.data)
                .map_err(RuntimeError::StateMachine)?;
        }
        Ok(())
    }

    /// Snapshots the state machine and compacts the log once the number of
    /// applied but uncompacted entries reaches the configured threshold.
    ///
    /// A failed snapshot surfaces to the caller, which decides whether to retry.
    /// Swallowing it would let the log grow without bound behind a policy that
    /// silently never fires.
    fn maybe_compact(&mut self) -> RuntimeResult<(), St::Error, S::SnapshotError> {
        let Some(threshold) = self.snapshot_policy.compact_threshold else {
            return Ok(());
        };
        let boundary = self.node.persistent().log().snapshot_last_index();
        let uncompacted = self.node.volatile().last_applied.value_since(boundary);
        if uncompacted.is_none_or(|count| count < threshold.get() as u64) {
            return Ok(());
        }

        let data = self
            .state_machine
            .snapshot()
            .map_err(RuntimeError::StateMachine)?;
        match self.node.compact_to_snapshot(data) {
            Ok(_snapshot) => {}
            Err(CompactError::NothingToCompact) => return Ok(()),
        }
        self.node
            .save(&mut self.storage)
            .map_err(RuntimeError::Storage)?;
        Ok(())
    }

    /// Whether the most recent `handle` demoted this node from leader, clearing
    /// the flag.
    ///
    /// On a true result the caller must fail every client it has waiting. An
    /// uncommitted entry submitted to this node can still be overwritten by the
    /// next leader, so waiting for it to commit may never terminate.
    pub fn take_stepped_down(&mut self) -> bool {
        std::mem::replace(&mut self.stepped_down, false)
    }

    /// The event whose deadline has passed, if any. A leader watches the
    /// heartbeat interval; every other role watches the election timeout
    /// (section 5.2).
    pub fn poll_timers(&self) -> Option<Event<Cmd>> {
        let now = Instant::now();

        if matches!(self.node.role(), Role::Leader(_)) {
            if now >= self.heartbeat_deadline {
                return Some(Event::HeartbeatTimeout);
            }
            return None;
        }

        if now >= self.election_deadline {
            return Some(Event::ElectionTimeout);
        }

        None
    }

    /// The next instant at which `poll_timers` can return an event. Sleep until
    /// then instead of polling in a loop.
    pub fn next_deadline(&self) -> Instant {
        if matches!(self.node.role(), Role::Leader(_)) {
            self.heartbeat_deadline
        } else {
            self.election_deadline
        }
    }

    /// Appends a client command and returns the index it landed at. The command
    /// is not committed yet; the caller learns that from `take_outputs`.
    ///
    /// # Errors
    /// `NotLeaderError` when this node is not the leader, carrying the leader to
    /// retry against.
    pub fn submit(&mut self, command: Cmd) -> Result<LogIndex, NotLeaderError> {
        self.node.submit_command(command)
    }

    /// Proposes a membership change and returns the index it landed at.
    ///
    /// # Errors
    /// `SubmitError::NotLeader` when this node is not the leader.
    /// `SubmitError::ConfigChangePending` when an earlier change is uncommitted.
    pub fn submit_config_change(&mut self, config: ClusterConfig) -> Result<LogIndex, SubmitError> {
        self.node.propose_config_change(config)
    }

    /// Drains the configurations that took effect on append. The caller passes
    /// these to the transport so a newly added peer becomes reachable at once.
    pub fn take_config_changes(&mut self) -> Vec<ClusterConfig> {
        self.node.take_config_changes()
    }

    /// Drains the configurations that have committed, with the index of the
    /// entry that carried each. The caller resolves the membership requests
    /// waiting on them.
    pub fn take_committed_config_changes(&mut self) -> Vec<(LogIndex, ClusterConfig)> {
        self.node.take_committed_config_changes()
    }

    fn handle_message(&mut self, from: NodeId, message: Message<Cmd>) -> Vec<Command<Cmd>> {
        match message {
            Message::RequestVote(req) => self.node.handle_request_vote(from, req),
            Message::RequestVoteResponse(resp) => {
                self.node.handle_request_vote_response(from, resp)
            }
            Message::AppendEntries(req) => self.node.handle_append_entries(from, req),
            Message::AppendEntriesResponse(resp) => {
                self.node.handle_append_entries_response(from, resp)
            }
            Message::InstallSnapshot(req) => self.node.handle_install_snapshot(from, req),
            Message::InstallSnapshotResponse(resp) => {
                self.node.handle_install_snapshot_response(from, resp)
            }
        }
    }

    fn process_commands(&mut self, commands: &[Command<Cmd>]) {
        for command in commands {
            match command {
                Command::ResetElectionTimer => {
                    // Randomized between one and two election timeouts, so that
                    // nodes stand for election at different moments (section
                    // 5.2). Identical deadlines produce split votes that repeat
                    // indefinitely, since every retry collides again.
                    let base = self.config.election_timeout;
                    let jitter_ms = rand::rng().random_range(0..base.as_millis() as u64);
                    self.election_deadline =
                        Instant::now() + base + Duration::from_millis(jitter_ms);
                }
                Command::ResetHeartbeatTimer => {
                    self.heartbeat_deadline = Instant::now() + self.config.heartbeat_interval;
                }
                Command::Send { .. } => {
                    // Returned to the caller, which owns the transport.
                }
            }
        }
    }

    /// Drains the outputs applied since the last call, in commit order, as
    /// triples of term, index, and output.
    ///
    /// The term is part of the key because an index alone does not identify a
    /// submission: a later leader can write an unrelated entry at the same
    /// index. See `Applied::term`.
    pub fn take_outputs(&mut self) -> Vec<(Term, LogIndex, S::Output)> {
        std::mem::take(&mut self.pending_outputs)
    }

    fn apply_committed(&mut self) {
        let node_id = self.node.id();
        while let Some(applied) = self.node.take_entry_to_apply() {
            tracing::debug!(node = %node_id, index = %applied.index, "entry applied");
            let output = self.state_machine.apply(applied.command.clone());
            self.pending_outputs
                .push((applied.term, applied.index, output));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use super::*;
    use crate::app::kv::KvCommand;
    use crate::app::kv::KvResult;
    use crate::app::kv::KvStore;
    use crate::core::types::AppendEntriesResponse;
    use crate::core::types::InstallSnapshot;
    use crate::core::types::InstallSnapshotResponse;
    use crate::core::types::RequestVoteResponse;
    use crate::core::types::Snapshot;
    use crate::core::types::SnapshotMeta;
    use crate::core::types::Term;
    use crate::core::types::Vote;
    use crate::storage::MemoryStorage;

    fn test_config(id: u64, peers: &[u64]) -> ClusterConfig {
        let members: HashMap<NodeId, SocketAddr> = std::iter::once(id)
            .chain(peers.iter().copied())
            .map(|i| {
                let addr: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
                (NodeId::from(i), addr)
            })
            .collect();
        ClusterConfig::new(members).unwrap()
    }

    fn runtime(id: u64, peers: &[u64]) -> Runtime<KvCommand, KvStore, MemoryStorage<KvCommand>> {
        runtime_with_policy(id, peers, SnapshotPolicy::default())
    }

    fn runtime_with_policy(
        id: u64,
        peers: &[u64],
        snapshot_policy: SnapshotPolicy,
    ) -> Runtime<KvCommand, KvStore, MemoryStorage<KvCommand>> {
        let node = Node::new(NodeId::from(id), test_config(id, peers));
        Runtime::new(
            node,
            KvStore::new(),
            MemoryStorage::new(),
            TimerConfig::default(),
            snapshot_policy,
        )
    }

    /// Elects `rt` leader in its 2-peer cluster: election plus one granted vote from
    /// peer 2 is already a majority of 3, so peer 3 need never respond.
    fn elect_leader(rt: &mut Runtime<KvCommand, KvStore, MemoryStorage<KvCommand>>) {
        rt.handle(Event::ElectionTimeout).unwrap();
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            }),
        })
        .unwrap();
    }

    /// Submits `command` and simulates peer 2 acking replication through the
    /// resulting index, advancing (and applying) commit_index up to it.
    fn submit_and_commit(
        rt: &mut Runtime<KvCommand, KvStore, MemoryStorage<KvCommand>>,
        command: KvCommand,
    ) -> LogIndex {
        let index = rt.submit(command).unwrap();
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: index,
            }),
        })
        .unwrap();
        index
    }

    #[test]
    fn election_timeout_starts_election() {
        let mut rt = runtime(1, &[2, 3]);

        let commands = rt.handle(Event::ElectionTimeout).unwrap();

        assert!(matches!(rt.node().role(), Role::Candidate(_)));
        assert!(!commands.is_empty());
    }

    #[test]
    fn leader_applies_committed_entries() {
        let mut rt = runtime(1, &[2, 3]);

        // Become leader.
        rt.handle(Event::ElectionTimeout).unwrap();
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            }),
        })
        .unwrap();
        assert!(matches!(rt.node().role(), Role::Leader(_)));

        // Submit command.
        let index = rt.submit(KvCommand::Set {
            key: "username".to_string(),
            value: "miles".to_string(),
        });
        assert!(index.is_ok());

        // Simulate replication success (no-op at index 1, command at index 2).
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(2),
            }),
        })
        .unwrap();

        // Verify command was applied to state machine.
        let result = rt.state_machine.apply(KvCommand::Get {
            key: "username".to_string(),
        });
        assert_eq!(result, KvResult::Value(Some("miles".to_string())));
    }

    #[test]
    fn take_outputs_returns_applied_results() {
        let mut rt = runtime(1, &[2, 3]);

        // Become leader.
        rt.handle(Event::ElectionTimeout).unwrap();
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            }),
        })
        .unwrap();

        // Submit a command and replicate it (no-op at 1, command at 2).
        rt.submit(KvCommand::Set {
            key: "counter".to_string(),
            value: "1".to_string(),
        })
        .unwrap();
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(2),
            }),
        })
        .unwrap();

        // take_outputs should return exactly the Set result at index 2.
        let outputs = rt.take_outputs();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].0, Term::from(1));
        assert_eq!(outputs[0].1, LogIndex::from(2));
        assert_eq!(outputs[0].2, KvResult::Ok);

        // Subsequent call returns nothing until new commits arrive.
        assert!(rt.take_outputs().is_empty());
    }

    #[test]
    fn from_storage_restores_persistent_state() {
        let mut rt = runtime(1, &[2, 3]);

        // Winning the election appends a no-op, which handle persists.
        rt.handle(Event::ElectionTimeout).unwrap();
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: Term::from(1),
                vote: Vote::Granted,
            }),
        })
        .unwrap();

        // At this point storage holds: term=1, voted_for=1, log=[no-op@1].
        let expected_term = rt.node().persistent().current_term();
        let expected_log_len = rt.node().persistent().log().len();

        let storage = rt.storage;
        let restored = Runtime::from_storage(
            NodeId::from(1),
            test_config(1, &[2, 3]),
            KvStore::new(),
            storage,
            TimerConfig::default(),
            SnapshotPolicy::default(),
        )
        .unwrap();

        // Term and log are recovered from durable storage; node restarts as follower.
        assert_eq!(restored.node().persistent().current_term(), expected_term);
        assert_eq!(restored.node().persistent().log().len(), expected_log_len);
        assert!(matches!(restored.node().role(), Role::Follower(_)));
    }

    /// An un-jittered initial deadline means every node in a fresh
    /// cluster times out at exactly the same instant, reliably producing a split vote
    /// on cold start. Probabilistic: with jitter in [0, T) at millisecond granularity,
    /// 20 identical-by-chance draws is astronomically unlikely.
    #[test]
    fn initial_election_deadline_is_randomized_not_fixed_at_exactly_t() {
        let base = TimerConfig::default().election_timeout;

        let saw_jitter = (0..20).any(|_| {
            let node = Node::new(NodeId::from(1), test_config(1, &[2, 3]));
            let before = Instant::now();
            let rt: Runtime<KvCommand, KvStore, MemoryStorage<KvCommand>> = Runtime::new(
                node,
                KvStore::new(),
                MemoryStorage::new(),
                TimerConfig::default(),
                SnapshotPolicy::default(),
            );
            // 5ms comfortably exceeds constructor call overhead but is well inside the
            // [0, 300ms) jitter range, so this only trips on real jitter, not clock drift.
            rt.election_deadline.duration_since(before) > base + Duration::from_millis(5)
        });

        assert!(
            saw_jitter,
            "initial election deadline must be randomized in [T, 2T), not fixed at exactly T"
        );
    }

    #[test]
    fn timer_reset_on_election_timeout() {
        let mut rt = runtime(1, &[2, 3]);

        // Comparing against the deadline from before the call would be flaky,
        // because the initial deadline is jittered too and a small draw here can
        // land before a large one there. The invariant that actually holds is
        // that handling an ElectionTimeout pushes the deadline at least one full
        // election timeout past the current instant.
        let before = Instant::now();
        rt.handle(Event::ElectionTimeout).unwrap();

        assert!(
            rt.election_deadline >= before + TimerConfig::default().election_timeout,
            "handling ElectionTimeout must reset the deadline to at least T in the future"
        );
    }

    #[test]
    fn runtime_compacts_after_threshold_applies() {
        let mut rt = runtime_with_policy(
            1,
            &[2, 3],
            SnapshotPolicy {
                compact_threshold: NonZeroUsize::new(2),
            },
        );
        elect_leader(&mut rt);

        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "a".to_string(),
                value: "1".to_string(),
            },
        );
        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "b".to_string(),
                value: "2".to_string(),
            },
        );
        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "c".to_string(),
                value: "3".to_string(),
            },
        );

        // no-op@1 + "a"@2 cross the threshold and compact to 2; "b"@3 alone stays
        // below threshold; "c"@4 crosses it again relative to the new boundary.
        assert_eq!(
            rt.node().persistent().log().first_index(),
            LogIndex::from(5)
        );
        let loaded = rt.storage.load().unwrap();
        assert_eq!(
            loaded.snapshot.map(|s| s.meta.last_index),
            Some(LogIndex::from(4))
        );
    }

    #[test]
    fn runtime_below_threshold_does_not_compact() {
        let mut rt = runtime_with_policy(
            1,
            &[2, 3],
            SnapshotPolicy {
                compact_threshold: NonZeroUsize::new(10),
            },
        );
        elect_leader(&mut rt);

        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "x".to_string(),
                value: "1".to_string(),
            },
        );
        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "y".to_string(),
                value: "2".to_string(),
            },
        );

        assert_eq!(
            rt.node().persistent().log().snapshot_last_index(),
            LogIndex::default()
        );
        assert!(rt.storage.load().unwrap().snapshot.is_none());
    }

    #[test]
    fn no_policy_never_compacts() {
        let mut rt = runtime_with_policy(1, &[2, 3], SnapshotPolicy::default());
        elect_leader(&mut rt);

        for i in 0..5 {
            submit_and_commit(
                &mut rt,
                KvCommand::Set {
                    key: format!("key-{i}"),
                    value: format!("value-{i}"),
                },
            );
        }

        assert_eq!(
            rt.node().persistent().log().snapshot_last_index(),
            LogIndex::default()
        );
        assert!(rt.storage.load().unwrap().snapshot.is_none());
    }

    #[test]
    fn from_storage_restores_state_machine_from_snapshot() {
        let mut rt = runtime_with_policy(
            1,
            &[2, 3],
            SnapshotPolicy {
                compact_threshold: NonZeroUsize::new(1),
            },
        );
        elect_leader(&mut rt);
        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "region".to_string(),
                value: "eu-west-1".to_string(),
            },
        );
        assert_eq!(
            rt.node().persistent().log().snapshot_last_index(),
            LogIndex::from(2),
            "threshold of 1 must have compacted after the first commit"
        );

        let storage = rt.storage;
        let mut restored = Runtime::from_storage(
            NodeId::from(1),
            test_config(1, &[2, 3]),
            KvStore::new(),
            storage,
            TimerConfig::default(),
            SnapshotPolicy::default(),
        )
        .unwrap();

        // The compacted entries are gone from the log, so the value can only
        // have come from the restored state machine.
        assert!(
            restored
                .node()
                .persistent()
                .log()
                .entry(LogIndex::from(1))
                .is_none()
        );
        assert_eq!(
            restored.state_machine_mut().apply(KvCommand::Get {
                key: "region".to_string()
            }),
            KvResult::Value(Some("eu-west-1".to_string()))
        );
    }

    #[test]
    fn from_storage_replays_only_suffix_after_snapshot() {
        let mut rt = runtime_with_policy(
            1,
            &[2, 3],
            SnapshotPolicy {
                compact_threshold: NonZeroUsize::new(1),
            },
        );
        elect_leader(&mut rt);
        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "counter".to_string(),
                value: "1".to_string(),
            },
        );

        // Appended and persisted but never acknowledged, so it is still
        // uncommitted when the simulated crash happens.
        rt.submit(KvCommand::Set {
            key: "counter".to_string(),
            value: "2".to_string(),
        })
        .unwrap();
        rt.handle(Event::HeartbeatTimeout).unwrap();

        let storage = rt.storage;
        let mut restored = Runtime::from_storage(
            NodeId::from(1),
            test_config(1, &[2, 3]),
            KvStore::new(),
            storage,
            TimerConfig::default(),
            SnapshotPolicy::default(),
        )
        .unwrap();

        // Both positions must restart exactly at the snapshot boundary. At zero
        // the restored state machine would be replayed over; past the boundary
        // the uncommitted suffix entry would be treated as already applied.
        assert_eq!(restored.node().volatile().commit_index, LogIndex::from(2));
        assert_eq!(restored.node().volatile().last_applied, LogIndex::from(2));
        assert_eq!(
            restored.node().persistent().log().last_index(),
            LogIndex::from(3)
        );
        assert_eq!(
            restored.state_machine_mut().apply(KvCommand::Get {
                key: "counter".to_string()
            }),
            KvResult::Value(Some("1".to_string())),
            "the uncommitted suffix entry must not be applied on restart"
        );
    }

    #[test]
    fn follower_runtime_restores_on_install_snapshot() {
        let mut rt = runtime(1, &[2, 3]);

        let mut source = KvStore::new();
        source.apply(KvCommand::Set {
            key: "region".to_string(),
            value: "eu-west-1".to_string(),
        });
        let data = source.snapshot().unwrap();

        let commands = rt
            .handle(Event::Message {
                from: NodeId::from(2),
                message: Message::InstallSnapshot(InstallSnapshot {
                    term: Term::from(1),
                    leader_id: NodeId::from(2),
                    snapshot: Snapshot {
                        meta: SnapshotMeta {
                            last_index: LogIndex::from(5),
                            last_term: Term::from(1),
                            config: test_config(1, &[2, 3]),
                        },
                        data,
                    },
                }),
            })
            .unwrap();

        assert!(commands.iter().any(|c| matches!(
            c,
            Command::Send {
                to,
                message: Message::InstallSnapshotResponse(InstallSnapshotResponse::Installed {
                    last_index,
                    ..
                }),
            } if *to == NodeId::from(2) && *last_index == LogIndex::from(5)
        )));
        assert_eq!(
            rt.state_machine.apply(KvCommand::Get {
                key: "region".to_string()
            }),
            KvResult::Value(Some("eu-west-1".to_string()))
        );
    }

    #[test]
    fn compaction_does_not_trip_stepped_down() {
        let mut rt = runtime_with_policy(
            1,
            &[2, 3],
            SnapshotPolicy {
                compact_threshold: NonZeroUsize::new(1),
            },
        );
        elect_leader(&mut rt);

        submit_and_commit(
            &mut rt,
            KvCommand::Set {
                key: "x".to_string(),
                value: "1".to_string(),
            },
        );

        assert_eq!(
            rt.node().persistent().log().snapshot_last_index(),
            LogIndex::from(2),
            "precondition: this commit must have triggered a compaction"
        );
        assert!(!rt.take_stepped_down());
    }
}
