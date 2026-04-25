use std::time::{Duration, Instant};

use rand::Rng;

use crate::command::Command;
use crate::node::{Node, Role};
use crate::storage::Storage;
use crate::types::{LogIndex, Message, NodeId};

/// Trait for state machines that can apply commands.
pub trait StateMachine<Cmd> {
    type Output;
    fn apply(&mut self, command: Cmd) -> Self::Output;
}

/// Events that drive the runtime.
pub enum Event<Cmd> {
    ElectionTimeout,
    HeartbeatTimeout,
    Message { from: NodeId, message: Message<Cmd> },
}

/// Timer configuration.
pub struct TimerConfig {
    pub election_timeout: Duration,
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

/// Runtime that wraps a Raft node with timer management and durable storage.
pub struct Runtime<Cmd, S: StateMachine<Cmd>, St> {
    node: Node<Cmd>,
    state_machine: S,
    storage: St,
    config: TimerConfig,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    /// Outputs produced by applying committed entries, in log order.
    /// Drained by the caller via take_outputs after each handle() call.
    pending_outputs: Vec<(LogIndex, S::Output)>,
}

impl<Cmd: Clone, S: StateMachine<Cmd>, St: Storage<Cmd>> Runtime<Cmd, S, St> {
    pub fn new(node: Node<Cmd>, state_machine: S, storage: St, config: TimerConfig) -> Self {
        let now = Instant::now();
        Self {
            node,
            state_machine,
            storage,
            election_deadline: now + config.election_timeout,
            heartbeat_deadline: now + config.heartbeat_interval,
            config,
            pending_outputs: Vec::new(),
        }
    }

    /// Restarts as follower (§5.1). State machine starts fresh — committed entries are
    /// replayed lazily as the leader drives AppendEntries with the updated commit index.
    pub fn from_storage(
        id: NodeId,
        peers: Vec<NodeId>,
        state_machine: S,
        storage: St,
        config: TimerConfig,
    ) -> Result<Self, St::Error> {
        let node = Node::from_storage(id, peers, &storage)?;
        Ok(Self::new(node, state_machine, storage, config))
    }

    pub fn node(&self) -> &Node<Cmd> {
        &self.node
    }

    pub fn state_machine(&self) -> &S {
        &self.state_machine
    }

    pub fn state_machine_mut(&mut self) -> &mut S {
        &mut self.state_machine
    }

    /// Callers must not transmit responses before this returns — §5.1 requires durable state
    /// before responding to any RPC.
    pub fn handle(&mut self, event: Event<Cmd>) -> Result<Vec<Command<Cmd>>, St::Error> {
        let commands = match event {
            Event::ElectionTimeout => self.node.election_timeout(),
            Event::HeartbeatTimeout => self.node.heartbeat_timeout(),
            Event::Message { from, message } => self.handle_message(from, message),
        };

        self.process_commands(&commands);
        self.node.save(&mut self.storage)?;
        self.apply_committed();

        Ok(commands)
    }

    /// §5.2: leaders check the heartbeat deadline; others check the election deadline.
    pub fn poll_timers(&self) -> Option<Event<Cmd>> {
        let now = Instant::now();

        if matches!(self.node.role, Role::Leader(_)) {
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

    /// Use to avoid busy-waiting: sleep until this instant, then call poll_timers.
    pub fn next_deadline(&self) -> Instant {
        if matches!(self.node.role, Role::Leader(_)) {
            self.heartbeat_deadline
        } else {
            self.election_deadline
        }
    }

    /// Returns the assigned log index. None if not leader — client must retry elsewhere.
    pub fn submit(&mut self, command: Cmd) -> Option<LogIndex> {
        self.node.submit_command(command)
    }

    fn handle_message(&mut self, from: NodeId, message: Message<Cmd>) -> Vec<Command<Cmd>> {
        match message {
            Message::RequestVote(req) => self.node.handle_request_vote(from, req),
            Message::RequestVoteResponse(resp) => self.node.handle_request_vote_response(from, resp),
            Message::AppendEntries(req) => self.node.handle_append_entries(from, req),
            Message::AppendEntriesResponse(resp) => {
                self.node.handle_append_entries_response(from, resp)
            }
        }
    }

    fn process_commands(&mut self, commands: &[Command<Cmd>]) {
        for command in commands {
            match command {
                Command::ResetElectionTimer => {
                    // §5.2: randomize in [T, 2T] so nodes time out at different moments,
                    // preventing repeated split votes when multiple candidates start at once.
                    let base = self.config.election_timeout;
                    let jitter_ms = rand::rng().random_range(0..base.as_millis() as u64);
                    self.election_deadline = Instant::now() + base + Duration::from_millis(jitter_ms);
                }
                Command::ResetHeartbeatTimer => {
                    self.heartbeat_deadline = Instant::now() + self.config.heartbeat_interval;
                }
                Command::Send { .. } => {
                    // Sending is handled by caller.
                }
            }
        }
    }

    /// Returns (log_index, output) pairs in commit order since the last call; drains the buffer.
    pub fn take_outputs(&mut self) -> Vec<(LogIndex, S::Output)> {
        std::mem::take(&mut self.pending_outputs)
    }

    fn apply_committed(&mut self) {
        let node_id = self.node.id;
        while let Some(applied) = self.node.take_entry_to_apply() {
            tracing::debug!(node = %node_id, index = %applied.index, "entry applied");
            let output = self.state_machine.apply(applied.command.clone());
            self.pending_outputs.push((applied.index, output));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{KvCommand, KvResult, KvStore};
    use crate::storage::MemoryStorage;
    use crate::types::{AppendEntriesResponse, RequestVoteResponse, Term};

    fn runtime(id: u64, peers: &[u64]) -> Runtime<KvCommand, KvStore, MemoryStorage<KvCommand>> {
        let node = Node::new(
            NodeId::from(id),
            peers.iter().map(|&p| NodeId::from(p)).collect(),
        );
        Runtime::new(node, KvStore::new(), MemoryStorage::new(), TimerConfig::default())
    }

    #[test]
    fn election_timeout_starts_election() {
        let mut rt = runtime(1, &[2, 3]);

        let commands = rt.handle(Event::ElectionTimeout).unwrap();

        assert!(matches!(rt.node().role, Role::Candidate(_)));
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
                vote_granted: true,
            }),
        })
        .unwrap();
        assert!(matches!(rt.node().role, Role::Leader(_)));

        // Submit command.
        let index = rt.submit(KvCommand::Set {
            key: "username".to_string(),
            value: "miles".to_string(),
        });
        assert!(index.is_some());

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
                vote_granted: true,
            }),
        })
        .unwrap();

        // Submit a command and replicate it (no-op at 1, command at 2).
        rt.submit(KvCommand::Set {
            key: "counter".to_string(),
            value: "1".to_string(),
        });
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
        assert_eq!(outputs[0].0, LogIndex::from(2));
        assert_eq!(outputs[0].1, KvResult::Ok);

        // Subsequent call returns nothing until new commits arrive.
        assert!(rt.take_outputs().is_empty());
    }

    #[test]
    fn from_storage_restores_persistent_state() {
        let mut rt = runtime(1, &[2, 3]);

        // Become leader — handle() persists the no-op to storage on each call.
        rt.handle(Event::ElectionTimeout).unwrap();
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            }),
        })
        .unwrap();

        // At this point storage holds: term=1, voted_for=1, log=[no-op@1].
        let expected_term = rt.node().persistent.current_term;
        let expected_log_len = rt.node().persistent.log.len();

        let storage = rt.storage;
        let restored = Runtime::from_storage(
            NodeId::from(1),
            vec![NodeId::from(2), NodeId::from(3)],
            KvStore::new(),
            storage,
            TimerConfig::default(),
        )
        .unwrap();

        // Term and log are recovered from durable storage; node restarts as follower.
        assert_eq!(restored.node().persistent.current_term, expected_term);
        assert_eq!(restored.node().persistent.log.len(), expected_log_len);
        assert!(matches!(restored.node().role, Role::Follower(_)));
    }

    #[test]
    fn timer_reset_on_election_timeout() {
        let mut rt = runtime(1, &[2, 3]);
        let initial_deadline = rt.election_deadline;

        std::thread::sleep(Duration::from_millis(10));
        rt.handle(Event::ElectionTimeout).unwrap();

        assert!(rt.election_deadline > initial_deadline);
    }
}
