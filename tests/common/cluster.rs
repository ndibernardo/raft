use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;

use raft::Command;
use raft::MemoryStorage;
use raft::Node;
use raft::Role;
use raft::Runtime;
use raft::StateMachine;
use raft::TimerConfig;
use raft::types::ClusterConfig;
use raft::types::Message;
use raft::types::NodeId;

/// A message in flight between nodes.
struct InFlight<Cmd> {
    from: NodeId,
    to: NodeId,
    message: Message<Cmd>,
}

/// Simulated cluster for testing. Uses MemoryStorage on every node.
pub struct Cluster<Cmd, S: StateMachine<Cmd>> {
    runtimes: Vec<Runtime<Cmd, S, MemoryStorage<Cmd>>>,
    messages: VecDeque<InFlight<Cmd>>,
}

impl<Cmd: Clone, S: StateMachine<Cmd> + Default> Cluster<Cmd, S> {
    /// Create a cluster with the given number of nodes.
    ///
    /// # Panics
    /// If `size` is 0 — a cluster harness with no nodes isn't a meaningful test fixture.
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "Cluster::new requires at least one node");
        let ids: Vec<NodeId> = (1..=size).map(|i| NodeId::from(i as u64)).collect();

        // Shared config: all nodes start with the same full membership.
        // Addresses are dummies — cluster harness delivers messages in-memory, not over TCP.
        let members: HashMap<NodeId, SocketAddr> = ids
            .iter()
            .map(|&id| {
                // Construct directly from octets to avoid parsing and unwrap.
                let addr = SocketAddr::from(([127, 0, 0, 1], (9000 + id.value()) as u16));
                (id, addr)
            })
            .collect();
        let config = match ClusterConfig::new(members) {
            Ok(config) => config,
            // `size > 0` was asserted above, so `members` is never empty.
            Err(_) => unreachable!("Cluster::new asserts size > 0, so members is non-empty"),
        };

        let runtimes = ids
            .iter()
            .map(|&id| {
                let node = Node::new(id, config.clone());
                Runtime::new(
                    node,
                    S::default(),
                    MemoryStorage::new(),
                    TimerConfig::default(),
                )
            })
            .collect();

        Self {
            runtimes,
            messages: VecDeque::new(),
        }
    }

    /// Get a reference to a node's runtime by index (0-based).
    pub fn runtime(&self, index: usize) -> &Runtime<Cmd, S, MemoryStorage<Cmd>> {
        &self.runtimes[index]
    }

    /// Get a mutable reference to a node's runtime by index (0-based).
    pub fn runtime_mut(&mut self, index: usize) -> &mut Runtime<Cmd, S, MemoryStorage<Cmd>> {
        &mut self.runtimes[index]
    }

    /// Trigger election timeout on a specific node.
    pub fn election_timeout(&mut self, index: usize) {
        let commands = self.runtimes[index]
            .handle(raft::Event::ElectionTimeout)
            .unwrap_or_else(|e| match e {});
        self.queue_commands(index, commands);
    }

    /// Trigger heartbeat timeout on a specific node.
    pub fn heartbeat_timeout(&mut self, index: usize) {
        let commands = self.runtimes[index]
            .handle(raft::Event::HeartbeatTimeout)
            .unwrap_or_else(|e| match e {});
        self.queue_commands(index, commands);
    }

    /// Deliver one pending message. Returns true if a message was available.
    pub fn deliver_one(&mut self) -> bool {
        if let Some(msg) = self.messages.pop_front() {
            self.deliver(msg);
            true
        } else {
            false
        }
    }

    /// Deliver all pending messages.
    pub fn deliver_all(&mut self) {
        while let Some(msg) = self.messages.pop_front() {
            self.deliver(msg);
        }
    }

    /// Deliver a single message and queue any responses.
    fn deliver(&mut self, inflight: InFlight<Cmd>) {
        let to_index = self.node_index(inflight.to);
        if let Some(index) = to_index {
            let commands = self.runtimes[index]
                .handle(raft::Event::Message {
                    from: inflight.from,
                    message: inflight.message,
                })
                .unwrap_or_else(|e| match e {});
            self.queue_commands(index, commands);
        }
    }

    /// Queue outgoing commands from a node.
    fn queue_commands(&mut self, from_index: usize, commands: Vec<Command<Cmd>>) {
        let from_id = self.runtimes[from_index].node().id();
        for command in commands {
            if let Command::Send { to, message } = command {
                self.messages.push_back(InFlight {
                    from: from_id,
                    to,
                    message,
                });
            }
        }
    }

    /// Find runtime index by node ID.
    fn node_index(&self, id: NodeId) -> Option<usize> {
        self.runtimes.iter().position(|rt| rt.node().id() == id)
    }

    /// Find the current leader, if any.
    pub fn leader(&self) -> Option<usize> {
        self.runtimes
            .iter()
            .position(|rt| matches!(rt.node().role(), Role::Leader(_)))
    }

    /// Count nodes in each role.
    pub fn role_counts(&self) -> (usize, usize, usize) {
        let mut followers = 0;
        let mut candidates = 0;
        let mut leaders = 0;

        for rt in &self.runtimes {
            match rt.node().role() {
                Role::Follower(_) => followers += 1,
                Role::Candidate(_) => candidates += 1,
                Role::Leader(_) => leaders += 1,
            }
        }

        (followers, candidates, leaders)
    }
}

#[cfg(test)]
mod proptest_tests {
    use std::collections::HashMap;

    use proptest::prelude::*;
    use raft::Role;
    use raft::kv::KvCommand;
    use raft::kv::KvStore;
    use raft::types::LogIndex;

    use super::*;

    const N: usize = 3;

    #[derive(Debug, Clone)]
    enum Op {
        ElectionTimeout(usize),
        HeartbeatTimeout(usize),
        DeliverOne,
        DeliverAll,
        Submit { node: usize, key: u8, val: u8 },
    }

    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            // delivery weighted higher so the cluster has a chance to make progress
            3 => (0..N).prop_map(Op::ElectionTimeout),
            2 => (0..N).prop_map(Op::HeartbeatTimeout),
            5 => Just(Op::DeliverOne),
            3 => Just(Op::DeliverAll),
            2 => (0..N, 0u8..3u8, 0u8..3u8)
                .prop_map(|(n, k, v)| Op::Submit { node: n, key: k, val: v }),
        ]
    }

    fn apply(cluster: &mut Cluster<KvCommand, KvStore>, op: Op) {
        const KEYS: [&str; 3] = ["a", "b", "c"];
        const VALS: [&str; 3] = ["1", "2", "3"];
        match op {
            Op::ElectionTimeout(i) => cluster.election_timeout(i),
            Op::HeartbeatTimeout(i) => cluster.heartbeat_timeout(i),
            Op::DeliverOne => {
                cluster.deliver_one();
            }
            Op::DeliverAll => cluster.deliver_all(),
            Op::Submit { node, key, val } => {
                let _ = cluster.runtime_mut(node).submit(KvCommand::Set {
                    key: KEYS[key as usize].to_string(),
                    value: VALS[val as usize].to_string(),
                });
            }
        }
    }

    /// §5.2 Election Safety: at most one leader per term at any point in time.
    fn check_election_safety(cluster: &Cluster<KvCommand, KvStore>) {
        let mut leaders: HashMap<raft::types::Term, usize> = HashMap::new();
        for i in 0..N {
            let node = cluster.runtime(i).node();
            if matches!(node.role(), Role::Leader(_)) {
                let term = node.persistent().current_term();
                assert!(
                    leaders.insert(term, i).is_none(),
                    "election safety violated: two leaders in term {term}"
                );
            }
        }
    }

    /// §5.4.3 State Machine Safety: if two nodes have both committed up to some
    /// index, the committed entries at every position must be identical.
    fn check_state_machine_safety(cluster: &Cluster<KvCommand, KvStore>) {
        for i in 0..N {
            for j in (i + 1)..N {
                let ni = cluster.runtime(i).node();
                let nj = cluster.runtime(j).node();
                let min_commit =
                    std::cmp::min(ni.volatile().commit_index, nj.volatile().commit_index);

                let mut idx = LogIndex::from(1u64);
                while idx <= min_commit {
                    let ai = idx.to_array_index().unwrap();
                    let ei = ni.persistent().log().get(ai).unwrap_or_else(|| {
                        panic!(
                            "node {i} has commit_index {min_commit} \
                             but log only has {} entries",
                            ni.persistent().log().len()
                        )
                    });
                    let ej = nj.persistent().log().get(ai).unwrap_or_else(|| {
                        panic!(
                            "node {j} has commit_index {min_commit} \
                             but log only has {} entries",
                            nj.persistent().log().len()
                        )
                    });
                    assert_eq!(
                        (ei.term, &ei.payload),
                        (ej.term, &ej.payload),
                        "state machine safety violated at index {idx}: \
                         nodes {i} and {j} have different committed entries"
                    );
                    idx = idx.next();
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        #[test]
        fn safety_invariants_hold(ops in proptest::collection::vec(arb_op(), 1..=60)) {
            let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(N);
            for op in ops {
                apply(&mut cluster, op);
                check_election_safety(&cluster);
                check_state_machine_safety(&cluster);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use raft::kv::KvCommand;
    use raft::kv::KvResult;
    use raft::kv::KvStore;
    use raft::types::LogIndex;

    use super::*;

    #[test]
    fn single_node_becomes_leader() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(1);

        cluster.election_timeout(0);

        assert!(cluster.leader().is_some());
    }

    #[test]
    fn three_node_leader_election() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

        // Node 0 starts election.
        cluster.election_timeout(0);
        assert_eq!(cluster.role_counts(), (2, 1, 0));

        // Deliver vote requests and responses.
        cluster.deliver_all();

        // Node 0 should be leader.
        assert_eq!(cluster.leader(), Some(0));
        assert_eq!(cluster.role_counts(), (2, 0, 1));
    }

    #[test]
    fn leader_replicates_to_followers() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

        // Elect leader.
        cluster.election_timeout(0);
        cluster.deliver_all();
        assert_eq!(cluster.leader(), Some(0));

        // Submit command to leader (no-op is at index 1, command at index 2).
        let index = cluster.runtime_mut(0).submit(KvCommand::Set {
            key: "x".to_string(),
            value: "1".to_string(),
        });
        assert_eq!(index, Ok(LogIndex::from(2)));

        // Send heartbeats with new entries (no-op + command).
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // Verify all nodes have both entries (no-op + command).
        for i in 0..3 {
            assert_eq!(cluster.runtime(i).node().persistent().log().len(), 2);
        }

        // Verify leader committed and applied both entries.
        assert_eq!(
            cluster.runtime(0).node().volatile().commit_index,
            LogIndex::from(2)
        );

        // Verify state machine applied on leader.
        let result = cluster
            .runtime_mut(0)
            .state_machine_mut()
            .apply(KvCommand::Get {
                key: "x".to_string(),
            });
        assert_eq!(result, KvResult::Value(Some("1".to_string())));
    }

    #[test]
    fn followers_commit_on_leader_heartbeat() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

        // Elect leader and submit command.
        cluster.election_timeout(0);
        cluster.deliver_all();

        cluster
            .runtime_mut(0)
            .submit(KvCommand::Set {
                key: "y".to_string(),
                value: "2".to_string(),
            })
            .unwrap();

        // First heartbeat: replicate entry.
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // Second heartbeat: propagate commit index.
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // Verify followers committed (no-op at 1 + command at 2).
        for i in 1..3 {
            assert_eq!(
                cluster.runtime(i).node().volatile().commit_index,
                LogIndex::from(2)
            );
        }
    }
}
