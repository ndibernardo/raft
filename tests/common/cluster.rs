use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::net::SocketAddr;

use raft::Command;
use raft::MemoryStorage;
use raft::Node;
use raft::Role;
use raft::Runtime;
use raft::SnapshotPolicy;
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

/// A placeholder address for a simulated node. The harness routes messages in
/// memory by `NodeId` and never opens a socket, so only uniqueness matters.
fn dummy_addr(id_value: u64) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], (9000 + id_value) as u16))
}

/// A cluster simulated in one process, with `MemoryStorage` on every node.
///
/// Message delivery is explicit rather than concurrent: the harness queues what
/// each node emits and delivers it on demand, so a test can drive an exact
/// interleaving and reproduce it on every run.
pub struct Cluster<Cmd, S: StateMachine<Cmd>> {
    runtimes: Vec<Runtime<Cmd, S, MemoryStorage<Cmd>>>,
    messages: VecDeque<InFlight<Cmd>>,
    /// Nodes cut off from the rest of the cluster. Messages to or from them are
    /// dropped in flight until `heal_partition`.
    partitioned: HashSet<NodeId>,
    /// Applied to every node created by `with_snapshot_policy` and `add_node`,
    /// so a node joining later compacts on the same schedule as its peers.
    snapshot_policy: SnapshotPolicy,
    /// The configuration each node was created with, by index.
    ///
    /// `restart` passes this back to `Runtime::from_storage` as
    /// `initial_config`, the fallback a restarted node uses when neither its log
    /// nor its snapshot carries a `ConfigChange`.
    initial_configs: Vec<ClusterConfig>,
}

impl<Cmd: Clone, S: StateMachine<Cmd> + Default> Cluster<Cmd, S> {
    /// A cluster of `size` nodes with automatic compaction disabled.
    ///
    /// # Panics
    /// If `size` is 0, which is not a meaningful fixture.
    pub fn new(size: usize) -> Self {
        Self::with_snapshot_policy(size, SnapshotPolicy::default())
    }

    /// A cluster of `size` nodes, each compacting according to `snapshot_policy`.
    ///
    /// # Panics
    /// If `size` is 0, which is not a meaningful fixture.
    pub fn with_snapshot_policy(size: usize, snapshot_policy: SnapshotPolicy) -> Self {
        assert!(size > 0, "Cluster::new requires at least one node");
        let ids: Vec<NodeId> = (1..=size).map(|i| NodeId::from(i as u64)).collect();

        // Every node starts from the same full membership, as a cluster brought
        // up from a shared configuration file would.
        let members: HashMap<NodeId, SocketAddr> =
            ids.iter().map(|&id| (id, dummy_addr(id.value()))).collect();
        let config = match ClusterConfig::new(members) {
            Ok(config) => config,
            // The assertion above guarantees members is non-empty.
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
                    snapshot_policy,
                )
            })
            .collect();

        Self {
            runtimes,
            messages: VecDeque::new(),
            partitioned: HashSet::new(),
            snapshot_policy,
            initial_configs: ids.iter().map(|_| config.clone()).collect(),
        }
    }

    /// Adds a node to a running cluster, simulating a member that joins partway
    /// through a test.
    ///
    /// The new node's configuration contains only itself. That view is replaced
    /// as soon as the leader reaches it, since both AppendEntries and
    /// InstallSnapshot carry the real membership.
    pub fn add_node(&mut self, id_value: u64) {
        let id = NodeId::from(id_value);
        let solo_config = match ClusterConfig::new(HashMap::from([(id, dummy_addr(id_value))])) {
            Ok(config) => config,
            Err(_) => unreachable!("a single-member config is always valid"),
        };
        let node = Node::new(id, solo_config.clone());
        let runtime = Runtime::new(
            node,
            S::default(),
            MemoryStorage::new(),
            TimerConfig::default(),
            self.snapshot_policy,
        );
        self.runtimes.push(runtime);
        self.initial_configs.push(solo_config);
    }

    /// Crashes and restarts node `index`, rebuilding its runtime from its own
    /// storage through `Runtime::from_storage`, exactly as a process restart
    /// would. All volatile state is lost and the node comes back as a follower.
    ///
    /// The node keeps its position in `runtimes`, so callers can go on
    /// addressing it by the same index.
    ///
    /// # Panics
    /// If `from_storage` fails. For `KvStore` that requires a restore failure,
    /// which cannot happen for the reason given on `election_timeout`.
    #[allow(clippy::unwrap_used)]
    pub fn restart(&mut self, index: usize) {
        let old = self.runtimes.remove(index);
        let id = old.node().id();
        let storage = old.into_storage();
        let restored = Runtime::from_storage(
            id,
            self.initial_configs[index].clone(),
            S::default(),
            storage,
            TimerConfig::default(),
            self.snapshot_policy,
        )
        .unwrap();
        self.runtimes.insert(index, restored);
    }

    /// Cuts node `index` off from the cluster. Messages to or from it are
    /// dropped in flight, not queued, until `heal_partition`.
    pub fn partition(&mut self, index: usize) {
        self.partitioned.insert(self.runtimes[index].node().id());
    }

    /// Reconnects a previously partitioned node.
    pub fn heal_partition(&mut self, index: usize) {
        self.partitioned.remove(&self.runtimes[index].node().id());
    }

    /// A node's runtime, by zero-based index.
    pub fn runtime(&self, index: usize) -> &Runtime<Cmd, S, MemoryStorage<Cmd>> {
        &self.runtimes[index]
    }

    /// Mutable access to a node's runtime, by zero-based index.
    pub fn runtime_mut(&mut self, index: usize) -> &mut Runtime<Cmd, S, MemoryStorage<Cmd>> {
        &mut self.runtimes[index]
    }

    /// Fires the election timeout on node `index` and queues what it emits.
    // KvStore's snapshot and restore only serialize an in-memory
    // HashMap<String, String>, which never fails in practice, so RuntimeError is
    // unreachable here even when the snapshot policy compacts. Not provable, but
    // not worth a fallible harness API threaded through every test.
    #[allow(clippy::unwrap_used)]
    pub fn election_timeout(&mut self, index: usize) {
        let commands = self.runtimes[index]
            .handle(raft::Event::ElectionTimeout)
            .unwrap();
        self.queue_commands(index, commands);
    }

    /// Fires the heartbeat interval on node `index` and queues what it emits.
    #[allow(clippy::unwrap_used)]
    pub fn heartbeat_timeout(&mut self, index: usize) {
        let commands = self.runtimes[index]
            .handle(raft::Event::HeartbeatTimeout)
            .unwrap();
        self.queue_commands(index, commands);
    }

    /// Delivers the oldest queued message. Returns whether there was one.
    pub fn deliver_one(&mut self) -> bool {
        if let Some(msg) = self.messages.pop_front() {
            self.deliver(msg);
            true
        } else {
            false
        }
    }

    /// Delivers queued messages until none remain, including those generated by
    /// the deliveries themselves.
    pub fn deliver_all(&mut self) {
        while let Some(msg) = self.messages.pop_front() {
            self.deliver(msg);
        }
    }

    /// Delivers one message and queues whatever the recipient emits in response.
    ///
    /// A message is discarded, not deferred, when either endpoint is
    /// partitioned. A real network drops packets it cannot route rather than
    /// buffering them until the link returns.
    #[allow(clippy::unwrap_used)]
    fn deliver(&mut self, inflight: InFlight<Cmd>) {
        if self.partitioned.contains(&inflight.from) || self.partitioned.contains(&inflight.to) {
            return;
        }
        let to_index = self.node_index(inflight.to);
        if let Some(index) = to_index {
            let commands = self.runtimes[index]
                .handle(raft::Event::Message {
                    from: inflight.from,
                    message: inflight.message,
                })
                .unwrap();
            self.queue_commands(index, commands);
        }
    }

    /// Queues the `Send` commands a node emitted. Timer commands are irrelevant
    /// here, since the harness drives timeouts explicitly.
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

    /// Index of the node with `id`, or `None` if no such node exists.
    fn node_index(&self, id: NodeId) -> Option<usize> {
        self.runtimes.iter().position(|rt| rt.node().id() == id)
    }

    /// Index of a node currently believing itself leader, if any. Two nodes can
    /// hold that belief at once across different terms, in which case this
    /// returns the first.
    pub fn leader(&self) -> Option<usize> {
        self.runtimes
            .iter()
            .position(|rt| matches!(rt.node().role(), Role::Leader(_)))
    }

    /// Counts of followers, candidates, and leaders, in that order.
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
    use std::num::NonZeroUsize;

    use proptest::prelude::*;
    use raft::Role;
    use raft::SnapshotPolicy;
    use raft::kv::KvCommand;
    use raft::kv::KvStore;
    use raft::types::LogIndex;

    use super::*;

    const N: usize = 3;
    const KEYS: [&str; 3] = ["a", "b", "c"];

    /// One step a generated schedule can take against the cluster.
    #[derive(Debug, Clone)]
    enum Op {
        ElectionTimeout(usize),
        HeartbeatTimeout(usize),
        DeliverOne,
        DeliverAll,
        Submit { node: usize, key: u8, val: u8 },
        Partition(usize),
        HealPartition(usize),
        Restart(usize),
    }

    /// Generates operations with delivery weighted heavily, so that schedules
    /// actually reach a committed state instead of thrashing between elections.
    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0..N).prop_map(Op::ElectionTimeout),
            2 => (0..N).prop_map(Op::HeartbeatTimeout),
            5 => Just(Op::DeliverOne),
            3 => Just(Op::DeliverAll),
            2 => (0..N, 0u8..3u8, 0u8..3u8)
                .prop_map(|(n, k, v)| Op::Submit { node: n, key: k, val: v }),
            1 => (0..N).prop_map(Op::Partition),
            1 => (0..N).prop_map(Op::HealPartition),
            1 => (0..N).prop_map(Op::Restart),
        ]
    }

    fn apply(cluster: &mut Cluster<KvCommand, KvStore>, op: Op) {
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
            Op::Partition(i) => cluster.partition(i),
            Op::HealPartition(i) => cluster.heal_partition(i),
            Op::Restart(i) => cluster.restart(i),
        }
    }

    /// Election Safety (section 5.2): no term ever has two leaders. Nodes in
    /// different terms may both believe they lead, which is allowed.
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

    /// State Machine Safety (section 5.4.3): where two nodes have both committed
    /// through some index, their entries at every position up to it are equal.
    ///
    /// Compaction removes entries below a node's `first_index`, so a direct
    /// comparison is only possible across the overlap of the two retained
    /// ranges. The discarded prefix is covered by the snapshot itself, and the
    /// term check at the start of the overlap is what ties the two together.
    fn check_state_machine_safety(cluster: &Cluster<KvCommand, KvStore>) {
        for i in 0..N {
            for j in (i + 1)..N {
                let ni = cluster.runtime(i).node();
                let nj = cluster.runtime(j).node();
                let min_commit =
                    std::cmp::min(ni.volatile().commit_index, nj.volatile().commit_index);
                let overlap_start = std::cmp::max(
                    ni.persistent().log().first_index(),
                    nj.persistent().log().first_index(),
                );

                if let Some(boundary) = overlap_start.prev()
                    && boundary <= min_commit
                {
                    assert_eq!(
                        ni.persistent().log().term_at(boundary),
                        nj.persistent().log().term_at(boundary),
                        "state machine safety violated at compaction boundary {boundary}: \
                         nodes {i} and {j} disagree on the term of their common prefix"
                    );
                }

                let mut idx = overlap_start;
                while idx <= min_commit {
                    let ei = ni.persistent().log().entry(idx).unwrap_or_else(|| {
                        panic!(
                            "node {i} has commit_index {min_commit} \
                             but log only has {} entries",
                            ni.persistent().log().len()
                        )
                    });
                    let ej = nj.persistent().log().entry(idx).unwrap_or_else(|| {
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

    /// Two nodes that have applied through the same index hold the same values.
    ///
    /// This covers what compaction puts out of reach of
    /// `check_state_machine_safety`: once entries fall below a compaction
    /// boundary they cannot be compared in the log, but the state they produced
    /// is still observable through a read.
    fn check_applied_state_equivalence(cluster: &mut Cluster<KvCommand, KvStore>) {
        let applied: Vec<LogIndex> = (0..N)
            .map(|i| cluster.runtime(i).node().volatile().last_applied)
            .collect();
        for i in 0..N {
            for j in (i + 1)..N {
                if applied[i] != applied[j] || applied[i] == LogIndex::default() {
                    continue;
                }
                for key in KEYS {
                    let vi = cluster
                        .runtime_mut(i)
                        .state_machine_mut()
                        .apply(KvCommand::Get {
                            key: key.to_string(),
                        });
                    let vj = cluster
                        .runtime_mut(j)
                        .state_machine_mut()
                        .apply(KvCommand::Get {
                            key: key.to_string(),
                        });
                    assert_eq!(
                        vi, vj,
                        "nodes {i} and {j} both applied through index {:?} \
                         but disagree on key {key:?}",
                        applied[i]
                    );
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        #[test]
        fn safety_invariants_hold(ops in proptest::collection::vec(arb_op(), 1..=60)) {
            let mut cluster: Cluster<KvCommand, KvStore> = Cluster::with_snapshot_policy(
                N,
                SnapshotPolicy { compact_threshold: NonZeroUsize::new(2) },
            );
            for op in ops {
                apply(&mut cluster, op);
                check_election_safety(&cluster);
                check_state_machine_safety(&cluster);
                check_applied_state_equivalence(&mut cluster);
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

        cluster.election_timeout(0);
        assert_eq!(cluster.role_counts(), (2, 1, 0));

        // Carries the vote requests out and the responses back.
        cluster.deliver_all();

        assert_eq!(cluster.leader(), Some(0));
        assert_eq!(cluster.role_counts(), (2, 0, 1));
    }

    #[test]
    fn leader_replicates_to_followers() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

        cluster.election_timeout(0);
        cluster.deliver_all();
        assert_eq!(cluster.leader(), Some(0));

        // Index 1 holds the leader's no-op, so the command lands at index 2.
        let index = cluster.runtime_mut(0).submit(KvCommand::Set {
            key: "x".to_string(),
            value: "1".to_string(),
        });
        assert_eq!(index, Ok(LogIndex::from(2)));

        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // Both the no-op and the command reach every node.
        for i in 0..3 {
            assert_eq!(cluster.runtime(i).node().persistent().log().len(), 2);
        }

        assert_eq!(
            cluster.runtime(0).node().volatile().commit_index,
            LogIndex::from(2)
        );

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

        cluster.election_timeout(0);
        cluster.deliver_all();

        cluster
            .runtime_mut(0)
            .submit(KvCommand::Set {
                key: "y".to_string(),
                value: "2".to_string(),
            })
            .unwrap();

        // The first round replicates the entry, which is what lets the leader
        // commit it. Only the second round carries that commit index to the
        // followers.
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // The no-op at index 1 and the command at index 2 are both committed.
        for i in 1..3 {
            assert_eq!(
                cluster.runtime(i).node().volatile().commit_index,
                LogIndex::from(2)
            );
        }
    }
}
