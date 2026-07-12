use raft::cluster::Cluster;
use raft::kv::{KvCommand, KvResult, KvStore};
use raft::node::Role;
use raft::types::{LogIndex, Term};

fn set(key: &str, value: &str) -> KvCommand {
    KvCommand::Set { key: key.to_string(), value: value.to_string() }
}

/// Drive node `i` to win an election and deliver all resulting messages.
fn elect(cluster: &mut Cluster<KvCommand, KvStore>, i: usize) {
    cluster.election_timeout(i);
    cluster.deliver_all();
}

/// A single-node cluster needs no peers — it transitions to leader immediately
/// on election timeout without any message exchange.
#[test]
fn single_node_becomes_leader_without_messages() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(1);

    cluster.election_timeout(0);

    assert_eq!(cluster.leader(), Some(0), "single node must win its own election");
    assert_eq!(cluster.role_counts(), (0, 0, 1));
}

/// Standard 3-node election: one candidate, two voters, majority achieved.
#[test]
fn three_node_cluster_elects_single_leader() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

    elect(&mut cluster, 0);

    assert_eq!(cluster.leader(), Some(0));
    let (followers, candidates, leaders) = cluster.role_counts();
    assert_eq!(leaders, 1, "exactly one leader");
    assert_eq!(candidates, 0, "no lingering candidates");
    assert_eq!(followers, 2);
}

/// 5-node cluster: candidate needs 3/5 votes (self + 2 peers).
#[test]
fn five_node_cluster_elects_with_majority() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(5);

    elect(&mut cluster, 2);

    assert_eq!(cluster.leader(), Some(2));
    assert_eq!(cluster.role_counts(), (4, 0, 1));
}

/// When a follower times out while a leader is active, it starts a new election
/// in a higher term. The current leader steps down (hears term N+1 in the vote
/// request) and the new candidate wins.
#[test]
fn re_election_after_leader_steps_down() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

    // Node 0 wins term 1.
    elect(&mut cluster, 0);
    assert_eq!(cluster.leader(), Some(0));

    // Node 1 times out → starts election in term 2.
    // Node 0 receives the higher-term RequestVote, steps down, grants vote.
    elect(&mut cluster, 1);

    assert_eq!(cluster.leader(), Some(1), "node 1 must win the new election");
    assert!(
        matches!(cluster.runtime(0).node().role, Role::Follower(_)),
        "former leader must be a follower"
    );
}

/// After the first heartbeat, followers learn who the current leader is.
#[test]
fn followers_track_leader_identity() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    let leader_id = cluster.runtime(0).node().id;
    for i in 1..3 {
        if let Role::Follower(f) = &cluster.runtime(i).node().role {
            assert_eq!(
                f.leader_id(),
                Some(leader_id),
                "follower {i} must track the current leader"
            );
        }
    }
}

/// Once a command is committed and applied, the state machine reflects it.
#[test]
fn state_machine_reflects_committed_command() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster.runtime_mut(0).submit(set("city", "amsterdam")).unwrap();

    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    let result = cluster
        .runtime_mut(0)
        .state_machine_mut()
        .apply(KvCommand::Get { key: "city".to_string() });
    assert_eq!(result, KvResult::Value(Some("amsterdam".to_string())));
}

/// A submitted command produces no output until a majority acknowledges it.
#[test]
fn outputs_empty_before_commit() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster.runtime_mut(0).submit(set("pending", "true")).unwrap();

    assert!(
        cluster.runtime_mut(0).take_outputs().is_empty(),
        "command must not be applied before majority ack"
    );
}

/// Leadership can transfer across three consecutive terms without losing safety.
/// Each leader must replicate its no-op before the next election: §5.4.1 prevents
/// a node with a stale log from winning a vote, so skipping replication would
/// cause the next candidate's RequestVote to be denied.
#[test]
fn three_consecutive_re_elections() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

    elect(&mut cluster, 0);
    cluster.heartbeat_timeout(0); // replicate term-1 no-op to all nodes
    cluster.deliver_all();

    elect(&mut cluster, 1);
    cluster.heartbeat_timeout(1); // replicate term-2 no-op to all nodes
    cluster.deliver_all();

    elect(&mut cluster, 2); // node 2 now has an up-to-date log → wins term 3

    assert_eq!(cluster.leader(), Some(2));
    assert_eq!(cluster.role_counts(), (2, 0, 1));
    assert!(matches!(cluster.runtime(0).node().role, Role::Follower(_)));
    assert!(matches!(cluster.runtime(1).node().role, Role::Follower(_)));
}

/// Leader appends a no-op on election (§8) plus the submitted command.
/// After one heartbeat round-trip all nodes must have both entries.
#[test]
fn command_replicated_to_all_followers_after_one_heartbeat() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster.runtime_mut(0).submit(set("hostname", "node-alpha")).unwrap();

    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for i in 0..3 {
        assert_eq!(
            cluster.runtime(i).node().persistent.log.len(),
            2, // no-op at index 1, command at index 2
            "node {i} must have both log entries"
        );
    }
}

/// The leader commits once a majority acknowledges. A second heartbeat carries
/// the updated commit_index to followers so they can apply the entry too.
#[test]
fn command_committed_and_applied_on_all_nodes() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster.runtime_mut(0).submit(set("region", "eu-west-1")).unwrap();

    // First heartbeat: replicate entries; leader commits (majority = self + one ack).
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert_eq!(
        cluster.runtime(0).node().volatile.commit_index,
        LogIndex::from(2),
        "leader must have committed both entries"
    );

    // Second heartbeat: propagate commit_index to followers.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for i in 1..3 {
        assert_eq!(
            cluster.runtime(i).node().volatile.commit_index,
            LogIndex::from(2),
            "follower {i} must have received the updated commit index"
        );
    }
}

/// Multiple commands are replicated and applied in submission order.
#[test]
fn multiple_commands_applied_in_order() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster.runtime_mut(0).submit(set("user", "miles")).unwrap();
    cluster.runtime_mut(0).submit(set("role", "admin")).unwrap();

    cluster.heartbeat_timeout(0);
    cluster.deliver_all();
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    // Drain applied outputs from the leader (indices 2 and 3; index 1 is no-op).
    let outputs = cluster.runtime_mut(0).take_outputs();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0], (Term::from(1), LogIndex::from(2), KvResult::Ok));
    assert_eq!(outputs[1], (Term::from(1), LogIndex::from(3), KvResult::Ok));
}

/// Submitting to a non-leader returns Err — the client must redirect.
#[test]
fn submit_to_follower_returns_none() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    // Node 1 is a follower.
    let index = cluster.runtime_mut(1).submit(set("key", "value"));
    assert!(index.is_err(), "follower must reject client commands");
}

/// After re-election the new leader can still accept and commit commands.
#[test]
fn new_leader_accepts_commands_after_re_election() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);
    elect(&mut cluster, 1); // node 1 wins term 2

    assert_eq!(cluster.leader(), Some(1));

    let index = cluster.runtime_mut(1).submit(set("datacenter", "ams1"));
    assert!(index.is_ok(), "new leader must accept commands");

    cluster.heartbeat_timeout(1);
    cluster.deliver_all();
    cluster.heartbeat_timeout(1);
    cluster.deliver_all();

    assert_eq!(
        cluster.runtime(1).node().volatile.commit_index,
        index.unwrap(),
        "new leader must commit its own command"
    );
}
