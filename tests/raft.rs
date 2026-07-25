mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use common::Cluster;
use raft::Event;
use raft::FileStorage;
use raft::Node;
use raft::Role;
use raft::Runtime;
use raft::SnapshotPolicy;
use raft::StateMachine;
use raft::Storage;
use raft::TimerConfig;
use raft::kv::KvCommand;
use raft::kv::KvResult;
use raft::kv::KvStore;
use raft::types::AppendEntries;
use raft::types::AppendEntriesResponse;
use raft::types::ClusterConfig;
use raft::types::InstallSnapshot;
use raft::types::LogEntry;
use raft::types::LogIndex;
use raft::types::LogPayload;
use raft::types::Message;
use raft::types::NodeId;
use raft::types::RequestVoteResponse;
use raft::types::Snapshot;
use raft::types::SnapshotData;
use raft::types::SnapshotMeta;
use raft::types::Term;
use raft::types::Vote;

fn set(key: &str, value: &str) -> KvCommand {
    KvCommand::Set {
        key: key.to_string(),
        value: value.to_string(),
    }
}

/// Dummy dial-in address — none of these tests exchange real bytes over a socket.
fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
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

    assert_eq!(
        cluster.leader(),
        Some(0),
        "single node must win its own election"
    );
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

    assert_eq!(
        cluster.leader(),
        Some(1),
        "node 1 must win the new election"
    );
    assert!(
        matches!(cluster.runtime(0).node().role(), Role::Follower(_)),
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

    let leader_id = cluster.runtime(0).node().id();
    for i in 1..3 {
        if let Role::Follower(f) = cluster.runtime(i).node().role() {
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

    cluster
        .runtime_mut(0)
        .submit(set("city", "amsterdam"))
        .unwrap();

    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    let result = cluster
        .runtime_mut(0)
        .state_machine_mut()
        .apply(KvCommand::Get {
            key: "city".to_string(),
        });
    assert_eq!(result, KvResult::Value(Some("amsterdam".to_string())));
}

/// A submitted command produces no output until a majority acknowledges it.
#[test]
fn outputs_empty_before_commit() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster
        .runtime_mut(0)
        .submit(set("pending", "true"))
        .unwrap();

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
    assert!(matches!(
        cluster.runtime(0).node().role(),
        Role::Follower(_)
    ));
    assert!(matches!(
        cluster.runtime(1).node().role(),
        Role::Follower(_)
    ));
}

/// Leader appends a no-op on election (§8) plus the submitted command.
/// After one heartbeat round-trip all nodes must have both entries.
#[test]
fn command_replicated_to_all_followers_after_one_heartbeat() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    cluster
        .runtime_mut(0)
        .submit(set("hostname", "node-alpha"))
        .unwrap();

    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for i in 0..3 {
        assert_eq!(
            cluster.runtime(i).node().persistent().log().len(),
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

    cluster
        .runtime_mut(0)
        .submit(set("region", "eu-west-1"))
        .unwrap();

    // First heartbeat: replicate entries; leader commits (majority = self + one ack).
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert_eq!(
        cluster.runtime(0).node().volatile().commit_index,
        LogIndex::from(2),
        "leader must have committed both entries"
    );

    // Second heartbeat: propagate commit_index to followers.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for i in 1..3 {
        assert_eq!(
            cluster.runtime(i).node().volatile().commit_index,
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
        cluster.runtime(1).node().volatile().commit_index,
        index.unwrap(),
        "new leader must commit its own command"
    );
}

/// A follower partitioned before the leader ever compacts falls behind the
/// compacted prefix entirely. Once healed, its `next_index` can no longer be
/// satisfied by `AppendEntries` — the entries it needs are gone — so the leader
/// must fall back to `InstallSnapshot`, and the follower must end up with the
/// same KV state as the leader despite never having replayed the compacted log.
#[test]
fn lagging_follower_catches_up_via_install_snapshot() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::with_snapshot_policy(
        3,
        SnapshotPolicy {
            compact_threshold: NonZeroUsize::new(2),
        },
    );
    elect(&mut cluster, 0);

    // Node 2 falls behind before it receives even the leader's no-op.
    cluster.partition(2);

    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        cluster.runtime_mut(0).submit(set(key, value)).unwrap();
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();
    }
    // Propagate the trailing commit index to node 1.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert!(
        cluster
            .runtime(0)
            .node()
            .persistent()
            .log()
            .snapshot_last_index()
            > LogIndex::from(0),
        "leader must have compacted past the threshold while node 2 was partitioned"
    );

    cluster.heal_partition(2);
    // Leader's next_index for node 2 is still 1 — below the compacted boundary — so
    // this heartbeat must send InstallSnapshot instead of AppendEntries.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();
    // A second round in case any entries trailing the last compaction still need
    // ordinary replication after the snapshot lands.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for key in ["a", "b", "c", "d"] {
        let expected = cluster
            .runtime_mut(0)
            .state_machine_mut()
            .apply(KvCommand::Get {
                key: key.to_string(),
            });
        let actual = cluster
            .runtime_mut(2)
            .state_machine_mut()
            .apply(KvCommand::Get {
                key: key.to_string(),
            });
        assert_eq!(
            actual, expected,
            "node 2 must converge to the leader's value for {key:?} via InstallSnapshot"
        );
    }
    assert!(
        cluster.runtime(2).node().persistent().log().first_index() > LogIndex::from(1),
        "node 2's log must start after the installed snapshot boundary, not at index 1"
    );
}

/// A single node's committed-and-compacted state, plus an uncommitted suffix
/// appended just before "crash," must both survive a restart from `FileStorage`.
#[test]
fn restarted_node_recovers_from_snapshot_plus_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let config = ClusterConfig::new(HashMap::from([
        (NodeId::from(1), addr(19101)),
        (NodeId::from(2), addr(19102)),
    ]))
    .unwrap();

    let mut rt: Runtime<KvCommand, KvStore, FileStorage<KvCommand>> = Runtime::new(
        Node::new(NodeId::from(1), config.clone()),
        KvStore::new(),
        FileStorage::open(dir.path()).unwrap(),
        TimerConfig::default(),
        SnapshotPolicy {
            compact_threshold: NonZeroUsize::new(1),
        },
    );

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

    // Committed and compacted (threshold 1) via a simulated ack from peer 2.
    let index = rt.submit(set("region", "eu-west-1")).unwrap();
    rt.handle(Event::Message {
        from: NodeId::from(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
            term: Term::from(1),
            match_index: index,
        }),
    })
    .unwrap();
    assert!(
        rt.node().persistent().log().snapshot_last_index() > LogIndex::from(0),
        "threshold of 1 must have compacted after the first commit"
    );

    // Appended and persisted, but never acked — still uncommitted at "crash" time.
    rt.submit(set("region", "us-east-1")).unwrap();
    rt.handle(Event::HeartbeatTimeout).unwrap();

    // "Crash": drop the handle, then reopen the same data directory as a fresh restart.
    drop(rt);
    let mut restored = Runtime::from_storage(
        NodeId::from(1),
        config,
        KvStore::new(),
        FileStorage::open(dir.path()).unwrap(),
        TimerConfig::default(),
        SnapshotPolicy::default(),
    )
    .unwrap();

    assert!(
        restored
            .node()
            .persistent()
            .log()
            .entry(LogIndex::from(1))
            .is_none(),
        "compacted entries must not be replayable from the log after restart"
    );
    assert_eq!(
        restored.state_machine_mut().apply(KvCommand::Get {
            key: "region".to_string()
        }),
        KvResult::Value(Some("eu-west-1".to_string())),
        "restored state machine must reflect the committed snapshot, \
         not the uncommitted suffix written just before the crash"
    );
}

/// Discard-case `InstallSnapshot` (boundary conflicts with the local log) must
/// clear the whole stale suffix from durable storage, not just the compacted
/// prefix — otherwise a later append lands after leftover entries the node
/// itself discarded, and node and storage disagree about the log on restart.
#[test]
fn restarted_node_discards_stale_suffix_after_conflicting_install_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let config = ClusterConfig::new(HashMap::from([
        (NodeId::from(1), addr(19201)),
        (NodeId::from(2), addr(19202)),
        (NodeId::from(3), addr(19203)),
    ]))
    .unwrap();

    let mut rt: Runtime<KvCommand, KvStore, FileStorage<KvCommand>> = Runtime::new(
        Node::new(NodeId::from(1), config.clone()),
        KvStore::new(),
        FileStorage::open(dir.path()).unwrap(),
        TimerConfig::default(),
        SnapshotPolicy::default(),
    );

    // Old leader (node 3, term 1) replicates 3 entries; none commit.
    let entries: Vec<LogEntry<KvCommand>> = ["a", "b", "c"]
        .iter()
        .map(|k| LogEntry {
            term: Term::from(1),
            payload: LogPayload::Command(set(k, "1")),
        })
        .collect();
    rt.handle(Event::Message {
        from: NodeId::from(3),
        message: Message::AppendEntries(AppendEntries {
            term: Term::from(1),
            leader_id: NodeId::from(3),
            prev_log_index: LogIndex::from(0),
            prev_log_term: Term::from(0),
            entries,
            leader_commit: LogIndex::from(0),
        }),
    })
    .unwrap();

    // New leader (node 2, term 9) installs a snapshot at index 2, term 9 —
    // conflicts with our term-1 entry at 2, so the whole log must be discarded.
    let mut source = KvStore::new();
    source.apply(set("a", "leader"));
    let data: SnapshotData = StateMachine::snapshot(&source).unwrap();
    rt.handle(Event::Message {
        from: NodeId::from(2),
        message: Message::InstallSnapshot(InstallSnapshot {
            term: Term::from(9),
            leader_id: NodeId::from(2),
            snapshot: Snapshot {
                meta: SnapshotMeta {
                    last_index: LogIndex::from(2),
                    last_term: Term::from(9),
                    config: config.clone(),
                },
                data,
            },
        }),
    })
    .unwrap();
    assert_eq!(rt.node().persistent().log().len(), 0);

    // New leader replicates a fresh entry at index 3 (term 9); node acks it.
    rt.handle(Event::Message {
        from: NodeId::from(2),
        message: Message::AppendEntries(AppendEntries {
            term: Term::from(9),
            leader_id: NodeId::from(2),
            prev_log_index: LogIndex::from(2),
            prev_log_term: Term::from(9),
            entries: vec![LogEntry {
                term: Term::from(9),
                payload: LogPayload::Command(set("d", "fresh")),
            }],
            leader_commit: LogIndex::from(2),
        }),
    })
    .unwrap();

    // "Crash": reopen the same data directory and inspect the durable log.
    drop(rt);
    let reopened: FileStorage<KvCommand> = FileStorage::open(dir.path()).unwrap();
    let loaded = reopened.load().unwrap();
    assert_eq!(
        loaded.entries.len(),
        1,
        "storage must hold exactly the one fresh entry after the boundary; \
         a stale discarded entry surviving here diverges node and storage"
    );
    assert_eq!(loaded.entries[0].term, Term::from(9));
}

/// A membership change compacted away by the time a lagging node catches up must
/// still be learned — from the installed snapshot's config, since the log entry
/// that carried it no longer exists.
#[test]
fn snapshot_preserves_membership_config() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::with_snapshot_policy(
        3,
        SnapshotPolicy {
            compact_threshold: NonZeroUsize::new(1),
        },
    );
    elect(&mut cluster, 0);

    let new_member = NodeId::from(4);
    cluster.add_node(4);

    let new_config = cluster
        .runtime(0)
        .node()
        .config()
        .with_member(new_member, addr(19104));
    let config_change_index = cluster
        .runtime_mut(0)
        .submit_config_change(new_config)
        .unwrap();
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    // Node 2 falls behind starting now, before the config change is compacted away.
    cluster.partition(2);

    for (key, value) in [("x", "1"), ("y", "2"), ("z", "3")] {
        cluster.runtime_mut(0).submit(set(key, value)).unwrap();
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();
    }
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert!(
        cluster
            .runtime(0)
            .node()
            .persistent()
            .log()
            .snapshot_last_index()
            > config_change_index,
        "the config-change entry at index {config_change_index:?} must have been compacted away"
    );

    cluster.heal_partition(2);
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert!(
        cluster.runtime(2).node().config().contains(new_member),
        "node 2 must learn the new member from the installed snapshot's config, \
         since the config-change log entry itself was compacted away before it caught up"
    );
}

/// Election safety must still hold once every node's log has been fully
/// compacted — the vote comparison in this case relies entirely on the
/// snapshot boundary's index/term, since `entries` is empty on every node.
#[test]
fn leader_election_works_when_all_logs_fully_compacted() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::with_snapshot_policy(
        3,
        SnapshotPolicy {
            compact_threshold: NonZeroUsize::new(1),
        },
    );
    elect(&mut cluster, 0);

    cluster.runtime_mut(0).submit(set("x", "1")).unwrap();
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();
    // Second round propagates commit_index to followers so they apply and compact too.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for i in 0..3 {
        assert_eq!(
            cluster.runtime(i).node().persistent().log().len(),
            0,
            "node {i}'s log must be fully compacted, with nothing left to replay"
        );
    }

    // Simulate leader failure: node 1 times out and starts a new election.
    elect(&mut cluster, 1);

    assert_eq!(
        cluster.leader(),
        Some(1),
        "election must still succeed once every log is fully compacted"
    );
}
