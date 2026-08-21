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
use raft::types::Membership;
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

/// A placeholder address. No test here opens a socket, so only uniqueness
/// within a configuration matters.
fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// Drives node `i` through an election and delivers everything it produces.
fn elect(cluster: &mut Cluster<KvCommand, KvStore>, i: usize) {
    cluster.election_timeout(i);
    cluster.deliver_all();
}

/// A single-node cluster reaches leadership on the election timeout alone. Its
/// own vote is already a majority, so no message is exchanged.
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

/// A lone leader commits and applies without any peer traffic. Its own log is
/// the whole quorum, so nothing will ever acknowledge on its behalf.
#[test]
fn single_node_applies_a_command_with_no_peer_to_acknowledge_it() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(1);
    cluster.election_timeout(0);

    cluster
        .runtime_mut(0)
        .submit(set("city", "amsterdam"))
        .unwrap();
    // The runtime applies committed entries while handling an event. No peer
    // will ever send one here, so the heartbeat tick is what pumps the apply
    // loop, exactly as the server loop drives it.
    cluster.heartbeat_timeout(0);

    let outputs = cluster.runtime_mut(0).take_outputs();
    assert_eq!(
        outputs.len(),
        1,
        "the command commits on the local append alone"
    );

    let result = cluster
        .runtime_mut(0)
        .state_machine_mut()
        .apply(KvCommand::Get {
            key: "city".to_string(),
        });
    assert_eq!(result, KvResult::Value(Some("amsterdam".to_string())));
}

/// What a lone leader committed survives a restart, and is restored from the
/// snapshot the commit made compactable.
#[test]
fn single_node_recovers_what_it_committed_alone_across_a_restart() {
    let policy = SnapshotPolicy {
        compact_threshold: NonZeroUsize::new(2),
    };
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::with_snapshot_policy(1, policy);
    cluster.election_timeout(0);

    for (key, value) in [("city", "amsterdam"), ("country", "netherlands")] {
        cluster.runtime_mut(0).submit(set(key, value)).unwrap();
        cluster.heartbeat_timeout(0);
        cluster.runtime_mut(0).take_outputs();
    }

    cluster.restart(0);
    // commit_index is volatile and comes back at the snapshot boundary, so the
    // recovered suffix is durable but uncommitted. Only a new leader commits it,
    // and on a lone node nothing but its own append can.
    cluster.election_timeout(0);

    let result = cluster
        .runtime_mut(0)
        .state_machine_mut()
        .apply(KvCommand::Get {
            key: "country".to_string(),
        });
    assert_eq!(
        result,
        KvResult::Value(Some("netherlands".to_string())),
        "a restart must not lose entries that committed without peer acks"
    );
}

/// A lone leader commits its own membership change, so growing a one-node
/// cluster is not blocked on the member it is about to add.
#[test]
fn single_node_commits_the_config_change_that_adds_a_second_member() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(1);
    cluster.election_timeout(0);

    let members: HashMap<NodeId, SocketAddr> =
        [(NodeId::from(1), addr(9001))].into_iter().collect();
    let index = cluster
        .runtime_mut(0)
        .submit_config_change(ClusterConfig::new(members).unwrap())
        .unwrap();

    assert_eq!(index, LogIndex::from(2));
    assert_eq!(
        cluster.runtime(0).node().volatile().commit_index,
        LogIndex::from(2),
        "the sole member is the quorum for its own change"
    );
}

/// A three-node election: one candidate and two voters, of whom one suffices
/// for a majority.
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

/// In a five-node cluster a candidate needs three votes: its own plus two peers.
#[test]
fn five_node_cluster_elects_with_majority() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(5);

    elect(&mut cluster, 2);

    assert_eq!(cluster.leader(), Some(2));
    assert_eq!(cluster.role_counts(), (4, 0, 1));
}

/// A follower that times out while a leader is active starts an election in a
/// higher term. The leader sees that term in the vote request, steps down, and
/// the candidate wins.
#[test]
fn re_election_after_leader_steps_down() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

    elect(&mut cluster, 0);
    assert_eq!(cluster.leader(), Some(0));

    // Node 1 stands for election in term 2. Node 0 sees the higher term in the
    // vote request, steps down, and grants the vote.
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

/// Leadership transfers across three consecutive terms without losing safety.
///
/// Each leader replicates its no-op before the next election because the
/// up-to-date check of section 5.4.1 denies a vote to a candidate with a stale
/// log. Skipping a replication round would leave the next candidate behind and
/// its RequestVote refused.
#[test]
fn three_consecutive_re_elections() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

    elect(&mut cluster, 0);
    cluster.heartbeat_timeout(0); // replicates the term 1 no-op
    cluster.deliver_all();

    elect(&mut cluster, 1);
    cluster.heartbeat_timeout(1); // replicates the term 2 no-op
    cluster.deliver_all();

    elect(&mut cluster, 2); // node 2's log is current, so it wins term 3

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

/// A new leader appends a no-op (section 8) and then the submitted command. One
/// heartbeat round trip carries both to every node.
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
            2, // the no-op at index 1 and the command at index 2
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

    // The first round replicates the entries. One acknowledgement plus the
    // leader's own copy is a majority of three, so the leader commits.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert_eq!(
        cluster.runtime(0).node().volatile().commit_index,
        LogIndex::from(2),
        "leader must have committed both entries"
    );

    // The second round carries that commit index to the followers.
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

    // Indices 2 and 3 carry the commands; index 1 is the no-op, which produces
    // no output.
    let outputs = cluster.runtime_mut(0).take_outputs();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0], (Term::from(1), LogIndex::from(2), KvResult::Ok));
    assert_eq!(outputs[1], (Term::from(1), LogIndex::from(3), KvResult::Ok));
}

/// A follower refuses a client command, so the client can be redirected to the
/// leader instead of silently writing to a node that cannot replicate.
#[test]
fn submit_to_follower_returns_none() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);

    let index = cluster.runtime_mut(1).submit(set("key", "value"));
    assert!(index.is_err(), "follower must reject client commands");
}

/// After re-election the new leader can still accept and commit commands.
#[test]
fn new_leader_accepts_commands_after_re_election() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);
    elect(&mut cluster, 1); // node 1 takes over in term 2

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
/// entire compacted prefix.
///
/// Once healed, its `next_index` points at entries the leader no longer holds,
/// so AppendEntries cannot serve it and the leader must send a snapshot instead.
/// The follower has to reach the leader's state without ever replaying the
/// compacted log.
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
    // Carries the last commit index to node 1, which lets it compact too.
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
    // The leader's next_index for node 2 is still 1, below the compacted
    // boundary, so this round must send InstallSnapshot rather than
    // AppendEntries.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();
    // A second round replicates any entries appended after the last compaction,
    // which the snapshot does not cover.
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

/// Both halves of a node's durable state survive a restart from `FileStorage`:
/// the committed prefix folded into a snapshot, and the uncommitted suffix
/// appended just before the crash.
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

    // A simulated acknowledgement from peer 2 commits the entry, and the
    // threshold of 1 compacts it immediately.
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

    // Appended and persisted but never acknowledged, so it is still uncommitted
    // when the crash happens.
    rt.submit(set("region", "us-east-1")).unwrap();
    rt.handle(Event::HeartbeatTimeout).unwrap();

    // Dropping the runtime and reopening the same directory is the crash.
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

/// An `InstallSnapshot` whose boundary conflicts with the local log must clear
/// the whole stale suffix from durable storage, not only the compacted prefix.
///
/// Otherwise a later append lands after entries the node itself discarded, and
/// the log on disk no longer matches the one in memory once the node restarts.
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

    // A term 1 leader replicates three entries, none of which commit.
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

    // A term 9 leader installs a snapshot whose boundary at index 2 claims term
    // 9, conflicting with the local term 1 entry there. The whole log goes.
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

    // The new leader then replicates a fresh term 9 entry at index 3.
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

    // Reopening the same directory shows what the crash would have left behind.
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

/// A lagging node still learns a membership change whose log entry was compacted
/// away before it caught up, because the snapshot carries the configuration that
/// entry established.
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

    // Node 2 stops receiving anything from here on, while the config change is
    // still in the log but before compaction removes it.
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

/// An election still succeeds once every node's log is fully compacted.
///
/// With no entries retained anywhere, the up-to-date comparison has nothing to
/// read from the log and rests entirely on the index and term preserved at the
/// snapshot boundary.
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
    // The second round carries the commit index to the followers, so they apply
    // and compact as well.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for i in 0..3 {
        assert_eq!(
            cluster.runtime(i).node().persistent().log().len(),
            0,
            "node {i}'s log must be fully compacted, with nothing left to replay"
        );
    }

    // The leader is presumed lost, so node 1 times out and stands for election.
    elect(&mut cluster, 1);

    assert_eq!(
        cluster.leader(),
        Some(1),
        "election must still succeed once every log is fully compacted"
    );
}

/// A leader that removes itself keeps replicating until the removal commits,
/// then steps down and leaves the remaining members able to elect a successor
/// on their own (dissertation section 4.2.2).
#[test]
fn a_leader_that_removes_itself_steps_down_and_the_rest_carry_on() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    let leader_id = cluster.runtime(0).node().id();
    let without_leader = cluster
        .runtime(0)
        .node()
        .config()
        .without_member(leader_id)
        .unwrap();

    cluster
        .runtime_mut(0)
        .submit_config_change(without_leader)
        .unwrap();
    assert_eq!(
        cluster.runtime(0).node().local_membership(),
        Membership::NonMember
    );
    assert!(
        matches!(cluster.runtime(0).node().role(), Role::Leader(_)),
        "the removal has not committed, so it must keep replicating it"
    );

    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert!(
        matches!(cluster.runtime(0).node().role(), Role::Follower(_)),
        "once the removal commits the leader must step down"
    );

    // The two remaining members are a cluster of their own and can elect a
    // leader without the removed node's vote.
    elect(&mut cluster, 1);
    assert_eq!(cluster.leader(), Some(1));
}

/// After a removal commits, the remaining members are the whole cluster: they
/// commit on their own quorum, and the removed node stops receiving entries.
///
/// The removed node is dropped from replication the moment the change is
/// appended, so it never learns of its own removal. Stopping it from disrupting
/// the cluster it still believes it belongs to needs the minimum-election-timeout
/// rule of dissertation section 4.2.3, which is not implemented here.
#[test]
fn a_removed_follower_stops_receiving_entries_while_the_rest_commit() {
    let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);
    elect(&mut cluster, 0);
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    let removed_id = cluster.runtime(2).node().id();
    let without_node = cluster
        .runtime(0)
        .node()
        .config()
        .without_member(removed_id)
        .unwrap();
    cluster
        .runtime_mut(0)
        .submit_config_change(without_node)
        .unwrap();
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    let removed_last_index = cluster.runtime(2).node().persistent().log().last_index();

    cluster
        .runtime_mut(0)
        .submit(set("city", "amsterdam"))
        .unwrap();
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    assert_eq!(
        cluster.runtime(2).node().persistent().log().last_index(),
        removed_last_index,
        "a removed member must no longer receive replication"
    );

    // One more round so the follower learns the advanced commit index.
    cluster.heartbeat_timeout(0);
    cluster.deliver_all();

    for i in [0, 1] {
        let result = cluster
            .runtime_mut(i)
            .state_machine_mut()
            .apply(KvCommand::Get {
                key: "city".to_string(),
            });
        assert_eq!(
            result,
            KvResult::Value(Some("amsterdam".to_string())),
            "node {i} is part of the quorum of the remaining configuration"
        );
    }
    assert_eq!(cluster.leader(), Some(0));
}
