use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;

use crate::app::runtime::Event;
use crate::app::runtime::Runtime;
use crate::app::runtime::RuntimeError;
use crate::app::runtime::SnapshotPolicy;
use crate::app::runtime::StateMachine;
use crate::app::runtime::TimerConfig;
use crate::app::transport::Transport;
use crate::app::transport::TransportError;
use crate::core::command::Command;
use crate::core::node::NotLeaderError;
use crate::core::node::SubmitError;
use crate::core::types::ClusterConfig;
use crate::core::types::ConfigError;
use crate::core::types::LogIndex;
use crate::core::types::NodeId;
use crate::core::types::Term;
use crate::storage::file::FileStorage;
use crate::storage::file::FileStorageError;

/// Why the server could not start or could not continue running.
#[derive(Debug, thiserror::Error)]
pub enum ServerError<SmErr: std::error::Error> {
    #[error("storage: {0}")]
    Storage(#[from] FileStorageError),
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
    #[error("config: {0}")]
    Config(#[from] ConfigError),
    #[error("state machine: {0}")]
    StateMachine(SmErr),
}

impl<SmErr: std::error::Error> From<RuntimeError<FileStorageError, SmErr>> for ServerError<SmErr> {
    fn from(err: RuntimeError<FileStorageError, SmErr>) -> Self {
        match err {
            RuntimeError::Storage(e) => Self::Storage(e),
            RuntimeError::StateMachine(e) => Self::StateMachine(e),
        }
    }
}

/// Outcome of a submitted client command, returned over its response channel.
#[derive(Debug)]
pub enum ApiResponse<Output> {
    /// The command committed and was applied, yielding this output.
    Result(Output),
    /// This node cannot accept writes. The hint names where to retry.
    NotLeader { leader_hint: Option<NodeId> },
}

/// A submitted command paired with the channel its result is returned on.
pub type Pending<Cmd, Output> = (Cmd, oneshot::Sender<ApiResponse<Output>>);

/// A requested change to cluster membership. One server at a time, as
/// dissertation section 4.1 requires.
pub enum MembershipRequest {
    Add { id: NodeId, addr: SocketAddr },
    Remove { id: NodeId },
}

/// Outcome of a membership change.
pub enum MembershipResult {
    /// The change committed.
    Ok,
    /// This node is not the leader.
    NotLeader,
    /// The change was refused: another one is still uncommitted, or it would
    /// have emptied the cluster.
    Rejected,
    /// Leadership changed before the change committed. The next leader decides
    /// whether the entry survives or is overwritten, so the caller must re-read
    /// the configuration rather than assume either outcome.
    Indeterminate,
}

/// A membership request paired with the channel its result is returned on.
pub type MembershipPending = (MembershipRequest, oneshot::Sender<MembershipResult>);

/// A `ConfigChange` this node appended: the entry that carries it, and the
/// membership it installs.
///
/// The term belongs here because an index alone does not identify an entry. Only
/// the pair names the one entry a caller is waiting on.
struct AppendedConfigChange {
    term: Term,
    index: LogIndex,
    config: ClusterConfig,
}

/// A membership caller awaiting the commit of the exact entry its request
/// appended.
struct MembershipWaiter {
    /// The membership the caller asked for. Checked against the configuration
    /// that actually commits at the awaited term and index.
    expected: ClusterConfig,
    resp_tx: oneshot::Sender<MembershipResult>,
}

/// Why `Server::apply_membership_request` could not apply a membership change.
#[derive(Debug)]
enum MembershipApplyError {
    Submit(SubmitError),
    /// The removal would have left the configuration with no members, which
    /// makes quorum unreachable forever.
    WouldLeaveNoMembers,
}

impl From<SubmitError> for MembershipApplyError {
    fn from(e: SubmitError) -> Self {
        Self::Submit(e)
    }
}

/// Startup configuration, validated at the boundary.
///
/// Every field is already a domain type, parsed once by the CLI in `main.rs`,
/// so `Server::start` never parses a string and never fails on a malformed one.
pub struct Config {
    pub id: NodeId,
    /// Address the Raft listener binds to.
    pub addr: SocketAddr,
    /// Other members at startup. Excludes this node.
    pub peers: HashMap<NodeId, SocketAddr>,
    /// Directory holding the log, snapshot, and metadata files.
    pub data_dir: PathBuf,
    pub snapshot_policy: SnapshotPolicy,
}

/// A running Raft node: log persisted to disk, RPCs over TCP, and an HTTP API in
/// front of it. Generic over the command type and the state machine applying it.
pub struct Server<Cmd, SM: StateMachine<Cmd>> {
    runtime: Runtime<Cmd, SM, FileStorage<Cmd>>,
    transport: Transport<Cmd>,
    client_rx: mpsc::Receiver<Pending<Cmd, SM::Output>>,
    /// Clients awaiting a result, keyed by the term and index of the entry they
    /// submitted.
    ///
    /// The term is part of the key because an index alone does not identify a
    /// submission. If leadership changes before the entry commits, a different
    /// leader can write an unrelated entry at the same index, and an
    /// index-keyed map would hand that entry's result to this client.
    pending: HashMap<(Term, LogIndex), oneshot::Sender<ApiResponse<SM::Output>>>,
    membership_rx: mpsc::Receiver<MembershipPending>,
    /// Membership callers awaiting commit, keyed by the term and index of the
    /// `ConfigChange` they appended.
    ///
    /// Keyed like `pending`, and for the same reason: after a leadership change
    /// a different configuration can occupy the same index, and an index-keyed
    /// map would report that unrelated change as this caller's success.
    pending_membership: HashMap<(Term, LogIndex), MembershipWaiter>,
}

impl<Cmd, SM> Server<Cmd, SM>
where
    Cmd: Clone + Send + 'static + Serialize + for<'de> Deserialize<'de>,
    SM: StateMachine<Cmd>,
{
    /// Restores persistent state from disk and binds the Raft listener.
    ///
    /// # Errors
    /// `ServerError::Config` if the membership is empty.
    /// `ServerError::Storage` if the data directory cannot be read.
    /// `ServerError::StateMachine` if a persisted snapshot cannot be restored.
    /// `ServerError::Transport` if the listener address cannot be bound.
    pub fn start(
        config: Config,
        state_machine: SM,
        client_rx: mpsc::Receiver<Pending<Cmd, SM::Output>>,
        membership_rx: mpsc::Receiver<MembershipPending>,
    ) -> Result<Self, ServerError<SM::SnapshotError>> {
        let local_id = config.id;
        let addr = config.addr;
        let snapshot_policy = config.snapshot_policy;

        // The configuration must include this node, because quorum is computed
        // over the whole membership and crash recovery rescans the log against
        // it. Inserting local_id also guarantees the map is non-empty.
        let mut members = config.peers.clone();
        members.insert(local_id, addr);
        let initial_config = ClusterConfig::new(members)?;

        let storage = FileStorage::open(&config.data_dir)?;
        let runtime = Runtime::from_storage(
            local_id,
            initial_config,
            state_machine,
            storage,
            TimerConfig::default(),
            snapshot_policy,
        )?;

        // The transport routes to peers only; this node is never dialed.
        let transport = Transport::bind(local_id, addr, config.peers)?;

        tracing::info!(node = %local_id, addr = %addr, "raft listener bound");

        Ok(Self {
            runtime,
            transport,
            client_rx,
            pending: HashMap::new(),
            membership_rx,
            pending_membership: HashMap::new(),
        })
    }

    /// Runs the event loop: client requests, then timers, then inbound messages.
    /// Does not return under normal operation.
    ///
    /// # Errors
    /// `ServerError::Storage` or `ServerError::StateMachine` if persisting or
    /// snapshotting fails. Both are fatal, since the node can no longer honour
    /// the durability guarantee its responses imply.
    pub fn run(&mut self) -> Result<(), ServerError<SM::SnapshotError>> {
        loop {
            self.poll_client_requests();
            self.poll_membership_requests();

            if let Some(event) = self.runtime.poll_timers() {
                let commands = self.runtime.handle(event)?;
                self.apply_config_changes();
                self.dispatch(commands);
                if self.runtime.take_stepped_down() {
                    self.purge_pending();
                }
                self.resolve_outputs();
                self.resolve_membership_outputs();
                continue;
            }

            let wait = self
                .runtime
                .next_deadline()
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(5));

            if let Some((from, message)) = self.transport.recv_timeout(wait) {
                let commands = self.runtime.handle(Event::Message { from, message })?;
                self.apply_config_changes();
                self.dispatch(commands);
                if self.runtime.take_stepped_down() {
                    self.purge_pending();
                }
                self.resolve_outputs();
                self.resolve_membership_outputs();
            }
        }
    }

    /// Drains queued client commands, submitting each and registering the
    /// caller to be answered once the entry commits.
    fn poll_client_requests(&mut self) {
        while let Ok((command, resp_tx)) = self.client_rx.try_recv() {
            match self.runtime.submit(command) {
                Ok(index) => {
                    // submit stamped the entry with the current term a moment
                    // ago, and nothing between then and here can change it, so
                    // this read yields the term the entry was submitted under.
                    let term = self.runtime.node().persistent().current_term();
                    tracing::debug!(node = %self.runtime.node().id(), %term, %index, "client command queued");
                    self.pending.insert((term, index), resp_tx);
                }
                Err(NotLeaderError { leader_hint }) => {
                    let _ = resp_tx.send(ApiResponse::NotLeader { leader_hint });
                }
            }
        }
    }

    /// Drains queued membership requests, answering a refusal immediately and
    /// registering an accepted change to be answered once it commits.
    fn poll_membership_requests(&mut self) {
        while let Ok((req, resp_tx)) = self.membership_rx.try_recv() {
            match self.apply_membership_request(req) {
                Ok(appended) => {
                    let AppendedConfigChange {
                        term,
                        index,
                        config,
                    } = appended;
                    tracing::debug!(node = %self.runtime.node().id(), %term, %index, "membership change queued");
                    self.pending_membership.insert(
                        (term, index),
                        MembershipWaiter {
                            expected: config,
                            resp_tx,
                        },
                    );
                }
                Err(MembershipApplyError::Submit(SubmitError::NotLeader { .. })) => {
                    let _ = resp_tx.send(MembershipResult::NotLeader);
                }
                Err(MembershipApplyError::Submit(SubmitError::ConfigChangePending))
                | Err(MembershipApplyError::WouldLeaveNoMembers) => {
                    let _ = resp_tx.send(MembershipResult::Rejected);
                }
            }
        }
    }

    /// Derives the next configuration from the current one and submits it.
    ///
    /// The transport peer map is synchronized before returning. The
    /// configuration takes effect on `Node` the moment it is appended (section
    /// 4.1), so a transport lagging even one loop iteration behind would make
    /// the next heartbeat target a peer it cannot resolve.
    fn apply_membership_request(
        &mut self,
        req: MembershipRequest,
    ) -> Result<AppendedConfigChange, MembershipApplyError> {
        let current = self.runtime.node().config();
        let config = match req {
            MembershipRequest::Add { id, addr } => current.with_member(id, addr),
            MembershipRequest::Remove { id } => current
                .without_member(id)
                .map_err(|_| MembershipApplyError::WouldLeaveNoMembers)?,
        };
        let index = self.runtime.submit_config_change(config.clone())?;
        // submit_config_change stamped the entry with the current term a moment
        // ago, and nothing between then and here can change it, so this read
        // yields the term the entry was appended under.
        let term = self.runtime.node().persistent().current_term();
        self.apply_config_changes();
        Ok(AppendedConfigChange {
            term,
            index,
            config,
        })
    }

    /// Brings the transport peer map in line with every configuration that has
    /// taken effect since the last call.
    fn apply_config_changes(&mut self) {
        for config in self.runtime.take_config_changes() {
            let self_id = self.runtime.node().id();
            let to_remove: Vec<NodeId> = self
                .transport
                .peer_ids()
                .into_iter()
                .filter(|&id| !config.contains(id))
                .collect();
            for id in to_remove {
                self.transport.remove_peer(id);
            }
            for (peer_id, addr) in config.members() {
                if peer_id != self_id {
                    self.transport.add_peer(peer_id, addr);
                }
            }
        }
    }

    /// Answers the clients whose entries have just been applied. An output with
    /// no waiting client is discarded, which is the normal case on a follower
    /// and after a restart.
    fn resolve_outputs(&mut self) {
        for (term, index, result) in self.runtime.take_outputs() {
            if let Some(tx) = self.pending.remove(&(term, index)) {
                let _ = tx.send(ApiResponse::Result(result));
            }
        }
    }

    /// Fails every waiting client with `NotLeader` after this node steps down.
    ///
    /// Their entries were never committed and the next leader may overwrite
    /// them, so waiting would end in a timeout that says less than the immediate
    /// answer does.
    fn purge_pending(&mut self) {
        let leader_hint = self.runtime.node().leader_hint();
        for (_, tx) in self.pending.drain() {
            let _ = tx.send(ApiResponse::NotLeader { leader_hint });
        }
        for (_, waiter) in self.pending_membership.drain() {
            let _ = waiter.resp_tx.send(MembershipResult::Indeterminate);
        }
    }

    /// Answers the membership requests whose `ConfigChange` entries have just
    /// committed.
    fn resolve_membership_outputs(&mut self) {
        for (term, index, config) in self.runtime.take_committed_config_changes() {
            let Some(waiter) = self.pending_membership.remove(&(term, index)) else {
                continue;
            };
            // The Log Matching Property makes term and index enough to identify
            // an entry, so this comparison should always hold. It is kept
            // because reporting someone else's change as this caller's success
            // is the failure being guarded against, and silence is cheaper than
            // a wrong answer.
            let result = if waiter.expected == config {
                MembershipResult::Ok
            } else {
                MembershipResult::Indeterminate
            };
            let _ = waiter.resp_tx.send(result);
        }
    }

    /// Hands every `Send` command to the transport, logging failures rather than
    /// propagating them.
    ///
    /// Sends are fire and forget by design, so an unreachable or unregistered
    /// peer is an ordinary condition that retries recover from. Treating it as
    /// fatal would let one bad address stop the whole node.
    fn dispatch(&self, commands: Vec<Command<Cmd>>) {
        for command in commands {
            if let Command::Send { to, message } = command
                && let Err(err) = self.transport.send(to, message)
            {
                tracing::warn!(peer = %to, error = %err, "failed to send message");
            }
        }
    }
}

#[cfg(all(test, feature = "kv"))]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::app::kv::KvCommand;
    use crate::app::kv::KvStore;
    use crate::core::node::Role;
    use crate::core::types::AppendEntries;
    use crate::core::types::AppendEntriesResponse;
    use crate::core::types::LogEntry;
    use crate::core::types::LogPayload;
    use crate::core::types::Message;
    use crate::core::types::RequestVote;
    use crate::core::types::Term;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// A single-node server whose configuration holds only itself, listening on
    /// an ephemeral port.
    fn test_server(id: u64, dir: &Path) -> Server<KvCommand, KvStore> {
        let local_id = NodeId::from(id);
        let listen_addr = test_addr(0);
        let config = ClusterConfig::new(HashMap::from([(local_id, listen_addr)])).unwrap();

        let storage = FileStorage::open(dir).unwrap();
        let runtime = Runtime::from_storage(
            local_id,
            config,
            KvStore::new(),
            storage,
            TimerConfig::default(),
            // Low enough that compaction is reachable within a handful of
            // commands, unlike the production default of 1024.
            SnapshotPolicy {
                compact_threshold: std::num::NonZeroUsize::new(8),
            },
        )
        .unwrap();
        let transport = Transport::bind(local_id, listen_addr, HashMap::new()).unwrap();
        let (_client_tx, client_rx) = mpsc::channel();
        let (_membership_tx, membership_rx) = mpsc::channel();

        Server {
            runtime,
            transport,
            client_rx,
            pending: HashMap::new(),
            membership_rx,
            pending_membership: HashMap::new(),
        }
    }

    /// Adding a member must update the transport peer map at once. Otherwise the
    /// next heartbeat targets a peer the transport cannot resolve, and
    /// `Transport::send` reports `UnknownPeer` for a member the node considers
    /// active.
    #[test]
    fn membership_add_updates_transport_before_next_heartbeat() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());

        server.runtime.handle(Event::ElectionTimeout).unwrap();
        assert!(matches!(server.runtime.node().role(), Role::Leader(_)));

        let new_peer = NodeId::from(2);
        let index = server.apply_membership_request(MembershipRequest::Add {
            id: new_peer,
            addr: test_addr(19999),
        });

        assert!(index.is_ok(), "leader must accept the membership change");
        assert!(
            server.transport.peer_ids().contains(&new_peer),
            "transport must track the new peer immediately, before any heartbeat tries to reach it"
        );
    }

    /// A second membership change while the first is uncommitted must be
    /// rejected as pending, distinguishably from a not-leader refusal. Collapsing
    /// the two would tell the client to retry elsewhere when the correct answer
    /// is to retry here, later.
    #[test]
    fn second_membership_request_is_rejected_while_first_uncommitted() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());

        server.runtime.handle(Event::ElectionTimeout).unwrap();
        assert!(matches!(server.runtime.node().role(), Role::Leader(_)));

        let first = server.apply_membership_request(MembershipRequest::Add {
            id: NodeId::from(2),
            addr: test_addr(19999),
        });
        assert!(first.is_ok(), "first change must be accepted");

        let second = server.apply_membership_request(MembershipRequest::Add {
            id: NodeId::from(3),
            addr: test_addr(19998),
        });
        assert!(
            matches!(
                second,
                Err(MembershipApplyError::Submit(
                    SubmitError::ConfigChangePending
                ))
            ),
            "second change must be distinguishably rejected as pending, not conflated with not-leader"
        );
    }

    /// Removing the only member of a single-node cluster must be refused. An
    /// empty configuration computes a majority over zero voters, after which no
    /// entry can ever commit and no leader can ever be elected.
    #[test]
    fn removing_the_last_member_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());

        server.runtime.handle(Event::ElectionTimeout).unwrap();
        assert!(matches!(server.runtime.node().role(), Role::Leader(_)));

        let result = server.apply_membership_request(MembershipRequest::Remove {
            id: NodeId::from(1),
        });

        assert!(
            matches!(result, Err(MembershipApplyError::WouldLeaveNoMembers)),
            "removing the last member must be rejected, not silently poison the config"
        );
    }

    /// A send to a peer that is not, or is no longer, registered must not be
    /// fatal. Sends are fire and forget by design, and a removed member is
    /// exactly the case that produces one.
    #[test]
    fn dispatch_does_not_propagate_unknown_peer_as_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let server = test_server(1, dir.path());

        let message = Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
            term: Term::default(),
            match_index: LogIndex::default(),
        });

        server.dispatch(vec![Command::Send {
            to: NodeId::from(99),
            message,
        }]);
    }

    /// If leadership changes before a submitted command commits, a
    /// different entry can later commit at the same log index. A client waiting on the
    /// original submission must never receive that unrelated command's result.
    #[test]
    fn stranded_client_never_receives_a_different_commands_result() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());

        // Node 1 becomes leader alone in term 1. A client command then lands at
        // index 2, index 1 being the leader's own no-op.
        server.runtime.handle(Event::ElectionTimeout).unwrap();
        assert!(matches!(server.runtime.node().role(), Role::Leader(_)));
        let leader_term = server.runtime.node().persistent().current_term();

        let index = server
            .runtime
            .submit(KvCommand::Set {
                key: "username".to_string(),
                value: "miles".to_string(),
            })
            .unwrap();
        assert_eq!(index, LogIndex::from(2));
        let (resp_tx, mut resp_rx) = oneshot::channel();
        server.pending.insert((leader_term, index), resp_tx);

        // A RequestVote from a higher term forces node 1 to step down, leaving
        // the command at index 2 stranded and uncommitted.
        let commands = server
            .runtime
            .handle(Event::Message {
                from: NodeId::from(2),
                message: Message::RequestVote(RequestVote {
                    term: Term::from(5),
                    candidate_id: NodeId::from(2),
                    last_log_index: LogIndex::from(1),
                    last_log_term: leader_term,
                }),
            })
            .unwrap();
        server.apply_config_changes();
        server.dispatch(commands);
        if server.runtime.take_stepped_down() {
            server.purge_pending();
        }
        server.resolve_outputs();
        assert!(matches!(server.runtime.node().role(), Role::Follower(_)));

        // A term 6 leader overwrites index 2 with an unrelated command and
        // directs node 1 to commit it.
        let commands = server
            .runtime
            .handle(Event::Message {
                from: NodeId::from(2),
                message: Message::AppendEntries(AppendEntries {
                    term: Term::from(6),
                    leader_id: NodeId::from(2),
                    prev_log_index: LogIndex::from(1),
                    prev_log_term: leader_term,
                    entries: vec![LogEntry {
                        term: Term::from(6),
                        payload: LogPayload::Command(KvCommand::Set {
                            key: "region".to_string(),
                            value: "eu-west-1".to_string(),
                        }),
                    }],
                    leader_commit: LogIndex::from(2),
                }),
            })
            .unwrap();
        server.apply_config_changes();
        server.dispatch(commands);
        if server.runtime.take_stepped_down() {
            server.purge_pending();
        }
        server.resolve_outputs();

        // The original client must never see the unrelated command's result.
        // Either it failed with NotLeader when the node stepped down, or it is
        // still waiting; both are correct. Only a Result here would mean the
        // wrong answer was delivered.
        match resp_rx.try_recv() {
            Ok(ApiResponse::Result(result)) => {
                panic!(
                    "stranded client incorrectly received a different command's result: {result:?}"
                )
            }
            Ok(ApiResponse::NotLeader { .. }) | Err(_) => {}
        }
    }

    /// A membership request registers a waiter and is answered once its own
    /// `ConfigChange` commits. A lone leader commits it on the local append.
    #[test]
    fn membership_waiter_is_answered_when_its_own_change_commits() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());
        server.runtime.handle(Event::ElectionTimeout).unwrap();

        let appended = server
            .apply_membership_request(MembershipRequest::Add {
                id: NodeId::from(2),
                addr: test_addr(19999),
            })
            .unwrap();
        let (resp_tx, mut resp_rx) = oneshot::channel();
        server.pending_membership.insert(
            (appended.term, appended.index),
            MembershipWaiter {
                expected: appended.config.clone(),
                resp_tx,
            },
        );

        // The new member is part of the quorum from the moment the change is
        // appended, so the entry commits only once it acknowledges.
        server
            .runtime
            .handle(Event::Message {
                from: NodeId::from(2),
                message: Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
                    term: appended.term,
                    match_index: appended.index,
                }),
            })
            .unwrap();
        server.resolve_membership_outputs();

        assert!(
            matches!(resp_rx.try_recv(), Ok(MembershipResult::Ok)),
            "the caller's own change committed, so it must be told so"
        );
    }

    /// A committed `ConfigChange` with nobody waiting on it is discarded. That
    /// is the normal case on a follower and after a restart.
    #[test]
    fn committed_change_with_no_waiter_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());
        server.runtime.handle(Event::ElectionTimeout).unwrap();

        let appended = server
            .apply_membership_request(MembershipRequest::Add {
                id: NodeId::from(2),
                addr: test_addr(19999),
            })
            .unwrap();
        server
            .runtime
            .handle(Event::Message {
                from: NodeId::from(2),
                message: Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
                    term: appended.term,
                    match_index: appended.index,
                }),
            })
            .unwrap();

        server.resolve_membership_outputs();

        assert!(server.pending_membership.is_empty());
    }

    /// Stepping down strands every membership waiter. The next leader decides
    /// whether the entry survives, so the caller is told the outcome is
    /// undecided rather than being left to time out.
    #[test]
    fn stepping_down_answers_membership_waiters_as_indeterminate() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());
        server.runtime.handle(Event::ElectionTimeout).unwrap();

        let appended = server
            .apply_membership_request(MembershipRequest::Add {
                id: NodeId::from(2),
                addr: test_addr(19999),
            })
            .unwrap();
        let (resp_tx, mut resp_rx) = oneshot::channel();
        server.pending_membership.insert(
            (appended.term, appended.index),
            MembershipWaiter {
                expected: appended.config,
                resp_tx,
            },
        );

        server.purge_pending();

        assert!(
            matches!(resp_rx.try_recv(), Ok(MembershipResult::Indeterminate)),
            "a stranded membership caller must be told the outcome is undecided"
        );
        assert!(server.pending_membership.is_empty());
    }

    /// If leadership changes before a membership change commits, a different
    /// `ConfigChange` can later commit at the same log index. A caller waiting
    /// on the original request must never be told its own change succeeded.
    #[test]
    fn stranded_membership_client_never_receives_a_different_configs_result() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = test_server(1, dir.path());

        // Node 1 leads alone in term 1. Its no-op takes index 1, so the
        // membership change lands at index 2.
        server.runtime.handle(Event::ElectionTimeout).unwrap();
        let leader_term = server.runtime.node().persistent().current_term();

        let appended = server
            .apply_membership_request(MembershipRequest::Add {
                id: NodeId::from(2),
                addr: test_addr(19999),
            })
            .unwrap();
        assert_eq!(appended.index, LogIndex::from(2));
        let (resp_tx, mut resp_rx) = oneshot::channel();
        server.pending_membership.insert(
            (appended.term, appended.index),
            MembershipWaiter {
                expected: appended.config,
                resp_tx,
            },
        );

        // A higher term forces node 1 to step down before its change commits.
        let commands = server
            .runtime
            .handle(Event::Message {
                from: NodeId::from(2),
                message: Message::RequestVote(RequestVote {
                    term: Term::from(5),
                    candidate_id: NodeId::from(2),
                    last_log_index: LogIndex::from(1),
                    last_log_term: leader_term,
                }),
            })
            .unwrap();
        server.apply_config_changes();
        server.dispatch(commands);
        if server.runtime.take_stepped_down() {
            server.purge_pending();
        }
        server.resolve_membership_outputs();

        // A term 6 leader overwrites index 2 with an unrelated configuration
        // and directs node 1 to commit it.
        let unrelated = ClusterConfig::new(HashMap::from([
            (NodeId::from(1), test_addr(19001)),
            (NodeId::from(7), test_addr(19007)),
        ]))
        .unwrap();
        let commands = server
            .runtime
            .handle(Event::Message {
                from: NodeId::from(2),
                message: Message::AppendEntries(AppendEntries {
                    term: Term::from(6),
                    leader_id: NodeId::from(2),
                    prev_log_index: LogIndex::from(1),
                    prev_log_term: leader_term,
                    entries: vec![LogEntry {
                        term: Term::from(6),
                        payload: LogPayload::ConfigChange(unrelated),
                    }],
                    leader_commit: LogIndex::from(2),
                }),
            })
            .unwrap();
        server.apply_config_changes();
        server.dispatch(commands);
        if server.runtime.take_stepped_down() {
            server.purge_pending();
        }
        server.resolve_membership_outputs();

        // Either the caller was told the outcome is undecided when the node
        // stepped down, or it is still waiting. Both are correct. Only an Ok
        // here would mean a different configuration's commit was reported as
        // this caller's success.
        match resp_rx.try_recv() {
            Ok(MembershipResult::Ok) => {
                panic!("stranded membership caller was told an unrelated config change succeeded")
            }
            Ok(MembershipResult::NotLeader)
            | Ok(MembershipResult::Rejected)
            | Ok(MembershipResult::Indeterminate)
            | Err(_) => {}
        }
    }
}
