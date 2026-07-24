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

/// Outcome of a submitted client command, delivered back over its response channel.
#[derive(Debug)]
pub enum ApiResponse<Output> {
    Result(Output),
    NotLeader { leader_hint: Option<NodeId> },
}

/// A submitted command paired with the channel to deliver its result.
pub type Pending<Cmd, Output> = (Cmd, oneshot::Sender<ApiResponse<Output>>);

pub enum MembershipRequest {
    Add { id: NodeId, addr: SocketAddr },
    Remove { id: NodeId },
}

pub enum MembershipResult {
    Ok,
    NotLeader,
    /// Another config change is already uncommitted.
    Rejected,
}

/// Membership request paired with the channel to deliver its result.
pub type MembershipPending = (MembershipRequest, oneshot::Sender<MembershipResult>);

/// Why `Server::apply_membership_request` could not apply a membership change.
#[derive(Debug)]
enum MembershipApplyError {
    Submit(SubmitError),
    /// Removing this member would leave the config with zero members.
    WouldLeaveNoMembers,
}

impl From<SubmitError> for MembershipApplyError {
    fn from(e: SubmitError) -> Self {
        Self::Submit(e)
    }
}

/// Boundary-validated startup config: every field is already a valid domain type,
/// parsed once at the CLI (see `main.rs`), so `Server::start` never re-parses strings.
pub struct Config {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub peers: HashMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
}

/// A running Raft node: persistent log on disk, RPCs over TCP. Generic over the
/// submitted command type and the state machine that applies it.
pub struct Server<Cmd, SM: StateMachine<Cmd>> {
    runtime: Runtime<Cmd, SM, FileStorage<Cmd>>,
    transport: Transport<Cmd>,
    client_rx: mpsc::Receiver<Pending<Cmd, SM::Output>>,
    /// Keyed by (term, index) of the submitted entry, not index alone — if leadership
    /// changes before commit, a different entry can later land at the same index, and
    /// a bare-index key would deliver that unrelated result to this client.
    pending: HashMap<(Term, LogIndex), oneshot::Sender<ApiResponse<SM::Output>>>,
    membership_rx: mpsc::Receiver<MembershipPending>,
    pending_membership: HashMap<LogIndex, oneshot::Sender<MembershipResult>>,
}

impl<Cmd, SM> Server<Cmd, SM>
where
    Cmd: Clone + Send + 'static + Serialize + for<'de> Deserialize<'de>,
    SM: StateMachine<Cmd>,
{
    /// Restores persistent state from disk and binds the Raft listener.
    pub fn start(
        config: Config,
        state_machine: SM,
        client_rx: mpsc::Receiver<Pending<Cmd, SM::Output>>,
        membership_rx: mpsc::Receiver<MembershipPending>,
    ) -> Result<Self, ServerError<SM::SnapshotError>> {
        let local_id = config.id;
        let addr = config.addr;

        // Initial config includes self so crash-recovery can rescan the log correctly.
        // Always non-empty: local_id is inserted unconditionally below.
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
            SnapshotPolicy::default(),
        )?;

        // Transport only tracks peers (not self).
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

    /// Run the Raft event loop. Returns only on I/O error.
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

    fn poll_client_requests(&mut self) {
        while let Ok((command, resp_tx)) = self.client_rx.try_recv() {
            match self.runtime.submit(command) {
                Ok(index) => {
                    // submit() just appended this entry as the current term, so reading
                    // the term back here is exactly the term it was submitted under.
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

    fn poll_membership_requests(&mut self) {
        while let Ok((req, resp_tx)) = self.membership_rx.try_recv() {
            match self.apply_membership_request(req) {
                Ok(index) => {
                    tracing::debug!(node = %self.runtime.node().id(), %index, "membership change queued");
                    self.pending_membership.insert(index, resp_tx);
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

    /// Build the next config from the current one and submit it.
    ///
    /// Syncs the transport peer map before returning — the new config takes effect on
    /// `Node` immediately on append (§4.1), so transport must not lag behind it even by
    /// one event-loop iteration, or the next heartbeat sends to a peer transport doesn't
    /// know about yet.
    fn apply_membership_request(
        &mut self,
        req: MembershipRequest,
    ) -> Result<LogIndex, MembershipApplyError> {
        let current = self.runtime.node().config();
        let new_config = match req {
            MembershipRequest::Add { id, addr } => current.with_member(id, addr),
            MembershipRequest::Remove { id } => current
                .without_member(id)
                .map_err(|_| MembershipApplyError::WouldLeaveNoMembers)?,
        };
        let index = self.runtime.submit_config_change(new_config)?;
        self.apply_config_changes();
        Ok(index)
    }

    /// Sync the transport peer map whenever a new config takes effect on append.
    fn apply_config_changes(&mut self) {
        for config in self.runtime.take_config_changes() {
            let self_id = self.runtime.node().id();
            // Remove peers that are no longer in the new config.
            let to_remove: Vec<NodeId> = self
                .transport
                .peer_ids()
                .into_iter()
                .filter(|&id| !config.contains(id))
                .collect();
            for id in to_remove {
                self.transport.remove_peer(id);
            }
            // Add or update peers from the new config.
            for (peer_id, addr) in config.members() {
                if peer_id != self_id {
                    self.transport.add_peer(peer_id, addr);
                }
            }
        }
    }

    fn resolve_outputs(&mut self) {
        for (term, index, result) in self.runtime.take_outputs() {
            if let Some(tx) = self.pending.remove(&(term, index)) {
                let _ = tx.send(ApiResponse::Result(result));
            }
        }
    }

    /// Fail every outstanding client fast with `NotLeader` instead of leaving it to time
    /// out — their entries may never commit now that this node is no longer leader.
    fn purge_pending(&mut self) {
        let leader_hint = self.runtime.node().leader_hint();
        for (_, tx) in self.pending.drain() {
            let _ = tx.send(ApiResponse::NotLeader { leader_hint });
        }
    }

    fn resolve_membership_outputs(&mut self) {
        for (index, _config) in self.runtime.take_committed_config_changes() {
            if let Some(tx) = self.pending_membership.remove(&index) {
                let _ = tx.send(MembershipResult::Ok);
            }
        }
    }

    /// Sends are fire-and-forget (`transport.rs` doc comment) — a single unreachable or
    /// unknown peer must never take down the event loop.
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

    /// Single-node server: config contains only itself, listener on an ephemeral port.
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
            SnapshotPolicy::default(),
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

    /// adding a member must sync the transport peer map immediately —
    /// otherwise the next heartbeat tries to send to a peer transport doesn't know about,
    /// `Transport::send` returns `UnknownPeer`, and (pre-fix) `run()` propagates that error
    /// and the whole server dies.
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

    /// Before the SubmitError fix both "not leader" and "another change pending"
    /// collapsed to the same `None`, so `MembershipResult::Rejected` was dead code —
    /// clients always saw a misleading `NotLeader` instead of the true 409 conflict.
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

    /// Before ClusterConfig::new validated non-emptiness, removing a single-node
    /// cluster's only member was fully representable and would have poisoned quorum
    /// math (majority of zero voters) rather than being rejected up front.
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

    /// a send to a peer that isn't (or is no longer) registered must not be treated
    /// as fatal — sends are fire-and-forget by design (see `transport.rs`).
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

        // Node 1 becomes leader alone, in term 1, and a client submits a command that
        // lands at index 2 (index 1 is the leader's own no-op).
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

        // A higher-term RequestVote from another node forces node 1 to step down —
        // its own command at index 2 is now stranded, uncommitted.
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

        // A new leader (term 6) overwrites index 2 with an unrelated command and directs
        // node 1 to commit it immediately.
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

        // The original client must never see the unrelated command's result — either it
        // failed fast with NotLeader (purge-on-stepdown) or it's still pending; either is
        // fine. Only an ApiResponse::Result here would mean the wrong answer was delivered.
        match resp_rx.try_recv() {
            Ok(ApiResponse::Result(result)) => {
                panic!(
                    "stranded client incorrectly received a different command's result: {result:?}"
                )
            }
            Ok(ApiResponse::NotLeader { .. }) | Err(_) => {}
        }
    }
}
