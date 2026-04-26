use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::client_api::{
    ApiResponse, MembershipPending, MembershipRequest, MembershipResult, Pending,
};
use crate::command::Command;
use crate::file_storage::{FileStorage, FileStorageError};
use crate::kv::{KvCommand, KvStore};
use crate::runtime::{Event, Runtime, TimerConfig};
use crate::transport::{Transport, TransportError};
use crate::types::{ClusterConfig, LogIndex, NodeId};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("storage: {0}")]
    Storage(#[from] FileStorageError),
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
    #[error("config: {0}")]
    Config(String),
}

pub struct Config {
    pub id: u64,
    pub addr: String,
    pub peers: HashMap<String, String>,
    pub data_dir: PathBuf,
    pub client_addr: Option<String>,
}

/// A running Raft KV node: persistent log on disk, RPCs over TCP.
pub struct Server {
    runtime: Runtime<KvCommand, KvStore, FileStorage<KvCommand>>,
    transport: Transport<KvCommand>,
    client_rx: mpsc::Receiver<Pending>,
    pending: HashMap<LogIndex, oneshot::Sender<ApiResponse>>,
    membership_rx: mpsc::Receiver<MembershipPending>,
    pending_membership: HashMap<LogIndex, oneshot::Sender<MembershipResult>>,
}

impl Server {
    /// Restores persistent state from disk and binds the Raft listener.
    pub fn start(
        config: Config,
        client_rx: mpsc::Receiver<Pending>,
        membership_rx: mpsc::Receiver<MembershipPending>,
    ) -> Result<Self, ServerError> {
        let local_id = NodeId::from(config.id);

        let addr: SocketAddr = config
            .addr
            .parse()
            .map_err(|e| ServerError::Config(format!("invalid addr '{}': {e}", config.addr)))?;

        let peers = parse_peers(&config.peers)?;

        // Initial config includes self so crash-recovery can rescan the log correctly.
        let mut members = peers.clone();
        members.insert(local_id, addr);
        let initial_config = ClusterConfig::new(members);

        let storage = FileStorage::open(&config.data_dir)?;
        let runtime = Runtime::from_storage(
            local_id,
            initial_config,
            KvStore::new(),
            storage,
            TimerConfig::default(),
        )?;

        // Transport only tracks peers (not self).
        let transport = Transport::bind(local_id, addr, peers)?;

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
    pub fn run(&mut self) -> Result<(), ServerError> {
        loop {
            self.poll_client_requests();
            self.poll_membership_requests();

            if let Some(event) = self.runtime.poll_timers() {
                let commands = self.runtime.handle(event)?;
                self.dispatch(commands)?;
                self.apply_config_changes();
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
                self.dispatch(commands)?;
                self.apply_config_changes();
                self.resolve_outputs();
                self.resolve_membership_outputs();
            }
        }
    }

    fn poll_client_requests(&mut self) {
        while let Ok((command, resp_tx)) = self.client_rx.try_recv() {
            match self.runtime.submit(command) {
                Some(index) => {
                    tracing::debug!(node = %self.runtime.node().id, %index, "client command queued");
                    self.pending.insert(index, resp_tx);
                }
                None => {
                    let _ = resp_tx.send(ApiResponse::NotLeader);
                }
            }
        }
    }

    fn poll_membership_requests(&mut self) {
        while let Ok((req, resp_tx)) = self.membership_rx.try_recv() {
            match self.apply_membership_request(req) {
                Some(index) => {
                    tracing::debug!(node = %self.runtime.node().id, %index, "membership change queued");
                    self.pending_membership.insert(index, resp_tx);
                }
                None => {
                    let _ = resp_tx.send(MembershipResult::NotLeader);
                }
            }
        }
    }

    /// Build the new config from the current one and submit it. None means not leader or
    /// another change is already uncommitted (propose_config_change returns None for both).
    fn apply_membership_request(&mut self, req: MembershipRequest) -> Option<LogIndex> {
        let mut new_members = self.runtime.node().config.members.clone();
        match req {
            MembershipRequest::Add { id, addr } => {
                new_members.insert(id, addr);
            }
            MembershipRequest::Remove { id } => {
                new_members.remove(&id);
            }
        }
        self.runtime.submit_config_change(ClusterConfig::new(new_members))
    }

    /// Sync the transport peer map whenever a new config takes effect on append.
    fn apply_config_changes(&mut self) {
        for config in self.runtime.take_config_changes() {
            let self_id = self.runtime.node().id;
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
            for (&peer_id, &addr) in &config.members {
                if peer_id != self_id {
                    self.transport.add_peer(peer_id, addr);
                }
            }
        }
    }

    fn resolve_outputs(&mut self) {
        for (index, result) in self.runtime.take_outputs() {
            if let Some(tx) = self.pending.remove(&index) {
                let _ = tx.send(ApiResponse::Result(result));
            }
        }
    }

    fn resolve_membership_outputs(&mut self) {
        for (index, _config) in self.runtime.take_committed_config_changes() {
            if let Some(tx) = self.pending_membership.remove(&index) {
                let _ = tx.send(MembershipResult::Ok);
            }
        }
    }

    fn dispatch(&self, commands: Vec<Command<KvCommand>>) -> Result<(), ServerError> {
        for command in commands {
            if let Command::Send { to, message } = command {
                self.transport.send(to, message)?;
            }
        }
        Ok(())
    }
}

fn parse_peers(raw: &HashMap<String, String>) -> Result<HashMap<NodeId, SocketAddr>, ServerError> {
    raw.iter()
        .map(|(id_str, addr_str)| {
            let id: u64 = id_str
                .parse()
                .map_err(|_| ServerError::Config(format!("invalid peer id: {id_str}")))?;
            let addr: SocketAddr = addr_str.parse().map_err(|e| {
                ServerError::Config(format!("invalid peer addr '{addr_str}': {e}"))
            })?;
            Ok((NodeId::from(id), addr))
        })
        .collect()
}
