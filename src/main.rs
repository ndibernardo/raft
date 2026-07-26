use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::mpsc;

use clap::Parser;
use raft::Config;
use raft::Server;
use raft::SnapshotPolicy;
use raft::client_api;
use raft::kv::KvStore;
use raft::types::NodeId;

/// Compact once 1024 applied entries have accumulated past the last snapshot.
const PRODUCTION_SNAPSHOT_THRESHOLD: Option<NonZeroUsize> = NonZeroUsize::new(1024);

/// One `--peer` entry in `ID=ADDR` form.
#[derive(Clone)]
struct PeerSpec {
    id: NodeId,
    addr: SocketAddr,
}

/// Why a `--peer` argument could not be parsed.
#[derive(Debug, thiserror::Error)]
enum PeerSpecError {
    #[error("--peer must be ID=ADDR, got '{0}'")]
    MissingSeparator(String),
    #[error("invalid peer id '{0}': {1}")]
    InvalidId(String, std::num::ParseIntError),
    #[error("invalid peer addr '{0}': {1}")]
    InvalidAddr(String, std::net::AddrParseError),
}

impl FromStr for PeerSpec {
    type Err = PeerSpecError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id_str, addr_str) = s
            .split_once('=')
            .ok_or_else(|| PeerSpecError::MissingSeparator(s.to_string()))?;
        let id: u64 = id_str
            .parse()
            .map_err(|e| PeerSpecError::InvalidId(id_str.to_string(), e))?;
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| PeerSpecError::InvalidAddr(addr_str.to_string(), e))?;
        Ok(Self {
            id: NodeId::from(id),
            addr,
        })
    }
}

/// Command line arguments. Parsed into domain types here, at the boundary, so
/// nothing downstream handles raw strings.
#[derive(Parser)]
struct Args {
    /// Unique node ID within the cluster.
    #[arg(long)]
    id: u64,

    /// Raft RPC listen address.
    #[arg(long)]
    addr: SocketAddr,

    /// Peer in ID=ADDR form; repeat for each peer.
    #[arg(long = "peer")]
    peers: Vec<PeerSpec>,

    /// Persistent state directory.
    #[arg(long)]
    data_dir: std::path::PathBuf,

    /// HTTP client API listen address.
    #[arg(long)]
    client_addr: Option<SocketAddr>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("raft=info")),
        )
        .init();

    let args = Args::parse();

    let peers: HashMap<NodeId, SocketAddr> = args.peers.iter().map(|p| (p.id, p.addr)).collect();

    let (client_tx, client_rx) = mpsc::channel::<client_api::Pending>();
    let (membership_tx, membership_rx) = mpsc::channel::<raft::MembershipPending>();

    let config = Config {
        id: NodeId::from(args.id),
        addr: args.addr,
        peers,
        data_dir: args.data_dir,
        snapshot_policy: SnapshotPolicy {
            compact_threshold: PRODUCTION_SNAPSHOT_THRESHOLD,
        },
    };

    if let Some(addr) = args.client_addr {
        client_api::start(addr, client_tx.clone(), membership_tx.clone());
    }

    Server::start(config, KvStore::new(), client_rx, membership_rx)?.run()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_spec_parses_id_and_addr() {
        let spec: PeerSpec = "2=127.0.0.1:9002".parse().unwrap();
        assert_eq!(spec.id, NodeId::from(2));
        assert_eq!(spec.addr, "127.0.0.1:9002".parse().unwrap());
    }

    #[test]
    fn peer_spec_rejects_missing_separator() {
        let result: Result<PeerSpec, _> = "2-127.0.0.1:9002".parse();
        assert!(matches!(result, Err(PeerSpecError::MissingSeparator(_))));
    }

    #[test]
    fn peer_spec_rejects_invalid_id() {
        let result: Result<PeerSpec, _> = "node-two=127.0.0.1:9002".parse();
        assert!(matches!(result, Err(PeerSpecError::InvalidId(..))));
    }

    #[test]
    fn peer_spec_rejects_invalid_addr() {
        let result: Result<PeerSpec, _> = "2=not-an-address".parse();
        assert!(matches!(result, Err(PeerSpecError::InvalidAddr(..))));
    }
}
