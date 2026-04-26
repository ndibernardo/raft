use std::collections::HashMap;
use std::sync::mpsc;

use clap::Parser;
use raft::client_api;
use raft::server::{Config, Server};

#[derive(Parser)]
struct Args {
    /// Unique node ID within the cluster.
    #[arg(long)]
    id: u64,

    /// Raft RPC listen address.
    #[arg(long)]
    addr: String,

    /// Peer in ID=ADDR form; repeat for each peer.
    #[arg(long = "peer")]
    peers: Vec<String>,

    /// Persistent state directory.
    #[arg(long)]
    data_dir: std::path::PathBuf,

    /// HTTP client API listen address.
    #[arg(long)]
    client_addr: Option<String>,
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

    let mut peers: HashMap<String, String> = HashMap::new();
    for p in &args.peers {
        let (id, addr) = p
            .split_once('=')
            .ok_or_else(|| format!("--peer must be ID=ADDR, got: {p}"))?;
        peers.insert(id.to_string(), addr.to_string());
    }

    let (client_tx, client_rx) = mpsc::channel::<client_api::Pending>();
    let (membership_tx, membership_rx) = mpsc::channel::<client_api::MembershipPending>();

    let config = Config {
        id: args.id,
        addr: args.addr,
        peers,
        data_dir: args.data_dir,
        client_addr: args.client_addr.clone(),
    };

    if let Some(ref addr_str) = args.client_addr {
        let addr = addr_str
            .parse()
            .map_err(|e| format!("invalid --client-addr '{addr_str}': {e}"))?;
        client_api::start(addr, client_tx.clone(), membership_tx.clone());
    }

    Server::start(config, client_rx, membership_rx)?.run()?;

    Ok(())
}
