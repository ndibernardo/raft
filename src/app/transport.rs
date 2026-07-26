use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::io::{self};
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use crate::core::types::Message;
use crate::core::types::NodeId;

/// Why a transport operation failed.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),
}

/// A Raft message paired with the sender's identity, as it travels on the wire.
/// The identity is not authenticated.
#[derive(Serialize, Deserialize)]
struct Envelope<Cmd> {
    from: NodeId,
    message: Message<Cmd>,
}

/// TCP transport for Raft RPCs.
///
/// A message is framed as a four-byte big-endian length followed by a
/// JSON-encoded `Envelope`. One background thread accepts connections and hands
/// each to a short-lived thread that reads a single message and forwards it into
/// the receive channel.
///
/// Sends are fire and forget, on ephemeral threads, and a failure is dropped
/// without notice. That is sound because Raft already assumes an unreliable
/// network: a lost message is indistinguishable from a slow one, and both are
/// recovered by the sender's own timeout and retry.
pub struct Transport<Cmd> {
    local_id: NodeId,
    peers: HashMap<NodeId, SocketAddr>,
    rx: mpsc::Receiver<(NodeId, Message<Cmd>)>,
    /// Holding this handle keeps the listener open. Dropping the transport drops
    /// the last reference, which closes the socket and makes the accept loop
    /// fail and exit.
    _listener: Arc<TcpListener>,
}

impl<Cmd> Transport<Cmd>
where
    Cmd: Send + 'static + Serialize + for<'de> Deserialize<'de>,
{
    /// Binds a listener on `addr` and starts accepting inbound RPCs.
    ///
    /// # Errors
    /// `TransportError::Io` if the address cannot be bound.
    pub fn bind(
        local_id: NodeId,
        addr: SocketAddr,
        peers: HashMap<NodeId, SocketAddr>,
    ) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self::start(local_id, listener, peers))
    }

    fn start(local_id: NodeId, listener: TcpListener, peers: HashMap<NodeId, SocketAddr>) -> Self {
        let listener = Arc::new(listener);
        let (tx, rx) = mpsc::channel();
        let listener_bg = Arc::clone(&listener);
        thread::spawn(move || accept_loop::<Cmd>(listener_bg, tx));
        Self {
            local_id,
            peers,
            rx,
            _listener: listener,
        }
    }

    /// Registers a peer, replacing the address if it is already known.
    pub fn add_peer(&mut self, peer: NodeId, addr: SocketAddr) {
        self.peers.insert(peer, addr);
    }

    /// Deregisters a peer. Later sends to it fail with `UnknownPeer`.
    pub fn remove_peer(&mut self, peer: NodeId) {
        self.peers.remove(&peer);
    }

    /// All currently registered peer IDs.
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.keys().copied().collect()
    }

    /// Queues `message` for `to` and returns immediately, without waiting for
    /// the connection or the write.
    ///
    /// # Errors
    /// `TransportError::UnknownPeer` if `to` is not registered. This is the only
    /// error reported synchronously; a dial or write failure is dropped.
    pub fn send(&self, to: NodeId, message: Message<Cmd>) -> Result<(), TransportError> {
        let addr = self
            .peers
            .get(&to)
            .copied()
            .ok_or(TransportError::UnknownPeer(to))?;
        let from = self.local_id;
        thread::spawn(move || {
            let _ = dial_and_send(addr, from, message);
        });
        Ok(())
    }

    /// Waits up to `timeout` for an inbound message. `None` means the wait
    /// elapsed with nothing to deliver.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<(NodeId, Message<Cmd>)> {
        self.rx.recv_timeout(timeout).ok()
    }

    /// The address the listener is bound to, which resolves the port when the
    /// caller bound to port 0.
    ///
    /// # Errors
    /// `TransportError::Io` if the socket cannot be queried.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self._listener.local_addr()?)
    }
}

/// Accepts connections until the listener closes, handing each to its own
/// thread so one slow sender cannot stall the others.
fn accept_loop<Cmd>(listener: Arc<TcpListener>, tx: mpsc::Sender<(NodeId, Message<Cmd>)>)
where
    Cmd: Send + 'static + for<'de> Deserialize<'de>,
{
    while let Ok((stream, _)) = listener.accept() {
        let tx = tx.clone();
        thread::spawn(move || {
            // Without a read timeout, a sender that connects and then stalls
            // holds its thread forever.
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            if let Ok(env) = read_envelope::<Cmd>(&stream) {
                let _ = tx.send((env.from, env.message));
            }
        });
    }
}

/// Reads one length-prefixed envelope from `stream`.
fn read_envelope<Cmd: for<'de> Deserialize<'de>>(
    mut stream: &TcpStream,
) -> Result<Envelope<Cmd>, TransportError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Connects to `addr` and writes one length-prefixed envelope. Both the connect
/// and the write are bounded, so a peer that is down or wedged cannot hold the
/// calling thread open.
fn dial_and_send<Cmd: Serialize>(
    addr: SocketAddr,
    from: NodeId,
    message: Message<Cmd>,
) -> Result<(), TransportError> {
    let envelope = Envelope { from, message };
    let bytes = serde_json::to_vec(&envelope)?;
    let Ok(len) = u32::try_from(bytes.len()) else {
        return Err(TransportError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "message exceeds 4 GiB",
        )));
    };
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(200))?;
    stream.set_write_timeout(Some(Duration::from_millis(500)))?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&bytes)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::LogIndex;
    use crate::core::types::RequestVote;
    use crate::core::types::Term;

    fn make_pair() -> (Transport<String>, Transport<String>) {
        // Bind to port 0 first to learn the assigned addresses.
        let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
        let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let addr_b = listener_b.local_addr().unwrap();

        let id_a = NodeId::from(1);
        let id_b = NodeId::from(2);

        let transport_a = Transport::start(id_a, listener_a, [(id_b, addr_b)].into());
        let transport_b = Transport::start(id_b, listener_b, [(id_a, addr_a)].into());
        (transport_a, transport_b)
    }

    #[test]
    fn request_vote_roundtrip() {
        let (a, b) = make_pair();

        a.send(
            NodeId::from(2),
            Message::RequestVote(RequestVote {
                term: Term::from(3),
                candidate_id: NodeId::from(1),
                last_log_index: LogIndex::from(0),
                last_log_term: Term::from(0),
            }),
        )
        .unwrap();

        let (from, msg) = b.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(from, NodeId::from(1));
        let Message::RequestVote(rv) = msg else {
            panic!("wrong variant")
        };
        assert_eq!(rv.term, Term::from(3));
        assert_eq!(rv.candidate_id, NodeId::from(1));
    }

    #[test]
    fn recv_timeout_returns_none_on_silence() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let t: Transport<String> = Transport::start(NodeId::from(9), listener, HashMap::new());
        assert!(t.recv_timeout(Duration::from_millis(50)).is_none());
    }

    #[test]
    fn bidirectional_exchange() {
        use crate::core::types::AppendEntries;
        use crate::core::types::AppendEntriesResponse;

        let (a, b) = make_pair();

        // Node A sends an AppendEntries to node B.
        a.send(
            NodeId::from(2),
            Message::AppendEntries(AppendEntries {
                term: Term::from(1),
                leader_id: NodeId::from(1),
                prev_log_index: LogIndex::from(0),
                prev_log_term: Term::from(0),
                entries: vec![],
                leader_commit: LogIndex::from(0),
            }),
        )
        .unwrap();

        let (from, msg) = b.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(from, NodeId::from(1));
        assert!(matches!(msg, Message::AppendEntries(_)));

        // Node B answers over its own connection back to node A.
        b.send(
            NodeId::from(1),
            Message::AppendEntriesResponse(AppendEntriesResponse::Accepted {
                term: Term::from(1),
                match_index: LogIndex::from(0),
            }),
        )
        .unwrap();

        let (from, msg) = a.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(from, NodeId::from(2));
        let Message::AppendEntriesResponse(resp) = msg else {
            panic!("wrong variant")
        };
        assert!(matches!(resp, AppendEntriesResponse::Accepted { .. }));
    }
}
