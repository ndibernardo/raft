use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::types::{Message, NodeId};

/// Error type for transport operations.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),
}

/// Wire envelope: wraps a Raft message with the sender's identity.
#[derive(Serialize, Deserialize)]
struct Envelope<Cmd> {
    from: NodeId,
    message: Message<Cmd>,
}

/// TCP transport for Raft RPCs.
///
/// Messages are framed with a 4-byte big-endian length prefix followed by a
/// JSON-serialized `Envelope`. A background thread accepts incoming connections;
/// each is dispatched to its own short-lived thread which reads one message and
/// forwards it into the receive channel. Outbound messages are sent fire-and-forget
/// on ephemeral threads — failed sends are silently dropped, consistent with
/// Raft's assumption of unreliable networks (timeout/retry handles losses).
pub struct Transport<Cmd> {
    local_id: NodeId,
    peers: HashMap<NodeId, SocketAddr>,
    rx: mpsc::Receiver<(NodeId, Message<Cmd>)>,
    /// Keeping this Arc alive closes the listener when Transport is dropped,
    /// which causes the accept loop to receive an error and exit.
    _listener: Arc<TcpListener>,
}

impl<Cmd> Transport<Cmd>
where
    Cmd: Send + 'static + Serialize + for<'de> Deserialize<'de>,
{
    /// Bind a listener on `addr` and start accepting inbound Raft RPCs.
    pub fn bind(
        local_id: NodeId,
        addr: SocketAddr,
        peers: HashMap<NodeId, SocketAddr>,
    ) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self::start(local_id, listener, peers))
    }

    fn start(
        local_id: NodeId,
        listener: TcpListener,
        peers: HashMap<NodeId, SocketAddr>,
    ) -> Self {
        let listener = Arc::new(listener);
        let (tx, rx) = mpsc::channel();
        let listener_bg = Arc::clone(&listener);
        thread::spawn(move || accept_loop::<Cmd>(listener_bg, tx));
        Self { local_id, peers, rx, _listener: listener }
    }

    /// Fire-and-forget: returns immediately; UnknownPeer is the only synchronous error.
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

    /// Returns None on timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<(NodeId, Message<Cmd>)> {
        self.rx.recv_timeout(timeout).ok()
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self._listener.local_addr()?)
    }
}

fn accept_loop<Cmd>(listener: Arc<TcpListener>, tx: mpsc::Sender<(NodeId, Message<Cmd>)>)
where
    Cmd: Send + 'static + for<'de> Deserialize<'de>,
{
    while let Ok((stream, _)) = listener.accept() {
        let tx = tx.clone();
        thread::spawn(move || {
            // Bound how long we wait for a slow/misbehaving sender.
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            if let Ok(env) = read_envelope::<Cmd>(&stream) {
                let _ = tx.send((env.from, env.message));
            }
        });
    }
}

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
    use crate::types::{LogIndex, RequestVote, Term};

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
        let Message::RequestVote(rv) = msg else { panic!("wrong variant") };
        assert_eq!(rv.term, Term::from(3));
        assert_eq!(rv.candidate_id, NodeId::from(1));
    }

    #[test]
    fn recv_timeout_returns_none_on_silence() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let t: Transport<String> =
            Transport::start(NodeId::from(9), listener, HashMap::new());
        assert!(t.recv_timeout(Duration::from_millis(50)).is_none());
    }

    #[test]
    fn bidirectional_exchange() {
        use crate::types::{AppendEntries, AppendEntriesResponse};

        let (a, b) = make_pair();

        // A → B: AppendEntries
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

        // B → A: AppendEntriesResponse
        b.send(
            NodeId::from(1),
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term::from(1),
                success: true,
                match_index: LogIndex::from(0),
            }),
        )
        .unwrap();

        let (from, msg) = a.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(from, NodeId::from(2));
        let Message::AppendEntriesResponse(resp) = msg else { panic!("wrong variant") };
        assert!(resp.success);
    }
}
