use serde::Deserialize;
use serde::Serialize;

use super::log::LogEntry;
use super::primitives::LogIndex;
use super::primitives::NodeId;
use super::primitives::Term;
use super::snapshot::Snapshot;

/// RequestVote RPC arguments.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// RequestVote RPC response.
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote: Vote,
}

/// Outcome of a RequestVote decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Vote {
    Granted,
    Denied,
}

/// AppendEntries RPC arguments.
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntries<Cmd> {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<Cmd>>,
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response.
#[derive(Debug, Serialize, Deserialize)]
pub enum AppendEntriesResponse {
    Accepted {
        term: Term,
        match_index: LogIndex,
    },
    /// Term mismatch or log inconsistency; match_index is undefined.
    Rejected {
        term: Term,
    },
}

impl AppendEntriesResponse {
    pub fn term(&self) -> Term {
        match self {
            Self::Accepted { term, .. } | Self::Rejected { term } => *term,
        }
    }
}

/// InstallSnapshot RPC arguments (paper §7, single-message variant — no
/// offset/done chunking).
#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshot {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: Snapshot,
}

/// InstallSnapshot RPC response.
#[derive(Debug, Serialize, Deserialize)]
pub enum InstallSnapshotResponse {
    /// Snapshot installed, or already covered by this node's commit index;
    /// the leader may advance next_index/match_index to `last_index`.
    Installed { term: Term, last_index: LogIndex },
    /// Stale leader term.
    Rejected { term: Term },
}

impl InstallSnapshotResponse {
    pub fn term(&self) -> Term {
        match self {
            Self::Installed { term, .. } | Self::Rejected { term } => *term,
        }
    }
}

/// All possible Raft messages.
#[derive(Debug, Serialize, Deserialize)]
pub enum Message<Cmd> {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries<Cmd>),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResponse(InstallSnapshotResponse),
}
