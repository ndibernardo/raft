use serde::Deserialize;
use serde::Serialize;

use super::log::LogEntry;
use super::primitives::LogIndex;
use super::primitives::NodeId;
use super::primitives::Term;
use super::snapshot::Snapshot;

/// RequestVote RPC arguments (section 5.2).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVote {
    /// Term the candidate is standing for.
    pub term: Term,
    pub candidate_id: NodeId,
    /// Last index in the candidate's log, used for the up-to-date check.
    pub last_log_index: LogIndex,
    /// Term of the entry at `last_log_index`.
    pub last_log_term: Term,
}

/// RequestVote RPC response.
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// The voter's current term, so a stale candidate can step down.
    pub term: Term,
    pub vote: Vote,
}

/// Outcome of a RequestVote decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Vote {
    Granted,
    Denied,
}

/// AppendEntries RPC arguments (section 5.3). Also serves as the heartbeat,
/// in which case `entries` is empty.
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntries<Cmd> {
    pub term: Term,
    pub leader_id: NodeId,
    /// Index immediately preceding `entries`. The follower rejects the request
    /// unless its own log matches at this position.
    pub prev_log_index: LogIndex,
    /// Term the leader expects to find at `prev_log_index`.
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<Cmd>>,
    /// The leader's commit index, which lets the follower advance its own.
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response.
#[derive(Debug, Serialize, Deserialize)]
pub enum AppendEntriesResponse {
    /// The follower's log now matches the leader up to `match_index`.
    Accepted { term: Term, match_index: LogIndex },
    /// The leader's term is stale, or the log consistency check at
    /// `prev_log_index` failed. No match index is implied.
    Rejected { term: Term },
}

impl AppendEntriesResponse {
    /// The responder's current term, which is present in every variant.
    pub fn term(&self) -> Term {
        match self {
            Self::Accepted { term, .. } | Self::Rejected { term } => *term,
        }
    }
}

/// InstallSnapshot RPC arguments (section 7).
///
/// The whole snapshot travels in one message. The `offset` and `done` fields
/// from the paper, which chunk a large snapshot across several RPCs, are not
/// implemented.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshot {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: Snapshot,
}

/// InstallSnapshot RPC response.
#[derive(Debug, Serialize, Deserialize)]
pub enum InstallSnapshotResponse {
    /// The snapshot was installed, or the follower had already committed past
    /// it. Either way the leader may advance `next_index` and `match_index` to
    /// `last_index`.
    Installed { term: Term, last_index: LogIndex },
    /// The leader's term is stale.
    Rejected { term: Term },
}

impl InstallSnapshotResponse {
    /// The responder's current term, which is present in every variant.
    pub fn term(&self) -> Term {
        match self {
            Self::Installed { term, .. } | Self::Rejected { term } => *term,
        }
    }
}

/// Every message that can travel between nodes, request and response alike.
#[derive(Debug, Serialize, Deserialize)]
pub enum Message<Cmd> {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries<Cmd>),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResponse(InstallSnapshotResponse),
}
