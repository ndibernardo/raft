use serde::{Deserialize, Serialize};

use super::log::LogEntry;
use super::primitives::{LogIndex, NodeId, Term};

/// RequestVote RPC arguments.
#[derive(Clone, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// RequestVote RPC response.
#[derive(Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

/// AppendEntries RPC arguments.
#[derive(Serialize, Deserialize)]
pub struct AppendEntries<Cmd> {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<Cmd>>,
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response.
#[derive(Serialize, Deserialize)]
pub enum AppendEntriesResponse {
    Accepted { term: Term, match_index: LogIndex },
    /// Term mismatch or log inconsistency; match_index is undefined.
    Rejected { term: Term },
}

impl AppendEntriesResponse {
    pub fn term(&self) -> Term {
        match self {
            Self::Accepted { term, .. } | Self::Rejected { term } => *term,
        }
    }
}

/// All possible Raft messages.
#[derive(Serialize, Deserialize)]
pub enum Message<Cmd> {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries<Cmd>),
    AppendEntriesResponse(AppendEntriesResponse),
}
