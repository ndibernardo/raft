use serde::{Deserialize, Serialize};

use super::config::ClusterConfig;
use super::primitives::Term;

/// The data carried by a log entry. §8: leaders append a NoOp on election to commit
/// prior-term entries via Log Matching without direct commitment of old terms.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogPayload<Cmd> {
    NoOp,
    Command(Cmd),
    /// Single-server membership change (dissertation §4.1).
    /// Takes effect immediately when appended, not when committed.
    ConfigChange(ClusterConfig),
}

/// A single entry in the replicated log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry<Cmd> {
    pub term: Term,
    pub payload: LogPayload<Cmd>,
}
