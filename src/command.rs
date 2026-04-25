use crate::types::{Message, NodeId};

pub enum Command<Cmd> {
    Send { to: NodeId, message: Message<Cmd> },
    ResetElectionTimer,
    /// Leader only — non-leaders never need this deadline.
    ResetHeartbeatTimer,
}
