use crate::types::Message;
use crate::types::NodeId;

pub enum Command<Cmd> {
    Send {
        to: NodeId,
        message: Message<Cmd>,
    },
    ResetElectionTimer,
    /// Leader only — non-leaders never need this deadline.
    ResetHeartbeatTimer,
}
