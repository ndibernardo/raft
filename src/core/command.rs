use crate::core::types::Message;
use crate::core::types::NodeId;

/// A side effect the driver must perform on behalf of a `Node`.
///
/// `Node` is a pure state machine: it never sends bytes and never reads a
/// clock. Every outward action it needs is returned as one of these variants,
/// which the runtime executes in order.
pub enum Command<Cmd> {
    /// Deliver `message` to peer `to`. Delivery may fail or be reordered.
    Send { to: NodeId, message: Message<Cmd> },
    /// Restart the randomized election timeout.
    ResetElectionTimer,
    /// Restart the heartbeat interval. Only a leader emits this.
    ResetHeartbeatTimer,
}
