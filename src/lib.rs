//! An implementation of the Raft consensus algorithm.
//!
//! The crate is layered so that the protocol itself stays free of I/O.
//!
//! `Node` is a pure state machine: it consumes events (a timeout, an incoming
//! message, a client submission), returns the `Command`s that must be carried
//! out, and never touches a socket or a clock. That makes every protocol
//! decision reproducible from a sequence of inputs alone.
//!
//! `Runtime` supplies what `Node` deliberately lacks: timers, durable
//! `Storage`, and a `StateMachine` to apply committed commands to. `Server`
//! wraps the runtime in an event loop with a TCP `Transport` and an HTTP client
//! API.
//!
//! Section references in the documentation point at the Raft paper and Diego
//! Ongaro's dissertation.

mod app;
mod core;
mod storage;

pub use core::command::Command;
pub use core::node::Node;
pub use core::node::NotLeaderError;
pub use core::node::Role;
pub use core::node::SubmitError;
pub use core::types;

#[cfg(feature = "kv")]
pub use app::client_api;
#[cfg(feature = "kv")]
pub use app::kv;
pub use app::runtime::Event;
pub use app::runtime::Runtime;
pub use app::runtime::RuntimeError;
pub use app::runtime::SnapshotPolicy;
pub use app::runtime::StateMachine;
pub use app::runtime::TimerConfig;
pub use app::server::Config;
pub use app::server::MembershipPending;
pub use app::server::MembershipRequest;
pub use app::server::MembershipResult;
pub use app::server::Server;
pub use app::server::ServerError;
pub use app::transport::Transport;
pub use app::transport::TransportError;
pub use storage::FileStorage;
pub use storage::FileStorageError;
pub use storage::LoadedState;
pub use storage::MemoryStorage;
pub use storage::Storage;
