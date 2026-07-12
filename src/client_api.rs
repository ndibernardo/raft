use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router, body::Bytes};
use serde::Deserialize;
use tokio::sync::oneshot;

use crate::kv::{KvCommand, KvResult};
use crate::types::NodeId;

#[derive(Debug)]
pub enum ApiResponse {
    Result(KvResult),
    NotLeader { leader_hint: Option<NodeId> },
}

/// KV command paired with the channel to deliver its result.
pub type Pending = (KvCommand, oneshot::Sender<ApiResponse>);

pub enum MembershipRequest {
    Add { id: NodeId, addr: SocketAddr },
    Remove { id: NodeId },
}

pub enum MembershipResult {
    Ok,
    NotLeader,
    /// Another config change is already uncommitted.
    Rejected,
}

/// Membership request paired with the channel to deliver its result.
pub type MembershipPending = (MembershipRequest, oneshot::Sender<MembershipResult>);

#[derive(Clone)]
struct AppState {
    kv_tx: mpsc::Sender<Pending>,
    membership_tx: mpsc::Sender<MembershipPending>,
}

/// Spawns a background thread hosting the axum HTTP server; requests forwarded via the channels.
pub fn start(
    addr: SocketAddr,
    kv_tx: mpsc::Sender<Pending>,
    membership_tx: mpsc::Sender<MembershipPending>,
) {
    thread::spawn(move || {
        match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(serve(addr, kv_tx, membership_tx)),
            Err(e) => eprintln!("client api: failed to start tokio runtime: {e}"),
        }
    });
}

async fn serve(
    addr: SocketAddr,
    kv_tx: mpsc::Sender<Pending>,
    membership_tx: mpsc::Sender<MembershipPending>,
) {
    let state = AppState { kv_tx, membership_tx };
    let app = Router::new()
        .route("/kv/{key}", get(handle_get))
        .route("/kv/{key}", put(handle_put))
        .route("/kv/{key}", delete(handle_delete))
        .route("/cluster/members", post(handle_add_member))
        .route("/cluster/members/{id}", delete(handle_remove_member))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("client api: bind {addr} failed: {e}");
            return;
        }
    };

    tracing::info!(%addr, "client api listening");

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("client api: server error: {e}");
    }
}

async fn handle_get(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, String) {
    submit_kv(state.kv_tx, KvCommand::Get { key }).await
}

async fn handle_put(
    State(state): State<AppState>,
    Path(key): Path<String>,
    body: Bytes,
) -> (StatusCode, String) {
    let value = String::from_utf8_lossy(&body).into_owned();
    submit_kv(state.kv_tx, KvCommand::Set { key, value }).await
}

async fn handle_delete(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, String) {
    submit_kv(state.kv_tx, KvCommand::Delete { key }).await
}

/// DTO for `POST /cluster/members`.
#[derive(Deserialize)]
struct AddMemberBody {
    id: u64,
    addr: String,
}

async fn handle_add_member(
    State(state): State<AppState>,
    Json(body): Json<AddMemberBody>,
) -> (StatusCode, String) {
    let addr: SocketAddr = match body.addr.parse() {
        Ok(a) => a,
        Err(_) => return (StatusCode::BAD_REQUEST, format!("invalid addr: {}", body.addr)),
    };
    let req = MembershipRequest::Add { id: NodeId::from(body.id), addr };
    submit_membership(state.membership_tx, req).await
}

async fn handle_remove_member(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> (StatusCode, String) {
    let req = MembershipRequest::Remove { id: NodeId::from(id) };
    submit_membership(state.membership_tx, req).await
}

/// 5-second timeout; SERVICE_UNAVAILABLE on timeout or channel error.
async fn submit_kv(tx: mpsc::Sender<Pending>, command: KvCommand) -> (StatusCode, String) {
    let (resp_tx, resp_rx) = oneshot::channel::<ApiResponse>();

    if tx.send((command, resp_tx)).is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "server shutting down".into());
    }

    let result = tokio::time::timeout(Duration::from_secs(5), resp_rx).await;

    match result {
        Ok(Ok(ApiResponse::Result(KvResult::Ok))) => (StatusCode::OK, "ok".into()),
        Ok(Ok(ApiResponse::Result(KvResult::Value(Some(v))))) => (StatusCode::OK, v),
        Ok(Ok(ApiResponse::Result(KvResult::Value(None)))) => {
            (StatusCode::NOT_FOUND, String::new())
        }
        Ok(Ok(ApiResponse::NotLeader { leader_hint: Some(id) })) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not the leader; leader hint: node {}", id.value()),
        ),
        Ok(Ok(ApiResponse::NotLeader { leader_hint: None })) => {
            (StatusCode::SERVICE_UNAVAILABLE, "not the leader".into())
        }
        Ok(Err(_)) | Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "timeout".into()),
    }
}

async fn submit_membership(
    tx: mpsc::Sender<MembershipPending>,
    req: MembershipRequest,
) -> (StatusCode, String) {
    let (resp_tx, resp_rx) = oneshot::channel::<MembershipResult>();

    if tx.send((req, resp_tx)).is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "server shutting down".into());
    }

    let result = tokio::time::timeout(Duration::from_secs(10), resp_rx).await;

    match result {
        Ok(Ok(MembershipResult::Ok)) => (StatusCode::OK, "ok".into()),
        Ok(Ok(MembershipResult::NotLeader)) => {
            (StatusCode::SERVICE_UNAVAILABLE, "not the leader".into())
        }
        Ok(Ok(MembershipResult::Rejected)) => {
            (StatusCode::CONFLICT, "another config change is pending".into())
        }
        Ok(Err(_)) | Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "timeout".into()),
    }
}
