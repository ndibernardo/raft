use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, put};
use axum::{body::Bytes, Router};
use tokio::sync::oneshot;

use crate::kv::{KvCommand, KvResult};

pub enum ApiResponse {
    Result(KvResult),
    NotLeader,
}

/// Command paired with the channel to deliver its result.
pub type Pending = (KvCommand, oneshot::Sender<ApiResponse>);

/// Spawns a background thread hosting the axum HTTP server; requests forwarded via `tx`.
pub fn start(addr: SocketAddr, tx: mpsc::Sender<Pending>) {
    thread::spawn(move || {
        match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(serve(addr, tx)),
            Err(e) => eprintln!("client api: failed to start tokio runtime: {e}"),
        }
    });
}

async fn serve(addr: SocketAddr, tx: mpsc::Sender<Pending>) {
    let app = Router::new()
        .route("/kv/{key}", get(handle_get))
        .route("/kv/{key}", put(handle_put))
        .route("/kv/{key}", delete(handle_delete))
        .with_state(tx);

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
    State(tx): State<mpsc::Sender<Pending>>,
    Path(key): Path<String>,
) -> (StatusCode, String) {
    submit(tx, KvCommand::Get { key }).await
}

async fn handle_put(
    State(tx): State<mpsc::Sender<Pending>>,
    Path(key): Path<String>,
    body: Bytes,
) -> (StatusCode, String) {
    let value = String::from_utf8_lossy(&body).into_owned();
    submit(tx, KvCommand::Set { key, value }).await
}

async fn handle_delete(
    State(tx): State<mpsc::Sender<Pending>>,
    Path(key): Path<String>,
) -> (StatusCode, String) {
    submit(tx, KvCommand::Delete { key }).await
}

/// 5-second timeout; SERVICE_UNAVAILABLE on timeout or channel error.
async fn submit(tx: mpsc::Sender<Pending>, command: KvCommand) -> (StatusCode, String) {
    let (resp_tx, resp_rx) = oneshot::channel::<ApiResponse>();

    if tx.send((command, resp_tx)).is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "server shutting down".into());
    }

    let result = tokio::time::timeout(Duration::from_secs(5), resp_rx).await;

    match result {
        Ok(Ok(ApiResponse::Result(KvResult::Ok))) => (StatusCode::OK, "ok".into()),
        Ok(Ok(ApiResponse::Result(KvResult::Value(Some(v))))) => (StatusCode::OK, v),
        Ok(Ok(ApiResponse::Result(KvResult::Value(None)))) => (StatusCode::NOT_FOUND, String::new()),
        Ok(Ok(ApiResponse::NotLeader)) => {
            (StatusCode::SERVICE_UNAVAILABLE, "not the leader".into())
        }
        Ok(Err(_)) | Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "timeout".into()),
    }
}
