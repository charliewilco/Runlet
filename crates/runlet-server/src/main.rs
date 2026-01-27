mod http;
mod routes;
mod sse;
mod state;

use crate::http::build_router;
use crate::state::AppState;
use runlet_core::Store;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let args = normalize_args(&args);

    let log_level = arg_value(&args, "--log-level");
    let filter = log_level
        .map(EnvFilter::new)
        .unwrap_or_else(|| EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let addr = arg_value(&args, "--addr")
        .unwrap_or_else(|| "127.0.0.1:8787".to_string());

    let max_events_per_job = arg_value(&args, "--max-events-per-job")
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000usize);

    let max_line_bytes = arg_value(&args, "--max-line-bytes")
        .and_then(|value| value.parse().ok())
        .unwrap_or(16_384usize);

    let store = Store::new(max_events_per_job);
    let state = AppState::new(store, max_line_bytes);

    let app = build_router(state);

    let addr: SocketAddr = addr.parse().expect("invalid --addr");
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

fn normalize_args(args: &[String]) -> Vec<String> {
    if args.len() >= 2 && args[1] == "serve" {
        let mut normalized = Vec::with_capacity(args.len() - 1);
        normalized.push(args[0].clone());
        normalized.extend_from_slice(&args[2..]);
        normalized
    } else {
        args.to_vec()
    }
}

fn arg_value(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == key)
        .and_then(|idx| args.get(idx + 1))
        .cloned()
}
