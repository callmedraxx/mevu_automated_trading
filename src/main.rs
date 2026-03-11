mod crypto;

use axum::{Router, routing::get};
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(|| async { "Welcome to Automated Trading Engine"}))
        .route("/health", get(health_check));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum::serve(
        tokio::net::TcpListener::bind(addr).await.unwrap(),app
    ).await.unwrap();
}

async fn health_check() -> &'static str {
    "ok"
}
