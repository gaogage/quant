//! Quant API 服务器
//!
//! API 前缀: /api/v1/quant
//! 可观测性: tracing + tower-http trace layer + trace_id 传播

use axum::{
    http::Request,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, Span};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

mod routes;

/// 应用状态
pub struct AppState {
    pub start_time: chrono::DateTime<chrono::Utc>,
}

#[tokio::main]
async fn main() {
    // 初始化 tracing subscriber
    // RUST_LOG 环境变量控制日志级别，默认 info
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("quant=info,quant_api=info")),
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    dotenv::dotenv().ok();

    let state = Arc::new(AppState {
        start_time: chrono::Utc::now(),
    });

    // TraceLayer: 自动为每个 HTTP 请求创建 span，注入 trace_id
    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            let trace_id = Uuid::new_v4().to_string();
            // 注入 trace_id 到 request extensions，供下游 handler 读取
            info_span!(
                "http_request",
                method = %request.method(),
                uri = %request.uri(),
                trace_id = %trace_id,
            )
        })
        .on_request(|_request: &Request<_>, _span: &Span| {
            info!("HTTP 请求开始");
        })
        .on_response(
            |response: &axum::response::Response, latency: std::time::Duration, _span: &Span| {
                info!(
                    status = response.status().as_u16(),
                    latency_ms = latency.as_millis(),
                    "HTTP 请求完成"
                );
            },
        );

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/quant/backtests", post(routes::create_backtest))
        .route("/api/v1/quant/backtests/:id", get(routes::get_backtest))
        .route("/api/v1/quant/backtests/:id/report", get(routes::get_report))
        .route("/api/v1/quant/backtests/:id/equity", get(routes::get_equity))
        .layer(trace_layer)
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let addr = format!("0.0.0.0:{}", port);
    info!(%addr, "Quant API 启动");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> impl IntoResponse {
    let start = Instant::now();
    let resp = Json(json!({
        "code": 0,
        "message": "ok",
        "service": "quant-api",
        "version": env!("CARGO_PKG_VERSION"),
    }));
    info!(
        latency_us = start.elapsed().as_micros(),
        "健康检查"
    );
    resp
}
