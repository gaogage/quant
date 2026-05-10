//! Quant API 服务器 — Phase 1 数据底座

use axum::{http::Request, response::IntoResponse, routing::{get, post}, Json, Router};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, Span};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

mod routes;

pub struct AppState {
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub db: sqlx::PgPool,
    pub tushare: quant_data::tushare::client::TushareClient,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("quant=info")))
        .with_target(true)
        .init();

    dotenv::dotenv().ok();

    // 数据库连接池
    let db = quant_data::db::pool_from_env().await.expect("数据库连接失败");
    info!("数据库已连接");

    // Tushare 客户端
    let tushare = quant_data::tushare::client::TushareClient::from_env()
        .expect("Tushare 客户端初始化失败");
    info!("Tushare 客户端已初始化");

    let state = Arc::new(AppState {
        start_time: chrono::Utc::now(),
        db,
        tushare,
    });

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            info_span!("http", method=%request.method(), uri=%request.uri(), trace_id=%Uuid::new_v4())
        })
        .on_response(|resp: &axum::response::Response, latency: std::time::Duration, _span: &Span| {
            info!(status=resp.status().as_u16(), latency_ms=latency.as_millis(), "响应");
        });

    let app = Router::new()
        .route("/health", get(health))
        // 回测
        .route("/api/v1/quant/backtests", post(routes::create_backtest))
        .route("/api/v1/quant/backtests/{id}", get(routes::get_backtest))
        .route("/api/v1/quant/backtests/{id}/report", get(routes::get_report))
        // 数据同步
        .route("/api/v1/quant/data/sync/stock-basic", post(routes::sync::sync_stock_basic))
        .route("/api/v1/quant/data/sync/daily", post(routes::sync::sync_daily))
        .route("/api/v1/quant/data/stats", get(routes::sync::data_stats))
        .layer(trace_layer)
        .with_state(state);

    let port: u16 = std::env::var("PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(8080);
    let addr = format!("0.0.0.0:{}", port);
    info!(%addr, "Quant API 启动");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> impl IntoResponse {
    Json(json!({"code":0,"message":"ok","service":"quant-api"}))
}
