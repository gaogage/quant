//! Quant API 服务器 — Phase 1 数据底座

use axum::{http::Request, response::IntoResponse, routing::{get, post}, Json, Router};
use serde_json::json;
use std::sync::Arc;
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

    // Tushare Pro 客户端
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
        .route("/api/v1/quant/backtests/run", post(routes::backtest::run_backtest))
        .route("/api/v1/quant/backtests/run-factor", post(routes::backtest::run_factor_backtest))
        // 数据同步
        .route("/api/v1/quant/data/sync/stock-basic", post(routes::sync::sync_stock_basic))
        .route("/api/v1/quant/data/sync/daily", post(routes::sync::sync_daily))
        .route("/api/v1/quant/data/sync/adj-factor", post(routes::sync::sync_adj_factor))
        .route("/api/v1/quant/data/sync/index-daily", post(routes::sync::sync_index_daily))
        .route("/api/v1/quant/data/sync/trade-cal", post(routes::sync::sync_trade_cal))
        .route("/api/v1/quant/data/quality-check", post(routes::sync::quality_check))
        .route("/api/v1/quant/data/stats", get(routes::sync::data_stats))
        .route("/api/v1/quant/data/sync/financial", post(routes::sync::sync_financial))
        // 因子
        .route("/api/v1/quant/factors/list", get(routes::factors::list_factors))
        .route("/api/v1/quant/factors/compute", post(routes::factors::compute_factor))
        .route("/api/v1/quant/factors/evaluate", post(routes::factors::evaluate_factor))
        .route("/api/v1/quant/factors/sync", post(routes::factors::sync_factor_values))
        .route("/api/v1/quant/factors/batch-sync", post(routes::factors::batch_sync_factors))
        .route("/api/v1/quant/factors/evaluate-all", post(routes::factors::evaluate_all_factors))
        .route("/api/v1/quant/factors/combine", post(routes::factors::combine_factors))
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
