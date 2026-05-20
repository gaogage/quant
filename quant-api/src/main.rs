//! Quant API 服务器 — Phase 1 数据底座

use axum::{
    http::Request,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("quant=info")),
        )
        .with_target(true)
        .init();

    dotenv::dotenv().ok();

    // 数据库连接池
    let db = quant_data::db::pool_from_env()
        .await
        .expect("数据库连接失败");
    info!("数据库已连接");

    // Tushare Pro 客户端
    let tushare =
        quant_data::tushare::client::TushareClient::from_env().expect("Tushare 客户端初始化失败");
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
        .route(
            "/api/v1/quant/backtests",
            get(routes::backtest::list_backtests).post(routes::backtest::run_backtest),
        )
        .route(
            "/api/v1/quant/backtests/{task_id}/summary",
            get(routes::backtest::backtest_summary),
        )
        .route(
            "/api/v1/quant/backtests/{task_id}/equity-curve",
            get(routes::backtest::backtest_equity_curve),
        )
        .route(
            "/api/v1/quant/backtests/run",
            post(routes::backtest::run_backtest),
        )
        .route(
            "/api/v1/quant/backtests/run-factor",
            post(routes::backtest::run_factor_backtest),
        )
        .route(
            "/api/v1/quant/backtests/run-prediction",
            post(routes::backtest::run_prediction_backtest),
        )
        // 数据同步
        .route(
            "/api/v1/quant/data/sync/stock-basic",
            post(routes::sync::sync_stock_basic),
        )
        .route(
            "/api/v1/quant/data/sync-tasks",
            post(routes::sync::create_sync_task),
        )
        .route(
            "/api/v1/quant/data/sync-tasks/{task_id}",
            get(routes::sync::sync_task_status),
        )
        .route(
            "/api/v1/quant/data/sync-tasks/{task_id}/cancel",
            post(routes::sync::cancel_sync_task),
        )
        .route(
            "/api/v1/quant/data/sync/daily",
            post(routes::sync::sync_daily),
        )
        .route(
            "/api/v1/quant/data/sync/daily/background",
            post(routes::sync::sync_daily_background),
        )
        .route(
            "/api/v1/quant/data/sync/tasks/{task_id}",
            get(routes::sync::sync_task_status),
        )
        .route(
            "/api/v1/quant/data/sync/tasks/{task_id}/cancel",
            post(routes::sync::cancel_sync_task),
        )
        .route(
            "/api/v1/quant/data/sync/adj-factor",
            post(routes::sync::sync_adj_factor),
        )
        .route(
            "/api/v1/quant/data/sync/adj-factor/background",
            post(routes::sync::sync_adj_factor_background),
        )
        .route(
            "/api/v1/quant/data/sync/index-daily",
            post(routes::sync::sync_index_daily),
        )
        .route(
            "/api/v1/quant/data/sync/trade-cal",
            post(routes::sync::sync_trade_cal),
        )
        .route(
            "/api/v1/quant/data/quality-check",
            post(routes::sync::quality_check),
        )
        .route("/api/v1/quant/data/stats", get(routes::sync::data_stats))
        .route(
            "/api/v1/quant/data/sync/financial",
            post(routes::sync::sync_financial),
        )
        // 因子
        .route(
            "/api/v1/quant/factors/list",
            get(routes::factors::list_factors),
        )
        .route(
            "/api/v1/quant/factors",
            get(routes::factors::list_factor_definitions),
        )
        .route(
            "/api/v1/quant/factor-definitions",
            get(routes::factors::list_factor_definitions)
                .post(routes::factors::register_factor_definition),
        )
        .route(
            "/api/v1/quant/factor-definitions/{factor_code}/{version}",
            get(routes::factors::get_factor_definition),
        )
        .route(
            "/api/v1/quant/factors/compute",
            post(routes::factors::compute_factor),
        )
        .route(
            "/api/v1/quant/factors/evaluate",
            post(routes::factors::evaluate_factor),
        )
        .route(
            "/api/v1/quant/factors/sync",
            post(routes::factors::sync_factor_values),
        )
        .route(
            "/api/v1/quant/factors/sync-financial",
            post(routes::factors::sync_financial_factor_values),
        )
        .route(
            "/api/v1/quant/factors/batch-sync",
            post(routes::factors::batch_sync_factors),
        )
        .route(
            "/api/v1/quant/factors/batch-sync/background",
            post(routes::factors::batch_sync_factors_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-price-volume-backfill/background",
            post(routes::factors::backfill_phase7_price_volume_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-financial-quality-backfill/background",
            post(routes::factors::backfill_phase7_financial_quality_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-industry-residual-quality-backfill/background",
            post(routes::factors::backfill_phase7_industry_residual_quality_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-relative-strength-backfill/background",
            post(routes::factors::backfill_phase7_relative_strength_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-quality-relative-strength-backfill/background",
            post(routes::factors::backfill_phase7_quality_relative_strength_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-growth-recovery-backfill/background",
            post(routes::factors::backfill_phase7_growth_recovery_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-valuation-backfill/background",
            post(routes::factors::backfill_phase7_valuation_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-moneyflow-backfill/background",
            post(routes::factors::backfill_phase7_moneyflow_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-event-alpha-backfill/background",
            post(routes::factors::backfill_phase7_event_alpha_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-event-surprise-backfill/background",
            post(routes::factors::backfill_phase7_event_surprise_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-event-window-alpha-backfill/background",
            post(routes::factors::backfill_phase7_event_window_alpha_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-alpha-blend-backfill/background",
            post(routes::factors::backfill_phase7_alpha_blend_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-alpha-blend-profiles-backfill/background",
            post(routes::factors::backfill_phase7_alpha_blend_profiles_background),
        )
        .route(
            "/api/v1/quant/factors/evaluate-all",
            post(routes::factors::evaluate_all_factors),
        )
        .route(
            "/api/v1/quant/factors/evaluate-all/background",
            post(routes::factors::evaluate_all_factors_background),
        )
        .route(
            "/api/v1/quant/factors/combine",
            post(routes::factors::combine_factors),
        )
        .route(
            "/api/v1/quant/factors/neutralize",
            post(routes::factors::neutralize_factors),
        )
        // ML / 预测集
        .route(
            "/api/v1/quant/ml/prediction-sets/linear-smoke",
            post(routes::ml::create_linear_prediction_set),
        )
        .route(
            "/api/v1/quant/ml/training-tasks/linear",
            post(routes::ml::train_linear_model),
        )
        .route(
            "/api/v1/quant/ml/prediction-sets/walk-forward-linear",
            post(routes::ml::create_walk_forward_linear_prediction_set),
        )
        .route(
            "/api/v1/quant/ml/prediction-sets/evaluate",
            post(routes::ml::evaluate_prediction_set),
        )
        // 组合风控
        .route(
            "/api/v1/quant/portfolio/reports/{task_id}",
            get(routes::portfolio::portfolio_report),
        )
        .route(
            "/api/v1/quant/portfolio/policies/{policy_id}",
            get(routes::portfolio::portfolio_policy),
        )
        // 仿真交易
        .route(
            "/api/v1/quant/paper/accounts",
            post(routes::paper::create_paper_account),
        )
        .route(
            "/api/v1/quant/paper/accounts/{account_id}",
            get(routes::paper::paper_account_summary),
        )
        .route(
            "/api/v1/quant/paper/orders",
            post(routes::paper::submit_paper_order),
        )
        .route(
            "/api/v1/quant/paper/orders/{order_id}/fills",
            post(routes::paper::fill_paper_order),
        )
        .route(
            "/api/v1/quant/paper/health",
            get(routes::paper::paper_health),
        )
        // 参数优化
        .route(
            "/api/v1/quant/optimizations",
            post(routes::optimization::create_optimization),
        )
        .route(
            "/api/v1/quant/optimizations/phase7-layered",
            post(routes::optimization::create_phase7_layered_optimization),
        )
        .route(
            "/api/v1/quant/optimizations/phase7-professional-discovery",
            post(routes::optimization::run_phase7_professional_discovery),
        )
        .route(
            "/api/v1/quant/optimizations/{optimization_task_id}",
            get(routes::optimization::get_optimization),
        )
        .route(
            "/api/v1/quant/optimizations/{optimization_task_id}/trials",
            get(routes::optimization::list_optimization_trials),
        )
        .route(
            "/api/v1/quant/optimizations/{optimization_task_id}/run",
            post(routes::optimization::run_optimization_trials),
        )
        .route(
            "/api/v1/quant/optimizations/{optimization_task_id}/promote",
            post(routes::optimization::promote_optimization_trial),
        )
        .route(
            "/api/v1/quant/optimizations/{optimization_task_id}/robustness-gates",
            post(routes::optimization::evaluate_optimization_robustness),
        )
        .route(
            "/api/v1/quant/optimizations/{optimization_task_id}/trials/{trial_id}/robustness-gates",
            post(routes::optimization::evaluate_optimization_trial_robustness),
        )
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
    Json(json!({"code":0,"message":"ok","service":"quant-api"}))
}
