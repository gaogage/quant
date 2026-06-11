//! Quant API 服务器 — Phase 1 数据底座

use axum::{
    http::Request,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, Span};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

mod auth;
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

    // Clone for scheduler (before move into AppState)
    let tushare_for_scheduler = tushare.clone();

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
            "/api/v1/quant/data/sync/fund-adj",
            post(routes::sync::sync_fund_adj),
        )
        .route(
            "/api/v1/quant/data/sync/moneyflow-hsgt",
            post(routes::sync::sync_moneyflow_hsgt),
        )
        .route(
            "/api/v1/quant/data/sync/margin",
            post(routes::sync::sync_margin),
        )
        .route(
            "/api/v1/quant/data/sync/fund-basic",
            post(routes::sync::sync_fund_basic),
        )
        .route(
            "/api/v1/quant/data/sync/namechange",
            post(routes::sync::sync_namechange),
        )
        .route(
            "/api/v1/quant/data/sync/suspension",
            post(routes::sync::sync_suspension),
        )
        .route(
            "/api/v1/quant/data/sync/suspension/backfill",
            post(routes::sync::sync_suspension_backfill),
        )
        .route(
            "/api/v1/quant/data/sync/limit",
            post(routes::sync::sync_limit_list),
        )
        .route(
            "/api/v1/quant/data/sync/limit/backfill",
            post(routes::sync::sync_limit_backfill),
        )
        .route(
            "/api/v1/quant/data/sync/historical",
            post(routes::sync::sync_historical),
        )
        .route(
            "/api/v1/quant/data/sync/fund-daily",
            post(routes::sync::sync_fund_daily),
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
        // 数据清理
        .route(
            "/api/v1/quant/data/cleanup/stats",
            get(routes::cleanup::cleanup_stats),
        )
        .route(
            "/api/v1/quant/data/cleanup/preview",
            post(routes::cleanup::cleanup_preview),
        )
        .route(
            "/api/v1/quant/data/cleanup",
            post(routes::cleanup::execute_cleanup),
        )
        .route(
            "/api/v1/quant/data/cleanup/backtest-tasks/{task_id}/keep",
            post(routes::cleanup::mark_backtest_kept),
        )
        .route(
            "/api/v1/quant/data/cleanup/expired-stats",
            get(routes::cleanup::expired_stats),
        )
        .route("/api/v1/quant/data/stats", get(routes::sync::data_stats))
        .route(
            "/api/v1/quant/data/phase7-feasibility-audit",
            get(routes::sync::phase7_feasibility_audit),
        )
        .route(
            "/api/v1/quant/data/phase7-optional-source-coverage-sync",
            post(routes::sync::phase7_optional_source_coverage_sync),
        )
        .route(
            "/api/v1/quant/data/phase7-optional-source-coverage-batches",
            post(routes::sync::phase7_optional_source_coverage_batches),
        )
        .route(
            "/api/v1/quant/data/phase7-coverage-expansion-runner",
            post(routes::sync::phase7_coverage_expansion_runner),
        )
        .route(
            "/api/v1/quant/data/tushare/permission-smoke",
            post(routes::sync::tushare_permission_smoke),
        )
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
            "/api/v1/quant/factors/phase7-cashflow-quality-backfill/background",
            post(routes::factors::backfill_phase7_cashflow_quality_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-dividend-quality-backfill/background",
            post(routes::factors::backfill_phase7_dividend_quality_background),
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
            "/api/v1/quant/ml/training-tasks/nonlinear-quantile-ranker",
            post(routes::ml::train_nonlinear_quantile_ranker),
        )
        .route(
            "/api/v1/quant/ml/prediction-sets/walk-forward-linear",
            post(routes::ml::create_walk_forward_linear_prediction_set),
        )
        .route(
            "/api/v1/quant/ml/prediction-sets/walk-forward-nonlinear-quantile-ranker",
            post(routes::ml::create_walk_forward_nonlinear_quantile_ranker_prediction_set),
        )
        .route(
            "/api/v1/quant/ml/prediction-sets/evaluate",
            post(routes::ml::evaluate_prediction_set),
        )
        .route(
            "/api/v1/quant/ml/prediction-sets/cache-economics/report",
            post(routes::ml::report_prediction_set_cache_economics),
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
        // MVO 多资产配置
        .route(
            "/api/v1/quant/portfolio/mvo-backtest",
            post(routes::portfolio::mvo_backtest),
        )
        .route(
            "/api/v1/quant/backtests/{task_id}/mvo-simulate",
            post(routes::portfolio::mvo_simulate),
        )
        .route(
            "/api/v1/quant/experiments/{experiment_run_id}/mvo-overlay",
            post(routes::portfolio::mvo_experiment_overlay),
        )
        .route(
            "/api/v1/quant/experiments/{experiment_run_id}/blueprint-report",
            get(routes::portfolio::blueprint_report),
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
        .route(
            "/api/v1/quant/paper/signals/generate",
            post(routes::paper::generate_paper_signals),
        )
        .route(
            "/api/v1/quant/paper/nav/compute",
            post(routes::paper::compute_paper_nav),
        )
        .route(
            "/api/v1/quant/paper/nav/simulate",
            post(routes::paper::simulate_paper_nav),
        )
        .route(
            "/api/v1/quant/paper/nav/simulate-multi",
            post(routes::paper::simulate_multi_window),
        )
        .route(
            "/api/v1/quant/paper/historical-replay",
            post(routes::paper::historical_replay),
        )
        .route(
            "/api/v1/quant/paper/historical-replay-v19",
            post(routes::paper_v19_replay::historical_replay_v19),
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
            "/api/v1/quant/optimizations/phase7-oos-walk-forward-discovery",
            post(routes::optimization::run_phase7_oos_walk_forward_discovery),
        )
        .route(
            "/api/v1/quant/optimizations/phase7-oos-profile-comparison/plan",
            post(routes::optimization::plan_phase7_oos_profile_comparison),
        )
        .route(
            "/api/v1/quant/optimizations/phase7-oos-profile-comparison/smoke",
            post(routes::optimization::launch_phase7_oos_profile_comparison_smoke),
        )
        .route(
            "/api/v1/quant/experiments/return-risk-cache-economics/report",
            post(routes::optimization::report_return_risk_cache_economics),
        )
        .route(
            "/api/v1/quant/experiments/{experiment_run_id}",
            get(routes::optimization::get_experiment_run),
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
            "/api/v1/quant/optimizations/{optimization_task_id}/elite-validation-report",
            post(routes::optimization::generate_elite_validation_report),
        )
        .route(
            "/api/v1/quant/optimizations/{optimization_task_id}/trials/{trial_id}/robustness-gates",
            post(routes::optimization::evaluate_optimization_trial_robustness),
        )
        // ── 认证 ──
        .route("/api/v1/auth/login", post(routes::users::login))
        .route("/api/v1/auth/refresh", post(routes::users::refresh))
        .route("/api/v1/auth/logout", post(routes::users::logout))
        .route("/api/v1/users/me", get(routes::users::get_me))
        .route("/api/v1/users/me/password", axum::routing::put(routes::users::change_password))
        // ── 管理 ──
        .route("/api/v1/admin/users", get(routes::admin::list_users))
        .route("/api/v1/admin/users", post(routes::admin::create_user))
        .route("/api/v1/admin/users/{id}", axum::routing::put(routes::admin::update_user))
        .route("/api/v1/admin/users/{id}", axum::routing::delete(routes::admin::delete_user))
        .route("/api/v1/admin/users/{id}/password", axum::routing::put(routes::admin::reset_user_password))
        .route("/api/v1/admin/tasks", get(routes::admin::list_tasks))
        .route("/api/v1/admin/tasks/{name}", axum::routing::put(routes::admin::update_task))
        .route("/api/v1/admin/tasks/{name}/run", post(routes::admin::run_task))
        .route("/api/v1/admin/tasks/check-deps", get(routes::admin::check_task_deps))
        .route("/api/v1/admin/ml/rebuild-full-universe", post(routes::admin::rebuild_full_universe))
        .route("/api/v1/admin/sync/status", get(routes::admin::sync_status))
        .route("/api/v1/admin/sync/repair", post(routes::admin::repair_sync))
        // ── 策略 ──
        .route("/api/v1/strategies", get(routes::strategies::list_strategies))
        .route("/api/v1/strategies", post(routes::strategies::create_strategy))
        .route("/api/v1/strategies/{id}", get(routes::strategies::get_strategy))
        .route("/api/v1/strategies/{id}", axum::routing::put(routes::strategies::update_strategy))
        .route("/api/v1/strategies/{id}", axum::routing::delete(routes::strategies::delete_strategy))
        // ── 账号 ──
        .route("/api/v1/accounts", get(routes::accounts::list_accounts))
        .route("/api/v1/accounts", post(routes::accounts::create_account))
        .route("/api/v1/accounts/{id}", get(routes::accounts::account_detail))
        .route("/api/v1/accounts/{id}", axum::routing::put(routes::accounts::update_account))
        .route("/api/v1/accounts/{id}", axum::routing::delete(routes::accounts::delete_account))
        .route("/api/v1/accounts/{id}/reset", post(routes::accounts::reset_account))
        .route("/api/v1/accounts/{id}/push-dingtalk", post(routes::accounts::push_account_dingtalk))
        .layer(CorsLayer::permissive())
        .layer(trace_layer);

    // 提取 db 用于后台调度器 (必须在 with_state 之前, 因为 state 会被 move)
    let db_for_scheduler = state.db.clone();
    let app = app.with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);
    let addr = format!("0.0.0.0:{}", port);
    info!(%addr, "Quant API 启动");

    // 启动后台调度器（数据同步 + 模拟交易）
    routes::scheduler::start_scheduler(db_for_scheduler, tushare_for_scheduler, port);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> impl IntoResponse {
    Json(json!({"code":0,"message":"ok","service":"quant-api"}))
}
