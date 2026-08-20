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
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, Span};
use tracing_subscriber::{fmt::time::LocalTime, EnvFilter};
use uuid::Uuid;

mod auth;
mod phase7_alpha_admission;
mod routes;
mod sync_task_registry;

use sync_task_registry::new_registry;

pub struct AppState {
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub db: sqlx::PgPool,
    pub tushare: quant_data::tushare::client::TushareClient,
    /// 后台同步任务注册表:保存 tokio::spawn 的 AbortHandle,
    /// 供 cancel_sync_task 主动 abort 僵尸 task(见 sync_task_registry 模块)。
    pub sync_tasks: Arc<sync_task_registry::SyncTaskRegistry>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_timer(LocalTime::rfc_3339())
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("quant=info")),
        )
        .with_target(true)
        .init();

    dotenv::dotenv().ok();

    // 启动时校验 JWT_SECRET:release 构建未设置则 panic(fail-fast)。
    auth::jwt::validate_secret_at_startup();

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
        sync_tasks: new_registry(),
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
        .route(
            "/api/v1/quant/backtests/cleanup-stale",
            post(routes::backtest::cleanup_stale_backtest_tasks),
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
            "/api/v1/quant/data/sync-tasks/cleanup-stale",
            post(routes::sync::cleanup_stale_sync_tasks),
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
            "/api/v1/quant/data/sync/adj-factor/backfill",
            post(routes::sync::sync_adj_factor_backfill),
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
            "/api/v1/quant/data/sync/suspension/derive-from-daily",
            post(routes::sync::derive_suspension_from_daily),
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
        .route(
            "/api/v1/quant/data/account-data-health",
            post(routes::sync::account_data_health),
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
        // 蓝图进度
        .route(
            "/api/v1/quant/blueprint/progress",
            get(routes::blueprint::blueprint_progress),
        )
        .route("/api/v1/quant/data/stats", get(routes::sync::data_stats))
        .route(
            "/api/v1/quant/data/phase7-feasibility-audit",
            get(routes::sync::phase7_feasibility_audit),
        )
        .route(
            "/api/v1/quant/data/futures-price-chain/schema-contract",
            get(routes::sync::futures_price_chain_schema_contract),
        )
        .route(
            "/api/v1/quant/data/equity-pledge-pressure/schema-contract",
            get(routes::sync::equity_pledge_pressure_schema_contract),
        )
        .route(
            "/api/v1/quant/data/margin-detail/schema-contract",
            get(routes::sync::margin_detail_schema_contract),
        )
        .route(
            "/api/v1/quant/data/shareholder-structure/schema-contract",
            get(routes::sync::shareholder_structure_schema_contract),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/schema-contract",
            get(routes::sync::exchange_announcement_order_capacity_schema_contract),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/next-source-admission-plan",
            get(routes::sync::exchange_announcement_order_capacity_next_source_admission_plan),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/source-contract",
            get(routes::sync::structured_order_capacity_price_chain_source_contract),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/vendor-admission-plan",
            get(routes::sync::structured_order_capacity_price_chain_vendor_admission_plan),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/source-evidence-inventory",
            get(routes::sync::structured_order_capacity_price_chain_source_evidence_inventory),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/cninfo-access-smoke-contract",
            get(routes::sync::structured_order_capacity_price_chain_cninfo_access_smoke_contract),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-contract",
            get(routes::sync::structured_order_capacity_price_chain_cninfo_operator_evidence_contract),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-audit",
            get(routes::sync::structured_order_capacity_price_chain_cninfo_operator_evidence_audit),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/cninfo-permission-sample-smoke-plan",
            get(routes::sync::structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan),
        )
        .route(
            "/api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-manifest-template",
            get(routes::sync::structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/manual-schema-review",
            get(routes::sync::exchange_announcement_order_capacity_manual_schema_review),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/sync-plan",
            get(routes::sync::exchange_announcement_order_capacity_sync_plan),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/coverage-quality-audit-contract",
            get(routes::sync::exchange_announcement_order_capacity_coverage_quality_audit_contract),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/coverage-quality-audit",
            get(routes::sync::exchange_announcement_order_capacity_coverage_quality_audit),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/admission-readiness-audit",
            get(routes::sync::exchange_announcement_order_capacity_admission_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/manual-precision-sample-audit",
            get(routes::sync::exchange_announcement_order_capacity_manual_precision_sample_audit),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/sync",
            post(routes::sync::exchange_announcement_order_capacity_sync),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/bounded-sync",
            post(routes::sync::exchange_announcement_order_capacity_bounded_sync),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/permission-smoke",
            post(routes::sync::exchange_announcement_order_capacity_permission_smoke),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/detail-audit",
            post(routes::sync::exchange_announcement_order_capacity_detail_audit),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/pdf-parser-readiness",
            get(routes::sync::exchange_announcement_order_capacity_pdf_parser_readiness),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/pdf-detail-audit",
            post(routes::sync::exchange_announcement_order_capacity_pdf_detail_audit),
        )
        .route(
            "/api/v1/quant/data/exchange-announcement-order-capacity/ocr-blocked-row-audit",
            get(routes::sync::exchange_announcement_order_capacity_ocr_blocked_row_audit),
        )
        .route(
            "/api/v1/quant/data/margin-detail/readiness-audit",
            get(routes::sync::margin_detail_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/margin-detail/coverage-audit",
            get(routes::sync::margin_detail_coverage_audit),
        )
        .route(
            "/api/v1/quant/data/margin-detail/sync-plan",
            get(routes::sync::margin_detail_sync_plan),
        )
        .route(
            "/api/v1/quant/data/margin-detail/sync",
            post(routes::sync::margin_detail_sync),
        )
        .route(
            "/api/v1/quant/data/shareholder-structure/readiness-audit",
            get(routes::sync::shareholder_structure_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/shareholder-structure/coverage-audit",
            get(routes::sync::shareholder_structure_coverage_audit),
        )
        .route(
            "/api/v1/quant/data/shareholder-structure/sync-plan",
            get(routes::sync::shareholder_structure_sync_plan),
        )
        .route(
            "/api/v1/quant/data/shareholder-structure/sync",
            post(routes::sync::shareholder_structure_sync),
        )
        .route(
            "/api/v1/quant/data/equity-pledge-pressure/readiness-audit",
            get(routes::sync::equity_pledge_pressure_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/equity-pledge-pressure/coverage-audit",
            get(routes::sync::equity_pledge_pressure_coverage_audit),
        )
        .route(
            "/api/v1/quant/data/equity-pledge-pressure/sync",
            post(routes::sync::equity_pledge_pressure_sync),
        )
        .route(
            "/api/v1/quant/data/futures-price-chain/readiness-audit",
            get(routes::sync::futures_price_chain_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/futures-price-chain/mapping-audit",
            get(routes::sync::futures_price_chain_mapping_audit),
        )
        .route(
            "/api/v1/quant/data/futures-price-chain/coverage-audit",
            get(routes::sync::futures_price_chain_coverage_audit),
        )
        .route(
            "/api/v1/quant/data/futures-price-chain/mapping-template",
            get(routes::sync::futures_price_chain_mapping_template),
        )
        .route(
            "/api/v1/quant/data/futures-price-chain/mapping-validate",
            post(routes::sync::futures_price_chain_mapping_validate),
        )
        .route(
            "/api/v1/quant/data/futures-price-chain/sync",
            post(routes::sync::futures_price_chain_sync),
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
            "/api/v1/quant/data/phase7-share-float-coverage-batches",
            post(routes::sync::phase7_share_float_coverage_batches),
        )
        .route(
            "/api/v1/quant/data/phase7-share-float-readiness-audit",
            post(routes::sync::phase7_share_float_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/phase7-industry-membership-coverage-audit",
            get(routes::sync::phase7_industry_membership_coverage_audit),
        )
        .route(
            "/api/v1/quant/data/tushare/permission-smoke",
            post(routes::sync::tushare_permission_smoke),
        )
        .route(
            "/api/v1/quant/data/main-business/available-at-audit",
            post(routes::sync::main_business_available_at_audit),
        )
        .route(
            "/api/v1/quant/data/main-business/readiness-audit",
            get(routes::sync::main_business_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/broad-analyst-revision/audit",
            get(routes::sync::broad_analyst_revision_audit),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/schema-contract",
            get(routes::sync::akshare_analyst_revision_schema_contract),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/permission-smoke",
            post(routes::sync::akshare_analyst_revision_permission_smoke),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/available-at-audit",
            get(routes::sync::akshare_analyst_revision_available_at_audit),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/history-replay-audit",
            post(routes::sync::akshare_analyst_revision_history_replay_audit),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/sync-plan",
            get(routes::sync::akshare_analyst_revision_sync_plan),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/sync",
            post(routes::sync::akshare_analyst_revision_sync),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/readiness-audit",
            get(routes::sync::akshare_analyst_revision_readiness_audit),
        )
        .route(
            "/api/v1/quant/data/akshare/analyst-revision/coverage-audit",
            get(routes::sync::akshare_analyst_revision_coverage_audit),
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
            "/api/v1/quant/factors/p42b-large-cap-momentum-reversal-backfill/background",
            post(routes::factors::backfill_p42b_large_cap_momentum_reversal_background),
        )
        .route(
            "/api/v1/quant/factors/p42b-defensive-low-vol-quality-backfill/background",
            post(routes::factors::backfill_p42b_defensive_low_vol_quality_background),
        )
        .route(
            "/api/v1/quant/factors/materialize-pit-combo/background",
            post(routes::factors::materialize_pit_combo_background),
        )
        .route(
            "/api/v1/quant/factors/p42b-overlay-combo/materialize/background",
            post(routes::factors::materialize_p42b_overlay_combo_background),
        )
        .route(
            "/api/v1/quant/factors/evaluate-rolling-pit/background",
            post(routes::factors::evaluate_rolling_pit_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-financial-quality-backfill/background",
            post(routes::factors::backfill_phase7_financial_quality_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-financial-quality-change-backfill/background",
            post(routes::factors::backfill_phase7_financial_quality_change_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-earnings-recovery-persistence-backfill/background",
            post(routes::factors::backfill_phase7_earnings_recovery_persistence_background),
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
            "/api/v1/quant/factors/phase7-moneyflow-congestion-backfill/background",
            post(routes::factors::backfill_phase7_moneyflow_congestion_background),
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
            "/api/v1/quant/factors/phase7-forecast-revision-surprise-backfill/background",
            post(routes::factors::backfill_phase7_forecast_revision_surprise_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-repurchase-supply-shock-backfill/background",
            post(routes::factors::backfill_phase7_repurchase_supply_shock_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-block-trade-supply-demand-backfill/background",
            post(routes::factors::backfill_phase7_block_trade_supply_demand_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-unlock-supply-pressure-backfill/background",
            post(routes::factors::backfill_phase7_unlock_supply_pressure_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-supply-float-shock-backfill/background",
            post(routes::factors::backfill_phase7_supply_float_shock_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-liquidity-quality-backfill/background",
            post(routes::factors::backfill_phase7_liquidity_quality_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-market-residual-risk-backfill/background",
            post(routes::factors::backfill_phase7_market_residual_risk_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-industry-prosperity-backfill/background",
            post(routes::factors::backfill_phase7_industry_prosperity_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-futures-price-chain-backfill/background",
            post(routes::factors::backfill_phase7_futures_price_chain_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-equity-pledge-pressure-backfill/background",
            post(routes::factors::backfill_phase7_equity_pledge_pressure_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-shareholder-structure-backfill/background",
            post(routes::factors::backfill_phase7_shareholder_structure_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-margin-detail-backfill/background",
            post(routes::factors::backfill_phase7_margin_detail_background),
        )
        .route(
            "/api/v1/quant/factors/phase7-analyst-revision-backfill/background",
            post(routes::factors::backfill_phase7_analyst_revision_background),
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
        .route(
            "/api/v1/quant/ml/prediction-sets/readiness/report",
            post(routes::ml::report_prediction_set_readiness),
        )
        .route(
            "/api/v1/quant/ml/feature-profiles/readiness/report",
            post(routes::optimization::report_feature_profile_readiness),
        )
        .route(
            "/api/v1/quant/alpha-sources/diagnostics/report",
            post(routes::optimization::report_alpha_source_diagnostics),
        )
        .route(
            "/api/v1/quant/alpha-sources/main-business/diagnostics/report",
            post(routes::optimization::report_main_business_diagnostics),
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
            post(routes::historical_replay::historical_replay),
        )
        .route(
            "/api/v1/quant/paper/mvo-benchmark-sync",
            post(routes::historical_replay::mvo_benchmark_sync),
        )
        // 回撤归因
        .route(
            "/api/v1/quant/attribution/drawdown",
            post(routes::attribution::drawdown_attribution),
        )
        // 因子分桶 IC 分析
        .route(
            "/api/v1/quant/factors/analysis/bucketed-ic",
            post(routes::factor_analysis::bucketed_ic_analysis),
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
            "/api/v1/quant/optimizations/cleanup-stale",
            post(routes::optimization::cleanup_stale_optimization_tasks),
        )
        .route(
            "/api/v1/quant/experiments/return-risk-cache-economics/report",
            post(routes::optimization::report_return_risk_cache_economics),
        )
        .route(
            "/api/v1/quant/experiments/cleanup-stale",
            post(routes::optimization::cleanup_stale_experiment_runs),
        )
        .route(
            "/api/v1/quant/experiments/{experiment_run_id}/sleeve-admission-diagnostics",
            get(routes::optimization::get_sleeve_admission_diagnostics),
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
        .route(
            "/api/v1/users/me/password",
            axum::routing::put(routes::users::change_password),
        )
        // ── 管理 ──
        .route("/api/v1/admin/users", get(routes::admin::list_users))
        .route("/api/v1/admin/users", post(routes::admin::create_user))
        .route(
            "/api/v1/admin/users/{id}",
            axum::routing::put(routes::admin::update_user),
        )
        .route(
            "/api/v1/admin/users/{id}",
            axum::routing::delete(routes::admin::delete_user),
        )
        .route(
            "/api/v1/admin/users/{id}/password",
            axum::routing::put(routes::admin::reset_user_password),
        )
        .route("/api/v1/admin/tasks", get(routes::admin::list_tasks))
        .route(
            "/api/v1/admin/tasks/{name}",
            axum::routing::put(routes::admin::update_task),
        )
        .route(
            "/api/v1/admin/tasks/{name}/run",
            post(routes::admin::run_task),
        )
        .route(
            "/api/v1/admin/tasks/check-deps",
            get(routes::admin::check_task_deps),
        )
        .route(
            "/api/v1/admin/ml/rebuild-full-universe",
            post(routes::admin::rebuild_full_universe),
        )
        .route(
            "/api/v1/admin/rebalance",
            post(routes::admin::manual_rebalance),
        )
        .route(
            "/api/v1/admin/performance-report",
            post(routes::admin::trigger_performance_report),
        )
        .route(
            "/api/v1/admin/trade-detail-notification",
            post(routes::admin::trigger_trade_detail_notification),
        )
        .route("/api/v1/admin/sync/status", get(routes::admin::sync_status))
        .route(
            "/api/v1/admin/sync/repair",
            post(routes::admin::repair_sync),
        )
        .route(
            "/api/v1/admin/factor-health",
            get(routes::admin::factor_health),
        )
        // ── 策略 ──
        .route(
            "/api/v1/strategies",
            get(routes::strategies::list_strategies),
        )
        .route(
            "/api/v1/strategies",
            post(routes::strategies::create_strategy),
        )
        .route(
            "/api/v1/strategies/{id}",
            get(routes::strategies::get_strategy),
        )
        .route(
            "/api/v1/strategies/{id}",
            axum::routing::put(routes::strategies::update_strategy),
        )
        .route(
            "/api/v1/strategies/{id}",
            axum::routing::delete(routes::strategies::delete_strategy),
        )
        .route(
            "/api/v1/strategies/{id}/equity-curve/sync",
            post(routes::equity_curve_sync::handle_equity_curve_sync),
        )
        .route(
            "/api/v1/strategies/{id}/equity-curve/readiness-audit",
            get(routes::equity_curve_sync::handle_equity_curve_readiness_audit),
        )
        .route(
            "/api/v1/strategies/{id}/equity-curve/composite-sync",
            post(routes::equity_curve_sync::handle_composite_equity_curve_sync),
        )
        // ── 账号 ──
        .route("/api/v1/accounts", get(routes::accounts::list_accounts))
        .route("/api/v1/accounts", post(routes::accounts::create_account))
        .route(
            "/api/v1/accounts/{id}",
            get(routes::accounts::account_detail),
        )
        .route(
            "/api/v1/accounts/{id}",
            axum::routing::put(routes::accounts::update_account),
        )
        .route(
            "/api/v1/accounts/{id}",
            axum::routing::delete(routes::accounts::delete_account),
        )
        .route(
            "/api/v1/accounts/{id}/reset",
            post(routes::accounts::reset_account),
        )
        .route(
            "/api/v1/accounts/{id}/push-dingtalk",
            post(routes::accounts::push_account_dingtalk),
        )
        .route(
            "/api/v1/accounts/{id}/nav-history",
            get(routes::accounts::nav_history),
        )
        .route(
            "/api/v1/accounts/{id}/rebalance-history",
            get(routes::accounts::rebalance_history),
        )
        .layer(CorsLayer::permissive())
        .layer(trace_layer);

    // 提取 db 用于后台调度器 (必须在 with_state 之前, 因为 state 会被 move)
    let db_for_scheduler = state.db.clone();
    let app = app.with_state(state);

    // 前端静态托管：单端口同时供 API + 前端 SPA。
    // /api/* 已由上面路由匹配，未匹配的请求落到此处：
    //   - 命中静态文件(wasm/js/css) → ServeDir 返回
    //   - 其余(SPA 前端路由) → 回退 index.html
    // dist 路径：env QUANT_UI_DIST(生产) 优先，否则相对 quant-ui/dist(本地开发)
    let ui_dist = std::env::var("QUANT_UI_DIST").unwrap_or_else(|_| "quant-ui/dist".to_string());
    // 静态服务：未命中文件回退 index.html(SPA)。
    // 配套 no-cache 中间件(下方)强制浏览器每次 revalidate —— 防止部署新版后
    // 浏览器用旧缓存的 index.html(引用已删除的旧 hash WASM)导致白屏/点击无反应。
    let app = app.fallback_service(
        ServeDir::new(&ui_dist).fallback(ServeFile::new(format!("{}/index.html", ui_dist))),
    );
    // 全局加 Cache-Control: no-cache。带 hash 的 WASM/JS 内容寻址，no-cache 仅多
    // 一次 304 校验；API 本就不该缓存。用 from_fn 处理标准 axum 响应，类型明确。
    let app = app.layer(axum::middleware::from_fn(
        |req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| async move {
            let mut resp = next.run(req).await;
            resp.headers_mut()
                .entry(axum::http::header::CACHE_CONTROL)
                .or_insert(axum::http::HeaderValue::from_static("no-cache"));
            resp
        },
    ));
    info!(%ui_dist, "前端静态托管已挂载 (SPA fallback + no-cache)");

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);
    let addr = format!("0.0.0.0:{}", port);
    info!(%addr, "Quant API 启动");

    // 启动后台调度器（数据同步 + 模拟交易）。本地接口验证可显式关闭，避免误触发 EOD 同步。
    if std::env::var("QUANT_DISABLE_SCHEDULER").ok().as_deref() == Some("1") {
        info!("后台调度器已通过 QUANT_DISABLE_SCHEDULER=1 跳过");
    } else {
        routes::scheduler::start_scheduler(db_for_scheduler, tushare_for_scheduler, port);
    }

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> impl IntoResponse {
    Json(json!({"code":0,"message":"ok","service":"quant-api"}))
}
