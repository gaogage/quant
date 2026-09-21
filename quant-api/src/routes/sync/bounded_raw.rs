use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀

use crate::AppState;

use super::*;

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OptionalSourceCoverageSyncReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub max_symbols: Option<usize>,
    #[serde(default)]
    pub offset_symbols: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct Phase7OptionalSourceCoverageBatchReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub start_offset: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct Phase7CoverageExpansionRunnerReq {
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
    #[serde(default)]
    pub stop_when_readiness_at_least_partial: Option<bool>,
    #[serde(default)]
    pub auto_continue: Option<bool>,
    #[serde(default)]
    pub max_rounds: Option<usize>,
    #[serde(default)]
    pub target_coverage_ratio: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]

pub struct Phase7ShareFloatCoverageReq {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub chunk_granularity: Option<String>,
    #[serde(default)]
    pub max_chunks: Option<usize>,
    #[serde(default)]
    pub plan_only: Option<bool>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub data_version_prefix: Option<String>,
}

pub(crate) async fn execute_sync_task(
    state: Arc<AppState>,
    task_id: String,
    req: DataSyncTaskReq,
) -> Result<serde_json::Value, String> {
    match req.dataset.as_str() {
        "stock_basic" => {
            let count = quant_data::sync::sync_stock_basic(&state.db, &state.tushare, &task_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "daily" | "stock_daily" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for daily sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_daily_bars(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "daily", "status": "completed", "count": count}),
            )
        }
        "daily_basic" | "stock_daily_basic" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_daily_basic(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "daily_basic", "status": "completed", "count": count}),
            )
        }
        "moneyflow" | "stock_moneyflow" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_moneyflow(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "moneyflow", "status": "completed", "count": count}),
            )
        }
        "moneyflow_hsgt" | "hsgt_moneyflow" => {
            let (start, end) = require_range(&req)?;
            let count =
                quant_data::sync::sync_moneyflow_hsgt(&state.db, &state.tushare, start, end)
                    .await
                    .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "moneyflow_hsgt", "status": "completed", "count": count}),
            )
        }
        "margin" | "market_margin" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_margin(&state.db, &state.tushare, start, end)
                .await
                .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "margin", "status": "completed", "count": count}),
            )
        }
        "margin_detail" | "market_stock_margin_detail" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_margin_detail(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "margin_detail", "status": "completed", "count": count}),
            )
        }
        "block_trade" | "market_stock_block_trade" => {
            let (start, end) = require_range(&req)?;
            let count =
                quant_data::sync::sync_block_trade(&state.db, &state.tushare, &task_id, start, end)
                    .await
                    .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "block_trade", "status": "completed", "count": count}),
            )
        }
        "industry_membership" | "market_stock_industry_membership_pit" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_industry_membership(
                &state.db,
                &state.tushare,
                &task_id,
                &req.index_codes,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                count as i32,
                count as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "industry_membership", "status": "completed", "count": count}),
            )
        }
        "forecast" | "stock_forecast" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_forecast(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "forecast", "status": "completed", "count": count}),
            )
        }
        "express" | "stock_express" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_express(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "express", "status": "completed", "count": count}),
            )
        }
        "disclosure_date" | "stock_disclosure_date" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_disclosure_date(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "disclosure_date", "status": "completed", "count": count}),
            )
        }
        "cashflow" | "stock_cashflow" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for cashflow sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_cashflow(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "cashflow", "status": "completed", "count": count}),
            )
        }
        "dividend" | "stock_dividend" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for dividend sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_dividend(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "dividend", "status": "completed", "count": count}),
            )
        }
        "repurchase" | "stock_repurchase" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for repurchase sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_repurchase(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "repurchase", "status": "completed", "count": count}),
            )
        }
        "share_float" | "stock_share_float" => {
            if req.symbols.is_empty() && !optional_source_all_symbols_allowed(req.mode.as_deref()) {
                return Err(
                    "symbols must not be empty for share_float sync unless mode=full_market is set"
                        .into(),
                );
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_share_float(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "share_float", "status": "completed", "count": count}),
            )
        }
        "main_business" | "stock_main_business" => {
            let (start, end) = require_range(&req)?;
            let business_type = req
                .mode
                .as_deref()
                .filter(|value| matches!(*value, "P" | "D" | "I"))
                .unwrap_or("P");
            let count = quant_data::sync::sync_main_business(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                start,
                end,
                business_type,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "main_business", "status": "completed", "count": count}),
            )
        }
        "futures_price_chain" | "futures_price_chain_raw" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_futures_price_chain(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                &req.exchanges,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "futures_price_chain", "status": "completed", "count": count}),
            )
        }
        "equity_pledge_pressure" | "equity_pledge_pressure_raw" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_equity_pledge_pressure(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "equity_pledge_pressure", "status": "completed", "count": count}),
            )
        }
        "shareholder_structure" | "shareholder_structure_raw" => {
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_shareholder_structure(
                &state.db,
                &state.tushare,
                &task_id,
                &req.symbols,
                &req.source_filters,
                start,
                end,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": "shareholder_structure", "status": "completed", "count": count}),
            )
        }
        "adj_factor" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for adj_factor sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_adj_factor(
                &state.db,
                &state.tushare,
                &req.symbols,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "index_daily" => {
            let codes = if req.index_codes.is_empty() {
                &req.symbols
            } else {
                &req.index_codes
            };
            if codes.is_empty() {
                return Err("index_codes must not be empty for index_daily sync".into());
            }
            let (start, end) = require_range(&req)?;
            let count = quant_data::sync::sync_index_daily(
                &state.db,
                &state.tushare,
                codes,
                start,
                end,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": count}),
            )
        }
        "trade_cal" => {
            let exchanges = if req.exchanges.is_empty() {
                vec!["SSE".to_string(), "SZSE".to_string()]
            } else {
                req.exchanges.clone()
            };
            let mut total = 0usize;
            for exchange in &exchanges {
                let child_task_id = format!("{}-{}", task_id, exchange.to_lowercase());
                total += quant_data::sync::sync_trade_calendar_with_task(
                    &state.db,
                    &state.tushare,
                    exchange,
                    &child_task_id,
                )
                .await
                .map_err(|e| e.to_string())?;
            }
            quant_data::repository::update_sync_task(
                &state.db,
                &task_id,
                "completed",
                total as i32,
                total as i32,
                0,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(
                json!({"task_id": task_id, "dataset": req.dataset, "status": "completed", "count": total}),
            )
        }
        "financial" => {
            if req.symbols.is_empty() {
                return Err("symbols must not be empty for financial sync".into());
            }
            let (statements, indicators) = quant_data::sync::sync_financial_data_with_task(
                &state.db,
                &state.tushare,
                &req.symbols,
                &task_id,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(json!({
                "task_id": task_id,
                "dataset": req.dataset,
                "status": "completed",
                "statements": statements,
                "indicators": indicators
            }))
        }
        other => Err(format!("unsupported dataset: {}", other)),
    }
}

// ── 第六批覆盖率测试：execute_sync_task 参数校验与早退分支 ──
// 模式沿用 coverage_batches.rs fourth_batch：直调 inner 函数，仅覆盖
// 空 symbols / full_market 守卫 / 缺日期 / 未知 dataset 等校验早退分支，
// 这些分支在触碰数据库与 Tushare 客户端之前返回，不产生任何写库副作用。
#[cfg(test)]
mod sixth_batch {
    use super::*;

    /// 构造真实本机 PG 连接（DATABASE_URL 缺省 postgres://gaocheng@localhost/quant）。
    /// 早退分支不使用 db/tushare，但函数签名要求完整 AppState。
    async fn test_app_state() -> crate::AppState {
        dotenv::dotenv().ok();
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("test db connect");
        crate::AppState {
            start_time: chrono::Utc::now(),
            db,
            tushare: quant_data::tushare::client::TushareClient::from_env()
                .expect("Tushare client init（dotenv 加载 quant/.env 后需 TUSHARE_TOKEN）"),
            sync_tasks: crate::sync_task_registry::new_registry(),
        }
    }

    /// DataSyncTaskReq 无 Default derive，手工列全字段构造测试请求。
    fn make_req(dataset: &str) -> DataSyncTaskReq {
        DataSyncTaskReq {
            dataset: dataset.to_string(),
            source: super::default_source(),
            mode: None,
            symbols: Vec::new(),
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: None,
            end_date: None,
            data_version_id: None,
            background: false,
            quality_check: false,
            create_data_version: false,
            retry_of_task_id: None,
            reason: None,
        }
    }

    /// 早退分支不落库，task_id 仅作返回值标识，仍用 zzz_test_ 前缀保持卫生。
    const TEST_TASK_ID: &str = "zzz_test_sixth_bounded_early_exit";

    #[tokio::test]
    async fn execute_daily_and_stock_daily_require_nonempty_symbols() {
        let state = std::sync::Arc::new(test_app_state().await);
        for dataset in ["daily", "stock_daily"] {
            let req = make_req(dataset);
            let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), req)
                .await
                .expect_err("daily 空 symbols 必须拒绝");
            assert!(
                error.contains("symbols must not be empty for daily sync"),
                "dataset={dataset} 实际错误: {error}"
            );
        }
    }

    #[tokio::test]
    async fn execute_adj_factor_and_financial_require_nonempty_symbols() {
        let state = std::sync::Arc::new(test_app_state().await);
        let adj_req = make_req("adj_factor");
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), adj_req)
            .await
            .expect_err("adj_factor 空 symbols 必须拒绝");
        assert!(
            error.contains("symbols must not be empty for adj_factor sync"),
            "实际错误: {error}"
        );

        let fin_req = make_req("financial");
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), fin_req)
            .await
            .expect_err("financial 空 symbols 必须拒绝");
        assert!(
            error.contains("symbols must not be empty for financial sync"),
            "实际错误: {error}"
        );
    }

    #[tokio::test]
    async fn execute_index_daily_requires_codes_from_either_field() {
        let state = std::sync::Arc::new(test_app_state().await);
        // index_codes 与 symbols 双空 → 明确报错
        let empty_req = make_req("index_daily");
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), empty_req)
            .await
            .expect_err("index_daily 双空必须拒绝");
        assert!(
            error.contains("index_codes must not be empty for index_daily sync"),
            "实际错误: {error}"
        );

        // index_codes 有值 → 通过 codes 校验后在日期校验早退（不触 Tushare）
        let mut req = make_req("index_daily");
        req.index_codes = vec!["000001.SH".to_string()];
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), req)
            .await
            .expect_err("index_daily 缺日期必须拒绝");
        assert_eq!(error, "start_date is required");

        // symbols 兜底：仅提供 symbols 同样通过 codes 校验
        let mut fallback_req = make_req("index_daily");
        fallback_req.symbols = vec!["000300.SH".to_string()];
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), fallback_req)
            .await
            .expect_err("index_daily 仅 symbols 时缺日期必须拒绝");
        assert_eq!(error, "start_date is required");
    }

    #[tokio::test]
    async fn execute_optional_sources_guard_empty_symbols_behind_full_market_mode() {
        let state = std::sync::Arc::new(test_app_state().await);
        for dataset in ["cashflow", "dividend", "repurchase", "share_float"] {
            // 无 mode：空 symbols 直接拒绝
            let req = make_req(dataset);
            let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), req)
                .await
                .expect_err("可选源空 symbols 必须拒绝");
            assert!(
                error.contains(&format!(
                    "symbols must not be empty for {dataset} sync unless mode=full_market is set"
                )),
                "dataset={dataset} 实际错误: {error}"
            );

            // mode=full_market：守卫放行，推进到日期校验早退
            let mut full_market_req = make_req(dataset);
            full_market_req.mode = Some("full_market".to_string());
            let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), full_market_req)
                .await
                .expect_err("full_market 放行后缺日期必须拒绝");
            assert_eq!(
                error, "start_date is required",
                "dataset={dataset} 实际错误: {error}"
            );
        }
    }

    #[tokio::test]
    async fn execute_range_bound_datasets_validate_date_presence() {
        let state = std::sync::Arc::new(test_app_state().await);
        // daily_basic：symbols 非空但缺 start_date
        let mut missing_start = make_req("daily_basic");
        missing_start.symbols = vec!["600000.SH".to_string()];
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), missing_start)
            .await
            .expect_err("缺 start_date 必须拒绝");
        assert_eq!(error, "start_date is required");

        // moneyflow：补 start 后缺 end_date
        let mut missing_end = make_req("moneyflow");
        missing_end.symbols = vec!["600000.SH".to_string()];
        missing_end.start_date = Some("20240101".into());
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), missing_end)
            .await
            .expect_err("缺 end_date 必须拒绝");
        assert_eq!(error, "end_date is required");

        // margin：市场级数据集同样走 require_range 守卫
        let mut margin_req = make_req("margin");
        margin_req.start_date = Some("20240101".into());
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), margin_req)
            .await
            .expect_err("margin 缺 end_date 必须拒绝");
        assert_eq!(error, "end_date is required");
    }

    #[tokio::test]
    async fn execute_rejects_unknown_dataset_before_any_sync() {
        let state = std::sync::Arc::new(test_app_state().await);
        let req = make_req("zzz_unknown_dataset");
        let error = execute_sync_task(state.clone(), TEST_TASK_ID.to_string(), req)
            .await
            .expect_err("未知 dataset 必须拒绝");
        assert_eq!(error, "unsupported dataset: zzz_unknown_dataset");
    }
}
