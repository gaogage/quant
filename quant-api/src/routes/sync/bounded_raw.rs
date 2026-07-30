use serde::Deserialize;
use serde_json::json;
use sqlx::Row;
use std::{
    hash::Hasher,
    sync::Arc,
};

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

