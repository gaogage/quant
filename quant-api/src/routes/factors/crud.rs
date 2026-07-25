//! Factor CRUD routes — create/update/delete/list factors and definitions.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, NaiveDate};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use quant_factor::factors::price_volume::*;
use quant_factor::neutralize::NeutralizeConfig;
use quant_factor::*;

use crate::AppState;
use super::*;

#[derive(Debug, Deserialize)]
pub struct RegisterFactorDefinitionRequest {
    pub factor_code: String,
    pub version: String,
    pub name: String,
    pub category: String,
    pub frequency: Option<String>,
    pub dependencies: Option<serde_json::Value>,
    pub parameters: Option<serde_json::Value>,
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListFactorDefinitionsQuery {
    pub factor_code: Option<String>,
    pub version: Option<String>,
    pub category: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

// ─── Handlers ─────────────────────────────────────────────────────

pub async fn list_factors() -> impl IntoResponse {
    let factors = vec![
        FactorInfo {
            name: "mom_5d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 5}),
        },
        FactorInfo {
            name: "mom_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
        FactorInfo {
            name: "mom_60d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 60}),
        },
        FactorInfo {
            name: "vol_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20, "annualized": true}),
        },
        FactorInfo {
            name: "vol_60d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 60, "annualized": true}),
        },
        FactorInfo {
            name: "downvol_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20, "annualized": true}),
        },
        FactorInfo {
            name: "rev_5d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 5}),
        },
        FactorInfo {
            name: "rev_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
        FactorInfo {
            name: "turn_5d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 5}),
        },
        FactorInfo {
            name: "turn_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
        FactorInfo {
            name: "amihud_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20, "scale": 1_000_000_000.0}),
        },
        FactorInfo {
            name: "amt_intensity_20d".into(),
            category: "price_volume".into(),
            version: "1.0.0".into(),
            params: json!({"period": 20}),
        },
    ];

    Json(json!({"code": 0, "data": {"factors": factors}}))
}

pub async fn list_factor_definitions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListFactorDefinitionsQuery>,
) -> impl IntoResponse {
    let factor_code = normalize_filter(query.factor_code);
    let version = normalize_filter(query.version);
    let category = normalize_filter(query.category);
    let status = normalize_filter(query.status);
    let limit = normalize_limit(query.limit);

    let rows = sqlx::query_as::<_, FactorDefinitionRow>(
        "SELECT factor_id, factor_code, version, name, category, frequency,
           dependencies, parameters, status, created_at, updated_at
         FROM factor_definition
         WHERE ($1::text IS NULL OR factor_code = $1)
           AND ($2::text IS NULL OR version = $2)
           AND ($3::text IS NULL OR category = $3)
           AND ($4::text IS NULL OR status = $4)
         ORDER BY updated_at DESC, factor_code ASC, version ASC
         LIMIT $5",
    )
    .bind(factor_code.as_deref())
    .bind(version.as_deref())
    .bind(category.as_deref())
    .bind(status.as_deref())
    .bind(limit)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => Json(json!({
            "code": 0,
            "data": {
                "definitions": rows.into_iter().map(factor_definition_from_row).collect::<Vec<_>>()
            }
        })),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to list factor definitions: {}", error)}),
        ),
    }
}

pub async fn get_factor_definition(
    State(state): State<Arc<AppState>>,
    Path((factor_code, version)): Path<(String, String)>,
) -> impl IntoResponse {
    let row = sqlx::query_as::<_, FactorDefinitionRow>(
        "SELECT factor_id, factor_code, version, name, category, frequency,
           dependencies, parameters, status, created_at, updated_at
         FROM factor_definition
         WHERE factor_code = $1 AND version = $2",
    )
    .bind(&factor_code)
    .bind(&version)
    .fetch_optional(&state.db)
    .await;

    match row {
        Ok(Some(row)) => Json(json!({"code": 0, "data": factor_definition_from_row(row)})),
        Ok(None) => Json(
            json!({"code": 1, "message": format!("factor definition not found: {}@{}", factor_code, version)}),
        ),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to get factor definition: {}", error)}),
        ),
    }
}

pub async fn register_factor_definition(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterFactorDefinitionRequest>,
) -> impl IntoResponse {
    let input = match req.into_definition() {
        Ok(input) => input,
        Err(message) => return Json(json!({"code": 1, "message": message})),
    };

    match upsert_factor_definition_input(&state.db, &input).await {
        Ok(definition) => Json(json!({"code": 0, "data": definition})),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to upsert factor definition: {}", error)}),
        ),
    }
}

pub async fn compute_factor(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ComputeFactorRequest>,
) -> impl IntoResponse {
    // Parse factor parameters
    let (factor_name, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    // Determine symbols (as Vec<String> for load_bars)
    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let symbols = req.symbols.as_ref().unwrap_or(&default_symbols);

    // Determine date range
    let end_date = req.end_date.as_deref().unwrap_or("20250509");
    let start_date = req.start_date.as_deref().unwrap_or("20240101");

    // Load daily bars from database
    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 1).await {
        Ok(b) => b,
        Err(e) => {
            return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)}));
        }
    };

    let input = FactorInput {
        bars,
        trade_dates: vec![],
    };

    // Compute factor
    let mut output = match compute_price_volume_factor(factor_name, period, &input) {
        Some(output) => output,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    // Apply standardization if requested
    if let Some(ref std_method) = req.standardize {
        let method = match std_method.as_str() {
            "zscore" => StandardizeMethod::ZScore,
            "rank" => StandardizeMethod::Rank,
            s if s.starts_with("winsorized_") => {
                let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                StandardizeMethod::Winsorized(sigma)
            }
            _ => StandardizeMethod::ZScore,
        };
        output = standardize(&output, method);
    }

    // Convert to JSON-friendly format
    let values: Vec<serde_json::Value> = output
        .values
        .iter()
        .map(|fv| {
            json!({
                "symbol": fv.symbol,
                "date": fv.date.format("%Y%m%d").to_string(),
                "value": fv.value,
            })
        })
        .collect();

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": output.name,
            "metadata": output.metadata,
            "value_count": values.len(),
            "values": values,
        }
    }))
}

pub async fn evaluate_factor(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateFactorRequest>,
) -> impl IntoResponse {
    let (factor_name, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let symbols = req.symbols.as_ref().unwrap_or(&default_symbols);
    let end_date = req.end_date.as_deref().unwrap_or("20250509");
    let start_date = req.start_date.as_deref().unwrap_or("20240101");

    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 2).await {
        Ok(b) => b,
        Err(e) => {
            return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)}));
        }
    };

    let input = FactorInput {
        bars: bars.clone(),
        trade_dates: vec![],
    };

    let output = match compute_price_volume_factor(factor_name, period, &input) {
        Some(output) => output,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    // Build forward returns: 1-day forward return per (symbol, date)
    let mut forward_returns: HashMap<(String, chrono::NaiveDate), f64> = HashMap::new();
    for (sym, sym_bars) in &bars {
        for i in 0..sym_bars.len() - 1 {
            let close_t: f64 = sym_bars[i].close.try_into().unwrap_or(f64::NAN);
            let close_t1: f64 = sym_bars[i + 1].close.try_into().unwrap_or(f64::NAN);
            if close_t > 0.0 {
                forward_returns.insert(
                    (sym.clone(), sym_bars[i].trade_date),
                    (close_t1 - close_t) / close_t,
                );
            }
        }
    }

    let evaluation = evaluate(&output, &forward_returns, req.n_quantiles);

    Json(json!({
        "code": 0,
        "data": evaluation,
    }))
}

// ─── Persist computed factors ─────────────────────────────────────

#[derive(Debug, Deserialize)]

pub struct SyncFactorRequest {
    pub factor: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub standardize: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncFinancialFactorRequest {
    pub factor: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug)]
pub(crate) struct FinancialIndicatorRow {
    pub(crate) symbol: String,
    pub(crate) ann_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) value: Decimal,
}

impl FinancialIndicatorRow {
    pub(crate) fn to_factor_value(&self) -> FinancialFactorValue {
        let value: f64 = self.value.try_into().unwrap_or(f64::NAN);
        FinancialFactorValue {
            symbol: self.symbol.clone(),
            date: self.ann_date,
            available_at: self.ann_date,
            report_end_date: self.end_date,
            value,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FinancialFactorValue {
    pub(crate) symbol: String,
    pub(crate) date: NaiveDate,
    pub(crate) available_at: NaiveDate,
    pub(crate) report_end_date: NaiveDate,
    pub(crate) value: f64,
}

pub(crate) struct FinancialFactorSpec {
    pub(crate) code: &'static str,
    pub(crate) source_column: &'static str,
    pub(crate) name: &'static str,
    pub(crate) category: &'static str,
}


pub(crate) fn parse_financial_factor(name: &str) -> Option<FinancialFactorSpec> {
    match name {
        "roe" | "roe_ttm" => Some(FinancialFactorSpec {
            code: "fin_roe",
            source_column: "roe",
            name: "roe",
            category: "fundamental",
        }),
        "roa" | "roa_ttm" => Some(FinancialFactorSpec {
            code: "fin_roa",
            source_column: "roa",
            name: "roa",
            category: "fundamental",
        }),
        "eps" => Some(FinancialFactorSpec {
            code: "fin_eps",
            source_column: "eps",
            name: "eps",
            category: "fundamental",
        }),
        "gross_margin" => Some(FinancialFactorSpec {
            code: "fin_gross_margin",
            source_column: "gross_margin",
            name: "gross_margin",
            category: "fundamental",
        }),
        "netprofit_margin" => Some(FinancialFactorSpec {
            code: "fin_netprofit_margin",
            source_column: "netprofit_margin",
            name: "netprofit_margin",
            category: "fundamental",
        }),
        "debt_to_assets" => Some(FinancialFactorSpec {
            code: "fin_debt_to_assets",
            source_column: "debt_to_assets",
            name: "debt_to_assets",
            category: "fundamental",
        }),
        "current_ratio" => Some(FinancialFactorSpec {
            code: "fin_current_ratio",
            source_column: "current_ratio",
            name: "current_ratio",
            category: "fundamental",
        }),
        "quick_ratio" => Some(FinancialFactorSpec {
            code: "fin_quick_ratio",
            source_column: "quick_ratio",
            name: "quick_ratio",
            category: "fundamental",
        }),
        _ => None,
    }
}


pub async fn sync_factor_values(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncFactorRequest>,
) -> impl IntoResponse {
    let compute_req = ComputeFactorRequest {
        factor: req.factor.clone(),
        symbols: req.symbols,
        start_date: req.start_date,
        end_date: req.end_date,
        standardize: req.standardize,
    };

    // Reuse compute_factor logic by calling the inner function directly
    let (factor_name, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}))
        }
    };

    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let symbols = compute_req.symbols.as_ref().unwrap_or(&default_symbols);
    let end_date = compute_req.end_date.as_deref().unwrap_or("20250509");
    let start_date = compute_req.start_date.as_deref().unwrap_or("20240101");

    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 1).await {
        Ok(b) => b,
        Err(e) => {
            return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)}))
        }
    };

    let input = FactorInput {
        bars,
        trade_dates: vec![],
    };
    let mut output = match compute_price_volume_factor(factor_name, period, &input) {
        Some(output) => output,
        None => {
            return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)}));
        }
    };

    let std_method = if let Some(ref m) = compute_req.standardize {
        match m.as_str() {
            "zscore" => Some("zscore".to_string()),
            "rank" => Some("rank".to_string()),
            s if s.starts_with("winsorized_") => Some(s.to_string()),
            _ => None,
        }
    } else {
        None
    };

    if let Some(ref m) = std_method {
        let method = match m.as_str() {
            "zscore" => StandardizeMethod::ZScore,
            "rank" => StandardizeMethod::Rank,
            s if s.starts_with("winsorized_") => {
                let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                StandardizeMethod::Winsorized(sigma)
            }
            _ => StandardizeMethod::ZScore,
        };
        output = standardize(&output, method);
    }

    if let Err(error) = upsert_factor_definition(&state.db, &output, &req.version).await {
        return Json(
            json!({"code": 1, "message": format!("Failed to upsert factor definition: {}", error)}),
        );
    }

    // Persist to DB
    let mut inserted = 0u64;
    for fv in &output.values {
        let available_at = fv.available_at.unwrap_or(fv.date);
        let result = sqlx::query(
            "INSERT INTO factor_value
               (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
               raw_value = EXCLUDED.raw_value,
               normalized_value = EXCLUDED.normalized_value,
               available_at = EXCLUDED.available_at,
               created_at = NOW()"
        )
        .bind(&output.name)
        .bind(&req.version)
        .bind(&fv.symbol)
        .bind(fv.date)
        .bind(fv.value)
        .bind(if std_method.is_some() { Some(fv.value) } else { None::<f64> })
        .bind(available_at)
        .execute(&state.db)
        .await;

        match result {
            Ok(_) => inserted += 1,
            Err(e) => tracing::warn!(
                "Failed to insert factor value for {} {}: {}",
                fv.symbol,
                fv.date,
                e
            ),
        }
    }

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": output.name,
            "version": req.version,
            "total_values": output.values.len(),
            "inserted": inserted,
            "standardized": std_method.is_some(),
        }
    }))
}


pub async fn sync_financial_factor_values(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncFinancialFactorRequest>,
) -> impl IntoResponse {
    let spec = match parse_financial_factor(&req.factor) {
        Some(spec) => spec,
        None => {
            return Json(
                json!({"code": 1, "message": format!("Unknown financial factor: {}", req.factor)}),
            )
        }
    };

    let start_d = req.start_date.as_deref().unwrap_or("20100101");
    let end_d = req.end_date.as_deref().unwrap_or("20261231");
    let symbols = req.symbols.unwrap_or_default();
    let symbol_filter = if symbols.is_empty() {
        None
    } else {
        Some(symbols)
    };

    let rows = match sqlx::query_as::<_, (String, NaiveDate, NaiveDate, Decimal)>(&format!(
        "SELECT ts_code, ann_date, end_date, {column}
             FROM market_financial_indicator
             WHERE {column} IS NOT NULL
               AND ann_date >= $1::date
               AND ann_date <= $2::date
               AND ($3::text[] IS NULL OR ts_code = ANY($3))
             ORDER BY ann_date, ts_code",
        column = spec.source_column
    ))
    .bind(start_d)
    .bind(end_d)
    .bind(symbol_filter.as_deref())
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to load financial factor source: {}", error)}),
            );
        }
    };

    let factor_values = rows
        .into_iter()
        .map(
            |(symbol, ann_date, end_date, value)| FinancialIndicatorRow {
                symbol,
                ann_date,
                end_date,
                value,
            },
        )
        .map(|row| row.to_factor_value())
        .filter(|fv| fv.value.is_finite())
        .collect::<Vec<_>>();

    let definition = FactorDefinitionInput {
        factor_code: spec.code.to_string(),
        version: req.version.clone(),
        name: spec.name.to_string(),
        category: spec.category.to_string(),
        frequency: "quarterly_report".to_string(),
        dependencies: json!(["market_financial_indicator"]),
        parameters: json!({
            "source_column": spec.source_column,
            "pit_date": "ann_date",
            "report_period_column": "end_date",
        }),
        status: "active".to_string(),
    };

    if let Err(error) = upsert_factor_definition_input(&state.db, &definition).await {
        return Json(
            json!({"code": 1, "message": format!("Failed to upsert factor definition: {}", error)}),
        );
    }

    let mut inserted = 0u64;
    let mut min_trade_date: Option<NaiveDate> = None;
    let mut max_trade_date: Option<NaiveDate> = None;
    let mut max_report_end_date: Option<NaiveDate> = None;

    for fv in &factor_values {
        let result = sqlx::query(
            "INSERT INTO factor_value
               (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
             VALUES ($1, $2, $3, $4, $5, NULL, $6)
             ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
               raw_value = EXCLUDED.raw_value,
               normalized_value = EXCLUDED.normalized_value,
               available_at = EXCLUDED.available_at,
               created_at = NOW()",
        )
        .bind(spec.code)
        .bind(&req.version)
        .bind(&fv.symbol)
        .bind(fv.date)
        .bind(fv.value)
        .bind(fv.available_at)
        .execute(&state.db)
        .await;

        match result {
            Ok(_) => {
                inserted += 1;
                min_trade_date = Some(min_trade_date.map_or(fv.date, |date| date.min(fv.date)));
                max_trade_date = Some(max_trade_date.map_or(fv.date, |date| date.max(fv.date)));
                max_report_end_date = Some(
                    max_report_end_date
                        .map_or(fv.report_end_date, |date| date.max(fv.report_end_date)),
                );
            }
            Err(error) => tracing::warn!(
                factor = spec.code,
                symbol = %fv.symbol,
                date = %fv.date,
                "Failed to insert financial factor value: {}",
                error
            ),
        }
    }

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": spec.code,
            "version": req.version,
            "source_column": spec.source_column,
            "total_values": factor_values.len(),
            "inserted": inserted,
            "pit_date": "ann_date",
            "report_period_column": "end_date",
            "min_trade_date": min_trade_date.map(|date| date.to_string()),
            "max_trade_date": max_trade_date.map(|date| date.to_string()),
            "max_report_end_date": max_report_end_date.map(|date| date.to_string()),
        }
    }))
}

// ─── Batch sync all symbols (inline, no callback overhead) ───────


pub async fn batch_sync_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchSyncRequest>,
) -> impl IntoResponse {
    let start_d = req.start_date.as_deref().unwrap_or("20240101");
    let end_d = req.end_date.as_deref().unwrap_or("20250509");
    let is_std = req.standardize.is_some();
    let chunk_size = req.chunk_size;

    // Load symbol list
    let all_syms: Vec<String> = match sqlx::query_scalar(
        "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => return Json(json!({"code":1,"message":format!("{}",e)})),
    };

    let (ftype, period) = match parse_factor(&req.factor) {
        Some(f) => f,
        None => return Json(json!({"code":1,"message":format!("unknown factor: {}", req.factor)})),
    };
    let mut total_vals = 0usize;
    let mut inserted = 0usize;
    let mut errors: Vec<String> = Vec::new();

    for chunk in all_syms.chunks(chunk_size) {
        let syms: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();

        // Load bars for this chunk
        let mut bars_map: HashMap<String, Vec<DailyBar>> = HashMap::new();
        for &sym in &syms {
            let rows = sqlx::query_as::<_, (String, NaiveDate, Decimal, Decimal, Decimal, Decimal, Option<Decimal>, Option<Decimal>, Decimal, Decimal)>(
                "SELECT symbol, trade_date, open, high, low, close, pre_close, pct_change, volume, amount
                 FROM market_stock_daily_bar_adj
                 WHERE symbol = $1 AND trade_date >= $2::date AND trade_date <= $3::date
                 ORDER BY trade_date ASC"
            ).bind(sym).bind(start_d).bind(end_d).fetch_all(&state.db).await;

            match rows {
                Ok(r) if r.len() > period + 1 => {
                    let bars: Vec<DailyBar> = r
                        .into_iter()
                        .map(|(s, d, o, h, l, c, pc, cp, v, a)| DailyBar {
                            symbol: s,
                            trade_date: d,
                            open: o,
                            high: h,
                            low: l,
                            close: c,
                            pre_close: pc,
                            change_pct: cp,
                            volume: v,
                            amount: a,
                        })
                        .collect();
                    bars_map.insert(sym.to_string(), bars);
                }
                Ok(_) => {}
                Err(e) => {
                    errors.push(format!("{}/{}: {}", sym, "load", e));
                }
            }
        }

        if bars_map.is_empty() {
            continue;
        }

        let input = FactorInput {
            bars: bars_map,
            trade_dates: vec![],
        };

        // Compute
        let mut output = match compute_price_volume_factor(ftype, period, &input) {
            Some(output) => output,
            None => break,
        };

        // Standardize
        if let Some(ref m) = req.standardize {
            let method = match m.as_str() {
                "zscore" => StandardizeMethod::ZScore,
                "rank" => StandardizeMethod::Rank,
                s if s.starts_with("winsorized_") => {
                    let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                    StandardizeMethod::Winsorized(sigma)
                }
                _ => StandardizeMethod::ZScore,
            };
            tracing::info!(factor=%req.factor, method=%m, sigma=?method, "foreground standardize");
            output = standardize(&output, method);
        }

        if let Err(e) = upsert_factor_definition(&state.db, &output, &req.version).await {
            errors.push(format!("factor_definition/{}: {}", output.name, e));
            continue;
        }

        total_vals += output.values.len();

        match upsert_factor_values(&state.db, &output, &req.version, is_std).await {
            Ok(saved) => inserted += saved,
            Err(error) => errors.push(error),
        }
    }

    let factor_output_name = format!("{}_{}d{}", ftype, period, if is_std { "_std" } else { "" });

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": factor_output_name,
            "version": req.version,
            "symbols_total": all_syms.len(),
            "total_values": total_vals,
            "inserted": inserted,
            "errors": errors.iter().take(5).collect::<Vec<_>>(),
            "error_count": errors.len(),
            "standardized": is_std,
        }
    }))
}

/// POST /api/v1/quant/factors/batch-sync/background
///
/// 后台批量计算单个因子，立即返回 task_id。
/// 通过 GET /api/v1/quant/data/sync/tasks/:task_id 查询进度。

pub async fn batch_sync_factors_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchSyncRequest>,
) -> impl IntoResponse {
    let task_id = background_factor_task_id();
    let factor_name = req.factor.clone();
    let version = req.version.clone();
    let start = req.start_date.clone();
    let end = req.end_date.clone();
    let std_method = req.standardize.clone();
    let chunk_size = req.chunk_size;

    info!(task_id = %task_id, factor = %factor_name, "后台计算因子");

    // Create sync task record for status tracking
    let _ = sqlx::query(
        "INSERT INTO data_sync_task (task_id, task_type, source, status) VALUES ($1, $2, 'factor', 'running')"
    )
    .bind(&task_id)
    .bind(&format!("factor:{}", factor_name))
    .execute(&state.db)
    .await;

    let state = state.clone();
    let tid = task_id.clone();
    let fname = factor_name.clone();

    tokio::spawn(async move {
        let result: Result<serde_json::Value, String> = async {
            let all_syms: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol"
            ).fetch_all(&state.db).await.map_err(|e| e.to_string())?;

            let (ftype, period) = parse_factor(&fname)
                .ok_or_else(|| format!("unknown factor: {}", fname))?;

            let start_d = start.as_deref().unwrap_or("20160101");
            let end_d = end.as_deref().unwrap_or("20260511");
            let is_std = std_method.is_some();
            let mut total_vals = 0usize;
            let mut inserted = 0usize;
            let mut errors: Vec<String> = Vec::new();

            for chunk in all_syms.chunks(chunk_size) {
                let syms: Vec<String> = chunk.iter().map(|s| s.clone()).collect();
                let sym_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();

                // Batch-load all bars for this chunk in ONE query
                let all_rows: Vec<(String, NaiveDate, Decimal, Decimal, Decimal, Decimal, Option<Decimal>, Option<Decimal>, Decimal, Decimal)> =
                    sqlx::query_as("SELECT symbol, trade_date, open, high, low, close, pre_close, pct_change, volume, amount
                        FROM market_stock_daily_bar_adj WHERE symbol = ANY($1) AND trade_date >= $2::date AND trade_date <= $3::date ORDER BY symbol, trade_date ASC")
                    .bind(&sym_refs).bind(start_d).bind(end_d)
                    .fetch_all(&state.db).await
                    .map_err(|e| format!("batch query failed ({} symbols, {} - {}): {}", sym_refs.len(), start_d, end_d, e))?;

                // Group by symbol
                let mut bars_map: HashMap<String, Vec<DailyBar>> = HashMap::new();
                for (sym, d, o, h, l, c, pc, cp, v, a) in all_rows {
                    if let (Some(pc_val), Some(cp_val)) = (pc, cp) {
                        bars_map.entry(sym.clone()).or_default().push(DailyBar {
                            symbol: sym, trade_date: d, open: o, high: h, low: l,
                            close: c, pre_close: Some(pc_val), change_pct: Some(cp_val),
                            volume: v, amount: a,
                        });
                    } else {
                        bars_map.entry(sym.clone()).or_default().push(DailyBar {
                            symbol: sym, trade_date: d, open: o, high: h, low: l,
                            close: c, pre_close: pc, change_pct: cp,
                            volume: v, amount: a,
                        });
                    }
                }
                // Filter symbols with insufficient data
                bars_map.retain(|_, bars| bars.len() > period + 1);

                if bars_map.is_empty() { continue; }

                let input = FactorInput { bars: bars_map, trade_dates: vec![] };
                let mut output = match compute_price_volume_factor(ftype, period, &input) {
                    Some(output) => output,
                    None => { errors.push(format!("Unknown factor: {}", ftype)); break; }
                };

                if let Some(ref m) = std_method {
                    let method = match m.as_str() {
                        "zscore" => StandardizeMethod::ZScore,
                        "rank" => StandardizeMethod::Rank,
                        s if s.starts_with("winsorized_") => {
                            let sigma: f64 = s.trim_start_matches("winsorized_").parse().unwrap_or(3.0);
                            StandardizeMethod::Winsorized(sigma)
                        }
                        _ => StandardizeMethod::ZScore,
                    };
                    output = standardize(&output, method);
                }

                if let Err(e) = upsert_factor_definition(&state.db, &output, &version).await {
                    errors.push(format!("factor_definition/{}: {}", output.name, e));
                    continue;
                }

                total_vals += output.values.len();

                match upsert_factor_values(&state.db, &output, &version, is_std).await {
                    Ok(saved) => inserted += saved,
                    Err(error) => errors.push(error),
                }
            }

            let factor_output_name = format!("{}_{}d{}", ftype, period, if is_std { "_std" } else { "" });
            Ok(serde_json::json!({
                "factor_name": factor_output_name, "version": version,
                "total_values": total_vals, "inserted": inserted,
                "error_count": errors.len()
            }))
        }.await;

        match result {
            Ok(data) => {
                info!(task_id = %tid, "后台计算因子完成");
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='completed', total_count=$1, success_count=$2, failed_count=$3, progress=100, completed_at=now() WHERE task_id=$4"
                )
                .bind(data["total_values"].as_i64().unwrap_or(0) as i32)
                .bind(data["inserted"].as_i64().unwrap_or(0) as i32)
                .bind(data["error_count"].as_i64().unwrap_or(0) as i32)
                .bind(&tid)
                .execute(&state.db).await;
            }
            Err(e) => {
                tracing::error!(task_id = %tid, error = %e, "后台计算因子失败");
                let _ = sqlx::query(
                    "UPDATE data_sync_task SET status='failed', error_message=$2, progress=0, completed_at=now() WHERE task_id=$1"
                )
                .bind(&tid)
                .bind(&e)
                .execute(&state.db).await;
            }
        }
    });

    Json(
        json!({"code": 0, "data": {"task_id": task_id, "status": "running", "factor": factor_name}}),
    )
}

/// POST /api/v1/quant/factors/phase7-price-volume-backfill/background
///
/// Set-based backfill for the Phase 7 price-volume alpha bundle and its
/// equal-weight combo score. This is the Rust API path for full historical

async fn upsert_factor_values(
    db: &sqlx::PgPool,
    output: &FactorOutput,
    version: &str,
    store_normalized: bool,
) -> Result<usize, String> {
    let mut saved = 0usize;

    for chunk in output.values.chunks(2_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO factor_value \
             (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at) ",
        );

        builder.push_values(chunk, |mut row, fv| {
            let normalized_value = if store_normalized {
                Some(fv.value)
            } else {
                None::<f64>
            };
            row.push_bind(&output.name)
                .push_bind(version)
                .push_bind(&fv.symbol)
                .push_bind(fv.date)
                .push_bind(fv.value)
                .push_bind(normalized_value)
                .push_bind(fv.available_at.unwrap_or(fv.date));
        });

        builder.push(
            " ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET \
              raw_value = EXCLUDED.raw_value, \
              normalized_value = EXCLUDED.normalized_value, \
              available_at = EXCLUDED.available_at, \
              created_at = NOW()",
        );

        let result = builder.build().execute(db).await.map_err(|error| {
            format!(
                "Failed to upsert factor values for {}: {}",
                output.name, error
            )
        })?;
        saved += result.rows_affected() as usize;
    }

    Ok(saved)
}

// ─── Evaluate all factors (from DB) ────────────────────────

#[derive(Debug, Deserialize)]

pub struct EvaluateAllRequest {
    #[serde(default = "default_horizon")]
    #[allow(dead_code)]
    pub horizon: i16,
    #[serde(default)]
    #[allow(dead_code)]
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    /// Enable industry/size neutralization before evaluation
    #[serde(default)]
    pub neutralize: Option<bool>,
    #[serde(default)]
    pub neutralize_industry: Option<bool>,
    #[serde(default)]
    pub neutralize_size: Option<bool>,
}

fn default_horizon() -> i16 {
    1
}


pub async fn evaluate_all_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateAllRequest>,
) -> impl IntoResponse {
    let end_d = req.end_date.as_deref().unwrap_or("20250509");
    let start_d = req.start_date.as_deref().unwrap_or("20230101");

    // Optional neutralization config
    let do_neutralize = req.neutralize.unwrap_or(false);
    let do_ind = req.neutralize_industry.unwrap_or(true);
    let do_sz = req.neutralize_size.unwrap_or(false);

    // Pre-load industry map + size proxy (once, reused for all factors)
    let neut_config: Option<NeutralizeConfig> = if do_neutralize {
        let ind_rows = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'",
        )
        .fetch_all(&state.db)
        .await
        .ok()
        .unwrap_or_default();
        let industries: HashMap<String, String> = ind_rows
            .into_iter()
            .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
            .collect();

        let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
        if do_sz {
            let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                "SELECT symbol, trade_date, amount FROM market_stock_daily_bar_adj
                 WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0
                 ORDER BY symbol, trade_date",
            )
            .bind(start_d)
            .bind(end_d)
            .fetch_all(&state.db)
            .await;
            if let Ok(rows) = amt_rows {
                for (sym, date, amt) in rows {
                    if let Some(a) = amt {
                        let a_val: f64 = a.try_into().unwrap_or(0.0);
                        if a_val > 0.0 {
                            size_proxy.entry(sym).or_default().push((date, a_val));
                        }
                    }
                }
            }
        }
        Some(NeutralizeConfig {
            industries,
            size_proxy,
        })
    } else {
        None
    };

    // Get all factor versions
    let all_factors: Vec<(String, String)> = match sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT factor_code, factor_version FROM factor_value ORDER BY factor_code",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => return Json(json!({"code":1,"message":format!("{}",e)})),
    };

    let mut results = Vec::new();

    for (code, ver) in &all_factors {
        // Load factor values
        let fv_rows =
            sqlx::query_as::<_, (String, chrono::NaiveDate, Option<rust_decimal::Decimal>)>(
                "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
             FROM factor_value
             WHERE factor_code=$1 AND factor_version=$2
               AND trade_date>=$3::date AND trade_date<=$4::date
             ORDER BY trade_date, symbol",
            )
            .bind(code)
            .bind(ver)
            .bind(start_d)
            .bind(end_d)
            .fetch_all(&state.db)
            .await;

        let fv_rows = match fv_rows {
            Ok(r) => r,
            Err(e) => {
                results.push(json!({"factor":code,"version":ver,"error":e.to_string()}));
                continue;
            }
        };

        // Build forward returns
        let symbols: Vec<String> = fv_rows
            .iter()
            .map(|(s, _, _)| s.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        let bars = match load_bars(&state.db, &symbols, start_d, end_d, 2).await {
            Ok(b) => b,
            Err(e) => {
                results.push(json!({"factor":code,"version":ver,"error":e.to_string()}));
                continue;
            }
        };

        let mut forward_returns: HashMap<(String, chrono::NaiveDate), f64> = HashMap::new();
        for (sym, sym_bars) in &bars {
            for i in 0..sym_bars.len() - 1 {
                let c: f64 = sym_bars[i].close.try_into().unwrap_or(f64::NAN);
                let c1: f64 = sym_bars[i + 1].close.try_into().unwrap_or(f64::NAN);
                if c > 0.0 {
                    forward_returns.insert((sym.clone(), sym_bars[i].trade_date), (c1 - c) / c);
                }
            }
        }

        // Create FactorOutput
        let values: Vec<FactorValue> = fv_rows
            .iter()
            .filter_map(|(sym, date, val)| {
                val.and_then(|v| {
                    use rust_decimal::prelude::ToPrimitive;
                    v.to_f64().map(|fv| FactorValue {
                        symbol: sym.clone(),
                        date: *date,
                        value: fv,
                        available_at: None,
                    })
                })
            })
            .collect();

        if values.len() < 100 {
            continue;
        }

        let output = FactorOutput {
            name: code.clone(),
            values,
            metadata: FactorMetadata {
                factor_name: code.clone(),
                category: FactorCategory::PriceVolume,
                version: ver.clone(),
                params: serde_json::json!({}),
                computed_at: chrono::Utc::now(),
                symbol_count: 0,
                date_count: 0,
                coverage_ratio: 0.0,
                mean: f64::NAN,
                std: f64::NAN,
                min: f64::NAN,
                max: f64::NAN,
            },
        };

        // Optional: neutralize before evaluation
        let output = if let Some(ref cfg) = neut_config {
            use quant_factor::neutralize::neutralize;
            let (neut, _res) = neutralize(&output, cfg, do_ind, do_sz);
            neut
        } else {
            output
        };

        let evaluation = evaluate(&output, &forward_returns, 5);
        results.push(
            serde_json::to_value(&evaluation).unwrap_or(json!({"error":"serialization failed"})),
        );
    }

    Json(json!({"code":0,"data":{"evaluations":results,"count":results.len()}}))
}

/// POST /api/v1/quant/factors/evaluate-all/background
///
/// 后台评估所有因子 IC/ICIR，立即返回 task_id。

pub async fn evaluate_all_factors_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvaluateAllRequest>,
) -> impl IntoResponse {
    let task_id = chrono::Utc::now().format("ev-%Y%m%d-%H%M%S%3f").to_string();
    let start_d = req.start_date.clone();
    let end_d = req.end_date.clone();
    let do_neutralize = req.neutralize.unwrap_or(false);
    let do_ind = req.neutralize_industry.unwrap_or(true);
    let do_sz = req.neutralize_size.unwrap_or(false);
    let horizon = req.horizon as usize;

    info!(task_id = %task_id, horizon = horizon, "后台评估所有因子");

    let _ = sqlx::query(
        "INSERT INTO data_sync_task (task_id, task_type, source, status) VALUES ($1, 'evaluate_all', 'factor', 'running')"
    ).bind(&task_id).execute(&state.db).await;

    let state = state.clone();
    let tid = task_id.clone();

    tokio::spawn(async move {
        let result: Result<usize, String> = async {
            let start = start_d.as_deref().unwrap_or("20160101");
            let end = end_d.as_deref().unwrap_or("20260511");
            let do_neut = do_neutralize;
            let di = do_ind;
            let ds = do_sz;

            let neut_config: Option<NeutralizeConfig> = if do_neut {
                let ind_rows = sqlx::query_as::<_, (String, Option<String>)>(
                    "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'"
                ).fetch_all(&state.db).await.ok().unwrap_or_default();
                let industries: HashMap<String, String> = ind_rows.into_iter()
                    .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
                    .collect();
                let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
                if ds {
                    let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                        "SELECT symbol, trade_date, amount FROM market_stock_daily_bar_adj
                         WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0 ORDER BY symbol, trade_date"
                    ).bind(start).bind(end).fetch_all(&state.db).await;
                    if let Ok(rows) = amt_rows {
                        for (sym, date, amt) in rows {
                            if let Some(a) = amt {
                                let a_val: f64 = a.try_into().unwrap_or(0.0);
                                if a_val > 0.0 { size_proxy.entry(sym).or_default().push((date, a_val)); }
                            }
                        }
                    }
                }
                Some(NeutralizeConfig { industries, size_proxy })
            } else { None };

            let all_factors: Vec<(String, String)> = sqlx::query_as::<_, (String, String)>(
                "SELECT DISTINCT factor_code, factor_version FROM factor_value ORDER BY factor_code"
            ).fetch_all(&state.db).await.map_err(|e| e.to_string())?;

            // Load forward returns ONCE for all factors — support multi-horizon
            // Load close prices grouped by symbol, compute N-day forward return
            let fwd_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                "SELECT symbol, trade_date, close FROM market_stock_daily_bar_adj
                 WHERE trade_date >= $1::date AND trade_date <= $2::date
                   AND close > 0
                 ORDER BY symbol, trade_date"
            ).bind(start).bind(end).fetch_all(&state.db).await
              .map_err(|e| format!("Failed to load forward returns: {}", e))?;

            info!(fwd_rows = fwd_rows.len(), horizon = horizon, "Loaded close prices");

            // Group close prices by symbol: symbol -> [(date, close)]
            let mut close_by_sym: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
            for (sym, date, close) in &fwd_rows {
                if let Some(c) = close {
                    let cf: f64 = (*c).try_into().unwrap_or(0.0);
                    if cf > 0.0 {
                        close_by_sym.entry(sym.clone()).or_default().push((*date, cf));
                    }
                }
            }

            // Build N-day forward returns: date T -> (close_T+N - close_T) / close_T
            let mut fwd_map: HashMap<NaiveDate, HashMap<String, f64>> = HashMap::new();
            for (sym, prices) in &close_by_sym {
                for i in 0..prices.len().saturating_sub(horizon) {
                    let (date, close_t) = prices[i];
                    let (_date_n, close_n) = prices[i + horizon];
                    let ret = (close_n - close_t) / close_t;
                    fwd_map.entry(date).or_default().insert(sym.clone(), ret);
                }
            }
            info!(fwd_dates = fwd_map.len(), "Forward return map built");

            let mut count = 0usize;
            for (code, ver) in &all_factors {
                info!(factor = %code, version = %ver, "Evaluating factor");

                let fv_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                    "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
                     FROM factor_value WHERE factor_code=$1 AND factor_version=$2
                     AND trade_date >= $3::date AND trade_date <= $4::date ORDER BY symbol, trade_date"
                ).bind(code).bind(ver).bind(start).bind(end)
                  .fetch_all(&state.db).await.unwrap_or_default();

                info!(factor = %code, fv_rows = fv_rows.len(), "Factor values loaded");

                if fv_rows.len() < 100 { continue; }

                let mut values_map: HashMap<NaiveDate, Vec<(String, f64)>> = HashMap::new();
                for (sym, date, val) in &fv_rows {
                    if let Some(v) = val {
                        let vf: f64 = (*v).try_into().unwrap_or(0.0);
                        values_map.entry(*date).or_default().push((sym.clone(), vf));
                    }
                }

                let mut output = FactorOutput {
                    name: code.clone(),
                    values: vec![],
                    metadata: FactorMetadata {
                        factor_name: code.clone(),
                        category: FactorCategory::PriceVolume,
                        version: ver.clone(),
                        params: json!({}),
                        computed_at: chrono::Utc::now(),
                        symbol_count: 0,
                        date_count: 0,
                        coverage_ratio: 0.0,
                        mean: 0.0, std: 0.0, min: 0.0, max: 0.0,
                    },
                };
                let mut forward_returns: HashMap<(String, NaiveDate), f64> = HashMap::new();
                let dates: Vec<NaiveDate> = {
                    let mut ds: Vec<NaiveDate> = values_map.keys().copied().collect();
                    ds.sort(); ds
                };
                for &date in &dates {
                    if let (Some(vals), Some(fwds)) = (values_map.get(&date), fwd_map.get(&date)) {
                        for (sym, val) in vals {
                            let fv = FactorValue { symbol: sym.clone(), date, value: *val, available_at: None };
                            output.values.push(fv);
                            if let Some(fwd) = fwds.get(sym) {
                                forward_returns.insert((sym.clone(), date), *fwd);
                            }
                        }
                    }
                }

                if let Some(ref cfg) = neut_config {
                    use quant_factor::neutralize::neutralize;
                    let (neut, _) = neutralize(&output, cfg, di, ds);
                    output = neut;
                }

                let evaluation = evaluate(&output, &forward_returns, 5);
                let ic_json = serde_json::to_value(&evaluation.ic_series).unwrap_or(json!([]));
                let rank_ic_json =
                    serde_json::to_value(&evaluation.rank_ic_series).unwrap_or(json!([]));
                let qr_json = serde_json::to_value(&evaluation.quantile_returns).unwrap_or(json!([]));
                let insert_result = sqlx::query(
                    "INSERT INTO factor_evaluation (factor_code, factor_version, horizon, start_date, end_date,
                     mean_ic, ic_ir, mean_rank_ic, rank_ic_ir, ic_series, rank_ic_series,
                     quantile_spread, quantile_returns, period_count, symbol_count, total_pairs)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,0,0)
                     ON CONFLICT (factor_code, factor_version, horizon, start_date, end_date) DO UPDATE SET
                     mean_ic=EXCLUDED.mean_ic, ic_ir=EXCLUDED.ic_ir,
                     mean_rank_ic=EXCLUDED.mean_rank_ic, rank_ic_ir=EXCLUDED.rank_ic_ir,
                     ic_series=EXCLUDED.ic_series, rank_ic_series=EXCLUDED.rank_ic_series,
                     quantile_spread=EXCLUDED.quantile_spread, quantile_returns=EXCLUDED.quantile_returns,
                     period_count=EXCLUDED.period_count"
                )
                .bind(code).bind(ver)
                .bind(horizon as i32)
                .bind(evaluation.date_range.0).bind(evaluation.date_range.1)
                .bind(evaluation.mean_ic).bind(evaluation.ic_ir)
                .bind(evaluation.mean_rank_ic).bind(evaluation.rank_ic_ir)
                .bind(&ic_json).bind(&rank_ic_json)
                .bind(evaluation.quantile_spread).bind(&qr_json)
                .bind(evaluation.period_count as i32)
                .execute(&state.db).await;
                if let Err(ref e) = insert_result {
                    tracing::error!(factor = %code, error = %e, "Failed to insert evaluation");
                }
                count += 1;
            }
            Ok(count)
        }.await;

        match result {
            Ok(count) => {
                info!(task_id = %tid, count = count, "后台评估完成");
                let _ = sqlx::query("UPDATE data_sync_task SET status='completed', total_count=$1, progress=100, completed_at=now() WHERE task_id=$2")
                    .bind(count as i32).bind(&tid).execute(&state.db).await;
            }
            Err(e) => {
                tracing::error!(task_id = %tid, error = %e, "后台评估失败");
                let _ = sqlx::query("UPDATE data_sync_task SET status='failed', completed_at=now() WHERE task_id=$1")
                    .bind(&tid).execute(&state.db).await;
            }
        }
    });

    Json(json!({"code": 0, "data": {"task_id": task_id, "status": "running"}}))
}

// ─── Combine factors ────────────────────────────────────────

#[derive(Debug, Deserialize)]

pub struct CombineFactorsRequest {
    pub combo_name: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub factors: Vec<FactorRef>,
    #[serde(default = "default_combine_method")]
    pub method: String,
    #[serde(default = "default_horizon")]
    pub horizon: i16,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FactorRef {
    pub factor_code: String,
    #[serde(default = "default_version")]
    pub factor_version: String,
}

fn default_combine_method() -> String {
    "icir_weighted".to_string()
}

pub async fn combine_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CombineFactorsRequest>,
) -> impl IntoResponse {
    let factors: Vec<(String, String)> = req
        .factors
        .iter()
        .map(|f| (f.factor_code.clone(), f.factor_version.clone()))
        .collect();

    let method = match req.method.as_str() {
        "equal_weight" => CombineMethod::EqualWeight,
        _ => CombineMethod::IcirWeighted,
    };

    let weights = match compute_weights(&state.db, &factors, method, req.horizon).await {
        Ok(w) => w,
        Err(e) => return Json(json!({"code":1,"message":e})),
    };

    let sd = req
        .start_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());
    let ed = req
        .end_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());

    let inserted =
        match combine_and_persist(&state.db, &req.combo_name, &req.version, &weights, sd, ed).await
        {
            Ok(n) => n,
            Err(e) => return Json(json!({"code":1,"message":e})),
        };

    let weights_json: Vec<serde_json::Value> = weights
        .iter()
        .map(|w| {
            json!({
                "factor_code": w.factor_code,
                "factor_version": w.factor_version,
                "weight": w.weight,
            })
        })
        .collect();

    Json(
        json!({"code":0,"data":{"combo_name":req.combo_name,"version":req.version,"weights":weights_json,"inserted":inserted}}),
    )
}

// ─── Helpers ──────────────────────────────────────────────────────

/// Parse factor string like "mom_20d" → ("momentum", 20) or "turn_5d" → ("turnover", 5)

pub(crate) fn parse_factor(name: &str) -> Option<(&'static str, usize)> {
    let name = name.strip_suffix("_std").unwrap_or(name);
    if let Some(rest) = name.strip_prefix("mom_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("momentum", period))
    } else if let Some(rest) = name.strip_prefix("vol_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("volatility", period))
    } else if let Some(rest) = name.strip_prefix("downvol_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("downside_volatility", period))
    } else if let Some(rest) = name.strip_prefix("rev_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("reversal", period))
    } else if let Some(rest) = name.strip_prefix("turn_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("turnover", period))
    } else if let Some(rest) = name.strip_prefix("amihud_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("amihud_illiquidity", period))
    } else if let Some(rest) = name.strip_prefix("amt_intensity_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("amount_intensity", period))
    } else if let Some(rest) = name.strip_prefix("rsi_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("rsi", period))
    } else if let Some(rest) = name.strip_prefix("bb_pos_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("bb_position", period))
    } else if let Some(rest) = name.strip_prefix("atr_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("atr", period))
    } else if let Some(rest) = name.strip_prefix("amp_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("amplitude", period))
    } else if let Some(rest) = name.strip_prefix("vp_corr_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("vol_price_corr", period))
    } else if let Some(rest) = name.strip_prefix("skew_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("skewness", period))
    } else if let Some(rest) = name.strip_prefix("maxdd_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("max_drawdown", period))
    } else {
        None
    }
}

/// Load daily bars from PostgreSQL

async fn load_bars(
    pool: &sqlx::PgPool,
    symbols: &[String],
    start_date: &str,
    end_date: &str,
    min_records: usize,
) -> Result<HashMap<String, Vec<DailyBar>>, Box<dyn std::error::Error>> {
    let mut result: HashMap<String, Vec<DailyBar>> = HashMap::new();

    for sym in symbols {
        let rows = sqlx::query_as::<_, (String, chrono::NaiveDate, rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, rust_decimal::Decimal, rust_decimal::Decimal)>(
            "SELECT symbol, trade_date, open, high, low, close, pre_close, pct_change, volume, amount
             FROM market_stock_daily_bar_adj
             WHERE symbol = $1 AND trade_date >= $2::date AND trade_date <= $3::date
             ORDER BY trade_date ASC"
        )
        .bind(sym)
        .bind(start_date)
        .bind(end_date)
        .fetch_all(pool)
        .await?;

        if rows.len() < min_records {
            tracing::warn!(
                "{} only has {} records (need {})",
                sym,
                rows.len(),
                min_records
            );
        }

        let bars: Vec<DailyBar> = rows
            .into_iter()
            .map(|(s, d, o, h, l, c, pc, cp, v, a)| DailyBar {
                symbol: s,
                trade_date: d,
                open: o,
                high: h,
                low: l,
                close: c,
                pre_close: pc,
                change_pct: cp,
                volume: v,
                amount: a,
            })
            .collect();

        result.insert(sym.to_string(), bars);
    }

    Ok(result)
}

// ─── Factor Neutralization ─────────────────────────────────────────

#[derive(Debug, Deserialize)]

pub struct NeutralizeRequest {
    pub factor: String,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub do_industry: bool,
    #[serde(default)]
    pub do_size: bool,
}


pub async fn neutralize_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<NeutralizeRequest>,
) -> impl IntoResponse {
    use quant_factor::neutralize::{neutralize, NeutralizeConfig};

    // 1. Load factor values
    let rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, trade_date, raw_value FROM factor_value
         WHERE factor_code = $1 AND factor_version = '1.0.0'
           AND trade_date >= $2::date AND trade_date <= $3::date
         ORDER BY trade_date, symbol",
    )
    .bind(&req.factor)
    .bind(&req.start_date)
    .bind(&req.end_date)
    .fetch_all(&state.db)
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => return Json(json!({"code":1,"message":format!("load factor: {}",e)})),
    };

    let values: Vec<FactorValue> = rows
        .into_iter()
        .filter_map(|(sym, date, raw)| {
            raw.map(|r| FactorValue {
                symbol: sym,
                date,
                value: r.try_into().unwrap_or(f64::NAN),
                available_at: None,
            })
        })
        .collect();

    if values.is_empty() {
        return Json(json!({"code":1,"message":"no factor values found"}));
    }

    let output = FactorOutput {
        name: req.factor.clone(),
        values,
        metadata: FactorMetadata {
            factor_name: req.factor.clone(),
            category: FactorCategory::PriceVolume,
            version: "1.0.0".into(),
            params: json!({}),
            computed_at: chrono::Utc::now(),
            symbol_count: 0,
            date_count: 0,
            coverage_ratio: 0.0,
            mean: 0.0,
            std: 0.0,
            min: 0.0,
            max: 0.0,
        },
    };

    // 2. Load industry map
    let ind_rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'",
    )
    .fetch_all(&state.db)
    .await;

    let industries: HashMap<String, String> = match ind_rows {
        Ok(r) => r
            .into_iter()
            .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
            .collect(),
        Err(e) => return Json(json!({"code":1,"message":format!("load industries: {}",e)})),
    };

    // 3. Load size proxy (log daily amount)
    let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, trade_date, amount FROM market_stock_daily_bar_adj
         WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0
         ORDER BY symbol, trade_date",
    )
    .bind(&req.start_date)
    .bind(&req.end_date)
    .fetch_all(&state.db)
    .await;

    let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
    if let Ok(rows) = amt_rows {
        for (sym, date, amt) in rows {
            if let Some(a) = amt {
                let a_val: f64 = a.try_into().unwrap_or(0.0);
                if a_val > 0.0 {
                    size_proxy.entry(sym).or_default().push((date, a_val));
                }
            }
        }
    }

    let config = NeutralizeConfig {
        industries,
        size_proxy,
    };

    // 4. Neutralize
    let (neut_output, result) = neutralize(&output, &config, req.do_industry, req.do_size);

    // 5. Save neutralized values (back to original factor's neutralized_value column)
    let mut saved = 0usize;
    for chunk in neut_output.values.chunks(500) {
        let mut tx = match state.db.begin().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        for fv in chunk {
            if !fv.value.is_finite() {
                continue;
            }
            let res = sqlx::query(
                "UPDATE factor_value SET neutralized_value = $4
                 WHERE factor_code = $1 AND factor_version = '1.0.0'
                   AND symbol = $2 AND trade_date = $3",
            )
            .bind(&req.factor) // original factor name
            .bind(&fv.symbol)
            .bind(fv.date)
            .bind(fv.value)
            .execute(&mut *tx)
            .await;
            if let Ok(r) = res {
                saved += r.rows_affected() as usize;
            }
        }
        let _ = tx.commit().await;
    }

    Json(json!({
        "code": 0,
        "data": {
            "factor_name": neut_output.name,
            "original_count": result.original_count,
            "neutralized_count": result.neutralized_count,
            "saved": saved,
            "industry_neutral": result.industry_neutral,
            "size_neutral": result.size_neutral,
            "sample_values": &neut_output.values[..neut_output.values.len().min(5)]
                .iter().map(|v| json!({"symbol":v.symbol,"date":v.date,"value":v.value})).collect::<Vec<_>>(),
        }
    }))
}

