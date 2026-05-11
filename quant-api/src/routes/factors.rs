//! Factor API routes — compute, standardize, and evaluate factors

use axum::{extract::State, response::IntoResponse, Json};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

use quant_factor::factors::price_volume::*;
use quant_factor::neutralize::NeutralizeConfig;
use quant_factor::*;

use crate::AppState;

// ─── Request/Response types ───────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ComputeFactorRequest {
    pub factor: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    /// Standardization method: "zscore", "rank", "winsorized_3"
    #[serde(default)]
    pub standardize: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FactorInfo {
    pub name: String,
    pub category: String,
    pub version: String,
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateFactorRequest {
    pub factor: String,
    pub symbols: Option<Vec<String>>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    #[serde(default = "default_n_quantiles")]
    pub n_quantiles: usize,
}

fn default_n_quantiles() -> usize { 5 }

#[derive(Debug, Deserialize)]
pub struct BatchSyncRequest {
    pub factor: String,
    #[serde(default = "default_version")]
    pub version: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub standardize: Option<String>,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
}

fn default_version() -> String { "1.0.0".to_string() }
fn default_chunk_size() -> usize { 100 }

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
    ];

    Json(json!({"code": 0, "data": {"factors": factors}}))
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
    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
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

    let input = FactorInput { bars, trade_dates: vec![] };

    // Compute factor
    let mut output = match factor_name {
        "momentum" => MomentumFactor::new(period).compute(&input),
        "volatility" => VolatilityFactor::new(period).compute(&input),
        "turnover" => TurnoverFactor::new(period).compute(&input),
        _ => unreachable!(),
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
    let values: Vec<serde_json::Value> = output.values.iter().map(|fv| {
        json!({
            "symbol": fv.symbol,
            "date": fv.date.format("%Y%m%d").to_string(),
            "value": fv.value,
        })
    }).collect();

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

    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let symbols = req.symbols.as_ref().unwrap_or(&default_symbols);
    let end_date = req.end_date.as_deref().unwrap_or("20250509");
    let start_date = req.start_date.as_deref().unwrap_or("20240101");

    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 2).await {
        Ok(b) => b,
        Err(e) => {
            return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)}));
        }
    };

    let input = FactorInput { bars: bars.clone(), trade_dates: vec![] };

    let output = match factor_name {
        "momentum" => MomentumFactor::new(period).compute(&input),
        "volatility" => VolatilityFactor::new(period).compute(&input),
        "turnover" => TurnoverFactor::new(period).compute(&input),
        _ => unreachable!(),
    };

    // Build forward returns: 1-day forward return per (symbol, date)
    let mut forward_returns: HashMap<(String, chrono::NaiveDate), f64> = HashMap::new();
    for (sym, sym_bars) in &bars {
        for i in 0..sym_bars.len() - 1 {
            let close_t: f64 = sym_bars[i].close.try_into().unwrap_or(f64::NAN);
            let close_t1: f64 = sym_bars[i + 1].close.try_into().unwrap_or(f64::NAN);
            if close_t > 0.0 {
                forward_returns.insert((sym.clone(), sym_bars[i].trade_date), (close_t1 - close_t) / close_t);
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
        None => return Json(json!({"code": 1, "message": format!("Unknown factor: {}", req.factor)})),
    };

    let default_symbols = ["000001.SZ", "000002.SZ", "000300.SH"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let symbols = compute_req.symbols.as_ref().unwrap_or(&default_symbols);
    let end_date = compute_req.end_date.as_deref().unwrap_or("20250509");
    let start_date = compute_req.start_date.as_deref().unwrap_or("20240101");

    let bars = match load_bars(&state.db, symbols, start_date, end_date, period + 1).await {
        Ok(b) => b,
        Err(e) => return Json(json!({"code": 1, "message": format!("Failed to load bars: {}", e)})),
    };

    let input = FactorInput { bars, trade_dates: vec![] };
    let mut output = match factor_name {
        "momentum" => MomentumFactor::new(period).compute(&input),
        "volatility" => VolatilityFactor::new(period).compute(&input),
        "turnover" => TurnoverFactor::new(period).compute(&input),
        _ => unreachable!(),
    };

    let std_method = if let Some(ref m) = compute_req.standardize {
        match m.as_str() {
            "zscore" => Some("zscore".to_string()),
            "rank" => Some("rank".to_string()),
            s if s.starts_with("winsorized_") => Some(s.to_string()),
            _ => None,
        }
    } else { None };

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

    // Persist to DB
    let mut inserted = 0u64;
    for fv in &output.values {
        let result = sqlx::query(
            "INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
               raw_value = EXCLUDED.raw_value,
               normalized_value = EXCLUDED.normalized_value,
               created_at = NOW()"
        )
        .bind(&output.name)
        .bind(&req.version)
        .bind(&fv.symbol)
        .bind(fv.date)
        .bind(fv.value)
        .bind(if std_method.is_some() { Some(fv.value) } else { None::<f64> })
        .execute(&state.db)
        .await;

        match result {
            Ok(_) => inserted += 1,
            Err(e) => tracing::warn!("Failed to insert factor value for {} {}: {}", fv.symbol, fv.date, e),
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
        "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol"
    ).fetch_all(&state.db).await {
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
                 FROM market_stock_daily_bar
                 WHERE symbol = $1 AND trade_date >= $2::date AND trade_date <= $3::date
                 ORDER BY trade_date ASC"
            ).bind(sym).bind(start_d).bind(end_d).fetch_all(&state.db).await;

            match rows {
                Ok(r) if r.len() > period + 1 => {
                    let bars: Vec<DailyBar> = r.into_iter().map(|(s, d, o, h, l, c, pc, cp, v, a)| DailyBar {
                        symbol: s, trade_date: d, open: o, high: h, low: l,
                        close: c, pre_close: pc, change_pct: cp, volume: v, amount: a,
                    }).collect();
                    bars_map.insert(sym.to_string(), bars);
                }
                Ok(_) => {}
                Err(e) => { errors.push(format!("{}/{}: {}", sym, "load", e)); }
            }
        }

        if bars_map.is_empty() { continue; }

        let input = FactorInput { bars: bars_map, trade_dates: vec![] };

        // Compute
        let mut output = match ftype {
            "momentum" => MomentumFactor::new(period).compute(&input),
            "volatility" => VolatilityFactor::new(period).compute(&input),
            "turnover" => TurnoverFactor::new(period).compute(&input),
            "rsi" => RSIFactor::new(period).compute(&input),
            "bb_position" => BBandPositionFactor::new(period).compute(&input),
            "atr" => ATRFactor::new(period).compute(&input),
            "amplitude" => AmplitudeFactor::new(period).compute(&input),
            "vol_price_corr" => VolPriceCorrFactor::new(period).compute(&input),
            "skewness" => SkewnessFactor::new(period).compute(&input),
            "max_drawdown" => MaxDrawdownFactor::new(period).compute(&input),
            _ => break,
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
            output = standardize(&output, method);
        }

        total_vals += output.values.len();

        // Persist
        for fv in &output.values {
            match sqlx::query(
                "INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value)
                 VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
                   raw_value=EXCLUDED.raw_value, normalized_value=EXCLUDED.normalized_value, created_at=NOW()"
            )
            .bind(&output.name).bind(&req.version).bind(&fv.symbol).bind(fv.date)
            .bind(fv.value).bind(if is_std { Some(fv.value) } else { None::<f64> })
            .execute(&state.db).await
            {
                Ok(_) => inserted += 1,
                Err(e) => errors.push(format!("{}: {}", fv.symbol, e)),
            }
        }
    }

    let factor_output_name = format!("{}_{}d{}",
        ftype, period,
        if is_std { "_std" } else { "" }
    );

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

fn default_horizon() -> i16 { 1 }

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
            "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'"
        ).fetch_all(&state.db).await.ok().unwrap_or_default();
        let industries: HashMap<String, String> = ind_rows.into_iter()
            .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
            .collect();

        let mut size_proxy: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
        if do_sz {
            let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
                "SELECT symbol, trade_date, amount FROM market_stock_daily_bar
                 WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0
                 ORDER BY symbol, trade_date"
            ).bind(start_d).bind(end_d).fetch_all(&state.db).await;
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

    // Get all factor versions
    let all_factors: Vec<(String, String)> = match sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT factor_code, factor_version FROM factor_value ORDER BY factor_code"
    ).fetch_all(&state.db).await {
        Ok(v) => v,
        Err(e) => return Json(json!({"code":1,"message":format!("{}",e)})),
    };

    let mut results = Vec::new();

    for (code, ver) in &all_factors {
        // Load factor values
        let fv_rows = sqlx::query_as::<_, (String, chrono::NaiveDate, Option<rust_decimal::Decimal>)>(
            "SELECT symbol, trade_date, COALESCE(normalized_value, raw_value)
             FROM factor_value
             WHERE factor_code=$1 AND factor_version=$2
               AND trade_date>=$3::date AND trade_date<=$4::date
             ORDER BY trade_date, symbol"
        ).bind(code).bind(ver).bind(start_d).bind(end_d).fetch_all(&state.db).await;

        let fv_rows = match fv_rows {
            Ok(r) => r,
            Err(e) => { results.push(json!({"factor":code,"version":ver,"error":e.to_string()})); continue; }
        };

        // Build forward returns
        let symbols: Vec<String> = fv_rows.iter().map(|(s,_,_)| s.clone()).collect::<std::collections::HashSet<_>>().into_iter().collect();
        let bars = match load_bars(&state.db, &symbols, start_d, end_d, 2).await {
            Ok(b) => b,
            Err(e) => { results.push(json!({"factor":code,"version":ver,"error":e.to_string()})); continue; }
        };

        let mut forward_returns: HashMap<(String, chrono::NaiveDate), f64> = HashMap::new();
        for (sym, sym_bars) in &bars {
            for i in 0..sym_bars.len()-1 {
                let c: f64 = sym_bars[i].close.try_into().unwrap_or(f64::NAN);
                let c1: f64 = sym_bars[i+1].close.try_into().unwrap_or(f64::NAN);
                if c > 0.0 { forward_returns.insert((sym.clone(), sym_bars[i].trade_date), (c1-c)/c); }
            }
        }

        // Create FactorOutput
        let values: Vec<FactorValue> = fv_rows.iter()
            .filter_map(|(sym, date, val)| val.and_then(|v| {
                use rust_decimal::prelude::ToPrimitive;
                v.to_f64().map(|fv| FactorValue { symbol: sym.clone(), date: *date, value: fv })
            })).collect();

        if values.len() < 100 { continue; }

        let output = FactorOutput {
            name: code.clone(),
            values,
            metadata: FactorMetadata {
                factor_name: code.clone(), category: FactorCategory::PriceVolume,
                version: ver.clone(), params: serde_json::json!({}),
                computed_at: chrono::Utc::now(),
                symbol_count: 0, date_count: 0, coverage_ratio: 0.0,
                mean: f64::NAN, std: f64::NAN, min: f64::NAN, max: f64::NAN,
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
        results.push(serde_json::to_value(&evaluation).unwrap_or(json!({"error":"serialization failed"})));
    }

    Json(json!({"code":0,"data":{"evaluations":results,"count":results.len()}}))
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
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FactorRef {
    pub factor_code: String,
    #[serde(default = "default_version")]
    pub factor_version: String,
}

fn default_combine_method() -> String { "icir_weighted".to_string() }

pub async fn combine_factors(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CombineFactorsRequest>,
) -> impl IntoResponse {
    let factors: Vec<(String, String)> = req.factors.iter()
        .map(|f| (f.factor_code.clone(), f.factor_version.clone()))
        .collect();

    let method = match req.method.as_str() {
        "equal_weight" => CombineMethod::EqualWeight,
        _ => CombineMethod::IcirWeighted,
    };

    let weights = match compute_weights(&state.db, &factors, method, 1).await {
        Ok(w) => w,
        Err(e) => return Json(json!({"code":1,"message":e})),
    };

    let sd = req.start_date.as_ref().and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());
    let ed = req.end_date.as_ref().and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());

    let inserted = match combine_and_persist(&state.db, &req.combo_name, &req.version, &weights, sd, ed).await {
        Ok(n) => n,
        Err(e) => return Json(json!({"code":1,"message":e})),
    };

    let weights_json: Vec<serde_json::Value> = weights.iter().map(|w| json!({
        "factor_code": w.factor_code,
        "factor_version": w.factor_version,
        "weight": w.weight,
    })).collect();

    Json(json!({"code":0,"data":{"combo_name":req.combo_name,"version":req.version,"weights":weights_json,"inserted":inserted}}))
}

// ─── Helpers ──────────────────────────────────────────────────────

/// Parse factor string like "mom_20d" → ("momentum", 20) or "turn_5d" → ("turnover", 5)
fn parse_factor(name: &str) -> Option<(&'static str, usize)> {
    if let Some(rest) = name.strip_prefix("mom_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("momentum", period))
    } else if let Some(rest) = name.strip_prefix("vol_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("volatility", period))
    } else if let Some(rest) = name.strip_prefix("turn_") {
        let period: usize = rest.trim_end_matches('d').parse().ok()?;
        Some(("turnover", period))
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
             FROM market_stock_daily_bar
             WHERE symbol = $1 AND trade_date >= $2::date AND trade_date <= $3::date
             ORDER BY trade_date ASC"
        )
        .bind(sym)
        .bind(start_date)
        .bind(end_date)
        .fetch_all(pool)
        .await?;

        if rows.len() < min_records {
            tracing::warn!("{} only has {} records (need {})", sym, rows.len(), min_records);
        }

        let bars: Vec<DailyBar> = rows.into_iter().map(|(s, d, o, h, l, c, pc, cp, v, a)| {
            DailyBar {
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
            }
        }).collect();

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
         ORDER BY trade_date, symbol"
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

    let values: Vec<FactorValue> = rows.into_iter()
        .filter_map(|(sym, date, raw)| raw.map(|r| FactorValue {
            symbol: sym, date, value: r.try_into().unwrap_or(f64::NAN),
        }))
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
            symbol_count: 0, date_count: 0, coverage_ratio: 0.0,
            mean: 0.0, std: 0.0, min: 0.0, max: 0.0,
        },
    };

    // 2. Load industry map
    let ind_rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, industry FROM market_stock WHERE list_status = 'L'"
    ).fetch_all(&state.db).await;

    let industries: HashMap<String, String> = match ind_rows {
        Ok(r) => r.into_iter()
            .filter_map(|(s, i)| i.filter(|i| !i.is_empty()).map(|i| (s, i)))
            .collect(),
        Err(e) => return Json(json!({"code":1,"message":format!("load industries: {}",e)})),
    };

    // 3. Load size proxy (log daily amount)
    let amt_rows = sqlx::query_as::<_, (String, NaiveDate, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, trade_date, amount FROM market_stock_daily_bar
         WHERE trade_date >= $1::date AND trade_date <= $2::date AND amount > 0
         ORDER BY symbol, trade_date"
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

    let config = NeutralizeConfig { industries, size_proxy };

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
            if !fv.value.is_finite() { continue; }
            let res = sqlx::query(
                "UPDATE factor_value SET neutralized_value = $4
                 WHERE factor_code = $1 AND factor_version = '1.0.0'
                   AND symbol = $2 AND trade_date = $3"
            )
            .bind(&req.factor)  // original factor name
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
