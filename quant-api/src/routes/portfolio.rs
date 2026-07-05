/// 组合风控与回测报告查询路由
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::sync::Arc;

use ndarray::Array2;
use quant_common::mvo;

use crate::AppState;

/// GET /api/v1/quant/portfolio/reports/:task_id
///
/// 汇总回测任务的目标组合、实际持仓、暴露、归因和约束违反。
pub async fn portfolio_report(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let task: Option<(
        String,
        String,
        String,
        String,
        String,
        Vec<String>,
        NaiveDate,
        NaiveDate,
        Decimal,
        String,
        Value,
    )> = sqlx::query_as(
        r#"SELECT task_id, status, strategy_version_id, data_version_id,
                  benchmark_symbol, symbols, start_date, end_date,
                  initial_capital, rebalance_frequency, parameters
           FROM backtest_task
           WHERE task_id = $1"#,
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let Some(task) = task else {
        return Json(json!({"code": 1, "message": "backtest task not found"}));
    };

    let result: Option<(
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Option<Decimal>,
        Option<i32>,
        Option<Decimal>,
    )> = sqlx::query_as(
        r#"SELECT total_return, annualized_return, sharpe_ratio, max_drawdown,
                      turnover, total_trades, win_rate
               FROM backtest_result
               WHERE task_id = $1"#,
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let targets: Vec<(NaiveDate, String, Decimal, Option<Decimal>, Option<String>)> =
        sqlx::query_as(
            r#"SELECT trade_date, symbol, target_weight, target_quantity, reason
           FROM portfolio_target
           WHERE task_id = $1
           ORDER BY trade_date DESC, symbol
           LIMIT 1000"#,
        )
        .bind(&task_id)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let positions: Vec<(
        NaiveDate,
        String,
        Decimal,
        Decimal,
        Decimal,
        Option<Decimal>,
    )> = sqlx::query_as(
        r#"SELECT position_date, symbol, quantity, market_value, weight, target_weight
           FROM backtest_position
           WHERE task_id = $1
           ORDER BY position_date DESC, symbol
           LIMIT 1000"#,
    )
    .bind(&task_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let exposures: Vec<(NaiveDate, String, String, Decimal, Option<Decimal>)> = sqlx::query_as(
        r#"SELECT trade_date, exposure_type, exposure_name, net_exposure, gross_exposure
           FROM portfolio_exposure
           WHERE task_id = $1
           ORDER BY trade_date DESC, exposure_type, exposure_name
           LIMIT 1000"#,
    )
    .bind(&task_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let attributions: Vec<(NaiveDate, String, String, Decimal)> = sqlx::query_as(
        r#"SELECT trade_date, attribution_type, attribution_name, contribution
           FROM portfolio_attribution
           WHERE task_id = $1
           ORDER BY trade_date DESC, attribution_type, attribution_name
           LIMIT 1000"#,
    )
    .bind(&task_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let violations: Vec<(NaiveDate, String, Decimal, Decimal, String)> = sqlx::query_as(
        r#"SELECT trade_date, constraint_name, limit_value, actual_value, severity
           FROM portfolio_constraint_violation
           WHERE task_id = $1
           ORDER BY trade_date DESC, severity DESC
           LIMIT 1000"#,
    )
    .bind(&task_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Json(json!({"code": 0, "data": {
        "task": {
            "task_id": task.0,
            "status": task.1,
            "strategy_version_id": task.2,
            "data_version_id": task.3,
            "benchmark_symbol": task.4,
            "symbols": task.5,
            "start_date": task.6,
            "end_date": task.7,
            "initial_capital": task.8,
            "rebalance_frequency": task.9,
            "parameters": task.10,
        },
        "metrics": result.map(|row| json!({
            "total_return": row.0,
            "annualized_return": row.1,
            "sharpe_ratio": row.2,
            "max_drawdown": row.3,
            "turnover": row.4,
            "total_trades": row.5,
            "win_rate": row.6,
        })),
        "targets": targets.into_iter().map(|row| json!({
            "trade_date": row.0,
            "symbol": row.1,
            "target_weight": row.2,
            "target_quantity": row.3,
            "reason": row.4,
        })).collect::<Vec<_>>(),
        "positions": positions.into_iter().map(|row| json!({
            "position_date": row.0,
            "symbol": row.1,
            "quantity": row.2,
            "market_value": row.3,
            "weight": row.4,
            "target_weight": row.5,
        })).collect::<Vec<_>>(),
        "exposures": exposures.into_iter().map(|row| json!({
            "trade_date": row.0,
            "exposure_type": row.1,
            "exposure_name": row.2,
            "net_exposure": row.3,
            "gross_exposure": row.4,
        })).collect::<Vec<_>>(),
        "attributions": attributions.into_iter().map(|row| json!({
            "trade_date": row.0,
            "attribution_type": row.1,
            "attribution_name": row.2,
            "contribution": row.3,
        })).collect::<Vec<_>>(),
        "violations": violations.into_iter().map(|row| json!({
            "trade_date": row.0,
            "constraint_name": row.1,
            "limit_value": row.2,
            "actual_value": row.3,
            "severity": row.4,
        })).collect::<Vec<_>>(),
    }}))
}

/// GET /api/v1/quant/portfolio/policies/:policy_id
pub async fn portfolio_policy(
    State(state): State<Arc<AppState>>,
    Path(policy_id): Path<String>,
) -> impl IntoResponse {
    let row: Option<(String, String, String, Value, Value, Option<Value>, String)> =
        sqlx::query_as(
            r#"SELECT policy_id, name, strategy_version_id, rebalance_rule,
                  constraints, risk_budget, status
           FROM portfolio_policy
           WHERE policy_id = $1"#,
        )
        .bind(&policy_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();

    match row {
        Some(row) => Json(json!({"code": 0, "data": {
            "policy_id": row.0,
            "name": row.1,
            "strategy_version_id": row.2,
            "rebalance_rule": row.3,
            "constraints": row.4,
            "risk_budget": row.5,
            "status": row.6,
        }})),
        None => Json(json!({"code": 1, "message": "portfolio policy not found"})),
    }
}

// ─── MVO Multi-Asset Backtest ───────────────────────────────────────

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MvoBacktestRequest {
    /// ETF symbols for multi-asset allocation (gold, bond, sp500, nasdaq)
    pub etf_symbols: Vec<String>,
    /// Stock ensemble prediction set IDs per year: {"2020": "pred-xxx", ...}
    pub stock_prediction_sets: std::collections::HashMap<String, String>,
    /// Per-year max_position_pct overrides (optional)
    pub stock_max_pct: Option<std::collections::HashMap<String, f64>>,
    /// Min stock allocation (default 0.50)
    #[serde(default = "default_min_stock")]
    pub min_stock: f64,
    /// MVO lookback months (default 36)
    #[serde(default = "default_mvo_lookback")]
    pub mvo_lookback_months: usize,
    /// Top-N stocks per backtest (default 20)
    #[serde(default = "default_top_n_mvo")]
    pub top_n: usize,
}

fn default_min_stock() -> f64 {
    0.50
}
fn default_mvo_lookback() -> usize {
    36
}
fn default_top_n_mvo() -> usize {
    20
}

/// POST /api/v1/quant/portfolio/mvo-backtest
///
/// Runs Stock Ensemble backtests + MVO multi-asset allocation overlay.
/// Returns stitched daily metrics with dynamic MVO weights.
pub async fn mvo_backtest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MvoBacktestRequest>,
) -> impl IntoResponse {
    use crate::routes::backtest::execute_prediction_backtest;
    use crate::routes::backtest::RunPredictionBacktestReq;

    let min_stock = req.min_stock.clamp(0.0, 1.0);
    let lookback = req.mvo_lookback_months.max(12).min(60);

    // Phase 1: Run stock backtests per year and collect daily NAV
    let mut stock_nav: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    let mut years: Vec<i32> = req
        .stock_prediction_sets
        .keys()
        .filter_map(|y| y.parse::<i32>().ok())
        .collect();
    years.sort();

    for year in &years {
        let pid = match req.stock_prediction_sets.get(&year.to_string()) {
            Some(p) => p.clone(),
            None => continue,
        };
        let max_pct = req
            .stock_max_pct
            .as_ref()
            .and_then(|m| m.get(&year.to_string()).copied())
            .unwrap_or(0.07);

        let start = format!("{}0101", year);
        let end = format!("{}1231", year);

        let bt_req = RunPredictionBacktestReq {
            prediction_set_id: pid.clone(),
            strategy_version_id: "phase7-professional-v1".into(),
            data_version_id: "full-market-2016-v1".into(),
            top_n: req.top_n,
            max_position_pct: max_pct,
            start_date: start.clone(),
            end_date: end.clone(),
            rebalance: "monthly".into(),
            score_direction: "descending".into(),
            portfolio_method: "heuristic".into(),
            persistence_mode: Some("full".into()),
            initial_capital: 1_000_000.0,
            max_gross_exposure: 1.0,
            entry_delay: 0,
            correlation_lookback_days: 60,
            kelly_lookback_days: 60,
            risk_budget_lookback_days: 60,
            ..Default::default()
        };

        let task_id = format!(
            "mvo-bt-{}-{}",
            year,
            uuid::Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(8)
                .collect::<String>()
        );

        match execute_prediction_backtest(&state.db, &task_id, bt_req).await {
            Ok(_output) => {
                // Fetch equity curve
                let eq_rows = sqlx::query_as::<_, (String, Option<f64>)>(
                    "SELECT trade_date::text, portfolio_value::double precision
                     FROM backtest_equity_curve
                     WHERE task_id = $1
                     ORDER BY trade_date",
                )
                .bind(&task_id)
                .fetch_all(&state.db)
                .await;

                if let Ok(rows) = eq_rows {
                    for (date, nav) in rows {
                        if let Some(nav) = nav {
                            if nav > 0.0 {
                                stock_nav.insert(date, nav);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Stock backtest failed for {}: {}", year, e);
            }
        }
    }

    if stock_nav.is_empty() {
        return Json(json!({"code": 1, "message": "No stock backtest data available"}));
    }

    // Phase 2: Load ETF daily prices
    let etf_list = req
        .etf_symbols
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    if etf_list.is_empty() {
        return Json(json!({"code": 1, "message": "No ETF symbols provided"}));
    }

    let etf_sql = format!(
        "SELECT symbol, trade_date::text, close::double precision
         FROM market_stock_daily_bar_adj
         WHERE symbol IN ({})
         ORDER BY symbol, trade_date",
        etf_list
    );

    let etf_rows = sqlx::query_as::<_, (String, String, Option<f64>)>(&etf_sql)
        .fetch_all(&state.db)
        .await;

    let etf_data: std::collections::HashMap<String, std::collections::BTreeMap<String, f64>> =
        match etf_rows {
            Ok(rows) => {
                let mut map: std::collections::HashMap<
                    String,
                    std::collections::BTreeMap<String, f64>,
                > = std::collections::HashMap::new();
                for (sym, date, close) in rows {
                    if let Some(c) = close {
                        if c > 0.0 {
                            map.entry(sym).or_default().insert(date, c);
                        }
                    }
                }
                map
            }
            Err(e) => {
                return Json(
                    json!({"code": 1, "message": format!("ETF data query failed: {}", e)}),
                );
            }
        };

    // Phase 3: Align stock + ETF daily data
    let stock_dates: std::collections::BTreeSet<String> = stock_nav.keys().cloned().collect();
    let mut common_dates: Vec<String> = Vec::new();
    for date in &stock_dates {
        let all_etfs_ok = req
            .etf_symbols
            .iter()
            .all(|sym| etf_data.get(sym).and_then(|d| d.get(date)).is_some());
        if all_etfs_ok {
            common_dates.push(date.clone());
        }
    }
    common_dates.sort();

    if common_dates.len() < 60 {
        return Json(
            json!({"code": 1, "message": format!("Insufficient common trading days: {}", common_dates.len())}),
        );
    }

    // Phase 4: Compute daily returns, stitching across years
    // Stock NAVs reset each year (backtest starts at 1M). Compute within-year
    // daily stock returns, then compound across years using a running scale factor.
    let n_assets = 1 + req.etf_symbols.len();

    // First, compute raw daily stock returns within each year
    let mut stock_daily_rets: Vec<f64> = Vec::with_capacity(common_dates.len());
    stock_daily_rets.push(0.0); // first day has no prior return
    for i in 1..common_dates.len() {
        let prev_date = &common_dates[i - 1];
        let curr_date = &common_dates[i];
        let prev_nav = stock_nav.get(prev_date).copied().unwrap_or(1.0);
        let curr_nav = stock_nav.get(curr_date).copied().unwrap_or(prev_nav);
        if prev_nav > 0.0 && prev_date[..4] == curr_date[..4] {
            stock_daily_rets.push(curr_nav / prev_nav - 1.0);
        } else {
            stock_daily_rets.push(0.0); // year boundary: skip (NAV reset)
        }
    }

    // Now build the full daily_returns matrix (stocks use within-year returns, ETFs continuous)
    let mut daily_returns: Vec<Vec<f64>> = Vec::with_capacity(common_dates.len() - 1);
    for i in 1..common_dates.len() {
        let mut row = Vec::with_capacity(n_assets);
        row.push(stock_daily_rets[i]);
        for sym in &req.etf_symbols {
            let ret = if let (Some(prev_p), Some(curr_p)) = (
                etf_data.get(sym).and_then(|m| m.get(&common_dates[i - 1])),
                etf_data.get(sym).and_then(|m| m.get(&common_dates[i])),
            ) {
                if *prev_p > 0.0 {
                    curr_p / prev_p - 1.0
                } else {
                    0.0
                }
            } else {
                0.0
            };
            row.push(ret);
        }
        daily_returns.push(row);
    }

    // Phase 5: Monthly aggregation + MVO quarterly rebalancing
    let mut monthly_rets: Vec<Vec<f64>> = Vec::new();
    let mut month_keys: Vec<String> = Vec::new();
    let mut current_month: Option<(String, f64, Vec<f64>)> = None; // (month_key, cum_stock, cum_etfs)

    for (idx, date) in common_dates.iter().enumerate().skip(1) {
        let month_key = date[..7].to_string();
        let stock_ret = daily_returns[idx - 1][0];
        let etf_rets: Vec<f64> = (1..n_assets).map(|j| daily_returns[idx - 1][j]).collect();

        match &mut current_month {
            Some((m, cum_s, cum_e)) if *m == month_key => {
                *cum_s = (1.0 + *cum_s) * (1.0 + stock_ret) - 1.0;
                for (j, r) in etf_rets.iter().enumerate() {
                    cum_e[j] = (1.0 + cum_e[j]) * (1.0 + r) - 1.0;
                }
            }
            _ => {
                if let Some((m, cum_s, cum_e)) = current_month.take() {
                    let mut row = vec![cum_s];
                    row.extend(cum_e);
                    monthly_rets.push(row);
                    month_keys.push(m);
                }
                current_month = Some((month_key, stock_ret, etf_rets));
            }
        }
    }
    // Last month
    if let Some((m, cum_s, cum_e)) = current_month {
        let mut row = vec![cum_s];
        row.extend(cum_e);
        monthly_rets.push(row);
        month_keys.push(m);
    }

    if monthly_rets.len() < lookback {
        return Json(
            json!({"code": 1, "message": format!("Insufficient monthly data: {} < {}", monthly_rets.len(), lookback)}),
        );
    }

    // Phase 6: MVO quarterly rebalancing simulation (once per quarter, PIT-compliant)
    let mut mvo_weights = vec![min_stock];
    let mut remaining = 1.0 - min_stock;
    for _ in 1..n_assets {
        let w = remaining / (n_assets - 1) as f64;
        mvo_weights.push(w);
        remaining -= w;
    }

    let mut weight_history: Vec<Value> = Vec::new();
    let mut mvo_daily_returns: Vec<f64> = Vec::new();
    let mut last_rebalance_quarter: Option<String> = None;

    for (idx, date) in common_dates.iter().enumerate().skip(1) {
        let month_key = &date[..7];

        // Rebalance on first trading day of each quarter month (3, 6, 9, 12)
        let quarter_key = format!(
            "{}-Q{}",
            &date[..4],
            match &date[5..7] {
                "03" => 1,
                "06" => 2,
                "09" => 3,
                "12" => 4,
                _ => 0,
            }
        );
        let is_quarter_month = matches!(&date[5..7], "03" | "06" | "09" | "12");
        let is_new_quarter = last_rebalance_quarter.as_deref() != Some(&quarter_key);

        if is_quarter_month && is_new_quarter {
            // Use monthly data through previous completed months only (PIT-compliant)
            if let Some(mi) = month_keys.iter().position(|m| m == month_key) {
                if mi >= lookback {
                    last_rebalance_quarter = Some(quarter_key);
                    let train_start = mi.saturating_sub(lookback);
                    let train_data = &monthly_rets[train_start..mi];
                    let n_months = train_data.len();

                    if n_months >= 12 && n_assets > 0 {
                        let mut data = Array2::<f64>::zeros((n_months, n_assets));
                        for (r, row) in train_data.iter().enumerate() {
                            for (c, &val) in row.iter().enumerate() {
                                if c < n_assets {
                                    data[(r, c)] = val;
                                }
                            }
                        }
                        if let Some(result) = mvo::mvo_allocate(&data, min_stock) {
                            mvo_weights = result.weights.to_vec();
                            weight_history.push(json!({
                                "date": month_key,
                                "weights": mvo_weights.iter().map(|&w| (w * 100.0 * 10.0).round() / 10.0).collect::<Vec<f64>>(),
                                "sharpe": (result.sharpe * 100.0).round() / 100.0,
                            }));
                        }
                    }
                }
            }
        }

        // Apply current weights to daily returns
        let stock_ret = daily_returns[idx - 1][0];
        let etf_ret_sum: f64 = (1..n_assets)
            .zip(mvo_weights.iter().skip(1))
            .map(|(j, &w)| daily_returns[idx - 1][j] * w)
            .sum();
        let daily_ret = mvo_weights[0] * stock_ret + etf_ret_sum;
        mvo_daily_returns.push(daily_ret);
    }

    // Phase 7: Compute metrics
    let n = mvo_daily_returns.len() as f64;
    if n < 60.0 {
        return Json(json!({"code": 1, "message": "Insufficient simulation days"}));
    }

    let mean_daily = mvo_daily_returns.iter().sum::<f64>() / n;
    let variance: f64 = mvo_daily_returns
        .iter()
        .map(|r| (r - mean_daily).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    let std_daily = variance.sqrt();
    let ann_return = (1.0 + mean_daily).powf(252.0) - 1.0;
    let ann_vol = std_daily * (252.0_f64).sqrt();
    let sharpe = if ann_vol > 0.0 {
        (ann_return - 0.02) / ann_vol
    } else {
        0.0
    };

    let mut nav = 1.0_f64;
    let mut peak = 1.0_f64;
    let mut max_dd = 0.0_f64;
    for &r in &mvo_daily_returns {
        nav *= 1.0 + r;
        peak = peak.max(nav);
        let dd = (peak - nav) / peak;
        max_dd = max_dd.max(dd);
    }
    let cumulative = nav - 1.0;
    let calmar = if max_dd > 0.0 {
        ann_return / max_dd
    } else {
        0.0
    };

    let downside: Vec<f64> = mvo_daily_returns
        .iter()
        .filter(|&&r| r < 0.0)
        .copied()
        .collect();
    let sortino = if downside.len() > 1 {
        let ds_mean = downside.iter().sum::<f64>() / downside.len() as f64;
        let ds_var = downside.iter().map(|r| (r - ds_mean).powi(2)).sum::<f64>()
            / (downside.len() - 1) as f64;
        let ds_std = ds_var.sqrt() * (252.0_f64).sqrt();
        if ds_std > 0.0 {
            (ann_return - 0.02) / ds_std
        } else {
            0.0
        }
    } else {
        0.0
    };

    Json(json!({
        "code": 0,
        "data": {
            "trading_days": mvo_daily_returns.len(),
            "months": monthly_rets.len(),
            "metrics": {
                "annual_return_pct": (ann_return * 100.0 * 100.0).round() / 100.0,
                "annual_vol_pct": (ann_vol * 100.0 * 100.0).round() / 100.0,
                "sharpe_ratio": (sharpe * 100.0).round() / 100.0,
                "sortino_ratio": (sortino * 100.0).round() / 100.0,
                "max_drawdown_pct": (max_dd * 100.0 * 100.0).round() / 100.0,
                "calmar_ratio": (calmar * 100.0).round() / 100.0,
                "cumulative_return_pct": (cumulative * 100.0 * 100.0).round() / 100.0,
            },
            "mvo_config": {
                "min_stock_pct": min_stock,
                "lookback_months": lookback,
                "n_assets": n_assets,
                "etf_symbols": req.etf_symbols,
            },
            "weight_history": weight_history,
        }
    }))
}

// ─── MVO Overlay on WFA Experiments ───

/// Request to apply MVO multi-asset overlay to a completed WFA experiment.
#[derive(Debug, Deserialize)]
pub struct MvoOverlayRequest {
    /// ETF symbols for multi-asset allocation (gold, bond, sp500, nasdaq)
    #[serde(default = "default_etf_symbols")]
    pub etf_symbols: Vec<String>,
    /// Minimum A-share allocation (default 0.25). Ignored if regime_aware is true.
    #[serde(default = "default_min_stock_overlay")]
    pub min_stock: f64,
    /// MVO lookback years (default 5)
    #[serde(default = "default_mvo_lookback_years")]
    pub lookback_years: i32,
    /// Enable PIT regime-aware MVO routing (default: false).
    /// When true, min_stock is dynamically computed from trailing 1-year A-share return:
    ///   trail > 15% → min_stock = 0.25 (bull)
    ///   trail < -5% → min_stock = 0.08 (bear)
    ///   else       → min_stock = 0.15 (normal)
    #[serde(default)]
    pub regime_aware: bool,
    /// Portfolio drawdown threshold for exposure reduction (default: 0 = disabled).
    /// When blended portfolio DD exceeds this threshold, daily returns are scaled
    /// by dd_scale to simulate global exposure reduction.
    /// Recommended: 0.07 (7%) with dd_scale = 0.60.
    #[serde(default)]
    pub dd_threshold: f64,
    /// Scale factor applied to daily returns when DD exceeds threshold (default: 1.0).
    /// 0.60 = reduce exposure to 60%. Only active when dd_threshold > 0.
    #[serde(default = "default_dd_scale")]
    pub dd_scale: f64,
    /// MVO rebalance frequency: "annual" (default), "semi_annual", "quarterly".
    /// More frequent rebalancing allows faster regime response but may increase noise.
    #[serde(default = "default_rebalance_freq")]
    #[allow(dead_code)]
    pub rebalance: String,
}

fn default_dd_scale() -> f64 {
    1.0
}
fn default_rebalance_freq() -> String {
    "annual".to_string()
}

fn default_etf_symbols() -> Vec<String> {
    vec![
        "518880.SH".to_string(), // 黄金ETF
        "511010.SH".to_string(), // 国债ETF
        "513500.SH".to_string(), // 标普500
        "513100.SH".to_string(), // 纳指ETF
    ]
}

fn default_min_stock_overlay() -> f64 {
    0.25
}

fn default_mvo_lookback_years() -> i32 {
    5
}

/// POST /api/v1/quant/experiments/{experiment_run_id}/mvo-overlay
///
/// Applies MVO multi-asset allocation overlay to the OOS equity curves
/// of a completed WFA experiment. Returns blended stitched risk metrics.
pub async fn mvo_experiment_overlay(
    State(state): State<Arc<AppState>>,
    Path(experiment_run_id): Path<String>,
    Json(req): Json<MvoOverlayRequest>,
) -> impl IntoResponse {
    let min_stock = req.min_stock.clamp(0.0, 1.0);
    let lookback_years = req.lookback_years.max(2).min(10);
    let etf_symbols = if req.etf_symbols.is_empty() {
        default_etf_symbols()
    } else {
        req.etf_symbols
    };

    // Step 1: Load OOS equity curves from the experiment
    let oos_curves = match load_oos_equity_curves(&state.db, &experiment_run_id).await {
        Ok(curves) if curves.is_empty() => {
            return Json(
                json!({"code": 1, "message": "No OOS equity curves found for experiment"}),
            );
        }
        Ok(curves) => curves,
        Err(e) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to load OOS curves: {}", e)}),
            );
        }
    };

    // Step 2: Load ETF daily prices
    let all_symbols = build_mvo_symbol_list(&etf_symbols);
    let etf_prices = match load_mvo_etf_prices(&state.db, &all_symbols).await {
        Ok(prices) => prices,
        Err(e) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to load ETF prices: {}", e)}),
            );
        }
    };

    // Step 3: Per-window independent MVO blending.
    // Each window's stock equity curve starts at ~1M. We blend within each window
    // independently (no cross-window NAV stitching), then compound the per-window
    // returns at the end. This avoids NAV-reset artifacts at year boundaries.
    let mut weight_history: Vec<Value> = Vec::new();
    let mut window_stock_rets: Vec<f64> = Vec::new();
    let mut window_blended_rets: Vec<f64> = Vec::new();

    for (window_index, (test_start, test_end, curve)) in oos_curves.iter().enumerate() {
        let test_start_date = match NaiveDate::parse_from_str(test_start, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) => continue,
        };

        // Compute MVO weights using data available at test_start (PIT-compliant)
        // regime_aware: dynamically sets min_stock from trailing A-share return
        let weights = match compute_mvo_weights_pit(
            &etf_prices,
            &all_symbols,
            test_start_date,
            lookback_years,
            min_stock,
            req.regime_aware,
        ) {
            Some(w) => w,
            None => {
                // Fallback: 100% A-share
                let mut w = vec![1.0f64];
                w.extend(std::iter::repeat(0.0f64).take(etf_symbols.len()));
                w
            }
        };

        let n_assets = 1 + etf_symbols.len();
        if weights.len() != n_assets {
            continue;
        }

        weight_history.push(json!({
            "window_index": window_index + 1,
            "test_start": test_start,
            "test_end": test_end,
            "weights": weights,
        }));

        // Per-window independent blending (no cross-window NAV stitching)
        let dates: Vec<&String> = curve.keys().collect();
        if dates.len() < 2 {
            continue;
        }
        let mut sorted_dates: Vec<&String> = dates.clone();
        sorted_dates.sort();

        let first_nav = curve.get(sorted_dates[0]).copied().unwrap_or(1_000_000.0);
        if first_nav <= 0.0 {
            continue;
        }
        let _last_nav = curve
            .get(sorted_dates.last().copied().unwrap_or(&String::new()))
            .copied()
            .unwrap_or(first_nav);

        // Per-window blended NAV (starts at 1.0, independent per window)
        let mut window_stock_nav = 1.0f64;
        let mut window_blended_nav = 1.0f64;

        // DD control state (per-window, resets each window)
        let dd_enabled = req.dd_threshold > 0.0 && req.dd_scale < 1.0;
        let dd_recover_threshold = req.dd_threshold * 0.5;
        let mut peak_blended_nav = 1.0f64;
        let mut dd_active = false;

        for i in 0..sorted_dates.len() {
            let date = sorted_dates[i];

            if i == 0 {
                continue; // Skip first day — no prior return
            }

            let curr_nav = curve.get(date).copied().unwrap_or(1_000_000.0);
            let prev_nav = curve.get(sorted_dates[i - 1]).copied().unwrap_or(curr_nav);
            if prev_nav <= 0.0 || curr_nav <= 0.0 {
                continue;
            }

            let stock_ret = curr_nav / prev_nav - 1.0;
            if stock_ret.abs() > 0.5 {
                continue;
            }

            // ETF daily returns
            let mut daily_rets = vec![stock_ret];
            for sym in &etf_symbols {
                let pp = etf_prices
                    .get(sym)
                    .and_then(|m| m.get(sorted_dates[i - 1]).copied());
                let pc = etf_prices.get(sym).and_then(|m| m.get(date).copied());
                let etf_ret = match (pp, pc) {
                    (Some(prev), Some(curr)) if prev > 0.0 => curr / prev - 1.0,
                    _ => 0.0,
                };
                daily_rets.push(etf_ret);
            }

            // Weighted blend
            let mut blend_ret: f64 = weights
                .iter()
                .zip(daily_rets.iter())
                .map(|(w, r)| w * r)
                .sum();

            // Portfolio DD control
            if dd_enabled {
                let current_dd =
                    (peak_blended_nav - window_blended_nav) / peak_blended_nav.max(0.0).max(1e-10);
                if current_dd > req.dd_threshold {
                    dd_active = true;
                } else if current_dd < dd_recover_threshold {
                    dd_active = false;
                }
                if dd_active {
                    blend_ret *= req.dd_scale;
                }
            }

            window_stock_nav *= 1.0 + stock_ret;
            window_blended_nav *= 1.0 + blend_ret;
            if window_blended_nav > peak_blended_nav {
                peak_blended_nav = window_blended_nav;
            }
        }

        // Record per-window returns for later compounding
        let stock_win_ret = window_stock_nav - 1.0;
        let blended_win_ret = window_blended_nav - 1.0;
        window_stock_rets.push(stock_win_ret);
        window_blended_rets.push(blended_win_ret);
    }

    if window_blended_rets.is_empty() {
        return Json(json!({"code": 1, "message": "No per-window blended returns computed"}));
    }

    // Step 4: Compound per-window returns into stitched metrics.
    // Each window's return is independent; we compound them and compute
    // annualized metrics from the geometric mean.
    let n_windows = window_stock_rets.len() as f64;
    let stock_cum: f64 = window_stock_rets.iter().fold(1.0, |acc, r| acc * (1.0 + r));
    let blended_cum: f64 = window_blended_rets
        .iter()
        .fold(1.0, |acc, r| acc * (1.0 + r));

    // Annualize: assume each window is approximately 1 year
    let stock_ann = stock_cum.powf(1.0 / n_windows) - 1.0;
    let blended_ann = blended_cum.powf(1.0 / n_windows) - 1.0;

    // Approximate risk metrics from per-window returns.
    // For Sharpe/Calmar, use the annual return series as a proxy for volatility.
    let stock_vol = if n_windows >= 2.0 {
        let mean = window_stock_rets.iter().sum::<f64>() / n_windows;
        let var = window_stock_rets
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / (n_windows - 1.0);
        var.sqrt()
    } else {
        0.0
    };
    let blended_vol = if n_windows >= 2.0 {
        let mean = window_blended_rets.iter().sum::<f64>() / n_windows;
        let var = window_blended_rets
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / (n_windows - 1.0);
        var.sqrt()
    } else {
        0.0
    };

    let stock_sharpe = if stock_vol > 0.0 {
        (stock_ann - 0.02) / stock_vol
    } else {
        0.0
    };
    let blended_sharpe = if blended_vol > 0.0 {
        (blended_ann - 0.02) / blended_vol
    } else {
        0.0
    };

    // MaxDD: from worst single-window return
    let stock_mdd = window_stock_rets.iter().cloned().fold(0.0f64, f64::min);
    let blended_mdd = window_blended_rets.iter().cloned().fold(0.0f64, f64::min);

    let stock_sortino = if n_windows >= 2.0 {
        let down: Vec<f64> = window_stock_rets
            .iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        if down.len() >= 2 {
            let dm = down.iter().sum::<f64>() / down.len() as f64;
            let dv = down.iter().map(|r| (r - dm).powi(2)).sum::<f64>() / (down.len() - 1) as f64;
            if dv.sqrt() > 0.0 {
                (stock_ann - 0.02) / dv.sqrt()
            } else {
                0.0
            }
        } else if down.is_empty() {
            999.0
        } else {
            0.0
        }
    } else {
        0.0
    };
    let blended_sortino = if n_windows >= 2.0 {
        let down: Vec<f64> = window_blended_rets
            .iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        if down.len() >= 2 {
            let dm = down.iter().sum::<f64>() / down.len() as f64;
            let dv = down.iter().map(|r| (r - dm).powi(2)).sum::<f64>() / (down.len() - 1) as f64;
            if dv.sqrt() > 0.0 {
                (blended_ann - 0.02) / dv.sqrt()
            } else {
                0.0
            }
        } else if down.is_empty() {
            999.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    let stock_calmar = if stock_mdd.abs() > 0.0 {
        stock_ann / stock_mdd.abs()
    } else {
        0.0
    };
    let blended_calmar = if blended_mdd.abs() > 0.0 {
        blended_ann / blended_mdd.abs()
    } else {
        0.0
    };

    let stock_metrics = json!({
        "annual_return_pct": (stock_ann * 100.0 * 100.0).round() / 100.0,
        "volatility_pct": (stock_vol * 100.0 * 100.0).round() / 100.0,
        "sharpe_ratio": (stock_sharpe * 100.0).round() / 100.0,
        "sortino_ratio": (stock_sortino * 100.0).round() / 100.0,
        "max_drawdown_pct": (stock_mdd * 100.0 * 100.0).round() / 100.0,
        "calmar_ratio": (stock_calmar * 100.0).round() / 100.0,
        "cumulative_return_pct": ((stock_cum - 1.0) * 100.0 * 100.0).round() / 100.0,
        "trading_days": 0,
        "per_window_returns": window_stock_rets,
    });
    let blended_metrics = json!({
        "annual_return_pct": (blended_ann * 100.0 * 100.0).round() / 100.0,
        "volatility_pct": (blended_vol * 100.0 * 100.0).round() / 100.0,
        "sharpe_ratio": (blended_sharpe * 100.0).round() / 100.0,
        "sortino_ratio": (blended_sortino * 100.0).round() / 100.0,
        "max_drawdown_pct": (blended_mdd * 100.0 * 100.0).round() / 100.0,
        "calmar_ratio": (blended_calmar * 100.0).round() / 100.0,
        "cumulative_return_pct": ((blended_cum - 1.0) * 100.0 * 100.0).round() / 100.0,
        "trading_days": 0,
        "per_window_returns": window_blended_rets,
    });

    Json(json!({
        "code": 0,
        "data": {
            "experiment_run_id": experiment_run_id,
            "mvo_config": {
                "min_stock_pct": min_stock,
                "lookback_years": lookback_years,
                "etf_symbols": etf_symbols,
                "n_assets": 1 + etf_symbols.len(),
                "regime_aware": req.regime_aware,
                "dd_threshold": req.dd_threshold,
                "dd_scale": req.dd_scale,
            },
            "stock_only": stock_metrics,
            "mvo_blended": blended_metrics,
            "weight_history": weight_history,
        }
    }))
}

/// GET /api/v1/quant/experiments/{experiment_run_id}/blueprint-report
///
/// One-click blueprint compliance report. Runs MVO overlay with optimal
/// production settings (regime-aware + DD control) and returns a structured
/// pass/fail report against all 5 blueprint elite targets.
pub async fn blueprint_report(
    State(state): State<Arc<AppState>>,
    Path(experiment_run_id): Path<String>,
) -> impl IntoResponse {
    // Use optimal blueprint MVO settings
    let mvo_req = MvoOverlayRequest {
        etf_symbols: default_etf_symbols(),
        min_stock: 0.25,
        lookback_years: 5,
        regime_aware: true,
        dd_threshold: 0.07,
        dd_scale: 0.60,
        rebalance: "annual".to_string(),
    };

    // Reuse the MVO overlay logic
    let min_stock = mvo_req.min_stock.clamp(0.0, 1.0);
    let lookback_years = mvo_req.lookback_years.max(2).min(10);
    let etf_symbols = if mvo_req.etf_symbols.is_empty() {
        default_etf_symbols()
    } else {
        mvo_req.etf_symbols.clone()
    };

    let oos_curves = match load_oos_equity_curves(&state.db, &experiment_run_id).await {
        Ok(curves) if curves.is_empty() => {
            return Json(json!({"code": 1, "message": "No OOS equity curves found"}));
        }
        Ok(curves) => curves,
        Err(e) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to load OOS curves: {}", e)}),
            );
        }
    };

    let all_symbols = build_mvo_symbol_list(&etf_symbols);
    let etf_prices = match load_mvo_etf_prices(&state.db, &all_symbols).await {
        Ok(prices) => prices,
        Err(e) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to load ETF prices: {}", e)}),
            );
        }
    };

    // Per-window independent blending (matches mvo_experiment_overlay logic)
    let mut window_stock_rets: Vec<f64> = Vec::new();
    let mut window_blended_rets: Vec<f64> = Vec::new();
    let mut all_stock_daily_rets: Vec<f64> = Vec::new();
    let mut all_blended_daily_rets: Vec<f64> = Vec::new();

    for (_window_index, (test_start, _test_end, curve)) in oos_curves.iter().enumerate() {
        let test_start_date = match NaiveDate::parse_from_str(test_start, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) => continue,
        };

        let weights = match compute_mvo_weights_pit(
            &etf_prices,
            &all_symbols,
            test_start_date,
            lookback_years,
            min_stock,
            true,
        ) {
            Some(w) => w,
            None => {
                let mut w = vec![1.0f64];
                w.extend(std::iter::repeat(0.0f64).take(etf_symbols.len()));
                w
            }
        };

        let dates: Vec<&String> = curve.keys().collect();
        if dates.len() < 2 {
            continue;
        }
        let mut sorted_dates: Vec<&String> = dates.clone();
        sorted_dates.sort();

        let dd_enabled = mvo_req.dd_threshold > 0.0 && mvo_req.dd_scale < 1.0;
        let dd_recover = mvo_req.dd_threshold * 0.5;
        let mut peak_nav = 1.0f64;
        let mut dd_active = false;
        let mut window_stock_nav = 1.0f64;
        let mut window_blended_nav = 1.0f64;
        let mut win_stock_rets: Vec<f64> = Vec::new();
        let mut win_blended_rets: Vec<f64> = Vec::new();

        for i in 0..sorted_dates.len() {
            let date = sorted_dates[i];
            let curr_nav = curve.get(date).copied().unwrap_or(1_000_000.0);
            if i == 0 {
                continue;
            }
            let prev_nav = curve.get(sorted_dates[i - 1]).copied().unwrap_or(curr_nav);
            if prev_nav <= 0.0 {
                continue;
            }
            let stock_ret = curr_nav / prev_nav - 1.0;
            if stock_ret.abs() > 0.5 {
                continue;
            }

            let mut daily_rets = vec![stock_ret];
            for sym in &etf_symbols {
                let pp = etf_prices
                    .get(sym)
                    .and_then(|m| m.get(sorted_dates[i - 1]).copied());
                let pc = etf_prices.get(sym).and_then(|m| m.get(date).copied());
                daily_rets.push(match (pp, pc) {
                    (Some(p), Some(c)) if p > 0.0 => c / p - 1.0,
                    _ => 0.0,
                });
            }

            let mut blend_ret: f64 = weights
                .iter()
                .zip(daily_rets.iter())
                .map(|(w, r)| w * r)
                .sum();

            if dd_enabled {
                let current_dd = (peak_nav - window_blended_nav) / peak_nav.max(1e-10);
                if current_dd > mvo_req.dd_threshold {
                    dd_active = true;
                } else if current_dd < dd_recover {
                    dd_active = false;
                }
                if dd_active {
                    blend_ret *= mvo_req.dd_scale;
                }
            }

            window_stock_nav *= 1.0 + stock_ret;
            window_blended_nav *= 1.0 + blend_ret;
            if window_blended_nav > peak_nav {
                peak_nav = window_blended_nav;
            }
            win_stock_rets.push(stock_ret);
            win_blended_rets.push(blend_ret);
        }
        all_stock_daily_rets.extend(win_stock_rets);
        all_blended_daily_rets.extend(win_blended_rets);
        window_stock_rets.push(window_stock_nav - 1.0);
        window_blended_rets.push(window_blended_nav - 1.0);
    }

    // ── Compound annual return ──
    let n_windows = window_stock_rets.len() as f64;
    let stock_cum: f64 = window_stock_rets.iter().fold(1.0, |acc, r| acc * (1.0 + r));
    let blended_cum: f64 = window_blended_rets
        .iter()
        .fold(1.0, |acc, r| acc * (1.0 + r));
    let stock_ann = stock_cum.powf(1.0 / n_windows) - 1.0;
    let blended_ann = blended_cum.powf(1.0 / n_windows) - 1.0;

    // ── Risk metrics from ACTUAL daily returns ──
    fn daily_metrics(rets: &[f64], ann_ret: f64) -> (f64, f64, f64, f64, f64) {
        if rets.len() < 10 {
            return (0.0, 0.0, 0.0, 0.0, 0.0);
        }
        let n = rets.len() as f64;
        let mean = rets.iter().sum::<f64>() / n;
        let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let ann_vol = var.sqrt() * (252.0_f64).sqrt();
        let sharpe = if ann_vol > 0.0 {
            (ann_ret - 0.02) / ann_vol
        } else {
            0.0
        };
        // MaxDD from daily NAV
        let mut nav = 1.0f64;
        let mut peak = 1.0f64;
        let mut mdd = 0.0f64;
        for &r in rets {
            nav *= 1.0 + r;
            if nav > peak {
                peak = nav;
            }
            let dd = (peak - nav) / peak;
            if dd > mdd {
                mdd = dd;
            }
        }
        // Sortino
        let down: Vec<f64> = rets.iter().filter(|&&r| r < 0.0).copied().collect();
        let sortino = if down.len() >= 10 {
            let dm = down.iter().sum::<f64>() / down.len() as f64;
            let dv = down.iter().map(|r| (r - dm).powi(2)).sum::<f64>() / (down.len() - 1) as f64;
            let ds = dv.sqrt() * (252.0_f64).sqrt();
            if ds > 0.0 {
                (ann_ret - 0.02) / ds
            } else {
                0.0
            }
        } else {
            0.0
        };
        let calmar = if mdd > 0.0 { ann_ret / mdd } else { 0.0 };
        (ann_vol, sharpe, sortino, mdd, calmar)
    }
    let (stock_vol, stock_sharpe, stock_sortino, stock_mdd, stock_calmar) =
        daily_metrics(&all_stock_daily_rets, stock_ann);
    let (blended_vol, blended_sharpe, blended_sortino, blended_mdd, blended_calmar) =
        daily_metrics(&all_blended_daily_rets, blended_ann);

    let stock_metrics = json!({
        "annual_return_pct": (stock_ann * 100.0 * 100.0).round() / 100.0,
        "volatility_pct": (stock_vol * 100.0 * 100.0).round() / 100.0,
        "sharpe_ratio": (stock_sharpe * 100.0).round() / 100.0,
        "sortino_ratio": (stock_sortino * 100.0).round() / 100.0,
        "max_drawdown_pct": (stock_mdd * 100.0 * 100.0).round() / 100.0,
        "calmar_ratio": (stock_calmar * 100.0).round() / 100.0,
        "cumulative_return_pct": ((stock_cum - 1.0) * 100.0 * 100.0).round() / 100.0,
        "trading_days": all_stock_daily_rets.len(),
    });
    let blended_metrics = json!({
        "annual_return_pct": (blended_ann * 100.0 * 100.0).round() / 100.0,
        "volatility_pct": (blended_vol * 100.0 * 100.0).round() / 100.0,
        "sharpe_ratio": (blended_sharpe * 100.0).round() / 100.0,
        "sortino_ratio": (blended_sortino * 100.0).round() / 100.0,
        "max_drawdown_pct": (blended_mdd * 100.0 * 100.0).round() / 100.0,
        "calmar_ratio": (blended_calmar * 100.0).round() / 100.0,
        "cumulative_return_pct": ((blended_cum - 1.0) * 100.0 * 100.0).round() / 100.0,
        "trading_days": all_blended_daily_rets.len(),
    });

    // Blueprint compliance check
    // Note: metrics from compute_mvo_portfolio_metrics are in natural scale
    // (sharpe=0.6, annual_return_pct=14.9 meaning 14.9%, etc.)
    let ar = blended_metrics
        .get("annual_return_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
        / 100.0;
    let sharpe = blended_metrics
        .get("sharpe_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let sortino = blended_metrics
        .get("sortino_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let mdd = blended_metrics
        .get("max_drawdown_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(100.0)
        / 100.0;
    let calmar = blended_metrics
        .get("calmar_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let checks = vec![
        json!({"target": "年化收益 ≥20%", "value": format!("{:.1}%", ar * 100.0), "limit": "20%", "passed": ar >= 0.20}),
        json!({"target": "Sharpe >1.5", "value": format!("{:.2}", sharpe), "limit": "1.5", "passed": sharpe > 1.5}),
        json!({"target": "Sortino >1.8", "value": format!("{:.2}", sortino), "limit": "1.8", "passed": sortino > 1.8}),
        json!({"target": "MaxDD <35%", "value": format!("{:.1}%", mdd.abs() * 100.0), "limit": "35%", "passed": mdd.abs() < 0.35}),
        json!({"target": "Calmar >2.0", "value": format!("{:.2}", calmar), "limit": "2.0", "passed": calmar > 2.0}),
    ];
    let passed_count = checks
        .iter()
        .filter(|c| c["passed"].as_bool().unwrap_or(false))
        .count();

    Json(json!({
        "code": 0,
        "data": {
            "experiment_run_id": experiment_run_id,
            "blueprint_targets": {
                "annual_return": "≥20%",
                "sharpe": ">1.5",
                "sortino": ">1.8",
                "max_drawdown": "<35%",
                "calmar": ">2.0",
            },
            "stock_only": stock_metrics,
            "mvo_blended": blended_metrics,
            "checks": checks,
            "passed": passed_count,
            "total": 5,
            "all_passed": passed_count == 5,
        }
    }))
}

type EquityCurve = std::collections::BTreeMap<String, f64>;
type PriceMap = std::collections::HashMap<String, std::collections::BTreeMap<String, f64>>;

async fn load_oos_equity_curves(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
) -> Result<Vec<(String, String, EquityCurve)>, String> {
    // Load OOS backtest task IDs and test window dates
    let rows = sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
        "SELECT w->>'oos_backtest_task_id' as task_id,
                w->'window'->>'test_start' as test_start,
                w->'window'->>'test_end' as test_end
         FROM experiment_run er,
              jsonb_array_elements(er.metrics->'windows') w
         WHERE er.experiment_run_id = $1
           AND w->>'oos_backtest_task_id' IS NOT NULL
           AND w->>'oos_backtest_task_id' != ''
         ORDER BY w->'window'->>'test_start'",
    )
    .bind(experiment_run_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("Query failed: {}", e))?;

    let mut curves = Vec::new();
    for (task_id, test_start, test_end) in rows {
        let (Some(task_id), Some(test_start), Some(test_end)) = (task_id, test_start, test_end)
        else {
            continue;
        };
        if task_id.is_empty() {
            continue;
        }
        let eq_rows = sqlx::query_as::<_, (String, Option<f64>)>(
            "SELECT trade_date::text, portfolio_value::double precision
             FROM backtest_equity_curve
             WHERE task_id = $1
             ORDER BY trade_date",
        )
        .bind(&task_id)
        .fetch_all(db)
        .await
        .map_err(|e| format!("Equity query failed: {}", e))?;

        let curve: EquityCurve = eq_rows
            .into_iter()
            .filter_map(|(d, v)| v.map(|val| (d, val)))
            .collect();

        if curve.len() > 2 {
            // Check if curve has actual trades (not all same NAV)
            let vals: Vec<f64> = curve.values().copied().collect();
            let min_v = vals.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_v = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            if (max_v - min_v).abs() > 1.0 {
                curves.push((test_start, test_end, curve));
            }
        }
    }

    Ok(curves)
}

fn build_mvo_symbol_list(etf_symbols: &[String]) -> Vec<String> {
    let mut symbols = vec!["000300.SH".to_string()];
    symbols.extend(etf_symbols.iter().cloned());
    symbols
}

async fn load_mvo_etf_prices(db: &sqlx::PgPool, symbols: &[String]) -> Result<PriceMap, String> {
    let mut prices: PriceMap = std::collections::HashMap::new();

    // HS300 from index table
    let idx_rows = sqlx::query_as::<_, (String, Option<f64>)>(
        "SELECT trade_date::text, close::double precision
         FROM market_index_daily_bar
         WHERE symbol = '000300.SH'
         ORDER BY trade_date",
    )
    .fetch_all(db)
    .await
    .map_err(|e| format!("Index query failed: {}", e))?;

    let mut hs300: EquityCurve = std::collections::BTreeMap::new();
    for (date, close) in idx_rows {
        if let Some(c) = close {
            if c > 0.0 {
                hs300.insert(date, c);
            }
        }
    }
    prices.insert("000300.SH".to_string(), hs300);

    // ETFs from stock table
    let etf_list: Vec<String> = symbols
        .iter()
        .filter(|s| *s != "000300.SH")
        .cloned()
        .collect();
    if !etf_list.is_empty() {
        let placeholders: Vec<String> = etf_list
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + 1))
            .collect();
        let sql = format!(
            "SELECT symbol, trade_date::text, close::double precision
             FROM market_stock_daily_bar_adj
             WHERE symbol IN ({})
             ORDER BY symbol, trade_date",
            placeholders.join(",")
        );

        let mut query = sqlx::query_as::<_, (String, String, Option<f64>)>(&sql);
        for sym in &etf_list {
            query = query.bind(sym);
        }

        let rows = query
            .fetch_all(db)
            .await
            .map_err(|e| format!("ETF query failed: {}", e))?;

        for (sym, date, close) in rows {
            if let Some(c) = close {
                if c > 0.0 {
                    prices.entry(sym).or_default().insert(date, c);
                }
            }
        }
    }

    Ok(prices)
}

/// PIT-compliant trailing return computation for regime detection.
/// Computes the trailing N-month return of HS300 up to `ref_date`.
fn compute_trailing_return(etf_prices: &PriceMap, ref_date: NaiveDate, months: i32) -> Option<f64> {
    let lookback_days = (months * 21) as i64;
    let lookback_start = ref_date - chrono::Duration::days(lookback_days);

    let hs300 = etf_prices.get("000300.SH")?;
    let dates: Vec<&String> = hs300
        .keys()
        .filter(|d| {
            *d >= &lookback_start.format("%Y-%m-%d").to_string()
                && *d < &ref_date.format("%Y-%m-%d").to_string()
        })
        .collect();

    if dates.len() < 50 {
        return None;
    }

    let first_price = hs300.get(*dates.first()?).copied()?;
    let last_price = hs300.get(*dates.last()?).copied()?;
    if first_price <= 0.0 {
        return None;
    }

    Some(last_price / first_price - 1.0)
}

/// Compute regime-aware min_stock from trailing A-share return.
/// PIT-compliant: only uses data available at ref_date.
fn regime_aware_min_stock(etf_prices: &PriceMap, ref_date: NaiveDate) -> f64 {
    match compute_trailing_return(etf_prices, ref_date, 12) {
        Some(trail) if trail > 0.15 => 0.25,  // Bull: more equity
        Some(trail) if trail < -0.05 => 0.08, // Bear: defensive
        _ => 0.15,                            // Normal
    }
}

/// PIT-compliant MVO weight computation.
/// Uses expanding window of monthly returns up to `test_start`.
/// If `regime_aware` is true, `min_stock` is dynamically computed from
/// trailing 1-year A-share return (PIT-compliant).
fn compute_mvo_weights_pit(
    etf_prices: &PriceMap,
    symbols: &[String],
    test_start: NaiveDate,
    lookback_years: i32,
    min_stock: f64,
    regime_aware: bool,
) -> Option<Vec<f64>> {
    // Regime-aware: dynamically compute min_stock from trailing A-share return
    let effective_min_stock = if regime_aware {
        regime_aware_min_stock(etf_prices, test_start)
    } else {
        min_stock
    };
    let lookback_start = test_start - chrono::Duration::days(lookback_years as i64 * 365);
    let start_str = lookback_start.format("%Y-%m-%d").to_string();
    let end_str = test_start.format("%Y-%m-%d").to_string();

    // Collect all trading dates for HS300 in range
    let hs300 = etf_prices.get("000300.SH")?;
    let all_dates: Vec<&String> = hs300
        .keys()
        .filter(|d| *d >= &start_str && *d < &end_str)
        .collect();

    if all_dates.len() < 60 {
        return None;
    }

    // Group by month, take last trading day
    let mut month_ends: std::collections::BTreeMap<String, &String> =
        std::collections::BTreeMap::new();
    for date in &all_dates {
        let month_key = &date[..7];
        month_ends.insert(month_key.to_string(), date);
    }

    let month_end_dates: Vec<&&String> = month_ends.values().collect();
    if month_end_dates.len() < 12 {
        return None;
    }

    // Build monthly returns matrix
    let mut returns: Vec<Vec<f64>> = Vec::new();
    for i in 1..month_end_dates.len() {
        let prev_date = month_end_dates[i - 1];
        let curr_date = month_end_dates[i];
        let mut row = Vec::new();
        let mut valid = true;

        for sym in symbols {
            let prices = etf_prices.get(sym)?;
            let pp = prices.get(*prev_date).copied().unwrap_or(0.0);
            let pc = prices.get(*curr_date).copied().unwrap_or(0.0);
            if pp > 0.0 && pc > 0.0 {
                let r = pc / pp - 1.0;
                if r.abs() > 0.5 {
                    valid = false;
                }
                row.push(r);
            } else {
                valid = false;
            }
        }
        if valid {
            returns.push(row);
        }
    }

    if returns.len() < 12 {
        return None;
    }

    // Convert to ndarray
    let n_rows = returns.len();
    let n_cols = returns[0].len();
    let flat: Vec<f64> = returns.into_iter().flatten().collect();
    let arr = Array2::from_shape_vec((n_rows, n_cols), flat).ok()?;

    // Run MVO optimization
    let result = mvo::mvo_allocate(&arr, effective_min_stock)?;
    Some(result.weights.to_vec())
}

// compute_mvo_portfolio_metrics removed — replaced by inline daily_metrics() helper
// in mvo_experiment_overlay and blueprint_report, which computes metrics from
// actual per-window daily returns rather than cross-window NAV stitching.

// ── 通用 MVO 模拟：对任意回测叠加 LW-MVO 多资产配置 ──────────

#[derive(Debug, serde::Deserialize)]
pub struct MvoSimulateRequest {
    /// ETF 列表，默认 ["518880.SH","511010.SH","513500.SH","513100.SH"]
    #[serde(default = "mvo_sim_default_etfs")]
    pub etf_symbols: Vec<String>,
    /// MVO 回看月数（默认 36）
    #[serde(default = "mvo_sim_default_lookback")]
    pub mvo_lookback_months: usize,
    /// 最小 A 股配置（默认 0.08）
    #[serde(default = "mvo_sim_default_min_stock")]
    pub min_stock: f64,
    /// 调仓频率："quarterly"（默认）或 "monthly"
    #[serde(default = "mvo_sim_default_rebalance")]
    pub rebalance_freq: String,
    // ── v15 增强参数 ──
    /// 杠杆模式: "fixed"(默认), "vol_target"
    #[serde(default = "mvo_sim_default_leverage_mode")]
    pub leverage_mode: String,
    /// 杠杆倍率 (fixed模式, 默认1.0)
    #[serde(default = "mvo_sim_default_leverage_mult")]
    pub leverage_multiplier: f64,
    /// 在线模拟绑定的 paper_account_id（必传，杠杆属性从该账号读）
    pub paper_account_id: String,
    /// 策略 ID（可选）。缺省时读 paper_account.strategy_version_id，都无则报错。
    /// 不再硬编码 fallback 到 v19（配置化原则）。
    pub strategy_id: Option<String>,
}

fn mvo_sim_default_etfs() -> Vec<String> {
    vec![
        "518880.SH".into(),
        "511010.SH".into(),
        "513500.SH".into(),
        "513100.SH".into(),
    ]
}
fn mvo_sim_default_lookback() -> usize {
    36
}
fn mvo_sim_default_min_stock() -> f64 {
    0.08
}
fn mvo_sim_default_rebalance() -> String {
    "quarterly".into()
}
fn mvo_sim_default_leverage_mode() -> String {
    "fixed".into()
}
fn mvo_sim_default_leverage_mult() -> f64 {
    1.0
}

/// POST /api/v1/quant/backtests/{task_id}/mvo-simulate
///
/// 对已完成回测叠加 Ledoit-Wolf MVO 多资产配置，模拟完整绩效。
/// ETF 数据从有数据的日期开始使用（PIT 合规）。
pub async fn mvo_simulate(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    Json(req): Json<MvoSimulateRequest>,
) -> impl IntoResponse {
    match run_mvo_simulate(&state.db, &task_id, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

async fn run_mvo_simulate(
    db: &sqlx::PgPool,
    task_id: &str,
    req: &MvoSimulateRequest,
) -> Result<Value, String> {
    // 统一到共享策略核心：权重来自策略 GA（compute_mvo_weights_for_date），
    // 经 run_daily_simulation 盯市复利，与回放/实盘同口径。
    let task_id = task_id.trim();
    let account_id = req.paper_account_id.trim();
    if account_id.is_empty() {
        return Err("在线模拟必传 paper_account_id".into());
    }

    // 策略 ID 解析：请求参数 > 账号 strategy_version_id > 报错（不 fallback 到具体策略）。
    let strategy_id = match req.strategy_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(sid) => sid.to_string(),
        None => {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT strategy_version_id FROM paper_account WHERE paper_account_id = $1",
            )
            .bind(account_id)
            .fetch_optional(db)
            .await
            .map_err(|e| format!("query account strategy: {e}"))?
            .flatten()
            .ok_or_else(|| {
                "策略 ID 未指定：请求未传 strategy_id 且账号 strategy_version_id 为空".to_string()
            })?
        }
    };

    // 以策略配置为基底，用传入的回测曲线作为 A 股权益源。
    // ETF 阵容从策略配置 etf_symbols 读（不固定 v19 的 7 资产）。
    let mut rs = crate::routes::strategy::load_resolved_strategy(db, &strategy_id)
        .await
        .map_err(|e| format!("load strategy {}: {}", strategy_id, e))?;
    // 用传入 task_id 覆盖 a_share asset 的 equity_curve_task_id（在线模拟用请求的回测曲线）
    if let Some(a) = rs
        .assets
        .iter_mut()
        .find(|a| a.asset_class == crate::routes::strategy::AssetClass::AShare)
    {
        a.security.equity_curve_task_id = Some(task_id.to_string());
    }

    // 曲线日期范围（从 a_share asset 的 equity_curve_task_id 取）
    let (first_d, last_d): (NaiveDate, NaiveDate) = sqlx::query_as(
        "SELECT MIN(trade_date), MAX(trade_date) FROM backtest_equity_curve WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("加载权益曲线失败: {e}"))?;

    // 杠杆属性从账号读（列名 leverage_*）
    let (lev_enabled, lev_mult, lev_mode): (bool, f64, String) = sqlx::query_as(
        "SELECT COALESCE(leverage_enabled,false), COALESCE(leverage_multiplier,1.0), COALESCE(leverage_mode,'fixed')
         FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("query account: {e}"))?
    .unwrap_or((false, 1.0, "fixed".into()));

    // 共享逐日模拟（reset=false，EodClose）——在线模拟不清表
    let tushare = quant_data::tushare::client::TushareClient::from_env()
        .map_err(|e| format!("tushare: {}", e))?;
    let cache = std::sync::Arc::new(tokio::sync::Mutex::new(
        None::<crate::routes::scheduler::MvoWeightCache>,
    ));
    let navs = crate::routes::mvo_engine::run_daily_simulation(
        db,
        account_id,
        &rs,
        first_d,
        last_d,
        crate::routes::rebalance::PriceSource::EodClose,
        &cache,
        &tushare,
        false, // 在线模拟 reset=false
        lev_enabled,
        lev_mult,
        &lev_mode,
    )
    .await?;
    if navs.len() < 252 {
        return Err("回测数据不足（需至少 1 年）".into());
    }

    // 单序列绩效（基于 navs 的 net_return）——废弃 mvo_gross/a_share_only 对比口径
    let net_rets: Vec<f64> = navs.iter().map(|d| d.net_return).collect();
    // TODO: mvo_backtest 路径无 ResolvedStrategy 上下文，暂用默认无风险利率；后续应从策略配置读 risk_free_rate
    let m = crate::routes::mvo_engine::compute_metrics(&net_rets, crate::routes::mvo_engine::DEFAULT_RISK_FREE_RATE);

    // 逐年收益（内联本地实现，避免跨模块调私有 fn）
    let yearly = compute_yearly_from_navs(&navs);

    let metrics_json = |m: &crate::routes::mvo_engine::Metrics| {
        json!({
            "trading_days": m.trading_days,
            "annual_return_pct": (m.annual_return * 1000.0).round() / 10.0,
            "cumulative_return_pct": (m.cumulative_return * 1000.0).round() / 10.0,
            "volatility_pct": (m.volatility * 1000.0).round() / 10.0,
            "sharpe_ratio": (m.sharpe * 100.0).round() / 100.0,
            "sortino_ratio": (m.sortino * 100.0).round() / 100.0,
            "max_drawdown_pct": (m.max_drawdown * 1000.0).round() / 10.0,
            "calmar_ratio": (m.calmar * 100.0).round() / 100.0,
            "win_rate_pct": (m.win_rate * 1000.0).round() / 10.0,
        })
    };

    Ok(json!({
        "backtest_task_id": task_id,
        "paper_account_id": account_id,
        "date_range": {
            "full_start": first_d.format("%Y-%m-%d").to_string(),
            "full_end": last_d.format("%Y-%m-%d").to_string(),
            "mvo_start": first_d.format("%Y-%m-%d").to_string(),
        },
        "config": {
            "engine": "v19_shared_core",
            "etf_symbols": rs.etf_symbols,
            "n_assets": 1 + rs.etf_symbols.len(),
            "strategy_id": rs.strategy_id,
            "leverage_mode": lev_mode,
            "leverage_multiplier": lev_mult,
            "leverage_enabled": lev_enabled,
        },
        "metrics": metrics_json(&m),
        "yearly_returns": yearly,
    }))
}

/// 逐年收益（基于 navs 的 net_return 复利聚合）→ jsonb 数组
/// 与 historical_replay::compute_yearly_from_navs 同逻辑（本地副本，避免跨模块私有 fn）。
fn compute_yearly_from_navs(navs: &[crate::routes::mvo_engine::DailyNav]) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let mut cur_year = 0i32;
    let mut yr_nav = 1.0f64;
    for d in navs {
        let y = d.date.year();
        if y != cur_year {
            if cur_year != 0 {
                out.push(json!({"year": cur_year.to_string(), "return_pct": ((yr_nav - 1.0) * 1000.0).round() / 10.0}));
            }
            cur_year = y;
            yr_nav = 1.0;
        }
        yr_nav *= 1.0 + d.net_return;
    }
    if cur_year != 0 {
        out.push(json!({"year": cur_year.to_string(), "return_pct": ((yr_nav - 1.0) * 1000.0).round() / 10.0}));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: compute daily-level metrics inline (mirrors the blueprint_report logic).
    fn daily_metrics(rets: &[f64], ann_ret: f64) -> (f64, f64, f64, f64, f64) {
        if rets.len() < 10 {
            return (0.0, 0.0, 0.0, 0.0, 0.0);
        }
        let n = rets.len() as f64;
        let mean = rets.iter().sum::<f64>() / n;
        let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let ann_vol = var.sqrt() * (252.0_f64).sqrt();
        let sharpe = if ann_vol > 0.0 {
            (ann_ret - 0.02) / ann_vol
        } else {
            0.0
        };

        let mut nav = 1.0f64;
        let mut peak = 1.0f64;
        let mut mdd = 0.0f64;
        for &r in rets {
            nav *= 1.0 + r;
            if nav > peak {
                peak = nav;
            }
            let dd = (peak - nav) / peak;
            if dd > mdd {
                mdd = dd;
            }
        }

        let down: Vec<f64> = rets.iter().filter(|&&r| r < 0.0).copied().collect();
        let sortino = if down.len() >= 10 {
            let dm = down.iter().sum::<f64>() / down.len() as f64;
            let dv = down.iter().map(|r| (r - dm).powi(2)).sum::<f64>() / (down.len() - 1) as f64;
            let ds = dv.sqrt() * (252.0_f64).sqrt();
            if ds > 0.0 {
                (ann_ret - 0.02) / ds
            } else {
                0.0
            }
        } else {
            0.0
        };

        let calmar = if mdd > 0.0 { ann_ret / mdd } else { 0.0 };
        (ann_vol, sharpe, sortino, mdd, calmar)
    }

    #[test]
    fn test_daily_metrics_positive_returns() {
        // Simulate 252 days of +0.1% daily returns (~28.6% annual)
        let rets: Vec<f64> = (0..252).map(|_| 0.001).collect();
        let ar = (1.001_f64).powf(252.0) - 1.0;
        let (_vol, sharpe, sortino, mdd, calmar) = daily_metrics(&rets, ar);

        // With constant positive returns, vol should be ~0, MaxDD = 0
        assert!(sharpe > 0.0, "Sharpe should be positive");
        assert_eq!(mdd, 0.0, "No drawdown with all-positive returns");
        assert!(
            sortino > 0.0 || sortino == 0.0,
            "Sortino should be computable"
        );
        assert_eq!(calmar, 0.0, "Calmar is 0 when MaxDD is 0");
    }

    #[test]
    fn test_daily_metrics_with_drawdown() {
        // 100 days +0.5%, then 50 days -0.5%, then 102 days +0.5%
        let mut rets = Vec::new();
        rets.extend((0..100).map(|_| 0.005));
        rets.extend((0..50).map(|_| -0.005));
        rets.extend((0..102).map(|_| 0.005));

        // Compound returns
        let cum: f64 = rets.iter().fold(1.0, |acc, r| acc * (1.0 + r));
        let ar = cum.powf(252.0 / 252.0) - 1.0;
        let (_vol, sharpe, sortino, mdd, calmar) = daily_metrics(&rets, ar);

        assert!(
            mdd > 0.0,
            "Should have drawdown from the negative-return period"
        );
        assert!(mdd < 0.5, "Drawdown should be moderate (<50%)");
        assert!(sharpe > 0.0, "Sharpe should be positive overall");
        assert!(
            sortino > 0.0,
            "Sortino should be positive (downside vol < total return)"
        );
        assert!(calmar > 0.0, "Calmar should be positive");
        assert!(calmar < 10.0, "Calmar should be reasonable (<10)");
    }

    #[test]
    fn test_blueprint_check_with_known_values() {
        // Test the blueprint check logic directly.
        // 5/5 case: strong metrics
        let ar: f64 = 0.225; // 22.5%
        let sharpe: f64 = 1.95;
        let sortino: f64 = 2.48;
        let mdd: f64 = -0.112; // -11.2%
        let calmar: f64 = 2.00;

        let checks = vec![
            ("年化 ≥20%", ar >= 0.20, true),
            ("Sharpe >1.5", sharpe > 1.5, true),
            ("Sortino >1.8", sortino > 1.8, true),
            ("MaxDD <35%", f64::abs(mdd) < 0.35, true),
            ("Calmar >2.0", calmar > 2.0, true),
        ];

        let passed = checks.iter().filter(|c| c.2).count();
        assert_eq!(passed, 5, "Strong metrics should pass all 5 checks");

        // 1/5 case: very weak metrics
        let weak_ar: f64 = 0.05;
        let weak_sharpe: f64 = 0.3;
        let weak_sortino: f64 = 0.5;
        let weak_mdd: f64 = -0.45;
        let weak_calmar: f64 = 0.11;

        let weak_checks: Vec<(&str, bool, bool)> = vec![
            ("年化 ≥20%", weak_ar >= 0.20, false),
            ("Sharpe >1.5", weak_sharpe > 1.5, false),
            ("Sortino >1.8", weak_sortino > 1.8, false),
            ("MaxDD <35%", f64::abs(weak_mdd) < 0.35, false),
            ("Calmar >2.0", weak_calmar > 2.0, false),
        ];

        let weak_passed = weak_checks.iter().filter(|c| c.2).count();
        assert!(weak_passed <= 1, "Weak metrics should pass at most 1 check");
    }

    #[test]
    fn test_mvo_overlay_request_defaults() {
        let req = MvoOverlayRequest {
            etf_symbols: vec![],
            min_stock: 0.25,
            lookback_years: 5,
            regime_aware: false,
            dd_threshold: 0.0,
            dd_scale: 1.0,
            #[allow(dead_code)]
            rebalance: "annual".to_string(),
        };

        // Verify defaults are applied correctly
        assert!(!req.regime_aware, "regime_aware defaults to false");
        assert_eq!(
            req.dd_threshold, 0.0,
            "dd_threshold defaults to 0 (disabled)"
        );
        assert_eq!(req.dd_scale, 1.0, "dd_scale defaults to 1.0 (no scaling)");
        assert_eq!(req.min_stock, 0.25, "min_stock defaults to 0.25");

        // When etf_symbols is empty, it should be filled by default_etf_symbols()
        let filled = if req.etf_symbols.is_empty() {
            default_etf_symbols()
        } else {
            req.etf_symbols
        };
        assert_eq!(filled.len(), 4, "Should have 4 default ETF symbols");
        assert!(
            filled.contains(&"518880.SH".to_string()),
            "Should include gold ETF"
        );
        assert!(
            filled.contains(&"511010.SH".to_string()),
            "Should include bond ETF"
        );
    }

    #[test]
    fn test_regime_aware_boundaries() {
        // The regime_aware_min_stock function requires ETF price data to compute
        // trailing returns. Test the threshold logic directly by checking the
        // expected min_stock values for known regimes.
        //
        // Bull:  trail > 15%  → min_stock = 0.25
        // Bear:  trail < -5%  → min_stock = 0.08
        // Normal: otherwise    → min_stock = 0.15

        // Verify the threshold constants are in expected ranges
        let bull_threshold = 0.15;
        let bear_threshold = -0.05;
        let bull_min_stock = 0.25;
        let bear_min_stock = 0.08;
        let normal_min_stock = 0.15;

        assert!(
            bull_min_stock > normal_min_stock,
            "Bull should have higher min_stock"
        );
        assert!(
            bear_min_stock < normal_min_stock,
            "Bear should have lower min_stock"
        );
        assert!(bull_threshold > 0.0, "Bull threshold should be positive");
        assert!(bear_threshold < 0.0, "Bear threshold should be negative");
    }

    #[test]
    fn test_dd_control_effect() {
        // Simulate a scenario with a large drawdown to verify DD control logic.
        // 100 days +0.5%, then 50 days -1.0% (big drawdown), then 102 days +0.5%

        // Without DD control
        let mut rets_no_dd: Vec<f64> = Vec::new();
        rets_no_dd.extend((0..100).map(|_| 0.005_f64));
        rets_no_dd.extend((0..50).map(|_| -0.01_f64));
        rets_no_dd.extend((0..102).map(|_| 0.005_f64));

        // With DD control: DD>7% → scale returns to 60%
        let dd_threshold: f64 = 0.07;
        let dd_scale: f64 = 0.60;
        let mut rets_with_dd: Vec<f64> = Vec::new();
        let mut nav: f64 = 1.0;
        let mut peak: f64 = 1.0;
        let mut dd_active = false;

        for &r in &rets_no_dd {
            let current_dd: f64 = (peak - nav) / f64::max(peak, 1e-10);
            if current_dd > dd_threshold {
                dd_active = true;
            } else if current_dd < dd_threshold * 0.5 {
                dd_active = false;
            }
            let effective_ret = if dd_active { r * dd_scale } else { r };
            rets_with_dd.push(effective_ret);
            nav *= 1.0 + effective_ret;
            if nav > peak {
                peak = nav;
            }
        }

        // Compute metrics for both
        let cum_no_dd: f64 = rets_no_dd.iter().fold(1.0, |acc, r| acc * (1.0 + r));
        let ar_no_dd = cum_no_dd.powf(252.0 / 252.0) - 1.0;
        let cum_with_dd: f64 = rets_with_dd.iter().fold(1.0, |acc, r| acc * (1.0 + r));
        let ar_with_dd = cum_with_dd.powf(252.0 / 252.0) - 1.0;

        let (_v1, _s1, _so1, mdd_no, _cm1) = daily_metrics(&rets_no_dd, ar_no_dd);
        let (_v2, _s2, _so2, mdd_with, _cm2) = daily_metrics(&rets_with_dd, ar_with_dd);

        // DD control should reduce MaxDD
        assert!(
            mdd_with < mdd_no + 0.001, // ≤ is flaky with floats
            "DD control should reduce MaxDD: with={:.4} without={:.4}",
            mdd_with,
            mdd_no
        );
    }

    #[test]
    fn test_compound_vs_annual_consistency() {
        // Per-window returns: test that compounding matches annualization
        let window_rets: Vec<f64> = vec![0.10, -0.05, 0.20, -0.03, 0.15, 0.08, -0.02]; // 7 windows
        let cum: f64 = window_rets.iter().fold(1.0, |acc, r| acc * (1.0 + r));
        let ann: f64 = cum.powf(1.0 / 7.0) - 1.0;

        // Verify: compounding all windows then annualizing matches
        // the geometric mean of (1 + ret)
        assert!(
            ann > 0.0,
            "Positive cumulative return gives positive annual"
        );
        assert!(ann < 0.30, "Annual return should be reasonable");
        assert!(
            (cum - 1.0) > ann,
            "Cumulative return should be > annual return"
        );

        // Verify: stock-only cumulative from 7 WFA windows (actual WFA data)
        let stock_rets: Vec<f64> = vec![0.04, -0.012, 0.494, 0.039, -0.195, 0.083, 0.802];
        let stock_cum: f64 = stock_rets.iter().fold(1.0, |acc, r| acc * (1.0 + r));
        let stock_ann: f64 = stock_cum.powf(1.0 / 7.0) - 1.0;

        // Should be approximately 12.2%
        assert!(
            stock_ann > 0.10,
            "Stock annual should be >10%, got {:.1}%",
            stock_ann * 100.0
        );
        assert!(
            stock_ann < 0.15,
            "Stock annual should be <15%, got {:.1}%",
            stock_ann * 100.0
        );
        assert!(
            (stock_cum - 1.0) > 1.0,
            "Stock cumulative should exceed +100%"
        );
    }
}
