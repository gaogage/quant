/// 组合风控与回测报告查询路由
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::sync::Arc;

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

use ndarray::Array2;
use quant_common::mvo;
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

fn default_min_stock() -> f64 { 0.50 }
fn default_mvo_lookback() -> usize { 36 }
fn default_top_n_mvo() -> usize { 20 }

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
    let mut years: Vec<i32> = req.stock_prediction_sets.keys()
        .filter_map(|y| y.parse::<i32>().ok())
        .collect();
    years.sort();

    for year in &years {
        let pid = match req.stock_prediction_sets.get(&year.to_string()) {
            Some(p) => p.clone(),
            None => continue,
        };
        let max_pct = req.stock_max_pct.as_ref()
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

        let task_id = format!("mvo-bt-{}-{}", year, uuid::Uuid::new_v4().simple().to_string().chars().take(8).collect::<String>());

        match execute_prediction_backtest(&state.db, &task_id, bt_req).await {
            Ok(_output) => {
                // Fetch equity curve
                let eq_rows = sqlx::query_as::<_, (String, Option<f64>)>(
                    "SELECT trade_date::text, portfolio_value::double precision
                     FROM backtest_equity_curve
                     WHERE task_id = $1
                     ORDER BY trade_date"
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
    let etf_list = req.etf_symbols.iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    if etf_list.is_empty() {
        return Json(json!({"code": 1, "message": "No ETF symbols provided"}));
    }

    let etf_sql = format!(
        "SELECT symbol, trade_date::text, close::double precision
         FROM market_stock_daily_bar
         WHERE symbol IN ({})
         ORDER BY symbol, trade_date",
        etf_list
    );

    let etf_rows = sqlx::query_as::<_, (String, String, Option<f64>)>(&etf_sql)
        .fetch_all(&state.db)
        .await;

    let etf_data: std::collections::HashMap<String, std::collections::BTreeMap<String, f64>> = match etf_rows {
        Ok(rows) => {
            let mut map: std::collections::HashMap<String, std::collections::BTreeMap<String, f64>> = std::collections::HashMap::new();
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
            return Json(json!({"code": 1, "message": format!("ETF data query failed: {}", e)}));
        }
    };

    // Phase 3: Align stock + ETF daily data
    let stock_dates: std::collections::BTreeSet<String> = stock_nav.keys().cloned().collect();
    let mut common_dates: Vec<String> = Vec::new();
    for date in &stock_dates {
        let all_etfs_ok = req.etf_symbols.iter().all(|sym| {
            etf_data.get(sym).and_then(|d| d.get(date)).is_some()
        });
        if all_etfs_ok {
            common_dates.push(date.clone());
        }
    }
    common_dates.sort();

    if common_dates.len() < 60 {
        return Json(json!({"code": 1, "message": format!("Insufficient common trading days: {}", common_dates.len())}));
    }

    // Phase 4: Compute daily returns, stitching across years
    // Stock NAVs reset each year (backtest starts at 1M). Compute within-year
    // daily stock returns, then compound across years using a running scale factor.
    let n_assets = 1 + req.etf_symbols.len();

    // First, compute raw daily stock returns within each year
    let mut stock_daily_rets: Vec<f64> = Vec::with_capacity(common_dates.len());
    stock_daily_rets.push(0.0); // first day has no prior return
    for i in 1..common_dates.len() {
        let prev_date = &common_dates[i-1];
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
                etf_data.get(sym).and_then(|m| m.get(&common_dates[i-1])),
                etf_data.get(sym).and_then(|m| m.get(&common_dates[i])),
            ) {
                if *prev_p > 0.0 { curr_p / prev_p - 1.0 } else { 0.0 }
            } else { 0.0 };
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
        let stock_ret = daily_returns[idx-1][0];
        let etf_rets: Vec<f64> = (1..n_assets).map(|j| daily_returns[idx-1][j]).collect();

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
        return Json(json!({"code": 1, "message": format!("Insufficient monthly data: {} < {}", monthly_rets.len(), lookback)}));
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
        let quarter_key = format!("{}-Q{}", &date[..4],
            match &date[5..7] { "03" => 1, "06" => 2, "09" => 3, "12" => 4, _ => 0 });
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
                                if c < n_assets { data[(r, c)] = val; }
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
        let stock_ret = daily_returns[idx-1][0];
        let etf_ret_sum: f64 = (1..n_assets).zip(mvo_weights.iter().skip(1))
            .map(|(j, &w)| daily_returns[idx-1][j] * w)
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
    let variance: f64 = mvo_daily_returns.iter().map(|r| (r - mean_daily).powi(2)).sum::<f64>() / (n - 1.0);
    let std_daily = variance.sqrt();
    let ann_return = (1.0 + mean_daily).powf(252.0) - 1.0;
    let ann_vol = std_daily * (252.0_f64).sqrt();
    let sharpe = if ann_vol > 0.0 { (ann_return - 0.02) / ann_vol } else { 0.0 };

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
    let calmar = if max_dd > 0.0 { ann_return / max_dd } else { 0.0 };

    let downside: Vec<f64> = mvo_daily_returns.iter().filter(|&&r| r < 0.0).copied().collect();
    let sortino = if downside.len() > 1 {
        let ds_mean = downside.iter().sum::<f64>() / downside.len() as f64;
        let ds_var = downside.iter().map(|r| (r - ds_mean).powi(2)).sum::<f64>() / (downside.len() - 1) as f64;
        let ds_std = ds_var.sqrt() * (252.0_f64).sqrt();
        if ds_std > 0.0 { (ann_return - 0.02) / ds_std } else { 0.0 }
    } else { 0.0 };

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
