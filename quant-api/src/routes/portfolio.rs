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
