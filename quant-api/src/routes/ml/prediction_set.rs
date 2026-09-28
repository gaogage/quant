use super::*;
use chrono::NaiveDate;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use std::collections::BTreeMap;
use std::time::Duration as StdDuration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct PredictionSetCacheEconomicsInput {
    pub(crate) prediction_set_id: String,
    pub(crate) status: String,
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) metadata: Value,
    pub(crate) prediction_rows: i64,
    pub(crate) symbol_count: i64,
    pub(crate) trading_day_count: i64,
}

pub(crate) struct NormalizedLinearPredictionSetRequest {
    pub(crate) model_code: String,
    pub(crate) model_version: String,
    pub(crate) model_version_id: String,
    pub(crate) prediction_set_id: String,
    pub(crate) data_version_id: String,
    pub(crate) feature_set_version_id: String,
    pub(crate) training_dataset_id: String,
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) factors: Vec<LinearFactorWeight>,
}

#[derive(Debug, Clone)]
pub(crate) struct PredictionRow {
    pub(crate) prediction_set_id: String,
    pub(crate) symbol: String,
    pub(crate) trade_date: NaiveDate,
    pub(crate) score: f64,
    pub(crate) probability: Option<f64>,
    pub(crate) rank: i32,
    pub(crate) available_at: NaiveDate,
}

pub(crate) struct NormalizedPredictionSetEvaluationRequest {
    pub(crate) prediction_set_id: String,
    pub(crate) backtest_task_id: String,
    pub(crate) min_trade_count: i64,
    pub(crate) max_drawdown: f64,
    pub(crate) min_excess_return: f64,
}

pub(crate) async fn evaluate_prediction_set_inner(
    db: &sqlx::PgPool,
    req: EvaluatePredictionSetRequest,
) -> Result<Value, String> {
    let req = normalize_prediction_set_evaluation_request(&req)?;

    let prediction = sqlx::query_as::<
        _,
        (
            Option<i64>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<i64>,
        ),
    >(
        "SELECT COUNT(*)::bigint, MIN(trade_date), MAX(trade_date), COUNT(DISTINCT symbol)::bigint
         FROM model_prediction
         WHERE prediction_set_id = $1",
    )
    .bind(&req.prediction_set_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize model_prediction: {}", error))?;

    let backtest = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT status, prediction_set_id, error_message
         FROM backtest_task
         WHERE task_id = $1",
    )
    .bind(&req.backtest_task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load backtest_task: {}", error))?
    .ok_or_else(|| "backtest_task not found".to_string())?;
    if backtest.1.as_deref() != Some(req.prediction_set_id.as_str()) {
        return Err("backtest_task.prediction_set_id does not match request".into());
    }

    let result = sqlx::query_as::<
        _,
        (
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<i32>,
            Option<f64>,
        ),
    >(
        "SELECT total_return::double precision,
                benchmark_return::double precision,
                excess_return::double precision,
                max_drawdown::double precision,
                total_trades,
                turnover::double precision
         FROM backtest_result
         WHERE task_id = $1",
    )
    .bind(&req.backtest_task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load backtest_result: {}", error))?
    .unwrap_or((None, None, None, None, None, None));

    let target_count = sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
        "SELECT COUNT(*)::bigint, COUNT(DISTINCT symbol)::bigint
         FROM portfolio_target
         WHERE task_id = $1",
    )
    .bind(&req.backtest_task_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize portfolio_target: {}", error))?;

    let prediction_rows = prediction.0.unwrap_or(0);
    let target_rows = target_count.0.unwrap_or(0);
    let trade_count = i64::from(result.4.unwrap_or(0));
    let max_drawdown = result.3.unwrap_or(0.0);
    let excess_return = result.2.unwrap_or(0.0);
    let gates = evaluate_prediction_gates(
        trade_count,
        max_drawdown,
        excess_return,
        req.min_trade_count,
        req.max_drawdown,
        req.min_excess_return,
    );
    let status = prediction_evaluation_status(&gates);
    let metrics = json!({
        "prediction_rows": prediction_rows,
        "prediction_symbol_count": prediction.3.unwrap_or(0),
        "prediction_start_date": prediction.1,
        "prediction_end_date": prediction.2,
        "target_rows": target_rows,
        "target_symbol_count": target_count.1.unwrap_or(0),
        "trade_count": trade_count,
        "total_return": result.0,
        "benchmark_return": result.1,
        "excess_return": result.2,
        "max_drawdown": result.3,
        "turnover": result.5,
        "backtest_status": backtest.0,
        "backtest_error": backtest.2,
        "gates": gates,
    });
    let config = json!({
        "prediction_set_id": req.prediction_set_id,
        "backtest_task_id": req.backtest_task_id,
        "gate_policy": {
            "min_trade_count": req.min_trade_count,
            "max_drawdown": req.max_drawdown,
            "min_excess_return": req.min_excess_return,
        }
    });
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'ml_prediction_backtest_gate', 'prediction_set', $2, $3, $4,
                 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(&req.prediction_set_id)
    .bind(&config)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert prediction evaluation experiment_run: {}",
            error
        )
    })?;

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "prediction_set_id": req.prediction_set_id,
        "backtest_task_id": req.backtest_task_id,
        "status": status,
        "metrics": metrics,
    }))
}

pub(crate) async fn build_prediction_set_cache_economics_report(
    db: &sqlx::PgPool,
    req: &PredictionSetCacheEconomicsReportRequest,
) -> Result<Value, String> {
    let prediction_set_ids = normalize_prediction_set_cache_economics_request(req)?;
    let mut inputs = Vec::with_capacity(prediction_set_ids.len());
    for prediction_set_id in &prediction_set_ids {
        inputs.push(load_prediction_set_cache_economics_input(db, prediction_set_id).await?);
    }
    let report = prediction_set_cache_economics_report_json(&inputs);
    let experiment_run_id = if req.persist_report.unwrap_or(true) {
        Some(persist_prediction_set_cache_economics_report(db, &prediction_set_ids, &report).await?)
    } else {
        None
    };

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "report": report,
    }))
}

pub(crate) fn normalize_prediction_set_cache_economics_request(
    req: &PredictionSetCacheEconomicsReportRequest,
) -> Result<Vec<String>, String> {
    let mut ids = Vec::new();
    for id in &req.prediction_set_ids {
        let trimmed = id.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !ids.iter().any(|existing: &String| existing == trimmed) {
            ids.push(trimmed.to_string());
        }
    }
    if ids.is_empty() {
        return Err("prediction_set_ids must not be empty".into());
    }
    if ids.len() > 20 {
        return Err("prediction_set_ids supports at most 20 sets per report".into());
    }
    Ok(ids)
}

pub(crate) async fn load_prediction_set_cache_economics_input(
    db: &sqlx::PgPool,
    prediction_set_id: &str,
) -> Result<PredictionSetCacheEconomicsInput, String> {
    let row = sqlx::query_as::<_, (String, NaiveDate, NaiveDate, Option<Value>)>(
        "SELECT status, start_date, end_date, metadata
         FROM prediction_set
         WHERE prediction_set_id = $1",
    )
    .bind(prediction_set_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load prediction_set: {}", error))?
    .ok_or_else(|| format!("prediction_set not found: {}", prediction_set_id))?;

    let summary = sqlx::query_as::<_, (Option<i64>, Option<i64>, Option<i64>)>(
        "SELECT COUNT(*)::bigint,
                COUNT(DISTINCT symbol)::bigint,
                COUNT(DISTINCT trade_date)::bigint
         FROM model_prediction
         WHERE prediction_set_id = $1",
    )
    .bind(prediction_set_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize model_prediction: {}", error))?;

    Ok(PredictionSetCacheEconomicsInput {
        prediction_set_id: prediction_set_id.to_string(),
        status: row.0,
        start_date: row.1,
        end_date: row.2,
        metadata: row.3.unwrap_or_else(|| json!({})),
        prediction_rows: summary.0.unwrap_or(0),
        symbol_count: summary.1.unwrap_or(0),
        trading_day_count: summary.2.unwrap_or(0),
    })
}

pub(crate) async fn persist_prediction_set_cache_economics_report(
    db: &sqlx::PgPool,
    prediction_set_ids: &[String],
    report: &Value,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let related_entity_id = prediction_set_ids
        .first()
        .cloned()
        .unwrap_or_else(|| "prediction_set_group".to_string());
    let config = json!({
        "prediction_set_ids": prediction_set_ids,
        "report_type": "prediction_set_cache_economics",
        "point_in_time_scope": "prediction_set metadata and model_prediction only; no backtest/OOS metrics",
    });
    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'prediction_set_cache_economics_report', 'prediction_set_group', $2,
                 $3, $4, 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(&related_entity_id)
    .bind(&config)
    .bind(report)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert prediction-set cache economics experiment_run: {}",
            error
        )
    })?;
    Ok(experiment_run_id)
}

pub(crate) fn prediction_set_cache_economics_report_json(
    inputs: &[PredictionSetCacheEconomicsInput],
) -> Value {
    let sets = inputs
        .iter()
        .map(prediction_set_cache_economics_set_json)
        .collect::<Vec<_>>();
    let total_prediction_rows = inputs
        .iter()
        .map(|input| input.prediction_rows)
        .sum::<i64>();
    let missing_cache_metadata_count = sets
        .iter()
        .filter(|set| !set["cache_metadata_present"].as_bool().unwrap_or(false))
        .count();
    let missing_insert_telemetry_count = sets
        .iter()
        .filter(|set| !set["insert_telemetry_present"].as_bool().unwrap_or(false))
        .count();
    let missing_generation_telemetry_count = sets
        .iter()
        .filter(|set| {
            !set["generation_telemetry_present"]
                .as_bool()
                .unwrap_or(false)
        })
        .count();
    let max_generation_elapsed_ms = sets
        .iter()
        .filter_map(|set| {
            set["prediction_generation_telemetry"]["elapsed_ms"]
                .as_u64()
                .or_else(|| {
                    set["prediction_generation_telemetry"]["elapsed_ms"]
                        .as_i64()
                        .and_then(|value| u64::try_from(value).ok())
                })
        })
        .max()
        .unwrap_or(0);
    let max_rows_per_trading_day = inputs
        .iter()
        .map(rows_per_trading_day)
        .fold(0.0_f64, f64::max);
    let max_rows_per_symbol = inputs.iter().map(rows_per_symbol).fold(0.0_f64, f64::max);
    let recommendation = if missing_cache_metadata_count > 0 || missing_insert_telemetry_count > 0 {
        "audit_uncached_prediction_sets"
    } else if max_rows_per_trading_day >= 100_000.0 {
        "prefer_window_cache_and_background_insert"
    } else {
        "cache_metadata_complete"
    };
    json!({
        "prediction_set_count": inputs.len(),
        "total_prediction_rows": total_prediction_rows,
        "sets": sets,
        "economics": {
            "recommendation": recommendation,
            "missing_cache_metadata_count": missing_cache_metadata_count,
            "missing_insert_telemetry_count": missing_insert_telemetry_count,
            "missing_generation_telemetry_count": missing_generation_telemetry_count,
            "max_generation_elapsed_ms": max_generation_elapsed_ms,
            "max_rows_per_trading_day": max_rows_per_trading_day,
            "max_rows_per_symbol": max_rows_per_symbol,
            "point_in_time_scope": "uses prediction_set metadata and model_prediction density only; does not read backtest/OOS metrics"
        }
    })
}

pub(crate) fn prediction_set_cache_economics_set_json(
    input: &PredictionSetCacheEconomicsInput,
) -> Value {
    let feature_matrix_cache = input
        .metadata
        .get("feature_matrix_cache")
        .cloned()
        .unwrap_or(Value::Null);
    let cache_metadata_present = !feature_matrix_cache.is_null();
    let prediction_insert_telemetry = input
        .metadata
        .get("prediction_insert_telemetry")
        .cloned()
        .unwrap_or(Value::Null);
    let insert_telemetry_present = !prediction_insert_telemetry.is_null();
    let prediction_generation_telemetry = input
        .metadata
        .get("prediction_generation_telemetry")
        .cloned()
        .unwrap_or(Value::Null);
    let generation_telemetry_present = !prediction_generation_telemetry.is_null();
    json!({
        "prediction_set_id": input.prediction_set_id,
        "status": input.status,
        "start_date": input.start_date,
        "end_date": input.end_date,
        "prediction_rows": input.prediction_rows,
        "symbol_count": input.symbol_count,
        "trading_day_count": input.trading_day_count,
        "rows_per_trading_day": rows_per_trading_day(input),
        "rows_per_symbol": rows_per_symbol(input),
        "cache_metadata_present": cache_metadata_present,
        "feature_matrix_cache": feature_matrix_cache,
        "insert_telemetry_present": insert_telemetry_present,
        "prediction_insert_telemetry": prediction_insert_telemetry,
        "generation_telemetry_present": generation_telemetry_present,
        "prediction_generation_telemetry": prediction_generation_telemetry,
    })
}

pub(crate) fn rows_per_trading_day(input: &PredictionSetCacheEconomicsInput) -> f64 {
    if input.trading_day_count <= 0 {
        return 0.0;
    }
    (input.prediction_rows as f64 / input.trading_day_count as f64).round()
}

pub(crate) fn rows_per_symbol(input: &PredictionSetCacheEconomicsInput) -> f64 {
    if input.symbol_count <= 0 {
        return 0.0;
    }
    (input.prediction_rows as f64 / input.symbol_count as f64).round()
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadinessThresholds {
    pub min_day_coverage_ratio: f64,
    pub min_daily_rows: i64,
    pub min_p95_daily_row_ratio: f64,
}

impl ReadinessThresholds {
    pub(crate) fn from_options(
        min_day_coverage_ratio: Option<f64>,
        min_daily_rows: Option<i64>,
        min_p95_daily_row_ratio: Option<f64>,
    ) -> Self {
        Self {
            min_day_coverage_ratio: bounded_finite_f64(min_day_coverage_ratio, 0.98, 0.50, 1.0),
            min_daily_rows: min_daily_rows.unwrap_or(20).clamp(1, 10_000),
            min_p95_daily_row_ratio: bounded_finite_f64(min_p95_daily_row_ratio, 0.50, 0.05, 1.0),
        }
    }
}

impl Default for ReadinessThresholds {
    fn default() -> Self {
        Self::from_options(None, None, None)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DailyCountDistribution {
    pub(crate) min_rows: i64,
    pub(crate) p50_rows: i64,
    pub(crate) p95_rows: i64,
    pub(crate) max_rows: i64,
    pub(crate) weak_day_count: usize,
    pub(crate) weak_day_threshold: i64,
}

pub(crate) fn bounded_finite_f64(value: Option<f64>, default: f64, min: f64, max: f64) -> f64 {
    value
        .filter(|value| value.is_finite())
        .unwrap_or(default)
        .clamp(min, max)
}

pub(crate) fn percentile_disc_i64(sorted: &[i64], percentile: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

pub(crate) fn daily_count_distribution(
    counts: &[i64],
    thresholds: ReadinessThresholds,
) -> DailyCountDistribution {
    if counts.is_empty() {
        return DailyCountDistribution {
            min_rows: 0,
            p50_rows: 0,
            p95_rows: 0,
            max_rows: 0,
            weak_day_count: 0,
            weak_day_threshold: thresholds.min_daily_rows,
        };
    }

    let mut sorted = counts.to_vec();
    sorted.sort_unstable();
    let p95_rows = percentile_disc_i64(&sorted, 0.95);
    let weak_day_threshold = thresholds
        .min_daily_rows
        .max((p95_rows as f64 * thresholds.min_p95_daily_row_ratio).floor() as i64);
    let weak_day_count = counts
        .iter()
        .filter(|count| **count < weak_day_threshold)
        .count();

    DailyCountDistribution {
        min_rows: *sorted.first().unwrap_or(&0),
        p50_rows: percentile_disc_i64(&sorted, 0.50),
        p95_rows,
        max_rows: *sorted.last().unwrap_or(&0),
        weak_day_count,
        weak_day_threshold,
    }
}

pub(crate) fn readiness_ratio(numerator: i64, denominator: i64) -> f64 {
    if denominator <= 0 {
        return 1.0;
    }
    numerator.max(0) as f64 / denominator as f64
}

pub(crate) fn readiness_gate(
    gate: &str,
    passed: bool,
    actual: Value,
    expected: Value,
    detail: impl Into<String>,
) -> Value {
    json!({
        "gate": gate,
        "passed": passed,
        "actual": actual,
        "expected": expected,
        "detail": detail.into(),
    })
}

pub(crate) fn parse_readiness_date(
    value: Option<&str>,
    field: &str,
) -> Result<Option<NaiveDate>, String> {
    value
        .map(|raw| {
            let trimmed = raw.trim();
            NaiveDate::parse_from_str(trimmed, "%Y%m%d")
                .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y-%m-%d"))
                .map_err(|_| format!("{} must use YYYYMMDD or YYYY-MM-DD format", field))
        })
        .transpose()
}

pub(crate) async fn readiness_expected_open_day_count(
    db: &sqlx::PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<i64, String> {
    let calendar_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT trade_date)::int8
         FROM market_trade_calendar
         WHERE trade_date >= $1 AND trade_date <= $2 AND is_open = true",
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to count market_trade_calendar days: {}", error))?;
    if calendar_count > 0 {
        return Ok(calendar_count);
    }

    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT trade_date)::int8
         FROM market_index_daily_bar
         WHERE symbol='000300.SH' AND trade_date >= $1 AND trade_date <= $2",
    )
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to count index trading days: {}", error))
}

pub(crate) async fn build_prediction_set_readiness_report_from_request(
    db: &sqlx::PgPool,
    req: &PredictionSetReadinessRequest,
) -> Result<Value, String> {
    let start = parse_readiness_date(req.start_date.as_deref(), "start_date")?;
    let end = parse_readiness_date(req.end_date.as_deref(), "end_date")?;
    let thresholds = ReadinessThresholds::from_options(
        req.min_day_coverage_ratio,
        req.min_daily_rows,
        req.min_p95_daily_row_ratio,
    );
    let report =
        build_prediction_set_readiness_report(db, &req.prediction_set_id, start, end, thresholds)
            .await?;
    let experiment_run_id = if req.persist_report.unwrap_or(true) {
        Some(persist_prediction_set_readiness_report(db, &req.prediction_set_id, &report).await?)
    } else {
        None
    };
    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "report": report,
    }))
}

pub(crate) async fn build_prediction_set_readiness_report(
    db: &sqlx::PgPool,
    prediction_set_id: &str,
    start_override: Option<NaiveDate>,
    end_override: Option<NaiveDate>,
    thresholds: ReadinessThresholds,
) -> Result<Value, String> {
    let prediction_set_id = prediction_set_id.trim();
    if prediction_set_id.is_empty() {
        return Err("prediction_set_id must not be empty".into());
    }
    let set_row = sqlx::query_as::<
        _,
        (
            String,
            NaiveDate,
            NaiveDate,
            Option<NaiveDate>,
            String,
            Option<Value>,
        ),
    >(
        "SELECT status, start_date, end_date, training_end_date, feature_set_version_id, metadata
         FROM prediction_set
         WHERE prediction_set_id = $1",
    )
    .bind(prediction_set_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load prediction_set readiness row: {}", error))?
    .ok_or_else(|| format!("prediction_set not found: {}", prediction_set_id))?;

    let start = start_override.unwrap_or(set_row.1);
    let end = end_override.unwrap_or(set_row.2);
    if start > end {
        return Err("prediction-set readiness start_date cannot be after end_date".into());
    }

    let daily_rows = sqlx::query_as::<_, (NaiveDate, i64, i64, i64)>(
        "SELECT trade_date,
                COUNT(*)::int8 AS rows,
                COUNT(DISTINCT symbol)::int8 AS symbols,
                COUNT(*) FILTER (WHERE available_at > trade_date)::int8 AS future_leak_rows
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2 AND trade_date <= $3
         GROUP BY trade_date
         ORDER BY trade_date",
    )
    .bind(prediction_set_id)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to summarize model_prediction readiness: {}", error))?;

    let expected_days = readiness_expected_open_day_count(db, start, end).await?;
    let actual_days = daily_rows.len() as i64;
    let prediction_rows = daily_rows.iter().map(|(_, rows, _, _)| *rows).sum::<i64>();
    let symbol_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT symbol)::int8
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2 AND trade_date <= $3",
    )
    .bind(prediction_set_id)
    .bind(start)
    .bind(end)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to count model_prediction symbols: {}", error))?;
    let future_leak_rows = daily_rows
        .iter()
        .map(|(_, _, _, future_leak)| *future_leak)
        .sum::<i64>();
    let counts = daily_rows
        .iter()
        .map(|(_, rows, _, _)| *rows)
        .collect::<Vec<_>>();
    let distribution = daily_count_distribution(&counts, thresholds);
    let day_coverage_ratio = readiness_ratio(actual_days, expected_days);
    let set_covers_requested_window = set_row.1 <= start && set_row.2 >= end;
    let training_end_pit_ok = set_row.3.map(|date| date < start).unwrap_or(true);
    let gates = vec![
        readiness_gate(
            "prediction_set_status_ready",
            set_row.0 == "ready",
            json!(set_row.0),
            json!("ready"),
            "prediction_set.status must be ready before use",
        ),
        readiness_gate(
            "prediction_set_date_range",
            set_covers_requested_window,
            json!({"set_start": set_row.1, "set_end": set_row.2, "requested_start": start, "requested_end": end}),
            json!("set_start <= requested_start and set_end >= requested_end"),
            "prediction_set declared date range must cover the requested evaluation window",
        ),
        readiness_gate(
            "training_end_before_requested_start",
            training_end_pit_ok,
            json!(set_row.3),
            json!(format!("< {}", start)),
            "when prediction_set.training_end_date is present it must be before the requested prediction window",
        ),
        readiness_gate(
            "prediction_day_coverage",
            day_coverage_ratio >= thresholds.min_day_coverage_ratio,
            json!(day_coverage_ratio),
            json!(thresholds.min_day_coverage_ratio),
            "model_prediction must cover nearly all expected open days in the requested window",
        ),
        readiness_gate(
            "prediction_future_leak_rows",
            future_leak_rows == 0,
            json!(future_leak_rows),
            json!(0),
            "model_prediction.available_at must not be after trade_date",
        ),
        readiness_gate(
            "prediction_daily_median_rows",
            distribution.p50_rows >= thresholds.min_daily_rows,
            json!(distribution.p50_rows),
            json!(thresholds.min_daily_rows),
            "daily cross-section median must be large enough for meaningful ranking",
        ),
        readiness_gate(
            "prediction_daily_row_cliff",
            distribution.weak_day_count == 0,
            json!({
                "weak_day_count": distribution.weak_day_count,
                "weak_day_threshold": distribution.weak_day_threshold,
                "p95_rows": distribution.p95_rows,
            }),
            json!("weak_day_count = 0"),
            "daily row counts must not collapse relative to the set's own p95 cross-section",
        ),
    ];
    let passed = gates
        .iter()
        .all(|gate| gate["passed"].as_bool().unwrap_or(false));

    Ok(json!({
        "readiness_type": "prediction_set",
        "prediction_set_id": prediction_set_id,
        "feature_set_version_id": set_row.4,
        "status": set_row.0,
        "set_start_date": set_row.1,
        "set_end_date": set_row.2,
        "training_end_date": set_row.3,
        "requested_start_date": start,
        "requested_end_date": end,
        "passed": passed,
        "level": if passed { "green" } else { "red" },
        "thresholds": {
            "min_day_coverage_ratio": thresholds.min_day_coverage_ratio,
            "min_daily_rows": thresholds.min_daily_rows,
            "min_p95_daily_row_ratio": thresholds.min_p95_daily_row_ratio,
        },
        "summary": {
            "expected_open_days": expected_days,
            "actual_prediction_days": actual_days,
            "missing_open_days": (expected_days - actual_days).max(0),
            "day_coverage_ratio": day_coverage_ratio,
            "prediction_rows": prediction_rows,
            "symbol_count": symbol_count,
            "future_leak_rows": future_leak_rows,
            "daily_rows": {
                "min": distribution.min_rows,
                "p50": distribution.p50_rows,
                "p95": distribution.p95_rows,
                "max": distribution.max_rows,
                "weak_day_count": distribution.weak_day_count,
                "weak_day_threshold": distribution.weak_day_threshold,
            },
        },
        "gates": gates,
        "metadata": set_row.5.unwrap_or_else(|| json!({})),
        "repair": {
            "repairable": false,
            "reason": "prediction sets must be rebuilt through the PIT training/prediction pipeline; point fixes to model_prediction rows are not safe"
        }
    }))
}

pub(crate) fn prediction_readiness_passed(report: &Value) -> bool {
    report
        .get("passed")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

pub(crate) async fn persist_prediction_set_readiness_report(
    db: &sqlx::PgPool,
    prediction_set_id: &str,
    report: &Value,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let config = json!({
        "prediction_set_id": prediction_set_id,
        "report_type": "prediction_set_readiness",
        "point_in_time_scope": "prediction_set metadata and model_prediction only; no backtest/OOS metrics",
    });
    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'prediction_set_readiness_report', 'prediction_set', $2,
                 $3, $4, 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(prediction_set_id)
    .bind(&config)
    .bind(report)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to persist prediction-set readiness report: {}",
            error
        )
    })?;
    Ok(experiment_run_id)
}

pub(crate) fn normalize_prediction_set_evaluation_request(
    req: &EvaluatePredictionSetRequest,
) -> Result<NormalizedPredictionSetEvaluationRequest, String> {
    let prediction_set_id = req.prediction_set_id.trim().to_string();
    let backtest_task_id = req.backtest_task_id.trim().to_string();
    if prediction_set_id.is_empty() || backtest_task_id.is_empty() {
        return Err("prediction_set_id/backtest_task_id must not be empty".into());
    }
    let min_trade_count = req.min_trade_count.unwrap_or(1);
    if min_trade_count < 0 {
        return Err("min_trade_count must be non-negative".into());
    }
    let max_drawdown = req.max_drawdown.unwrap_or(0.20);
    if !max_drawdown.is_finite() || max_drawdown < 0.0 {
        return Err("max_drawdown must be a non-negative finite number".into());
    }
    let min_excess_return = req.min_excess_return.unwrap_or(0.0);
    if !min_excess_return.is_finite() {
        return Err("min_excess_return must be finite".into());
    }

    Ok(NormalizedPredictionSetEvaluationRequest {
        prediction_set_id,
        backtest_task_id,
        min_trade_count,
        max_drawdown,
        min_excess_return,
    })
}

pub(crate) fn evaluate_prediction_gates(
    trade_count: i64,
    max_drawdown: f64,
    excess_return: f64,
    min_trade_count: i64,
    max_drawdown_limit: f64,
    min_excess_return: f64,
) -> Value {
    json!([
        {
            "gate": "min_trade_count",
            "passed": trade_count >= min_trade_count,
            "limit": min_trade_count,
            "actual": trade_count
        },
        {
            "gate": "max_drawdown",
            "passed": max_drawdown <= max_drawdown_limit,
            "limit": max_drawdown_limit,
            "actual": max_drawdown
        },
        {
            "gate": "min_excess_return",
            "passed": excess_return >= min_excess_return,
            "limit": min_excess_return,
            "actual": excess_return
        }
    ])
}

pub(crate) fn prediction_evaluation_status(gates: &Value) -> &'static str {
    let passed = gates
        .as_array()
        .map(|items| {
            items
                .iter()
                .all(|item| item.get("passed").and_then(Value::as_bool).unwrap_or(false))
        })
        .unwrap_or(false);
    if passed {
        "approved_candidate"
    } else {
        "review_required"
    }
}

pub(crate) async fn create_linear_prediction_set_inner(
    db: &sqlx::PgPool,
    req: LinearPredictionSetRequest,
) -> Result<Value, String> {
    let req = normalize_linear_prediction_request(&req)?;
    ensure_data_version_exists(db, &req.data_version_id).await?;
    let metadata = json!({
        "model_type": "linear_factor_smoke",
        "factors": req.factors,
        "point_in_time_policy": "model_prediction.available_at = trade_date",
    });
    let artifact_hash = stable_metadata_hash(&metadata);
    let prediction_hash = stable_metadata_hash(&json!({
        "model_version_id": req.model_version_id,
        "prediction_set_id": req.prediction_set_id,
        "data_version_id": req.data_version_id,
        "feature_set_version_id": req.feature_set_version_id,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "factors": metadata["factors"],
    }));

    let rows = build_linear_prediction_rows(db, &req).await?;
    if rows.is_empty() {
        return Err("linear prediction smoke found no factor values".into());
    }

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin ML prediction transaction: {}", error))?;

    sqlx::query(
        "INSERT INTO training_dataset
           (training_dataset_id, data_version_id, feature_set_version_id, label_definition,
            train_window, validation_window, test_window, sample_filter, split_policy,
            dataset_hash, status, metadata)
         VALUES ($1, $2, $3, '{}'::jsonb, $4, $5, $6, '{}'::jsonb, $7, $8, 'frozen', $9)
         ON CONFLICT (training_dataset_id) DO UPDATE SET
            data_version_id = EXCLUDED.data_version_id,
            feature_set_version_id = EXCLUDED.feature_set_version_id,
            train_window = EXCLUDED.train_window,
            validation_window = EXCLUDED.validation_window,
            test_window = EXCLUDED.test_window,
            split_policy = EXCLUDED.split_policy,
            dataset_hash = EXCLUDED.dataset_hash,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata",
    )
    .bind(&req.training_dataset_id)
    .bind(&req.data_version_id)
    .bind(&req.feature_set_version_id)
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"type": "phase5a_smoke_same_window"}))
    .bind(stable_metadata_hash(&metadata))
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert training_dataset: {}", error))?;

    sqlx::query(
        "INSERT INTO model_registry
           (model_version_id, model_code, model_type, version, feature_version_id,
            label_definition, training_window, validation_metrics, test_metrics,
            artifact_path, artifact_hash, status, training_dataset_id)
         VALUES ($1, $2, 'linear_factor_smoke', $3, $4, '{}'::jsonb, $5,
                 $6, $7, $8, $9, 'active', $10)
         ON CONFLICT (model_version_id) DO UPDATE SET
            feature_version_id = EXCLUDED.feature_version_id,
            validation_metrics = EXCLUDED.validation_metrics,
            test_metrics = EXCLUDED.test_metrics,
            artifact_hash = EXCLUDED.artifact_hash,
            status = EXCLUDED.status,
            training_dataset_id = EXCLUDED.training_dataset_id",
    )
    .bind(&req.model_version_id)
    .bind(&req.model_code)
    .bind(&req.model_version)
    .bind(&req.feature_set_version_id)
    .bind(json!({"start": req.start_date, "end": req.end_date}))
    .bind(json!({"row_count": rows.len()}))
    .bind(json!({"row_count": rows.len()}))
    .bind(format!("artifact://{}", req.model_version_id))
    .bind(&artifact_hash)
    .bind(&req.training_dataset_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert model_registry: {}", error))?;

    sqlx::query(
        "INSERT INTO prediction_set
           (prediction_set_id, model_version_id, feature_set_version_id, data_version_id,
            start_date, end_date, prediction_hash, status, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'ready', $8)
         ON CONFLICT (prediction_set_id) DO UPDATE SET
            model_version_id = EXCLUDED.model_version_id,
            feature_set_version_id = EXCLUDED.feature_set_version_id,
            data_version_id = EXCLUDED.data_version_id,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            prediction_hash = EXCLUDED.prediction_hash,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata",
    )
    .bind(&req.prediction_set_id)
    .bind(&req.model_version_id)
    .bind(&req.feature_set_version_id)
    .bind(&req.data_version_id)
    .bind(req.start_date)
    .bind(req.end_date)
    .bind(&prediction_hash)
    .bind(&metadata)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to upsert prediction_set: {}", error))?;

    sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
        .bind(&req.prediction_set_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to clear model_prediction: {}", error))?;

    for row in &rows {
        sqlx::query(
            "INSERT INTO model_prediction
               (prediction_set_id, trade_date, symbol, score, probability, rank, available_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&row.prediction_set_id)
        .bind(row.trade_date)
        .bind(&row.symbol)
        .bind(row.score)
        .bind(row.probability)
        .bind(row.rank)
        .bind(row.available_at)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to insert model_prediction: {}", error))?;
    }

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit ML prediction transaction: {}", error))?;

    Ok(json!({
        "model_version_id": req.model_version_id,
        "training_dataset_id": req.training_dataset_id,
        "prediction_set_id": req.prediction_set_id,
        "prediction_hash": prediction_hash,
        "inserted": rows.len(),
        "min_trade_date": rows.iter().map(|row| row.trade_date).min(),
        "max_trade_date": rows.iter().map(|row| row.trade_date).max(),
        "point_in_time": "available_at = trade_date",
    }))
}

pub(crate) fn normalize_linear_prediction_request(
    req: &LinearPredictionSetRequest,
) -> Result<NormalizedLinearPredictionSetRequest, String> {
    if req.factors.is_empty() {
        return Err("factors must not be empty".into());
    }
    let model_code = req.model_code.trim().to_string();
    let model_version = req.model_version.trim().to_string();
    let data_version_id = req.data_version_id.trim().to_string();
    let feature_set_version_id = req.feature_set_version_id.trim().to_string();
    let training_dataset_id = req.training_dataset_id.trim().to_string();
    if model_code.is_empty()
        || model_version.is_empty()
        || data_version_id.is_empty()
        || feature_set_version_id.is_empty()
        || training_dataset_id.is_empty()
    {
        return Err("model_code/model_version/data_version_id/feature_set_version_id/training_dataset_id must not be empty".into());
    }
    let start_date = parse_yyyymmdd(&req.start_date, "start_date")?;
    let end_date = parse_yyyymmdd(&req.end_date, "end_date")?;
    if end_date < start_date {
        return Err("end_date must be greater than or equal to start_date".into());
    }

    Ok(NormalizedLinearPredictionSetRequest {
        model_version_id: req
            .model_version_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}@{}", model_code, model_version)),
        prediction_set_id: req
            .prediction_set_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                format!(
                    "pred-{}-{}-{}-{}",
                    model_code,
                    model_version,
                    req.start_date.trim(),
                    req.end_date.trim()
                )
            }),
        model_code,
        model_version,
        data_version_id,
        feature_set_version_id,
        training_dataset_id,
        start_date,
        end_date,
        factors: req.factors.clone(),
    })
}

pub(crate) fn missing_data_version_error_message(data_version_id: &str) -> String {
    format!(
        "data_version_id '{}' does not exist in data_version; run data readiness/sync first or use an existing canonical data_version_id",
        data_version_id
    )
}

pub(crate) async fn ensure_data_version_exists(
    db: &sqlx::PgPool,
    data_version_id: &str,
) -> Result<(), String> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM data_version WHERE data_version_id = $1)")
            .bind(data_version_id)
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to check data_version: {}", error))?;
    if !exists {
        return Err(missing_data_version_error_message(data_version_id));
    }
    Ok(())
}

pub(crate) async fn build_linear_prediction_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearPredictionSetRequest,
) -> Result<Vec<PredictionRow>, String> {
    let feature_rows = load_prediction_feature_matrix_rows(db, req).await?;
    let weights = req
        .factors
        .iter()
        .map(|factor| factor.weight)
        .collect::<Vec<_>>();

    prediction_rows_from_feature_matrix_rows(
        &req.prediction_set_id,
        feature_rows,
        &weights,
        req.factors.len(),
    )
}

pub(crate) async fn load_prediction_feature_matrix_rows(
    db: &sqlx::PgPool,
    req: &NormalizedLinearPredictionSetRequest,
) -> Result<Vec<TrainingFeatureMatrixRow>, String> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "WITH requested(factor_code, factor_version, factor_idx) AS (",
    );
    builder.push_values(
        req.factors.iter().enumerate(),
        |mut row, (factor_idx, factor)| {
            row.push_bind(&factor.factor_code)
                .push_bind(&factor.factor_version)
                .push_bind(factor_idx as i32);
        },
    );
    builder.push(
        ")
         SELECT fv.symbol,
                fv.trade_date,
                array_agg(fv.normalized_value::double precision ORDER BY requested.factor_idx)::double precision[] AS features
         FROM factor_value fv
         JOIN requested
           ON requested.factor_code = fv.factor_code
          AND requested.factor_version = fv.factor_version
         WHERE fv.trade_date >= ",
    );
    builder.push_bind(req.start_date);
    builder.push(
        "
           AND fv.trade_date <= ",
    );
    builder.push_bind(req.end_date);
    builder.push(
        "
           AND (fv.available_at IS NULL OR fv.available_at <= fv.trade_date)
         GROUP BY fv.symbol, fv.trade_date
         HAVING COUNT(*) = ",
    );
    builder.push_bind(req.factors.len() as i64);
    builder.push(
        "
            AND bool_and(fv.normalized_value IS NOT NULL)
         ORDER BY fv.trade_date, fv.symbol",
    );

    let rows = builder
        .build_query_as::<(String, NaiveDate, Vec<f64>)>()
        .fetch_all(db)
        .await
        .map_err(|error| format!("Failed to load prediction factor matrix: {}", error))?;

    Ok(rows
        .into_iter()
        .map(|(symbol, trade_date, features)| TrainingFeatureMatrixRow {
            symbol,
            trade_date,
            features,
        })
        .collect())
}

pub(crate) fn prediction_rows_from_feature_matrix_rows(
    prediction_set_id: &str,
    feature_rows: Vec<TrainingFeatureMatrixRow>,
    weights: &[f64],
    factor_count: usize,
) -> Result<Vec<PredictionRow>, String> {
    let mut by_date: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    if weights.len() != factor_count {
        return Err("prediction weights count must match factor count".into());
    }
    for row in feature_rows {
        if row.features.len() != factor_count || row.features.iter().any(|value| !value.is_finite())
        {
            continue;
        }
        let score = row
            .features
            .iter()
            .zip(weights.iter())
            .map(|(feature, weight)| feature * weight)
            .sum::<f64>();
        if !score.is_finite() {
            continue;
        }
        by_date
            .entry(row.trade_date)
            .or_default()
            .push((row.symbol, score));
    }

    let mut predictions = Vec::new();
    for (trade_date, mut rows) in by_date {
        rows.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        for (idx, (symbol, score)) in rows.into_iter().enumerate() {
            predictions.push(build_prediction_row(
                prediction_set_id,
                &symbol,
                &trade_date.to_string(),
                score,
                (idx + 1) as i32,
            )?);
        }
    }

    Ok(predictions)
}

pub(crate) fn build_prediction_row(
    prediction_set_id: &str,
    symbol: &str,
    trade_date: &str,
    score: f64,
    rank: i32,
) -> Result<PredictionRow, String> {
    let trade_date = NaiveDate::parse_from_str(trade_date, "%Y-%m-%d")
        .map_err(|_| "trade_date must use YYYY-MM-DD format".to_string())?;
    Ok(PredictionRow {
        prediction_set_id: prediction_set_id.to_string(),
        symbol: symbol.to_string(),
        trade_date,
        score,
        probability: Some(1.0 / (1.0 + (-score).exp())),
        rank,
        available_at: trade_date,
    })
}

pub(crate) async fn insert_prediction_rows(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[PredictionRow],
) -> Result<(), String> {
    for chunk in rows.chunks(prediction_insert_batch_size()) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO model_prediction
               (prediction_set_id, trade_date, symbol, score, probability, rank, available_at) ",
        );
        builder.push_values(chunk, |mut row_builder, row| {
            row_builder
                .push_bind(&row.prediction_set_id)
                .push_bind(row.trade_date)
                .push_bind(&row.symbol)
                .push_bind(row.score)
                .push_bind(row.probability)
                .push_bind(row.rank)
                .push_bind(row.available_at);
        });
        builder
            .build()
            .execute(&mut **tx)
            .await
            .map_err(|error| format!("Failed to insert model_prediction rows: {}", error))?;
    }
    Ok(())
}

/// model_prediction 批量插入分批行数。OnceLock 缓存——插入分批与遥测口径必须同值。
pub(crate) fn prediction_insert_batch_size() -> usize {
    // 任务80: C类特许 → env 化（默认=原写死值）
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("ML_PREDICTION_INSERT_BATCH")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(5_000)
    })
}

pub(crate) fn prediction_insert_telemetry(rows: &[PredictionRow]) -> Value {
    let chunk_row_counts = rows
        .chunks(prediction_insert_batch_size())
        .map(|chunk| chunk.len())
        .collect::<Vec<_>>();
    json!({
        "mode": "bulk_insert",
        "row_count": rows.len(),
        "batch_size": prediction_insert_batch_size(),
        "batch_count": chunk_row_counts.len(),
        "chunk_row_counts": chunk_row_counts,
    })
}

pub(crate) fn prediction_progress_stage(
    stage: &str,
    total_units: usize,
    completed_units: usize,
    row_count: usize,
    elapsed: StdDuration,
) -> Value {
    json!({
        "stage": stage,
        "total_units": total_units,
        "completed_units": completed_units,
        "row_count": row_count,
        "elapsed_ms": elapsed.as_millis() as u64,
        "progress_pct": progress_pct(total_units, completed_units),
    })
}

pub(crate) fn prediction_generation_telemetry(
    operation: &str,
    total_units: usize,
    completed_units: usize,
    skipped_units: usize,
    prediction_rows: usize,
    elapsed: StdDuration,
    stages: Vec<Value>,
) -> Value {
    json!({
        "operation": operation,
        "total_units": total_units,
        "completed_units": completed_units,
        "skipped_units": skipped_units,
        "prediction_rows": prediction_rows,
        "elapsed_ms": elapsed.as_millis() as u64,
        "progress_pct": progress_pct(total_units, completed_units + skipped_units),
        "stages": stages,
    })
}

pub(crate) fn progress_pct(total_units: usize, completed_units: usize) -> f64 {
    if total_units == 0 {
        return 100.0;
    }
    ((completed_units.min(total_units) as f64 / total_units as f64) * 10_000.0).round() / 100.0
}

pub(crate) fn feature_matrix_cache_metadata(
    scope: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    row_count: usize,
) -> Value {
    json!({
        "scope": scope,
        "start_date": start_date,
        "end_date": end_date,
        "row_count": row_count,
    })
}

pub(crate) fn stable_metadata_hash(value: &Value) -> String {
    let canonical = canonical_json(value);
    let mut hash = 14695981039346656037u64;
    for byte in canonical.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    format!("hash-{:016x}", hash)
}

pub(crate) fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let body = entries
                .into_iter()
                .map(|(key, value)| format!("\"{}\":{}", key, canonical_json(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{}}}", body)
        }
        Value::Array(values) => {
            let body = values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{}]", body)
        }
        _ => value.to_string(),
    }
}

pub(crate) fn parse_yyyymmdd(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .map_err(|_| format!("{} must use YYYYMMDD format", field))
}
