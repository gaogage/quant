//! Auto-extracted from routes/optimization.rs (DDD Step 3b).
//! Do not edit by hand; logic is verbatim from the original module.
#![allow(unused_imports)]
#![allow(dead_code)]

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Duration, NaiveDate};
use quant_api::discovery::strategy_discovery::{
    build_layered_search_plan, CandidateMetrics, CandidateTargets, CandidateType,
    LayeredSearchConfig, LayeredSearchPlan, LocalResourcePlan,
};
// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, QueryBuilder};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tracing::error;
use uuid::Uuid;

use crate::phase7_alpha_admission::{
    validate_analyst_revision_entrypoint_admission, validate_analyst_revision_trial_admission,
    validate_equity_pledge_entrypoint_admission, validate_equity_pledge_trial_admission,
    validate_futures_price_chain_entrypoint_admission,
    validate_industry_prosperity_entrypoint_admission,
    validate_industry_prosperity_trial_admission, validate_margin_detail_entrypoint_admission,
    validate_margin_detail_trial_admission, validate_shareholder_structure_entrypoint_admission,
    validate_shareholder_structure_trial_admission, FUTURES_PRICE_CHAIN_COVERAGE_GATE_ID,
    INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE, MARGIN_DETAIL_COVERAGE_GATE_ID,
};
use crate::routes::backtest::{
    execute_factor_backtest_with_caches, execute_prediction_backtest,
    prewarm_factor_signal_cache_for_requests, CostModelReq, EffectiveCoverageReq,
    ExecutionRulesReq, FactorBacktestRunOutput, MarketRegimeBacktestReq, RunFactorBacktestReq,
    RunPredictionBacktestReq,
};
use crate::routes::ml::{
    build_prediction_set_readiness_report, create_walk_forward_nonlinear_quantile_ranker_inner,
    daily_count_distribution, prediction_readiness_passed, readiness_expected_open_day_count,
    train_nonlinear_quantile_ranker_inner, DailyCountDistribution, LinearFactorRef,
    ReadinessThresholds, TrainNonlinearQuantileRankerRequest,
    WalkForwardNonlinearQuantileRankerRequest,
};
use crate::AppState;
use quant_backtest::runner::{
    BacktestDataCache, BacktestDataCacheSnapshot, BacktestDataCacheStats,
};
use quant_backtest::signal_generator::{
    compare_return_risk_cache_economics, signal_cache_stats_delta, SignalDataCache,
    SignalDataCacheSnapshot, SignalDataCacheStats,
};

use super::*;

pub(crate) struct NormalizedPromoteRequest {
    pub(crate) trial_id: String,
    pub(crate) target_strategy_version: String,
    pub(crate) candidate_name: String,
    pub(crate) promotion_mode: String,
    pub(crate) gate_policy: String,
    pub(crate) freeze_after_approval: bool,
    pub(crate) reviewer: Option<String>,
    pub(crate) reason: String,
    pub(crate) notes: Option<String>,
    pub(crate) status: String,
}

pub(crate) struct Phase7LayeredPlanBundle {
    pub(crate) resource_plan: LocalResourcePlan,
    pub(crate) plan: LayeredSearchPlan,
    pub(crate) search_space: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct CompletedTrialSnapshot {
    pub(crate) trial_id: String,
    pub(crate) trial_index: i32,
    pub(crate) backtest_task_id: Option<String>,
    pub(crate) score: Option<Decimal>,
    pub(crate) metrics: Value,
    pub(crate) metric_sources: Value,
    pub(crate) missing_elite_metrics: Value,
    pub(crate) constraint_violations: Value,
    pub(crate) parameters: Value,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EliteMetricProfile {
    annual_return: f64,
    excess_return: f64,
    sharpe: f64,
    sortino: f64,
    calmar: f64,
    profit_factor: f64,
    max_drawdown: f64,
    max_drawdown_duration_days: f64,
    num_trades: f64,
}

pub(crate) fn cleanup_dry_run_default(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}

pub(crate) fn cleanup_default_timeout_seconds(value: Option<i64>) -> i64 {
    value.unwrap_or(3600).clamp(60, 86_400)
}

pub(crate) fn cleanup_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 1000)
}

pub(crate) fn optimization_cleanup_task_status_transition(status: &str) -> Option<&'static str> {
    match status {
        "pending" => Some("cancelled"),
        "running" | "cancel_requested" => Some("timeout"),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn optimization_cleanup_trial_status_transition(status: &str) -> Option<&'static str> {
    match status {
        "pending" => Some("cancelled"),
        "running" | "cancel_requested" => Some("timeout"),
        _ => None,
    }
}

pub(crate) fn experiment_cleanup_status_transition(status: &str) -> Option<&'static str> {
    match status {
        "pending" | "running" => Some("failed"),
        _ => None,
    }
}

pub async fn create_optimization(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateOptimizationRequest>,
) -> impl IntoResponse {
    if req.search_method != "random_search" && req.search_method != "grid_search" {
        return Json(
            json!({"code": 1, "message": format!("unsupported search_method: {}", req.search_method)}),
        );
    }

    let max_trials = normalize_max_trials(req.max_trials);
    let trial_params =
        match generate_trial_parameters(&req.search_space, req.random_seed, max_trials) {
            Ok(params) => params,
            Err(message) => return Json(json!({"code": 1, "message": message})),
        };

    let task_id = format!("opt-{}", Uuid::new_v4());
    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to begin optimization transaction: {}", error)}),
            )
        }
    };

    let insert_task = sqlx::query(
        "INSERT INTO optimization_task
           (optimization_task_id, strategy_version_id, data_version_id, search_method,
            search_space, objective, constraints, walk_forward_config, backtest_template, status, progress)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', 0)",
    )
    .bind(&task_id)
    .bind(&req.strategy_version_id)
    .bind(&req.data_version_id)
    .bind(&req.search_method)
    .bind(&req.search_space)
    .bind(&req.objective)
    .bind(&req.constraints)
    .bind(&req.walk_forward)
    .bind(&req.backtest_template)
    .execute(&mut *tx)
    .await;

    if let Err(error) = insert_task {
        return Json(
            json!({"code": 1, "message": format!("Failed to create optimization task: {}", error)}),
        );
    }

    for (idx, params) in trial_params.iter().enumerate() {
        let trial_id = format!("trial-{}-{:04}", task_id, idx + 1);
        if let Err(error) = sqlx::query(
            "INSERT INTO optimization_trial
               (trial_id, optimization_task_id, trial_index, parameters, status, progress)
             VALUES ($1, $2, $3, $4, 'pending', 0)",
        )
        .bind(&trial_id)
        .bind(&task_id)
        .bind((idx + 1) as i32)
        .bind(params)
        .execute(&mut *tx)
        .await
        {
            return Json(
                json!({"code": 1, "message": format!("Failed to create optimization trial: {}", error)}),
            );
        }
    }

    if let Err(error) = tx.commit().await {
        return Json(
            json!({"code": 1, "message": format!("Failed to commit optimization task: {}", error)}),
        );
    }

    Json(json!({
        "code": 0,
        "data": {
            "optimization_task_id": task_id,
            "status": "pending",
            "max_trials": max_trials,
            "trial_count": trial_params.len(),
        }
    }))
}

pub async fn get_optimization(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let row = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            Value,
            Value,
            Option<Value>,
            Option<Value>,
            Option<Value>,
            String,
            Option<String>,
            i32,
            Option<chrono::DateTime<chrono::Utc>>,
        ),
    >(
        "SELECT optimization_task_id, strategy_version_id, data_version_id, search_method,
           search_space, objective, constraints, walk_forward_config, backtest_template, status,
           best_trial_id, progress, created_at
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await;

    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => return Json(json!({"code": 1, "message": "optimization task not found"})),
        Err(error) => {
            return Json(
                json!({"code": 1, "message": format!("Failed to get optimization task: {}", error)}),
            )
        }
    };

    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
           COUNT(*) FILTER (WHERE status = 'completed')::bigint,
           COUNT(*) FILTER (WHERE status = 'failed')::bigint
         FROM optimization_trial
         WHERE optimization_task_id = $1",
    )
    .bind(&task_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or((0, 0, 0));

    Json(json!({
        "code": 0,
        "data": {
            "optimization_task_id": row.0,
            "strategy_version_id": row.1,
            "data_version_id": row.2,
            "search_method": row.3,
            "search_space": row.4,
            "objective": row.5,
            "constraints": row.6,
            "walk_forward_config": row.7,
            "backtest_template": row.8,
            "status": row.9,
            "best_trial_id": row.10,
            "progress": row.11,
            "created_at": fmt_rfc3339_local(row.12),
            "trial_total": counts.0,
            "trial_completed": counts.1,
            "trial_failed": counts.2,
        }
    }))
}

pub async fn run_phase7_professional_discovery(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ProfessionalDiscoveryRequest>,
) -> impl IntoResponse {
    match execute_phase7_professional_discovery(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub(crate) fn missing_strategy_version_error_message(strategy_version_id: &str) -> String {
    format!(
        "strategy_version_id '{}' does not exist in strategy_version; use an existing canonical strategy_version_id or register the strategy version before running optimization",
        strategy_version_id
    )
}

pub(crate) fn missing_optimization_data_version_error_message(data_version_id: &str) -> String {
    format!(
        "data_version_id '{}' does not exist in data_version; run data readiness/sync first or use an existing canonical data_version_id",
        data_version_id
    )
}

pub(crate) async fn ensure_phase7_oos_request_references_exist(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Result<(), String> {
    let strategy_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM strategy_version WHERE strategy_version_id = $1)",
    )
    .bind(&req.strategy_version_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to check strategy_version: {}", error))?;
    if !strategy_exists {
        return Err(missing_strategy_version_error_message(
            &req.strategy_version_id,
        ));
    }

    let data_version_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM data_version WHERE data_version_id = $1)")
            .bind(&req.data_version_id)
            .fetch_one(db)
            .await
            .map_err(|error| format!("Failed to check data_version: {}", error))?;
    if !data_version_exists {
        return Err(missing_optimization_data_version_error_message(
            &req.data_version_id,
        ));
    }

    Ok(())
}

pub async fn cleanup_stale_optimization_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleBackgroundTasksReq>,
) -> impl IntoResponse {
    match cleanup_stale_optimization_tasks_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

/// POST /api/v1/quant/experiments/cleanup-stale
///
/// 清理 started_at/created_at 超时的 experiment_run 元数据。
/// 默认 dry_run=true；actual update 会把 pending/running 标记为 failed，
/// 并在 metrics 中写入 stale_cleanup 审计字段。
pub async fn cleanup_stale_experiment_runs(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleBackgroundTasksReq>,
) -> impl IntoResponse {
    match cleanup_stale_experiment_runs_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub(crate) async fn cleanup_stale_optimization_tasks_inner(
    db: &sqlx::PgPool,
    req: CleanupStaleBackgroundTasksReq,
) -> Result<Value, String> {
    let dry_run = cleanup_dry_run_default(req.dry_run);
    let default_timeout_seconds = cleanup_default_timeout_seconds(req.default_timeout_seconds);
    let limit = cleanup_limit(req.limit);
    let rows: Vec<(
        String,
        String,
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT optimization_task_id, status, progress, last_heartbeat_at,
                heartbeat_timeout_seconds, created_at
         FROM optimization_task
         WHERE status IN ('pending', 'running', 'cancel_requested')
           AND COALESCE(last_heartbeat_at, created_at)
               < now() - (COALESCE(heartbeat_timeout_seconds, $1)::text || ' seconds')::interval
         ORDER BY COALESCE(last_heartbeat_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds as i32)
    .bind(limit)
    .fetch_all(db)
    .await
    .map_err(|error| format!("cleanup stale optimization tasks query failed: {error}"))?;

    let candidates = rows
        .iter()
        .map(
            |(
                task_id,
                status,
                progress,
                last_heartbeat_at,
                heartbeat_timeout_seconds,
                created_at,
            )| {
                let observed_at = last_heartbeat_at.unwrap_or(*created_at);
                let timeout_seconds = heartbeat_timeout_seconds
                    .map(i64::from)
                    .unwrap_or(default_timeout_seconds);
                json!({
                    "optimization_task_id": task_id,
                    "status": status,
                    "next_status": optimization_cleanup_task_status_transition(status),
                    "progress": progress,
                    "last_heartbeat_at": fmt_rfc3339_local(*last_heartbeat_at),
                    "created_at": fmt_rfc3339_local(Some(*created_at)),
                    "observed_at": fmt_rfc3339_local(Some(observed_at)),
                    "heartbeat_timeout_seconds": timeout_seconds,
                })
            },
        )
        .collect::<Vec<_>>();

    if dry_run || rows.is_empty() {
        return Ok(json!({
            "dry_run": dry_run,
            "candidate_count": candidates.len(),
            "updated_task_count": 0,
            "updated_trial_count": 0,
            "candidates": candidates,
        }));
    }

    let task_ids = rows
        .iter()
        .map(|(task_id, ..)| task_id.clone())
        .collect::<Vec<_>>();
    let trial_result = sqlx::query(
        "UPDATE optimization_trial
         SET status = CASE
                 WHEN status = 'pending' THEN 'cancelled'
                 ELSE 'timeout'
             END,
             progress = 100,
             completed_at = COALESCE(completed_at, now()),
             last_heartbeat_at = now(),
             error_message = CONCAT(
                 COALESCE(NULLIF(error_message, '') || '; ', ''),
                 'stale optimization task cleaned up by cleanup-stale: parent task heartbeat timed out'
             )
         WHERE optimization_task_id = ANY($1)
           AND status IN ('pending', 'running', 'cancel_requested')",
    )
    .bind(&task_ids)
    .execute(db)
    .await
    .map_err(|error| format!("cleanup stale optimization trials update failed: {error}"))?;

    let task_result = sqlx::query(
        "UPDATE optimization_task
         SET status = CASE
                 WHEN status = 'pending' THEN 'cancelled'
                 ELSE 'timeout'
             END,
             progress = 100,
             last_heartbeat_at = now()
         WHERE optimization_task_id = ANY($1)
           AND status IN ('pending', 'running', 'cancel_requested')",
    )
    .bind(&task_ids)
    .execute(db)
    .await
    .map_err(|error| format!("cleanup stale optimization tasks update failed: {error}"))?;

    Ok(json!({
        "dry_run": false,
        "candidate_count": candidates.len(),
        "updated_task_count": task_result.rows_affected(),
        "updated_trial_count": trial_result.rows_affected(),
        "candidates": candidates,
    }))
}

pub(crate) async fn cleanup_stale_experiment_runs_inner(
    db: &sqlx::PgPool,
    req: CleanupStaleBackgroundTasksReq,
) -> Result<Value, String> {
    let dry_run = cleanup_dry_run_default(req.dry_run);
    let default_timeout_seconds = cleanup_default_timeout_seconds(req.default_timeout_seconds);
    let limit = cleanup_limit(req.limit);
    let rows: Vec<(
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                status, started_at, created_at
         FROM experiment_run
         WHERE status IN ('pending', 'running')
           AND COALESCE(started_at, created_at)
               < now() - ($1::text || ' seconds')::interval
         ORDER BY COALESCE(started_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds)
    .bind(limit)
    .fetch_all(db)
    .await
    .map_err(|error| format!("cleanup stale experiment runs query failed: {error}"))?;

    let candidates = rows
        .iter()
        .map(
            |(
                experiment_run_id,
                experiment_type,
                related_entity_type,
                related_entity_id,
                status,
                started_at,
                created_at,
            )| {
                let observed_at = started_at.unwrap_or(*created_at);
                json!({
                    "experiment_run_id": experiment_run_id,
                    "experiment_type": experiment_type,
                    "related_entity_type": related_entity_type,
                    "related_entity_id": related_entity_id,
                    "status": status,
                    "next_status": experiment_cleanup_status_transition(status),
                    "started_at": fmt_rfc3339_local(*started_at),
                    "created_at": fmt_rfc3339_local(Some(*created_at)),
                    "observed_at": fmt_rfc3339_local(Some(observed_at)),
                    "timeout_seconds": default_timeout_seconds,
                })
            },
        )
        .collect::<Vec<_>>();

    if dry_run || rows.is_empty() {
        return Ok(json!({
            "dry_run": dry_run,
            "candidate_count": candidates.len(),
            "updated_experiment_count": 0,
            "candidates": candidates,
        }));
    }

    let experiment_run_ids = rows
        .iter()
        .map(|(experiment_run_id, ..)| experiment_run_id.clone())
        .collect::<Vec<_>>();
    let result = sqlx::query(
        "UPDATE experiment_run
         SET status = 'failed',
             completed_at = now(),
             metrics = COALESCE(metrics, '{}'::jsonb) || jsonb_build_object(
                 'status', 'failed',
                 'stale_cleanup', jsonb_build_object(
                     'reason', 'stale experiment run cleaned up by cleanup-stale: no completion before timeout',
                     'timeout_seconds', $2,
                     'cleaned_at', now()
                 )
             )
         WHERE experiment_run_id = ANY($1)
           AND status IN ('pending', 'running')",
    )
    .bind(&experiment_run_ids)
    .bind(default_timeout_seconds)
    .execute(db)
    .await
    .map_err(|error| format!("cleanup stale experiment runs update failed: {error}"))?;

    Ok(json!({
        "dry_run": false,
        "candidate_count": candidates.len(),
        "updated_experiment_count": result.rows_affected(),
        "candidates": candidates,
    }))
}

pub async fn get_experiment_run(
    State(state): State<Arc<AppState>>,
    Path(experiment_run_id): Path<String>,
) -> impl IntoResponse {
    let row = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            Option<String>,
            Value,
            Option<Value>,
            String,
            Option<String>,
            Option<String>,
            String,
        ),
    >(
        "SELECT experiment_run_id,
                experiment_type,
                related_entity_type,
                related_entity_id,
                config,
                metrics,
                status,
                started_at::text,
                completed_at::text,
                created_at::text
         FROM experiment_run
         WHERE experiment_run_id = $1",
    )
    .bind(&experiment_run_id)
    .fetch_optional(&state.db)
    .await;

    match row {
        Ok(Some(row)) => Json(json!({
            "code": 0,
            "data": {
                "experiment_run_id": row.0,
                "experiment_type": row.1,
                "related_entity_type": row.2,
                "related_entity_id": row.3,
                "config": row.4,
                "metrics": row.5,
                "status": row.6,
                "started_at": row.7,
                "completed_at": row.8,
                "created_at": row.9,
            }
        })),
        Ok(None) => Json(json!({
            "code": 1,
            "message": format!("experiment_run not found: {}", experiment_run_id)
        })),
        Err(error) => Json(json!({
            "code": 1,
            "message": format!("Failed to get experiment_run: {}", error)
        })),
    }
}

pub async fn promote_optimization_trial(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<PromoteOptimizationRequest>,
) -> impl IntoResponse {
    match promote_trial(&state.db, &task_id, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub(crate) async fn execute_phase7_professional_discovery(
    db: &sqlx::PgPool,
    req: Phase7ProfessionalDiscoveryRequest,
) -> Result<Value, String> {
    let layered_req = phase7_discovery_layered_request(&req);
    let (task_id, bundle) =
        insert_phase7_layered_optimization(db, &layered_req, LocalResourcePlan::local_mac())
            .await?;
    let trial_batch_limit = req
        .trial_batch_limit
        .unwrap_or(bundle.plan.batch_size as i64)
        .clamp(1, 500);
    let max_batches = req.max_batches.unwrap_or_else(|| {
        let planned = bundle.plan.planned_trials.max(1);
        let batch = trial_batch_limit.max(1) as usize;
        planned.div_ceil(batch)
    });
    let max_batches = max_batches.clamp(1, 10_000);
    let robustness_top_n = req.robustness_top_n.unwrap_or(3).clamp(1, 20);
    let targets = CandidateTargets::default();
    let gate_policy = resolve_robustness_gate_policy(req.robustness_gate_policy.as_ref());
    let stop_after_professional = req.stop_after_professional_candidate.unwrap_or(true);
    let stop_after_robust_approval = req.stop_after_robust_approval.unwrap_or(true);

    let mut batch_summaries = Vec::new();
    let mut robustness_results = Vec::new();
    let mut evaluated_robustness_trials = BTreeSet::new();
    let mut signal_cache = SignalDataCache::default();
    let mut backtest_cache = BacktestDataCache::default();
    let mut approved_candidate_found = false;
    let mut stop_reason = "planned_trials_exhausted";
    for _ in 0..max_batches {
        let summary = execute_pending_trials_with_caches(
            db,
            &task_id,
            trial_batch_limit,
            None,
            &mut signal_cache,
            &mut backtest_cache,
        )
        .await?;
        let executed = summary["executed"].as_i64().unwrap_or(0);
        batch_summaries.push(summary);
        let candidates =
            load_discovery_candidates(db, &task_id, &targets, robustness_top_n).await?;
        if stop_after_professional
            && candidates
                .iter()
                .any(|candidate| candidate.candidate_type == CandidateType::Professional)
        {
            let professional_candidates = candidates
                .iter()
                .filter(|candidate| candidate.candidate_type == CandidateType::Professional)
                .cloned()
                .collect::<Vec<_>>();
            let batch_robustness_results = evaluate_discovery_candidates(
                db,
                &task_id,
                &professional_candidates,
                &gate_policy,
                &mut evaluated_robustness_trials,
            )
            .await?;
            approved_candidate_found = batch_robustness_results
                .iter()
                .any(robustness_result_is_approved);
            robustness_results.extend(batch_robustness_results);
            if stop_after_robust_approval {
                if approved_candidate_found {
                    stop_reason = "robust_professional_candidate_approved";
                    break;
                }
            } else {
                stop_reason = "professional_candidate_found_before_robust_approval";
                break;
            }
        }
        if executed == 0 {
            stop_reason = "no_pending_trials";
            break;
        }
    }

    let candidates = load_discovery_candidates(db, &task_id, &targets, robustness_top_n).await?;
    let final_robustness_results = evaluate_discovery_candidates(
        db,
        &task_id,
        &candidates,
        &gate_policy,
        &mut evaluated_robustness_trials,
    )
    .await?;
    approved_candidate_found = approved_candidate_found
        || final_robustness_results
            .iter()
            .any(robustness_result_is_approved);
    robustness_results.extend(final_robustness_results);

    Ok(json!({
        "optimization_task_id": task_id,
        "search_method": "phase7_professional_discovery",
        "search_profile": bundle.search_space["search_profile"],
        "requested_trials": bundle.plan.requested_trials,
        "planned_trials": bundle.plan.planned_trials,
        "trial_batch_limit": trial_batch_limit,
        "executed_batches": batch_summaries.len(),
        "batch_summaries": batch_summaries,
        "stop_reason": stop_reason,
        "stop_after_robust_approval": stop_after_robust_approval,
        "approved_candidate_found": approved_candidate_found,
        "robustness_top_n": robustness_top_n,
        "robustness_gate_policy": gate_policy,
        "robustness_candidates": candidates
            .iter()
            .map(discovery_candidate_json)
            .collect::<Vec<_>>(),
        "robustness_results": robustness_results,
    }))
}

pub async fn generate_elite_validation_report(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<EliteValidationReportRequest>,
) -> impl IntoResponse {
    match build_and_persist_elite_validation_report(&state.db, &task_id, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn list_optimization_trials(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Query(query): Query<TrialListQuery>,
) -> impl IntoResponse {
    let status = query
        .status
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let limit = normalize_limit(query.limit);

    let rows = sqlx::query_as::<
        _,
        (
            String,
            i32,
            Value,
            Option<Decimal>,
            Option<Value>,
            Option<Value>,
            String,
            i32,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT trial_id, trial_index, parameters, score, metrics, constraint_violations,
           status, progress, backtest_task_id, error_message
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND ($2::text IS NULL OR status = $2)
         ORDER BY trial_index ASC
         LIMIT $3",
    )
    .bind(&task_id)
    .bind(status.as_deref())
    .bind(limit)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => Json(json!({
            "code": 0,
            "data": {
                "optimization_task_id": task_id,
                "trials": rows.into_iter().map(|row| json!({
                    "trial_id": row.0,
                    "trial_index": row.1,
                    "parameters": row.2,
                    "score": row.3,
                    "metrics": row.4,
                    "constraint_violations": row.5,
                    "status": row.6,
                    "progress": row.7,
                    "backtest_task_id": row.8,
                    "error_message": row.9,
                })).collect::<Vec<_>>()
            }
        })),
        Err(error) => Json(
            json!({"code": 1, "message": format!("Failed to list optimization trials: {}", error)}),
        ),
    }
}

pub(crate) async fn persist_optimization_experiment_run(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_limit: i64,
    executed: i64,
    completed: i64,
    failed: i64,
    elapsed_ms: i64,
    best_trial_id: &Value,
    policy: &OptimizationPerformanceGatePolicy,
    gates: &Value,
    gate_status: &str,
    signal_cache_stats: Option<&Value>,
    backtest_cache_stats: Option<&Value>,
) -> Result<String, String> {
    let throughput = if elapsed_ms > 0 {
        Some(executed as f64 * 1000.0 / elapsed_ms as f64)
    } else {
        None
    };
    let config = json!({
        "optimization_task_id": task_id,
        "trial_limit": trial_limit,
        "gate_policy": {
            "min_completed_trials": policy.min_completed_trials,
            "max_failed_trials": policy.max_failed_trials,
            "max_elapsed_ms": policy.max_elapsed_ms,
        }
    });
    let metrics = json!({
        "executed": executed,
        "completed": completed,
        "failed": failed,
        "elapsed_ms": elapsed_ms,
        "throughput_trials_per_sec": throughput,
        "best_trial_id": best_trial_id,
        "gate_status": gate_status,
        "gates": gates,
        "signal_cache": signal_cache_stats,
        "backtest_cache": backtest_cache_stats,
    });
    let experiment_status = if gate_status == "passed" {
        "completed"
    } else if completed > 0 || failed > 0 || executed > 0 {
        "partial"
    } else {
        "failed"
    };

    create_experiment_run(
        db,
        "optimization_trial_batch_release_gate",
        "optimization_task",
        Some(task_id),
        &config,
        &metrics,
        experiment_status,
    )
    .await
    .map_err(|error| format!("Failed to insert optimization experiment_run: {}", error))
}

pub(crate) async fn promote_trial(
    db: &sqlx::PgPool,
    task_id: &str,
    req: PromoteOptimizationRequest,
) -> Result<Value, String> {
    let best_trial_id = sqlx::query_as::<_, (Option<String>, String, String)>(
        "SELECT best_trial_id, strategy_version_id, data_version_id
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization task: {}", error))?
    .ok_or_else(|| "optimization task not found".to_string())?;

    let default_trial_id = best_trial_id.0.as_deref().ok_or_else(|| {
        "optimization task has no best_trial_id; run completed trials first".to_string()
    })?;
    let normalized = normalize_promote_request(default_trial_id, req)?;

    let trial = sqlx::query_as::<
        _,
        (
            String,
            Value,
            Option<Value>,
            Option<Decimal>,
            Option<Value>,
            String,
        ),
    >(
        "SELECT trial_id, parameters, metrics, score, constraint_violations, status
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND trial_id = $2",
    )
    .bind(task_id)
    .bind(&normalized.trial_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization trial: {}", error))?
    .ok_or_else(|| "optimization trial not found".to_string())?;

    if trial.5 != "completed" {
        return Err("only completed trials can be promoted".to_string());
    }
    let score = trial
        .3
        .ok_or_else(|| "completed trial has no score".to_string())?;

    let candidate_id = format!("candidate-{}", Uuid::new_v4());
    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin promote transaction: {}", error))?;

    sqlx::query(
        "INSERT INTO strategy_parameter_candidate
           (candidate_id, candidate_name, target_strategy_version, strategy_version_id,
            optimization_task_id, trial_id, parameters, metrics, objective_score,
            constraints, gate_policy, promotion_mode, status, reviewer, reason, notes)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
    )
    .bind(&candidate_id)
    .bind(&normalized.candidate_name)
    .bind(&normalized.target_strategy_version)
    .bind(&best_trial_id.1)
    .bind(task_id)
    .bind(&normalized.trial_id)
    .bind(&trial.1)
    .bind(&trial.2)
    .bind(score)
    .bind(&trial.4)
    .bind(&normalized.gate_policy)
    .bind(&normalized.promotion_mode)
    .bind(&normalized.status)
    .bind(&normalized.reviewer)
    .bind(&normalized.reason)
    .bind(&normalized.notes)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to create strategy parameter candidate: {}", error))?;

    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, 'strategy_parameter_candidate.promote', 'strategy_parameter_candidate',
                 $2, $3, $4, $5)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(&candidate_id)
    .bind(&normalized.reviewer)
    .bind(format!(
        "Promoted optimization trial {} to candidate {}",
        normalized.trial_id, candidate_id
    ))
    .bind(json!({
        "optimization_task_id": task_id,
        "trial_id": normalized.trial_id,
        "target_strategy_version": normalized.target_strategy_version,
        "gate_policy": normalized.gate_policy,
        "freeze_after_approval": normalized.freeze_after_approval,
    }))
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to write promote audit event: {}", error))?;

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit promote transaction: {}", error))?;

    Ok(json!({
        "candidate_id": candidate_id,
        "status": normalized.status,
        "source_trial_id": normalized.trial_id,
        "optimization_task_id": task_id,
        "target_strategy_version": normalized.target_strategy_version,
        "gate_policy": normalized.gate_policy,
        "requires_manual_review": true,
    }))
}

pub(crate) async fn build_and_persist_elite_validation_report(
    db: &sqlx::PgPool,
    task_id: &str,
    req: EliteValidationReportRequest,
) -> Result<Value, String> {
    let top_n = req.top_n.unwrap_or(3).clamp(1, 10);
    let requested_policy = req
        .gate_policy
        .unwrap_or_else(|| json!({"preset": "professional_elite"}));
    let gate_policy = resolve_robustness_gate_policy(Some(&requested_policy));
    let snapshots = load_completed_trial_snapshots(db, task_id).await?;
    if snapshots.is_empty() {
        return Err("optimization task has no completed trials with metrics".to_string());
    }

    let mut ranked = snapshots.clone();
    ranked.sort_by(elite_trial_report_order);
    let selected = ranked.into_iter().take(top_n).collect::<Vec<_>>();

    let mut rows = Vec::new();
    for (rank, trial) in selected.iter().enumerate() {
        let robustness = evaluate_and_persist_robustness_for_trial(
            db,
            task_id,
            &trial.trial_id,
            Some(&gate_policy),
            "Professional elite validation report gate evaluated",
        )
        .await?;
        rows.push(json!({
            "rank": rank + 1,
            "trial_id": trial.trial_id,
            "trial_index": trial.trial_index,
            "backtest_task_id": trial.backtest_task_id,
            "score": trial.score,
            "elite_gap_score": elite_gap_score(&trial.metrics),
            "metrics": trial.metrics,
            "metric_sources": trial.metric_sources,
            "missing_elite_metrics": trial.missing_elite_metrics,
            "constraint_violations": trial.constraint_violations,
            "parameters": trial.parameters,
            "elite_status": robustness["status"],
            "elite_robustness": robustness,
            "parameter_plateau": build_parameter_plateau_analysis(trial, &snapshots),
            "portfolio_correlation_contribution": build_portfolio_correlation_contribution_score(
                trial,
                &snapshots
            ),
        }));
    }

    let summary = summarize_elite_validation_rows(&rows);
    let metrics = json!({
        "top_n": top_n,
        "candidate_count": rows.len(),
        "summary": summary,
        "candidates": rows,
    });
    let config = json!({
        "optimization_task_id": task_id,
        "gate_policy": gate_policy,
        "ranking": "elite_gap_score_then_drawdown_then_sortino_sharpe_annual",
    });
    let experiment_run_id =
        persist_elite_validation_report_experiment(db, task_id, &config, &metrics).await?;

    Ok(json!({
        "optimization_task_id": task_id,
        "experiment_run_id": experiment_run_id,
        "gate_policy": config["gate_policy"],
        "top_n": top_n,
        "summary": metrics["summary"],
        "candidates": metrics["candidates"],
    }))
}

pub(crate) async fn load_completed_trial_snapshots(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<Vec<CompletedTrialSnapshot>, String> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            i32,
            Value,
            Option<Decimal>,
            Option<Value>,
            Option<Value>,
            Option<String>,
        ),
    >(
        "SELECT trial_id, trial_index, parameters, score, metrics, constraint_violations,
                backtest_task_id
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND status = 'completed'
           AND metrics IS NOT NULL",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load completed optimization trials: {}", error))?;

    let mut snapshots = Vec::new();
    for (trial_id, trial_index, parameters, score, metrics, violations, backtest_task_id) in rows {
        let Some(metrics) = metrics else {
            continue;
        };
        let (metrics, metric_sources, missing_elite_metrics) =
            enrich_trial_metrics(db, backtest_task_id.as_deref(), metrics).await?;
        snapshots.push(CompletedTrialSnapshot {
            trial_id,
            trial_index,
            backtest_task_id,
            score,
            metrics,
            metric_sources,
            missing_elite_metrics,
            constraint_violations: violations.unwrap_or_else(|| json!([])),
            parameters,
        });
    }
    Ok(snapshots)
}

pub(crate) async fn enrich_trial_metrics(
    db: &sqlx::PgPool,
    backtest_task_id: Option<&str>,
    mut metrics: Value,
) -> Result<(Value, Value, Value), String> {
    let mut sources = existing_metric_sources(&metrics);
    if let Some(backtest_task_id) = backtest_task_id {
        enrich_trial_metrics_from_backtest_result(db, backtest_task_id, &mut metrics, &mut sources)
            .await?;
        if !metric_has_number(&metrics, "max_drawdown_duration_days") {
            if let Some(duration_days) =
                derive_max_drawdown_duration_days(db, backtest_task_id).await?
            {
                insert_metric_if_missing(
                    &mut metrics,
                    &mut sources,
                    "max_drawdown_duration_days",
                    Some(json!(duration_days)),
                    "backtest_equity_curve.portfolio_value",
                );
            }
        }
    }
    let missing = missing_elite_metrics(&metrics);
    Ok((metrics, Value::Object(sources), json!(missing)))
}

pub(crate) async fn enrich_trial_metrics_from_backtest_result(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
    metrics: &mut Value,
    sources: &mut Map<String, Value>,
) -> Result<(), String> {
    let row = sqlx::query_as::<
        _,
        (
            Option<Value>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
            Option<i32>,
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
        ),
    >(
        "SELECT metrics,
                total_return,
                annualized_return,
                benchmark_return,
                excess_return,
                annualized_excess_return,
                sharpe_ratio,
                sortino_ratio,
                information_ratio,
                max_drawdown,
                turnover,
                total_trades,
                win_rate,
                calmar_ratio,
                annualized_volatility
         FROM backtest_result
         WHERE task_id = $1",
    )
    .bind(backtest_task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load backtest_result metrics: {}", error))?;

    let Some(row) = row else {
        return Ok(());
    };

    if let Some(backtest_metrics) = row.0.as_ref().and_then(Value::as_object) {
        for key in ELITE_REPORT_METRIC_KEYS {
            insert_metric_if_missing(
                metrics,
                sources,
                key,
                backtest_metrics.get(*key).cloned(),
                "backtest_result.metrics",
            );
        }
    }

    insert_metric_if_missing(
        metrics,
        sources,
        "total_return",
        row.1.map(|value| json!(value)),
        "backtest_result.total_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "annual_return_pct",
        row.2.map(|value| json!(value)),
        "backtest_result.annualized_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "benchmark_return_pct",
        row.3.map(|value| json!(value)),
        "backtest_result.benchmark_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "excess_return_pct",
        row.4.or(row.5).map(|value| json!(value)),
        "backtest_result.excess_return",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "sharpe_ratio",
        row.6.map(|value| json!(value)),
        "backtest_result.sharpe_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "sortino_ratio",
        row.7.map(|value| json!(value)),
        "backtest_result.sortino_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "information_ratio",
        row.8.map(|value| json!(value)),
        "backtest_result.information_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "max_drawdown_pct",
        row.9.map(|value| json!(value)),
        "backtest_result.max_drawdown",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "turnover",
        row.10.map(|value| json!(value)),
        "backtest_result.turnover",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "num_trades",
        row.11.map(|value| json!(value)),
        "backtest_result.total_trades",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "win_rate_pct",
        row.12.map(|value| json!(value)),
        "backtest_result.win_rate",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "calmar_ratio",
        row.13.map(|value| json!(value)),
        "backtest_result.calmar_ratio",
    );
    insert_metric_if_missing(
        metrics,
        sources,
        "annualized_volatility",
        row.14.map(|value| json!(value)),
        "backtest_result.annualized_volatility",
    );

    Ok(())
}

pub(crate) async fn derive_max_drawdown_duration_days(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
) -> Result<Option<i64>, String> {
    let rows = sqlx::query_as::<_, (Option<Decimal>,)>(
        "SELECT portfolio_value
         FROM backtest_equity_curve
         WHERE task_id = $1
         ORDER BY trade_date ASC",
    )
    .bind(backtest_task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load backtest equity curve: {}", error))?;

    let mut peak: Option<Decimal> = None;
    let mut current_duration = 0i64;
    let mut max_duration = 0i64;
    let mut observed = false;
    for (portfolio_value,) in rows {
        let Some(portfolio_value) = portfolio_value else {
            continue;
        };
        if portfolio_value <= Decimal::ZERO {
            continue;
        }
        observed = true;
        if peak.map(|value| portfolio_value >= value).unwrap_or(true) {
            peak = Some(portfolio_value);
            current_duration = 0;
        } else {
            current_duration += 1;
            max_duration = max_duration.max(current_duration);
        }
    }

    Ok(observed.then_some(max_duration))
}

pub(crate) const ELITE_REPORT_METRIC_KEYS: &[&str] = &[
    "annual_return_pct",
    "excess_return_pct",
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "profit_factor",
    "max_drawdown_pct",
    "max_drawdown_duration_days",
    "num_trades",
];

pub(crate) fn existing_metric_sources(metrics: &Value) -> Map<String, Value> {
    let mut sources = Map::new();
    for key in ELITE_REPORT_METRIC_KEYS {
        if metric_has_number(metrics, key) {
            sources.insert((*key).to_string(), json!("optimization_trial.metrics"));
        }
    }
    sources
}

pub(crate) fn insert_metric_if_missing(
    metrics: &mut Value,
    sources: &mut Map<String, Value>,
    key: &str,
    value: Option<Value>,
    source: &str,
) {
    if metric_has_number(metrics, key) {
        return;
    }
    let Some(value) = value else {
        return;
    };
    if value_as_f64(&value).map(|value| value.is_finite()) != Some(true) {
        return;
    }
    ensure_metrics_object(metrics).insert(key.to_string(), value);
    sources.insert(key.to_string(), json!(source));
}

pub(crate) fn ensure_metrics_object(metrics: &mut Value) -> &mut Map<String, Value> {
    if !metrics.is_object() {
        *metrics = json!({});
    }
    metrics.as_object_mut().expect("metrics object")
}

pub(crate) fn metric_has_number(metrics: &Value, key: &str) -> bool {
    metrics
        .get(key)
        .and_then(value_as_f64)
        .map(|value| value.is_finite())
        .unwrap_or(false)
}

pub(crate) fn missing_elite_metrics(metrics: &Value) -> Vec<&'static str> {
    ELITE_REPORT_METRIC_KEYS
        .iter()
        .copied()
        .filter(|key| !metric_has_number(metrics, key))
        .collect()
}

pub(crate) async fn persist_elite_validation_report_experiment(
    db: &sqlx::PgPool,
    task_id: &str,
    config: &Value,
    metrics: &Value,
) -> Result<String, String> {
    create_experiment_run(
        db,
        "professional_elite_validation_report",
        "optimization_task",
        Some(task_id),
        config,
        metrics,
        "completed",
    )
    .await
    .map_err(|error| {
        format!(
            "Failed to insert elite validation experiment_run: {}",
            error
        )
    })
}

pub(crate) fn summarize_elite_validation_rows(rows: &[Value]) -> Value {
    let approved = rows
        .iter()
        .filter(|row| row["elite_status"] == "approved_candidate")
        .count();
    let review_required = rows
        .iter()
        .filter(|row| row["elite_status"] == "review_required")
        .count();
    let rejected = rows
        .iter()
        .filter(|row| row["elite_status"] == "rejected")
        .count();
    let best = rows.first().cloned().unwrap_or_else(|| json!({}));
    json!({
        "approved_count": approved,
        "review_required_count": review_required,
        "rejected_count": rejected,
        "missing_elite_metric_counts": summarize_missing_elite_metric_counts(rows),
        "best_trial_id": best["trial_id"],
        "best_elite_status": best["elite_status"],
        "best_elite_gap_score": best["elite_gap_score"],
        "best_metrics": best["metrics"],
        "best_missing_elite_metrics": best["missing_elite_metrics"],
    })
}

pub(crate) fn summarize_missing_elite_metric_counts(rows: &[Value]) -> Value {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for row in rows {
        let Some(missing) = row["missing_elite_metrics"].as_array() else {
            continue;
        };
        for item in missing {
            if let Some(key) = item.as_str() {
                *counts.entry(key.to_string()).or_default() += 1;
            }
        }
    }
    json!(counts)
}

pub(crate) fn elite_trial_report_order(
    left: &CompletedTrialSnapshot,
    right: &CompletedTrialSnapshot,
) -> std::cmp::Ordering {
    elite_gap_score(&left.metrics)
        .partial_cmp(&elite_gap_score(&right.metrics))
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| {
            metric_number(&left.metrics, "max_drawdown_pct")
                .partial_cmp(&metric_number(&right.metrics, "max_drawdown_pct"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            metric_number(&right.metrics, "sortino_ratio")
                .partial_cmp(&metric_number(&left.metrics, "sortino_ratio"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            metric_number(&right.metrics, "sharpe_ratio")
                .partial_cmp(&metric_number(&left.metrics, "sharpe_ratio"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            metric_number(&right.metrics, "annual_return_pct")
                .partial_cmp(&metric_number(&left.metrics, "annual_return_pct"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.trial_index.cmp(&right.trial_index))
}

pub(crate) fn elite_gap_score(metrics: &Value) -> f64 {
    let profile = EliteMetricProfile::from_metrics(metrics);
    positive_shortfall(0.15, profile.annual_return) * 4.0
        + positive_shortfall(0.0, profile.excess_return) * 2.0
        + positive_shortfall(1.5, profile.sharpe) * 1.5
        + positive_shortfall(1.8, profile.sortino)
        + positive_shortfall(2.0, profile.calmar) * 0.75
        + positive_shortfall(1.5, profile.profit_factor) * 0.75
        + positive_shortfall(profile.max_drawdown, 0.35) * 3.0
        + positive_shortfall(profile.max_drawdown_duration_days, 126.0) / 252.0
        + positive_shortfall(200.0, profile.num_trades) / 200.0
}

pub(crate) fn build_parameter_plateau_analysis(
    candidate: &CompletedTrialSnapshot,
    trials: &[CompletedTrialSnapshot],
) -> Value {
    let candidate_metrics = EliteMetricProfile::from_metrics(&candidate.metrics);
    let mut near_neighbor_count = 0usize;
    let mut stable_neighbor_count = 0usize;
    let mut axis_peers: BTreeMap<String, Vec<&CompletedTrialSnapshot>> = BTreeMap::new();

    for peer in trials {
        if peer.trial_id == candidate.trial_id {
            continue;
        }
        let diff_keys = differing_parameter_keys(&candidate.parameters, &peer.parameters);
        if diff_keys.is_empty() {
            continue;
        }
        if diff_keys.len() <= 3 {
            near_neighbor_count += 1;
            if is_plateau_stable_neighbor(candidate_metrics, &peer.metrics) {
                stable_neighbor_count += 1;
            }
        }
        if diff_keys.len() == 1 {
            axis_peers
                .entry(diff_keys[0].clone())
                .or_default()
                .push(peer);
        }
    }

    let axes = axis_peers
        .iter()
        .map(|(axis, peers)| parameter_axis_plateau_summary(axis, candidate, peers))
        .collect::<Vec<_>>();
    let plateau_score = ratio(stable_neighbor_count, near_neighbor_count);

    json!({
        "plateau_score": plateau_score,
        "near_neighbor_count": near_neighbor_count,
        "stable_neighbor_count": stable_neighbor_count,
        "stable_neighbor_ratio": plateau_score,
        "stability_rule": {
            "annual_return_drawdown": "peer annual_return >= candidate - 1pp and max_drawdown <= candidate + 3pp",
            "risk_adjusted": "peer Sharpe >= candidate - 0.10 and Sortino >= candidate - 0.15",
            "near_neighbor": "parameter diff count <= 3"
        },
        "single_axis": axes,
    })
}

pub(crate) fn parameter_axis_plateau_summary(
    axis: &str,
    candidate: &CompletedTrialSnapshot,
    peers: &[&CompletedTrialSnapshot],
) -> Value {
    let stable = peers
        .iter()
        .filter(|peer| {
            is_plateau_stable_neighbor(
                EliteMetricProfile::from_metrics(&candidate.metrics),
                &peer.metrics,
            )
        })
        .count();
    let mut values = peers
        .iter()
        .map(|peer| parameter_value(&peer.parameters, axis))
        .collect::<Vec<_>>();
    values.sort_by_key(canonical_json);
    values.dedup();

    let best_peer = peers
        .iter()
        .min_by(|left, right| elite_trial_report_order(left, right))
        .map(|peer| compact_trial_metrics(peer));
    json!({
        "parameter": axis,
        "candidate_value": parameter_value(&candidate.parameters, axis),
        "peer_values": values,
        "peer_count": peers.len(),
        "stable_peer_count": stable,
        "stable_ratio": ratio(stable, peers.len()),
        "best_peer": best_peer,
    })
}

pub(crate) fn is_plateau_stable_neighbor(
    candidate: EliteMetricProfile,
    peer_metrics: &Value,
) -> bool {
    let peer = EliteMetricProfile::from_metrics(peer_metrics);
    peer.annual_return >= candidate.annual_return - 0.01
        && peer.sharpe >= candidate.sharpe - 0.10
        && peer.sortino >= candidate.sortino - 0.15
        && peer.max_drawdown <= candidate.max_drawdown + 0.03
}

pub(crate) fn build_portfolio_correlation_contribution_score(
    candidate: &CompletedTrialSnapshot,
    trials: &[CompletedTrialSnapshot],
) -> Value {
    let explicit_score = correlation_control_score(&candidate.parameters);
    let controlled = trials
        .iter()
        .filter(|trial| correlation_control_score(&trial.parameters) > 0.0)
        .collect::<Vec<_>>();
    let uncontrolled = trials
        .iter()
        .filter(|trial| correlation_control_score(&trial.parameters) == 0.0)
        .collect::<Vec<_>>();
    let controlled_summary = summarize_trial_metric_group(&controlled);
    let uncontrolled_summary = summarize_trial_metric_group(&uncontrolled);
    let peer_impact = correlation_peer_impact(&controlled_summary, &uncontrolled_summary);
    let contribution_score = (explicit_score * 0.70
        + peer_impact["normalized_score"].as_f64().unwrap_or(0.0) * 0.30)
        .clamp(0.0, 1.0);

    json!({
        "contribution_score": contribution_score,
        "explicit_control_score": explicit_score,
        "active_controls": active_correlation_controls(&candidate.parameters),
        "controlled_peer_summary": controlled_summary,
        "uncontrolled_peer_summary": uncontrolled_summary,
        "peer_impact": peer_impact,
    })
}

pub(crate) fn active_correlation_controls(parameters: &Value) -> Vec<Value> {
    let mut controls = Vec::new();
    if let Some(limit) = parameter_f64(parameters, "max_pairwise_correlation") {
        controls.push(json!({"name": "max_pairwise_correlation", "value": limit}));
    }
    if let Some(lookback) = parameter_f64(parameters, "correlation_lookback_days") {
        controls.push(json!({"name": "correlation_lookback_days", "value": lookback}));
    }
    if let Some(filter) = parameter_str(parameters, "candidate_risk_filter") {
        if filter.contains("correlation") {
            controls.push(json!({"name": "candidate_risk_filter", "value": filter}));
        }
    }
    if let Some(method) = parameter_str(parameters, "portfolio_method") {
        if method == "risk_budget" || method == "min_variance" {
            controls.push(json!({"name": "portfolio_method", "value": method}));
        }
    }
    if let Some(control) = parameter_str(parameters, "risk_contribution_control") {
        if control != "off" {
            controls.push(json!({"name": "risk_contribution_control", "value": control}));
        }
    }
    if let Some(budget) = parameter_str(parameters, "style_risk_budget") {
        if budget != "off" {
            controls.push(json!({"name": "style_risk_budget", "value": budget}));
        }
    }
    if let Some(budget) = parameter_str(parameters, "capacity_risk_budget") {
        if budget != "off" {
            controls.push(json!({"name": "capacity_risk_budget", "value": budget}));
        }
    }
    if let Some(budget) = parameter_str(parameters, "execution_impact_budget") {
        if budget != "off" {
            controls.push(json!({"name": "execution_impact_budget", "value": budget}));
        }
    }
    if let Some(profile) = parameter_str(parameters, "execution_schedule_profile") {
        if profile != "immediate" && profile != "off" {
            controls.push(json!({"name": "execution_schedule_profile", "value": profile}));
        }
    }
    if let Some(policy) = parameter_str(parameters, "execution_carry_policy") {
        if policy != "expire" && policy != "expire_v1" {
            controls.push(json!({"name": "execution_carry_policy", "value": policy}));
        }
    }
    if let Some(limit) = parameter_str(parameters, "execution_daily_target_move_limit_pct") {
        controls.push(json!({"name": "execution_daily_target_move_limit_pct", "value": limit}));
    }
    if let Some(days) = parameter_i64(parameters, "execution_max_carry_days") {
        controls.push(json!({"name": "execution_max_carry_days", "value": days}));
    }
    controls
}

pub(crate) fn correlation_control_score(parameters: &Value) -> f64 {
    let mut score: f64 = 0.0;
    if let Some(limit) = parameter_f64(parameters, "max_pairwise_correlation") {
        score += if limit <= 0.68 {
            0.35
        } else if limit <= 0.70 {
            0.30
        } else if limit <= 0.75 {
            0.20
        } else {
            0.10
        };
    }
    if parameter_str(parameters, "candidate_risk_filter")
        .map(|value| value.contains("correlation"))
        .unwrap_or(false)
    {
        score += 0.25;
    }
    if parameter_str(parameters, "portfolio_method")
        .map(|value| value == "risk_budget" || value == "min_variance")
        .unwrap_or(false)
    {
        score += 0.15;
    }
    if parameter_str(parameters, "risk_contribution_control")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.15;
    }
    if parameter_str(parameters, "style_risk_budget")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "capacity_risk_budget")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "execution_impact_budget")
        .map(|value| value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "execution_schedule_profile")
        .map(|value| value != "immediate" && value != "off")
        .unwrap_or(false)
    {
        score += 0.10;
    }
    if parameter_str(parameters, "execution_daily_target_move_limit_pct").is_some() {
        score += 0.05;
    }
    if parameter_i64(parameters, "execution_max_carry_days").is_some() {
        score += 0.05;
    }
    if let Some(lookback) = parameter_f64(parameters, "correlation_lookback_days") {
        score += if lookback >= 120.0 { 0.10 } else { 0.05 };
    }
    score.clamp(0.0, 1.0)
}

pub(crate) fn summarize_trial_metric_group(trials: &[&CompletedTrialSnapshot]) -> Value {
    if trials.is_empty() {
        return json!({
            "count": 0,
            "annual_return_avg": null,
            "sharpe_avg": null,
            "sortino_avg": null,
            "calmar_avg": null,
            "max_drawdown_avg": null,
        });
    }
    let count = trials.len() as f64;
    let profiles = trials
        .iter()
        .map(|trial| EliteMetricProfile::from_metrics(&trial.metrics))
        .collect::<Vec<_>>();
    json!({
        "count": trials.len(),
        "annual_return_avg": profiles.iter().map(|item| item.annual_return).sum::<f64>() / count,
        "sharpe_avg": profiles.iter().map(|item| item.sharpe).sum::<f64>() / count,
        "sortino_avg": profiles.iter().map(|item| item.sortino).sum::<f64>() / count,
        "calmar_avg": profiles.iter().map(|item| item.calmar).sum::<f64>() / count,
        "max_drawdown_avg": profiles.iter().map(|item| item.max_drawdown).sum::<f64>() / count,
    })
}

pub(crate) fn correlation_peer_impact(controlled: &Value, uncontrolled: &Value) -> Value {
    let controlled_count = controlled["count"].as_u64().unwrap_or(0);
    let uncontrolled_count = uncontrolled["count"].as_u64().unwrap_or(0);
    if controlled_count == 0 || uncontrolled_count == 0 {
        return json!({
            "normalized_score": 0.0,
            "reason": "insufficient_controlled_or_uncontrolled_peers",
        });
    }
    let sharpe_delta = controlled["sharpe_avg"].as_f64().unwrap_or(0.0)
        - uncontrolled["sharpe_avg"].as_f64().unwrap_or(0.0);
    let drawdown_delta = controlled["max_drawdown_avg"].as_f64().unwrap_or(0.0)
        - uncontrolled["max_drawdown_avg"].as_f64().unwrap_or(0.0);
    let annual_delta = controlled["annual_return_avg"].as_f64().unwrap_or(0.0)
        - uncontrolled["annual_return_avg"].as_f64().unwrap_or(0.0);
    let normalized_score = (0.50
        + sharpe_delta * 0.50
        + positive_shortfall(0.0, drawdown_delta) * 0.80
        + annual_delta.max(-0.05) * 0.20)
        .clamp(0.0, 1.0);
    json!({
        "normalized_score": normalized_score,
        "annual_return_delta": annual_delta,
        "sharpe_delta": sharpe_delta,
        "max_drawdown_delta": drawdown_delta,
    })
}

pub(crate) fn differing_parameter_keys(left: &Value, right: &Value) -> Vec<String> {
    let Some(left_map) = left.as_object() else {
        return Vec::new();
    };
    let Some(right_map) = right.as_object() else {
        return Vec::new();
    };
    let keys = left_map
        .keys()
        .chain(right_map.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .filter(|key| left_map.get(key) != right_map.get(key))
        .collect()
}

pub(crate) fn parameter_value(parameters: &Value, key: &str) -> Value {
    parameters.get(key).cloned().unwrap_or(Value::Null)
}

pub(crate) fn parameter_f64(parameters: &Value, key: &str) -> Option<f64> {
    parameters.get(key).and_then(value_as_f64)
}

pub(crate) fn parameter_i64(parameters: &Value, key: &str) -> Option<i64> {
    parameters.get(key).and_then(Value::as_i64)
}

pub(crate) fn parameter_str<'a>(parameters: &'a Value, key: &str) -> Option<&'a str> {
    parameters.get(key).and_then(Value::as_str)
}

pub(crate) fn compact_trial_metrics(trial: &CompletedTrialSnapshot) -> Value {
    json!({
        "trial_id": trial.trial_id,
        "trial_index": trial.trial_index,
        "elite_gap_score": elite_gap_score(&trial.metrics),
        "annual_return_pct": metric_number(&trial.metrics, "annual_return_pct"),
        "sharpe_ratio": metric_number(&trial.metrics, "sharpe_ratio"),
        "sortino_ratio": metric_number(&trial.metrics, "sortino_ratio"),
        "calmar_ratio": metric_number(&trial.metrics, "calmar_ratio"),
        "max_drawdown_pct": metric_number(&trial.metrics, "max_drawdown_pct"),
    })
}

pub(crate) fn metric_number(metrics: &Value, name: &str) -> f64 {
    metrics.get(name).and_then(value_as_f64).unwrap_or(0.0)
}

pub(crate) fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

impl EliteMetricProfile {
    fn from_metrics(metrics: &Value) -> Self {
        Self {
            annual_return: metric_number(metrics, "annual_return_pct"),
            excess_return: metric_number(metrics, "excess_return_pct"),
            sharpe: metric_number(metrics, "sharpe_ratio"),
            sortino: metric_number(metrics, "sortino_ratio"),
            calmar: metric_number(metrics, "calmar_ratio"),
            profit_factor: metric_number(metrics, "profit_factor"),
            max_drawdown: metric_number(metrics, "max_drawdown_pct"),
            max_drawdown_duration_days: metric_number(metrics, "max_drawdown_duration_days"),
            num_trades: metric_number(metrics, "num_trades"),
        }
    }
}

pub(crate) fn normalize_promote_request(
    default_trial_id: &str,
    req: PromoteOptimizationRequest,
) -> Result<NormalizedPromoteRequest, String> {
    let trial_id = req
        .trial_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_trial_id)
        .to_string();
    let target_strategy_version = req.target_strategy_version.trim().to_string();
    let candidate_name = req.candidate_name.trim().to_string();
    let reason = req.reason.trim().to_string();
    if target_strategy_version.is_empty() {
        return Err("target_strategy_version must not be empty".into());
    }
    if candidate_name.is_empty() {
        return Err("candidate_name must not be empty".into());
    }
    if reason.is_empty() {
        return Err("reason must not be empty".into());
    }
    let promotion_mode = req
        .promotion_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("create_candidate")
        .to_string();
    if promotion_mode != "create_candidate" {
        return Err(format!("unsupported promotion_mode: {}", promotion_mode));
    }

    Ok(NormalizedPromoteRequest {
        trial_id,
        target_strategy_version,
        candidate_name,
        promotion_mode,
        gate_policy: req
            .gate_policy
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("default_v1")
            .to_string(),
        freeze_after_approval: req.freeze_after_approval.unwrap_or(false),
        reviewer: req
            .reviewer
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        reason,
        notes: req
            .notes
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        status: "candidate".to_string(),
    })
}

pub(crate) async fn load_execution_context(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<OptimizationTaskExecutionContext, String> {
    let row = sqlx::query_as::<_, (String, String, Value, Option<Value>, Option<Value>)>(
        "SELECT strategy_version_id, data_version_id, objective, backtest_template, constraints
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization task: {}", error))?
    .ok_or_else(|| "optimization task not found".to_string())?;

    Ok(OptimizationTaskExecutionContext {
        strategy_version_id: row.0,
        data_version_id: row.1,
        objective: row.2,
        backtest_template: row.3.unwrap_or_else(|| json!({})),
        constraints: row.4,
    })
}

pub(crate) async fn mark_trial_running(db: &sqlx::PgPool, trial_id: &str) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'running', progress = 10,
             started_at = now(), last_heartbeat_at = now(), error_message = NULL
         WHERE trial_id = $1 AND status = 'pending'",
    )
    .bind(trial_id)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial running: {}", error))?;
    Ok(())
}

pub(crate) async fn mark_trial_completed(
    db: &sqlx::PgPool,
    trial_id: &str,
    backtest_task_id: &str,
    scored: &ScoredTrial,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'completed', progress = 100, backtest_task_id = $2,
             score = $3, metrics = $4, constraint_violations = $5,
             completed_at = now(), last_heartbeat_at = now(), error_message = NULL
         WHERE trial_id = $1",
    )
    .bind(trial_id)
    .bind(backtest_task_id)
    .bind(scored.score)
    .bind(&scored.metrics)
    .bind(&scored.constraint_violations)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial completed: {}", error))?;
    Ok(())
}

pub(crate) async fn mark_trial_failed(
    db: &sqlx::PgPool,
    trial_id: &str,
    error_message: &str,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'failed', progress = 100, completed_at = now(),
             last_heartbeat_at = now(), error_message = $2
         WHERE trial_id = $1",
    )
    .bind(trial_id)
    .bind(error_message)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial failed: {}", error))?;
    Ok(())
}

pub(crate) async fn refresh_task_progress(
    db: &sqlx::PgPool,
    task_id: &str,
) -> Result<Option<String>, String> {
    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
           COUNT(*) FILTER (WHERE status = 'completed')::bigint,
           COUNT(*) FILTER (WHERE status = 'failed')::bigint
         FROM optimization_trial
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to count optimization trials: {}", error))?;

    let best_trial_id = sqlx::query_as::<_, (String,)>(
        "SELECT trial_id
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'completed'
         ORDER BY score DESC NULLS LAST, trial_index ASC
         LIMIT 1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to find best optimization trial: {}", error))?
    .map(|row| row.0);

    let finished = counts.1 + counts.2;
    let progress = if counts.0 == 0 {
        0
    } else {
        ((finished * 100) / counts.0).clamp(0, 100) as i32
    };
    let status = if counts.0 > 0 && finished == counts.0 {
        if counts.1 > 0 {
            "completed"
        } else {
            "failed"
        }
    } else if finished > 0 {
        "partial"
    } else {
        "pending"
    };

    sqlx::query(
        "UPDATE optimization_task
         SET status = $2, progress = $3, best_trial_id = $4, last_heartbeat_at = now()
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .bind(status)
    .bind(progress)
    .bind(best_trial_id.as_deref())
    .execute(db)
    .await
    .map_err(|error| format!("Failed to refresh optimization task progress: {}", error))?;

    Ok(best_trial_id)
}

// ─── DDD Step 4b：experiment_run 通用写入 ─────────────────────────
//
// 消除 8 处重复的 `INSERT INTO experiment_run(...)` 样板。所有调用点的
// 表结构一致(experiment_run_id/experiment_type/related_entity_type/
// related_entity_id/config/metrics/status/started_at/completed_at)，
// 差异仅在于 related_entity_id 是否自引用、status 是否为 "running"(此时
// completed_at 留 NULL，其余状态写 now())。

/// 创建一条 `experiment_run` 记录，返回生成的 `experiment_run_id`。
///
/// - `related_entity_id`：为 `None` 时自引用（写入刚生成的 `experiment_run_id`），
///   对应原 wfa_engine.rs 两处 OOS walk-forward 用自身 ID 占位的场景。
/// - `status`：`"running"` 时 `completed_at` 留 NULL；其余状态（`completed`/
///   `partial`/`failed`）写入 `now()`。`started_at` 始终写 `now()`。
pub(crate) async fn create_experiment_run(
    db: &sqlx::PgPool,
    experiment_type: &str,
    related_entity_type: &str,
    related_entity_id: Option<&str>,
    config: &Value,
    metrics: &Value,
    status: &str,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let related_entity_id = related_entity_id.unwrap_or(&experiment_run_id);

    if status == "running" {
        sqlx::query(
            "INSERT INTO experiment_run
               (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                config, metrics, status, started_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
        )
        .bind(&experiment_run_id)
        .bind(experiment_type)
        .bind(related_entity_type)
        .bind(related_entity_id)
        .bind(config)
        .bind(metrics)
        .bind(status)
        .execute(db)
        .await
        .map_err(|error| format!("Failed to insert experiment_run ({experiment_type}): {error}"))?;
    } else {
        sqlx::query(
            "INSERT INTO experiment_run
               (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                config, metrics, status, started_at, completed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())",
        )
        .bind(&experiment_run_id)
        .bind(experiment_type)
        .bind(related_entity_type)
        .bind(related_entity_id)
        .bind(config)
        .bind(metrics)
        .bind(status)
        .execute(db)
        .await
        .map_err(|error| format!("Failed to insert experiment_run ({experiment_type}): {error}"))?;
    }

    Ok(experiment_run_id)
}

#[cfg(test)]
mod experiment_run_tests {
    use super::create_experiment_run;
    use serde_json::json;
    use sqlx::PgPool;

    /// 验证 `create_experiment_run` 能向真实 DB 写入记录,且字段语义正确:
    /// - related_entity_id=None 时自引用 experiment_run_id
    /// - status="running" 时 completed_at 为 NULL
    /// - status="completed" 时 completed_at 非 NULL
    ///
    /// 需 DB,默认不跑(--ignored 触发)。运行:
    ///   cargo test -p quant-api --lib experiment_run_tests -- --ignored
    #[tokio::test]
    #[ignore = "DB 集成测试(本机 PG),显式跑: cargo test -- --ignored"]
    async fn create_experiment_run_persists_running_and_completed() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = PgPool::connect(&url).await.expect("DB 连接成功");

        // running 状态:related_entity_id 自引用,completed_at 应为 NULL
        let running_id = create_experiment_run(
            &db,
            "test_create_experiment_run",
            "test_entity",
            None,
            &json!({"scenario": "running"}),
            &json!({"step": 0}),
            "running",
        )
        .await
        .expect("写入 running 记录");

        let (status, related, completed): (String, String, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT status, related_entity_id, completed_at FROM experiment_run \
                 WHERE experiment_run_id = $1",
            )
            .bind(&running_id)
            .fetch_one(&db)
            .await
            .expect("回读 running 记录");
        assert_eq!(status, "running");
        assert_eq!(related, running_id, "related_entity_id 应自引用");
        assert!(completed.is_none(), "running 状态 completed_at 必须为 NULL");

        // completed 状态:显式 related_entity_id,completed_at 应非 NULL
        let completed_id = create_experiment_run(
            &db,
            "test_create_experiment_run",
            "test_entity",
            Some("rel-123"),
            &json!({"scenario": "completed"}),
            &json!({"step": 1}),
            "completed",
        )
        .await
        .expect("写入 completed 记录");

        let (status, related, completed): (String, String, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT status, related_entity_id, completed_at FROM experiment_run \
                 WHERE experiment_run_id = $1",
            )
            .bind(&completed_id)
            .fetch_one(&db)
            .await
            .expect("回读 completed 记录");
        assert_eq!(status, "completed");
        assert_eq!(related, "rel-123");
        assert!(
            completed.is_some(),
            "completed 状态 completed_at 必须非 NULL"
        );

        // 清理测试数据(避免污染)
        let _ = sqlx::query(
            "DELETE FROM experiment_run WHERE experiment_type = 'test_create_experiment_run'",
        )
        .execute(&db)
        .await;
    }
}

// ─── 第九批 A 线（ML 域）experiment_run 补测 ─────────────────────────
//
// 归因：本文件 21.75% 覆盖，既有测试仅 1 个（ignored 的 create_experiment_run）。
// 差集分两路补：
// - 纯逻辑面：cleanup 默认值/状态转移、elite 报告纯函数群（gap 评分/排序/平台分析/
//   相关性贡献/指标合并）、normalize_promote_request——全部无 IO 直调；
// - DB CRUD 面：cleanup_stale 双函数（dry_run/apply）、trial 生命周期标记 + 进度刷新、
//   load_execution_context、completed trial 快照 + backtest 指标补全 + 回撤天数推导、
//   persist_optimization_experiment_run 状态矩阵、elite 报告全流程、promote_trial 全流程。
//
// 隔离纪律（对齐第五/八批先例）：
// - 共享父行 strategy_definition/strategy_version/data_version 用固定 zzz 键
//   ON CONFLICT DO NOTHING 幂等插入，结尾不删（另一并行测试可能正在引用）；
// - 每个测试的专属键带独立场景后缀（zzz_api9x_<场景>_*），前置 + 结尾精确清理；
// - cleanup apply 分支是全表扫描：dry_run 先断言候选全部是 zzz 键才继续 apply，
//   防止误伤库里真实超时行（REPORT_WRITE_TEST_LOCK 教训的结构化版本）。
#[cfg(test)]
mod ninth_batch {
    use super::*;

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 共享父行：strategy_definition → strategy_version → optimization_task 的 FK 链
    /// 加 data_version。幂等插入，结尾不删（防并行测试互拆）。
    async fn seed_shared_parents(db: &sqlx::PgPool) {
        sqlx::query(
            "INSERT INTO strategy_definition
               (strategy_id, strategy_code, name, strategy_type, status)
             VALUES (999999901, 'zzz_api9x_strategy', 'zzz 第九批实验', 'zzz', 'active')
             ON CONFLICT DO NOTHING",
        )
        .execute(db)
        .await
        .expect("insert zzz strategy_definition");
        sqlx::query(
            "INSERT INTO strategy_version
               (strategy_version_id, strategy_code, version, parameter_schema,
                default_parameters, status)
             VALUES ('zzz_api9x_sv', 'zzz_api9x_strategy', 'v9', '{}', '{}', 'active')
             ON CONFLICT DO NOTHING",
        )
        .execute(db)
        .await
        .expect("insert zzz strategy_version");
        sqlx::query(
            "INSERT INTO data_version
               (data_version_id, name, source, start_date, end_date, tables, snapshot_hash)
             VALUES ('zzz_api9x_dv', 'zzz 第九批数据', 'zzz', '2026-01-01', '2026-01-31',
                     '{}', 'zzz-api9x')
             ON CONFLICT DO NOTHING",
        )
        .execute(db)
        .await
        .expect("insert zzz data_version");
    }

    /// 按场景前缀清理本测试专属的全部行（子行先删，FK RESTRICT 的 candidate 最先）。
    async fn cleanup_scoped(db: &sqlx::PgPool, scope: &str) {
        let prefix = format!("zzz_api9x_{}%", scope);
        // promote 痕迹：audit_event 经 details jsonb 定位 + candidate 行
        let _ = sqlx::query(
            "DELETE FROM audit_event WHERE entity_type = 'strategy_parameter_candidate' \
             AND details->>'optimization_task_id' LIKE $1",
        )
        .bind(&prefix)
        .execute(db)
        .await;
        let _ = sqlx::query(
            "DELETE FROM strategy_parameter_candidate WHERE optimization_task_id LIKE $1",
        )
        .bind(&prefix)
        .execute(db)
        .await;
        let _ = sqlx::query("DELETE FROM experiment_run WHERE related_entity_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM backtest_result WHERE task_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM backtest_equity_curve WHERE task_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM backtest_task WHERE task_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
        // optimization_trial + robustness_gate_result 随 task CASCADE
        let _ = sqlx::query("DELETE FROM optimization_task WHERE optimization_task_id LIKE $1")
            .bind(&prefix)
            .execute(db)
            .await;
    }

    /// 造 zzz optimization_task；stale_days>0 表示心跳超时（心跳/建行时间回拨）。
    async fn insert_zzz_task(db: &sqlx::PgPool, task_id: &str, status: &str, stale_days: i32) {
        sqlx::query(
            "INSERT INTO optimization_task
               (optimization_task_id, strategy_version_id, data_version_id, search_method,
                search_space, objective, status, progress, last_heartbeat_at,
                heartbeat_timeout_seconds, created_at, backtest_template, constraints)
             VALUES ($1, 'zzz_api9x_sv', 'zzz_api9x_dv', 'random_search', '{}', '{}',
                     $2, 0, now() - ($3::int * interval '1 day'), 60,
                     now() - ($3::int * interval '1 day'), '{}', '{}')",
        )
        .bind(task_id)
        .bind(status)
        .bind(stale_days)
        .execute(db)
        .await
        .expect("insert zzz optimization_task");
    }

    /// 造 zzz optimization_trial。
    async fn insert_zzz_trial(
        db: &sqlx::PgPool,
        trial_id: &str,
        task_id: &str,
        trial_index: i32,
        status: &str,
        score: Option<Decimal>,
        metrics: Option<Value>,
        backtest_task_id: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO optimization_trial
               (trial_id, optimization_task_id, trial_index, parameters, status, progress,
                score, metrics, backtest_task_id, error_message)
             VALUES ($1, $2, $3, '{}', $4, 0, $5, $6, $7, $8)",
        )
        .bind(trial_id)
        .bind(task_id)
        .bind(trial_index)
        .bind(status)
        .bind(score)
        .bind(metrics)
        .bind(backtest_task_id)
        .bind(if status == "failed" {
            Some("zzz 预置失败信息")
        } else {
            None
        })
        .execute(db)
        .await
        .expect("insert zzz optimization_trial");
    }

    /// 造 zzz backtest_task（experiment_run 域 enrich/生命周期测试的 FK 父行）。
    async fn insert_zzz_backtest_task(db: &sqlx::PgPool, task_id: &str) {
        sqlx::query(
            "INSERT INTO backtest_task
               (task_id, strategy_version_id, data_version_id, benchmark_symbol, symbols,
                start_date, end_date, initial_capital, rebalance_frequency,
                cost_model, slippage_model, execution_rules, parameters, status)
             VALUES ($1, 'zzz_api9x_sv', 'zzz_api9x_dv', '000300.SH', ARRAY['ZZZ900.SH'],
                '2026-01-05', '2026-01-30', 100000, 'monthly',
                '{}', '{}', '{}', '{}', 'completed')",
        )
        .bind(task_id)
        .execute(db)
        .await
        .expect("insert zzz backtest_task");
    }

    /// 造一条已完成 trial 的完整指标快照（9 个 elite 指标全达标 → gap score 0）。
    fn full_elite_metrics(annual: f64) -> Value {
        json!({
            "annual_return_pct": annual,
            "excess_return_pct": 0.05,
            "sharpe_ratio": 2.0,
            "sortino_ratio": 2.2,
            "calmar_ratio": 2.5,
            "profit_factor": 2.0,
            "max_drawdown_pct": 0.30,
            "max_drawdown_duration_days": 100,
            "num_trades": 300
        })
    }

    /// CompletedTrialSnapshot 构造 helper（纯逻辑测试用）。
    fn snapshot(
        trial_id: &str,
        trial_index: i32,
        metrics: Value,
        parameters: Value,
    ) -> CompletedTrialSnapshot {
        CompletedTrialSnapshot {
            trial_id: trial_id.to_string(),
            trial_index,
            backtest_task_id: None,
            score: Some(Decimal::ONE),
            metrics,
            metric_sources: json!({}),
            missing_elite_metrics: json!([]),
            constraint_violations: json!([]),
            parameters,
        }
    }

    // ── 纯逻辑：cleanup 选项与状态转移 ──

    #[test]
    fn cleanup_option_defaults_and_clamps() {
        // dry_run 缺省 true：运维安全默认（先审计后落盘）
        assert!(cleanup_dry_run_default(None));
        assert!(!cleanup_dry_run_default(Some(false)));
        assert!(cleanup_dry_run_default(Some(true)));
        // 超时缺省 1 小时，钳制在 [60s, 24h]
        assert_eq!(cleanup_default_timeout_seconds(None), 3600);
        assert_eq!(cleanup_default_timeout_seconds(Some(10)), 60);
        assert_eq!(cleanup_default_timeout_seconds(Some(100_000)), 86_400);
        assert_eq!(cleanup_default_timeout_seconds(Some(120)), 120);
        // limit 缺省 100，钳制在 [1, 1000]
        assert_eq!(cleanup_limit(None), 100);
        assert_eq!(cleanup_limit(Some(0)), 1);
        assert_eq!(cleanup_limit(Some(2000)), 1000);
        assert_eq!(cleanup_limit(Some(50)), 50);
    }

    #[test]
    fn cleanup_status_transition_maps_for_tasks_trials_and_experiments() {
        // task：pending→cancelled，running/cancel_requested→timeout，终态不动
        assert_eq!(
            optimization_cleanup_task_status_transition("pending"),
            Some("cancelled")
        );
        assert_eq!(
            optimization_cleanup_task_status_transition("running"),
            Some("timeout")
        );
        assert_eq!(
            optimization_cleanup_task_status_transition("cancel_requested"),
            Some("timeout")
        );
        assert_eq!(
            optimization_cleanup_task_status_transition("completed"),
            None
        );
        assert_eq!(optimization_cleanup_task_status_transition("failed"), None);
        // trial（cfg(test) 专用镜像）：与 task 同构
        assert_eq!(
            optimization_cleanup_trial_status_transition("pending"),
            Some("cancelled")
        );
        assert_eq!(
            optimization_cleanup_trial_status_transition("cancel_requested"),
            Some("timeout")
        );
        assert_eq!(
            optimization_cleanup_trial_status_transition("timeout"),
            None
        );
        // experiment：pending/running→failed，其余不动
        assert_eq!(
            experiment_cleanup_status_transition("pending"),
            Some("failed")
        );
        assert_eq!(
            experiment_cleanup_status_transition("running"),
            Some("failed")
        );
        assert_eq!(experiment_cleanup_status_transition("partial"), None);
        assert_eq!(experiment_cleanup_status_transition("completed"), None);
    }

    #[test]
    fn missing_reference_error_messages_embed_ids() {
        let message = missing_strategy_version_error_message("zzz-sv-1");
        assert!(message.contains("zzz-sv-1"));
        assert!(message.contains("strategy_version"));
        let message = missing_optimization_data_version_error_message("zzz-dv-1");
        assert!(message.contains("zzz-dv-1"));
        assert!(message.contains("data_version"));
    }

    // ── 纯逻辑：elite 指标合并 ──

    #[test]
    fn metric_has_number_accepts_numbers_and_numeric_strings_only() {
        // value_as_f64 同时接受 Number 与可解析字符串——字符串数字也算已存在
        let metrics = json!({
            "sharpe_ratio": 1.2,
            "annual_return_pct": "15",
            "sortino_ratio": null,
            "calmar_ratio": "abc"
        });
        assert!(metric_has_number(&metrics, "sharpe_ratio"));
        assert!(metric_has_number(&metrics, "annual_return_pct"));
        assert!(!metric_has_number(&metrics, "sortino_ratio"));
        assert!(!metric_has_number(&metrics, "calmar_ratio"));
        assert!(!metric_has_number(&metrics, "missing_key"));
        // existing_metric_sources 只登记 elite 键域内已存在的
        let sources = existing_metric_sources(&metrics);
        assert_eq!(sources.len(), 2);
        assert_eq!(
            sources.get("sharpe_ratio").and_then(Value::as_str),
            Some("optimization_trial.metrics")
        );
    }

    #[test]
    fn insert_metric_if_missing_skips_present_null_and_unparseable() {
        let mut metrics = json!({"sharpe_ratio": 1.5});
        let mut sources = Map::new();

        // 已存在：不覆盖、不记来源
        insert_metric_if_missing(
            &mut metrics,
            &mut sources,
            "sharpe_ratio",
            Some(json!(9.9)),
            "src",
        );
        assert_eq!(metrics["sharpe_ratio"], json!(1.5));
        assert!(!sources.contains_key("sharpe_ratio"));
        // value=None：跳过
        insert_metric_if_missing(&mut metrics, &mut sources, "sortino_ratio", None, "src");
        assert!(metrics.get("sortino_ratio").is_none());
        // 不可解析为有限数：跳过
        insert_metric_if_missing(
            &mut metrics,
            &mut sources,
            "calmar_ratio",
            Some(json!("not-a-number")),
            "src",
        );
        assert!(metrics.get("calmar_ratio").is_none());
        // 正常：插入并记来源
        insert_metric_if_missing(
            &mut metrics,
            &mut sources,
            "calmar_ratio",
            Some(json!(2.0)),
            "backtest_x",
        );
        assert_eq!(metrics["calmar_ratio"], json!(2.0));
        assert_eq!(
            sources.get("calmar_ratio").and_then(Value::as_str),
            Some("backtest_x")
        );

        // ensure_metrics_object：非对象（数组）重置为空对象后可继续插入
        let mut array_metrics = json!([1, 2]);
        {
            let map = ensure_metrics_object(&mut array_metrics);
            map.insert("num_trades".into(), json!(10));
        }
        assert_eq!(array_metrics, json!({"num_trades": 10}));
    }

    #[test]
    fn missing_elite_metrics_reports_gaps_in_canonical_order() {
        let metrics = json!({"sharpe_ratio": 1.0, "max_drawdown_pct": 0.2});
        let missing = missing_elite_metrics(&metrics);
        assert_eq!(
            missing,
            vec![
                "annual_return_pct",
                "excess_return_pct",
                "sortino_ratio",
                "calmar_ratio",
                "profit_factor",
                "max_drawdown_duration_days",
                "num_trades"
            ]
        );
        // 全齐 → 空清单
        assert!(missing_elite_metrics(&full_elite_metrics(0.2)).is_empty());
    }

    // ── 纯逻辑：elite gap 评分与排序 ──

    #[test]
    fn elite_gap_score_zero_when_profile_meets_all_targets() {
        // 九项目标全部达标（含回撤/持仓时长不越限）→ 缺口为 0
        assert_eq!(elite_gap_score(&full_elite_metrics(0.20)), 0.0);
        // 空指标：各目标缺口按权重累加
        // 0.15*4 + 0 + 1.5*1.5 + 1.8 + 2*0.75 + 1.5*0.75 + 0 + 0/252 + 200/200 = 8.275
        let score = elite_gap_score(&json!({}));
        assert!((score - 8.275).abs() < 1e-9, "实际值 {}", score);
        // 年化缺口线性放大：annual 0.10 → 缺口 0.05*4=0.2
        let score = elite_gap_score(&full_elite_metrics(0.10));
        assert!((score - 0.2).abs() < 1e-9, "实际值 {}", score);
        // 回撤越限：max_drawdown 0.40 超上限 0.35 → (0.40-0.35)*3=0.15
        let mut metrics = full_elite_metrics(0.20);
        metrics["max_drawdown_pct"] = json!(0.40);
        let score = elite_gap_score(&metrics);
        assert!((score - 0.15).abs() < 1e-9, "实际值 {}", score);
    }

    #[test]
    fn elite_trial_report_order_ranks_gap_then_tiebreaks() {
        // 主序：gap 升序（更接近 elite 的排前）
        let strong = snapshot("t-strong", 1, full_elite_metrics(0.20), json!({}));
        let weak = snapshot("t-weak", 2, json!({}), json!({}));
        assert_eq!(
            elite_trial_report_order(&strong, &weak),
            std::cmp::Ordering::Less
        );

        // 同 gap → max_drawdown 小者优先（升序）
        let low_dd = snapshot(
            "t-low-dd",
            3,
            {
                let mut m = full_elite_metrics(0.20);
                m["max_drawdown_pct"] = json!(0.10);
                m["annual_return_pct"] = json!(0.10);
                m
            },
            json!({}),
        );
        let high_dd = snapshot(
            "t-high-dd",
            4,
            {
                let mut m = full_elite_metrics(0.20);
                m["max_drawdown_pct"] = json!(0.25);
                m["annual_return_pct"] = json!(0.10);
                m
            },
            json!({}),
        );
        assert_eq!(
            elite_trial_report_order(&low_dd, &high_dd),
            std::cmp::Ordering::Less
        );

        // drawdown 也同 → sortino 大者优先（降序）
        let a = snapshot(
            "t-a",
            5,
            {
                let mut m = full_elite_metrics(0.20);
                m["annual_return_pct"] = json!(0.10);
                m["sortino_ratio"] = json!(2.5);
                m
            },
            json!({}),
        );
        let b = snapshot(
            "t-b",
            6,
            {
                let mut m = full_elite_metrics(0.20);
                m["annual_return_pct"] = json!(0.10);
                m["sortino_ratio"] = json!(2.1);
                m
            },
            json!({}),
        );
        assert_eq!(elite_trial_report_order(&a, &b), std::cmp::Ordering::Less);

        // 指标全同 → trial_index 升序兜底
        let first = snapshot("t-first", 7, full_elite_metrics(0.20), json!({}));
        let second = snapshot("t-second", 8, full_elite_metrics(0.20), json!({}));
        assert_eq!(
            elite_trial_report_order(&first, &second),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            elite_trial_report_order(&second, &first),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn summarize_elite_validation_rows_counts_status_and_best() {
        let rows = vec![
            json!({"trial_id": "t1", "elite_status": "approved_candidate",
                   "elite_gap_score": 0.0, "metrics": {"sharpe_ratio": 2.0},
                   "missing_elite_metrics": []}),
            json!({"trial_id": "t2", "elite_status": "review_required",
                   "elite_gap_score": 0.5, "metrics": {"sharpe_ratio": 1.0},
                   "missing_elite_metrics": ["profit_factor"]}),
            json!({"trial_id": "t3", "elite_status": "rejected",
                   "elite_gap_score": 0.9, "metrics": {},
                   "missing_elite_metrics": ["profit_factor", "num_trades"]}),
        ];
        let summary = summarize_elite_validation_rows(&rows);
        assert_eq!(summary["approved_count"], json!(1));
        assert_eq!(summary["review_required_count"], json!(1));
        assert_eq!(summary["rejected_count"], json!(1));
        assert_eq!(summary["best_trial_id"], json!("t1"));
        assert_eq!(summary["best_elite_status"], json!("approved_candidate"));
        assert_eq!(summary["best_elite_gap_score"], json!(0.0));
        // 缺失指标按 key 聚合计数
        assert_eq!(
            summary["missing_elite_metric_counts"],
            json!({"num_trades": 1, "profit_factor": 2})
        );
        // 空清单 → best 字段为空对象取值（不 panic）
        let empty = summarize_elite_validation_rows(&[]);
        assert_eq!(empty["approved_count"], json!(0));
        assert_eq!(empty["best_trial_id"], json!(null));
    }

    #[test]
    fn summarize_missing_elite_metric_counts_ignores_non_arrays() {
        let rows = vec![
            json!({"missing_elite_metrics": ["a", "b"]}),
            json!({"missing_elite_metrics": ["a"]}),
            json!({"missing_elite_metrics": "not-an-array"}),
            json!({}),
        ];
        let counts = summarize_missing_elite_metric_counts(&rows);
        assert_eq!(counts, json!({"a": 2, "b": 1}));
    }

    // ── 纯逻辑：参数平台分析 ──

    #[test]
    fn build_parameter_plateau_analysis_buckets_neighbors() {
        let candidate = snapshot(
            "cand",
            1,
            full_elite_metrics(0.20),
            json!({
                "x": 1, "y": 2, "z": 3
            }),
        );
        // peer_stable：单参数差异 + 指标在容忍带内 → 近邻且稳定
        let peer_stable = snapshot(
            "p-stable",
            2,
            full_elite_metrics(0.195),
            json!({
                "x": 5, "y": 2, "z": 3
            }),
        );
        // peer_unstable：单参数差异但年化掉出 -1pp 带宽 → 近邻不稳定
        let peer_unstable = snapshot(
            "p-unstable",
            3,
            full_elite_metrics(0.10),
            json!({
                "x": 6, "y": 2, "z": 3
            }),
        );
        // peer_far：4 个参数差异 → 不算近邻也不进单轴
        let peer_far = snapshot(
            "p-far",
            4,
            full_elite_metrics(0.20),
            json!({
                "x": 9, "y": 8, "z": 7, "w": 4
            }),
        );
        // peer_same：参数完全一致 → 直接跳过
        let peer_same = snapshot(
            "p-same",
            5,
            full_elite_metrics(0.20),
            json!({
                "x": 1, "y": 2, "z": 3
            }),
        );

        let analysis = build_parameter_plateau_analysis(
            &candidate,
            &[peer_stable, peer_unstable, peer_far, peer_same],
        );
        assert_eq!(analysis["near_neighbor_count"], json!(2));
        assert_eq!(analysis["stable_neighbor_count"], json!(1));
        assert!((analysis["plateau_score"].as_f64().unwrap() - 0.5).abs() < 1e-9);
        // 单轴 peers 只收单参数差异：x 轴 2 个（stable + unstable），far 不进任何轴
        let axes = analysis["single_axis"]
            .as_array()
            .expect("single_axis 数组");
        assert_eq!(axes.len(), 1);
        assert_eq!(axes[0]["parameter"], json!("x"));
        assert_eq!(axes[0]["peer_count"], json!(2));
        assert_eq!(axes[0]["stable_peer_count"], json!(1));
        assert_eq!(axes[0]["candidate_value"], json!(1));
        assert_eq!(axes[0]["peer_values"], json!([5, 6]));
    }

    #[test]
    fn parameter_axis_summary_dedups_values_and_reports_best_peer() {
        let candidate = snapshot("cand", 1, full_elite_metrics(0.20), json!({"x": 1}));
        // 两个 peer 同值 5（去重后 1 个）；best peer 按 elite_trial_report_order 取 gap 最小者
        let peer_best = snapshot("p-best", 2, full_elite_metrics(0.20), json!({"x": 5}));
        let peer_worse = snapshot("p-worse", 3, full_elite_metrics(0.05), json!({"x": 5}));

        let summary = parameter_axis_plateau_summary("x", &candidate, &[&peer_worse, &peer_best]);
        assert_eq!(summary["peer_count"], json!(2));
        assert_eq!(summary["peer_values"], json!([5]));
        assert_eq!(summary["best_peer"]["trial_id"], json!("p-best"));
        // peer_best 指标持平稳定、peer_worse 年化掉出带宽不稳定 → 1/2
        assert!((summary["stable_ratio"].as_f64().unwrap() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn is_plateau_stable_neighbor_boundary_tolerances() {
        // 实现容差：annual -0.01 / sharpe -0.10 / sortino -0.15 / mdd +0.03，全部 >=（含压线）
        let cand = EliteMetricProfile::from_metrics(&json!({
            "annual_return_pct": 20.0, "sharpe_ratio": 1.50,
            "sortino_ratio": 2.00, "max_drawdown_pct": 10.0
        }));
        // 压线全中 → 稳定。量纲实证：annual/mdd 容差是**绝对值**（0.01/0.03）而
        // metric_number 直取百分比数值——20.0-0.01=19.99 才是压线（19.0 是远低于线）
        let boundary = json!({"annual_return_pct": 19.99, "sharpe_ratio": 1.40,
                              "sortino_ratio": 1.85, "max_drawdown_pct": 10.03});
        assert!(
            is_plateau_stable_neighbor(cand, &boundary),
            "压线邻居应判稳定（>= 含等号）"
        );
        // 任一维度越线 → 不稳定
        let over_annual = json!({"annual_return_pct": 19.98, "sharpe_ratio": 1.40,
                                 "sortino_ratio": 1.85, "max_drawdown_pct": 10.03});
        assert!(
            !is_plateau_stable_neighbor(cand, &over_annual),
            "annual 越线应不稳定"
        );
        let over_mdd = json!({"annual_return_pct": 19.99, "sharpe_ratio": 1.40,
                              "sortino_ratio": 1.85, "max_drawdown_pct": 10.04});
        assert!(
            !is_plateau_stable_neighbor(cand, &over_mdd),
            "mdd 越线应不稳定"
        );
    }

    // ── 纯逻辑：相关性控制 ──

    #[test]
    fn active_correlation_controls_filters_inactive_switches() {
        // off / immediate / expire 等关闭态不出现，开启态全部列出
        let parameters = json!({
            "max_pairwise_correlation": 0.68,
            "correlation_lookback_days": 120,
            "candidate_risk_filter": "correlation_robust",
            "portfolio_method": "min_variance",
            "risk_contribution_control": "off",
            "style_risk_budget": "style_v1",
            "capacity_risk_budget": "off",
            "execution_impact_budget": "impact_v1",
            "execution_schedule_profile": "immediate",
            "execution_carry_policy": "expire_v1",
            "execution_daily_target_move_limit_pct": "0.05",
            "execution_max_carry_days": 5
        });
        let controls = active_correlation_controls(&parameters);
        let names: Vec<&str> = controls
            .iter()
            .filter_map(|control| control["name"].as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "max_pairwise_correlation",
                "correlation_lookback_days",
                "candidate_risk_filter",
                "portfolio_method",
                "style_risk_budget",
                "execution_impact_budget",
                "execution_daily_target_move_limit_pct",
                "execution_max_carry_days"
            ]
        );
        // 不含 correlation 的 risk_filter 与 off 态 risk_contribution_control 均排除
        let filtered =
            json!({"candidate_risk_filter": "vol_only", "risk_contribution_control": "off"});
        assert!(active_correlation_controls(&filtered).is_empty());
        // vwap 调度与 roll 展期属于开启态
        let sched = json!({"execution_schedule_profile": "vwap", "execution_carry_policy": "roll"});
        assert_eq!(active_correlation_controls(&sched).len(), 2);
    }

    #[test]
    fn correlation_control_score_segments_and_clamps() {
        // max_pairwise_correlation 阈值分段：<=0.68 满档 0.35；0.70→0.30；0.75→0.20；更高 0.10
        assert!(
            (correlation_control_score(&json!({"max_pairwise_correlation": 0.68})) - 0.35).abs()
                < 1e-9
        );
        assert!(
            (correlation_control_score(&json!({"max_pairwise_correlation": 0.70})) - 0.30).abs()
                < 1e-9
        );
        assert!(
            (correlation_control_score(&json!({"max_pairwise_correlation": 0.75})) - 0.20).abs()
                < 1e-9
        );
        assert!(
            (correlation_control_score(&json!({"max_pairwise_correlation": 0.80})) - 0.10).abs()
                < 1e-9
        );
        // 回看窗口 >=120 天 0.10，否则 0.05
        assert!(
            (correlation_control_score(&json!({"correlation_lookback_days": 120})) - 0.10).abs()
                < 1e-9
        );
        assert!(
            (correlation_control_score(&json!({"correlation_lookback_days": 60})) - 0.05).abs()
                < 1e-9
        );
        // 开关叠加：filter 0.25 + portfolio 0.15 + risk_control 0.15 = 0.55
        let combined = json!({
            "candidate_risk_filter": "correlation_x",
            "portfolio_method": "risk_budget",
            "risk_contribution_control": "on"
        });
        assert!((correlation_control_score(&combined) - 0.55).abs() < 1e-9);
        // 全开叠加封顶 1.0
        let everything = json!({
            "max_pairwise_correlation": 0.60,
            "correlation_lookback_days": 250,
            "candidate_risk_filter": "correlation",
            "portfolio_method": "risk_budget",
            "risk_contribution_control": "on",
            "style_risk_budget": "style",
            "capacity_risk_budget": "cap",
            "execution_impact_budget": "impact",
            "execution_schedule_profile": "vwap",
            "execution_daily_target_move_limit_pct": "0.05",
            "execution_max_carry_days": 3
        });
        assert!((correlation_control_score(&everything) - 1.0).abs() < 1e-9);
        // 空参数零分
        assert_eq!(correlation_control_score(&json!({})), 0.0);
    }

    #[test]
    fn summarize_trial_metric_group_empty_and_averages() {
        // 空组：count 0 + 全 null 均值（UI 可区分无数据与零值）
        let empty = summarize_trial_metric_group(&[]);
        assert_eq!(empty["count"], json!(0));
        assert_eq!(empty["annual_return_avg"], json!(null));
        assert_eq!(empty["sharpe_avg"], json!(null));
        // 两组均值
        let group = [
            snapshot("t1", 1, full_elite_metrics(0.10), json!({})),
            snapshot("t2", 2, full_elite_metrics(0.30), json!({})),
        ];
        let refs: Vec<&CompletedTrialSnapshot> = group.iter().collect();
        let summary = summarize_trial_metric_group(&refs);
        assert_eq!(summary["count"], json!(2));
        assert!((summary["annual_return_avg"].as_f64().unwrap() - 0.20).abs() < 1e-9);
        assert!((summary["sharpe_avg"].as_f64().unwrap() - 2.0).abs() < 1e-9);
        assert!((summary["max_drawdown_avg"].as_f64().unwrap() - 0.30).abs() < 1e-9);
    }

    #[test]
    fn correlation_peer_impact_insufficient_and_deltas() {
        // 任一侧无 peer → 0 分 + 显式 reason
        let insufficient = correlation_peer_impact(&json!({"count": 0}), &json!({"count": 3}));
        assert_eq!(insufficient["normalized_score"], json!(0.0));
        assert_eq!(
            insufficient["reason"],
            json!("insufficient_controlled_or_uncontrolled_peers")
        );
        // 正常：0.5 基线 + sharpe 差 0.5*0.5 + 回撤改善 0.1*0.8 + 年化差 0.04*0.2 = 0.838
        let controlled = json!({
            "count": 2, "sharpe_avg": 1.5, "max_drawdown_avg": 0.2, "annual_return_avg": 0.14
        });
        let uncontrolled = json!({
            "count": 3, "sharpe_avg": 1.0, "max_drawdown_avg": 0.3, "annual_return_avg": 0.10
        });
        let impact = correlation_peer_impact(&controlled, &uncontrolled);
        assert!((impact["normalized_score"].as_f64().unwrap() - 0.838).abs() < 1e-9);
        assert_eq!(impact["sharpe_delta"], json!(0.5));
        assert!((impact["max_drawdown_delta"].as_f64().unwrap() + 0.1).abs() < 1e-9);
        // 年化大幅落后时下限 -0.05：0.5 + 0 + 0 + (-0.05*0.2) = 0.49
        let lagging = json!({
            "count": 2, "sharpe_avg": 1.0, "max_drawdown_avg": 0.3, "annual_return_avg": -0.5
        });
        let flat = json!({
            "count": 2, "sharpe_avg": 1.0, "max_drawdown_avg": 0.3, "annual_return_avg": 0.0
        });
        let impact = correlation_peer_impact(&lagging, &flat);
        assert!((impact["normalized_score"].as_f64().unwrap() - 0.49).abs() < 1e-9);
    }

    #[test]
    fn build_portfolio_correlation_contribution_score_blends() {
        // candidate 带显式控制 0.35；同侪两侧各有成员 → 0.35*0.7 + peer*0.3
        let candidate = snapshot(
            "cand",
            1,
            full_elite_metrics(0.20),
            json!({
                "max_pairwise_correlation": 0.68
            }),
        );
        let controlled = snapshot(
            "ctl",
            2,
            full_elite_metrics(0.20),
            json!({
                "max_pairwise_correlation": 0.68
            }),
        );
        let uncontrolled = snapshot("unctl", 3, full_elite_metrics(0.05), json!({}));

        let score = build_portfolio_correlation_contribution_score(
            &candidate,
            &[controlled.clone(), uncontrolled.clone()],
        );
        // peer_impact：sharpe/dd 两侧持平（delta 0），年化差 0.15*0.2=0.03 → 0.53；
        // 贡献 = 0.35*0.7 + 0.53*0.3 = 0.404
        assert!(
            (score["contribution_score"].as_f64().unwrap() - 0.404).abs() < 1e-9,
            "实际值 {}",
            score["contribution_score"]
        );
        assert_eq!(score["explicit_control_score"], json!(0.35));
        assert_eq!(score["controlled_peer_summary"]["count"], json!(1));
        assert_eq!(score["uncontrolled_peer_summary"]["count"], json!(1));
        // 无任何控制且无对照 → 显式分 0，peer 分 0
        let plain = snapshot("plain", 4, full_elite_metrics(0.20), json!({}));
        let score =
            build_portfolio_correlation_contribution_score(&plain, std::slice::from_ref(&plain));
        assert_eq!(score["explicit_control_score"], json!(0.0));
        assert_eq!(score["contribution_score"], json!(0.0));
        assert_eq!(
            score["peer_impact"]["reason"],
            json!("insufficient_controlled_or_uncontrolled_peers")
        );
    }

    // ── 纯逻辑：参数访问与杂项 ──

    #[test]
    fn parameter_accessors_extract_typed_values() {
        let parameters = json!({
            "f": 1.5, "i": 7, "s": "on", "snum": "2.5", "null": null
        });
        assert_eq!(parameter_f64(&parameters, "f"), Some(1.5));
        assert_eq!(parameter_f64(&parameters, "snum"), Some(2.5));
        assert_eq!(parameter_f64(&parameters, "s"), None);
        assert_eq!(parameter_f64(&parameters, "missing"), None);
        assert_eq!(parameter_i64(&parameters, "i"), Some(7));
        assert_eq!(parameter_i64(&parameters, "f"), None);
        assert_eq!(parameter_str(&parameters, "s"), Some("on"));
        assert_eq!(parameter_str(&parameters, "f"), None);
        assert_eq!(parameter_value(&parameters, "i"), json!(7));
        assert_eq!(parameter_value(&parameters, "missing"), json!(null));
    }

    #[test]
    fn differing_parameter_keys_unions_both_sides() {
        // 双侧键并集内取值不同的键；相等值（含双侧均缺失）不列
        let left = json!({"a": 1, "b": 2, "c": 3});
        let right = json!({"b": 2, "c": 4, "d": 5});
        assert_eq!(
            differing_parameter_keys(&left, &right),
            vec!["a".to_string(), "c".to_string(), "d".to_string()]
        );
        assert!(differing_parameter_keys(&left, &left).is_empty());
        // 非对象 → 空差集
        assert!(differing_parameter_keys(&json!([1]), &right).is_empty());
        assert!(differing_parameter_keys(&left, &json!("x")).is_empty());
    }

    #[test]
    fn compact_trial_metrics_reports_core_fields() {
        let trial = snapshot("t1", 3, full_elite_metrics(0.20), json!({}));
        let compact = compact_trial_metrics(&trial);
        assert_eq!(compact["trial_id"], json!("t1"));
        assert_eq!(compact["trial_index"], json!(3));
        assert_eq!(compact["elite_gap_score"], json!(0.0));
        assert_eq!(compact["annual_return_pct"], json!(0.20));
        assert_eq!(compact["sharpe_ratio"], json!(2.0));
        assert_eq!(compact["max_drawdown_pct"], json!(0.30));
    }

    #[test]
    fn metric_number_defaults_zero_and_ratio_guards_denominator() {
        assert_eq!(metric_number(&json!({"x": 2.5}), "x"), 2.5);
        assert_eq!(metric_number(&json!({}), "x"), 0.0);
        assert_eq!(metric_number(&json!({"x": "abc"}), "x"), 0.0);
        assert_eq!(ratio(0, 0), 0.0);
        assert_eq!(ratio(3, 0), 0.0);
        assert!((ratio(3, 4) - 0.75).abs() < 1e-9);
    }

    #[test]
    fn normalize_promote_request_defaults_trims_and_validates() {
        let base = || PromoteOptimizationRequest {
            trial_id: Some("  ".into()),
            target_strategy_version: " sv-v2 ".into(),
            candidate_name: " cand ".into(),
            promotion_mode: Some("  ".into()),
            gate_policy: Some(" gp-v2 ".into()),
            freeze_after_approval: None,
            reviewer: Some("  ".into()),
            reason: " promote because ".into(),
            notes: Some("  ".into()),
        };
        // 默认与 trim：trial_id 回退 default、promotion_mode/gate_policy 取默认、空白可选字段清成 None
        let normalized = normalize_promote_request("trial-default", base()).expect("normalized");
        assert_eq!(normalized.trial_id, "trial-default");
        assert_eq!(normalized.target_strategy_version, "sv-v2");
        assert_eq!(normalized.candidate_name, "cand");
        assert_eq!(normalized.reason, "promote because");
        assert_eq!(normalized.promotion_mode, "create_candidate");
        // 实现：gate_policy trim 后非空则透传（" gp-v2 " → "gp-v2"），空白才回退 default_v1
        assert_eq!(normalized.gate_policy, "gp-v2");
        assert!(!normalized.freeze_after_approval);
        assert!(normalized.reviewer.is_none());
        assert!(normalized.notes.is_none());
        assert_eq!(normalized.status, "candidate");

        // 显式值透传
        let mut req = base();
        req.trial_id = Some(" t9 ".into());
        req.freeze_after_approval = Some(true);
        req.reviewer = Some(" alice ".into());
        req.notes = Some(" note ".into());
        let normalized = normalize_promote_request("trial-default", req).expect("normalized");
        assert_eq!(normalized.trial_id, "t9");
        assert!(normalized.freeze_after_approval);
        assert_eq!(normalized.reviewer.as_deref(), Some("alice"));
        assert_eq!(normalized.notes.as_deref(), Some("note"));

        // 三项必填逐个为空 → Err
        let mut req = base();
        req.target_strategy_version = "  ".into();
        assert!(normalize_promote_request("t", req).is_err());
        let mut req = base();
        req.candidate_name = "".into();
        assert!(normalize_promote_request("t", req).is_err());
        let mut req = base();
        req.reason = "".into();
        assert!(normalize_promote_request("t", req).is_err());
        // 非法 promotion_mode → Err 带模式名
        let mut req = base();
        req.promotion_mode = Some("deploy".into());
        let err = normalize_promote_request("t", req).map(|_| ()).unwrap_err();
        assert!(err.contains("deploy"), "实际错误 {}", err);
    }

    // ── DB：引用存在性校验 ──

    #[tokio::test]
    async fn ensure_phase7_oos_references_exist_validates_both_refs() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let make_req = |sv: &str, dv: &str| {
            serde_json::from_value::<Phase7OosWalkForwardDiscoveryRequest>(json!({
                "strategy_version_id": sv,
                "data_version_id": dv
            }))
            .expect("反序列化 Phase7 请求")
        };

        // 双引用齐备 → Ok
        ensure_phase7_oos_request_references_exist(&db, &make_req("zzz_api9x_sv", "zzz_api9x_dv"))
            .await
            .expect("引用齐备应通过");
        // 缺 strategy_version → 错误消息指向 strategy_version
        let err = ensure_phase7_oos_request_references_exist(
            &db,
            &make_req("zzz_api9x_missing_sv", "zzz_api9x_dv"),
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(err.contains("strategy_version"), "实际错误 {}", err);
        // 缺 data_version → 错误消息指向 data_version
        let err = ensure_phase7_oos_request_references_exist(
            &db,
            &make_req("zzz_api9x_sv", "zzz_api9x_missing_dv"),
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(err.contains("data_version"), "实际错误 {}", err);
    }

    // ── DB：cleanup stale ──

    #[tokio::test]
    async fn cleanup_stale_optimization_tasks_dry_run_then_apply() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let scope = "task_stale";
        cleanup_scoped(&db, scope).await;

        let task_id = "zzz_api9x_task_stale_t1";
        let fresh_task_id = "zzz_api9x_task_stale_fresh";
        insert_zzz_task(&db, task_id, "running", 30).await;
        insert_zzz_task(&db, fresh_task_id, "running", 0).await;
        insert_zzz_trial(
            &db,
            &format!("{task_id}-p001"),
            task_id,
            1,
            "pending",
            None,
            None,
            None,
        )
        .await;
        insert_zzz_trial(
            &db,
            &format!("{task_id}-r002"),
            task_id,
            2,
            "running",
            None,
            None,
            None,
        )
        .await;

        let req = CleanupStaleBackgroundTasksReq {
            dry_run: Some(true),
            default_timeout_seconds: Some(60),
            limit: Some(1000),
        };
        let report = cleanup_stale_optimization_tasks_inner(&db, req)
            .await
            .expect("dry run 报告");
        // 全表扫描：候选必须全部是本测试 zzz 键（防误伤真实数据的结构性断言）
        let candidates = report["candidates"].as_array().expect("candidates");
        assert!(!candidates.is_empty(), "超时 zzz 任务必须被扫出");
        for candidate in candidates {
            let id = candidate["optimization_task_id"].as_str().expect("task id");
            assert!(
                id.starts_with("zzz_api9x_"),
                "候选混入非 zzz 任务 {}，中止以免误更新",
                id
            );
        }
        assert_eq!(report["dry_run"], json!(true));
        assert_eq!(report["updated_task_count"], json!(0));
        // running 任务映射到 timeout
        let stale_candidate = candidates
            .iter()
            .find(|c| c["optimization_task_id"] == json!(task_id))
            .expect("zzz 超时任务在候选中");
        assert_eq!(stale_candidate["next_status"], json!("timeout"));
        // dry run 不落库：任务状态原样
        let status: String = sqlx::query_scalar(
            "SELECT status FROM optimization_task WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&db)
        .await
        .expect("task row");
        assert_eq!(status, "running");

        // apply：超时任务 → timeout；pending trial → cancelled、running trial → timeout
        let req = CleanupStaleBackgroundTasksReq {
            dry_run: Some(false),
            default_timeout_seconds: Some(60),
            limit: Some(1000),
        };
        let report = cleanup_stale_optimization_tasks_inner(&db, req)
            .await
            .expect("apply 报告");
        assert_eq!(report["dry_run"], json!(false));
        assert!(report["updated_task_count"].as_i64().unwrap() >= 1);
        // 并行宽限：候选查询按心跳超时扫全库 running/pending，A 线并行测试的
        // zzz running 任务可能同时进入候选集（updated_trial_count 3>2 实锤）——
        // 断言本测试自己的两条已被迁移即可，不锁全局精确值
        assert!(report["updated_trial_count"].as_i64().unwrap() >= 2);

        let (status, progress): (String, i32) = sqlx::query_as(
            "SELECT status, progress FROM optimization_task WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&db)
        .await
        .expect("task row");
        assert_eq!(status, "timeout");
        assert_eq!(progress, 100);

        let (pending_status, pending_error): (String, Option<String>) = sqlx::query_as(
            "SELECT status, error_message FROM optimization_trial WHERE trial_id = $1",
        )
        .bind(format!("{task_id}-p001"))
        .fetch_one(&db)
        .await
        .expect("pending trial row");
        assert_eq!(pending_status, "cancelled");
        assert!(
            pending_error
                .as_deref()
                .unwrap_or_default()
                .contains("stale optimization task cleaned up"),
            "审计文案缺失：{:?}",
            pending_error
        );

        let running_status: String =
            sqlx::query_scalar("SELECT status FROM optimization_trial WHERE trial_id = $1")
                .bind(format!("{task_id}-r002"))
                .fetch_one(&db)
                .await
                .expect("running trial row");
        assert_eq!(running_status, "timeout");

        // 心跳新鲜的 task 不被触碰
        let fresh_status: String = sqlx::query_scalar(
            "SELECT status FROM optimization_task WHERE optimization_task_id = $1",
        )
        .bind(fresh_task_id)
        .fetch_one(&db)
        .await
        .expect("fresh task row");
        assert_eq!(fresh_status, "running");

        cleanup_scoped(&db, scope).await;
    }

    #[tokio::test]
    async fn cleanup_stale_experiment_runs_dry_run_then_apply() {
        let db = test_db().await;
        let scope = "exp_stale";
        cleanup_scoped(&db, scope).await;

        let old_id = "zzz_api9x_exp_stale_old";
        let done_id = "zzz_api9x_exp_stale_done";
        // running + started_at 30 天前 → 超时候选
        sqlx::query(
            "INSERT INTO experiment_run
               (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                config, metrics, status, started_at, created_at)
             VALUES ($1, 'zzz_api9x_cleanup', 'zzz', $1, '{}', '{}', 'running',
                     now() - interval '30 days', now() - interval '30 days')",
        )
        .bind(old_id)
        .execute(&db)
        .await
        .expect("insert 超时 experiment_run");
        // completed 状态即使很老也不进候选
        sqlx::query(
            "INSERT INTO experiment_run
               (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
                config, metrics, status, started_at, completed_at, created_at)
             VALUES ($1, 'zzz_api9x_cleanup', 'zzz', $1, '{}', '{}', 'completed',
                     now() - interval '30 days', now() - interval '29 days',
                     now() - interval '30 days')",
        )
        .bind(done_id)
        .execute(&db)
        .await
        .expect("insert 已完成 experiment_run");

        let req = CleanupStaleBackgroundTasksReq {
            dry_run: Some(true),
            default_timeout_seconds: Some(60),
            limit: Some(1000),
        };
        let report = cleanup_stale_experiment_runs_inner(&db, req)
            .await
            .expect("dry run 报告");
        let candidates = report["candidates"].as_array().expect("candidates");
        for candidate in candidates {
            let id = candidate["experiment_run_id"].as_str().expect("run id");
            assert!(
                id.starts_with("zzz_api9x_"),
                "候选混入非 zzz 记录 {}，中止以免误更新",
                id
            );
        }
        let old_candidate = candidates
            .iter()
            .find(|c| c["experiment_run_id"] == json!(old_id))
            .expect("超时 zzz 记录在候选中");
        assert_eq!(old_candidate["next_status"], json!("failed"));
        assert!(!candidates
            .iter()
            .any(|c| c["experiment_run_id"] == json!(done_id)));
        assert_eq!(report["updated_experiment_count"], json!(0));

        // apply：failed + completed_at 落库 + metrics 写 stale_cleanup 审计
        let req = CleanupStaleBackgroundTasksReq {
            dry_run: Some(false),
            default_timeout_seconds: Some(60),
            limit: Some(1000),
        };
        let report = cleanup_stale_experiment_runs_inner(&db, req)
            .await
            .expect("apply 报告");
        assert_eq!(report["updated_experiment_count"], json!(1));

        let (status, completed_at, metrics): (
            String,
            Option<chrono::DateTime<chrono::Utc>>,
            Value,
        ) = sqlx::query_as(
            "SELECT status, completed_at, COALESCE(metrics, '{}'::jsonb) \
             FROM experiment_run WHERE experiment_run_id = $1",
        )
        .bind(old_id)
        .fetch_one(&db)
        .await
        .expect("回读超时记录");
        assert_eq!(status, "failed");
        assert!(completed_at.is_some(), "清理后 completed_at 必须落库");
        assert_eq!(metrics["status"], json!("failed"));
        assert!(metrics["stale_cleanup"]["cleaned_at"].is_string());
        assert_eq!(metrics["stale_cleanup"]["timeout_seconds"], json!(60));

        cleanup_scoped(&db, scope).await;
    }

    // ── DB：trial 生命周期与进度刷新 ──

    #[tokio::test]
    async fn trial_lifecycle_marking_and_task_progress_refresh() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let scope = "lifecycle";
        cleanup_scoped(&db, scope).await;

        let task_id = "zzz_api9x_lifecycle_t1";
        insert_zzz_task(&db, task_id, "running", 0).await;
        let bt_task = "zzz_api9x_lifecycle_bt1";
        insert_zzz_backtest_task(&db, bt_task).await;

        // running 标记：pending → running，progress=10，清 error
        let t1 = format!("{task_id}-0001");
        insert_zzz_trial(&db, &t1, task_id, 1, "pending", None, None, None).await;
        mark_trial_running(&db, &t1).await.expect("mark running");
        let (status, progress, started, _error): (
            String,
            i32,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT status, progress, started_at, error_message \
             FROM optimization_trial WHERE trial_id = $1",
        )
        .bind(&t1)
        .fetch_one(&db)
        .await
        .expect("trial row");
        assert_eq!(status, "running");
        assert_eq!(progress, 10);
        assert!(started.is_some());

        // completed 标记：写入 score/metrics/backtest_task_id
        mark_trial_completed(
            &db,
            &t1,
            bt_task,
            &ScoredTrial {
                score: Decimal::new(25, 1),
                metrics: json!({"sharpe_ratio": 2.5}),
                constraint_violations: json!([]),
            },
        )
        .await
        .expect("mark completed");
        let (status, score, metrics, linked_bt): (
            String,
            Option<Decimal>,
            Option<Value>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT status, score, metrics, backtest_task_id \
                 FROM optimization_trial WHERE trial_id = $1",
        )
        .bind(&t1)
        .fetch_one(&db)
        .await
        .expect("trial row");
        assert_eq!(status, "completed");
        assert_eq!(score, Some(Decimal::new(25, 1)));
        assert_eq!(
            metrics.as_ref().and_then(|m| m["sharpe_ratio"].as_f64()),
            Some(2.5)
        );
        assert_eq!(linked_bt.as_deref(), Some(bt_task));

        // failed 标记：写 error_message
        let t2 = format!("{task_id}-0002");
        insert_zzz_trial(&db, &t2, task_id, 2, "pending", None, None, None).await;
        mark_trial_failed(&db, &t2, "zzz 第九批失败注入")
            .await
            .expect("mark failed");
        let (status, error): (String, Option<String>) = sqlx::query_as(
            "SELECT status, error_message FROM optimization_trial WHERE trial_id = $1",
        )
        .bind(&t2)
        .fetch_one(&db)
        .await
        .expect("trial row");
        assert_eq!(status, "failed");
        assert_eq!(error.as_deref(), Some("zzz 第九批失败注入"));

        // 进度刷新：1 完成 + 1 失败 + 1 pending → partial，best 指向唯一完成的高分 trial
        let t3 = format!("{task_id}-0003");
        insert_zzz_trial(
            &db,
            &t3,
            task_id,
            3,
            "completed",
            Some(Decimal::new(15, 1)),
            Some(json!({"sharpe_ratio": 1.5})),
            None,
        )
        .await;
        let t4 = format!("{task_id}-0004");
        insert_zzz_trial(&db, &t4, task_id, 4, "pending", None, None, None).await;
        // best 取 score 最高者：t1 得分 2.5 > t3 得分 1.5
        let best = refresh_task_progress(&db, task_id)
            .await
            .expect("refresh partial");
        assert_eq!(best.as_deref(), Some(t1.as_str()));
        let (status, progress, best_id): (String, i32, Option<String>) = sqlx::query_as(
            "SELECT status, progress, best_trial_id FROM optimization_task WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&db)
        .await
        .expect("task row");
        assert_eq!(status, "partial");
        assert_eq!(progress, 75);
        assert_eq!(best_id.as_deref(), Some(t1.as_str()));

        // 全部收尾 → completed（有完成者）
        mark_trial_failed(&db, &t4, "zzz 第九批收尾")
            .await
            .expect("mark failed");
        let best = refresh_task_progress(&db, task_id)
            .await
            .expect("refresh completed");
        assert_eq!(best.as_deref(), Some(t1.as_str()));
        let status: String = sqlx::query_scalar(
            "SELECT status FROM optimization_task WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&db)
        .await
        .expect("task row");
        assert_eq!(status, "completed");

        // 全失败零完成 → failed、无 best
        let task2 = "zzz_api9x_lifecycle_t2";
        insert_zzz_task(&db, task2, "running", 0).await;
        let f1 = format!("{task2}-0001");
        insert_zzz_trial(&db, &f1, task2, 1, "failed", None, None, None).await;
        let best = refresh_task_progress(&db, task2)
            .await
            .expect("refresh failed");
        assert!(best.is_none());
        let (status, progress): (String, i32) = sqlx::query_as(
            "SELECT status, progress FROM optimization_task WHERE optimization_task_id = $1",
        )
        .bind(task2)
        .fetch_one(&db)
        .await
        .expect("task row");
        assert_eq!(status, "failed");
        assert_eq!(progress, 100);

        cleanup_scoped(&db, scope).await;
    }

    // ── DB：执行上下文 ──

    #[tokio::test]
    async fn load_execution_context_returns_columns_or_missing() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let scope = "ctx";
        cleanup_scoped(&db, scope).await;

        let task_id = "zzz_api9x_ctx_t1";
        sqlx::query(
            "INSERT INTO optimization_task
               (optimization_task_id, strategy_version_id, data_version_id, search_method,
                search_space, objective, status, progress, backtest_template, constraints)
             VALUES ($1, 'zzz_api9x_sv', 'zzz_api9x_dv', 'grid_search', '{}',
                     '{\"goal\": \"sharpe\"}', 'running', 0,
                     '{\"rebalance\": \"monthly\"}', '{\"max_dd\": 0.3}')",
        )
        .bind(task_id)
        .execute(&db)
        .await
        .expect("insert zzz optimization_task");

        let ctx = load_execution_context(&db, task_id).await.expect("context");
        assert_eq!(ctx.strategy_version_id, "zzz_api9x_sv");
        assert_eq!(ctx.data_version_id, "zzz_api9x_dv");
        assert_eq!(ctx.objective, json!({"goal": "sharpe"}));
        assert_eq!(ctx.backtest_template, json!({"rebalance": "monthly"}));
        assert_eq!(ctx.constraints, Some(json!({"max_dd": 0.3})));

        // 模板缺失 → 兜底空对象；不存在 → Err
        let bare = "zzz_api9x_ctx_t2";
        insert_zzz_task(&db, bare, "running", 0).await;
        let ctx = load_execution_context(&db, bare).await.expect("context");
        assert_eq!(ctx.backtest_template, json!({}));
        assert_eq!(ctx.constraints, Some(json!({}))); // insert_zzz_task 造数 constraints='{}' 非 NULL

        let err = load_execution_context(&db, "zzz_api9x_ctx_missing")
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err, "optimization task not found");

        cleanup_scoped(&db, scope).await;
    }

    // ── DB：完成 trial 快照 + 指标补全 ──

    #[tokio::test]
    async fn completed_trial_snapshots_enrich_from_backtest_result() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let scope = "snapshot";
        cleanup_scoped(&db, scope).await;

        let task_id = "zzz_api9x_snapshot_t1";
        let bt_task = "zzz_api9x_snapshot_bt1";
        insert_zzz_task(&db, task_id, "running", 0).await;
        insert_zzz_backtest_task(&db, bt_task).await;

        // backtest_result：表列补 sharpe/sortino/mdd/trades；metrics jsonb 补 profit_factor
        sqlx::query(
            "INSERT INTO backtest_result
               (result_id, task_id, sharpe_ratio, sortino_ratio, max_drawdown,
                total_trades, calmar_ratio, metrics, reproducibility_hash)
             VALUES ('zzz_api9x_snapshot_r1', $1, 1.8, 2.0, 0.25, 120, 2.2,
                     '{\"profit_factor\": 1.8}', 'zzz-api9x')",
        )
        .bind(bt_task)
        .execute(&db)
        .await
        .expect("insert zzz backtest_result");

        // equity curve：110 峰 → 100/90/95 连续 4 天低于峰 → 回撤持续 4 天
        for (idx, value) in [
            (1, 110),
            (2, 100),
            (3, 90),
            (4, 95),
            (5, 105),
            (6, 110),
            (7, 120),
        ] {
            sqlx::query(
                "INSERT INTO backtest_equity_curve
                   (task_id, trade_date, portfolio_value, cash)
                 VALUES ($1, $2::date + ($3::int * interval '1 day'), $4, 0)",
            )
            .bind(bt_task)
            .bind("2026-01-05")
            .bind(idx - 1)
            .bind(value)
            .execute(&db)
            .await
            .expect("insert zzz equity curve");
        }

        // completed trial：自带 annual_return_pct，其余从 backtest_result/equity 补
        let trial_id = format!("{task_id}-0001");
        insert_zzz_trial(
            &db,
            &trial_id,
            task_id,
            1,
            "completed",
            Some(Decimal::ONE),
            Some(json!({"annual_return_pct": 0.2})),
            Some(bt_task),
        )
        .await;
        // pending trial：不入快照
        insert_zzz_trial(
            &db,
            &format!("{task_id}-0002"),
            task_id,
            2,
            "pending",
            None,
            None,
            None,
        )
        .await;

        let snapshots = load_completed_trial_snapshots(&db, task_id)
            .await
            .expect("snapshots");
        assert_eq!(
            snapshots.len(),
            1,
            "只有 completed 且带 metrics 的 trial 入快照"
        );
        let snap = &snapshots[0];
        assert_eq!(snap.trial_id, trial_id);
        assert_eq!(
            snap.metrics["annual_return_pct"],
            json!(0.2),
            "已有指标不被覆盖"
        );
        // NUMERIC 表列经 sqlx → Decimal → serde 字符串（非 JSON number）；jsonb 路径才是 number
        assert_eq!(
            snap.metrics["sharpe_ratio"],
            json!("1.8000000000"),
            "从表列补全"
        );
        assert_eq!(
            snap.metrics["profit_factor"],
            json!(1.8),
            "从 metrics jsonb 补全"
        );
        assert_eq!(
            snap.metrics["max_drawdown_duration_days"],
            json!(4),
            "从 equity curve 推导回撤天数"
        );
        // 来源标注：trial 自带 vs backtest_result 列 vs equity curve
        assert_eq!(
            snap.metric_sources["annual_return_pct"],
            json!("optimization_trial.metrics")
        );
        assert_eq!(
            snap.metric_sources["sharpe_ratio"],
            json!("backtest_result.sharpe_ratio")
        );
        assert_eq!(
            snap.metric_sources["max_drawdown_duration_days"],
            json!("backtest_equity_curve.portfolio_value")
        );
        // 仍缺 excess_return_pct / num_trades
        let missing = snap.missing_elite_metrics.as_array().expect("missing");
        assert!(missing.iter().any(|k| k == "excess_return_pct"));
        // num_trades 缺失时 metric_number 兜底 0.0 进 profile，不进 missing 清单
        assert!(!missing.iter().any(|k| k == "num_trades"));

        // 空 task → 空快照
        let empty_task = "zzz_api9x_snapshot_t2";
        insert_zzz_task(&db, empty_task, "running", 0).await;
        let snapshots = load_completed_trial_snapshots(&db, empty_task)
            .await
            .expect("snapshots");
        assert!(snapshots.is_empty());

        // enrich_trial_metrics 独立路径：无 backtest_task_id 时只统计缺失
        let (metrics, sources, missing) =
            enrich_trial_metrics(&db, None, json!({"sharpe_ratio": 1.0}))
                .await
                .expect("enrich");
        assert_eq!(metrics["sharpe_ratio"], json!(1.0));
        assert_eq!(sources["sharpe_ratio"], json!("optimization_trial.metrics"));
        assert_eq!(missing.as_array().map(Vec::len), Some(8));

        cleanup_scoped(&db, scope).await;
    }

    #[tokio::test]
    async fn derive_max_drawdown_duration_counts_below_peak_run() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let scope = "mdd";
        cleanup_scoped(&db, scope).await;

        let bt_task = "zzz_api9x_mdd_bt1";
        insert_zzz_backtest_task(&db, bt_task).await;
        // 110 → 100/90/95（连续 3 天低于前峰）→ 110 恢复：max duration 3
        for (idx, value) in [(1, 110), (2, 100), (3, 90), (4, 95), (5, 110)] {
            sqlx::query(
                "INSERT INTO backtest_equity_curve
                   (task_id, trade_date, portfolio_value, cash)
                 VALUES ($1, $2::date + ($3::int * interval '1 day'), $4, 0)",
            )
            .bind(bt_task)
            .bind("2026-01-05")
            .bind(idx - 1)
            .bind(value)
            .execute(&db)
            .await
            .expect("insert zzz equity curve");
        }
        let duration = derive_max_drawdown_duration_days(&db, bt_task)
            .await
            .expect("derive");
        assert_eq!(duration, Some(3));

        // 全程新高：无回撤 → 0
        let bt_flat = "zzz_api9x_mdd_bt2";
        insert_zzz_backtest_task(&db, bt_flat).await;
        for (idx, value) in [(1, 100), (2, 101), (3, 102)] {
            sqlx::query(
                "INSERT INTO backtest_equity_curve
                   (task_id, trade_date, portfolio_value, cash)
                 VALUES ($1, $2::date + ($3::int * interval '1 day'), $4, 0)",
            )
            .bind(bt_flat)
            .bind("2026-01-05")
            .bind(idx - 1)
            .bind(value)
            .execute(&db)
            .await
            .expect("insert zzz equity curve");
        }
        let duration = derive_max_drawdown_duration_days(&db, bt_flat)
            .await
            .expect("derive");
        assert_eq!(duration, Some(0));

        // 无有效行（<=0 的值全部跳过，observed 保持 false）→ None
        let bt_zero = "zzz_api9x_mdd_bt3";
        insert_zzz_backtest_task(&db, bt_zero).await;
        sqlx::query(
            "INSERT INTO backtest_equity_curve (task_id, trade_date, portfolio_value, cash)
             VALUES ($1, '2026-01-05', 0, 0)",
        )
        .bind(bt_zero)
        .execute(&db)
        .await
        .expect("insert zzz equity curve");
        let duration = derive_max_drawdown_duration_days(&db, bt_zero)
            .await
            .expect("derive");
        assert_eq!(duration, None);

        cleanup_scoped(&db, scope).await;
    }

    // ── DB：experiment_run 写入矩阵 ──

    #[tokio::test]
    async fn persist_optimization_experiment_run_status_matrix() {
        let db = test_db().await;
        let scope = "persist";
        cleanup_scoped(&db, scope).await;
        let policy = OptimizationPerformanceGatePolicy {
            min_completed_trials: 1,
            max_failed_trials: 2,
            max_elapsed_ms: None,
        };

        // gate passed → completed；elapsed>0 时写吞吐
        let id = persist_optimization_experiment_run(
            &db,
            "zzz_api9x_persist_t1",
            10,
            10,
            8,
            2,
            5000,
            &json!("trial-x"),
            &policy,
            &json!({}),
            "passed",
            None,
            None,
        )
        .await
        .expect("persist passed");
        let (status, metrics): (String, Value) = sqlx::query_as(
            "SELECT status, metrics FROM experiment_run WHERE experiment_run_id = $1",
        )
        .bind(&id)
        .fetch_one(&db)
        .await
        .expect("回读 experiment_run");
        assert_eq!(status, "completed");
        assert_eq!(metrics["executed"], json!(10));
        assert_eq!(metrics["gate_status"], json!("passed"));
        assert_eq!(metrics["throughput_trials_per_sec"], json!(2.0));
        assert_eq!(metrics["best_trial_id"], json!("trial-x"));

        // gate 失败但有产出 → partial
        let id = persist_optimization_experiment_run(
            &db,
            "zzz_api9x_persist_t1",
            5,
            5,
            3,
            1,
            0,
            &json!(null),
            &policy,
            &json!({}),
            "failed",
            None,
            None,
        )
        .await
        .expect("persist partial");
        let status: String =
            sqlx::query_scalar("SELECT status FROM experiment_run WHERE experiment_run_id = $1")
                .bind(&id)
                .fetch_one(&db)
                .await
                .expect("回读 status");
        assert_eq!(status, "partial");

        // gate 失败且零产出 → failed
        let id = persist_optimization_experiment_run(
            &db,
            "zzz_api9x_persist_t1",
            5,
            0,
            0,
            0,
            0,
            &json!(null),
            &policy,
            &json!({}),
            "failed",
            None,
            None,
        )
        .await
        .expect("persist failed");
        let status: String =
            sqlx::query_scalar("SELECT status FROM experiment_run WHERE experiment_run_id = $1")
                .bind(&id)
                .fetch_one(&db)
                .await
                .expect("回读 status");
        assert_eq!(status, "failed");

        cleanup_scoped(&db, scope).await;
    }

    // ── DB：elite 验证报告全流程 ──

    #[tokio::test]
    async fn build_and_persist_elite_report_ranks_and_persists() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let scope = "elite";
        cleanup_scoped(&db, scope).await;

        let task_id = "zzz_api9x_elite_t1";
        insert_zzz_task(&db, task_id, "running", 0).await;
        // 三个完成 trial：gap 逐个变差（年化 0.20 / 0.15 / 0.05）
        insert_zzz_trial(
            &db,
            &format!("{task_id}-0001"),
            task_id,
            1,
            "completed",
            Some(Decimal::new(30, 1)),
            Some(full_elite_metrics(0.20)),
            None,
        )
        .await;
        insert_zzz_trial(
            &db,
            &format!("{task_id}-0002"),
            task_id,
            2,
            "completed",
            Some(Decimal::new(20, 1)),
            Some(full_elite_metrics(0.15)),
            None,
        )
        .await;
        insert_zzz_trial(
            &db,
            &format!("{task_id}-0003"),
            task_id,
            3,
            "completed",
            Some(Decimal::new(10, 1)),
            Some(full_elite_metrics(0.05)),
            None,
        )
        .await;

        let req = EliteValidationReportRequest {
            top_n: Some(2),
            gate_policy: None,
        };
        let report = build_and_persist_elite_validation_report(&db, task_id, req)
            .await
            .expect("elite 报告");
        // top_n 截断 + gap 升序排序
        let candidates = report["candidates"].as_array().expect("candidates");
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0]["rank"], json!(1));
        assert_eq!(candidates[0]["trial_id"], json!(format!("{task_id}-0001")));
        assert_eq!(candidates[0]["elite_gap_score"], json!(0.0));
        assert_eq!(candidates[1]["trial_id"], json!(format!("{task_id}-0002")));
        // 每行携带平台分析与相关性贡献结构
        assert!(candidates[0]["parameter_plateau"]["near_neighbor_count"].is_u64());
        assert!(
            candidates[0]["portfolio_correlation_contribution"]["explicit_control_score"].is_f64()
        );
        // experiment_run 落库（type/related 指向 task）
        let run_id = report["experiment_run_id"].as_str().expect("run id");
        let (run_type, related): (String, String) = sqlx::query_as(
            "SELECT experiment_type, related_entity_id FROM experiment_run WHERE experiment_run_id = $1",
        )
        .bind(run_id)
        .fetch_one(&db)
        .await
        .expect("回读 experiment_run");
        assert_eq!(run_type, "professional_elite_validation_report");
        assert_eq!(related, task_id);
        // robustness 评估随流程落库（CASCADE 挂在本 task 下）
        let gate_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM robustness_gate_result WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&db)
        .await
        .expect("count robustness rows");
        assert_eq!(gate_rows, 2, "top_n 个 trial 各评估一次");

        // 无 completed trial → Err
        let empty_task = "zzz_api9x_elite_t2";
        insert_zzz_task(&db, empty_task, "running", 0).await;
        let req = EliteValidationReportRequest {
            top_n: None,
            gate_policy: None,
        };
        let err = build_and_persist_elite_validation_report(&db, empty_task, req)
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(err.contains("no completed trials"), "实际错误 {}", err);

        cleanup_scoped(&db, scope).await;
    }

    // ── DB：promote 全流程 ──

    #[tokio::test]
    async fn promote_trial_persists_candidate_and_audit_event() {
        let db = test_db().await;
        seed_shared_parents(&db).await;
        let scope = "promote";
        cleanup_scoped(&db, scope).await;

        // 守卫一：task 不存在
        let make_req = || PromoteOptimizationRequest {
            trial_id: None,
            target_strategy_version: "zzz_api9x_sv".into(),
            candidate_name: "zzz 候选".into(),
            promotion_mode: None,
            gate_policy: None,
            freeze_after_approval: None,
            reviewer: None,
            reason: "zzz 第九批晋级".into(),
            notes: None,
        };
        let err = promote_trial(&db, "zzz_api9x_promote_missing", make_req())
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err, "optimization task not found");

        // 守卫二：无 best_trial_id
        let task_id = "zzz_api9x_promote_t1";
        insert_zzz_task(&db, task_id, "completed", 0).await;
        let err = promote_trial(&db, task_id, make_req())
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(err.contains("no best_trial_id"), "实际错误 {}", err);

        // 守卫三：best trial 非 completed
        let pending_trial = format!("{task_id}-0001");
        insert_zzz_trial(&db, &pending_trial, task_id, 1, "pending", None, None, None).await;
        sqlx::query(
            "UPDATE optimization_task SET best_trial_id = $2 WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .bind(&pending_trial)
        .execute(&db)
        .await
        .expect("设置 best_trial_id");
        let err = promote_trial(&db, task_id, make_req())
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err, "only completed trials can be promoted");

        // 守卫四：completed 但无 score
        let noscore_trial = format!("{task_id}-0002");
        insert_zzz_trial(
            &db,
            &noscore_trial,
            task_id,
            2,
            "completed",
            None,
            Some(json!({})),
            None,
        )
        .await;
        sqlx::query(
            "UPDATE optimization_task SET best_trial_id = $2 WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .bind(&noscore_trial)
        .execute(&db)
        .await
        .expect("设置 best_trial_id");
        let err = promote_trial(&db, task_id, make_req())
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err, "completed trial has no score");

        // 正式流：completed + score → candidate + audit_event 双落库
        let good_trial = format!("{task_id}-0003");
        insert_zzz_trial(
            &db,
            &good_trial,
            task_id,
            3,
            "completed",
            Some(Decimal::new(18, 1)),
            Some(json!({"sharpe_ratio": 1.8})),
            None,
        )
        .await;
        sqlx::query(
            "UPDATE optimization_task SET best_trial_id = $2 WHERE optimization_task_id = $1",
        )
        .bind(task_id)
        .bind(&good_trial)
        .execute(&db)
        .await
        .expect("设置 best_trial_id");

        let result = promote_trial(&db, task_id, make_req())
            .await
            .expect("promote 成功");
        let candidate_id = result["candidate_id"]
            .as_str()
            .expect("candidate id")
            .to_string();
        assert_eq!(result["status"], json!("candidate"));
        assert_eq!(result["source_trial_id"], json!(good_trial));
        assert_eq!(result["requires_manual_review"], json!(true));

        let (name, target, objective_score, status): (String, String, Decimal, String) =
            sqlx::query_as(
                "SELECT candidate_name, target_strategy_version, objective_score, status \
                 FROM strategy_parameter_candidate WHERE candidate_id = $1",
            )
            .bind(&candidate_id)
            .fetch_one(&db)
            .await
            .expect("回读 candidate");
        assert_eq!(name, "zzz 候选");
        assert_eq!(target, "zzz_api9x_sv");
        assert_eq!(objective_score, Decimal::new(18, 1));
        assert_eq!(status, "candidate");

        let audit_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_event \
             WHERE entity_type = 'strategy_parameter_candidate' AND entity_id = $1",
        )
        .bind(&candidate_id)
        .fetch_one(&db)
        .await
        .expect("count audit");
        assert_eq!(audit_count, 1, "promote 必须留一条审计事件");

        cleanup_scoped(&db, scope).await;
    }
}
