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
use quant_api::discovery::phase7::{
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

#[derive(Clone)]
pub(crate) struct OptimizationTaskExecutionContext {
    pub(crate) strategy_version_id: String,
    pub(crate) data_version_id: String,
    pub(crate) backtest_template: Value,
    pub(crate) objective: Value,
    pub(crate) constraints: Option<Value>,
}


pub(crate) struct ScoredTrial {
    pub(crate) score: Decimal,
    pub(crate) metrics: Value,
    pub(crate) constraint_violations: Value,
}


pub(crate) fn backtest_cache_stats_delta(
    before: BacktestDataCacheStats,
    after: BacktestDataCacheStats,
) -> BacktestDataCacheStats {
    BacktestDataCacheStats {
        trading_day_hits: after
            .trading_day_hits
            .saturating_sub(before.trading_day_hits),
        trading_day_misses: after
            .trading_day_misses
            .saturating_sub(before.trading_day_misses),
        benchmark_data_hits: after
            .benchmark_data_hits
            .saturating_sub(before.benchmark_data_hits),
        benchmark_data_misses: after
            .benchmark_data_misses
            .saturating_sub(before.benchmark_data_misses),
        daily_bar_symbol_hits: after
            .daily_bar_symbol_hits
            .saturating_sub(before.daily_bar_symbol_hits),
        daily_bar_covering_window_hits: after
            .daily_bar_covering_window_hits
            .saturating_sub(before.daily_bar_covering_window_hits),
        daily_bar_snapshot_hits: after
            .daily_bar_snapshot_hits
            .saturating_sub(before.daily_bar_snapshot_hits),
        daily_bar_symbol_misses: after
            .daily_bar_symbol_misses
            .saturating_sub(before.daily_bar_symbol_misses),
        trading_profile_symbol_hits: after
            .trading_profile_symbol_hits
            .saturating_sub(before.trading_profile_symbol_hits),
        trading_profile_symbol_misses: after
            .trading_profile_symbol_misses
            .saturating_sub(before.trading_profile_symbol_misses),
    }
}


#[derive(Debug, Clone)]
pub(crate) struct DiscoveryCandidate {
    pub(crate) trial_id: String,
    pub(crate) backtest_task_id: Option<String>,
    pub(crate) score: Option<Decimal>,
    pub(crate) candidate_type: CandidateType,
    pub(crate) professional_gap_score: Decimal,
    pub(crate) metrics: CandidateMetrics,
    pub(crate) parameters: Value,
}


pub(crate) enum OptimizationTrialBacktestRequest {
    Factor(RunFactorBacktestReq),
    Prediction(RunPredictionBacktestReq),
}


pub(crate) struct FactorSignalBatchPrewarmPlan {
    requested_trials: usize,
    factor_requests: Vec<RunFactorBacktestReq>,
    prediction_trials: usize,
    invalid_trials: usize,
}


pub async fn run_optimization_trials(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<RunOptimizationRequest>,
) -> impl IntoResponse {
    let trial_limit = normalize_limit(req.trial_limit);
    match execute_pending_trials(
        &state.db,
        &task_id,
        trial_limit,
        req.performance_gate.as_ref(),
    )
    .await
    {
        Ok(summary) => Json(json!({"code": 0, "data": summary})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}


pub(crate) async fn evaluate_discovery_candidates(
    db: &sqlx::PgPool,
    task_id: &str,
    candidates: &[DiscoveryCandidate],
    gate_policy: &Value,
    evaluated_trial_ids: &mut BTreeSet<String>,
) -> Result<Vec<Value>, String> {
    let mut results = Vec::new();
    for candidate in candidates {
        if !evaluated_trial_ids.insert(candidate.trial_id.clone()) {
            continue;
        }
        results.push(
            evaluate_and_persist_robustness_for_trial(
                db,
                task_id,
                &candidate.trial_id,
                Some(gate_policy),
                "Professional discovery robustness gate evaluated",
            )
            .await?,
        );
    }
    Ok(results)
}


pub(crate) fn generate_trial_parameters(
    search_space: &Value,
    seed: u64,
    max_trials: usize,
) -> Result<Vec<Value>, String> {
    let specs = search_space
        .as_object()
        .ok_or_else(|| "search_space must be a JSON object".to_string())?;
    let max_trials = normalize_max_trials(max_trials);
    let mut rng = DeterministicRng::new(seed);
    let mut trials = Vec::with_capacity(max_trials);

    for _ in 0..max_trials {
        let mut params = Map::new();
        for (name, spec) in specs {
            params.insert(name.clone(), sample_parameter(name, spec, &mut rng)?);
        }
        trials.push(Value::Object(params));
    }

    Ok(trials)
}


pub(crate) async fn execute_pending_trials(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_limit: i64,
    performance_gate: Option<&OptimizationPerformanceGateRequest>,
) -> Result<Value, String> {
    let mut signal_cache = SignalDataCache::default();
    let mut backtest_cache = BacktestDataCache::default();
    execute_pending_trials_with_caches(
        db,
        task_id,
        trial_limit,
        performance_gate,
        &mut signal_cache,
        &mut backtest_cache,
    )
    .await
}


pub(crate) async fn execute_pending_trials_with_caches(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_limit: i64,
    performance_gate: Option<&OptimizationPerformanceGateRequest>,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
) -> Result<Value, String> {
    execute_pending_trials_with_caches_and_concurrency(
        db,
        task_id,
        trial_limit,
        performance_gate,
        signal_cache,
        backtest_cache,
        1,
    )
    .await
}


pub(crate) fn plan_factor_signal_batch_prewarm_requests(
    task: &OptimizationTaskExecutionContext,
    pending_trials: &[(String, i32, Value)],
) -> FactorSignalBatchPrewarmPlan {
    let mut factor_requests = Vec::new();
    let mut prediction_trials = 0;
    let mut invalid_trials = 0;
    for (_trial_id, _trial_index, params) in pending_trials {
        match build_optimization_trial_request(task, params) {
            Ok(OptimizationTrialBacktestRequest::Factor(request)) => factor_requests.push(request),
            Ok(OptimizationTrialBacktestRequest::Prediction(_)) => {
                prediction_trials += 1;
            }
            Err(_) => {
                invalid_trials += 1;
            }
        }
    }

    FactorSignalBatchPrewarmPlan {
        requested_trials: pending_trials.len(),
        factor_requests,
        prediction_trials,
        invalid_trials,
    }
}


pub(crate) async fn execute_pending_trials_with_caches_and_concurrency(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_limit: i64,
    performance_gate: Option<&OptimizationPerformanceGateRequest>,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
    trial_concurrency: usize,
) -> Result<Value, String> {
    let started = Instant::now();
    let gate_policy = normalize_performance_gate(performance_gate, trial_limit)?;
    let task = load_execution_context(db, task_id).await?;
    let signal_cache_stats_before = signal_cache.stats();
    let backtest_cache_stats_before = backtest_cache.stats();
    let pending_trials = sqlx::query_as::<_, (String, i32, Value)>(
        "SELECT trial_id, trial_index, parameters
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'pending'
         ORDER BY trial_index ASC
         LIMIT $2",
    )
    .bind(task_id)
    .bind(trial_limit)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load pending optimization trials: {}", error))?;

    if pending_trials.is_empty() {
        refresh_task_progress(db, task_id).await?;
        let elapsed_ms = started.elapsed().as_millis() as i64;
        let best_trial_id = Value::Null;
        let gates = evaluate_optimization_performance_gates(0, 0, 0, elapsed_ms, &gate_policy);
        let gate_status = performance_gate_status(&gates);
        let experiment_run_id = persist_optimization_experiment_run(
            db,
            task_id,
            trial_limit,
            0,
            0,
            0,
            elapsed_ms,
            &best_trial_id,
            &gate_policy,
            &gates,
            gate_status,
            None,
            None,
        )
        .await?;
        return Ok(json!({
            "optimization_task_id": task_id,
            "executed": 0,
            "completed": 0,
            "failed": 0,
            "best_trial_id": best_trial_id,
            "elapsed_ms": elapsed_ms,
            "experiment_run_id": experiment_run_id,
            "performance_gate_status": gate_status,
            "performance_gates": gates,
        }));
    }

    sqlx::query(
        "UPDATE optimization_task
         SET status = 'running', last_heartbeat_at = now()
         WHERE optimization_task_id = $1 AND status IN ('pending', 'running', 'partial')",
    )
    .bind(task_id)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark optimization task running: {}", error))?;

    let trial_concurrency = trial_concurrency.clamp(1, pending_trials.len().max(1));
    if trial_concurrency > 1 {
        let signal_cache_snapshot = signal_cache.snapshot();
        let backtest_cache_snapshot = backtest_cache.snapshot();
        return execute_loaded_pending_trials_concurrently(
            db,
            task_id,
            trial_limit,
            &task,
            pending_trials,
            &gate_policy,
            started,
            trial_concurrency,
            Some(signal_cache_snapshot),
            Some(backtest_cache_snapshot),
        )
        .await;
    }

    let signal_batch_prewarm_plan =
        plan_factor_signal_batch_prewarm_requests(&task, &pending_trials);
    let signal_batch_prewarm = if signal_batch_prewarm_plan.factor_requests.is_empty() {
        None
    } else {
        let factor_request_count = signal_batch_prewarm_plan.factor_requests.len();
        Some(
            match prewarm_factor_signal_cache_for_requests(
                db,
                &signal_batch_prewarm_plan.factor_requests,
                signal_cache,
            )
            .await
            {
                Ok(report) => json!({
                    "status": "completed",
                    "requested_trials": signal_batch_prewarm_plan.requested_trials,
                    "factor_requests": factor_request_count,
                    "prediction_trials": signal_batch_prewarm_plan.prediction_trials,
                    "invalid_trials": signal_batch_prewarm_plan.invalid_trials,
                    "report": report,
                }),
                Err(error) => json!({
                    "status": "failed",
                    "requested_trials": signal_batch_prewarm_plan.requested_trials,
                    "factor_requests": factor_request_count,
                    "prediction_trials": signal_batch_prewarm_plan.prediction_trials,
                    "invalid_trials": signal_batch_prewarm_plan.invalid_trials,
                    "error": error,
                }),
            },
        )
    };

    let mut completed = 0;
    let mut failed = 0;
    for (trial_id, _trial_index, params) in pending_trials.iter() {
        let backtest_task_id = format!("optbt-{}", Uuid::new_v4());
        if let Err(error) = mark_trial_running(db, trial_id).await {
            failed += 1;
            let _ = mark_trial_failed(db, trial_id, &error).await;
            continue;
        }

        if let Some(reused) = find_reusable_trial(db, task_id, &task, trial_id, params).await? {
            mark_trial_reused(db, trial_id, &reused).await?;
            completed += 1;
            continue;
        }

        let request = match build_optimization_trial_request(&task, params) {
            Ok(request) => request,
            Err(error) => {
                failed += 1;
                mark_trial_failed(db, trial_id, &error).await?;
                continue;
            }
        };

        let execution_result = match request {
            OptimizationTrialBacktestRequest::Factor(request) => {
                execute_factor_backtest_with_caches(
                    db,
                    &backtest_task_id,
                    request,
                    Some(signal_cache),
                    Some(backtest_cache),
                )
                .await
            }
            OptimizationTrialBacktestRequest::Prediction(request) => {
                execute_prediction_backtest(db, &backtest_task_id, request).await
            }
        };

        match execution_result {
            Ok(output) => {
                let scored =
                    score_trial_with_output(&output, &task.objective, task.constraints.as_ref());
                mark_trial_completed(db, trial_id, &backtest_task_id, &scored).await?;
                completed += 1;
            }
            Err(error) => {
                failed += 1;
                mark_trial_failed(db, trial_id, &error).await?;
            }
        }
    }

    let best_trial_id = refresh_task_progress(db, task_id).await?;
    let elapsed_ms = started.elapsed().as_millis() as i64;
    let signal_cache_stats = json!(signal_cache_stats_delta(
        signal_cache_stats_before,
        signal_cache.stats()
    ));
    let backtest_cache_stats = json!(backtest_cache_stats_delta(
        backtest_cache_stats_before,
        backtest_cache.stats()
    ));
    let gates = evaluate_optimization_performance_gates(
        pending_trials.len() as i64,
        completed,
        failed,
        elapsed_ms,
        &gate_policy,
    );
    let gate_status = performance_gate_status(&gates);
    let best_trial_value = json!(best_trial_id);
    let experiment_run_id = persist_optimization_experiment_run(
        db,
        task_id,
        trial_limit,
        pending_trials.len() as i64,
        completed,
        failed,
        elapsed_ms,
        &best_trial_value,
        &gate_policy,
        &gates,
        gate_status,
        Some(&signal_cache_stats),
        Some(&backtest_cache_stats),
    )
    .await?;

    Ok(json!({
        "optimization_task_id": task_id,
        "executed": pending_trials.len(),
        "completed": completed,
        "failed": failed,
        "best_trial_id": best_trial_id,
        "elapsed_ms": elapsed_ms,
        "experiment_run_id": experiment_run_id,
        "performance_gate_status": gate_status,
        "performance_gates": gates,
        "trial_concurrency": 1,
        "cache_mode": "shared_window",
        "signal_batch_prewarm": signal_batch_prewarm,
        "signal_cache": signal_cache_stats,
        "backtest_cache": backtest_cache_stats,
    }))
}


pub(crate) async fn execute_loaded_pending_trials_concurrently(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_limit: i64,
    task: &OptimizationTaskExecutionContext,
    pending_trials: Vec<(String, i32, Value)>,
    gate_policy: &OptimizationPerformanceGatePolicy,
    started: Instant,
    trial_concurrency: usize,
    signal_cache_snapshot: Option<SignalDataCacheSnapshot>,
    backtest_cache_snapshot: Option<BacktestDataCacheSnapshot>,
) -> Result<Value, String> {
    let pending_count = pending_trials.len();
    let mut join_set = tokio::task::JoinSet::new();
    let mut completed = 0;
    let mut failed = 0;
    let mut signal_cache_stats = SignalDataCacheStats::default();
    let mut backtest_cache_stats = BacktestDataCacheStats::default();
    let mut signal_cache_snapshot = signal_cache_snapshot;
    let mut backtest_cache_snapshot = backtest_cache_snapshot;

    let mut pending_trials = pending_trials.into_iter();
    if let Some((trial_id, _trial_index, params)) = pending_trials.next() {
        let outcome = execute_single_pending_trial(
            db.clone(),
            task_id.to_string(),
            task.clone(),
            trial_id,
            params,
            signal_cache_snapshot.clone(),
            backtest_cache_snapshot.clone(),
        )
        .await;
        completed += outcome.completed;
        failed += outcome.failed;
        add_signal_cache_stats(&mut signal_cache_stats, outcome.signal_cache_stats);
        add_backtest_cache_stats(&mut backtest_cache_stats, outcome.backtest_cache_stats);
        signal_cache_snapshot = outcome.signal_cache_snapshot;
        backtest_cache_snapshot = outcome.backtest_cache_snapshot;
    }

    for (trial_id, _trial_index, params) in pending_trials {
        while join_set.len() >= trial_concurrency {
            merge_trial_execution_join(
                join_set
                    .join_next()
                    .await
                    .ok_or_else(|| "trial concurrency join set ended unexpectedly".to_string())?,
                &mut completed,
                &mut failed,
                &mut signal_cache_stats,
                &mut backtest_cache_stats,
            );
        }
        let db = db.clone();
        let task_id = task_id.to_string();
        let task = task.clone();
        let signal_cache_snapshot = signal_cache_snapshot.clone();
        let backtest_cache_snapshot = backtest_cache_snapshot.clone();
        join_set.spawn(async move {
            execute_single_pending_trial(
                db,
                task_id,
                task,
                trial_id,
                params,
                signal_cache_snapshot,
                backtest_cache_snapshot,
            )
            .await
        });
    }

    while let Some(result) = join_set.join_next().await {
        merge_trial_execution_join(
            result,
            &mut completed,
            &mut failed,
            &mut signal_cache_stats,
            &mut backtest_cache_stats,
        );
    }

    let best_trial_id = refresh_task_progress(db, task_id).await?;
    let elapsed_ms = started.elapsed().as_millis() as i64;
    let signal_cache_stats_json = json!(signal_cache_stats);
    let backtest_cache_stats_json = json!(backtest_cache_stats);
    let gates = evaluate_optimization_performance_gates(
        pending_count as i64,
        completed,
        failed,
        elapsed_ms,
        gate_policy,
    );
    let gate_status = performance_gate_status(&gates);
    let best_trial_value = json!(best_trial_id);
    let experiment_run_id = persist_optimization_experiment_run(
        db,
        task_id,
        trial_limit,
        pending_count as i64,
        completed,
        failed,
        elapsed_ms,
        &best_trial_value,
        gate_policy,
        &gates,
        gate_status,
        Some(&signal_cache_stats_json),
        Some(&backtest_cache_stats_json),
    )
    .await?;

    Ok(json!({
        "optimization_task_id": task_id,
        "executed": pending_count,
        "completed": completed,
        "failed": failed,
        "best_trial_id": best_trial_id,
        "elapsed_ms": elapsed_ms,
        "experiment_run_id": experiment_run_id,
        "performance_gate_status": gate_status,
        "performance_gates": gates,
        "trial_concurrency": trial_concurrency,
        "cache_mode": "per_trial_isolated",
        "signal_cache": signal_cache_stats_json,
        "backtest_cache": backtest_cache_stats_json,
    }))
}


pub(crate) fn merge_trial_execution_join(
    result: Result<TrialExecutionOutcome, tokio::task::JoinError>,
    completed: &mut i64,
    failed: &mut i64,
    signal_cache_stats: &mut SignalDataCacheStats,
    backtest_cache_stats: &mut BacktestDataCacheStats,
) {
    match result {
        Ok(outcome) => {
            *completed += outcome.completed;
            *failed += outcome.failed;
            add_signal_cache_stats(signal_cache_stats, outcome.signal_cache_stats);
            add_backtest_cache_stats(backtest_cache_stats, outcome.backtest_cache_stats);
        }
        Err(_) => {
            *failed += 1;
        }
    }
}


pub(crate) async fn execute_single_pending_trial(
    db: sqlx::PgPool,
    task_id: String,
    task: OptimizationTaskExecutionContext,
    trial_id: String,
    params: Value,
    signal_cache_snapshot: Option<SignalDataCacheSnapshot>,
    backtest_cache_snapshot: Option<BacktestDataCacheSnapshot>,
) -> TrialExecutionOutcome {
    let mut signal_cache = signal_cache_snapshot
        .as_ref()
        .map(SignalDataCache::from_snapshot)
        .unwrap_or_default();
    let mut backtest_cache = backtest_cache_snapshot
        .as_ref()
        .map(BacktestDataCache::from_snapshot)
        .unwrap_or_default();
    let mut completed = 0;
    let mut failed = 0;
    let backtest_task_id = format!("optbt-{}", Uuid::new_v4());

    let execution = async {
        mark_trial_running(&db, &trial_id).await?;
        if let Some(reused) = find_reusable_trial(&db, &task_id, &task, &trial_id, &params).await? {
            mark_trial_reused(&db, &trial_id, &reused).await?;
            return Ok::<bool, String>(true);
        }

        let request = build_optimization_trial_request(&task, &params)?;
        let output = match request {
            OptimizationTrialBacktestRequest::Factor(request) => {
                execute_factor_backtest_with_caches(
                    &db,
                    &backtest_task_id,
                    request,
                    Some(&mut signal_cache),
                    Some(&mut backtest_cache),
                )
                .await
            }
            OptimizationTrialBacktestRequest::Prediction(request) => {
                execute_prediction_backtest(&db, &backtest_task_id, request).await
            }
        }?;
        let scored = score_trial_with_output(&output, &task.objective, task.constraints.as_ref());
        mark_trial_completed(&db, &trial_id, &backtest_task_id, &scored).await?;
        Ok::<bool, String>(true)
    }
    .await;

    match execution {
        Ok(true) => completed += 1,
        Ok(false) => failed += 1,
        Err(error) => {
            failed += 1;
            let _ = mark_trial_failed(&db, &trial_id, &error).await;
        }
    }

    TrialExecutionOutcome {
        completed,
        failed,
        signal_cache_stats: signal_cache.stats(),
        backtest_cache_stats: backtest_cache.stats(),
        signal_cache_snapshot: Some(signal_cache.snapshot()),
        backtest_cache_snapshot: Some(backtest_cache.snapshot()),
    }
}


pub(crate) fn add_signal_cache_stats(total: &mut SignalDataCacheStats, value: SignalDataCacheStats) {
    total.combo_score_hits += value.combo_score_hits;
    total.combo_score_misses += value.combo_score_misses;
    total.trading_day_hits += value.trading_day_hits;
    total.trading_day_misses += value.trading_day_misses;
    total.return_history_hits += value.return_history_hits;
    total.return_history_covering_window_hits += value.return_history_covering_window_hits;
    total.return_history_snapshot_hits += value.return_history_snapshot_hits;
    total.return_history_misses += value.return_history_misses;
    total.persistent_return_history_hits += value.persistent_return_history_hits;
    total.persistent_return_history_misses += value.persistent_return_history_misses;
    total.persistent_return_history_writes += value.persistent_return_history_writes;
    total.average_amount_hits += value.average_amount_hits;
    total.average_amount_symbol_hits += value.average_amount_symbol_hits;
    total.average_amount_history_hits += value.average_amount_history_hits;
    total.average_amount_history_covering_window_hits +=
        value.average_amount_history_covering_window_hits;
    total.average_amount_history_snapshot_hits += value.average_amount_history_snapshot_hits;
    total.average_amount_misses += value.average_amount_misses;
    total.average_amount_symbol_misses += value.average_amount_symbol_misses;
    total.average_amount_history_misses += value.average_amount_history_misses;
    total.persistent_average_amount_history_hits += value.persistent_average_amount_history_hits;
    total.persistent_average_amount_history_misses +=
        value.persistent_average_amount_history_misses;
    total.persistent_average_amount_history_writes +=
        value.persistent_average_amount_history_writes;
    total.persistent_pit_average_amount_matrix_hits +=
        value.persistent_pit_average_amount_matrix_hits;
    total.persistent_pit_average_amount_matrix_misses +=
        value.persistent_pit_average_amount_matrix_misses;
    total.persistent_pit_average_amount_matrix_writes +=
        value.persistent_pit_average_amount_matrix_writes;
    total.persistent_return_risk_feature_matrix_hits +=
        value.persistent_return_risk_feature_matrix_hits;
    total.persistent_return_risk_feature_matrix_misses +=
        value.persistent_return_risk_feature_matrix_misses;
    total.persistent_return_risk_feature_matrix_writes +=
        value.persistent_return_risk_feature_matrix_writes;
    total.persistent_return_risk_stats_feature_matrix_hits +=
        value.persistent_return_risk_stats_feature_matrix_hits;
    total.persistent_return_risk_stats_feature_matrix_misses +=
        value.persistent_return_risk_stats_feature_matrix_misses;
    total.persistent_return_risk_stats_feature_matrix_writes +=
        value.persistent_return_risk_stats_feature_matrix_writes;
    total.persistent_return_risk_feature_matrix_rows_loaded +=
        value.persistent_return_risk_feature_matrix_rows_loaded;
    total.persistent_return_risk_feature_matrix_return_values_loaded +=
        value.persistent_return_risk_feature_matrix_return_values_loaded;
    total.persistent_return_risk_feature_matrix_rows_written +=
        value.persistent_return_risk_feature_matrix_rows_written;
    total.persistent_return_risk_feature_matrix_return_values_written +=
        value.persistent_return_risk_feature_matrix_return_values_written;
    total.persistent_return_risk_stats_feature_matrix_stats_rows_loaded +=
        value.persistent_return_risk_stats_feature_matrix_stats_rows_loaded;
    total.persistent_return_risk_stats_feature_matrix_pair_rows_loaded +=
        value.persistent_return_risk_stats_feature_matrix_pair_rows_loaded;
    total.persistent_return_risk_stats_feature_matrix_stats_rows_written +=
        value.persistent_return_risk_stats_feature_matrix_stats_rows_written;
    total.persistent_return_risk_stats_feature_matrix_pair_rows_written +=
        value.persistent_return_risk_stats_feature_matrix_pair_rows_written;
    total.prediction_score_hits += value.prediction_score_hits;
    total.prediction_score_misses += value.prediction_score_misses;
    total.industry_classification_hits += value.industry_classification_hits;
    total.industry_classification_misses += value.industry_classification_misses;
    total.benchmark_return_hits += value.benchmark_return_hits;
    total.benchmark_return_misses += value.benchmark_return_misses;
}


pub(crate) fn add_backtest_cache_stats(total: &mut BacktestDataCacheStats, value: BacktestDataCacheStats) {
    total.trading_day_hits += value.trading_day_hits;
    total.trading_day_misses += value.trading_day_misses;
    total.benchmark_data_hits += value.benchmark_data_hits;
    total.benchmark_data_misses += value.benchmark_data_misses;
    total.daily_bar_symbol_hits += value.daily_bar_symbol_hits;
    total.daily_bar_covering_window_hits += value.daily_bar_covering_window_hits;
    total.daily_bar_snapshot_hits += value.daily_bar_snapshot_hits;
    total.daily_bar_symbol_misses += value.daily_bar_symbol_misses;
    total.trading_profile_symbol_hits += value.trading_profile_symbol_hits;
    total.trading_profile_symbol_misses += value.trading_profile_symbol_misses;
}


pub(crate) async fn load_discovery_candidates(
    db: &sqlx::PgPool,
    task_id: &str,
    targets: &CandidateTargets,
    limit: usize,
) -> Result<Vec<DiscoveryCandidate>, String> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<Decimal>,
            Option<Value>,
            Option<Value>,
            Option<String>,
            Value,
        ),
    >(
        "SELECT trial_id, score, metrics, constraint_violations, backtest_task_id, parameters
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'completed' AND metrics IS NOT NULL",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load professional discovery candidates: {}",
            error
        )
    })?;

    let mut candidates = rows
        .into_iter()
        .filter_map(
            |(trial_id, score, metrics, _violations, backtest_task_id, parameters)| {
                let metrics_json = metrics?;
                let candidate_metrics = CandidateMetrics::from_optimization_metrics(&metrics_json);
                if candidate_metrics.annual_return < targets.min_annual_return
                    || candidate_metrics.excess_return <= targets.min_excess_return
                {
                    return None;
                }
                let candidate_type = targets.classify(&candidate_metrics);
                let professional_gap_score =
                    professional_candidate_gap_score(&candidate_metrics, targets);
                Some(DiscoveryCandidate {
                    trial_id,
                    backtest_task_id,
                    score,
                    candidate_type,
                    professional_gap_score,
                    metrics: candidate_metrics,
                    parameters,
                })
            },
        )
        .collect::<Vec<_>>();
    candidates.sort_by(discovery_candidate_order);
    candidates.truncate(limit);
    Ok(candidates)
}


pub(crate) fn discovery_candidate_order(
    left: &DiscoveryCandidate,
    right: &DiscoveryCandidate,
) -> std::cmp::Ordering {
    discovery_candidate_rank(left.candidate_type)
        .cmp(&discovery_candidate_rank(right.candidate_type))
        .then_with(|| {
            left.professional_gap_score
                .cmp(&right.professional_gap_score)
        })
        .then_with(|| left.metrics.max_drawdown.cmp(&right.metrics.max_drawdown))
        .then_with(|| right.metrics.sortino.cmp(&left.metrics.sortino))
        .then_with(|| right.metrics.sharpe.cmp(&left.metrics.sharpe))
        .then_with(|| right.metrics.annual_return.cmp(&left.metrics.annual_return))
}


pub(crate) fn professional_candidate_gap_score(
    metrics: &CandidateMetrics,
    targets: &CandidateTargets,
) -> Decimal {
    let annual_gap = positive_gap(targets.min_annual_return, metrics.annual_return);
    let excess_gap = positive_gap(targets.min_excess_return, metrics.excess_return);
    let sharpe_gap = positive_gap(targets.min_sharpe, metrics.sharpe);
    let sortino_gap = positive_gap(targets.min_sortino, metrics.sortino);
    let drawdown_gap = positive_gap(metrics.max_drawdown, targets.max_drawdown);

    annual_gap * Decimal::new(4, 0)
        + excess_gap * Decimal::new(2, 0)
        + sharpe_gap
        + sortino_gap
        + drawdown_gap * Decimal::new(5, 0)
}


pub(crate) fn positive_gap(limit: Decimal, actual: Decimal) -> Decimal {
    (limit - actual).max(Decimal::ZERO)
}


pub(crate) fn discovery_candidate_rank(candidate_type: CandidateType) -> u8 {
    match candidate_type {
        CandidateType::Professional => 0,
        CandidateType::ReviewRequired => 1,
        CandidateType::Defensive => 2,
        CandidateType::Research => 3,
    }
}


pub(crate) fn discovery_candidate_json(candidate: &DiscoveryCandidate) -> Value {
    json!({
        "trial_id": candidate.trial_id,
        "backtest_task_id": candidate.backtest_task_id,
        "score": candidate.score,
        "candidate_type": candidate.candidate_type,
        "professional_gap_score": candidate.professional_gap_score,
        "metrics": {
            "annual_return_pct": candidate.metrics.annual_return,
            "excess_return_pct": candidate.metrics.excess_return,
            "sharpe_ratio": candidate.metrics.sharpe,
            "sortino_ratio": candidate.metrics.sortino,
            "max_drawdown_pct": candidate.metrics.max_drawdown,
            "total_return_pct": candidate.metrics.total_return,
            "benchmark_return_pct": candidate.metrics.benchmark_return,
            "turnover": candidate.metrics.turnover,
            "num_trades": candidate.metrics.num_trades,
            "execution_schedule_expired_count": candidate.metrics.execution_schedule_expired_count,
            "execution_schedule_roll_forward_count": candidate.metrics.execution_schedule_roll_forward_count,
            "max_execution_target_gap_pct": candidate.metrics.max_execution_target_gap,
            "final_cash_weight_pct": candidate.metrics.final_cash_weight,
            "final_target_gross_exposure_pct": candidate.metrics.final_target_gross_exposure,
            "final_actual_gross_exposure_pct": candidate.metrics.final_actual_gross_exposure,
            "final_unfilled_target_gap_pct": candidate.metrics.final_unfilled_target_gap,
            "final_execution_fill_ratio": candidate.metrics.final_execution_fill_ratio,
        },
        "parameters": candidate.parameters,
    })
}


pub(crate) fn build_optimization_trial_request(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<OptimizationTrialBacktestRequest, String> {
    match trial_signal_source(task, parameters)?.as_deref() {
        Some("prediction") | Some("model_prediction") | Some("ml_prediction") => {
            build_prediction_trial_request(task, parameters)
                .map(OptimizationTrialBacktestRequest::Prediction)
        }
        Some("factor") | Some("factor_combo") | Some("prediction_blend") | None => {
            build_factor_trial_request(task, parameters)
                .map(OptimizationTrialBacktestRequest::Factor)
        }
        Some(other) => Err(format!("unsupported signal_source: {}", other)),
    }
}


pub(crate) fn trial_signal_source(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<Option<String>, String> {
    let template = task
        .backtest_template
        .as_object()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    let params = parameters
        .as_object()
        .ok_or_else(|| "trial parameters must be a JSON object".to_string())?;

    match params
        .get("signal_source")
        .or_else(|| template.get("signal_source"))
    {
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value.to_string()))
            }
        }
        Some(Value::Null) | None => {
            let has_prediction = params
                .get("prediction_set_id")
                .or_else(|| template.get("prediction_set_id"))
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            let has_combo = params
                .get("combo_name")
                .or_else(|| template.get("combo_name"))
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            if has_prediction && !has_combo {
                Ok(Some("model_prediction".to_string()))
            } else {
                Ok(None)
            }
        }
        Some(_) => Err("signal_source must be a string or null".to_string()),
    }
}


pub(crate) fn build_factor_trial_request(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<RunFactorBacktestReq, String> {
    let template = task
        .backtest_template
        .as_object()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    let params = parameters
        .as_object()
        .ok_or_else(|| "trial parameters must be a JSON object".to_string())?;
    validate_industry_prosperity_trial_admission(params, template)?;
    validate_equity_pledge_trial_admission(params, template)?;
    validate_shareholder_structure_trial_admission(params, template)?;
    validate_margin_detail_trial_admission(params, template)?;
    validate_analyst_revision_trial_admission(params, template)?;

    let string_value = |name: &str, default: Option<&str>| -> Result<String, String> {
        if let Some(value) = params.get(name).or_else(|| template.get(name)) {
            match value {
                Value::String(value) => Ok(value.clone()),
                Value::Number(value) => Ok(value.to_string()),
                _ => Err(format!("{} must be a string or number", name)),
            }
        } else if let Some(default) = default {
            Ok(default.to_string())
        } else {
            Err(format!("backtest_template.{} is required", name))
        }
    };
    let optional_string = |name: &str| -> Result<Option<String>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(Value::Number(value)) => Ok(Some(value.to_string())),
            _ => Err(format!("{} must be a string or number", name)),
        }
    };
    let optional_string_array = |name: &str| -> Result<Option<Vec<String>>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(vec![value.clone()])),
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| match value {
                    Value::String(value) => Ok(value.clone()),
                    Value::Number(value) => Ok(value.to_string()),
                    _ => Err(format!("{} must contain only strings or numbers", name)),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some),
            _ => Err(format!("{} must be a string or array", name)),
        }
    };
    let usize_value = |name: &str, default: usize| -> Result<usize, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_u64()
                .map(|value| value as usize)
                .ok_or_else(|| format!("{} must be a positive integer", name)),
            Some(Value::String(value)) => value
                .parse::<usize>()
                .map_err(|_| format!("{} must be a positive integer", name)),
            _ => Err(format!("{} must be a positive integer", name)),
        }
    };
    let f64_value = |name: &str, default: f64| -> Result<f64, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_f64()
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };
    let optional_f64_value = |name: &str| -> Result<Option<f64>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(value)) => value
                .as_f64()
                .map(Some)
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map(Some)
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };
    let optional_u32_value = |name: &str| -> Result<Option<u32>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(value)) => value
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .map(Some)
                .ok_or_else(|| format!("{} must be a positive integer", name)),
            Some(Value::String(value)) => value
                .parse::<u32>()
                .map(Some)
                .map_err(|_| format!("{} must be a positive integer", name)),
            _ => Err(format!("{} must be a positive integer", name)),
        }
    };

    let benchmark = optional_string("benchmark")?.or_else(|| Some("000300.SH".into()));
    let top_n = usize_value("top_n", 20)?;
    let effective_coverage = effective_coverage_policy_from_value(
        params
            .get("effective_coverage")
            .or_else(|| template.get("effective_coverage")),
        top_n,
    )?;
    let market_regime = market_regime_request_from_value(
        params
            .get("market_regime")
            .or_else(|| template.get("market_regime")),
        benchmark.as_deref().unwrap_or("000300.SH"),
    )?;

    Ok(RunFactorBacktestReq {
        combo_name: string_value("combo_name", None)?,
        version: string_value("version", Some("1.0.0"))?,
        strategy_version_id: task.strategy_version_id.clone(),
        data_version_id: task.data_version_id.clone(),
        research_dataset_id: optional_string("research_dataset_id")?,
        feature_set_version_id: optional_string("feature_set_version_id")?,
        prediction_set_id: optional_string("prediction_set_id")?,
        prediction_blend_weight: optional_f64_value("prediction_blend_weight")?,
        prediction_min_percentile: optional_f64_value("prediction_min_percentile")?,
        prediction_min_score: optional_f64_value("prediction_min_score")?,
        event_gate_combo_name: optional_string("event_gate_combo_name")?,
        event_gate_version: string_value("event_gate_version", Some("1.0.0"))?,
        event_gate_mode: optional_string("event_gate_mode")?,
        event_gate_min_score: optional_f64_value("event_gate_min_score")?,
        event_gate_boost_weight: optional_f64_value("event_gate_boost_weight")?,
        event_gate_score_direction: optional_string("event_gate_score_direction")?,
        event_gate_active_regimes: optional_string_array("event_gate_active_regimes")?,
        portfolio_policy_id: optional_string("portfolio_policy_id")?,
        top_n,
        rebalance: string_value("rebalance", Some("monthly"))?,
        entry_delay: usize_value("entry_delay", 0)?,
        min_amount: f64_value("min_amount", 0.0)?,
        max_position_pct: f64_value("max_position_pct", 0.10)?,
        skip_top_pct: f64_value("skip_top_pct", 0.0)?,
        max_pairwise_correlation: optional_f64_value("max_pairwise_correlation")?,
        correlation_lookback_days: usize_value("correlation_lookback_days", 60)?,
        kelly_fraction: f64_value("kelly_fraction", 0.0)?,
        kelly_lookback_days: usize_value("kelly_lookback_days", 60)?,
        max_gross_exposure: f64_value("max_gross_exposure", 1.0)?,
        score_direction: string_value("score_direction", Some("descending"))?,
        portfolio_method: string_value("portfolio_method", Some("heuristic"))?,
        risk_budget_lookback_days: usize_value("risk_budget_lookback_days", 60)?,
        capacity_penalty_strength: f64_value("capacity_penalty_strength", 0.0)?,
        industry_max_weight_pct: optional_f64_value("industry_max_weight_pct")?,
        capacity_risk_budget: optional_string("capacity_risk_budget")?,
        cash_utilization: optional_string("cash_utilization")?,
        execution_impact_budget: optional_string("execution_impact_budget")?,
        style_risk_budget: optional_string("style_risk_budget")?,
        candidate_risk_filter: optional_string("candidate_risk_filter")?,
        candidate_ranking: optional_string("candidate_ranking")?,
        risk_contribution_control: optional_string("risk_contribution_control")?,
        stress_fill_confidence_exposure: optional_string("stress_fill_confidence_exposure")?,
        rebalance_hysteresis_pct: optional_f64_value("rebalance_hysteresis_pct")?,
        partial_rebalance_ratio: optional_f64_value("partial_rebalance_ratio")?,
        score_candidate_pool_size: match params
            .get("score_candidate_pool_size")
            .or_else(|| template.get("score_candidate_pool_size"))
        {
            None | Some(Value::Null) => None,
            Some(_) => {
                let size = usize_value("score_candidate_pool_size", 0)?;
                if size == 0 {
                    None
                } else {
                    Some(size)
                }
            }
        },
        universe_profile: optional_string("universe_profile")?,
        effective_coverage,
        cost_model: optional_cost_model_from_maps(params, template)?,
        execution_rules: optional_execution_rules_from_maps(params, template)?,
        benchmark,
        market_regime,
        stop_loss_pct: optional_f64_value("stop_loss_pct")?,
        take_profit_pct: optional_f64_value("take_profit_pct")?,
        trailing_stop_pct: optional_f64_value("trailing_stop_pct")?,
        time_stop_days: optional_u32_value("time_stop_days")?,
        reentry_cooldown_days: optional_u32_value("reentry_cooldown_days")?,
        portfolio_drawdown_reduce_start_pct: optional_f64_value(
            "portfolio_drawdown_reduce_start_pct",
        )?,
        portfolio_drawdown_reduce_full_pct: optional_f64_value(
            "portfolio_drawdown_reduce_full_pct",
        )?,
        portfolio_drawdown_min_exposure: optional_f64_value("portfolio_drawdown_min_exposure")?,
        portfolio_drawdown_peak_lookback_days: match params
            .get("portfolio_drawdown_peak_lookback_days")
            .or_else(|| template.get("portfolio_drawdown_peak_lookback_days"))
        {
            None | Some(Value::Null) => None,
            Some(_) => {
                let days = usize_value("portfolio_drawdown_peak_lookback_days", 0)?;
                if days == 0 {
                    None
                } else {
                    Some(days)
                }
            }
        },
        portfolio_drawdown_recovery_start_pct: optional_f64_value(
            "portfolio_drawdown_recovery_start_pct",
        )?,
        portfolio_drawdown_recovery_full_pct: optional_f64_value(
            "portfolio_drawdown_recovery_full_pct",
        )?,
        portfolio_drawdown_recovery_boost: optional_f64_value("portfolio_drawdown_recovery_boost")?,
        portfolio_volatility_target_pct: optional_f64_value("portfolio_volatility_target_pct")?,
        portfolio_volatility_lookback_days: match params
            .get("portfolio_volatility_lookback_days")
            .or_else(|| template.get("portfolio_volatility_lookback_days"))
        {
            None | Some(Value::Null) => None,
            Some(_) => {
                let days = usize_value("portfolio_volatility_lookback_days", 0)?;
                if days == 0 {
                    None
                } else {
                    Some(days)
                }
            }
        },
        portfolio_volatility_min_exposure: optional_f64_value("portfolio_volatility_min_exposure")?,
        portfolio_volatility_max_exposure: optional_f64_value("portfolio_volatility_max_exposure")?,
        portfolio_sharpe_reduce_start: optional_f64_value("portfolio_sharpe_reduce_start")?,
        portfolio_sharpe_reduce_full: optional_f64_value("portfolio_sharpe_reduce_full")?,
        portfolio_sharpe_lookback_days: match params
            .get("portfolio_sharpe_lookback_days")
            .or_else(|| template.get("portfolio_sharpe_lookback_days"))
        {
            None | Some(Value::Null) => None,
            Some(_) => {
                let days = usize_value("portfolio_sharpe_lookback_days", 0)?;
                if days == 0 {
                    None
                } else {
                    Some(days)
                }
            }
        },
        portfolio_sharpe_min_exposure: optional_f64_value("portfolio_sharpe_min_exposure")?,
        start_date: string_value("start_date", None)?,
        end_date: string_value("end_date", None)?,
        initial_capital: f64_value("initial_capital", 1_000_000.0)?,
        mode: optional_string("mode")?.or_else(|| Some("standard".into())),
        persistence_mode: optional_string("persistence_mode")?
            .or_else(|| Some("summary_only".into())),
        return_risk_feature_cache_mode: optional_string("return_risk_feature_cache_mode")?,
        overlay_combo_name: optional_string("overlay_combo_name")?,
        overlay_version: string_value("overlay_version", Some("1.0.0"))?,
        overlay_score_direction: optional_string("overlay_score_direction")?,
        overlay_weight: optional_f64_value("overlay_weight")?,
    })
}


pub(crate) fn optional_cost_model_from_maps(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<Option<CostModelReq>, String> {
    optional_struct_from_merged_maps(params, template, "cost_model")
}


pub(crate) fn optional_execution_rules_from_maps(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<Option<ExecutionRulesReq>, String> {
    let mut merged =
        merged_object_from_maps(params, template, "execution_rules")?.unwrap_or_else(Map::new);
    for source in [template, params] {
        for key in [
            "execution_schedule_profile",
            "execution_carry_policy",
            "execution_daily_target_move_limit_pct",
            "execution_max_carry_days",
        ] {
            if let Some(value) = source.get(key) {
                match value {
                    Value::Null => {}
                    Value::String(raw) if key == "execution_daily_target_move_limit_pct" => {
                        let parsed = raw
                            .parse::<f64>()
                            .map_err(|_| format!("{} must be a string or number", key))?;
                        merged.insert(key.to_string(), json!(parsed));
                    }
                    Value::String(raw) if key == "execution_max_carry_days" => {
                        let parsed = raw
                            .parse::<i64>()
                            .map_err(|_| format!("{} must be a string or number", key))?;
                        merged.insert(key.to_string(), json!(parsed));
                    }
                    Value::String(_) | Value::Number(_) => {
                        merged.insert(key.to_string(), value.clone());
                    }
                    _ => return Err(format!("{} must be a string or number", key)),
                }
            }
        }
    }
    if merged.is_empty() {
        Ok(None)
    } else {
        serde_json::from_value(Value::Object(merged))
            .map(Some)
            .map_err(|error| format!("execution_rules must be a valid object: {}", error))
    }
}


pub(crate) fn optional_struct_from_merged_maps<T>(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
    key: &str,
) -> Result<Option<T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(merged) = merged_object_from_maps(params, template, key)? else {
        return Ok(None);
    };
    serde_json::from_value(Value::Object(merged))
        .map(Some)
        .map_err(|error| format!("{} must be a valid object: {}", key, error))
}


pub(crate) fn merged_object_from_maps(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
    key: &str,
) -> Result<Option<Map<String, Value>>, String> {
    let mut merged = Map::new();
    for value in [template.get(key), params.get(key)].into_iter().flatten() {
        match value {
            Value::Null => {}
            Value::Object(object) => {
                for (object_key, object_value) in object {
                    merged.insert(object_key.clone(), object_value.clone());
                }
            }
            _ => return Err(format!("{} must be a JSON object", key)),
        }
    }
    if merged.is_empty() {
        Ok(None)
    } else {
        Ok(Some(merged))
    }
}


pub(crate) fn build_prediction_trial_request(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<RunPredictionBacktestReq, String> {
    let template = task
        .backtest_template
        .as_object()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    let params = parameters
        .as_object()
        .ok_or_else(|| "trial parameters must be a JSON object".to_string())?;

    let string_value = |name: &str, default: Option<&str>| -> Result<String, String> {
        if let Some(value) = params.get(name).or_else(|| template.get(name)) {
            match value {
                Value::String(value) => Ok(value.clone()),
                Value::Number(value) => Ok(value.to_string()),
                _ => Err(format!("{} must be a string or number", name)),
            }
        } else if let Some(default) = default {
            Ok(default.to_string())
        } else {
            Err(format!("backtest_template.{} is required", name))
        }
    };
    let optional_string = |name: &str| -> Result<Option<String>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(Value::Number(value)) => Ok(Some(value.to_string())),
            _ => Err(format!("{} must be a string or number", name)),
        }
    };
    let usize_value = |name: &str, default: usize| -> Result<usize, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_u64()
                .map(|value| value as usize)
                .ok_or_else(|| format!("{} must be a positive integer", name)),
            Some(Value::String(value)) => value
                .parse::<usize>()
                .map_err(|_| format!("{} must be a positive integer", name)),
            _ => Err(format!("{} must be a positive integer", name)),
        }
    };
    let f64_value = |name: &str, default: f64| -> Result<f64, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Number(value)) => value
                .as_f64()
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };
    let optional_f64_value = |name: &str| -> Result<Option<f64>, String> {
        match params.get(name).or_else(|| template.get(name)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(value)) => value
                .as_f64()
                .map(Some)
                .ok_or_else(|| format!("{} must be a finite number", name)),
            Some(Value::String(value)) => value
                .parse::<f64>()
                .map(Some)
                .map_err(|_| format!("{} must be a finite number", name)),
            _ => Err(format!("{} must be a finite number", name)),
        }
    };

    Ok(RunPredictionBacktestReq {
        prediction_set_id: string_value("prediction_set_id", None)?,
        strategy_version_id: task.strategy_version_id.clone(),
        data_version_id: task.data_version_id.clone(),
        research_dataset_id: optional_string("research_dataset_id")?,
        feature_set_version_id: optional_string("feature_set_version_id")?,
        portfolio_policy_id: optional_string("portfolio_policy_id")?,
        top_n: usize_value("top_n", 20)?,
        rebalance: string_value("rebalance", Some("monthly"))?,
        entry_delay: usize_value("entry_delay", 0)?,
        min_amount: f64_value("min_amount", 0.0)?,
        max_position_pct: f64_value("max_position_pct", 0.10)?,
        skip_top_pct: f64_value("skip_top_pct", 0.0)?,
        max_pairwise_correlation: optional_f64_value("max_pairwise_correlation")?,
        correlation_lookback_days: usize_value("correlation_lookback_days", 60)?,
        kelly_fraction: f64_value("kelly_fraction", 0.0)?,
        kelly_lookback_days: usize_value("kelly_lookback_days", 60)?,
        max_gross_exposure: f64_value("max_gross_exposure", 1.0)?,
        score_direction: string_value("score_direction", Some("descending"))?,
        portfolio_method: string_value("portfolio_method", Some("heuristic"))?,
        risk_budget_lookback_days: usize_value("risk_budget_lookback_days", 60)?,
        capacity_penalty_strength: f64_value("capacity_penalty_strength", 0.0)?,
        industry_max_weight_pct: optional_f64_value("industry_max_weight_pct")?,
        capacity_risk_budget: optional_string("capacity_risk_budget")?,
        cash_utilization: optional_string("cash_utilization")?,
        execution_impact_budget: optional_string("execution_impact_budget")?,
        style_risk_budget: optional_string("style_risk_budget")?,
        candidate_risk_filter: optional_string("candidate_risk_filter")?,
        candidate_ranking: optional_string("candidate_ranking")?,
        risk_contribution_control: optional_string("risk_contribution_control")?,
        stress_fill_confidence_exposure: optional_string("stress_fill_confidence_exposure")?,
        rebalance_hysteresis_pct: optional_f64_value("rebalance_hysteresis_pct")?,
        partial_rebalance_ratio: optional_f64_value("partial_rebalance_ratio")?,
        cost_model: optional_cost_model_from_maps(params, template)?,
        execution_rules: optional_execution_rules_from_maps(params, template)?,
        benchmark: optional_string("benchmark")?.or_else(|| Some("000300.SH".into())),
        start_date: string_value("start_date", None)?,
        end_date: string_value("end_date", None)?,
        initial_capital: f64_value("initial_capital", 1_000_000.0)?,
        mode: optional_string("mode")?.or_else(|| Some("standard".into())),
        persistence_mode: optional_string("persistence_mode")?
            .or_else(|| Some("summary_only".into())),
        market_regime: optional_string("market_regime")?,
        portfolio_volatility_target_pct: optional_f64_value("portfolio_volatility_target_pct")?,
        portfolio_volatility_lookback_days: None,
        portfolio_volatility_min_exposure: optional_f64_value("portfolio_volatility_min_exposure")?,
        portfolio_volatility_max_exposure: optional_f64_value("portfolio_volatility_max_exposure")?,
        trailing_stop_pct: optional_f64_value("trailing_stop_pct")?,
    })
}


pub(crate) fn market_regime_request_from_value(
    value: Option<&Value>,
    default_benchmark: &str,
) -> Result<Option<MarketRegimeBacktestReq>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(policy)) => match policy.as_str() {
            "off" | "none" | "disabled" => Ok(None),
            "professional_default"
            | "drawdown_control_v1"
            | "drawdown_control_v2"
            | "quality_risk_off_v1"
            | "quality_crash_guard_v1"
            | "quality_crash_guard_v2"
            | "quality_crash_guard_v3"
            | "quality_bear_window_guard_v1"
            | "quality_bear_window_guard_v2"
            | "quality_regime_alpha_switch_v1"
            | "quality_regime_alpha_switch_value_v1"
            | "quality_regime_alpha_switch_recovery_v1"
            | "quality_regime_alpha_switch_blend_v1"
            | "quality_regime_alpha_overlay_value_05pct_v1"
            | "quality_regime_alpha_overlay_value_10pct_v1"
            | "quality_regime_alpha_overlay_blend_10pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
            | "quality_all_regime_event_window_sleeve_05pct_v1"
            | "quality_all_regime_event_window_sleeve_10pct_v1"
            | "quality_all_regime_event_window_sleeve_15pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1"
            | "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1"
            | "quality_bear_position_guard_v1"
            | "quality_bear_position_guard_v2"
            | "quality_bear_position_guard_v3"
            | "quality_event_window_position_guard_v1"
            | "quality_event_window_position_guard_v2"
            | "quality_event_window_position_guard_v3"
            | "quality_event_window_return_sharpe_router_v1"
            | "quality_event_window_return_sharpe_router_v2"
            | "quality_event_window_return_sharpe_router_v3"
            | "quality_event_window_return_sharpe_router_v4"
            | "quality_state_alpha_selector_v1"
            | "quality_state_alpha_selector_v2"
            | "quality_state_alpha_selector_v3"
            | "quality_state_alpha_overlay_selector_v1"
            | "quality_state_alpha_overlay_selector_v2"
            | "quality_state_alpha_overlay_selector_v3"
            | "quality_state_sharpe_bridge_router_v1"
            | "quality_state_sharpe_bridge_router_v2"
            | "quality_state_sharpe_bridge_router_v3"
            | "quality_frontier_regime_bridge_router_v1"
            | "quality_frontier_regime_bridge_router_v2"
            | "quality_frontier_regime_bridge_router_v3"
            | "quality_frontier_regime_bridge_router_v4"
            | "quality_frontier_regime_bridge_router_v5"
            | "quality_frontier_regime_bridge_router_v6"
            | "quality_frontier_regime_bridge_router_v7"
            | "quality_mixed_event_state_selector_v1"
            | "quality_mixed_event_state_selector_v2"
            | "quality_mixed_event_state_overlay_selector_v1"
            | "quality_mixed_event_state_overlay_selector_v2"
            | "quality_mixed_orthogonal_alpha_selector_v1"
            | "quality_mixed_orthogonal_alpha_selector_v2"
            | "quality_mixed_orthogonal_alpha_selector_v3"
            | "quality_mixed_orthogonal_risk_memory_router_v1"
            | "quality_mixed_orthogonal_risk_memory_router_v2"
            | "quality_mixed_orthogonal_risk_memory_router_v3"
            | "quality_nonlinear_alpha_router_v1"
            | "quality_nonlinear_alpha_router_v2"
            | "quality_nonlinear_alpha_risk_memory_router_v1"
            | "quality_nonlinear_alpha_risk_memory_router_v2"
            | "quality_nonlinear_alpha_risk_memory_router_v3"
            | "quality_mixed_state_risk_memory_router_v1"
            | "quality_mixed_state_risk_memory_router_v2"
            | "quality_mixed_state_risk_memory_router_v3"
            | "quality_mixed_state_risk_memory_router_v4"
            | "quality_mixed_state_risk_memory_router_v5"
            | "quality_mixed_state_risk_memory_router_v6"
            | "quality_mixed_state_risk_memory_router_v7"
            | "quality_mixed_state_risk_memory_router_v8"
            | "quality_mixed_state_risk_memory_router_v9"
            | "quality_mixed_state_risk_memory_router_v10"
            | "quality_mixed_state_risk_memory_router_v11"
            | "quality_mixed_state_risk_memory_router_v12"
            | "quality_mixed_state_risk_memory_router_v13"
            | "quality_mixed_state_risk_memory_router_v14"
            | "quality_mixed_state_risk_memory_router_v15"
            | "quality_mixed_state_risk_memory_router_v16"
            | "quality_mixed_state_risk_memory_router_v17"
            | "quality_mixed_state_risk_memory_router_v18" => Ok(Some(MarketRegimeBacktestReq {
                enabled: Some(true),
                policy: Some(policy.to_string()),
                benchmark: Some(default_benchmark.to_string()),
                lookback_days: None,
                min_observations: None,
            })),
            other => Err(format!("unsupported market_regime policy: {}", other)),
        },
        Some(Value::Object(map)) => {
            let enabled = map
                .get("enabled")
                .and_then(Value::as_bool)
                .or_else(|| map.get("active").and_then(Value::as_bool));
            let benchmark = map
                .get("benchmark")
                .and_then(Value::as_str)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .or_else(|| Some(default_benchmark.to_string()));
            let policy = map
                .get("policy")
                .or_else(|| map.get("name"))
                .and_then(Value::as_str)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let lookback_days = optional_usize_from_object(map, "lookback_days")?;
            let min_observations = optional_usize_from_object(map, "min_observations")?;
            Ok(Some(MarketRegimeBacktestReq {
                enabled,
                policy,
                benchmark,
                lookback_days,
                min_observations,
            }))
        }
        Some(_) => Err("market_regime must be a string, object, or null".to_string()),
    }
}


pub(crate) fn optional_usize_from_object(
    map: &Map<String, Value>,
    key: &str,
) -> Result<Option<usize>, String> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .map(|value| Some(value as usize))
            .ok_or_else(|| format!("market_regime.{} must be a positive integer", key)),
        Some(Value::String(value)) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("market_regime.{} must be a positive integer", key)),
        Some(_) => Err(format!("market_regime.{} must be a positive integer", key)),
    }
}


pub(crate) struct ReusableTrial {
    backtest_task_id: Option<String>,
    score: Decimal,
    metrics: Value,
    constraint_violations: Value,
}


pub(crate) async fn find_reusable_trial(
    db: &sqlx::PgPool,
    task_id: &str,
    task: &OptimizationTaskExecutionContext,
    current_trial_id: &str,
    parameters: &Value,
) -> Result<Option<ReusableTrial>, String> {
    let reuse_key = trial_reuse_key(
        &task.strategy_version_id,
        &task.data_version_id,
        &task.backtest_template,
        parameters,
    );
    let rows = sqlx::query_as::<_, (String, Value, Option<String>, Decimal, Value, Value)>(
        "SELECT trial_id, parameters, backtest_task_id, score, metrics, constraint_violations
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND trial_id <> $2
           AND status = 'completed'
           AND score IS NOT NULL
           AND metrics IS NOT NULL
         ORDER BY completed_at ASC NULLS LAST, trial_index ASC",
    )
    .bind(task_id)
    .bind(current_trial_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to find reusable optimization trial: {}", error))?;

    Ok(rows.into_iter().find_map(|row| {
        let other_key = trial_reuse_key(
            &task.strategy_version_id,
            &task.data_version_id,
            &task.backtest_template,
            &row.1,
        );
        if other_key == reuse_key {
            Some(ReusableTrial {
                backtest_task_id: row.2,
                score: row.3,
                metrics: row.4,
                constraint_violations: row.5,
            })
        } else {
            None
        }
    }))
}


pub(crate) async fn mark_trial_reused(
    db: &sqlx::PgPool,
    trial_id: &str,
    reused: &ReusableTrial,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE optimization_trial
         SET status = 'completed', progress = 100, backtest_task_id = $2,
             score = $3, metrics = $4, constraint_violations = $5,
             completed_at = now(), last_heartbeat_at = now(),
             error_message = 'reused completed trial with same strategy/data/template/parameters'
         WHERE trial_id = $1",
    )
    .bind(trial_id)
    .bind(reused.backtest_task_id.as_deref())
    .bind(reused.score)
    .bind(&reused.metrics)
    .bind(&reused.constraint_violations)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to mark trial reused: {}", error))?;
    Ok(())
}


pub(crate) fn trial_reuse_key(
    strategy_version_id: &str,
    data_version_id: &str,
    backtest_template: &Value,
    parameters: &Value,
) -> String {
    format!(
        "{}|{}|{}|{}",
        strategy_version_id,
        data_version_id,
        canonical_json(backtest_template),
        canonical_json(parameters)
    )
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


pub(crate) fn score_trial(
    metrics: &quant_backtest::metrics::BacktestMetrics,
    objective: &Value,
    constraints: Option<&Value>,
) -> ScoredTrial {
    let objective_type = objective
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("risk_adjusted");
    let mut score = if objective_type == "professional_candidate" {
        professional_candidate_objective_score(metrics, constraints)
    } else {
        metrics.information_ratio + metrics.excess_return_pct * Decimal::new(2, 1)
    };
    let mut violations = Vec::new();

    if let Some(limit) = constraint_decimal(constraints, "max_drawdown") {
        if metrics.max_drawdown_pct > limit {
            let excess = metrics.max_drawdown_pct - limit;
            score -= excess * Decimal::new(5, 1);
            violations.push(json!({
                "constraint": "max_drawdown",
                "limit": limit,
                "actual": metrics.max_drawdown_pct,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_annual_return") {
        if metrics.annual_return_pct < limit {
            let gap = limit - metrics.annual_return_pct;
            score -= gap * Decimal::new(4, 1);
            violations.push(json!({
                "constraint": "min_annual_return",
                "limit": limit,
                "actual": metrics.annual_return_pct,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_excess_return") {
        if metrics.excess_return_pct <= limit {
            let gap = limit - metrics.excess_return_pct;
            score -= gap.max(Decimal::ZERO) * Decimal::new(2, 1);
            violations.push(json!({
                "constraint": "min_excess_return",
                "limit": limit,
                "actual": metrics.excess_return_pct,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_sharpe") {
        if metrics.sharpe_ratio <= limit {
            let gap = limit - metrics.sharpe_ratio;
            score -= gap.max(Decimal::ZERO);
            violations.push(json!({
                "constraint": "min_sharpe",
                "limit": limit,
                "actual": metrics.sharpe_ratio,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_sortino") {
        if metrics.sortino_ratio < limit {
            let gap = limit - metrics.sortino_ratio;
            score -= gap;
            violations.push(json!({
                "constraint": "min_sortino",
                "limit": limit,
                "actual": metrics.sortino_ratio,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_calmar") {
        if metrics.calmar_ratio <= limit {
            let gap = limit - metrics.calmar_ratio;
            score -= gap.max(Decimal::ZERO) * Decimal::new(2, 0);
            violations.push(json!({
                "constraint": "min_calmar",
                "limit": limit,
                "actual": metrics.calmar_ratio,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_profit_factor") {
        if metrics.profit_factor < limit {
            let gap = limit - metrics.profit_factor;
            score -= gap.max(Decimal::ZERO);
            violations.push(json!({
                "constraint": "min_profit_factor",
                "limit": limit,
                "actual": metrics.profit_factor,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "max_turnover") {
        if metrics.turnover > limit {
            let excess = metrics.turnover - limit;
            score -= excess * Decimal::new(2, 1);
            violations.push(json!({
                "constraint": "max_turnover",
                "limit": limit,
                "actual": metrics.turnover,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_i64(constraints, "min_trade_count") {
        if (metrics.num_trades as i64) < limit {
            score -=
                Decimal::new((limit - metrics.num_trades as i64).max(0), 0) * Decimal::new(1, 1);
            violations.push(json!({
                "constraint": "min_trade_count",
                "limit": limit,
                "actual": metrics.num_trades,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_i64(constraints, "max_drawdown_duration_days") {
        if (metrics.max_drawdown_duration_days as i64) > limit {
            score -= Decimal::new(
                (metrics.max_drawdown_duration_days as i64 - limit).max(0),
                0,
            ) * Decimal::new(1, 3);
            violations.push(json!({
                "constraint": "max_drawdown_duration_days",
                "limit": limit,
                "actual": metrics.max_drawdown_duration_days,
                "severity": "hard"
            }));
        }
    }
    if let Some(limit) = constraint_decimal(constraints, "min_information_ratio") {
        if metrics.information_ratio < limit {
            let gap = limit - metrics.information_ratio;
            score -= gap;
            violations.push(json!({
                "constraint": "min_information_ratio",
                "limit": limit,
                "actual": metrics.information_ratio,
                "severity": "hard"
            }));
        }
    }

    ScoredTrial {
        score,
        metrics: json!({
            "total_return_pct": metrics.total_return_pct,
            "annual_return_pct": metrics.annual_return_pct,
            "sharpe_ratio": metrics.sharpe_ratio,
            "sortino_ratio": metrics.sortino_ratio,
            "calmar_ratio": metrics.calmar_ratio,
            "max_drawdown_pct": metrics.max_drawdown_pct,
            "max_drawdown_duration_days": metrics.max_drawdown_duration_days,
            "annualized_volatility": metrics.annualized_volatility,
            "benchmark_return_pct": metrics.benchmark_return_pct,
            "excess_return_pct": metrics.excess_return_pct,
            "information_ratio": metrics.information_ratio,
            "turnover": metrics.turnover,
            "num_trades": metrics.num_trades,
            "win_rate_pct": metrics.win_rate_pct,
            "profit_factor": metrics.profit_factor,
            "execution_schedule_expired_count": metrics.execution_schedule_expired_count,
            "execution_schedule_roll_forward_count": metrics.execution_schedule_roll_forward_count,
            "max_execution_target_gap_pct": metrics.max_execution_target_gap_pct,
            "final_cash_weight_pct": metrics.final_cash_weight_pct,
            "final_target_gross_exposure_pct": metrics.final_target_gross_exposure_pct,
            "final_actual_gross_exposure_pct": metrics.final_actual_gross_exposure_pct,
            "final_unfilled_target_gap_pct": metrics.final_unfilled_target_gap_pct,
            "final_execution_fill_ratio": metrics.final_execution_fill_ratio,
        }),
        constraint_violations: Value::Array(violations),
    }
}


pub(crate) fn professional_candidate_objective_score(
    metrics: &quant_backtest::metrics::BacktestMetrics,
    constraints: Option<&Value>,
) -> Decimal {
    let min_annual_return =
        constraint_decimal(constraints, "min_annual_return").unwrap_or(Decimal::new(15, 2));
    let min_excess_return =
        constraint_decimal(constraints, "min_excess_return").unwrap_or(Decimal::ZERO);
    let min_sharpe = constraint_decimal(constraints, "min_sharpe").unwrap_or(Decimal::ONE);
    let min_sortino = constraint_decimal(constraints, "min_sortino").unwrap_or(Decimal::new(15, 1));
    let min_calmar = constraint_decimal(constraints, "min_calmar");
    let max_drawdown =
        constraint_decimal(constraints, "max_drawdown").unwrap_or(Decimal::new(35, 2));

    let annual_gap = positive_decimal_gap(min_annual_return, metrics.annual_return_pct);
    let excess_gap = positive_decimal_gap(min_excess_return, metrics.excess_return_pct);
    let sharpe_gap = positive_decimal_gap(min_sharpe, metrics.sharpe_ratio);
    let sortino_gap = positive_decimal_gap(min_sortino, metrics.sortino_ratio);
    let calmar_gap = min_calmar
        .map(|limit| positive_decimal_gap(limit, metrics.calmar_ratio))
        .unwrap_or(Decimal::ZERO);
    let drawdown_gap = positive_decimal_gap(metrics.max_drawdown_pct, max_drawdown);
    let target_gap_score = annual_gap * Decimal::new(4, 0)
        + excess_gap * Decimal::new(2, 0)
        + sharpe_gap * Decimal::new(6, 0)
        + sortino_gap * Decimal::new(2, 0)
        + calmar_gap * Decimal::new(3, 0)
        + drawdown_gap * Decimal::new(5, 0);

    let annual_floor_passed = metrics.annual_return_pct >= min_annual_return;
    let excess_floor_passed = metrics.excess_return_pct > min_excess_return;
    let drawdown_passed = metrics.max_drawdown_pct < max_drawdown;
    let sortino_passed = metrics.sortino_ratio >= min_sortino;
    let calmar_passed = min_calmar
        .map(|limit| metrics.calmar_ratio > limit)
        .unwrap_or(false);

    let floor_bonus = [
        annual_floor_passed,
        excess_floor_passed,
        drawdown_passed,
        sortino_passed,
        calmar_passed,
    ]
    .into_iter()
    .filter(|passed| *passed)
    .count() as i64;

    Decimal::new(floor_bonus, 0) - target_gap_score
        + metrics.sharpe_ratio * Decimal::new(3, 0)
        + metrics.sortino_ratio * Decimal::new(1, 0)
        + metrics.calmar_ratio * Decimal::new(5, 1)
        + metrics.annual_return_pct
        + metrics.excess_return_pct * Decimal::new(1, 2)
        - metrics.max_drawdown_pct
}


pub(crate) fn positive_decimal_gap(limit: Decimal, actual: Decimal) -> Decimal {
    (limit - actual).max(Decimal::ZERO)
}


pub(crate) fn score_trial_with_output(
    output: &FactorBacktestRunOutput,
    objective: &Value,
    constraints: Option<&Value>,
) -> ScoredTrial {
    let mut scored = score_trial(&output.metrics, objective, constraints);
    if let Some(coverage) = output.effective_coverage.as_ref() {
        if let Some(metrics) = scored.metrics.as_object_mut() {
            metrics.insert("effective_coverage".to_string(), json!(coverage));
        }
    }
    scored
}


pub(crate) fn sample_parameter(name: &str, spec: &Value, rng: &mut DeterministicRng) -> Result<Value, String> {
    let spec = spec
        .as_object()
        .ok_or_else(|| format!("search_space.{} must be an object", name))?;
    let kind = spec
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("search_space.{}.type is required", name))?;

    match kind {
        "int" => {
            let min = spec
                .get("min")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("search_space.{}.min is required", name))?;
            let max = spec
                .get("max")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("search_space.{}.max is required", name))?;
            if min > max {
                return Err(format!("search_space.{} min must be <= max", name));
            }
            Ok(json!(rng.gen_i64(min, max)))
        }
        "float" => {
            let min = spec
                .get("min")
                .and_then(Value::as_f64)
                .ok_or_else(|| format!("search_space.{}.min is required", name))?;
            let max = spec
                .get("max")
                .and_then(Value::as_f64)
                .ok_or_else(|| format!("search_space.{}.max is required", name))?;
            if min > max {
                return Err(format!("search_space.{} min must be <= max", name));
            }
            Ok(json!(min + (max - min) * rng.gen_f64()))
        }
        "choice" => {
            let values = spec
                .get("values")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("search_space.{}.values is required", name))?;
            if values.is_empty() {
                return Err(format!("search_space.{}.values must not be empty", name));
            }
            Ok(values[rng.gen_usize(values.len())].clone())
        }
        other => Err(format!("unsupported search_space.{}.type: {}", name, other)),
    }
}


pub(crate) struct DeterministicRng {
    state: u64,
}


impl DeterministicRng {
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn gen_f64(&mut self) -> f64 {
        let value = self.next_u64() >> 11;
        value as f64 / ((1u64 << 53) as f64)
    }

    fn gen_i64(&mut self, min: i64, max: i64) -> i64 {
        let span = (max - min + 1) as u64;
        min + (self.next_u64() % span) as i64
    }

    pub(crate) fn gen_usize(&mut self, upper_exclusive: usize) -> usize {
        ((self.next_u64() >> 32) as usize) % upper_exclusive
    }
}


