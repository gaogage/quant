//! Optimization API routes — create optimization tasks and parameter trials

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::{Duration, NaiveDate};
use quant_common::phase7::{
    build_layered_search_plan, CandidateMetrics, CandidateTargets, CandidateType,
    LayeredSearchConfig, LayeredSearchPlan, LocalResourcePlan,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::error;
use uuid::Uuid;

use crate::routes::backtest::{
    execute_factor_backtest_with_caches, execute_prediction_backtest,
    prewarm_factor_signal_cache_for_requests, CostModelReq, EffectiveCoverageReq,
    ExecutionRulesReq, FactorBacktestRunOutput, MarketRegimeBacktestReq, RunFactorBacktestReq,
    RunPredictionBacktestReq,
};
use crate::routes::ml::{
    create_walk_forward_nonlinear_quantile_ranker_inner, train_nonlinear_quantile_ranker_inner,
    LinearFactorRef, TrainNonlinearQuantileRankerRequest,
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

#[derive(Debug, Deserialize)]
pub struct CreateOptimizationRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub search_method: String,
    pub search_space: Value,
    pub objective: Value,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    #[serde(default = "default_random_seed")]
    pub random_seed: u64,
    #[serde(default = "default_max_trials")]
    pub max_trials: usize,
}

#[derive(Debug, Deserialize)]
pub struct Phase7LayeredOptimizationRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub objective: Value,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    pub prediction_set_ids: Option<Vec<String>>,
    pub max_trials: Option<usize>,
    pub search_profile: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Phase7ProfessionalDiscoveryRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub objective: Option<Value>,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    pub prediction_set_ids: Option<Vec<String>>,
    pub max_trials: Option<usize>,
    pub search_profile: Option<String>,
    pub trial_batch_limit: Option<i64>,
    pub max_batches: Option<usize>,
    pub robustness_top_n: Option<usize>,
    pub robustness_gate_policy: Option<Value>,
    pub stop_after_professional_candidate: Option<bool>,
    pub stop_after_robust_approval: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OosWalkForwardDiscoveryRequest {
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub objective: Option<Value>,
    pub constraints: Option<Value>,
    pub walk_forward: Option<Value>,
    pub backtest_template: Option<Value>,
    pub prediction_set_ids: Option<Vec<String>>,
    pub max_trials_per_window: Option<usize>,
    pub search_profile: Option<String>,
    pub trial_batch_limit: Option<i64>,
    pub trial_concurrency: Option<usize>,
    pub train_cache_mode: Option<String>,
    pub max_batches_per_window: Option<usize>,
    pub train_window_days: Option<i64>,
    pub test_window_days: Option<i64>,
    pub step_days: Option<i64>,
    pub validation_mode: Option<String>,
    pub in_sample_ratio: Option<f64>,
    pub include_partial_last_window: Option<bool>,
    pub plan_only: Option<bool>,
    pub execution_mode: Option<String>,
    pub exhaustive_search: Option<bool>,
    pub require_train_robustness_approval: Option<bool>,
    pub train_robustness_gate_policy: Option<Value>,
    pub train_selection_gate_policy: Option<Value>,
    pub final_promotion_gate_policy: Option<Value>,
    pub min_stitched_oos_calmar: Option<f64>,
    pub min_positive_oos_window_ratio: Option<f64>,
    pub min_oos_window_count: Option<usize>,
    pub oos_top_n: Option<usize>,
    pub enable_cost_capacity_perturbation_gate: Option<bool>,
    pub cost_capacity_perturbations: Option<Vec<OosCostCapacityPerturbationRequest>>,
    pub min_cost_capacity_perturbation_pass_ratio: Option<f64>,
    pub min_perturbed_oos_calmar: Option<f64>,
    pub max_perturbed_oos_drawdown_pct: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Phase7OosProfileComparisonPlanRequest {
    pub base: Phase7OosWalkForwardDiscoveryRequest,
    pub profiles: Option<Vec<String>>,
    pub return_risk_cache_comparison: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OosCostCapacityPerturbationRequest {
    pub name: Option<String>,
    pub cost_multiplier: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub impact_cost_coefficient: Option<f64>,
    pub max_participation_rate: Option<f64>,
    pub capacity_penalty_strength: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct TrialListQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct RunOptimizationRequest {
    pub trial_limit: Option<i64>,
    pub performance_gate: Option<OptimizationPerformanceGateRequest>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OptimizationPerformanceGateRequest {
    pub min_completed_trials: Option<i64>,
    pub max_failed_trials: Option<i64>,
    pub max_elapsed_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PromoteOptimizationRequest {
    pub trial_id: Option<String>,
    pub target_strategy_version: String,
    pub candidate_name: String,
    pub promotion_mode: Option<String>,
    pub gate_policy: Option<String>,
    pub freeze_after_approval: Option<bool>,
    pub reviewer: Option<String>,
    pub reason: String,
    pub notes: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateRobustnessRequest {
    pub gate_policy: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct EliteValidationReportRequest {
    pub top_n: Option<usize>,
    pub gate_policy: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ReturnRiskCacheEconomicsReportRequest {
    pub raw_experiment_run_id: String,
    pub stats_experiment_run_id: String,
}

#[derive(Clone)]
struct OptimizationTaskExecutionContext {
    strategy_version_id: String,
    data_version_id: String,
    backtest_template: Value,
    objective: Value,
    constraints: Option<Value>,
}

struct ScoredTrial {
    score: Decimal,
    metrics: Value,
    constraint_violations: Value,
}

struct RobustnessEvaluation {
    status: String,
    gates: Value,
}

fn backtest_cache_stats_delta(
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
struct RobustnessDailyPoint {
    trade_date: NaiveDate,
    portfolio_value: f64,
    benchmark_value: Option<f64>,
}

#[derive(Debug, Clone)]
struct RobustnessMetricSummary {
    total_return: f64,
    annual_return: f64,
    sharpe_ratio: f64,
    sortino_ratio: f64,
    calmar_ratio: f64,
    max_drawdown: f64,
    benchmark_return: Option<f64>,
    excess_return: Option<f64>,
}

#[derive(Debug, Clone)]
struct RobustnessTimeSeriesAnalysis {
    market_scenarios: Value,
    walk_forward: Value,
    bootstrap: Value,
}

#[derive(Debug, Clone)]
struct OosDiscoveryWindow {
    window_index: usize,
    validation_mode: String,
    train_start: NaiveDate,
    train_end: NaiveDate,
    test_start: NaiveDate,
    test_end: NaiveDate,
}

#[derive(Debug, Clone)]
struct OosDiscoveryPlan {
    validation_mode: String,
    start_date: NaiveDate,
    end_date: NaiveDate,
    train_window_days: i64,
    test_window_days: i64,
    step_days: i64,
    in_sample_ratio: f64,
    include_partial_last_window: bool,
    windows: Vec<OosDiscoveryWindow>,
}

struct OosWindowExecution {
    window: OosDiscoveryWindow,
    train_optimization_task_id: String,
    train_execution_policy: OosTrainExecutionPolicy,
    train_batches: Vec<Value>,
    selected_candidate: DiscoveryCandidate,
    train_robustness: Option<Value>,
    train_cost_capacity_perturbations: Vec<OosCostCapacityPerturbationResult>,
    oos_backtest_task_id: String,
    oos_output: FactorBacktestRunOutput,
    oos_points: Vec<RobustnessDailyPoint>,
    cost_capacity_perturbations: Vec<OosCostCapacityPerturbationResult>,
}

#[derive(Debug, Clone)]
struct TrainWindowMlPredictionSets {
    train_prediction_set_id: String,
    test_prediction_set_id: String,
    training_task_id: String,
}

struct OosCostCapacityPerturbationResult {
    name: String,
    perturbation: OosCostCapacityPerturbationRequest,
    backtest_task_id: String,
    output: FactorBacktestRunOutput,
    passed: bool,
}

struct OosTrainCandidateEvaluation {
    candidate: DiscoveryCandidate,
    robustness: Value,
    train_cost_capacity_perturbations: Vec<OosCostCapacityPerturbationResult>,
    stress_summary: CostCapacityPerturbationSummary,
    stress_adjusted_score: Decimal,
    train_cost_gate_passed: bool,
}

#[derive(Debug, Clone)]
struct CostCapacityPerturbationSummary {
    passed_count: usize,
    total_count: usize,
    pass_ratio_ppm: i64,
    min_calmar: Decimal,
    avg_calmar: Decimal,
    max_drawdown: Decimal,
    min_annual_return: Decimal,
    avg_sharpe: Decimal,
    min_sortino: Decimal,
    min_num_trades: u64,
    max_final_cash_weight: Decimal,
    avg_final_cash_weight: Decimal,
    min_final_actual_gross_exposure: Decimal,
    max_final_unfilled_target_gap: Decimal,
    avg_final_unfilled_target_gap: Decimal,
    min_final_execution_fill_ratio: Decimal,
    avg_final_execution_fill_ratio: Decimal,
    max_execution_target_gap: Decimal,
    max_execution_schedule_expired_count: usize,
    total_execution_schedule_expired_count: usize,
}

#[derive(Debug, Clone)]
struct PredictionConfidenceStressFillQualityScoreBreakdown {
    total_score: Decimal,
    base_score: Decimal,
    stress_fill_objective_score: Decimal,
    prediction_confidence_score: Decimal,
    target_annual_return: Decimal,
    min_annual_return: Decimal,
    min_calmar: Decimal,
    min_sharpe: Decimal,
    min_sortino: Decimal,
    pass_ratio_bonus: Decimal,
    annual_quality_bonus: Decimal,
    calmar_quality_bonus: Decimal,
    sharpe_quality_bonus: Decimal,
    sortino_quality_bonus: Decimal,
    drawdown_tail_penalty: Decimal,
    min_annual_shortfall: Decimal,
    target_annual_shortfall: Decimal,
    calmar_shortfall: Decimal,
    sharpe_shortfall: Decimal,
    sortino_shortfall: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct OosCostCapacityGateConfig {
    enabled: bool,
    min_pass_ratio: f64,
    min_perturbed_calmar: f64,
    max_perturbed_drawdown_pct: f64,
}

struct OptimizationPerformanceGatePolicy {
    min_completed_trials: i64,
    max_failed_trials: i64,
    max_elapsed_ms: Option<i64>,
}

struct TrialExecutionOutcome {
    completed: i64,
    failed: i64,
    signal_cache_stats: SignalDataCacheStats,
    backtest_cache_stats: BacktestDataCacheStats,
    signal_cache_snapshot: Option<SignalDataCacheSnapshot>,
    backtest_cache_snapshot: Option<BacktestDataCacheSnapshot>,
}

struct RobustnessOverlayPersistenceFields {
    gate_result_id: String,
    status: String,
    gate_results: Value,
}

struct NormalizedPromoteRequest {
    trial_id: String,
    target_strategy_version: String,
    candidate_name: String,
    promotion_mode: String,
    gate_policy: String,
    freeze_after_approval: bool,
    reviewer: Option<String>,
    reason: String,
    notes: Option<String>,
    status: String,
}

struct Phase7LayeredPlanBundle {
    resource_plan: LocalResourcePlan,
    plan: LayeredSearchPlan,
    search_space: Value,
}

#[derive(Debug, Clone)]
struct DiscoveryCandidate {
    trial_id: String,
    backtest_task_id: Option<String>,
    score: Option<Decimal>,
    candidate_type: CandidateType,
    professional_gap_score: Decimal,
    metrics: CandidateMetrics,
    parameters: Value,
}

#[derive(Debug, Clone)]
struct CompletedTrialSnapshot {
    trial_id: String,
    trial_index: i32,
    backtest_task_id: Option<String>,
    score: Option<Decimal>,
    metrics: Value,
    metric_sources: Value,
    missing_elite_metrics: Value,
    constraint_violations: Value,
    parameters: Value,
}

#[derive(Debug, Clone, Copy)]
struct EliteMetricProfile {
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

enum OptimizationTrialBacktestRequest {
    Factor(RunFactorBacktestReq),
    Prediction(RunPredictionBacktestReq),
}

struct FactorSignalBatchPrewarmPlan {
    requested_trials: usize,
    factor_requests: Vec<RunFactorBacktestReq>,
    prediction_trials: usize,
    invalid_trials: usize,
}

fn default_random_seed() -> u64 {
    42
}

fn default_max_trials() -> usize {
    20
}

fn normalize_max_trials(value: usize) -> usize {
    value.clamp(1, 500)
}

fn normalize_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 500)
}

fn default_professional_robustness_policy() -> Value {
    json!({
        "candidate_tier": "professional_observation",
        "min_trade_count": 1,
        "min_annual_return": 0.15,
        "min_excess_return": 0.0,
        "min_sharpe": 1.0,
        "min_sortino": 1.5,
        "max_drawdown": 0.35,
        "min_score_gap": 0.0,
        "walk_forward_window_days": 756,
        "walk_forward_step_days": 63,
        "min_walk_forward_windows": 4,
        "min_positive_excess_window_ratio": 0.50,
        "min_positive_annual_return_window_ratio": 0.60,
        "min_walk_forward_median_sharpe": 0.30,
        "min_bootstrap_positive_return_probability": 0.70,
        "min_bootstrap_sharpe_p05": 0.0,
        "min_market_scenarios": 2,
        "bootstrap_trials": 512,
        "bootstrap_seed": 42
    })
}

fn default_professional_elite_robustness_policy() -> Value {
    json!({
        "candidate_tier": "professional_elite",
        "min_trade_count": 200,
        "min_annual_return": 0.15,
        "min_excess_return": 0.0,
        "min_sharpe": 1.5,
        "min_sortino": 1.8,
        "min_calmar": 2.0,
        "min_profit_factor": 1.5,
        "max_drawdown_duration_days": 126,
        "max_drawdown": 0.35,
        "min_score_gap": 0.0,
        "walk_forward_window_days": 756,
        "walk_forward_step_days": 63,
        "min_walk_forward_windows": 4,
        "min_positive_excess_window_ratio": 0.50,
        "min_positive_annual_return_window_ratio": 0.60,
        "min_walk_forward_median_sharpe": 0.80,
        "min_walk_forward_median_calmar": 1.2,
        "min_bootstrap_positive_return_probability": 0.80,
        "min_bootstrap_sharpe_p05": 0.0,
        "min_bootstrap_calmar_p05": 0.0,
        "max_bootstrap_drawdown_p95": 0.35,
        "min_market_scenarios": 2,
        "bootstrap_trials": 1000,
        "bootstrap_seed": 42
    })
}

fn default_oos_train_selection_gate_policy() -> Value {
    json!({
        "candidate_tier": "oos_train_selection",
        "enforce_trial_constraint_violations": false,
        "min_trade_count": 20,
        "min_annual_return": 0.0,
        "min_sharpe": 0.0,
        "max_drawdown": 0.50,
        "min_score_gap": -999.0,
        "walk_forward_window_days": 504,
        "walk_forward_step_days": 126,
        "min_walk_forward_windows": 1,
        "min_positive_excess_window_ratio": 0.0,
        "min_positive_annual_return_window_ratio": 0.50,
        "min_walk_forward_median_sharpe": 0.0,
        "min_bootstrap_positive_return_probability": 0.50,
        "min_bootstrap_sharpe_p05": -1.0,
        "min_market_scenarios": 1,
        "bootstrap_trials": 256,
        "bootstrap_seed": 42
    })
}

fn default_oos_train_selection_gate_policy_for_search_profile(
    search_profile: Option<&str>,
) -> Value {
    let base = default_oos_train_selection_gate_policy();
    match search_profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_default()
    {
        "professional_execution_rolling_carry_budget"
        | "execution_rolling_carry_budget"
        | "execution_roll_forward_budget"
        | "phase7_execution_rolling_carry_budget"
        | "phase7_dz"
        | "professional_execution_capacity_fill_frontier"
        | "execution_capacity_fill_frontier"
        | "capacity_fill_frontier"
        | "phase7_execution_capacity_fill_frontier"
        | "phase7_ea"
        | "professional_execution_alpha_capacity_bridge"
        | "execution_alpha_capacity_bridge"
        | "alpha_capacity_bridge"
        | "phase7_execution_alpha_capacity_bridge"
        | "phase7_eb"
        | "professional_execution_alpha_capacity_return_frontier"
        | "execution_alpha_capacity_return_frontier"
        | "alpha_capacity_return_frontier"
        | "phase7_execution_alpha_capacity_return_frontier"
        | "phase7_ec"
        | "professional_execution_stress_fill_return_frontier"
        | "execution_stress_fill_return_frontier"
        | "stress_fill_return_frontier"
        | "phase7_execution_stress_fill_return_frontier"
        | "phase7_ed"
        | "professional_execution_stress_risk_budget"
        | "execution_stress_risk_budget"
        | "stress_risk_budget"
        | "phase7_execution_stress_risk_budget"
        | "phase7_ee" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_execution_capacity_stress_return_gate"
        | "execution_capacity_stress_return_gate"
        | "capacity_stress_return_gate"
        | "phase7_execution_capacity_stress_return_gate"
        | "phase7_ef"
        | "professional_execution_low_impact_alpha_stress_return"
        | "execution_low_impact_alpha_stress_return"
        | "low_impact_alpha_stress_return"
        | "phase7_execution_low_impact_alpha_stress_return"
        | "phase7_eg"
        | "professional_execution_stress_target_scaling_return"
        | "execution_stress_target_scaling_return"
        | "stress_target_scaling_return"
        | "phase7_execution_stress_target_scaling_return"
        | "phase7_eh"
        | "professional_execution_stress_floor_scaling_return"
        | "execution_stress_floor_scaling_return"
        | "stress_floor_scaling_return"
        | "phase7_execution_stress_floor_scaling_return"
        | "phase7_ei"
        | "professional_execution_stress_floor_return_recovery"
        | "execution_stress_floor_return_recovery"
        | "stress_floor_return_recovery"
        | "phase7_execution_stress_floor_return_recovery"
        | "phase7_ej"
        | "professional_execution_pressure_headroom_floor"
        | "execution_pressure_headroom_floor"
        | "pressure_headroom_floor"
        | "phase7_execution_pressure_headroom_floor"
        | "phase7_ek"
        | "professional_execution_alpha_headroom_floor"
        | "execution_alpha_headroom_floor"
        | "alpha_headroom_floor"
        | "phase7_execution_alpha_headroom_floor"
        | "phase7_el"
        | "professional_execution_blended_alpha_headroom_floor"
        | "execution_blended_alpha_headroom_floor"
        | "blended_alpha_headroom_floor"
        | "phase7_execution_blended_alpha_headroom_floor"
        | "phase7_em"
        | "professional_execution_event_anchor_stress_bridge"
        | "execution_event_anchor_stress_bridge"
        | "event_anchor_stress_bridge"
        | "phase7_execution_event_anchor_stress_bridge"
        | "phase7_en"
        | "professional_execution_participation_aware_event_anchor"
        | "execution_participation_aware_event_anchor"
        | "participation_aware_event_anchor"
        | "phase7_execution_participation_aware_event_anchor"
        | "phase7_eo"
        | "professional_execution_cl_anchor_fill_recovery"
        | "execution_cl_anchor_fill_recovery"
        | "cl_anchor_fill_recovery"
        | "phase7_execution_cl_anchor_fill_recovery"
        | "phase7_ep"
        | "professional_execution_capacity_aware_candidate_ranking"
        | "execution_capacity_aware_candidate_ranking"
        | "capacity_aware_candidate_ranking"
        | "phase7_execution_capacity_aware_candidate_ranking"
        | "phase7_eq"
        | "professional_execution_pit_capacity_ranking"
        | "execution_pit_capacity_ranking"
        | "pit_capacity_ranking"
        | "phase7_execution_pit_capacity_ranking"
        | "phase7_er"
        | "professional_execution_pit_alpha_first_low_impact"
        | "execution_pit_alpha_first_low_impact"
        | "pit_alpha_first_low_impact"
        | "phase7_execution_pit_alpha_first_low_impact"
        | "phase7_es"
        | "professional_execution_pit_excess_return_recovery"
        | "execution_pit_excess_return_recovery"
        | "pit_excess_return_recovery"
        | "phase7_execution_pit_excess_return_recovery"
        | "phase7_et"
        | "professional_execution_bull_sleeve_cash_recovery"
        | "execution_bull_sleeve_cash_recovery"
        | "bull_sleeve_cash_recovery"
        | "phase7_execution_bull_sleeve_cash_recovery"
        | "phase7_eu"
        | "professional_execution_return_first_fill_repair"
        | "execution_return_first_fill_repair"
        | "return_first_fill_repair"
        | "phase7_execution_return_first_fill_repair"
        | "phase7_ey"
        | "professional_execution_pit_nonlinear_alpha_regime_rebuild"
        | "execution_pit_nonlinear_alpha_regime_rebuild"
        | "pit_nonlinear_alpha_regime_rebuild"
        | "phase7_execution_pit_nonlinear_alpha_regime_rebuild"
        | "phase7_ez"
        | "professional_execution_pit_quality_recovery_alpha"
        | "execution_pit_quality_recovery_alpha"
        | "pit_quality_recovery_alpha"
        | "phase7_execution_pit_quality_recovery_alpha"
        | "phase7_fa"
        | "professional_execution_event_post_return_curve_alpha"
        | "execution_event_post_return_curve_alpha"
        | "event_post_return_curve_alpha"
        | "phase7_execution_event_post_return_curve_alpha"
        | "phase7_fb"
        | "professional_execution_event_reaction_alpha"
        | "execution_event_reaction_alpha"
        | "event_reaction_alpha"
        | "phase7_execution_event_reaction_alpha"
        | "phase7_fc"
        | "professional_execution_broad_financial_feature_discovery"
        | "execution_broad_financial_feature_discovery"
        | "broad_financial_feature_discovery"
        | "phase7_execution_broad_financial_feature_discovery"
        | "phase7_fg"
        | "professional_execution_broad_financial_feature_stratified_discovery"
        | "execution_broad_financial_feature_stratified_discovery"
        | "broad_financial_feature_stratified_discovery"
        | "phase7_execution_broad_financial_feature_stratified_discovery"
        | "phase7_fh"
        | "professional_execution_native_alpha_fusion_discovery"
        | "execution_native_alpha_fusion_discovery"
        | "native_alpha_fusion_discovery"
        | "phase7_execution_native_alpha_fusion_discovery"
        | "phase7_fj"
        | "professional_trainable_alpha_admission_discovery"
        | "trainable_alpha_admission_discovery"
        | "phase7_trainable_alpha_admission"
        | "phase7_ft"
        | "professional_prediction_capacity_dual_objective"
        | "prediction_capacity_dual_objective"
        | "phase7_prediction_capacity_dual_objective"
        | "phase7_fl"
        | "professional_prediction_target_gross_signal_fidelity"
        | "prediction_target_gross_signal_fidelity"
        | "phase7_prediction_target_gross_signal_fidelity"
        | "phase7_fm"
        | "professional_prediction_confidence_turnover_discovery"
        | "prediction_confidence_turnover_discovery"
        | "phase7_prediction_confidence_turnover_discovery"
        | "phase7_fn"
        | "professional_prediction_confidence_alpha_lift"
        | "prediction_confidence_alpha_lift"
        | "phase7_prediction_confidence_alpha_lift"
        | "phase7_fo"
        | "professional_prediction_long_horizon_low_turnover"
        | "prediction_long_horizon_low_turnover"
        | "phase7_prediction_long_horizon_low_turnover"
        | "phase7_fp"
        | "professional_prediction_long_horizon_regime_alpha"
        | "prediction_long_horizon_regime_alpha"
        | "phase7_prediction_long_horizon_regime_alpha"
        | "phase7_fr"
        | "professional_prediction_h60_nonlinear_stress_discovery"
        | "prediction_h60_nonlinear_stress_discovery"
        | "h60_nonlinear_stress_discovery"
        | "phase7_prediction_h60_nonlinear_stress_discovery"
        | "phase7_fw"
        | "professional_prediction_h120_low_impact_stress_discovery"
        | "prediction_h120_low_impact_stress_discovery"
        | "h120_low_impact_stress_discovery"
        | "phase7_prediction_h120_low_impact_stress_discovery"
        | "phase7_fz"
        | "professional_train_window_nonlinear_ranking_discovery"
        | "train_window_nonlinear_ranking_discovery"
        | "phase7_train_window_nonlinear_ranking_discovery"
        | "phase7_fx"
        | "professional_train_window_stress_fill_target_exposure"
        | "train_window_stress_fill_target_exposure"
        | "stress_fill_target_exposure"
        | "phase7_train_window_stress_fill_target_exposure"
        | "phase7_ga" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "capacity_stress_return_score_v1",
                "capacity_stress_target_calmar": 2.0,
                "capacity_stress_target_annual_return": 0.15,
                "min_train_perturbed_annual_return": 0.05,
                "min_train_avg_perturbed_calmar": 1.2,
                "min_train_trade_count": 50,
                "min_train_final_actual_gross_exposure_pct": 0.25,
                "max_train_final_cash_weight_pct": 0.75,
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_train_window_ml_stress_fill_discovery"
        | "train_window_ml_stress_fill_discovery"
        | "ml_stress_fill_discovery"
        | "phase7_train_window_ml_stress_fill_discovery"
        | "phase7_gb" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1",
                "capacity_stress_target_calmar": 2.0,
                "capacity_stress_target_annual_return": 0.15,
                "min_train_perturbed_annual_return": 0.05,
                "min_train_avg_perturbed_calmar": 1.2,
                "min_train_trade_count": 100,
                "min_train_final_actual_gross_exposure_pct": 0.35,
                "max_train_final_cash_weight_pct": 0.65,
                "max_train_final_unfilled_target_gap_pct": 0.06,
                "min_train_final_execution_fill_ratio": 0.95,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_current_event_nonlinear_alpha_discovery"
        | "current_event_nonlinear_alpha_discovery"
        | "phase7_current_event_nonlinear_alpha_discovery"
        | "phase7_fs"
        | "professional_execution_oos_regime_alpha_rebuild"
        | "execution_oos_regime_alpha_rebuild"
        | "oos_regime_alpha_rebuild"
        | "phase7_execution_oos_regime_alpha_rebuild"
        | "phase7_ev"
        | "professional_execution_oos_benchmark_excess_rebuild"
        | "execution_oos_benchmark_excess_rebuild"
        | "oos_benchmark_excess_rebuild"
        | "phase7_execution_oos_benchmark_excess_rebuild"
        | "phase7_ew"
        | "professional_execution_oos_execution_adaptive_rebuild"
        | "execution_oos_execution_adaptive_rebuild"
        | "oos_execution_adaptive_rebuild"
        | "phase7_execution_oos_execution_adaptive_rebuild"
        | "phase7_ex" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "capacity_stress_return_score_v1",
                "capacity_stress_target_calmar": 2.0,
                "capacity_stress_target_annual_return": 0.15,
                "min_train_perturbed_annual_return": 0.05,
                "min_train_avg_perturbed_calmar": 1.2,
                "min_train_trade_count": 50,
                "min_train_final_actual_gross_exposure_pct": 0.25,
                "max_train_final_cash_weight_pct": 0.75,
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_execution_fill_ratio_budget"
        | "execution_fill_ratio_budget"
        | "execution_unfilled_gap_budget"
        | "phase7_execution_fill_ratio_budget"
        | "phase7_dy" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_execution_target_gap_pct": 0.08,
                "max_train_final_unfilled_target_gap_pct": 0.08,
                "min_train_final_execution_fill_ratio": 0.90,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        "professional_execution_cash_drag_aware_budget"
        | "execution_cash_drag_aware_budget"
        | "cash_drag_aware_execution_budget"
        | "phase7_execution_cash_drag_aware_budget"
        | "phase7_dv"
        | "professional_execution_feasible_fill_budget"
        | "execution_feasible_fill_budget"
        | "cash_utilization_execution_budget"
        | "phase7_execution_feasible_fill_budget"
        | "phase7_dx" => merge_gate_policy(
            base,
            &json!({
                "enable_train_cost_capacity_perturbation_gate": true,
                "enable_train_cost_capacity_stress_aware_selection": true,
                "min_train_cost_capacity_perturbation_pass_ratio": 0.80,
                "min_train_perturbed_calmar": 1.2,
                "max_train_perturbed_drawdown_pct": 0.35,
                "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
                "max_train_final_cash_weight_pct": 0.20,
                "max_train_execution_target_gap_pct": 0.08,
                "max_train_execution_schedule_expired_count": 0
            }),
        ),
        _ => base,
    }
}

fn default_oos_final_promotion_gate_policy(validation_mode: &str) -> Value {
    let min_oos_window_count = if validation_mode == "holdout_80_20" {
        1
    } else {
        3
    };
    json!({
        "candidate_tier": "oos_final_promotion",
        "min_stitched_oos_calmar": 1.2,
        "min_positive_oos_window_ratio": 0.60,
        "min_oos_window_count": min_oos_window_count,
        "require_train_selection_approval": true,
        "require_no_train_test_overlap": true,
        "min_cost_capacity_perturbation_pass_ratio": 0.80,
        "min_perturbed_oos_calmar": 1.2,
        "max_perturbed_oos_drawdown_pct": 0.35
    })
}

fn resolve_robustness_gate_policy(gate_policy: Option<&Value>) -> Value {
    match gate_policy {
        Some(policy) => {
            let preset = policy
                .get("preset")
                .or_else(|| policy.get("candidate_tier"))
                .or_else(|| policy.get("tier"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            match preset {
                "elite" | "professional_elite" => {
                    merge_gate_policy(default_professional_elite_robustness_policy(), policy)
                }
                "professional" | "professional_observation" | "observation" | "default" => {
                    merge_gate_policy(default_professional_robustness_policy(), policy)
                }
                _ => policy.clone(),
            }
        }
        None => default_professional_robustness_policy(),
    }
}

fn resolve_oos_train_selection_gate_policy(req: &Phase7OosWalkForwardDiscoveryRequest) -> Value {
    let base =
        default_oos_train_selection_gate_policy_for_search_profile(req.search_profile.as_deref());
    if let Some(policy) = req.train_selection_gate_policy.as_ref() {
        return resolve_oos_train_selection_policy(Some(policy), base);
    }
    if let Some(policy) = req.train_robustness_gate_policy.as_ref() {
        return resolve_robustness_gate_policy(Some(policy));
    }
    base
}

fn resolve_oos_train_selection_policy(gate_policy: Option<&Value>, base: Value) -> Value {
    match gate_policy {
        Some(policy) => {
            let preset = policy
                .get("preset")
                .or_else(|| policy.get("candidate_tier"))
                .or_else(|| policy.get("tier"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            match preset {
                "professional"
                | "professional_observation"
                | "observation"
                | "default"
                | "elite"
                | "professional_elite" => resolve_robustness_gate_policy(Some(policy)),
                "oos_train_selection" | "train_selection" | "relaxed_train_selection" | "" => {
                    merge_gate_policy(base, policy)
                }
                _ => merge_gate_policy(base, policy),
            }
        }
        None => base,
    }
}

fn resolve_oos_final_promotion_gate_policy(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    validation_mode: &str,
) -> Value {
    let mut policy = default_oos_final_promotion_gate_policy(validation_mode);
    if let Some(overrides) = req.final_promotion_gate_policy.as_ref() {
        policy = merge_gate_policy(policy, overrides);
    }
    if let Some(value) = req.min_stitched_oos_calmar {
        insert_policy_number(&mut policy, "min_stitched_oos_calmar", value);
    }
    if let Some(value) = req.min_positive_oos_window_ratio {
        insert_policy_number(&mut policy, "min_positive_oos_window_ratio", value);
    }
    if let Some(value) = req.min_oos_window_count {
        insert_policy_integer(&mut policy, "min_oos_window_count", value as i64);
    }
    if let Some(value) = req.min_cost_capacity_perturbation_pass_ratio {
        insert_policy_number(
            &mut policy,
            "min_cost_capacity_perturbation_pass_ratio",
            value,
        );
    }
    if let Some(value) = req.min_perturbed_oos_calmar {
        insert_policy_number(&mut policy, "min_perturbed_oos_calmar", value);
    }
    if let Some(value) = req.max_perturbed_oos_drawdown_pct {
        insert_policy_number(&mut policy, "max_perturbed_oos_drawdown_pct", value);
    }
    policy
}

fn insert_policy_number(policy: &mut Value, key: &str, value: f64) {
    if let Some(object) = policy.as_object_mut() {
        object.insert(key.to_string(), json!(value));
    }
}

fn insert_policy_integer(policy: &mut Value, key: &str, value: i64) {
    if let Some(object) = policy.as_object_mut() {
        object.insert(key.to_string(), json!(value));
    }
}

fn merge_gate_policy(mut base: Value, overrides: &Value) -> Value {
    if let (Some(base_map), Some(override_map)) = (base.as_object_mut(), overrides.as_object()) {
        for (key, value) in override_map {
            base_map.insert(key.clone(), value.clone());
        }
    }
    base
}

fn phase7_search_config(search_profile: Option<&str>) -> (String, LayeredSearchConfig) {
    match search_profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("local_professional")
    {
        "professional_risk_breakthrough"
        | "risk_breakthrough"
        | "drawdown_sortino_breakthrough"
        | "phase7_risk_breakthrough" => (
            "professional_risk_breakthrough".to_string(),
            LayeredSearchConfig::professional_risk_breakthrough_default(),
        ),
        "professional_sharpe_stabilization"
        | "sharpe_stabilization"
        | "phase7_sharpe_stabilization"
        | "phase7_t" => (
            "professional_sharpe_stabilization".to_string(),
            LayeredSearchConfig::professional_sharpe_stabilization_default(),
        ),
        "professional_regime_stabilization"
        | "regime_stabilization"
        | "phase7_regime_stabilization"
        | "phase7_u" => (
            "professional_regime_stabilization".to_string(),
            LayeredSearchConfig::professional_regime_stabilization_default(),
        ),
        "professional_bear_window_stabilization"
        | "bear_window_stabilization"
        | "phase7_bear_window"
        | "phase7_u2" => (
            "professional_bear_window_stabilization".to_string(),
            LayeredSearchConfig::professional_bear_window_stabilization_default(),
        ),
        "professional_style_risk_budget"
        | "style_risk_budget"
        | "phase7_style_risk_budget"
        | "phase7_v" => (
            "professional_style_risk_budget".to_string(),
            LayeredSearchConfig::professional_style_risk_budget_default(),
        ),
        "professional_second_alpha_source"
        | "second_alpha_source"
        | "phase7_second_alpha_source"
        | "phase7_w" => (
            "professional_second_alpha_source".to_string(),
            LayeredSearchConfig::professional_second_alpha_source_default(),
        ),
        "professional_residual_quality"
        | "residual_quality"
        | "industry_residual_quality"
        | "phase7_residual_quality"
        | "phase7_ag" => (
            "professional_residual_quality".to_string(),
            LayeredSearchConfig::professional_residual_quality_default(),
        ),
        "professional_residual_overlay_sharpe"
        | "residual_overlay_sharpe"
        | "quality_residual_overlay"
        | "phase7_ah" => (
            "professional_residual_overlay_sharpe".to_string(),
            LayeredSearchConfig::professional_residual_overlay_sharpe_default(),
        ),
        "professional_conditioned_second_alpha"
        | "conditioned_second_alpha"
        | "quality_conditioned_second_alpha"
        | "phase7_ak" => (
            "professional_conditioned_second_alpha".to_string(),
            LayeredSearchConfig::professional_conditioned_second_alpha_default(),
        ),
        "professional_valuation_guard_sharpe"
        | "valuation_guard_sharpe"
        | "phase7_valuation_guard"
        | "phase7_al" => (
            "professional_valuation_guard_sharpe".to_string(),
            LayeredSearchConfig::professional_valuation_guard_sharpe_default(),
        ),
        "professional_regime_conditioned_valuation_guard"
        | "regime_conditioned_valuation_guard"
        | "phase7_regime_conditioned_valuation_guard"
        | "phase7_am" => (
            "professional_regime_conditioned_valuation_guard".to_string(),
            LayeredSearchConfig::professional_regime_conditioned_valuation_guard_default(),
        ),
        "professional_regime_alpha_routing"
        | "regime_alpha_routing"
        | "phase7_regime_alpha_routing"
        | "phase7_an" => (
            "professional_regime_alpha_routing".to_string(),
            LayeredSearchConfig::professional_regime_alpha_routing_default(),
        ),
        "professional_regime_alpha_sleeve_search"
        | "regime_alpha_sleeve_search"
        | "phase7_regime_alpha_sleeve_search"
        | "phase7_ao" => (
            "professional_regime_alpha_sleeve_search".to_string(),
            LayeredSearchConfig::professional_regime_alpha_sleeve_search_default(),
        ),
        "professional_regime_alpha_overlay_search"
        | "regime_alpha_overlay_search"
        | "phase7_regime_alpha_overlay_search"
        | "phase7_ap" => (
            "professional_regime_alpha_overlay_search".to_string(),
            LayeredSearchConfig::professional_regime_alpha_overlay_search_default(),
        ),
        "professional_regime_alpha_sleeve_allocation"
        | "regime_alpha_sleeve_allocation"
        | "phase7_regime_alpha_sleeve_allocation"
        | "phase7_aq" => (
            "professional_regime_alpha_sleeve_allocation".to_string(),
            LayeredSearchConfig::professional_regime_alpha_sleeve_allocation_default(),
        ),
        "professional_low_risk_sleeve"
        | "low_risk_sleeve"
        | "phase7_low_risk_sleeve"
        | "phase7_ar" => (
            "professional_low_risk_sleeve".to_string(),
            LayeredSearchConfig::professional_low_risk_sleeve_default(),
        ),
        "professional_value_guard_sleeve_composition"
        | "value_guard_sleeve_composition"
        | "phase7_value_guard_sleeve"
        | "phase7_as" => (
            "professional_value_guard_sleeve_composition".to_string(),
            LayeredSearchConfig::professional_value_guard_sleeve_composition_default(),
        ),
        "professional_nearest_candidate_risk_model"
        | "nearest_candidate_risk_model"
        | "phase7_nearest_risk_model"
        | "phase7_at" => (
            "professional_nearest_candidate_risk_model".to_string(),
            LayeredSearchConfig::professional_nearest_candidate_risk_model_default(),
        ),
        "professional_event_regime_sleeve"
        | "event_regime_sleeve"
        | "phase7_event_regime_sleeve"
        | "phase7_av" => (
            "professional_event_regime_sleeve".to_string(),
            LayeredSearchConfig::professional_event_regime_sleeve_default(),
        ),
        "professional_event_window_sleeve_weight"
        | "event_window_sleeve_weight"
        | "phase7_event_window_sleeve_weight"
        | "phase7_aw" => (
            "professional_event_window_sleeve_weight".to_string(),
            LayeredSearchConfig::professional_event_window_sleeve_weight_default(),
        ),
        "professional_event_window_sleeve_upper_bound"
        | "event_window_sleeve_upper_bound"
        | "phase7_event_window_sleeve_upper_bound"
        | "phase7_ax" => (
            "professional_event_window_sleeve_upper_bound".to_string(),
            LayeredSearchConfig::professional_event_window_sleeve_upper_bound_default(),
        ),
        "professional_event_window_regime_placement"
        | "event_window_regime_placement"
        | "phase7_event_window_regime_placement"
        | "phase7_ay" => (
            "professional_event_window_regime_placement".to_string(),
            LayeredSearchConfig::professional_event_window_regime_placement_default(),
        ),
        "professional_event_window_decay"
        | "event_window_decay"
        | "phase7_event_window_decay"
        | "phase7_az" => (
            "professional_event_window_decay".to_string(),
            LayeredSearchConfig::professional_event_window_decay_default(),
        ),
        "professional_event_quality_segment"
        | "event_quality_segment"
        | "phase7_event_quality_segment"
        | "phase7_bb" => (
            "professional_event_quality_segment".to_string(),
            LayeredSearchConfig::professional_event_quality_segment_default(),
        ),
        "professional_event_surprise_nonlinear"
        | "event_surprise_nonlinear"
        | "phase7_event_surprise_nonlinear"
        | "phase7_ba" => (
            "professional_event_surprise_nonlinear".to_string(),
            LayeredSearchConfig::professional_event_surprise_nonlinear_default(),
        ),
        "professional_event_strength_segment"
        | "event_strength_segment"
        | "event_min_score_segment"
        | "phase7_event_strength_segment"
        | "phase7_bc" => (
            "professional_event_strength_segment".to_string(),
            LayeredSearchConfig::professional_event_strength_segment_default(),
        ),
        "professional_event_strength_boost"
        | "event_strength_boost"
        | "event_min_score_boost"
        | "phase7_event_strength_boost"
        | "phase7_bd" => (
            "professional_event_strength_boost".to_string(),
            LayeredSearchConfig::professional_event_strength_boost_default(),
        ),
        "professional_legacy_alpha_revalidation"
        | "legacy_alpha_revalidation"
        | "phase7_legacy_alpha_revalidation"
        | "phase7_be" => (
            "professional_legacy_alpha_revalidation".to_string(),
            LayeredSearchConfig::professional_legacy_alpha_revalidation_default(),
        ),
        "professional_current_anchor_risk_shape"
        | "current_anchor_risk_shape"
        | "event_window_anchor_risk_shape"
        | "phase7_current_anchor_risk_shape"
        | "phase7_bf" => (
            "professional_current_anchor_risk_shape".to_string(),
            LayeredSearchConfig::professional_current_anchor_risk_shape_default(),
        ),
        "professional_current_anchor_position_frontier"
        | "current_anchor_position_frontier"
        | "event_window_anchor_position_frontier"
        | "phase7_current_anchor_position_frontier"
        | "phase7_bg" => (
            "professional_current_anchor_position_frontier".to_string(),
            LayeredSearchConfig::professional_current_anchor_position_frontier_default(),
        ),
        "professional_current_anchor_weak_window_repair"
        | "current_anchor_weak_window_repair"
        | "event_window_anchor_weak_window_repair"
        | "phase7_current_anchor_weak_window_repair"
        | "phase7_bh" => (
            "professional_current_anchor_weak_window_repair".to_string(),
            LayeredSearchConfig::professional_current_anchor_weak_window_repair_default(),
        ),
        "professional_current_anchor_sharpe_return_bridge"
        | "current_anchor_sharpe_return_bridge"
        | "sharpe_return_bridge"
        | "phase7_current_anchor_sharpe_return_bridge"
        | "phase7_bi" => (
            "professional_current_anchor_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_current_anchor_sharpe_return_bridge_default(),
        ),
        "professional_high_sharpe_return_recovery"
        | "high_sharpe_return_recovery"
        | "sharpe_return_recovery"
        | "phase7_high_sharpe_return_recovery"
        | "phase7_bn" => (
            "professional_high_sharpe_return_recovery".to_string(),
            LayeredSearchConfig::professional_high_sharpe_return_recovery_default(),
        ),
        "professional_risk_memory_bridge"
        | "risk_memory_bridge"
        | "risk_budget_memory_bridge"
        | "phase7_risk_memory_bridge"
        | "phase7_bo" => (
            "professional_risk_memory_bridge".to_string(),
            LayeredSearchConfig::professional_risk_memory_bridge_default(),
        ),
        "professional_state_return_sharpe_router"
        | "state_return_sharpe_router"
        | "return_sharpe_router"
        | "phase7_state_return_sharpe_router"
        | "phase7_bp" => (
            "professional_state_return_sharpe_router".to_string(),
            LayeredSearchConfig::professional_state_return_sharpe_router_default(),
        ),
        "professional_state_return_sharpe_frontier"
        | "state_return_sharpe_frontier"
        | "return_sharpe_frontier"
        | "phase7_state_return_sharpe_frontier"
        | "phase7_bq" => (
            "professional_state_return_sharpe_frontier".to_string(),
            LayeredSearchConfig::professional_state_return_sharpe_frontier_default(),
        ),
        "professional_position_sharpe_return_bridge"
        | "position_sharpe_return_bridge"
        | "sharpe_return_position_bridge"
        | "phase7_position_sharpe_return_bridge"
        | "phase7_br" => (
            "professional_position_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_position_sharpe_return_bridge_default(),
        ),
        "professional_moderate_position_sharpe_return_bridge"
        | "moderate_position_sharpe_return_bridge"
        | "moderate_sharpe_return_position_bridge"
        | "phase7_moderate_position_sharpe_return_bridge"
        | "phase7_bs" => (
            "professional_moderate_position_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_moderate_position_sharpe_return_bridge_default(),
        ),
        "professional_correlation_frontier_sharpe_return"
        | "correlation_frontier_sharpe_return"
        | "corr_frontier_sharpe_return"
        | "phase7_correlation_frontier_sharpe_return"
        | "phase7_bt" => (
            "professional_correlation_frontier_sharpe_return".to_string(),
            LayeredSearchConfig::professional_correlation_frontier_sharpe_return_default(),
        ),
        "professional_correlation_threshold_sharpe_return"
        | "correlation_threshold_sharpe_return"
        | "corr_threshold_sharpe_return"
        | "phase7_correlation_threshold_sharpe_return"
        | "phase7_bu" => (
            "professional_correlation_threshold_sharpe_return".to_string(),
            LayeredSearchConfig::professional_correlation_threshold_sharpe_return_default(),
        ),
        "professional_soft_risk_frontier_sharpe_return"
        | "soft_risk_frontier_sharpe_return"
        | "phase7_soft_risk_frontier_sharpe_return"
        | "phase7_bv" => (
            "professional_soft_risk_frontier_sharpe_return".to_string(),
            LayeredSearchConfig::professional_soft_risk_frontier_sharpe_return_default(),
        ),
        "professional_regime_alpha_selector"
        | "regime_alpha_selector"
        | "phase7_regime_alpha_selector"
        | "phase7_bw" => (
            "professional_regime_alpha_selector".to_string(),
            LayeredSearchConfig::professional_regime_alpha_selector_default(),
        ),
        "professional_regime_alpha_overlay_frontier"
        | "regime_alpha_overlay_frontier"
        | "phase7_regime_alpha_overlay_frontier"
        | "phase7_bx" => (
            "professional_regime_alpha_overlay_frontier".to_string(),
            LayeredSearchConfig::professional_regime_alpha_overlay_frontier_default(),
        ),
        "professional_mixed_state_event_alpha"
        | "mixed_state_event_alpha"
        | "phase7_mixed_state_event_alpha"
        | "phase7_by" => (
            "professional_mixed_state_event_alpha".to_string(),
            LayeredSearchConfig::professional_mixed_state_event_alpha_default(),
        ),
        "professional_mixed_state_risk_memory"
        | "mixed_state_risk_memory"
        | "phase7_mixed_state_risk_memory"
        | "phase7_bz" => (
            "professional_mixed_state_risk_memory".to_string(),
            LayeredSearchConfig::professional_mixed_state_risk_memory_default(),
        ),
        "professional_mixed_state_risk_memory_frontier"
        | "mixed_state_risk_memory_frontier"
        | "phase7_mixed_state_risk_memory_frontier"
        | "phase7_ca" => (
            "professional_mixed_state_risk_memory_frontier".to_string(),
            LayeredSearchConfig::professional_mixed_state_risk_memory_frontier_default(),
        ),
        "professional_mixed_state_risk_memory_fine_frontier"
        | "mixed_state_risk_memory_fine_frontier"
        | "phase7_mixed_state_risk_memory_fine_frontier"
        | "phase7_cb" => (
            "professional_mixed_state_risk_memory_fine_frontier".to_string(),
            LayeredSearchConfig::professional_mixed_state_risk_memory_fine_frontier_default(),
        ),
        "professional_mixed_state_exposure_frontier"
        | "mixed_state_exposure_frontier"
        | "phase7_mixed_state_exposure_frontier"
        | "phase7_cc" => (
            "professional_mixed_state_exposure_frontier".to_string(),
            LayeredSearchConfig::professional_mixed_state_exposure_frontier_default(),
        ),
        "professional_mixed_state_orthogonal_alpha"
        | "mixed_state_orthogonal_alpha"
        | "phase7_mixed_state_orthogonal_alpha"
        | "phase7_cd" => (
            "professional_mixed_state_orthogonal_alpha".to_string(),
            LayeredSearchConfig::professional_mixed_state_orthogonal_alpha_default(),
        ),
        "professional_candidate_filter_alpha_bridge"
        | "candidate_filter_alpha_bridge"
        | "phase7_candidate_filter_alpha_bridge"
        | "phase7_ce" => (
            "professional_candidate_filter_alpha_bridge".to_string(),
            LayeredSearchConfig::professional_candidate_filter_alpha_bridge_default(),
        ),
        "professional_soft_candidate_filter_alpha_bridge"
        | "soft_candidate_filter_alpha_bridge"
        | "phase7_soft_candidate_filter_alpha_bridge"
        | "phase7_cf" => (
            "professional_soft_candidate_filter_alpha_bridge".to_string(),
            LayeredSearchConfig::professional_soft_candidate_filter_alpha_bridge_default(),
        ),
        "professional_sharpe_bridge_frontier"
        | "sharpe_bridge_frontier"
        | "phase7_sharpe_bridge_frontier"
        | "phase7_cg" => (
            "professional_sharpe_bridge_frontier".to_string(),
            LayeredSearchConfig::professional_sharpe_bridge_frontier_default(),
        ),
        "professional_annual_sharpe_floor_bridge"
        | "annual_sharpe_floor_bridge"
        | "phase7_annual_sharpe_floor_bridge"
        | "phase7_ch" => (
            "professional_annual_sharpe_floor_bridge".to_string(),
            LayeredSearchConfig::professional_annual_sharpe_floor_bridge_default(),
        ),
        "professional_risk_memory_relaxed_frontier"
        | "risk_memory_relaxed_frontier"
        | "phase7_risk_memory_relaxed_frontier"
        | "phase7_ci" => (
            "professional_risk_memory_relaxed_frontier".to_string(),
            LayeredSearchConfig::professional_risk_memory_relaxed_frontier_default(),
        ),
        "professional_sharpe_floor_auto_discovery"
        | "sharpe_floor_auto_discovery"
        | "phase7_sharpe_floor_auto_discovery"
        | "phase7_ck" => (
            "professional_sharpe_floor_auto_discovery".to_string(),
            LayeredSearchConfig::professional_sharpe_floor_auto_discovery_default(),
        ),
        "professional_nonlinear_alpha_auto_discovery"
        | "nonlinear_alpha_auto_discovery"
        | "phase7_nonlinear_alpha_auto_discovery"
        | "phase7_cl" => (
            "professional_nonlinear_alpha_auto_discovery".to_string(),
            LayeredSearchConfig::professional_nonlinear_alpha_auto_discovery_default(),
        ),
        "professional_nonlinear_sharpe_return_bridge"
        | "nonlinear_sharpe_return_bridge"
        | "phase7_nonlinear_sharpe_return_bridge"
        | "phase7_cm" => (
            "professional_nonlinear_sharpe_return_bridge".to_string(),
            LayeredSearchConfig::professional_nonlinear_sharpe_return_bridge_default(),
        ),
        "professional_prediction_confirmed_sharpe_bridge"
        | "prediction_confirmed_sharpe_bridge"
        | "phase7_prediction_confirmed_sharpe_bridge"
        | "phase7_cn" => (
            "professional_prediction_confirmed_sharpe_bridge".to_string(),
            LayeredSearchConfig::professional_prediction_confirmed_sharpe_bridge_default(),
        ),
        "professional_high_sharpe_boundary_return_bridge"
        | "high_sharpe_boundary_return_bridge"
        | "phase7_high_sharpe_boundary_return_bridge"
        | "phase7_co" => (
            "professional_high_sharpe_boundary_return_bridge".to_string(),
            LayeredSearchConfig::professional_high_sharpe_boundary_return_bridge_default(),
        ),
        "professional_high_sharpe_boundary_event_lift"
        | "high_sharpe_boundary_event_lift"
        | "phase7_high_sharpe_boundary_event_lift"
        | "phase7_cq" => (
            "professional_high_sharpe_boundary_event_lift".to_string(),
            LayeredSearchConfig::professional_high_sharpe_boundary_event_lift_default(),
        ),
        "professional_high_sharpe_micro_frontier"
        | "high_sharpe_micro_frontier"
        | "phase7_high_sharpe_micro_frontier"
        | "phase7_cr" => (
            "professional_high_sharpe_micro_frontier".to_string(),
            LayeredSearchConfig::professional_high_sharpe_micro_frontier_default(),
        ),
        "professional_v14_sharpe_return_lift"
        | "v14_sharpe_return_lift"
        | "phase7_v14_sharpe_return_lift"
        | "phase7_cs" => (
            "professional_v14_sharpe_return_lift".to_string(),
            LayeredSearchConfig::professional_v14_sharpe_return_lift_default(),
        ),
        "professional_v14_shape_lift"
        | "v14_shape_lift"
        | "phase7_v14_shape_lift"
        | "phase7_ct" => (
            "professional_v14_shape_lift".to_string(),
            LayeredSearchConfig::professional_v14_shape_lift_default(),
        ),
        "professional_v14_ultra_micro_lift"
        | "v14_ultra_micro_lift"
        | "phase7_v14_ultra_micro_lift"
        | "phase7_cu" => (
            "professional_v14_ultra_micro_lift".to_string(),
            LayeredSearchConfig::professional_v14_ultra_micro_lift_default(),
        ),
        "professional_v14_annual_floor_micro_lift"
        | "v14_annual_floor_micro_lift"
        | "phase7_v14_annual_floor_micro_lift"
        | "phase7_cz" => (
            "professional_v14_annual_floor_micro_lift".to_string(),
            LayeredSearchConfig::professional_v14_annual_floor_micro_lift_default(),
        ),
        "professional_v14_near_miss_annual_bridge"
        | "v14_near_miss_annual_bridge"
        | "phase7_v14_near_miss_annual_bridge"
        | "phase7_da" => (
            "professional_v14_near_miss_annual_bridge".to_string(),
            LayeredSearchConfig::professional_v14_near_miss_annual_bridge_default(),
        ),
        "professional_v14_corr70_annual_edge"
        | "v14_corr70_annual_edge"
        | "phase7_v14_corr70_annual_edge"
        | "phase7_db" => (
            "professional_v14_corr70_annual_edge".to_string(),
            LayeredSearchConfig::professional_v14_corr70_annual_edge_default(),
        ),
        "professional_execution_robust_candidate"
        | "execution_robust_candidate"
        | "phase7_execution_robust_candidate"
        | "phase7_dj" => (
            "professional_execution_robust_candidate".to_string(),
            LayeredSearchConfig::professional_execution_robust_candidate_default(),
        ),
        "professional_execution_low_turnover_alpha"
        | "execution_low_turnover_alpha"
        | "phase7_execution_low_turnover_alpha"
        | "phase7_dn" => (
            "professional_execution_low_turnover_alpha".to_string(),
            LayeredSearchConfig::professional_execution_low_turnover_alpha_default(),
        ),
        "professional_execution_capacity_budget"
        | "execution_capacity_budget"
        | "phase7_execution_capacity_budget"
        | "phase7_dq" => (
            "professional_execution_capacity_budget".to_string(),
            LayeredSearchConfig::professional_execution_capacity_budget_default(),
        ),
        "professional_execution_impact_budget"
        | "execution_impact_budget"
        | "phase7_execution_impact_budget"
        | "phase7_dr" => (
            "professional_execution_impact_budget".to_string(),
            LayeredSearchConfig::professional_execution_impact_budget_default(),
        ),
        "professional_execution_schedule_budget"
        | "execution_schedule_budget"
        | "phase7_execution_schedule_budget"
        | "phase7_ds" => (
            "professional_execution_schedule_budget".to_string(),
            LayeredSearchConfig::professional_execution_schedule_budget_default(),
        ),
        "professional_execution_patient_schedule_budget"
        | "execution_patient_schedule_budget"
        | "patient_execution_schedule_budget"
        | "phase7_execution_patient_schedule_budget"
        | "phase7_dt" => (
            "professional_execution_patient_schedule_budget".to_string(),
            LayeredSearchConfig::professional_execution_patient_schedule_budget_default(),
        ),
        "professional_execution_daily_cap_budget"
        | "execution_daily_cap_budget"
        | "execution_schedule_daily_cap_budget"
        | "phase7_execution_daily_cap_budget"
        | "phase7_du" => (
            "professional_execution_daily_cap_budget".to_string(),
            LayeredSearchConfig::professional_execution_daily_cap_budget_default(),
        ),
        "professional_execution_cash_drag_aware_budget"
        | "execution_cash_drag_aware_budget"
        | "cash_drag_aware_execution_budget"
        | "phase7_execution_cash_drag_aware_budget"
        | "phase7_dv" => (
            "professional_execution_cash_drag_aware_budget".to_string(),
            LayeredSearchConfig::professional_execution_cash_drag_aware_budget_default(),
        ),
        "professional_execution_feasible_fill_budget"
        | "execution_feasible_fill_budget"
        | "cash_utilization_execution_budget"
        | "phase7_execution_feasible_fill_budget"
        | "phase7_dx" => (
            "professional_execution_feasible_fill_budget".to_string(),
            LayeredSearchConfig::professional_execution_feasible_fill_budget_default(),
        ),
        "professional_execution_fill_ratio_budget"
        | "execution_fill_ratio_budget"
        | "execution_unfilled_gap_budget"
        | "phase7_execution_fill_ratio_budget"
        | "phase7_dy" => (
            "professional_execution_fill_ratio_budget".to_string(),
            LayeredSearchConfig::professional_execution_fill_ratio_budget_default(),
        ),
        "professional_execution_rolling_carry_budget"
        | "execution_rolling_carry_budget"
        | "execution_roll_forward_budget"
        | "phase7_execution_rolling_carry_budget"
        | "phase7_dz" => (
            "professional_execution_rolling_carry_budget".to_string(),
            LayeredSearchConfig::professional_execution_rolling_carry_budget_default(),
        ),
        "professional_execution_capacity_fill_frontier"
        | "execution_capacity_fill_frontier"
        | "capacity_fill_frontier"
        | "phase7_execution_capacity_fill_frontier"
        | "phase7_ea" => (
            "professional_execution_capacity_fill_frontier".to_string(),
            LayeredSearchConfig::professional_execution_capacity_fill_frontier_default(),
        ),
        "professional_execution_alpha_capacity_bridge"
        | "execution_alpha_capacity_bridge"
        | "alpha_capacity_bridge"
        | "phase7_execution_alpha_capacity_bridge"
        | "phase7_eb" => (
            "professional_execution_alpha_capacity_bridge".to_string(),
            LayeredSearchConfig::professional_execution_alpha_capacity_bridge_default(),
        ),
        "professional_execution_alpha_capacity_return_frontier"
        | "execution_alpha_capacity_return_frontier"
        | "alpha_capacity_return_frontier"
        | "phase7_execution_alpha_capacity_return_frontier"
        | "phase7_ec" => (
            "professional_execution_alpha_capacity_return_frontier".to_string(),
            LayeredSearchConfig::professional_execution_alpha_capacity_return_frontier_default(),
        ),
        "professional_execution_stress_fill_return_frontier"
        | "execution_stress_fill_return_frontier"
        | "stress_fill_return_frontier"
        | "phase7_execution_stress_fill_return_frontier"
        | "phase7_ed" => (
            "professional_execution_stress_fill_return_frontier".to_string(),
            LayeredSearchConfig::professional_execution_stress_fill_return_frontier_default(),
        ),
        "professional_execution_stress_risk_budget"
        | "execution_stress_risk_budget"
        | "stress_risk_budget"
        | "phase7_execution_stress_risk_budget"
        | "phase7_ee" => (
            "professional_execution_stress_risk_budget".to_string(),
            LayeredSearchConfig::professional_execution_stress_risk_budget_default(),
        ),
        "professional_execution_capacity_stress_return_gate"
        | "execution_capacity_stress_return_gate"
        | "capacity_stress_return_gate"
        | "phase7_execution_capacity_stress_return_gate"
        | "phase7_ef" => (
            "professional_execution_capacity_stress_return_gate".to_string(),
            LayeredSearchConfig::professional_execution_capacity_stress_return_gate_default(),
        ),
        "professional_execution_low_impact_alpha_stress_return"
        | "execution_low_impact_alpha_stress_return"
        | "low_impact_alpha_stress_return"
        | "phase7_execution_low_impact_alpha_stress_return"
        | "phase7_eg" => (
            "professional_execution_low_impact_alpha_stress_return".to_string(),
            LayeredSearchConfig::professional_execution_low_impact_alpha_stress_return_default(),
        ),
        "professional_execution_stress_target_scaling_return"
        | "execution_stress_target_scaling_return"
        | "stress_target_scaling_return"
        | "phase7_execution_stress_target_scaling_return"
        | "phase7_eh" => (
            "professional_execution_stress_target_scaling_return".to_string(),
            LayeredSearchConfig::professional_execution_stress_target_scaling_return_default(),
        ),
        "professional_execution_stress_floor_scaling_return"
        | "execution_stress_floor_scaling_return"
        | "stress_floor_scaling_return"
        | "phase7_execution_stress_floor_scaling_return"
        | "phase7_ei" => (
            "professional_execution_stress_floor_scaling_return".to_string(),
            LayeredSearchConfig::professional_execution_stress_floor_scaling_return_default(),
        ),
        "professional_execution_stress_floor_return_recovery"
        | "execution_stress_floor_return_recovery"
        | "stress_floor_return_recovery"
        | "phase7_execution_stress_floor_return_recovery"
        | "phase7_ej" => (
            "professional_execution_stress_floor_return_recovery".to_string(),
            LayeredSearchConfig::professional_execution_stress_floor_return_recovery_default(),
        ),
        "professional_execution_pressure_headroom_floor"
        | "execution_pressure_headroom_floor"
        | "pressure_headroom_floor"
        | "phase7_execution_pressure_headroom_floor"
        | "phase7_ek" => (
            "professional_execution_pressure_headroom_floor".to_string(),
            LayeredSearchConfig::professional_execution_pressure_headroom_floor_default(),
        ),
        "professional_execution_alpha_headroom_floor"
        | "execution_alpha_headroom_floor"
        | "alpha_headroom_floor"
        | "phase7_execution_alpha_headroom_floor"
        | "phase7_el" => (
            "professional_execution_alpha_headroom_floor".to_string(),
            LayeredSearchConfig::professional_execution_alpha_headroom_floor_default(),
        ),
        "professional_execution_blended_alpha_headroom_floor"
        | "execution_blended_alpha_headroom_floor"
        | "blended_alpha_headroom_floor"
        | "phase7_execution_blended_alpha_headroom_floor"
        | "phase7_em" => (
            "professional_execution_blended_alpha_headroom_floor".to_string(),
            LayeredSearchConfig::professional_execution_blended_alpha_headroom_floor_default(),
        ),
        "professional_execution_event_anchor_stress_bridge"
        | "execution_event_anchor_stress_bridge"
        | "event_anchor_stress_bridge"
        | "phase7_execution_event_anchor_stress_bridge"
        | "phase7_en" => (
            "professional_execution_event_anchor_stress_bridge".to_string(),
            LayeredSearchConfig::professional_execution_event_anchor_stress_bridge_default(),
        ),
        "professional_execution_participation_aware_event_anchor"
        | "execution_participation_aware_event_anchor"
        | "participation_aware_event_anchor"
        | "phase7_execution_participation_aware_event_anchor"
        | "phase7_eo" => (
            "professional_execution_participation_aware_event_anchor".to_string(),
            LayeredSearchConfig::professional_execution_participation_aware_event_anchor_default(),
        ),
        "professional_execution_cl_anchor_fill_recovery"
        | "execution_cl_anchor_fill_recovery"
        | "cl_anchor_fill_recovery"
        | "phase7_execution_cl_anchor_fill_recovery"
        | "phase7_ep" => (
            "professional_execution_cl_anchor_fill_recovery".to_string(),
            LayeredSearchConfig::professional_execution_cl_anchor_fill_recovery_default(),
        ),
        "professional_execution_capacity_aware_candidate_ranking"
        | "execution_capacity_aware_candidate_ranking"
        | "capacity_aware_candidate_ranking"
        | "phase7_execution_capacity_aware_candidate_ranking"
        | "phase7_eq" => (
            "professional_execution_capacity_aware_candidate_ranking".to_string(),
            LayeredSearchConfig::professional_execution_capacity_aware_candidate_ranking_default(),
        ),
        "professional_execution_pit_capacity_ranking"
        | "execution_pit_capacity_ranking"
        | "pit_capacity_ranking"
        | "phase7_execution_pit_capacity_ranking"
        | "phase7_er" => (
            "professional_execution_pit_capacity_ranking".to_string(),
            LayeredSearchConfig::professional_execution_pit_capacity_ranking_default(),
        ),
        "professional_execution_pit_alpha_first_low_impact"
        | "execution_pit_alpha_first_low_impact"
        | "pit_alpha_first_low_impact"
        | "phase7_execution_pit_alpha_first_low_impact"
        | "phase7_es" => (
            "professional_execution_pit_alpha_first_low_impact".to_string(),
            LayeredSearchConfig::professional_execution_pit_alpha_first_low_impact_default(),
        ),
        "professional_execution_pit_excess_return_recovery"
        | "execution_pit_excess_return_recovery"
        | "pit_excess_return_recovery"
        | "phase7_execution_pit_excess_return_recovery"
        | "phase7_et" => (
            "professional_execution_pit_excess_return_recovery".to_string(),
            LayeredSearchConfig::professional_execution_pit_excess_return_recovery_default(),
        ),
        "professional_execution_bull_sleeve_cash_recovery"
        | "execution_bull_sleeve_cash_recovery"
        | "bull_sleeve_cash_recovery"
        | "phase7_execution_bull_sleeve_cash_recovery"
        | "phase7_eu" => (
            "professional_execution_bull_sleeve_cash_recovery".to_string(),
            LayeredSearchConfig::professional_execution_bull_sleeve_cash_recovery_default(),
        ),
        "professional_execution_return_first_fill_repair"
        | "execution_return_first_fill_repair"
        | "return_first_fill_repair"
        | "phase7_execution_return_first_fill_repair"
        | "phase7_ey" => (
            "professional_execution_return_first_fill_repair".to_string(),
            LayeredSearchConfig::professional_execution_return_first_fill_repair_default(),
        ),
        "professional_execution_pit_nonlinear_alpha_regime_rebuild"
        | "execution_pit_nonlinear_alpha_regime_rebuild"
        | "pit_nonlinear_alpha_regime_rebuild"
        | "phase7_execution_pit_nonlinear_alpha_regime_rebuild"
        | "phase7_ez" => (
            "professional_execution_pit_nonlinear_alpha_regime_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_pit_nonlinear_alpha_regime_rebuild_default(
            ),
        ),
        "professional_execution_pit_quality_recovery_alpha"
        | "execution_pit_quality_recovery_alpha"
        | "pit_quality_recovery_alpha"
        | "phase7_execution_pit_quality_recovery_alpha"
        | "phase7_fa" => (
            "professional_execution_pit_quality_recovery_alpha".to_string(),
            LayeredSearchConfig::professional_execution_pit_quality_recovery_alpha_default(),
        ),
        "professional_execution_event_post_return_curve_alpha"
        | "execution_event_post_return_curve_alpha"
        | "event_post_return_curve_alpha"
        | "phase7_execution_event_post_return_curve_alpha"
        | "phase7_fb" => (
            "professional_execution_event_post_return_curve_alpha".to_string(),
            LayeredSearchConfig::professional_execution_event_post_return_curve_alpha_default(),
        ),
        "professional_execution_event_reaction_alpha"
        | "execution_event_reaction_alpha"
        | "event_reaction_alpha"
        | "phase7_execution_event_reaction_alpha"
        | "phase7_fc" => (
            "professional_execution_event_reaction_alpha".to_string(),
            LayeredSearchConfig::professional_execution_event_reaction_alpha_default(),
        ),
        "professional_execution_broad_financial_feature_discovery"
        | "execution_broad_financial_feature_discovery"
        | "broad_financial_feature_discovery"
        | "phase7_execution_broad_financial_feature_discovery"
        | "phase7_fg" => (
            "professional_execution_broad_financial_feature_discovery".to_string(),
            LayeredSearchConfig::professional_execution_broad_financial_feature_discovery_default(),
        ),
        "professional_execution_broad_financial_feature_stratified_discovery"
        | "execution_broad_financial_feature_stratified_discovery"
        | "broad_financial_feature_stratified_discovery"
        | "phase7_execution_broad_financial_feature_stratified_discovery"
        | "phase7_fh" => (
            "professional_execution_broad_financial_feature_stratified_discovery".to_string(),
            LayeredSearchConfig::professional_execution_broad_financial_feature_stratified_discovery_default(),
        ),
        "professional_execution_native_alpha_fusion_discovery"
        | "execution_native_alpha_fusion_discovery"
        | "native_alpha_fusion_discovery"
        | "phase7_execution_native_alpha_fusion_discovery"
        | "phase7_fj" => (
            "professional_execution_native_alpha_fusion_discovery".to_string(),
            LayeredSearchConfig::professional_execution_native_alpha_fusion_discovery_default(),
        ),
        "professional_trainable_alpha_admission_discovery"
        | "trainable_alpha_admission_discovery"
        | "phase7_trainable_alpha_admission"
        | "phase7_ft" => (
            "professional_trainable_alpha_admission_discovery".to_string(),
            LayeredSearchConfig::professional_trainable_alpha_admission_discovery_default(),
        ),
        "professional_prediction_capacity_dual_objective"
        | "prediction_capacity_dual_objective"
        | "phase7_prediction_capacity_dual_objective"
        | "phase7_fl" => (
            "professional_prediction_capacity_dual_objective".to_string(),
            LayeredSearchConfig::professional_prediction_capacity_dual_objective_default(),
        ),
        "professional_prediction_target_gross_signal_fidelity"
        | "prediction_target_gross_signal_fidelity"
        | "phase7_prediction_target_gross_signal_fidelity"
        | "phase7_fm" => (
            "professional_prediction_target_gross_signal_fidelity".to_string(),
            LayeredSearchConfig::professional_prediction_target_gross_signal_fidelity_default(),
        ),
        "professional_prediction_confidence_turnover_discovery"
        | "prediction_confidence_turnover_discovery"
        | "phase7_prediction_confidence_turnover_discovery"
        | "phase7_fn" => (
            "professional_prediction_confidence_turnover_discovery".to_string(),
            LayeredSearchConfig::professional_prediction_confidence_turnover_discovery_default(),
        ),
        "professional_prediction_confidence_alpha_lift"
        | "prediction_confidence_alpha_lift"
        | "phase7_prediction_confidence_alpha_lift"
        | "phase7_fo" => (
            "professional_prediction_confidence_alpha_lift".to_string(),
            LayeredSearchConfig::professional_prediction_confidence_alpha_lift_default(),
        ),
        "professional_prediction_long_horizon_low_turnover"
        | "prediction_long_horizon_low_turnover"
        | "phase7_prediction_long_horizon_low_turnover"
        | "phase7_fp" => (
            "professional_prediction_long_horizon_low_turnover".to_string(),
            LayeredSearchConfig::professional_prediction_long_horizon_low_turnover_default(),
        ),
        "professional_prediction_long_horizon_regime_alpha"
        | "prediction_long_horizon_regime_alpha"
        | "phase7_prediction_long_horizon_regime_alpha"
        | "phase7_fr" => (
            "professional_prediction_long_horizon_regime_alpha".to_string(),
            LayeredSearchConfig::professional_prediction_long_horizon_regime_alpha_default(),
        ),
        "professional_prediction_h60_nonlinear_stress_discovery"
        | "prediction_h60_nonlinear_stress_discovery"
        | "h60_nonlinear_stress_discovery"
        | "phase7_prediction_h60_nonlinear_stress_discovery"
        | "phase7_fw" => (
            "professional_prediction_h60_nonlinear_stress_discovery".to_string(),
            LayeredSearchConfig::professional_prediction_h60_nonlinear_stress_discovery_default(),
        ),
        "professional_prediction_h120_low_impact_stress_discovery"
        | "prediction_h120_low_impact_stress_discovery"
        | "h120_low_impact_stress_discovery"
        | "phase7_prediction_h120_low_impact_stress_discovery"
        | "phase7_fz" => (
            "professional_prediction_h120_low_impact_stress_discovery".to_string(),
            LayeredSearchConfig::professional_prediction_h120_low_impact_stress_discovery_default(),
        ),
        "professional_train_window_nonlinear_ranking_discovery"
        | "train_window_nonlinear_ranking_discovery"
        | "phase7_train_window_nonlinear_ranking_discovery"
        | "phase7_fx" => (
            "professional_train_window_nonlinear_ranking_discovery".to_string(),
            LayeredSearchConfig::professional_train_window_nonlinear_ranking_discovery_default(),
        ),
        "professional_train_window_stress_fill_target_exposure"
        | "train_window_stress_fill_target_exposure"
        | "stress_fill_target_exposure"
        | "phase7_train_window_stress_fill_target_exposure"
        | "phase7_ga" => (
            "professional_train_window_stress_fill_target_exposure".to_string(),
            LayeredSearchConfig::professional_train_window_stress_fill_target_exposure_default(),
        ),
        "professional_train_window_ml_stress_fill_discovery"
        | "train_window_ml_stress_fill_discovery"
        | "ml_stress_fill_discovery"
        | "phase7_train_window_ml_stress_fill_discovery"
        | "phase7_gb" => (
            "professional_train_window_ml_stress_fill_discovery".to_string(),
            LayeredSearchConfig::professional_train_window_ml_stress_fill_discovery_default(),
        ),
        "professional_current_event_nonlinear_alpha_discovery"
        | "current_event_nonlinear_alpha_discovery"
        | "phase7_current_event_nonlinear_alpha_discovery"
        | "phase7_fs" => (
            "professional_current_event_nonlinear_alpha_discovery".to_string(),
            LayeredSearchConfig::professional_current_event_nonlinear_alpha_discovery_default(),
        ),
        "professional_execution_oos_regime_alpha_rebuild"
        | "execution_oos_regime_alpha_rebuild"
        | "oos_regime_alpha_rebuild"
        | "phase7_execution_oos_regime_alpha_rebuild"
        | "phase7_ev" => (
            "professional_execution_oos_regime_alpha_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_oos_regime_alpha_rebuild_default(),
        ),
        "professional_execution_oos_benchmark_excess_rebuild"
        | "execution_oos_benchmark_excess_rebuild"
        | "oos_benchmark_excess_rebuild"
        | "phase7_execution_oos_benchmark_excess_rebuild"
        | "phase7_ew" => (
            "professional_execution_oos_benchmark_excess_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_oos_benchmark_excess_rebuild_default(),
        ),
        "professional_execution_oos_execution_adaptive_rebuild"
        | "execution_oos_execution_adaptive_rebuild"
        | "oos_execution_adaptive_rebuild"
        | "phase7_execution_oos_execution_adaptive_rebuild"
        | "phase7_ex" => (
            "professional_execution_oos_execution_adaptive_rebuild".to_string(),
            LayeredSearchConfig::professional_execution_oos_execution_adaptive_rebuild_default(),
        ),
        "professional_return_alpha_sharpe_bridge"
        | "return_alpha_sharpe_bridge"
        | "phase7_return_alpha_sharpe_bridge"
        | "phase7_cv" => (
            "professional_return_alpha_sharpe_bridge".to_string(),
            LayeredSearchConfig::professional_return_alpha_sharpe_bridge_default(),
        ),
        "professional_regime_frontier_bridge"
        | "regime_frontier_bridge"
        | "phase7_regime_frontier_bridge"
        | "phase7_cw" => (
            "professional_regime_frontier_bridge".to_string(),
            LayeredSearchConfig::professional_regime_frontier_bridge_default(),
        ),
        "professional_regime_frontier_decomposition"
        | "regime_frontier_decomposition"
        | "phase7_regime_frontier_decomposition"
        | "phase7_cx" => (
            "professional_regime_frontier_decomposition".to_string(),
            LayeredSearchConfig::professional_regime_frontier_decomposition_default(),
        ),
        "professional_high_sharpe_return_micro_bridge"
        | "high_sharpe_return_micro_bridge"
        | "phase7_high_sharpe_return_micro_bridge"
        | "phase7_cy" => (
            "professional_high_sharpe_return_micro_bridge".to_string(),
            LayeredSearchConfig::professional_high_sharpe_return_micro_bridge_default(),
        ),
        "professional_return_distribution_repair"
        | "return_distribution_repair"
        | "phase7_return_distribution_repair"
        | "phase7_cp" => (
            "professional_return_distribution_repair".to_string(),
            LayeredSearchConfig::professional_return_distribution_repair_default(),
        ),
        "professional_state_alpha_router"
        | "state_alpha_router"
        | "phase7_state_alpha_router"
        | "phase7_bj" => (
            "professional_state_alpha_router".to_string(),
            LayeredSearchConfig::professional_state_alpha_router_default(),
        ),
        "professional_state_position_risk_router"
        | "state_position_risk_router"
        | "phase7_state_position_risk_router"
        | "phase7_bk" => (
            "professional_state_position_risk_router".to_string(),
            LayeredSearchConfig::professional_state_position_risk_router_default(),
        ),
        "professional_event_position_risk_router"
        | "event_position_risk_router"
        | "phase7_event_position_risk_router"
        | "phase7_bl" => (
            "professional_event_position_risk_router".to_string(),
            LayeredSearchConfig::professional_event_position_risk_router_default(),
        ),
        "professional_all_regime_event_sleeve"
        | "all_regime_event_sleeve"
        | "phase7_all_regime_event_sleeve"
        | "phase7_bm" => (
            "professional_all_regime_event_sleeve".to_string(),
            LayeredSearchConfig::professional_all_regime_event_sleeve_default(),
        ),
        "professional_portfolio_sharpe_control"
        | "portfolio_sharpe_control"
        | "phase7_portfolio_sharpe_control"
        | "phase7_cj" => (
            "professional_portfolio_sharpe_control".to_string(),
            LayeredSearchConfig::professional_portfolio_sharpe_control_default(),
        ),
        "professional_volatility_sharpe" | "volatility_sharpe" | "phase7_ai" => (
            "professional_volatility_sharpe".to_string(),
            LayeredSearchConfig::professional_volatility_sharpe_default(),
        ),
        "professional_regime_position_sharpe"
        | "regime_position_sharpe"
        | "phase7_regime_position_sharpe"
        | "phase7_aj" => (
            "professional_regime_position_sharpe".to_string(),
            LayeredSearchConfig::professional_regime_position_sharpe_default(),
        ),
        "professional_anti_overfit_sharpe"
        | "anti_overfit_sharpe"
        | "phase7_anti_overfit_sharpe"
        | "phase7_ab" => (
            "professional_anti_overfit_sharpe".to_string(),
            LayeredSearchConfig::professional_anti_overfit_sharpe_default(),
        ),
        "professional_candidate_risk_filter"
        | "candidate_risk_filter"
        | "phase7_candidate_risk_filter"
        | "phase7_ac" => (
            "professional_candidate_risk_filter".to_string(),
            LayeredSearchConfig::professional_candidate_risk_filter_default(),
        ),
        "professional_risk_contribution"
        | "risk_contribution"
        | "phase7_risk_contribution"
        | "phase7_ad" => (
            "professional_risk_contribution".to_string(),
            LayeredSearchConfig::professional_risk_contribution_default(),
        ),
        "professional_event_conditioned_sharpe"
        | "event_conditioned_sharpe"
        | "phase7_event_conditioned"
        | "phase7_ae" => (
            "professional_event_conditioned_sharpe".to_string(),
            LayeredSearchConfig::professional_event_conditioned_sharpe_default(),
        ),
        "professional_breakthrough" | "breakthrough" | "phase7_breakthrough" => (
            "professional_breakthrough".to_string(),
            LayeredSearchConfig::professional_breakthrough_default(),
        ),
        _ => (
            "local_professional".to_string(),
            LayeredSearchConfig::local_professional_default(),
        ),
    }
}

#[cfg(test)]
fn build_phase7_layered_plan_bundle(
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
) -> Phase7LayeredPlanBundle {
    build_phase7_layered_plan_bundle_with_trial_cap(req, resource_plan, 500)
}

#[cfg(test)]
fn build_phase7_layered_plan_bundle_with_trial_cap(
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
) -> Phase7LayeredPlanBundle {
    build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
        req,
        resource_plan,
        max_trials_cap,
        None,
    )
}

fn build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
    req: &Phase7LayeredOptimizationRequest,
    mut resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
    internal_train_window_ml_prediction_set_id: Option<&str>,
) -> Phase7LayeredPlanBundle {
    if let Some(max_trials) = req.max_trials {
        resource_plan.max_trials = max_trials.clamp(1, max_trials_cap.max(1));
    }

    let (search_profile, mut config) = phase7_search_config(req.search_profile.as_deref());
    if profile_accepts_prediction_set_override(&search_profile) {
        if let Some(prediction_set_ids) = req.prediction_set_ids.as_ref() {
            config.prediction_set_ids = normalize_prediction_set_ids(prediction_set_ids);
            apply_prediction_set_override_to_seed_trials(
                &mut config.seed_trials,
                &config.prediction_set_ids,
            );
        }
    }
    if let Some(prediction_set_id) = internal_train_window_ml_prediction_set_id {
        if search_profile == "professional_train_window_ml_stress_fill_discovery" {
            apply_internal_train_window_ml_prediction_set_to_seed_trials(
                &mut config.seed_trials,
                prediction_set_id,
            );
        }
    }
    let plan = build_layered_search_plan(&config, &resource_plan);
    let search_space = json!({
        "phase": "7-D",
        "search_method": "phase7_layered_grid",
        "search_profile": search_profile,
        "config": config,
        "resource_plan": resource_plan.clone(),
        "requested_trials": plan.requested_trials,
        "planned_trials": plan.planned_trials,
        "truncated": plan.truncated,
    });

    Phase7LayeredPlanBundle {
        resource_plan,
        plan,
        search_space,
    }
}

fn profile_accepts_prediction_set_override(search_profile: &str) -> bool {
    !matches!(
        search_profile,
        "professional_train_window_nonlinear_ranking_discovery"
            | "professional_train_window_stress_fill_target_exposure"
            | "professional_train_window_ml_stress_fill_discovery"
    )
}

fn is_train_window_ml_stress_fill_profile(search_profile: Option<&str>) -> bool {
    matches!(
        search_profile.map(str::trim).unwrap_or_default(),
        "professional_train_window_ml_stress_fill_discovery"
            | "train_window_ml_stress_fill_discovery"
            | "ml_stress_fill_discovery"
            | "phase7_train_window_ml_stress_fill_discovery"
            | "phase7_gb"
    )
}

fn apply_internal_train_window_ml_prediction_set_to_seed_trials(
    seed_trials: &mut [Value],
    prediction_set_id: &str,
) {
    for seed in seed_trials {
        if seed.get("train_window_ml_ranking_profile").is_none() {
            continue;
        }
        let prediction_min_score = seed
            .get("train_window_ml_prediction_min_score")
            .and_then(Value::as_str)
            .unwrap_or("0.00")
            .to_string();
        seed["prediction_set_id"] = json!(prediction_set_id);
        seed["prediction_blend_weight"] = json!("0.20");
        seed["prediction_min_percentile"] = json!("0.30");
        seed["prediction_min_score"] = json!(prediction_min_score);
        seed["prediction_set_override"] = json!(true);
        seed["prediction_set_override_source"] = json!("train_window_ml_internal");
        seed["train_window_ml_prediction_set_scope"] = json!("generated_per_wfa_window_train_only");
    }
}

fn train_window_ml_oos_parameters(
    train_parameters: &Value,
    prediction_sets: Option<&TrainWindowMlPredictionSets>,
) -> Result<Value, String> {
    let Some(prediction_sets) = prediction_sets else {
        return Ok(train_parameters.clone());
    };
    let mut parameters = train_parameters.clone();
    let Some(object) = parameters.as_object_mut() else {
        return Err("train-window ML candidate parameters must be a JSON object".into());
    };
    object.insert(
        "prediction_set_id".to_string(),
        json!(prediction_sets.test_prediction_set_id),
    );
    object.insert("prediction_set_override".to_string(), json!(true));
    object.insert(
        "prediction_set_override_source".to_string(),
        json!("train_window_ml_internal_oos"),
    );
    object.insert(
        "train_window_ml_train_prediction_set_id".to_string(),
        json!(prediction_sets.train_prediction_set_id),
    );
    object.insert(
        "train_window_ml_training_task_id".to_string(),
        json!(prediction_sets.training_task_id),
    );
    Ok(parameters)
}

fn phase7_discovery_layered_request(
    req: &Phase7ProfessionalDiscoveryRequest,
) -> Phase7LayeredOptimizationRequest {
    Phase7LayeredOptimizationRequest {
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        objective: req.objective.clone().unwrap_or_else(|| {
            json!({
                "type": "professional_candidate",
                "benchmark": "000300.SH",
                "maximize": true
            })
        }),
        constraints: req.constraints.clone().or_else(|| {
            Some(json!({
                "min_annual_return": 0.15,
                "min_excess_return": 0.0,
                "min_sharpe": 1.0,
                "min_sortino": 1.5,
                "max_drawdown": 0.35,
                "min_trade_count": 1
            }))
        }),
        walk_forward: req.walk_forward.clone(),
        backtest_template: Some(
            req.backtest_template
                .clone()
                .unwrap_or_else(default_phase7_backtest_template),
        ),
        prediction_set_ids: req.prediction_set_ids.clone(),
        max_trials: req.max_trials,
        search_profile: Some(
            req.search_profile
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("professional_sharpe_stabilization")
                .to_string(),
        ),
    }
}

fn default_phase7_backtest_template() -> Value {
    json!({
        "mode": "standard",
        "persistence_mode": "summary_only",
        "benchmark": "000300.SH",
        "start_date": "20160201",
        "end_date": "20260515",
        "initial_capital": 1000000.0,
        "signal_timing": "close",
        "execution_timing": "next_open",
        "execution_price": "next_open",
        "effective_coverage": {
            "enabled": true,
            "mode": "adjust_start"
        }
    })
}

fn normalize_prediction_set_ids(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn apply_prediction_set_override_to_seed_trials(
    seed_trials: &mut [Value],
    prediction_set_ids: &[String],
) {
    let Some(prediction_set_id) = prediction_set_ids.first() else {
        return;
    };
    for seed in seed_trials {
        if seed.get("prediction_set_id").is_some() {
            seed["prediction_set_id"] = json!(prediction_set_id);
            seed["prediction_set_override"] = json!(true);
            seed["prediction_set_override_source"] = json!("request");
        }
    }
}

fn default_effective_coverage_policy(top_n: usize) -> EffectiveCoverageReq {
    EffectiveCoverageReq {
        enabled: Some(true),
        mode: Some("adjust_start".to_string()),
        min_rows: Some(top_n.max(1)),
        include_rebalance_warmup: Some(true),
        warmup_trading_days: Some(19),
    }
}

fn effective_coverage_policy_from_value(
    value: Option<&Value>,
    top_n: usize,
) -> Result<Option<EffectiveCoverageReq>, String> {
    let Some(value) = value else {
        return Ok(Some(default_effective_coverage_policy(top_n)));
    };
    match value {
        Value::Null => Ok(None),
        Value::Bool(false) => Ok(Some(EffectiveCoverageReq {
            enabled: Some(false),
            mode: None,
            min_rows: None,
            include_rebalance_warmup: None,
            warmup_trading_days: None,
        })),
        Value::Bool(true) => Ok(Some(default_effective_coverage_policy(top_n))),
        Value::String(mode) => {
            let mode = mode.trim();
            if mode.is_empty() || mode == "adjust_start" {
                Ok(Some(default_effective_coverage_policy(top_n)))
            } else if mode == "guard_only" {
                Ok(Some(EffectiveCoverageReq {
                    enabled: Some(true),
                    mode: Some("guard_only".to_string()),
                    min_rows: Some(top_n.max(1)),
                    include_rebalance_warmup: Some(true),
                    warmup_trading_days: Some(19),
                }))
            } else if mode == "off" || mode == "disabled" {
                Ok(None)
            } else {
                Err(format!("unsupported effective_coverage policy: {}", mode))
            }
        }
        Value::Object(_) => {
            let mut policy: EffectiveCoverageReq = serde_json::from_value(value.clone())
                .map_err(|error| format!("effective_coverage must be an object: {}", error))?;
            if policy.enabled != Some(false) {
                if policy.enabled.is_none() {
                    policy.enabled = Some(true);
                }
                if policy.mode.is_none() {
                    policy.mode = Some("adjust_start".to_string());
                }
                if policy.min_rows.is_none() {
                    policy.min_rows = Some(top_n.max(1));
                }
                if policy.include_rebalance_warmup.is_none() {
                    policy.include_rebalance_warmup = Some(true);
                }
                if policy.include_rebalance_warmup == Some(true)
                    && policy.warmup_trading_days.is_none()
                {
                    policy.warmup_trading_days = Some(19);
                }
            }
            Ok(Some(policy))
        }
        _ => Err("effective_coverage must be null, boolean, string, or object".to_string()),
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

pub async fn create_phase7_layered_optimization(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7LayeredOptimizationRequest>,
) -> impl IntoResponse {
    match insert_phase7_layered_optimization(&state.db, &req, LocalResourcePlan::local_mac()).await
    {
        Ok((task_id, bundle)) => Json(json!({
            "code": 0,
            "data": {
                "optimization_task_id": task_id,
                "status": "pending",
                "search_method": "phase7_layered_grid",
                "requested_trials": bundle.plan.requested_trials,
                "planned_trials": bundle.plan.planned_trials,
                "truncated": bundle.plan.truncated,
                "batch_size": bundle.plan.batch_size,
                "max_parallel_trials": bundle.plan.max_parallel_trials,
                "resource_plan": bundle.resource_plan,
                "search_profile": bundle.search_space["search_profile"],
            }
        })),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn insert_phase7_layered_optimization(
    db: &sqlx::PgPool,
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
) -> Result<(String, Phase7LayeredPlanBundle), String> {
    insert_phase7_layered_optimization_with_trial_cap(db, req, resource_plan, 500).await
}

async fn insert_phase7_layered_optimization_with_trial_cap(
    db: &sqlx::PgPool,
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
) -> Result<(String, Phase7LayeredPlanBundle), String> {
    insert_phase7_layered_optimization_with_trial_cap_and_internal_train_window_ml_prediction_set(
        db,
        req,
        resource_plan,
        max_trials_cap,
        None,
    )
    .await
}

async fn insert_phase7_layered_optimization_with_trial_cap_and_internal_train_window_ml_prediction_set(
    db: &sqlx::PgPool,
    req: &Phase7LayeredOptimizationRequest,
    resource_plan: LocalResourcePlan,
    max_trials_cap: usize,
    internal_train_window_ml_prediction_set_id: Option<&str>,
) -> Result<(String, Phase7LayeredPlanBundle), String> {
    let bundle =
        build_phase7_layered_plan_bundle_with_trial_cap_and_internal_train_window_ml_prediction_set(
            req,
            resource_plan,
            max_trials_cap,
            internal_train_window_ml_prediction_set_id,
        );
    let task_id = format!("opt-phase7-{}", Uuid::new_v4());
    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin phase7 optimization transaction: {}", error))?;

    let insert_task = sqlx::query(
        "INSERT INTO optimization_task
           (optimization_task_id, strategy_version_id, data_version_id, search_method,
            search_space, objective, constraints, walk_forward_config, backtest_template, status, progress)
         VALUES ($1, $2, $3, 'phase7_layered_grid', $4, $5, $6, $7, $8, 'pending', 0)",
    )
    .bind(&task_id)
    .bind(&req.strategy_version_id)
    .bind(&req.data_version_id)
    .bind(&bundle.search_space)
    .bind(&req.objective)
    .bind(&req.constraints)
    .bind(&req.walk_forward)
    .bind(&req.backtest_template)
    .execute(&mut *tx)
    .await;

    if let Err(error) = insert_task {
        return Err(format!(
            "Failed to create phase7 optimization task: {}",
            error
        ));
    }

    for trial in &bundle.plan.trials {
        let trial_id = format!("trial-{}-{:04}", task_id, trial.trial_index + 1);
        if let Err(error) = sqlx::query(
            "INSERT INTO optimization_trial
               (trial_id, optimization_task_id, trial_index, parameters, status, progress)
             VALUES ($1, $2, $3, $4, 'pending', 0)",
        )
        .bind(&trial_id)
        .bind(&task_id)
        .bind((trial.trial_index + 1) as i32)
        .bind(&trial.parameters)
        .execute(&mut *tx)
        .await
        {
            return Err(format!(
                "Failed to create phase7 optimization trial: {}",
                error
            ));
        }
    }

    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit phase7 optimization task: {}", error))?;

    Ok((task_id, bundle))
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
            "created_at": row.12.map(|ts| ts.to_rfc3339()),
            "trial_total": counts.0,
            "trial_completed": counts.1,
            "trial_failed": counts.2,
        }
    }))
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

pub async fn run_phase7_professional_discovery(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ProfessionalDiscoveryRequest>,
) -> impl IntoResponse {
    match execute_phase7_professional_discovery(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn run_phase7_oos_walk_forward_discovery(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7OosWalkForwardDiscoveryRequest>,
) -> impl IntoResponse {
    let result = match normalize_oos_execution_mode(req.execution_mode.as_deref()) {
        Ok("background") if !req.plan_only.unwrap_or(false) => {
            start_phase7_oos_walk_forward_discovery_background(&state.db, req).await
        }
        Ok(_) => execute_phase7_oos_walk_forward_discovery(&state.db, req, None).await,
        Err(message) => Err(message),
    };

    match result {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn plan_phase7_oos_profile_comparison(
    Json(req): Json<Phase7OosProfileComparisonPlanRequest>,
) -> impl IntoResponse {
    match build_phase7_oos_profile_comparison_plan(
        &req.base,
        req.profiles,
        req.return_risk_cache_comparison,
    ) {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn launch_phase7_oos_profile_comparison_smoke(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7OosProfileComparisonPlanRequest>,
) -> impl IntoResponse {
    match start_phase7_oos_profile_comparison_smoke(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
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

pub async fn report_return_risk_cache_economics(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ReturnRiskCacheEconomicsReportRequest>,
) -> impl IntoResponse {
    match build_return_risk_cache_economics_report(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

async fn execute_phase7_professional_discovery(
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

fn normalize_oos_execution_mode(mode: Option<&str>) -> Result<&'static str, String> {
    match mode
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("inline")
    {
        "inline" | "sync" | "synchronous" => Ok("inline"),
        "background" | "async" | "asynchronous" => Ok("background"),
        other => Err(format!("unsupported execution_mode: {}", other)),
    }
}

fn normalized_trial_concurrency(value: Option<usize>) -> usize {
    value
        .unwrap_or_else(|| LocalResourcePlan::local_mac().batch_size.min(4).max(1))
        .clamp(1, 16)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OosTrainExecutionPolicy {
    requested_cache_mode: &'static str,
    cache_mode: &'static str,
    requested_trial_concurrency: usize,
    trial_concurrency: usize,
}

fn normalize_oos_train_cache_mode(mode: Option<&str>) -> Result<&'static str, String> {
    match mode
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("shared_window")
    {
        "shared_window" | "shared" | "window_shared" | "reuse" => Ok("shared_window"),
        "shared_run" | "run_shared" => Ok("shared_run"),
        "per_trial_isolated" | "isolated" | "parallel_isolated" => Ok("per_trial_isolated"),
        "auto" | "adaptive" | "resource_adaptive" => Ok("auto"),
        other => Err(format!("unsupported train_cache_mode: {}", other)),
    }
}

fn resolve_oos_train_execution_policy(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    resource_plan: &LocalResourcePlan,
) -> Result<OosTrainExecutionPolicy, String> {
    let requested_cache_mode = normalize_oos_train_cache_mode(req.train_cache_mode.as_deref())?;
    let requested_trial_concurrency = normalized_trial_concurrency(req.trial_concurrency);
    let (cache_mode, trial_concurrency) = match requested_cache_mode {
        "shared_window" => ("shared_window", 1),
        "shared_run" => ("shared_run", 1),
        "per_trial_isolated" => ("per_trial_isolated", requested_trial_concurrency),
        "auto" => {
            let planned_trials = req
                .max_trials_per_window
                .unwrap_or(resource_plan.max_trials);
            let enough_work = planned_trials
                >= resource_plan
                    .batch_size
                    .saturating_mul(4)
                    .max(requested_trial_concurrency.saturating_mul(2));
            let enough_resources =
                resource_plan.max_parallel_trials >= 2 && resource_plan.memory_budget_gb >= 8;
            if enough_work && enough_resources {
                (
                    "per_trial_isolated",
                    requested_trial_concurrency.min(resource_plan.max_parallel_trials),
                )
            } else {
                ("shared_window", 1)
            }
        }
        other => return Err(format!("unsupported train_cache_mode: {}", other)),
    };
    Ok(OosTrainExecutionPolicy {
        requested_cache_mode,
        cache_mode,
        requested_trial_concurrency,
        trial_concurrency,
    })
}

async fn start_phase7_oos_walk_forward_discovery_background(
    db: &sqlx::PgPool,
    req: Phase7OosWalkForwardDiscoveryRequest,
) -> Result<Value, String> {
    let plan = build_oos_discovery_plan(&req)?;
    let plan_json = oos_discovery_plan_json(&plan);
    let experiment_run_id =
        create_running_oos_walk_forward_experiment(db, &req, &plan_json).await?;
    let db = db.clone();
    let background_req = req.clone();
    let background_experiment_run_id = experiment_run_id.clone();
    tokio::spawn(async move {
        if let Err(message) = execute_phase7_oos_walk_forward_discovery(
            &db,
            background_req,
            Some(background_experiment_run_id.clone()),
        )
        .await
        {
            error!(
                experiment_run_id = %background_experiment_run_id,
                error = %message,
                "Phase 7 OOS walk-forward discovery failed"
            );
            let _ = mark_oos_walk_forward_experiment_failed(
                &db,
                &background_experiment_run_id,
                &message,
            )
            .await;
        }
    });

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "search_method": "phase7_oos_walk_forward_discovery",
        "execution_mode": "background",
        "status": "running",
        "plan": plan_json,
        "resource_plan": LocalResourcePlan::local_mac(),
        "poll_url": format!("/api/v1/quant/experiments/{}", experiment_run_id),
    }))
}

async fn start_phase7_oos_profile_comparison_smoke(
    db: &sqlx::PgPool,
    req: Phase7OosProfileComparisonPlanRequest,
) -> Result<Value, String> {
    let return_risk_cache_comparison =
        return_risk_cache_comparison_enabled(req.return_risk_cache_comparison);
    let launch_requests = build_phase7_oos_profile_comparison_launch_requests(
        &req.base,
        req.profiles,
        req.return_risk_cache_comparison,
    )?;
    let mut launches = Vec::new();
    for launch_req in launch_requests {
        let search_profile = launch_req
            .search_profile
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let return_risk_feature_cache_mode = request_return_risk_feature_cache_mode(&launch_req)
            .unwrap_or_else(|| "inherited".into());
        let request = oos_request_plan_json(&launch_req);
        let launched = start_phase7_oos_walk_forward_discovery_background(db, launch_req).await?;
        launches.push(json!({
            "search_profile": search_profile,
            "return_risk_feature_cache_mode": return_risk_feature_cache_mode,
            "experiment_run_id": launched["experiment_run_id"],
            "status": launched["status"],
            "poll_url": launched["poll_url"],
            "request": request,
            "plan": launched["plan"],
        }));
    }
    let profile_count = launches
        .iter()
        .filter_map(|launch| launch.get("search_profile").and_then(Value::as_str))
        .collect::<BTreeSet<_>>()
        .len();
    let cache_economics_report_plan =
        cache_economics_report_launch_plan(return_risk_cache_comparison, &launches);

    Ok(json!({
        "search_method": "phase7_oos_profile_comparison_smoke",
        "execution_mode": "background",
        "profile_count": profile_count,
        "launch_count": launches.len(),
        "return_risk_cache_comparison": cache_economics_report_plan,
        "launches": launches,
    }))
}

async fn execute_phase7_oos_walk_forward_discovery(
    db: &sqlx::PgPool,
    req: Phase7OosWalkForwardDiscoveryRequest,
    experiment_run_id: Option<String>,
) -> Result<Value, String> {
    let plan = build_oos_discovery_plan(&req)?;
    let plan_json = oos_discovery_plan_json(&plan);
    if req.plan_only.unwrap_or(false) {
        return Ok(json!({
            "search_method": "phase7_oos_walk_forward_discovery",
            "plan_only": true,
            "plan": plan_json,
            "resource_plan": LocalResourcePlan::local_mac(),
        }));
    }

    let mut window_results = Vec::new();
    let mut stitched_points = Vec::new();
    let mut oos_signal_cache = SignalDataCache::default();
    let mut oos_backtest_cache = BacktestDataCache::default();
    let train_execution_policy =
        resolve_oos_train_execution_policy(&req, &LocalResourcePlan::local_mac())?;
    let shared_run_train_cache = train_execution_policy.cache_mode == "shared_run";
    let mut shared_train_signal_cache = if shared_run_train_cache {
        Some(SignalDataCache::default())
    } else {
        None
    };
    let mut shared_train_backtest_cache = if shared_run_train_cache {
        Some(BacktestDataCache::default())
    } else {
        None
    };
    let train_gate_policy = resolve_oos_train_selection_gate_policy(&req);
    let final_promotion_gate_policy =
        resolve_oos_final_promotion_gate_policy(&req, &plan.validation_mode);
    let oos_top_n = req.oos_top_n.unwrap_or(1).clamp(1, 20);

    for window in &plan.windows {
        let mut local_train_signal_cache = SignalDataCache::default();
        let mut local_train_backtest_cache = BacktestDataCache::default();
        let (train_signal_cache, train_backtest_cache) = if shared_run_train_cache {
            (
                shared_train_signal_cache
                    .as_mut()
                    .expect("shared_run train signal cache"),
                shared_train_backtest_cache
                    .as_mut()
                    .expect("shared_run train backtest cache"),
            )
        } else {
            (
                &mut local_train_signal_cache,
                &mut local_train_backtest_cache,
            )
        };
        let execution = execute_oos_discovery_window(
            db,
            &req,
            &train_execution_policy,
            window,
            oos_top_n,
            &train_gate_policy,
            &final_promotion_gate_policy,
            train_signal_cache,
            train_backtest_cache,
            &mut oos_signal_cache,
            &mut oos_backtest_cache,
        )
        .await?;
        append_stitched_oos_points(&mut stitched_points, &execution.oos_points);
        window_results.push(oos_window_execution_json(&execution, &train_gate_policy));
        if let Some(experiment_run_id) = experiment_run_id.as_deref() {
            let stitched_summary = if stitched_points.len() >= 2 {
                metric_summary_json(&summarize_points(&stitched_points))
            } else {
                json!({})
            };
            update_oos_walk_forward_experiment_progress(
                db,
                experiment_run_id,
                &plan,
                &window_results,
                &stitched_summary,
                &oos_cache_report(&oos_signal_cache, &oos_backtest_cache),
            )
            .await?;
        }
    }

    let stitched_summary = if stitched_points.len() >= 2 {
        metric_summary_json(&summarize_points(&stitched_points))
    } else {
        json!({})
    };
    let gate_report = build_oos_gate_report(
        &req,
        &plan,
        &window_results,
        &stitched_summary,
        &final_promotion_gate_policy,
    );
    let cache_report = oos_cache_report(&oos_signal_cache, &oos_backtest_cache);
    let experiment_run_id = match experiment_run_id {
        Some(existing_experiment_run_id) => {
            complete_oos_walk_forward_experiment(
                db,
                &existing_experiment_run_id,
                &req,
                &plan_json,
                &window_results,
                &stitched_summary,
                &gate_report,
                &cache_report,
            )
            .await?;
            existing_experiment_run_id
        }
        None => {
            persist_oos_walk_forward_experiment(
                db,
                &req,
                &plan_json,
                &window_results,
                &stitched_summary,
                &gate_report,
                &cache_report,
            )
            .await?
        }
    };

    Ok(json!({
        "experiment_run_id": experiment_run_id,
        "search_method": "phase7_oos_walk_forward_discovery",
        "execution_mode": normalize_oos_execution_mode(req.execution_mode.as_deref()).unwrap_or("inline"),
        "plan": plan_json,
        "window_count": plan.windows.len(),
        "windows": window_results,
        "stitched_oos": {
            "point_count": stitched_points.len(),
            "metrics": stitched_summary,
            "gates": gate_report,
            "status": oos_gate_status(&gate_report),
        },
        "cache": cache_report
    }))
}

async fn execute_oos_discovery_window(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_execution_policy: &OosTrainExecutionPolicy,
    window: &OosDiscoveryWindow,
    oos_top_n: usize,
    train_gate_policy: &Value,
    final_promotion_gate_policy: &Value,
    train_signal_cache: &mut SignalDataCache,
    train_backtest_cache: &mut BacktestDataCache,
    oos_signal_cache: &mut SignalDataCache,
    oos_backtest_cache: &mut BacktestDataCache,
) -> Result<OosWindowExecution, String> {
    let train_template = backtest_template_for_window(
        req.backtest_template
            .clone()
            .unwrap_or_else(default_phase7_backtest_template),
        window.train_start,
        window.train_end,
        "summary_only",
    )?;
    let train_template_for_selection = train_template.clone();
    let train_window_ml_prediction_sets =
        prepare_train_window_ml_prediction_sets_for_oos_window(db, req, window).await?;
    let train_req = Phase7ProfessionalDiscoveryRequest {
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        objective: req.objective.clone(),
        constraints: req.constraints.clone(),
        walk_forward: req.walk_forward.clone(),
        backtest_template: Some(train_template),
        prediction_set_ids: req.prediction_set_ids.clone(),
        max_trials: req
            .max_trials_per_window
            .or_else(|| req.exhaustive_search.unwrap_or(false).then_some(100_000)),
        search_profile: req.search_profile.clone(),
        trial_batch_limit: req.trial_batch_limit,
        max_batches: None,
        robustness_top_n: Some(oos_top_n),
        robustness_gate_policy: Some(train_gate_policy.clone()),
        stop_after_professional_candidate: Some(false),
        stop_after_robust_approval: Some(false),
    };
    let layered_req = phase7_discovery_layered_request(&train_req);
    let (train_task_id, bundle) =
        insert_phase7_layered_optimization_with_trial_cap_and_internal_train_window_ml_prediction_set(
            db,
            &layered_req,
            LocalResourcePlan::local_mac(),
            100_000,
            train_window_ml_prediction_sets
                .as_ref()
                .map(|sets| sets.train_prediction_set_id.as_str()),
        )
        .await?;
    let trial_batch_limit = req
        .trial_batch_limit
        .unwrap_or(bundle.plan.batch_size as i64)
        .clamp(1, 2_000);
    let max_batches = req.max_batches_per_window.unwrap_or_else(|| {
        let planned = bundle.plan.planned_trials.max(1);
        let batch = trial_batch_limit.max(1) as usize;
        planned.div_ceil(batch)
    });
    let max_batches = max_batches.clamp(1, 100_000);
    let mut train_batches = Vec::new();
    for _ in 0..max_batches {
        let batch = execute_pending_trials_with_caches_and_concurrency(
            db,
            &train_task_id,
            trial_batch_limit,
            None,
            &mut *train_signal_cache,
            &mut *train_backtest_cache,
            train_execution_policy.trial_concurrency,
        )
        .await?;
        let executed = batch["executed"].as_i64().unwrap_or(0);
        train_batches.push(annotate_oos_train_batch_cache_mode(
            batch,
            train_execution_policy,
        ));
        if executed == 0 {
            break;
        }
    }

    let training_candidates = load_oos_training_candidates(db, &train_task_id, oos_top_n).await?;
    let require_train_approval = req.require_train_robustness_approval.unwrap_or(true);
    let (selected_candidate, train_robustness, train_cost_capacity_perturbations) =
        select_oos_training_candidate(
            db,
            req,
            &train_template_for_selection,
            &train_task_id,
            training_candidates,
            train_gate_policy,
            require_train_approval,
            window.window_index,
            &mut *train_signal_cache,
            &mut *train_backtest_cache,
        )
        .await?;

    let test_template = backtest_template_for_window(
        req.backtest_template
            .clone()
            .unwrap_or_else(default_phase7_backtest_template),
        window.test_start,
        window.test_end,
        "summary_only",
    )?;
    let oos_backtest_task_id = format!("oosbt-{}", Uuid::new_v4());
    let perturbation_template = test_template.clone();
    let oos_parameters = train_window_ml_oos_parameters(
        &selected_candidate.parameters,
        train_window_ml_prediction_sets.as_ref(),
    )?;
    let oos_output = execute_oos_candidate_backtest(
        db,
        req,
        test_template,
        &oos_parameters,
        &oos_backtest_task_id,
        oos_signal_cache,
        oos_backtest_cache,
    )
    .await?;
    let oos_points = load_oos_equity_points(db, &oos_backtest_task_id).await?;
    let cost_capacity_perturbations = execute_oos_cost_capacity_perturbations(
        db,
        req,
        perturbation_template,
        &oos_parameters,
        final_promotion_gate_policy,
        oos_signal_cache,
        oos_backtest_cache,
    )
    .await?;

    Ok(OosWindowExecution {
        window: window.clone(),
        train_optimization_task_id: train_task_id,
        train_execution_policy: train_execution_policy.clone(),
        train_batches,
        selected_candidate,
        train_robustness,
        train_cost_capacity_perturbations,
        oos_backtest_task_id,
        oos_output,
        oos_points,
        cost_capacity_perturbations,
    })
}

async fn prepare_train_window_ml_prediction_sets_for_oos_window(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    window: &OosDiscoveryWindow,
) -> Result<Option<TrainWindowMlPredictionSets>, String> {
    if !is_train_window_ml_stress_fill_profile(req.search_profile.as_deref()) {
        return Ok(None);
    }

    let short_id = Uuid::new_v4().simple().to_string();
    let short_id = &short_id[..12];
    let train_prediction_set_id = format!("p7gb-w{}-{}-tr", window.window_index, short_id);
    let test_prediction_set_id = format!("p7gb-w{}-{}-te", window.window_index, short_id);
    let train_training_task_id = format!("train-p7gb-w{}-{}-tr", window.window_index, short_id);
    let test_training_task_id = format!("train-p7gb-w{}-{}-te", window.window_index, short_id);
    let training_task_id = format!("p7gb-w{}-{}", window.window_index, short_id);
    let label_horizon_days = train_window_ml_label_horizon_days(window);
    let train_lookback_days = train_window_ml_lookback_days(window, label_horizon_days);
    let train_prediction_start =
        window.train_start + Duration::days(train_lookback_days + label_horizon_days - 1);
    if train_prediction_start > window.train_end {
        return Err(format!(
            "phase7_gb train window {} is too short for train-window ML ranking: train_start={}, train_end={}, lookback_days={}, label_horizon_days={}",
            window.window_index,
            window.train_start,
            window.train_end,
            train_lookback_days,
            label_horizon_days
        ));
    }

    let feature_profile = phase7_train_window_ml_feature_profile();
    let factors = phase7_train_window_ml_factor_refs();
    if factors.is_empty() {
        return Err("phase7_gb train-window ML ranking factors must not be empty".into());
    }

    create_walk_forward_nonlinear_quantile_ranker_inner(
        db,
        WalkForwardNonlinearQuantileRankerRequest {
            model_code: "phase7_gb_train_window_nlq_ranker".to_string(),
            model_version: format!("w{}-{}-train", window.window_index, short_id),
            model_version_id: Some(format!("p7gb-nlq-w{}-{}-tr", window.window_index, short_id)),
            training_task_id: Some(train_training_task_id),
            prediction_set_id: Some(train_prediction_set_id.clone()),
            data_version_id: req.data_version_id.clone(),
            feature_set_version_id: feature_profile.to_string(),
            training_dataset_id: format!("ds-p7gb-w{}-{}-tr", window.window_index, short_id),
            prediction_start_date: train_prediction_start.format("%Y%m%d").to_string(),
            prediction_end_date: window.train_end.format("%Y%m%d").to_string(),
            train_lookback_days: Some(train_lookback_days),
            prediction_step_days: Some(20),
            label_horizon_days: Some(label_horizon_days),
            label_objective: Some("risk_adjusted_excess_return".to_string()),
            min_training_samples: Some(250),
            max_windows: None,
            bucket_count: Some(7),
            min_samples_per_bucket: Some(250),
            factors: factors.clone(),
        },
    )
    .await?;

    train_nonlinear_quantile_ranker_inner(
        db,
        TrainNonlinearQuantileRankerRequest {
            model_code: "phase7_gb_train_window_nlq_ranker".to_string(),
            model_version: format!("w{}-{}-test", window.window_index, short_id),
            model_version_id: Some(format!("p7gb-nlq-w{}-{}-te", window.window_index, short_id)),
            training_task_id: Some(test_training_task_id),
            prediction_set_id: Some(test_prediction_set_id.clone()),
            data_version_id: req.data_version_id.clone(),
            feature_set_version_id: feature_profile.to_string(),
            training_dataset_id: format!("ds-p7gb-w{}-{}-te", window.window_index, short_id),
            train_start_date: window.train_start.format("%Y%m%d").to_string(),
            train_end_date: window.train_end.format("%Y%m%d").to_string(),
            prediction_start_date: window.test_start.format("%Y%m%d").to_string(),
            prediction_end_date: window.test_end.format("%Y%m%d").to_string(),
            label_horizon_days: Some(label_horizon_days),
            label_objective: Some("risk_adjusted_excess_return".to_string()),
            bucket_count: Some(7),
            min_samples_per_bucket: Some(250),
            factors,
        },
    )
    .await?;

    Ok(Some(TrainWindowMlPredictionSets {
        train_prediction_set_id,
        test_prediction_set_id,
        training_task_id,
    }))
}

fn train_window_ml_label_horizon_days(window: &OosDiscoveryWindow) -> i64 {
    let train_days = (window.train_end - window.train_start).num_days().max(1);
    45.min((train_days / 4).max(5)).max(5)
}

fn train_window_ml_lookback_days(window: &OosDiscoveryWindow, label_horizon_days: i64) -> i64 {
    let train_days = (window.train_end - window.train_start).num_days().max(1);
    let max_lookback = (train_days - label_horizon_days - 5).max(30);
    252.min(max_lookback).max(30)
}

fn phase7_train_window_ml_feature_profile() -> &'static str {
    "phase7_gb_quality_value_recovery_low_impact_v2"
}

fn phase7_train_window_ml_factor_refs() -> Vec<LinearFactorRef> {
    phase7_train_window_ml_factor_refs_for_profile(phase7_train_window_ml_feature_profile())
}

fn phase7_train_window_ml_factor_refs_for_profile(profile: &str) -> Vec<LinearFactorRef> {
    let factor_codes: &[&str] = match profile {
        "phase7_gb_quality_value_recovery_low_impact_v2" => &[
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_current_ratio_daily_std",
            "fin_current_ratio_indrel_daily_std",
            "fin_debt_to_assets_daily_std",
            "fin_debt_to_assets_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_indrel_daily_std",
            "fin_netprofit_margin_yoy_delta_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "div_cash_sum_4y_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            "rev_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_net_amount_20d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
        ],
        "phase7_gb_quality_value_recovery_low_impact_v3" => &[
            // Financial quality (v2 base + industry-relative)
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_current_ratio_daily_std",
            "fin_current_ratio_indrel_daily_std",
            "fin_debt_to_assets_daily_std",
            "fin_debt_to_assets_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_indrel_daily_std",
            // Financial improvement / earnings recovery
            "fin_netprofit_margin_yoy_delta_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            // Valuation
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            // Cashflow quality
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            // Dividend quality
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "div_cash_sum_4y_std",
            // Multi-horizon momentum (5d/20d/60d)
            "mom_5d_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            // Multi-horizon reversal (5d/20d)
            "rev_5d_std",
            "rev_20d_std",
            // Liquidity & capacity
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            // Volatility & risk
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            // Money flow
            "mf_net_amount_5d_std",
            "mf_elg_net_amount_5d_std",
            "mf_net_amount_20d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
        ],
        _ => &[
            "fin_roe_daily_std",
            "fin_roe_indrel_daily_std",
            "fin_roa_indrel_daily_std",
            "fin_netprofit_margin_daily_std",
            "fin_netprofit_margin_indrel_daily_std",
            "fin_gross_margin_daily_std",
            "fin_gross_margin_yoy_delta_std",
            "fin_debt_to_assets_yoy_improve_std",
            "fin_eps_yoy_recovery_std",
            "fin_roe_yoy_delta_std",
            "fin_current_ratio_yoy_delta_std",
            "val_pb_low_std",
            "val_pe_ttm_low_std",
            "val_ps_ttm_low_std",
            "val_dividend_yield_ttm_std",
            "cf_ocf_to_profit_latest_std",
            "cf_ocf_positive_latest_std",
            "cf_ocf_profit_gap_latest_std",
            "cf_cash_buffer_latest_std",
            "div_recent_positive_std",
            "div_paid_years_4y_std",
            "div_stability_4y_std",
            "mom_20d_std",
            "mom_60d_std",
            "mkt_rel_mom_20d_std",
            "mkt_rel_mom_60d_std",
            "ind_rel_mom_20d_std",
            "ind_rel_mom_60d_std",
            "rev_20d_std",
            "amihud_20d_std",
            "amt_intensity_20d_std",
            "turn_20d_std",
            "vol_20d_std",
            "downvol_20d_std",
            "maxdd_60d_std",
            "mf_lg_elg_net_amount_20d_std",
            "mf_small_sell_pressure_20d_std",
        ],
    };

    factor_codes
        .iter()
        .map(|factor_code| LinearFactorRef {
            factor_code: (*factor_code).to_string(),
            factor_version: "1.0.0".to_string(),
        })
        .collect()
}

async fn execute_oos_cost_capacity_perturbations(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    test_template: Value,
    parameters: &Value,
    final_promotion_gate_policy: &Value,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
) -> Result<Vec<OosCostCapacityPerturbationResult>, String> {
    let perturbations = resolved_oos_cost_capacity_perturbations(req);
    let mut results = Vec::with_capacity(perturbations.len());
    for (index, perturbation) in perturbations.into_iter().enumerate() {
        let name = oos_cost_capacity_perturbation_name(&perturbation, index);
        let perturbed_parameters =
            apply_cost_capacity_perturbation_to_parameters(parameters, &perturbation)?;
        let backtest_task_id = format!("oosstress-{}", Uuid::new_v4());
        let output = execute_oos_candidate_backtest(
            db,
            req,
            test_template.clone(),
            &perturbed_parameters,
            &backtest_task_id,
            &mut *signal_cache,
            &mut *backtest_cache,
        )
        .await?;
        let passed =
            oos_cost_capacity_perturbation_passed(req, final_promotion_gate_policy, &output);
        results.push(OosCostCapacityPerturbationResult {
            name,
            perturbation,
            backtest_task_id,
            output,
            passed,
        });
    }
    Ok(results)
}

async fn execute_train_cost_capacity_perturbations(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_template: &Value,
    parameters: &Value,
    train_gate_policy: &Value,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
) -> Result<Vec<OosCostCapacityPerturbationResult>, String> {
    let gate = train_cost_capacity_perturbation_gate_config(req, train_gate_policy);
    if !gate.enabled {
        return Ok(Vec::new());
    }

    let perturbations = resolved_train_cost_capacity_perturbations(req, train_gate_policy);
    let mut results = Vec::with_capacity(perturbations.len());
    for (index, perturbation) in perturbations.into_iter().enumerate() {
        let name = oos_cost_capacity_perturbation_name(&perturbation, index);
        let perturbed_parameters =
            apply_cost_capacity_perturbation_to_parameters(parameters, &perturbation)?;
        let backtest_task_id = format!("trainstress-{}", Uuid::new_v4());
        let output = execute_oos_candidate_backtest(
            db,
            req,
            train_template.clone(),
            &perturbed_parameters,
            &backtest_task_id,
            &mut *signal_cache,
            &mut *backtest_cache,
        )
        .await?;
        let passed = cost_capacity_perturbation_passed_with_thresholds(
            &output,
            gate.min_perturbed_calmar,
            gate.max_perturbed_drawdown_pct,
        );
        results.push(OosCostCapacityPerturbationResult {
            name,
            perturbation,
            backtest_task_id,
            output,
            passed,
        });
    }
    Ok(results)
}

async fn execute_oos_candidate_backtest(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    test_template: Value,
    parameters: &Value,
    backtest_task_id: &str,
    signal_cache: &mut SignalDataCache,
    backtest_cache: &mut BacktestDataCache,
) -> Result<FactorBacktestRunOutput, String> {
    let task = OptimizationTaskExecutionContext {
        strategy_version_id: req.strategy_version_id.clone(),
        data_version_id: req.data_version_id.clone(),
        backtest_template: test_template,
        objective: req.objective.clone().unwrap_or_else(|| {
            json!({
                "type": "professional_candidate",
                "benchmark": "000300.SH",
                "maximize": true
            })
        }),
        constraints: req.constraints.clone(),
    };
    match build_optimization_trial_request(&task, parameters)? {
        OptimizationTrialBacktestRequest::Factor(request) => {
            let output = execute_factor_backtest_with_caches(
                db,
                backtest_task_id,
                request,
                Some(signal_cache),
                Some(backtest_cache),
            )
            .await?;
            if let Some(report) = output.market_data_prewarm_report.as_ref() {
                let _ = report;
            }
            Ok(output)
        }
        OptimizationTrialBacktestRequest::Prediction(request) => {
            execute_prediction_backtest(db, backtest_task_id, request).await
        }
    }
}

fn cost_capacity_perturbation_gate_enabled(req: &Phase7OosWalkForwardDiscoveryRequest) -> bool {
    req.enable_cost_capacity_perturbation_gate
        .unwrap_or_else(|| {
            req.cost_capacity_perturbations
                .as_ref()
                .map(|items| !items.is_empty())
                .unwrap_or(false)
        })
}

fn resolved_oos_cost_capacity_perturbations(
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Vec<OosCostCapacityPerturbationRequest> {
    if !cost_capacity_perturbation_gate_enabled(req) {
        return Vec::new();
    }
    req.cost_capacity_perturbations
        .clone()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(default_oos_cost_capacity_perturbations)
}

fn resolved_train_cost_capacity_perturbations(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_gate_policy: &Value,
) -> Vec<OosCostCapacityPerturbationRequest> {
    let gate = train_cost_capacity_perturbation_gate_config(req, train_gate_policy);
    if !gate.enabled {
        return Vec::new();
    }
    req.cost_capacity_perturbations
        .clone()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(default_oos_cost_capacity_perturbations)
}

fn train_cost_capacity_perturbation_gate_config(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_gate_policy: &Value,
) -> OosCostCapacityGateConfig {
    let enabled = constraint_bool(
        Some(train_gate_policy),
        "enable_train_cost_capacity_perturbation_gate",
    )
    .or_else(|| {
        constraint_bool(
            Some(train_gate_policy),
            "enable_cost_capacity_perturbation_gate",
        )
    })
    .unwrap_or(false);
    let min_pass_ratio = constraint_f64(
        Some(train_gate_policy),
        "min_train_cost_capacity_perturbation_pass_ratio",
    )
    .or_else(|| {
        constraint_f64(
            Some(train_gate_policy),
            "min_cost_capacity_perturbation_pass_ratio",
        )
    })
    .or(req.min_cost_capacity_perturbation_pass_ratio)
    .unwrap_or(0.80)
    .clamp(0.0, 1.0);
    let min_perturbed_calmar =
        constraint_f64(Some(train_gate_policy), "min_train_perturbed_calmar")
            .or_else(|| constraint_f64(Some(train_gate_policy), "min_perturbed_oos_calmar"))
            .or(req.min_perturbed_oos_calmar)
            .unwrap_or(1.2);
    let max_perturbed_drawdown_pct =
        constraint_f64(Some(train_gate_policy), "max_train_perturbed_drawdown_pct")
            .or_else(|| constraint_f64(Some(train_gate_policy), "max_perturbed_oos_drawdown_pct"))
            .or(req.max_perturbed_oos_drawdown_pct)
            .unwrap_or(0.35);

    OosCostCapacityGateConfig {
        enabled,
        min_pass_ratio,
        min_perturbed_calmar,
        max_perturbed_drawdown_pct,
    }
}

fn train_cost_capacity_stress_aware_selection_enabled(
    train_gate_policy: &Value,
    gate: &OosCostCapacityGateConfig,
) -> bool {
    if !gate.enabled {
        return false;
    }
    constraint_bool(
        Some(train_gate_policy),
        "enable_train_cost_capacity_stress_aware_selection",
    )
    .or_else(|| constraint_bool(Some(train_gate_policy), "stress_aware_train_selection"))
    .unwrap_or(true)
}

fn best_effort_train_selection_for_diagnostics_enabled(train_gate_policy: &Value) -> bool {
    constraint_bool(
        Some(train_gate_policy),
        "allow_best_effort_train_selection_for_diagnostics",
    )
    .or_else(|| {
        constraint_bool(
            Some(train_gate_policy),
            "diagnostic_best_effort_train_selection",
        )
    })
    .unwrap_or(false)
}

fn default_oos_cost_capacity_perturbations() -> Vec<OosCostCapacityPerturbationRequest> {
    vec![
        OosCostCapacityPerturbationRequest {
            name: Some("cost_up_150pct".to_string()),
            cost_multiplier: Some(1.5),
            slippage_bps: Some(0.0002),
            impact_cost_coefficient: None,
            max_participation_rate: None,
            capacity_penalty_strength: None,
        },
        OosCostCapacityPerturbationRequest {
            name: Some("impact_cost_2pct_participation_10pct".to_string()),
            cost_multiplier: Some(1.0),
            slippage_bps: None,
            impact_cost_coefficient: Some(0.02),
            max_participation_rate: Some(0.10),
            capacity_penalty_strength: None,
        },
        OosCostCapacityPerturbationRequest {
            name: Some("capacity_tight_participation_5pct".to_string()),
            cost_multiplier: None,
            slippage_bps: None,
            impact_cost_coefficient: None,
            max_participation_rate: Some(0.05),
            capacity_penalty_strength: Some(1.0),
        },
    ]
}

fn oos_cost_capacity_perturbation_name(
    perturbation: &OosCostCapacityPerturbationRequest,
    index: usize,
) -> String {
    perturbation
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("cost_capacity_perturbation_{}", index + 1))
}

fn apply_cost_capacity_perturbation_to_parameters(
    parameters: &Value,
    perturbation: &OosCostCapacityPerturbationRequest,
) -> Result<Value, String> {
    let mut object = parameters
        .as_object()
        .cloned()
        .ok_or_else(|| "selected candidate parameters must be a JSON object".to_string())?;
    if let Some(value) = perturbation.capacity_penalty_strength {
        object.insert("capacity_penalty_strength".to_string(), json!(value));
    }
    upsert_nested_number(
        &mut object,
        "cost_model",
        "cost_multiplier",
        perturbation.cost_multiplier,
    )?;
    upsert_nested_number(
        &mut object,
        "cost_model",
        "slippage_bps",
        perturbation.slippage_bps,
    )?;
    upsert_nested_number(
        &mut object,
        "cost_model",
        "impact_cost_coefficient",
        perturbation.impact_cost_coefficient,
    )?;
    upsert_nested_number(
        &mut object,
        "execution_rules",
        "max_participation_rate",
        perturbation.max_participation_rate,
    )?;
    Ok(Value::Object(object))
}

fn upsert_nested_number(
    object: &mut Map<String, Value>,
    parent: &str,
    child: &str,
    value: Option<f64>,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    match object.entry(parent.to_string()) {
        serde_json::map::Entry::Vacant(entry) => {
            entry.insert(json!({ child: value }));
        }
        serde_json::map::Entry::Occupied(mut entry) => {
            let parent_object = entry
                .get_mut()
                .as_object_mut()
                .ok_or_else(|| format!("{} must be a JSON object", parent))?;
            parent_object.insert(child.to_string(), json!(value));
        }
    }
    Ok(())
}

fn oos_cost_capacity_perturbation_passed(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    final_promotion_gate_policy: &Value,
    output: &FactorBacktestRunOutput,
) -> bool {
    let min_calmar = req
        .min_perturbed_oos_calmar
        .or_else(|| {
            constraint_f64(
                Some(final_promotion_gate_policy),
                "min_perturbed_oos_calmar",
            )
        })
        .unwrap_or(1.2);
    let max_drawdown = req
        .max_perturbed_oos_drawdown_pct
        .or_else(|| {
            constraint_f64(
                Some(final_promotion_gate_policy),
                "max_perturbed_oos_drawdown_pct",
            )
        })
        .unwrap_or(0.35);
    cost_capacity_perturbation_passed_with_thresholds(output, min_calmar, max_drawdown)
}

fn cost_capacity_perturbation_passed_with_thresholds(
    output: &FactorBacktestRunOutput,
    min_calmar: f64,
    max_drawdown: f64,
) -> bool {
    let min_calmar = Decimal::from_f64_retain(min_calmar).unwrap_or(Decimal::ZERO);
    let max_drawdown = Decimal::from_f64_retain(max_drawdown).unwrap_or(Decimal::MAX);
    output.metrics.calmar_ratio >= min_calmar && output.metrics.max_drawdown_pct <= max_drawdown
}

fn build_cost_capacity_pass_ratio_gate(
    gate_name: &str,
    passed_count: usize,
    total_count: usize,
    gate: &OosCostCapacityGateConfig,
) -> Value {
    let pass_ratio = ratio(passed_count, total_count);
    json!({
        "gate": gate_name,
        "passed": total_count > 0 && pass_ratio >= gate.min_pass_ratio,
        "limit": gate.min_pass_ratio,
        "actual": pass_ratio,
        "passed_count": passed_count,
        "total_count": total_count,
        "min_perturbed_oos_calmar": gate.min_perturbed_calmar,
        "max_perturbed_oos_drawdown_pct": gate.max_perturbed_drawdown_pct,
    })
}

fn cost_capacity_perturbation_pass_counts(
    results: &[OosCostCapacityPerturbationResult],
) -> (usize, usize) {
    (
        results.iter().filter(|result| result.passed).count(),
        results.len(),
    )
}

fn cost_capacity_perturbation_summary(
    results: &[OosCostCapacityPerturbationResult],
) -> CostCapacityPerturbationSummary {
    let (passed_count, total_count) = cost_capacity_perturbation_pass_counts(results);
    let pass_ratio_ppm = if total_count == 0 {
        0
    } else {
        ((passed_count as i64) * 1_000_000) / total_count as i64
    };
    let mut min_calmar = Decimal::MAX;
    let mut total_calmar = Decimal::ZERO;
    let mut max_drawdown = Decimal::ZERO;
    let mut min_annual_return = Decimal::MAX;
    let mut total_sharpe = Decimal::ZERO;
    let mut min_sortino = Decimal::MAX;
    let mut min_num_trades = u64::MAX;
    let mut max_final_cash_weight = Decimal::ZERO;
    let mut total_final_cash_weight = Decimal::ZERO;
    let mut min_final_actual_gross_exposure = Decimal::MAX;
    let mut max_final_unfilled_target_gap = Decimal::ZERO;
    let mut total_final_unfilled_target_gap = Decimal::ZERO;
    let mut min_final_execution_fill_ratio = Decimal::MAX;
    let mut total_final_execution_fill_ratio = Decimal::ZERO;
    let mut max_execution_target_gap = Decimal::ZERO;
    let mut max_execution_schedule_expired_count = 0usize;
    let mut total_execution_schedule_expired_count = 0usize;

    for result in results {
        let metrics = &result.output.metrics;
        min_calmar = min_calmar.min(metrics.calmar_ratio);
        total_calmar += metrics.calmar_ratio;
        max_drawdown = max_drawdown.max(metrics.max_drawdown_pct);
        min_annual_return = min_annual_return.min(metrics.annual_return_pct);
        total_sharpe += metrics.sharpe_ratio;
        min_sortino = min_sortino.min(metrics.sortino_ratio);
        min_num_trades = min_num_trades.min(metrics.num_trades as u64);
        max_final_cash_weight = max_final_cash_weight.max(metrics.final_cash_weight_pct);
        total_final_cash_weight += metrics.final_cash_weight_pct;
        min_final_actual_gross_exposure =
            min_final_actual_gross_exposure.min(metrics.final_actual_gross_exposure_pct);
        max_final_unfilled_target_gap =
            max_final_unfilled_target_gap.max(metrics.final_unfilled_target_gap_pct);
        total_final_unfilled_target_gap += metrics.final_unfilled_target_gap_pct;
        min_final_execution_fill_ratio =
            min_final_execution_fill_ratio.min(metrics.final_execution_fill_ratio);
        total_final_execution_fill_ratio += metrics.final_execution_fill_ratio;
        max_execution_target_gap =
            max_execution_target_gap.max(metrics.max_execution_target_gap_pct);
        max_execution_schedule_expired_count =
            max_execution_schedule_expired_count.max(metrics.execution_schedule_expired_count);
        total_execution_schedule_expired_count += metrics.execution_schedule_expired_count;
    }

    if total_count == 0 {
        min_calmar = Decimal::ZERO;
        min_annual_return = Decimal::ZERO;
        min_sortino = Decimal::ZERO;
        min_num_trades = 0;
        min_final_actual_gross_exposure = Decimal::ZERO;
        min_final_execution_fill_ratio = Decimal::ONE;
    }
    let denominator = Decimal::from(total_count.max(1) as i64);
    CostCapacityPerturbationSummary {
        passed_count,
        total_count,
        pass_ratio_ppm,
        min_calmar,
        avg_calmar: total_calmar / denominator,
        max_drawdown,
        min_annual_return,
        avg_sharpe: total_sharpe / denominator,
        min_sortino,
        min_num_trades,
        max_final_cash_weight,
        avg_final_cash_weight: total_final_cash_weight / denominator,
        min_final_actual_gross_exposure,
        max_final_unfilled_target_gap,
        avg_final_unfilled_target_gap: total_final_unfilled_target_gap / denominator,
        min_final_execution_fill_ratio,
        avg_final_execution_fill_ratio: total_final_execution_fill_ratio / denominator,
        max_execution_target_gap,
        max_execution_schedule_expired_count,
        total_execution_schedule_expired_count,
    }
}

#[cfg(test)]
fn cost_capacity_perturbation_summary_from_counts(
    passed_count: usize,
    total_count: usize,
) -> CostCapacityPerturbationSummary {
    let pass_ratio_ppm = if total_count == 0 {
        0
    } else {
        ((passed_count as i64) * 1_000_000) / total_count as i64
    };
    CostCapacityPerturbationSummary {
        passed_count,
        total_count,
        pass_ratio_ppm,
        min_calmar: Decimal::ZERO,
        avg_calmar: Decimal::ZERO,
        max_drawdown: Decimal::ZERO,
        min_annual_return: Decimal::ZERO,
        avg_sharpe: Decimal::ZERO,
        min_sortino: Decimal::ZERO,
        min_num_trades: total_count as u64,
        max_final_cash_weight: Decimal::ZERO,
        avg_final_cash_weight: Decimal::ZERO,
        min_final_actual_gross_exposure: Decimal::ONE,
        max_final_unfilled_target_gap: Decimal::ZERO,
        avg_final_unfilled_target_gap: Decimal::ZERO,
        min_final_execution_fill_ratio: Decimal::ONE,
        avg_final_execution_fill_ratio: Decimal::ZERO,
        max_execution_target_gap: Decimal::ZERO,
        max_execution_schedule_expired_count: 0,
        total_execution_schedule_expired_count: 0,
    }
}

fn clamp_decimal(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal {
    value.max(lower).min(upper)
}

fn train_cost_capacity_stress_score_profile(train_gate_policy: &Value) -> &'static str {
    let profile = constraint_str(Some(train_gate_policy), "train_stress_score_profile")
        .or_else(|| constraint_str(Some(train_gate_policy), "stress_aware_selection_score"))
        .or_else(|| {
            constraint_str(
                Some(train_gate_policy),
                "train_cost_capacity_stress_score_profile",
            )
        })
        .unwrap_or("train_stress_adjusted_score_v1")
        .trim();
    match profile {
        "capacity_stress_calmar_score_v1"
        | "capacity_stress"
        | "capacity_aware"
        | "capacity_calmar" => "capacity_stress_calmar_score_v1",
        "cash_drag_fill_gap_score_v1"
        | "cash_drag_fill_gap"
        | "cash_drag_aware"
        | "fill_gap_aware" => "cash_drag_fill_gap_score_v1",
        "capacity_stress_return_score_v1"
        | "capacity_stress_return"
        | "capacity_return"
        | "stress_return" => "capacity_stress_return_score_v1",
        "stress_fill_objective_score_v1"
        | "stress_fill_objective"
        | "fill_objective"
        | "stress_fill" => "stress_fill_objective_score_v1",
        "prediction_confidence_stress_fill_objective_score_v1"
        | "prediction_confidence_stress_fill"
        | "confidence_stress_fill"
        | "ml_confidence_stress_fill" => "prediction_confidence_stress_fill_objective_score_v1",
        "prediction_confidence_stress_fill_quality_score_v1"
        | "prediction_confidence_stress_fill_quality"
        | "confidence_stress_fill_quality"
        | "ml_confidence_stress_fill_quality" => {
            "prediction_confidence_stress_fill_quality_score_v1"
        }
        _ => "train_stress_adjusted_score_v1",
    }
}

fn train_candidate_stress_adjusted_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
) -> Decimal {
    let pass_ratio_score = Decimal::from(summary.pass_ratio_ppm);
    let min_calmar_score = clamp_decimal(summary.min_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(100_000);
    let avg_calmar_score = clamp_decimal(summary.avg_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(25_000);
    let avg_sharpe_score = clamp_decimal(summary.avg_sharpe, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(20_000);
    let min_return_score = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(30_000);
    let drawdown_penalty = clamp_decimal(summary.max_drawdown, Decimal::ZERO, Decimal::from(1))
        * Decimal::from(50_000);
    let professional_gap_penalty = clamp_decimal(
        candidate.professional_gap_score,
        Decimal::ZERO,
        Decimal::from(10),
    ) * Decimal::from(20_000);
    let raw_score_bonus = clamp_decimal(
        candidate.score.unwrap_or(Decimal::ZERO),
        Decimal::from(-100),
        Decimal::from(100),
    ) * Decimal::from(1_000);
    let base_quality_score = candidate.metrics.sharpe * Decimal::from(10_000)
        + candidate.metrics.sortino * Decimal::from(5_000)
        + candidate.metrics.annual_return * Decimal::from(5_000)
        - candidate.metrics.max_drawdown * Decimal::from(20_000)
        - professional_gap_penalty
        + raw_score_bonus;

    pass_ratio_score + min_calmar_score + avg_calmar_score + avg_sharpe_score + min_return_score
        - drawdown_penalty
        + base_quality_score
}

fn train_candidate_capacity_stress_calmar_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let target_calmar =
        constraint_decimal(Some(train_gate_policy), "capacity_stress_target_calmar")
            .or_else(|| constraint_decimal(Some(train_gate_policy), "min_train_perturbed_calmar"))
            .or_else(|| constraint_decimal(Some(train_gate_policy), "min_perturbed_oos_calmar"))
            .unwrap_or_else(|| Decimal::new(12, 1));
    let target_annual_return = constraint_decimal(
        Some(train_gate_policy),
        "capacity_stress_target_annual_return",
    )
    .unwrap_or_else(|| Decimal::new(5, 2));
    let pass_ratio_score = Decimal::from(summary.pass_ratio_ppm) * Decimal::from(2);
    let min_calmar_score = clamp_decimal(summary.min_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(220_000);
    let avg_calmar_score = clamp_decimal(summary.avg_calmar, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(60_000);
    let min_return_score = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(120_000);
    let avg_sharpe_score = clamp_decimal(summary.avg_sharpe, Decimal::from(-5), Decimal::from(5))
        * Decimal::from(15_000);
    let drawdown_penalty = clamp_decimal(summary.max_drawdown, Decimal::ZERO, Decimal::from(1))
        * Decimal::from(120_000);
    let calmar_shortfall_penalty =
        (target_calmar - summary.min_calmar).max(Decimal::ZERO) * Decimal::from(300_000);
    let annual_return_shortfall_penalty = (target_annual_return - summary.min_annual_return)
        .max(Decimal::ZERO)
        * Decimal::from(220_000);
    let base_quality_score = candidate.metrics.sharpe * Decimal::from(2_000)
        + candidate.metrics.sortino * Decimal::from(1_000)
        + candidate.metrics.annual_return * Decimal::from(2_000)
        - candidate.metrics.max_drawdown * Decimal::from(5_000);

    pass_ratio_score
        + min_calmar_score
        + avg_calmar_score
        + min_return_score
        + avg_sharpe_score
        + base_quality_score
        - drawdown_penalty
        - calmar_shortfall_penalty
        - annual_return_shortfall_penalty
}

fn train_candidate_capacity_stress_return_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let base = train_candidate_capacity_stress_calmar_score(candidate, summary, train_gate_policy);
    let target_annual_return = constraint_decimal(
        Some(train_gate_policy),
        "capacity_stress_target_annual_return",
    )
    .unwrap_or_else(|| Decimal::new(15, 2));
    let min_annual_return =
        constraint_decimal(Some(train_gate_policy), "min_train_perturbed_annual_return")
            .unwrap_or_else(|| Decimal::new(5, 2));
    let target_calmar =
        constraint_decimal(Some(train_gate_policy), "capacity_stress_target_calmar")
            .or_else(|| constraint_decimal(Some(train_gate_policy), "min_train_perturbed_calmar"))
            .unwrap_or_else(|| Decimal::new(2, 0));
    let min_annual_shortfall =
        positive_decimal_gap(min_annual_return, summary.min_annual_return) * Decimal::from(520_000);
    let target_annual_shortfall =
        positive_decimal_gap(target_annual_return, summary.min_annual_return)
            * Decimal::from(260_000);
    let target_calmar_shortfall =
        positive_decimal_gap(target_calmar, summary.min_calmar) * Decimal::from(180_000);
    let stress_return_bonus = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(220_000);
    let execution_quality_penalty =
        train_execution_quality_penalty(candidate, summary, train_gate_policy);

    base + stress_return_bonus
        - min_annual_shortfall
        - target_annual_shortfall
        - target_calmar_shortfall
        - execution_quality_penalty
}

fn train_execution_quality_penalty(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let mut penalty = Decimal::ZERO;
    if let Some(limit) = constraint_i64(Some(train_gate_policy), "min_train_trade_count") {
        let actual = candidate
            .metrics
            .num_trades
            .min(summary.min_num_trades)
            .min(i64::MAX as u64) as i64;
        penalty += Decimal::from((limit - actual).max(0)) * Decimal::from(12_000);
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_actual_gross_exposure_pct",
    ) {
        let actual = candidate
            .metrics
            .final_actual_gross_exposure
            .min(summary.min_final_actual_gross_exposure);
        penalty += positive_decimal_gap(limit, actual) * Decimal::from(700_000);
    }
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
    {
        let actual = candidate
            .metrics
            .final_cash_weight
            .max(summary.max_final_cash_weight);
        penalty += positive_decimal_gap(actual, limit) * Decimal::from(420_000);
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    ) {
        let actual = candidate
            .metrics
            .final_unfilled_target_gap
            .max(summary.max_final_unfilled_target_gap);
        penalty += positive_decimal_gap(actual, limit) * Decimal::from(620_000);
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    ) {
        let actual = candidate
            .metrics
            .final_execution_fill_ratio
            .min(summary.min_final_execution_fill_ratio);
        penalty += positive_decimal_gap(limit, actual) * Decimal::from(480_000);
    }
    penalty
}

fn train_candidate_stress_fill_objective_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let base = train_candidate_capacity_stress_return_score(candidate, summary, train_gate_policy);
    let min_exposure_limit = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_actual_gross_exposure_pct",
    )
    .unwrap_or_else(|| Decimal::new(35, 2));
    let max_cash_limit =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
            .unwrap_or_else(|| Decimal::new(65, 2));
    let max_gap_limit = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    )
    .unwrap_or_else(|| Decimal::new(6, 2));
    let min_fill_limit = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    )
    .unwrap_or_else(|| Decimal::new(95, 2));

    let actual_exposure = candidate
        .metrics
        .final_actual_gross_exposure
        .min(summary.min_final_actual_gross_exposure);
    let cash_weight = candidate
        .metrics
        .final_cash_weight
        .max(summary.max_final_cash_weight);
    let unfilled_gap = candidate
        .metrics
        .final_unfilled_target_gap
        .max(summary.max_final_unfilled_target_gap);
    let fill_ratio = candidate
        .metrics
        .final_execution_fill_ratio
        .min(summary.min_final_execution_fill_ratio);

    let tradable_exposure_bonus =
        clamp_decimal(actual_exposure, Decimal::ZERO, Decimal::ONE) * Decimal::from(260_000);
    let fill_ratio_bonus =
        clamp_decimal(fill_ratio, Decimal::ZERO, Decimal::ONE) * Decimal::from(180_000);
    let cash_efficiency_bonus =
        positive_decimal_gap(max_cash_limit, cash_weight) * Decimal::from(120_000);
    let exposure_shortfall_penalty =
        positive_decimal_gap(min_exposure_limit, actual_exposure) * Decimal::from(1_200_000);
    let cash_overrun_penalty =
        positive_decimal_gap(cash_weight, max_cash_limit) * Decimal::from(900_000);
    let gap_overrun_penalty =
        positive_decimal_gap(unfilled_gap, max_gap_limit) * Decimal::from(1_100_000);
    let fill_shortfall_penalty =
        positive_decimal_gap(min_fill_limit, fill_ratio) * Decimal::from(950_000);

    base + tradable_exposure_bonus + fill_ratio_bonus + cash_efficiency_bonus
        - exposure_shortfall_penalty
        - cash_overrun_penalty
        - gap_overrun_penalty
        - fill_shortfall_penalty
}

fn candidate_prediction_confidence_score(candidate: &DiscoveryCandidate) -> Decimal {
    let Some(parameters) = candidate.parameters.as_object() else {
        return Decimal::ZERO;
    };
    let has_train_window_internal_prediction = parameters
        .get("prediction_set_override_source")
        .and_then(Value::as_str)
        .map(|source| source == "train_window_ml_internal")
        .unwrap_or(false);
    let confidence_gate_bonus = parameters
        .get("prediction_confidence_gate_profile")
        .and_then(Value::as_str)
        .filter(|profile| *profile == "train_positive_raw_score_gate_v1")
        .map(|_| Decimal::from(180_000))
        .unwrap_or(Decimal::ZERO);
    let internal_prediction_bonus = if has_train_window_internal_prediction {
        Decimal::from(120_000)
    } else {
        Decimal::ZERO
    };
    let min_score = decimal_from_json(parameters.get("prediction_min_score"));
    let min_score_bonus = min_score
        .map(|score| {
            clamp_decimal(score, Decimal::new(-5, 2), Decimal::new(5, 2)) * Decimal::from(3_000_000)
        })
        .unwrap_or(Decimal::ZERO);
    let min_score_absence_penalty = if min_score.is_some() {
        Decimal::ZERO
    } else {
        Decimal::from(150_000)
    };
    let percentile_bonus = decimal_from_json(parameters.get("prediction_min_percentile"))
        .map(|percentile| {
            clamp_decimal(percentile, Decimal::ZERO, Decimal::ONE) * Decimal::from(160_000)
        })
        .unwrap_or(Decimal::ZERO);

    confidence_gate_bonus + internal_prediction_bonus + min_score_bonus + percentile_bonus
        - min_score_absence_penalty
}

fn train_candidate_prediction_confidence_stress_fill_objective_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    train_candidate_stress_fill_objective_score(candidate, summary, train_gate_policy)
        + candidate_prediction_confidence_score(candidate)
}

fn prediction_confidence_stress_fill_quality_score_breakdown(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> PredictionConfidenceStressFillQualityScoreBreakdown {
    let stress_fill_objective_score =
        train_candidate_stress_fill_objective_score(candidate, summary, train_gate_policy);
    let prediction_confidence_score = candidate_prediction_confidence_score(candidate);
    let base_score = stress_fill_objective_score + prediction_confidence_score;
    let target_annual_return = constraint_decimal(
        Some(train_gate_policy),
        "capacity_stress_target_annual_return",
    )
    .unwrap_or_else(|| Decimal::new(15, 2));
    let min_annual_return =
        constraint_decimal(Some(train_gate_policy), "min_train_perturbed_annual_return")
            .unwrap_or_else(|| Decimal::new(5, 2));
    let min_calmar = constraint_decimal(Some(train_gate_policy), "min_train_perturbed_calmar")
        .unwrap_or_else(|| Decimal::new(12, 1));
    let min_sharpe = constraint_decimal(Some(train_gate_policy), "min_train_avg_perturbed_sharpe")
        .unwrap_or_else(|| Decimal::new(5, 1));
    let min_sortino = constraint_decimal(Some(train_gate_policy), "min_train_perturbed_sortino")
        .unwrap_or_else(|| Decimal::ONE);

    let pass_ratio_bonus = Decimal::from(summary.pass_ratio_ppm) * Decimal::from(2);
    let annual_quality_bonus = clamp_decimal(
        summary.min_annual_return,
        Decimal::from(-1),
        Decimal::from(1),
    ) * Decimal::from(360_000);
    let calmar_quality_bonus =
        clamp_decimal(summary.min_calmar, Decimal::from(-5), Decimal::from(5))
            * Decimal::from(220_000);
    let sharpe_quality_bonus =
        clamp_decimal(summary.avg_sharpe, Decimal::from(-5), Decimal::from(5))
            * Decimal::from(140_000);
    let sortino_quality_bonus =
        clamp_decimal(summary.min_sortino, Decimal::from(-5), Decimal::from(5))
            * Decimal::from(120_000);
    let drawdown_tail_penalty =
        clamp_decimal(summary.max_drawdown, Decimal::ZERO, Decimal::ONE) * Decimal::from(260_000);
    let min_annual_shortfall =
        positive_decimal_gap(min_annual_return, summary.min_annual_return) * Decimal::from(760_000);
    let target_annual_shortfall =
        positive_decimal_gap(target_annual_return, summary.min_annual_return)
            * Decimal::from(380_000);
    let calmar_shortfall =
        positive_decimal_gap(min_calmar, summary.min_calmar) * Decimal::from(340_000);
    let sharpe_shortfall =
        positive_decimal_gap(min_sharpe, summary.avg_sharpe) * Decimal::from(260_000);
    let sortino_shortfall =
        positive_decimal_gap(min_sortino, summary.min_sortino) * Decimal::from(220_000);
    let total_score = base_score
        + pass_ratio_bonus
        + annual_quality_bonus
        + calmar_quality_bonus
        + sharpe_quality_bonus
        + sortino_quality_bonus
        - drawdown_tail_penalty
        - min_annual_shortfall
        - target_annual_shortfall
        - calmar_shortfall
        - sharpe_shortfall
        - sortino_shortfall;

    PredictionConfidenceStressFillQualityScoreBreakdown {
        total_score,
        base_score,
        stress_fill_objective_score,
        prediction_confidence_score,
        target_annual_return,
        min_annual_return,
        min_calmar,
        min_sharpe,
        min_sortino,
        pass_ratio_bonus,
        annual_quality_bonus,
        calmar_quality_bonus,
        sharpe_quality_bonus,
        sortino_quality_bonus,
        drawdown_tail_penalty,
        min_annual_shortfall,
        target_annual_shortfall,
        calmar_shortfall,
        sharpe_shortfall,
        sortino_shortfall,
    }
}

fn train_candidate_prediction_confidence_stress_fill_quality_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    prediction_confidence_stress_fill_quality_score_breakdown(candidate, summary, train_gate_policy)
        .total_score
}

fn train_candidate_cash_drag_fill_gap_score(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    let base = train_candidate_capacity_stress_calmar_score(candidate, summary, train_gate_policy);
    let max_cash_limit =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
            .unwrap_or_else(|| Decimal::new(20, 2));
    let max_gap_limit = constraint_decimal(
        Some(train_gate_policy),
        "max_train_execution_target_gap_pct",
    )
    .unwrap_or_else(|| Decimal::new(8, 2));
    let max_unfilled_gap_limit = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    )
    .unwrap_or(max_gap_limit);
    let min_fill_ratio_limit = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    )
    .unwrap_or_else(|| Decimal::new(90, 2));
    let max_expired_count = constraint_i64(
        Some(train_gate_policy),
        "max_train_execution_schedule_expired_count",
    )
    .unwrap_or(0)
    .max(0) as usize;

    let max_cash_weight = candidate
        .metrics
        .final_cash_weight
        .max(summary.max_final_cash_weight);
    let max_target_gap = candidate
        .metrics
        .max_execution_target_gap
        .max(summary.max_execution_target_gap);
    let max_unfilled_target_gap = candidate
        .metrics
        .final_unfilled_target_gap
        .max(summary.max_final_unfilled_target_gap);
    let min_execution_fill_ratio = candidate
        .metrics
        .final_execution_fill_ratio
        .min(summary.min_final_execution_fill_ratio);
    let expired_count = (candidate.metrics.execution_schedule_expired_count as usize)
        .max(summary.max_execution_schedule_expired_count);
    let total_expired_count = (candidate.metrics.execution_schedule_expired_count as usize)
        + summary.total_execution_schedule_expired_count;
    let cash_shortfall_penalty =
        positive_decimal_gap(max_cash_weight, max_cash_limit) * Decimal::from(420_000);
    let avg_cash_penalty = positive_decimal_gap(summary.avg_final_cash_weight, max_cash_limit)
        * Decimal::from(180_000);
    let gap_shortfall_penalty =
        positive_decimal_gap(max_target_gap, max_gap_limit) * Decimal::from(520_000);
    let unfilled_gap_penalty =
        positive_decimal_gap(max_unfilled_target_gap, max_unfilled_gap_limit)
            * Decimal::from(620_000);
    let fill_ratio_shortfall_penalty =
        positive_decimal_gap(min_fill_ratio_limit, min_execution_fill_ratio)
            * Decimal::from(480_000);
    let expired_penalty = Decimal::from(expired_count.saturating_sub(max_expired_count) as i64)
        * Decimal::from(45_000)
        + Decimal::from(total_expired_count as i64) * Decimal::from(10_000);

    base - cash_shortfall_penalty
        - avg_cash_penalty
        - gap_shortfall_penalty
        - unfilled_gap_penalty
        - fill_ratio_shortfall_penalty
        - expired_penalty
}

fn train_candidate_stress_adjusted_score_for_policy(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Decimal {
    match train_cost_capacity_stress_score_profile(train_gate_policy) {
        "capacity_stress_calmar_score_v1" => {
            train_candidate_capacity_stress_calmar_score(candidate, summary, train_gate_policy)
        }
        "capacity_stress_return_score_v1" => {
            train_candidate_capacity_stress_return_score(candidate, summary, train_gate_policy)
        }
        "cash_drag_fill_gap_score_v1" => {
            train_candidate_cash_drag_fill_gap_score(candidate, summary, train_gate_policy)
        }
        "stress_fill_objective_score_v1" => {
            train_candidate_stress_fill_objective_score(candidate, summary, train_gate_policy)
        }
        "prediction_confidence_stress_fill_objective_score_v1" => {
            train_candidate_prediction_confidence_stress_fill_objective_score(
                candidate,
                summary,
                train_gate_policy,
            )
        }
        "prediction_confidence_stress_fill_quality_score_v1" => {
            train_candidate_prediction_confidence_stress_fill_quality_score(
                candidate,
                summary,
                train_gate_policy,
            )
        }
        _ => train_candidate_stress_adjusted_score(candidate, summary),
    }
}

fn train_candidate_stress_score_breakdown(
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> Value {
    let profile = train_cost_capacity_stress_score_profile(train_gate_policy);
    let total_score =
        train_candidate_stress_adjusted_score_for_policy(candidate, summary, train_gate_policy);
    if profile != "prediction_confidence_stress_fill_quality_score_v1" {
        return json!({
            "profile": profile,
            "total_score": total_score,
            "summary": cost_capacity_perturbation_summary_json(summary),
        });
    }

    let breakdown = prediction_confidence_stress_fill_quality_score_breakdown(
        candidate,
        summary,
        train_gate_policy,
    );

    json!({
        "profile": profile,
        "total_score": breakdown.total_score,
        "base_score": breakdown.base_score,
        "summary": cost_capacity_perturbation_summary_json(summary),
        "targets": {
            "target_annual_return_pct": breakdown.target_annual_return,
            "min_annual_return_pct": breakdown.min_annual_return,
            "min_calmar": breakdown.min_calmar,
            "min_avg_sharpe": breakdown.min_sharpe,
            "min_sortino": breakdown.min_sortino,
        },
        "components": {
            "stress_fill_objective_score": breakdown.stress_fill_objective_score,
            "prediction_confidence_score": breakdown.prediction_confidence_score,
            "pass_ratio_bonus": breakdown.pass_ratio_bonus,
            "annual_quality_bonus": breakdown.annual_quality_bonus,
            "calmar_quality_bonus": breakdown.calmar_quality_bonus,
            "sharpe_quality_bonus": breakdown.sharpe_quality_bonus,
            "sortino_quality_bonus": breakdown.sortino_quality_bonus,
        },
        "penalties": {
            "drawdown_tail_penalty": breakdown.drawdown_tail_penalty,
            "min_annual_shortfall": breakdown.min_annual_shortfall,
            "target_annual_shortfall": breakdown.target_annual_shortfall,
            "calmar_shortfall": breakdown.calmar_shortfall,
            "sharpe_shortfall": breakdown.sharpe_shortfall,
            "sortino_shortfall": breakdown.sortino_shortfall,
        },
    })
}

fn train_candidate_evaluation_order(
    left: &OosTrainCandidateEvaluation,
    right: &OosTrainCandidateEvaluation,
) -> std::cmp::Ordering {
    right
        .stress_summary
        .pass_ratio_ppm
        .cmp(&left.stress_summary.pass_ratio_ppm)
        .then_with(|| right.stress_adjusted_score.cmp(&left.stress_adjusted_score))
        .then_with(|| {
            right
                .stress_summary
                .min_calmar
                .cmp(&left.stress_summary.min_calmar)
        })
        .then_with(|| {
            right
                .stress_summary
                .avg_calmar
                .cmp(&left.stress_summary.avg_calmar)
        })
        .then_with(|| {
            left.stress_summary
                .max_drawdown
                .cmp(&right.stress_summary.max_drawdown)
        })
        .then_with(|| {
            right
                .stress_summary
                .min_annual_return
                .cmp(&left.stress_summary.min_annual_return)
        })
        .then_with(|| {
            right
                .stress_summary
                .avg_sharpe
                .cmp(&left.stress_summary.avg_sharpe)
        })
        .then_with(|| discovery_candidate_order(&left.candidate, &right.candidate))
}

fn attach_train_cost_capacity_gate_to_robustness(
    mut robustness: Value,
    passed_count: usize,
    total_count: usize,
    gate: &OosCostCapacityGateConfig,
) -> (Value, bool) {
    if !gate.enabled {
        return (robustness, true);
    }

    let gate_report = build_cost_capacity_pass_ratio_gate(
        "train_cost_capacity_perturbation_pass_ratio",
        passed_count,
        total_count,
        gate,
    );
    let passed = gate_report["passed"].as_bool().unwrap_or(false);
    if let Some(object) = robustness.as_object_mut() {
        match object.entry("gate_results".to_string()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(Value::Array(vec![gate_report.clone()]));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                if let Some(items) = entry.get_mut().as_array_mut() {
                    items.push(gate_report.clone());
                }
            }
        }
        object.insert("train_cost_capacity_gate".to_string(), gate_report);
        if !passed {
            object.insert("status".to_string(), json!("rejected"));
        }
    }
    (robustness, passed)
}

fn attach_train_capacity_stress_return_gates_to_robustness(
    mut robustness: Value,
    _candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> (Value, bool) {
    let mut gates = Vec::new();
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "min_train_perturbed_annual_return")
    {
        let actual = summary.min_annual_return;
        gates.push(json!({
            "gate": "train_perturbed_annual_return",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "min_train_avg_perturbed_calmar")
    {
        let actual = summary.avg_calmar;
        gates.push(json!({
            "gate": "train_avg_perturbed_calmar",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if gates.is_empty() {
        return (robustness, true);
    }

    let passed = gates
        .iter()
        .all(|gate| gate["passed"].as_bool().unwrap_or(false));
    if let Some(object) = robustness.as_object_mut() {
        match object.entry("gate_results".to_string()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(Value::Array(gates.clone()));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                if let Some(items) = entry.get_mut().as_array_mut() {
                    items.extend(gates.iter().cloned());
                }
            }
        }
        object.insert(
            "train_capacity_stress_return_gates".to_string(),
            Value::Array(gates),
        );
        if !passed {
            object.insert("status".to_string(), json!("rejected"));
        }
    }
    (robustness, passed)
}

fn attach_train_cash_drag_fill_gap_gates_to_robustness(
    mut robustness: Value,
    candidate: &DiscoveryCandidate,
    summary: &CostCapacityPerturbationSummary,
    train_gate_policy: &Value,
) -> (Value, bool) {
    let mut gates = Vec::new();
    if let Some(limit) = constraint_i64(Some(train_gate_policy), "min_train_trade_count") {
        let actual = candidate
            .metrics
            .num_trades
            .min(summary.min_num_trades)
            .min(i64::MAX as u64) as i64;
        gates.push(json!({
            "gate": "train_trade_count",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) =
        constraint_decimal(Some(train_gate_policy), "max_train_final_cash_weight_pct")
    {
        let actual = candidate
            .metrics
            .final_cash_weight
            .max(summary.max_final_cash_weight);
        gates.push(json!({
            "gate": "train_final_cash_weight",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_actual_gross_exposure_pct",
    ) {
        let actual = candidate
            .metrics
            .final_actual_gross_exposure
            .min(summary.min_final_actual_gross_exposure);
        gates.push(json!({
            "gate": "train_final_actual_gross_exposure",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "max_train_execution_target_gap_pct",
    ) {
        let actual = candidate
            .metrics
            .max_execution_target_gap
            .max(summary.max_execution_target_gap);
        gates.push(json!({
            "gate": "train_execution_target_gap",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "max_train_final_unfilled_target_gap_pct",
    ) {
        let actual = candidate
            .metrics
            .final_unfilled_target_gap
            .max(summary.max_final_unfilled_target_gap);
        gates.push(json!({
            "gate": "train_final_unfilled_target_gap",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_decimal(
        Some(train_gate_policy),
        "min_train_final_execution_fill_ratio",
    ) {
        let actual = candidate
            .metrics
            .final_execution_fill_ratio
            .min(summary.min_final_execution_fill_ratio);
        gates.push(json!({
            "gate": "train_final_execution_fill_ratio",
            "passed": actual >= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if let Some(limit) = constraint_i64(
        Some(train_gate_policy),
        "max_train_execution_schedule_expired_count",
    ) {
        let limit = limit.max(0) as usize;
        let actual = (candidate.metrics.execution_schedule_expired_count as usize)
            .max(summary.max_execution_schedule_expired_count);
        gates.push(json!({
            "gate": "train_execution_schedule_expired_count",
            "passed": actual <= limit,
            "limit": limit,
            "actual": actual,
        }));
    }
    if gates.is_empty() {
        return (robustness, true);
    }

    let passed = gates
        .iter()
        .all(|gate| gate["passed"].as_bool().unwrap_or(false));
    if let Some(object) = robustness.as_object_mut() {
        match object.entry("gate_results".to_string()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(Value::Array(gates.clone()));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                if let Some(items) = entry.get_mut().as_array_mut() {
                    items.extend(gates.iter().cloned());
                }
            }
        }
        object.insert(
            "train_cash_drag_fill_gap_gates".to_string(),
            Value::Array(gates),
        );
        if !passed {
            object.insert("status".to_string(), json!("rejected"));
        }
    }
    (robustness, passed)
}

fn train_cost_capacity_overlay_persistence_fields(
    robustness: &Value,
) -> Result<RobustnessOverlayPersistenceFields, String> {
    let gate_result_id = robustness["gate_result_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "train cost/capacity robustness overlay missing gate_result_id".to_string())?
        .to_string();
    let status = robustness["status"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "train cost/capacity robustness overlay missing status".to_string())?
        .to_string();
    let gate_results = robustness
        .get("gate_results")
        .cloned()
        .ok_or_else(|| "train cost/capacity robustness overlay missing gate_results".to_string())?;
    Ok(RobustnessOverlayPersistenceFields {
        gate_result_id,
        status,
        gate_results,
    })
}

async fn persist_train_cost_capacity_robustness_overlay(
    db: &sqlx::PgPool,
    robustness: &Value,
) -> Result<(), String> {
    let fields = train_cost_capacity_overlay_persistence_fields(robustness)?;
    sqlx::query(
        "UPDATE robustness_gate_result
         SET gate_results = $2,
             status = $3,
             summary = $4
         WHERE gate_result_id = $1",
    )
    .bind(&fields.gate_result_id)
    .bind(&fields.gate_results)
    .bind(&fields.status)
    .bind(format!(
        "OOS walk-forward train-window robustness gate evaluated as {}",
        fields.status
    ))
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to persist train cost/capacity robustness overlay: {}",
            error
        )
    })?;
    Ok(())
}

fn build_oos_discovery_plan(
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Result<OosDiscoveryPlan, String> {
    let template = req
        .backtest_template
        .clone()
        .unwrap_or_else(default_phase7_backtest_template);
    let start_date = parse_template_date(&template, "start_date", "20160201")?;
    let end_date = parse_template_date(&template, "end_date", "20260515")?;
    if start_date >= end_date {
        return Err("backtest_template.start_date must be before end_date".into());
    }
    normalize_oos_train_cache_mode(req.train_cache_mode.as_deref())?;
    let validation_mode = req
        .validation_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("walk_forward")
        .to_string();
    let train_window_days = req.train_window_days.unwrap_or(365 * 3).max(30);
    let test_window_days = req.test_window_days.unwrap_or(365).max(5);
    let step_days = req.step_days.unwrap_or(test_window_days).max(1);
    let in_sample_ratio = req.in_sample_ratio.unwrap_or(0.80).clamp(0.50, 0.95);
    let include_partial_last_window = req.include_partial_last_window.unwrap_or(false);
    let windows = match validation_mode.as_str() {
        "holdout" | "holdout_80_20" | "oos_holdout" => {
            build_holdout_oos_windows(start_date, end_date, in_sample_ratio)?
        }
        "walk_forward" | "rolling_walk_forward" | "wfa" => build_rolling_oos_windows(
            start_date,
            end_date,
            train_window_days,
            test_window_days,
            step_days,
            include_partial_last_window,
        )?,
        other => return Err(format!("unsupported validation_mode: {}", other)),
    };
    if windows.is_empty() {
        return Err("OOS discovery plan has no valid train/test windows".into());
    }
    Ok(OosDiscoveryPlan {
        validation_mode,
        start_date,
        end_date,
        train_window_days,
        test_window_days,
        step_days,
        in_sample_ratio,
        include_partial_last_window,
        windows,
    })
}

fn default_phase7_oos_comparison_profiles() -> Vec<String> {
    vec![
        "phase7_ec".to_string(),
        "phase7_eu".to_string(),
        "phase7_ev".to_string(),
    ]
}

const RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT: &str =
    "/api/v1/quant/experiments/return-risk-cache-economics/report";

fn return_risk_cache_comparison_enabled(value: Option<bool>) -> bool {
    value.unwrap_or(false)
}

fn normalize_profile_comparison_profiles(
    profiles: Option<Vec<String>>,
) -> Result<Vec<String>, String> {
    let profiles = profiles.unwrap_or_else(default_phase7_oos_comparison_profiles);
    if profiles.is_empty() {
        return Err("profile comparison requires at least one search_profile".to_string());
    }
    profiles
        .into_iter()
        .map(|profile| {
            let profile = profile.trim().to_string();
            if profile.is_empty() {
                Err("profile comparison search_profile must not be empty".to_string())
            } else {
                Ok(profile)
            }
        })
        .collect()
}

fn profile_comparison_return_risk_cache_modes(enabled: bool) -> Vec<Option<&'static str>> {
    if enabled {
        vec![Some("raw_matrix"), Some("stats_matrix_experimental")]
    } else {
        vec![None]
    }
}

fn normalize_return_risk_feature_cache_mode(value: &str) -> Result<&'static str, String> {
    match value.trim() {
        "raw_matrix" | "raw-matrix" => Ok("raw_matrix"),
        "stats_matrix_experimental" | "stats-matrix-experimental" => {
            Ok("stats_matrix_experimental")
        }
        other => Err(format!(
            "return_risk_feature_cache_mode must be raw_matrix or stats_matrix_experimental, got {}",
            other
        )),
    }
}

fn set_request_return_risk_feature_cache_mode(
    req: &mut Phase7OosWalkForwardDiscoveryRequest,
    mode: &str,
) -> Result<(), String> {
    let mode = normalize_return_risk_feature_cache_mode(mode)?;
    let mut template = req
        .backtest_template
        .clone()
        .unwrap_or_else(default_phase7_backtest_template);
    let object = template
        .as_object_mut()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    object.insert("return_risk_feature_cache_mode".to_string(), json!(mode));
    req.backtest_template = Some(template);
    Ok(())
}

fn request_return_risk_feature_cache_mode(
    req: &Phase7OosWalkForwardDiscoveryRequest,
) -> Option<String> {
    req.backtest_template
        .as_ref()
        .and_then(|template| template.get("return_risk_feature_cache_mode"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn profile_comparison_request(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    search_profile: &str,
) -> Phase7OosWalkForwardDiscoveryRequest {
    let mut req = base.clone();
    req.search_profile = Some(search_profile.to_string());
    req.plan_only = Some(true);
    req.execution_mode = Some("inline".to_string());
    req.train_cache_mode = Some(
        req.train_cache_mode
            .filter(|mode| !mode.trim().is_empty())
            .unwrap_or_else(|| "auto".to_string()),
    );
    req
}

fn profile_comparison_request_for_cache_mode(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    search_profile: &str,
    cache_mode: Option<&str>,
) -> Result<Phase7OosWalkForwardDiscoveryRequest, String> {
    let mut req = profile_comparison_request(base, search_profile);
    if let Some(cache_mode) = cache_mode {
        set_request_return_risk_feature_cache_mode(&mut req, cache_mode)?;
    }
    Ok(req)
}

fn oos_request_plan_json(req: &Phase7OosWalkForwardDiscoveryRequest) -> Value {
    json!({
        "strategy_version_id": req.strategy_version_id,
        "data_version_id": req.data_version_id,
        "search_profile": req.search_profile,
        "max_trials_per_window": req.max_trials_per_window,
        "trial_batch_limit": req.trial_batch_limit,
        "trial_concurrency": req.trial_concurrency,
        "train_cache_mode": req.train_cache_mode,
        "max_batches_per_window": req.max_batches_per_window,
        "train_window_days": req.train_window_days,
        "test_window_days": req.test_window_days,
        "step_days": req.step_days,
        "validation_mode": req.validation_mode,
        "include_partial_last_window": req.include_partial_last_window,
        "plan_only": req.plan_only,
        "execution_mode": req.execution_mode,
        "oos_top_n": req.oos_top_n,
        "enable_cost_capacity_perturbation_gate": req.enable_cost_capacity_perturbation_gate,
        "return_risk_feature_cache_mode": request_return_risk_feature_cache_mode(req),
    })
}

fn build_phase7_oos_profile_comparison_plan(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    profiles: Option<Vec<String>>,
    return_risk_cache_comparison: Option<bool>,
) -> Result<Value, String> {
    let profiles = normalize_profile_comparison_profiles(profiles)?;
    let return_risk_cache_comparison =
        return_risk_cache_comparison_enabled(return_risk_cache_comparison);
    let cache_modes = profile_comparison_return_risk_cache_modes(return_risk_cache_comparison);

    let mut profile_plans = Vec::new();
    for profile in &profiles {
        for cache_mode in &cache_modes {
            let req = profile_comparison_request_for_cache_mode(base, profile, *cache_mode)?;
            let plan = build_oos_discovery_plan(&req)?;
            let plan_json = oos_discovery_plan_json(&plan);
            let config = oos_walk_forward_experiment_config(&req, &plan_json);
            let return_risk_feature_cache_mode = request_return_risk_feature_cache_mode(&req);
            profile_plans.push(json!({
                "search_profile": profile,
                "return_risk_feature_cache_mode": return_risk_feature_cache_mode,
                "cache_pair_key": if return_risk_cache_comparison {
                    json!(format!("{}:return_risk_cache", profile))
                } else {
                    Value::Null
                },
                "request": oos_request_plan_json(&req),
                "plan": plan_json,
                "config": config,
            }));
        }
    }

    Ok(json!({
        "search_method": "phase7_oos_profile_comparison_plan",
        "profile_count": profiles.len(),
        "launch_count": profile_plans.len(),
        "return_risk_cache_comparison": return_risk_cache_comparison_plan(
            return_risk_cache_comparison,
            profiles.len()
        ),
        "profiles": profile_plans,
    }))
}

fn bounded_profile_comparison_smoke_request(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    search_profile: &str,
) -> Result<Phase7OosWalkForwardDiscoveryRequest, String> {
    let mut req = profile_comparison_request(base, search_profile);
    req.plan_only = Some(false);
    req.execution_mode = Some("background".to_string());
    req.train_cache_mode = Some(
        req.train_cache_mode
            .filter(|mode| !mode.trim().is_empty())
            .unwrap_or_else(|| "auto".to_string()),
    );
    req.max_trials_per_window = Some(req.max_trials_per_window.unwrap_or(8).clamp(1, 8));
    req.trial_batch_limit = Some(req.trial_batch_limit.unwrap_or(8).clamp(1, 8));
    req.max_batches_per_window = Some(req.max_batches_per_window.unwrap_or(1).clamp(1, 1));
    req.oos_top_n = Some(req.oos_top_n.unwrap_or(3).clamp(1, 3));
    Ok(req)
}

fn build_phase7_oos_profile_comparison_launch_requests(
    base: &Phase7OosWalkForwardDiscoveryRequest,
    profiles: Option<Vec<String>>,
    return_risk_cache_comparison: Option<bool>,
) -> Result<Vec<Phase7OosWalkForwardDiscoveryRequest>, String> {
    let profiles = normalize_profile_comparison_profiles(profiles)
        .map_err(|message| message.replace("requires", "launch requires"))?;
    let return_risk_cache_comparison =
        return_risk_cache_comparison_enabled(return_risk_cache_comparison);
    let cache_modes = profile_comparison_return_risk_cache_modes(return_risk_cache_comparison);

    let mut requests = Vec::new();
    for profile in &profiles {
        for cache_mode in &cache_modes {
            let mut req = bounded_profile_comparison_smoke_request(base, profile)?;
            if let Some(cache_mode) = cache_mode {
                set_request_return_risk_feature_cache_mode(&mut req, cache_mode)?;
            }
            requests.push(req);
        }
    }
    Ok(requests)
}

fn return_risk_cache_comparison_plan(enabled: bool, pair_count: usize) -> Value {
    json!({
        "enabled": enabled,
        "pair_count": if enabled { pair_count } else { 0 },
        "modes": if enabled {
            json!(["raw_matrix", "stats_matrix_experimental"])
        } else {
            json!([])
        },
        "report_endpoint": if enabled {
            json!(RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT)
        } else {
            Value::Null
        },
        "pairing_rule": if enabled {
            json!("same search_profile, windows, gates, search budget, and training data; only backtest_template.return_risk_feature_cache_mode differs")
        } else {
            Value::Null
        },
    })
}

fn cache_economics_report_launch_plan(enabled: bool, launches: &[Value]) -> Value {
    if !enabled {
        return return_risk_cache_comparison_plan(false, 0);
    }

    let mut pairs: BTreeMap<String, (Option<String>, Option<String>)> = BTreeMap::new();
    for launch in launches {
        let Some(profile) = launch.get("search_profile").and_then(Value::as_str) else {
            continue;
        };
        let Some(mode) = launch
            .get("return_risk_feature_cache_mode")
            .and_then(Value::as_str)
        else {
            continue;
        };
        let Some(experiment_run_id) = launch.get("experiment_run_id").and_then(Value::as_str)
        else {
            continue;
        };
        let entry = pairs.entry(profile.to_string()).or_default();
        match mode {
            "raw_matrix" => entry.0 = Some(experiment_run_id.to_string()),
            "stats_matrix_experimental" => entry.1 = Some(experiment_run_id.to_string()),
            _ => {}
        }
    }

    let report_pairs = pairs
        .into_iter()
        .map(|(profile, (raw_id, stats_id))| {
            json!({
                "search_profile": profile,
                "raw_experiment_run_id": raw_id,
                "stats_experiment_run_id": stats_id,
                "ready_when": "both experiments are completed",
                "report_endpoint": RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT,
                "report_request": {
                    "raw_experiment_run_id": raw_id,
                    "stats_experiment_run_id": stats_id,
                }
            })
        })
        .collect::<Vec<_>>();

    json!({
        "enabled": true,
        "pair_count": report_pairs.len(),
        "modes": ["raw_matrix", "stats_matrix_experimental"],
        "report_endpoint": RETURN_RISK_CACHE_ECONOMICS_REPORT_ENDPOINT,
        "ready_when": "call report endpoint only after each raw/stats pair reaches completed status",
        "pairs": report_pairs,
    })
}

fn build_holdout_oos_windows(
    start_date: NaiveDate,
    end_date: NaiveDate,
    in_sample_ratio: f64,
) -> Result<Vec<OosDiscoveryWindow>, String> {
    let total_days = (end_date - start_date).num_days();
    if total_days < 30 {
        return Err("holdout OOS requires at least 30 calendar days".into());
    }
    let train_days = ((total_days as f64) * in_sample_ratio).round() as i64;
    let train_end = start_date + Duration::days(train_days.max(1));
    let test_start = train_end + Duration::days(1);
    if test_start > end_date {
        return Err("holdout split leaves no OOS test period".into());
    }
    Ok(vec![OosDiscoveryWindow {
        window_index: 1,
        validation_mode: "holdout_80_20".to_string(),
        train_start: start_date,
        train_end,
        test_start,
        test_end: end_date,
    }])
}

fn build_rolling_oos_windows(
    start_date: NaiveDate,
    end_date: NaiveDate,
    train_window_days: i64,
    test_window_days: i64,
    step_days: i64,
    include_partial_last_window: bool,
) -> Result<Vec<OosDiscoveryWindow>, String> {
    if train_window_days <= 0 || test_window_days <= 0 || step_days <= 0 {
        return Err("walk-forward train/test/step days must be positive".into());
    }
    let mut windows = Vec::new();
    let mut train_start = start_date;
    loop {
        let train_end = train_start + Duration::days(train_window_days - 1);
        let test_start = train_end + Duration::days(1);
        let mut test_end = test_start + Duration::days(test_window_days - 1);
        if test_start > end_date {
            break;
        }
        if test_end > end_date {
            if include_partial_last_window {
                test_end = end_date;
            } else {
                break;
            }
        }
        windows.push(OosDiscoveryWindow {
            window_index: windows.len() + 1,
            validation_mode: "walk_forward".to_string(),
            train_start,
            train_end,
            test_start,
            test_end,
        });
        train_start += Duration::days(step_days);
    }
    Ok(windows)
}

fn parse_template_date(template: &Value, key: &str, default: &str) -> Result<NaiveDate, String> {
    let value = template
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .trim();
    parse_oos_yyyymmdd(value, key)
}

fn parse_oos_yyyymmdd(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y%m%d").map_err(|_| format!("{} must be YYYYMMDD", field))
}

fn format_oos_yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn backtest_template_for_window(
    mut template: Value,
    start_date: NaiveDate,
    end_date: NaiveDate,
    persistence_mode: &str,
) -> Result<Value, String> {
    if start_date > end_date {
        return Err("OOS window start_date must be <= end_date".into());
    }
    let object = template
        .as_object_mut()
        .ok_or_else(|| "backtest_template must be a JSON object".to_string())?;
    object.insert(
        "start_date".to_string(),
        json!(format_oos_yyyymmdd(start_date)),
    );
    object.insert("end_date".to_string(), json!(format_oos_yyyymmdd(end_date)));
    object
        .entry("mode".to_string())
        .or_insert_with(|| json!("standard"));
    object.insert(
        "persistence_mode".to_string(),
        json!(persistence_mode.to_string()),
    );
    object
        .entry("effective_coverage".to_string())
        .or_insert_with(|| json!({"enabled": true, "mode": "adjust_start"}));
    Ok(template)
}

async fn load_oos_training_candidates(
    db: &sqlx::PgPool,
    task_id: &str,
    limit: usize,
) -> Result<Vec<DiscoveryCandidate>, String> {
    let targets = CandidateTargets::default();
    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<Decimal>,
            Option<Value>,
            Option<String>,
            Value,
        ),
    >(
        "SELECT trial_id, score, metrics, backtest_task_id, parameters
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND status = 'completed' AND metrics IS NOT NULL",
    )
    .bind(task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load OOS training candidates: {}", error))?;

    let mut candidates = rows
        .into_iter()
        .filter_map(|(trial_id, score, metrics, backtest_task_id, parameters)| {
            let metrics_json = metrics?;
            let candidate_metrics = CandidateMetrics::from_optimization_metrics(&metrics_json);
            let candidate_type = targets.classify(&candidate_metrics);
            let professional_gap_score =
                professional_candidate_gap_score(&candidate_metrics, &targets);
            Some(DiscoveryCandidate {
                trial_id,
                backtest_task_id,
                score,
                candidate_type,
                professional_gap_score,
                metrics: candidate_metrics,
                parameters,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(discovery_candidate_order);
    candidates.truncate(limit.max(1));
    Ok(candidates)
}

async fn select_oos_training_candidate(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    train_template: &Value,
    task_id: &str,
    candidates: Vec<DiscoveryCandidate>,
    gate_policy: &Value,
    require_train_approval: bool,
    window_index: usize,
    train_signal_cache: &mut SignalDataCache,
    train_backtest_cache: &mut BacktestDataCache,
) -> Result<
    (
        DiscoveryCandidate,
        Option<Value>,
        Vec<OosCostCapacityPerturbationResult>,
    ),
    String,
> {
    if candidates.is_empty() {
        return Err(format!(
            "window {} has no completed training candidates",
            window_index
        ));
    }

    let mut first_rejected: Option<(DiscoveryCandidate, Value)> = None;
    let train_cost_gate = train_cost_capacity_perturbation_gate_config(req, gate_policy);
    let stress_aware_selection =
        train_cost_capacity_stress_aware_selection_enabled(gate_policy, &train_cost_gate);
    let mut evaluated_candidates = Vec::new();
    for candidate in candidates {
        let robustness = evaluate_and_persist_robustness_for_trial(
            db,
            task_id,
            &candidate.trial_id,
            Some(gate_policy),
            "OOS walk-forward train-window robustness gate evaluated",
        )
        .await?;
        if !require_train_approval || robustness_result_is_approved(&robustness) {
            let train_cost_capacity_perturbations = execute_train_cost_capacity_perturbations(
                db,
                req,
                train_template,
                &candidate.parameters,
                gate_policy,
                train_signal_cache,
                train_backtest_cache,
            )
            .await?;
            let (passed_count, total_count) =
                cost_capacity_perturbation_pass_counts(&train_cost_capacity_perturbations);
            let stress_summary =
                cost_capacity_perturbation_summary(&train_cost_capacity_perturbations);
            let (robustness, train_cost_gate_passed) =
                attach_train_cost_capacity_gate_to_robustness(
                    robustness,
                    passed_count,
                    total_count,
                    &train_cost_gate,
                );
            let (robustness, fill_gap_gate_passed) =
                attach_train_cash_drag_fill_gap_gates_to_robustness(
                    robustness,
                    &candidate,
                    &stress_summary,
                    gate_policy,
                );
            let (robustness, stress_return_gate_passed) =
                attach_train_capacity_stress_return_gates_to_robustness(
                    robustness,
                    &candidate,
                    &stress_summary,
                    gate_policy,
                );
            let train_cost_gate_passed =
                train_cost_gate_passed && fill_gap_gate_passed && stress_return_gate_passed;
            if train_cost_gate.enabled {
                persist_train_cost_capacity_robustness_overlay(db, &robustness).await?;
            }
            if !stress_aware_selection && train_cost_gate_passed {
                return Ok((
                    candidate,
                    Some(robustness),
                    train_cost_capacity_perturbations,
                ));
            }
            let stress_adjusted_score = train_candidate_stress_adjusted_score_for_policy(
                &candidate,
                &stress_summary,
                gate_policy,
            );
            evaluated_candidates.push(OosTrainCandidateEvaluation {
                candidate: candidate.clone(),
                robustness: robustness.clone(),
                train_cost_capacity_perturbations,
                stress_summary,
                stress_adjusted_score,
                train_cost_gate_passed,
            });
            if first_rejected.is_none() {
                first_rejected = Some((candidate, robustness));
            }
            continue;
        }
        if first_rejected.is_none() {
            first_rejected = Some((candidate, robustness));
        }
    }

    if stress_aware_selection {
        evaluated_candidates.sort_by(train_candidate_evaluation_order);
        if let Some(index) = evaluated_candidates
            .iter()
            .position(|evaluation| evaluation.train_cost_gate_passed)
        {
            let evaluation = evaluated_candidates.remove(index);
            return Ok((
                evaluation.candidate,
                Some(evaluation.robustness),
                evaluation.train_cost_capacity_perturbations,
            ));
        }
        if best_effort_train_selection_for_diagnostics_enabled(gate_policy)
            && !evaluated_candidates.is_empty()
        {
            let evaluation = evaluated_candidates.remove(0);
            return Ok((
                evaluation.candidate,
                Some(evaluation.robustness),
                evaluation.train_cost_capacity_perturbations,
            ));
        }
        if let Some(evaluation) = evaluated_candidates.first() {
            return Err(format!(
                "window {} has no training candidate passing robustness; best stress-aware rejected trial {} status {} train cost/capacity pass ratio {}/{} stress_adjusted_score {}",
                window_index,
                evaluation.candidate.trial_id,
                evaluation
                    .robustness
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                evaluation.stress_summary.passed_count,
                evaluation.stress_summary.total_count,
                evaluation.stress_adjusted_score,
            ));
        }
    }

    if let Some((candidate, robustness)) = first_rejected {
        if best_effort_train_selection_for_diagnostics_enabled(gate_policy) {
            return Ok((candidate, Some(robustness), Vec::new()));
        }
        return Err(format!(
            "window {} has no training candidate passing robustness; best rejected trial {} status {}",
            window_index,
            candidate.trial_id,
            robustness
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ));
    }

    Err(format!(
        "window {} has no training candidate passing robustness",
        window_index
    ))
}

async fn load_oos_equity_points(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
) -> Result<Vec<RobustnessDailyPoint>, String> {
    sqlx::query_as::<_, (NaiveDate, f64, Option<f64>)>(
        "SELECT trade_date,
                portfolio_value::double precision,
                benchmark_value::double precision
         FROM backtest_equity_curve
         WHERE task_id = $1
         ORDER BY trade_date",
    )
    .bind(backtest_task_id)
    .fetch_all(db)
    .await
    .map_err(|error| format!("Failed to load OOS equity curve: {}", error))
    .map(|rows| {
        rows.into_iter()
            .map(
                |(trade_date, portfolio_value, benchmark_value)| RobustnessDailyPoint {
                    trade_date,
                    portfolio_value,
                    benchmark_value,
                },
            )
            .collect()
    })
}

fn append_stitched_oos_points(
    stitched: &mut Vec<RobustnessDailyPoint>,
    window_points: &[RobustnessDailyPoint],
) {
    if window_points.len() < 2 {
        return;
    }
    if stitched.is_empty() {
        stitched.push(RobustnessDailyPoint {
            trade_date: window_points[0].trade_date,
            portfolio_value: 1.0,
            benchmark_value: Some(1.0),
        });
    }
    let mut portfolio_nav = stitched
        .last()
        .map(|point| point.portfolio_value)
        .unwrap_or(1.0);
    let mut benchmark_nav = stitched
        .last()
        .and_then(|point| point.benchmark_value)
        .unwrap_or(1.0);
    let mut last_date = stitched.last().map(|point| point.trade_date);
    for pair in window_points.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];
        if last_date
            .map(|date| next.trade_date <= date)
            .unwrap_or(false)
        {
            continue;
        }
        if prev.portfolio_value > 0.0 && next.portfolio_value.is_finite() {
            portfolio_nav *= next.portfolio_value / prev.portfolio_value;
        }
        let benchmark_value = match (prev.benchmark_value, next.benchmark_value) {
            (Some(prev_benchmark), Some(next_benchmark))
                if prev_benchmark > 0.0 && next_benchmark.is_finite() =>
            {
                benchmark_nav *= next_benchmark / prev_benchmark;
                Some(benchmark_nav)
            }
            _ => None,
        };
        stitched.push(RobustnessDailyPoint {
            trade_date: next.trade_date,
            portfolio_value: portfolio_nav,
            benchmark_value,
        });
        last_date = Some(next.trade_date);
    }
}

fn build_oos_gate_report(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &OosDiscoveryPlan,
    window_results: &[Value],
    stitched_summary: &Value,
    final_promotion_gate_policy: &Value,
) -> Value {
    let min_calmar =
        constraint_f64(Some(final_promotion_gate_policy), "min_stitched_oos_calmar").unwrap_or(1.2);
    let min_positive_ratio = constraint_f64(
        Some(final_promotion_gate_policy),
        "min_positive_oos_window_ratio",
    )
    .unwrap_or(0.60);
    let min_window_count =
        constraint_i64(Some(final_promotion_gate_policy), "min_oos_window_count")
            .map(|value| value.max(0) as usize)
            .unwrap_or_else(|| {
                if plan.validation_mode == "holdout_80_20" {
                    1
                } else {
                    3
                }
            });
    let require_train_selection = final_promotion_gate_policy
        .get("require_train_selection_approval")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let require_no_overlap = final_promotion_gate_policy
        .get("require_no_train_test_overlap")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let positive_windows = window_results
        .iter()
        .filter(|window| {
            value_as_f64(&window["oos_metrics"]["annual_return_pct"])
                .map(|value| value > 0.0)
                .unwrap_or(false)
        })
        .count();
    let positive_ratio = ratio(positive_windows, window_results.len());
    let stitched_calmar = stitched_summary["calmar_ratio"].as_f64().unwrap_or(0.0);
    let no_overlap = plan
        .windows
        .iter()
        .all(|window| window.train_end < window.test_start);
    let train_robustness_passed = window_results.iter().all(|window| {
        window["train_robustness"]["status"]
            .as_str()
            .map(|status| status == "approved_candidate")
            .unwrap_or(false)
    });
    let mut gates = vec![
        {
            json!({
                "gate": "oos_window_count",
                "passed": window_results.len() >= min_window_count,
                "limit": min_window_count,
                "actual": window_results.len(),
            })
        },
        {
            json!({
                "gate": "no_train_test_overlap",
                "passed": !require_no_overlap || no_overlap,
                "actual": no_overlap,
                "required": require_no_overlap,
            })
        },
        {
            json!({
                "gate": "train_window_robustness_approved",
                "passed": !require_train_selection || train_robustness_passed,
                "actual": train_robustness_passed,
                "required": require_train_selection,
            })
        },
        {
            json!({
                "gate": "stitched_oos_calmar",
                "passed": stitched_calmar > min_calmar,
                "limit": min_calmar,
                "actual": stitched_calmar,
            })
        },
        {
            json!({
                "gate": "positive_oos_window_ratio",
                "passed": positive_ratio >= min_positive_ratio,
                "limit": min_positive_ratio,
                "actual": positive_ratio,
            })
        },
    ];
    if cost_capacity_perturbation_gate_enabled(req) {
        let perturbation_results = window_results
            .iter()
            .filter_map(|window| window["cost_capacity_perturbations"].as_array())
            .flat_map(|items| items.iter())
            .collect::<Vec<_>>();
        let perturbation_count = perturbation_results.len();
        let passed_count = perturbation_results
            .iter()
            .filter(|result| result["passed"].as_bool().unwrap_or(false))
            .count();
        let pass_ratio = ratio(passed_count, perturbation_count);
        let min_pass_ratio = req
            .min_cost_capacity_perturbation_pass_ratio
            .or_else(|| {
                constraint_f64(
                    Some(final_promotion_gate_policy),
                    "min_cost_capacity_perturbation_pass_ratio",
                )
            })
            .unwrap_or(0.80)
            .clamp(0.0, 1.0);
        let min_perturbed_oos_calmar = req
            .min_perturbed_oos_calmar
            .or_else(|| {
                constraint_f64(
                    Some(final_promotion_gate_policy),
                    "min_perturbed_oos_calmar",
                )
            })
            .unwrap_or(1.2);
        let max_perturbed_oos_drawdown_pct = req
            .max_perturbed_oos_drawdown_pct
            .or_else(|| {
                constraint_f64(
                    Some(final_promotion_gate_policy),
                    "max_perturbed_oos_drawdown_pct",
                )
            })
            .unwrap_or(0.35);
        gates.push(json!({
            "gate": "cost_capacity_perturbation_pass_ratio",
            "passed": perturbation_count > 0 && pass_ratio >= min_pass_ratio,
            "limit": min_pass_ratio,
            "actual": pass_ratio,
            "passed_count": passed_count,
            "total_count": perturbation_count,
            "min_perturbed_oos_calmar": min_perturbed_oos_calmar,
            "max_perturbed_oos_drawdown_pct": max_perturbed_oos_drawdown_pct,
        }));
    }
    Value::Array(gates)
}

fn oos_gate_status(gates: &Value) -> &'static str {
    let passed = gates
        .as_array()
        .map(|items| {
            items
                .iter()
                .all(|item| item["passed"].as_bool().unwrap_or(false))
        })
        .unwrap_or(false);
    if passed {
        "approved_oos_candidate"
    } else {
        "rejected"
    }
}

fn oos_discovery_plan_json(plan: &OosDiscoveryPlan) -> Value {
    json!({
        "validation_mode": plan.validation_mode,
        "start_date": plan.start_date,
        "end_date": plan.end_date,
        "train_window_days": plan.train_window_days,
        "test_window_days": plan.test_window_days,
        "step_days": plan.step_days,
        "in_sample_ratio": plan.in_sample_ratio,
        "include_partial_last_window": plan.include_partial_last_window,
        "window_count": plan.windows.len(),
        "windows": plan.windows.iter().map(oos_window_json).collect::<Vec<_>>(),
        "point_in_time_contract": {
            "rule": "available_at <= signal_date/cutoff",
            "train_selection_scope": "train window only",
            "test_evaluation_scope": "selected train-window parameters only",
            "execution_timing": "next_open by default",
        },
    })
}

fn oos_window_json(window: &OosDiscoveryWindow) -> Value {
    json!({
        "window_index": window.window_index,
        "validation_mode": window.validation_mode,
        "train_start": window.train_start,
        "train_end": window.train_end,
        "test_start": window.test_start,
        "test_end": window.test_end,
    })
}

fn oos_window_execution_json(execution: &OosWindowExecution, train_gate_policy: &Value) -> Value {
    let train_cost_capacity_summary =
        cost_capacity_perturbation_summary(&execution.train_cost_capacity_perturbations);
    oos_window_execution_json_with_train_summary(
        execution,
        train_gate_policy,
        &train_cost_capacity_summary,
    )
}

fn oos_window_execution_json_with_train_summary(
    execution: &OosWindowExecution,
    train_gate_policy: &Value,
    train_cost_capacity_summary: &CostCapacityPerturbationSummary,
) -> Value {
    let train_stress_score_profile = train_cost_capacity_stress_score_profile(train_gate_policy);
    let train_stress_adjusted_score = train_candidate_stress_adjusted_score_for_policy(
        &execution.selected_candidate,
        train_cost_capacity_summary,
        train_gate_policy,
    );
    let train_stress_score_breakdown = train_candidate_stress_score_breakdown(
        &execution.selected_candidate,
        train_cost_capacity_summary,
        train_gate_policy,
    );
    json!({
        "window": oos_window_json(&execution.window),
        "train_optimization_task_id": execution.train_optimization_task_id,
        "train_execution_policy": {
            "requested_cache_mode": execution.train_execution_policy.requested_cache_mode,
            "cache_mode": execution.train_execution_policy.cache_mode,
            "requested_trial_concurrency": execution.train_execution_policy.requested_trial_concurrency,
            "trial_concurrency": execution.train_execution_policy.trial_concurrency,
        },
        "train_batches": execution.train_batches,
        "selected_candidate": discovery_candidate_json(&execution.selected_candidate),
        "train_robustness": execution.train_robustness,
        "train_cost_capacity_perturbations": execution.train_cost_capacity_perturbations.iter().map(oos_cost_capacity_perturbation_result_json).collect::<Vec<_>>(),
        "train_cost_capacity_summary": cost_capacity_perturbation_summary_json(train_cost_capacity_summary),
        "train_stress_score_profile": train_stress_score_profile,
        "train_stress_adjusted_score": train_stress_adjusted_score,
        "train_stress_score_breakdown": train_stress_score_breakdown,
        "oos_backtest_task_id": execution.oos_backtest_task_id,
        "oos_market_data_prewarm_report": execution.oos_output.market_data_prewarm_report,
        "oos_market_feature_prewarm_report": execution.oos_output.market_feature_prewarm_report,
        "oos_metrics": {
            "annual_return_pct": execution.oos_output.metrics.annual_return_pct,
            "excess_return_pct": execution.oos_output.metrics.excess_return_pct,
            "sharpe_ratio": execution.oos_output.metrics.sharpe_ratio,
            "sortino_ratio": execution.oos_output.metrics.sortino_ratio,
            "calmar_ratio": execution.oos_output.metrics.calmar_ratio,
            "max_drawdown_pct": execution.oos_output.metrics.max_drawdown_pct,
            "max_drawdown_duration_days": execution.oos_output.metrics.max_drawdown_duration_days,
            "profit_factor": execution.oos_output.metrics.profit_factor,
            "num_trades": execution.oos_output.metrics.num_trades,
            "execution_schedule_expired_count": execution.oos_output.metrics.execution_schedule_expired_count,
            "execution_schedule_roll_forward_count": execution.oos_output.metrics.execution_schedule_roll_forward_count,
            "max_execution_target_gap_pct": execution.oos_output.metrics.max_execution_target_gap_pct,
            "final_cash_weight_pct": execution.oos_output.metrics.final_cash_weight_pct,
            "final_target_gross_exposure_pct": execution.oos_output.metrics.final_target_gross_exposure_pct,
            "final_actual_gross_exposure_pct": execution.oos_output.metrics.final_actual_gross_exposure_pct,
            "final_unfilled_target_gap_pct": execution.oos_output.metrics.final_unfilled_target_gap_pct,
            "final_execution_fill_ratio": execution.oos_output.metrics.final_execution_fill_ratio,
        },
        "oos_point_count": execution.oos_points.len(),
        "cost_capacity_perturbations": execution.cost_capacity_perturbations.iter().map(oos_cost_capacity_perturbation_result_json).collect::<Vec<_>>(),
    })
}

fn annotate_oos_train_batch_cache_mode(
    mut batch: Value,
    train_execution_policy: &OosTrainExecutionPolicy,
) -> Value {
    let cache_scope = match train_execution_policy.cache_mode {
        "shared_run" => "run",
        "shared_window" => "window",
        "per_trial_isolated" => "trial",
        other => other,
    };
    if let Some(object) = batch.as_object_mut() {
        object.insert(
            "requested_cache_mode".to_string(),
            json!(train_execution_policy.requested_cache_mode),
        );
        object.insert(
            "cache_mode".to_string(),
            json!(train_execution_policy.cache_mode),
        );
        object.insert("cache_scope".to_string(), json!(cache_scope));
        object.insert(
            "requested_trial_concurrency".to_string(),
            json!(train_execution_policy.requested_trial_concurrency),
        );
        object.insert(
            "trial_concurrency".to_string(),
            json!(train_execution_policy.trial_concurrency),
        );
    }
    batch
}

fn cost_capacity_perturbation_summary_json(summary: &CostCapacityPerturbationSummary) -> Value {
    json!({
        "passed_count": summary.passed_count,
        "total_count": summary.total_count,
        "pass_ratio": ratio(summary.passed_count, summary.total_count),
        "min_calmar": summary.min_calmar,
        "avg_calmar": summary.avg_calmar,
        "max_drawdown_pct": summary.max_drawdown,
        "min_annual_return_pct": summary.min_annual_return,
        "avg_sharpe": summary.avg_sharpe,
        "min_sortino": summary.min_sortino,
        "max_final_cash_weight_pct": summary.max_final_cash_weight,
        "avg_final_cash_weight_pct": summary.avg_final_cash_weight,
        "max_final_unfilled_target_gap_pct": summary.max_final_unfilled_target_gap,
        "avg_final_unfilled_target_gap_pct": summary.avg_final_unfilled_target_gap,
        "min_final_execution_fill_ratio": summary.min_final_execution_fill_ratio,
        "avg_final_execution_fill_ratio": summary.avg_final_execution_fill_ratio,
        "max_execution_target_gap_pct": summary.max_execution_target_gap,
        "max_execution_schedule_expired_count": summary.max_execution_schedule_expired_count,
        "total_execution_schedule_expired_count": summary.total_execution_schedule_expired_count,
    })
}

fn oos_cost_capacity_perturbation_result_json(result: &OosCostCapacityPerturbationResult) -> Value {
    json!({
        "name": result.name,
        "perturbation": result.perturbation,
        "backtest_task_id": result.backtest_task_id,
        "passed": result.passed,
        "metrics": {
            "annual_return_pct": result.output.metrics.annual_return_pct,
            "excess_return_pct": result.output.metrics.excess_return_pct,
            "sharpe_ratio": result.output.metrics.sharpe_ratio,
            "sortino_ratio": result.output.metrics.sortino_ratio,
            "calmar_ratio": result.output.metrics.calmar_ratio,
            "max_drawdown_pct": result.output.metrics.max_drawdown_pct,
            "profit_factor": result.output.metrics.profit_factor,
            "num_trades": result.output.metrics.num_trades,
            "execution_schedule_expired_count": result.output.metrics.execution_schedule_expired_count,
            "execution_schedule_roll_forward_count": result.output.metrics.execution_schedule_roll_forward_count,
            "max_execution_target_gap_pct": result.output.metrics.max_execution_target_gap_pct,
            "final_cash_weight_pct": result.output.metrics.final_cash_weight_pct,
            "final_target_gross_exposure_pct": result.output.metrics.final_target_gross_exposure_pct,
            "final_actual_gross_exposure_pct": result.output.metrics.final_actual_gross_exposure_pct,
            "final_unfilled_target_gap_pct": result.output.metrics.final_unfilled_target_gap_pct,
            "final_execution_fill_ratio": result.output.metrics.final_execution_fill_ratio,
        }
    })
}

async fn persist_oos_walk_forward_experiment(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
    windows: &[Value],
    stitched_summary: &Value,
    gates: &Value,
    cache_report: &Value,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let config = oos_walk_forward_experiment_config(req, plan);
    let metrics = json!({
        "windows": windows,
        "stitched_oos": {
            "metrics": stitched_summary,
            "gates": gates,
            "status": oos_gate_status(gates),
        },
        "cache": cache_report,
    });
    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'phase7_oos_walk_forward_discovery', 'oos_discovery', $2,
                 $3, $4, 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(&experiment_run_id)
    .bind(&config)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert OOS walk-forward experiment_run: {}",
            error
        )
    })?;
    Ok(experiment_run_id)
}

async fn create_running_oos_walk_forward_experiment(
    db: &sqlx::PgPool,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let config = oos_walk_forward_experiment_config(req, plan);
    let metrics = oos_walk_forward_progress_metrics(
        plan["window_count"].as_u64().unwrap_or(0) as usize,
        &[],
        &json!({}),
        &json!({}),
    );
    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at)
         VALUES ($1, 'phase7_oos_walk_forward_discovery', 'oos_discovery', $2,
                 $3, $4, 'running', now())",
    )
    .bind(&experiment_run_id)
    .bind(&experiment_run_id)
    .bind(&config)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert running OOS walk-forward experiment_run: {}",
            error
        )
    })?;
    Ok(experiment_run_id)
}

async fn update_oos_walk_forward_experiment_progress(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
    plan: &OosDiscoveryPlan,
    windows: &[Value],
    stitched_summary: &Value,
    cache_report: &Value,
) -> Result<(), String> {
    let metrics = oos_walk_forward_progress_metrics(
        plan.windows.len(),
        windows,
        stitched_summary,
        cache_report,
    );
    sqlx::query(
        "UPDATE experiment_run
         SET metrics = $2,
             status = 'running'
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to update OOS walk-forward experiment progress: {}",
            error
        )
    })?;
    Ok(())
}

async fn complete_oos_walk_forward_experiment(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
    windows: &[Value],
    stitched_summary: &Value,
    gates: &Value,
    cache_report: &Value,
) -> Result<(), String> {
    let config = oos_walk_forward_experiment_config(req, plan);
    let metrics = json!({
        "windows": windows,
        "stitched_oos": {
            "metrics": stitched_summary,
            "gates": gates,
            "status": oos_gate_status(gates),
        },
        "cache": cache_report,
    });
    sqlx::query(
        "UPDATE experiment_run
         SET config = $2,
             metrics = $3,
             status = 'completed',
             completed_at = now()
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .bind(&config)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to complete OOS walk-forward experiment_run: {}",
            error
        )
    })?;
    Ok(())
}

async fn mark_oos_walk_forward_experiment_failed(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
    message: &str,
) -> Result<(), String> {
    let metrics = json!({
        "status": "failed",
        "error_message": message,
    });
    sqlx::query(
        "UPDATE experiment_run
         SET metrics = coalesce(metrics, '{}'::jsonb) || $2::jsonb,
             status = 'failed',
             completed_at = now()
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .bind(&metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to mark OOS walk-forward experiment_run failed: {}",
            error
        )
    })?;
    Ok(())
}

fn oos_walk_forward_experiment_config(
    req: &Phase7OosWalkForwardDiscoveryRequest,
    plan: &Value,
) -> Value {
    let train_selection_gate_policy = resolve_oos_train_selection_gate_policy(req);
    let train_cost_gate =
        train_cost_capacity_perturbation_gate_config(req, &train_selection_gate_policy);
    let train_execution_policy =
        resolve_oos_train_execution_policy(req, &LocalResourcePlan::local_mac()).unwrap_or(
            OosTrainExecutionPolicy {
                requested_cache_mode: "shared_window",
                cache_mode: "shared_window",
                requested_trial_concurrency: 1,
                trial_concurrency: 1,
            },
        );
    let train_stress_score_profile =
        train_cost_capacity_stress_score_profile(&train_selection_gate_policy);
    json!({
        "strategy_version_id": req.strategy_version_id,
        "data_version_id": req.data_version_id,
        "search_profile": req.search_profile,
        "max_trials_per_window": req.max_trials_per_window,
        "exhaustive_search": req.exhaustive_search.unwrap_or(false),
        "trial_batch_limit": req.trial_batch_limit,
        "requested_trial_concurrency": train_execution_policy.requested_trial_concurrency,
        "trial_concurrency": train_execution_policy.trial_concurrency,
        "requested_train_cache_mode": train_execution_policy.requested_cache_mode,
        "train_trial_concurrency": train_execution_policy.trial_concurrency,
        "train_cache_mode": train_execution_policy.cache_mode,
        "return_risk_feature_cache_mode": request_return_risk_feature_cache_mode(req),
        "max_batches_per_window": req.max_batches_per_window,
        "require_train_robustness_approval": req.require_train_robustness_approval.unwrap_or(true),
        "train_selection_gate_policy": train_selection_gate_policy,
        "train_cost_capacity_perturbation_gate": {
            "enabled": train_cost_gate.enabled,
            "stress_aware_selection_enabled": train_cost_capacity_stress_aware_selection_enabled(
                &train_selection_gate_policy,
                &train_cost_gate
            ),
            "stress_aware_selection_score": train_stress_score_profile,
            "perturbations": resolved_train_cost_capacity_perturbations(
                req,
                &train_selection_gate_policy
            ),
            "min_pass_ratio": train_cost_gate.min_pass_ratio,
            "min_perturbed_oos_calmar": train_cost_gate.min_perturbed_calmar,
            "max_perturbed_oos_drawdown_pct": train_cost_gate.max_perturbed_drawdown_pct,
        },
        "legacy_train_robustness_gate_policy": req.train_robustness_gate_policy,
        "final_promotion_gate_policy": resolve_oos_final_promotion_gate_policy(
            req,
            plan["validation_mode"].as_str().unwrap_or("walk_forward")
        ),
        "min_oos_window_count": req.min_oos_window_count,
        "min_stitched_oos_calmar": req.min_stitched_oos_calmar.unwrap_or(1.2),
        "min_positive_oos_window_ratio": req.min_positive_oos_window_ratio.unwrap_or(0.60),
        "oos_top_n": req.oos_top_n.unwrap_or(1),
        "cost_capacity_perturbation_gate": {
            "enabled": cost_capacity_perturbation_gate_enabled(req),
            "perturbations": resolved_oos_cost_capacity_perturbations(req),
            "min_pass_ratio": req.min_cost_capacity_perturbation_pass_ratio.unwrap_or(0.80),
            "min_perturbed_oos_calmar": req.min_perturbed_oos_calmar.unwrap_or(1.2),
            "max_perturbed_oos_drawdown_pct": req.max_perturbed_oos_drawdown_pct.unwrap_or(0.35),
        },
        "execution_mode": normalize_oos_execution_mode(req.execution_mode.as_deref()).unwrap_or("inline"),
        "resource_plan": LocalResourcePlan::local_mac(),
        "plan": plan,
    })
}

fn oos_walk_forward_progress_metrics(
    total_windows: usize,
    windows: &[Value],
    stitched_summary: &Value,
    cache_report: &Value,
) -> Value {
    let completed_windows = windows.len();
    let progress_pct = if total_windows == 0 {
        0
    } else {
        ((completed_windows * 100) / total_windows).min(100)
    };
    json!({
        "status": "running",
        "completed_windows": completed_windows,
        "total_windows": total_windows,
        "progress_pct": progress_pct,
        "latest_window": windows.last(),
        "windows": windows,
        "stitched_oos": {
            "metrics": stitched_summary,
        },
        "cache": cache_report,
    })
}

fn oos_cache_report(signal_cache: &SignalDataCache, backtest_cache: &BacktestDataCache) -> Value {
    json!({
        "oos_signal_cache": signal_cache_stats_delta(
            SignalDataCacheStats::default(),
            signal_cache.stats()
        ),
        "oos_backtest_cache": backtest_cache_stats_delta(
            BacktestDataCacheStats::default(),
            backtest_cache.stats()
        ),
    })
}

async fn build_return_risk_cache_economics_report(
    db: &sqlx::PgPool,
    req: &ReturnRiskCacheEconomicsReportRequest,
) -> Result<Value, String> {
    let raw_run = load_experiment_run_metrics(db, &req.raw_experiment_run_id).await?;
    let stats_run = load_experiment_run_metrics(db, &req.stats_experiment_run_id).await?;
    ensure_completed_cache_economics_input(&req.raw_experiment_run_id, "raw", &raw_run.status)?;
    ensure_completed_cache_economics_input(
        &req.stats_experiment_run_id,
        "stats",
        &stats_run.status,
    )?;
    let raw_signal_cache = aggregate_signal_cache_stats_from_experiment_metrics(&raw_run.metrics);
    let stats_signal_cache =
        aggregate_signal_cache_stats_from_experiment_metrics(&stats_run.metrics);
    let report = return_risk_cache_economics_report_json(
        &req.raw_experiment_run_id,
        raw_signal_cache,
        &req.stats_experiment_run_id,
        stats_signal_cache,
    );
    let report_experiment_run_id = persist_return_risk_cache_economics_report(
        db,
        &req.raw_experiment_run_id,
        &req.stats_experiment_run_id,
        &report,
    )
    .await?;

    Ok(json!({
        "experiment_run_id": report_experiment_run_id,
        "report": report,
    }))
}

#[derive(Debug, Clone)]
struct ExperimentRunMetrics {
    metrics: Value,
    status: String,
}

async fn load_experiment_run_metrics(
    db: &sqlx::PgPool,
    experiment_run_id: &str,
) -> Result<ExperimentRunMetrics, String> {
    let row = sqlx::query_as::<_, (Option<Value>, String)>(
        "SELECT metrics, status
         FROM experiment_run
         WHERE experiment_run_id = $1",
    )
    .bind(experiment_run_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load experiment_run metrics: {}", error))?
    .ok_or_else(|| format!("experiment_run not found: {}", experiment_run_id))?;
    Ok(ExperimentRunMetrics {
        metrics: row.0.unwrap_or_else(|| json!({})),
        status: row.1,
    })
}

fn ensure_completed_cache_economics_input(
    experiment_run_id: &str,
    role: &str,
    status: &str,
) -> Result<(), String> {
    if status == "completed" {
        return Ok(());
    }
    Err(format!(
        "return-risk cache economics report requires completed {} experiment {}; status={}",
        role, experiment_run_id, status
    ))
}

async fn persist_return_risk_cache_economics_report(
    db: &sqlx::PgPool,
    raw_experiment_run_id: &str,
    stats_experiment_run_id: &str,
    report: &Value,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    let config = json!({
        "raw_experiment_run_id": raw_experiment_run_id,
        "stats_experiment_run_id": stats_experiment_run_id,
        "comparison": "return_risk_cache_economics",
    });
    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'return_risk_cache_economics_report', 'experiment_pair', $2,
                 $3, $4, 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(raw_experiment_run_id)
    .bind(&config)
    .bind(report)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert return/risk cache economics experiment_run: {}",
            error
        )
    })?;
    Ok(experiment_run_id)
}

fn return_risk_cache_economics_report_json(
    raw_experiment_run_id: &str,
    raw_signal_cache: SignalDataCacheStats,
    stats_experiment_run_id: &str,
    stats_signal_cache: SignalDataCacheStats,
) -> Value {
    let economics = compare_return_risk_cache_economics(raw_signal_cache, stats_signal_cache);
    json!({
        "raw_experiment_run_id": raw_experiment_run_id,
        "stats_experiment_run_id": stats_experiment_run_id,
        "raw_signal_cache": raw_signal_cache,
        "stats_signal_cache": stats_signal_cache,
        "cache_economics": economics,
    })
}

fn aggregate_signal_cache_stats_from_experiment_metrics(metrics: &Value) -> SignalDataCacheStats {
    let mut total = SignalDataCacheStats::default();
    add_signal_cache_stats_from_value(&mut total, metrics.get("signal_cache"));
    let has_oos_signal_cache = add_signal_cache_stats_from_value(
        &mut total,
        metrics
            .get("cache")
            .and_then(|cache| cache.get("oos_signal_cache")),
    );

    if let Some(windows) = metrics.get("windows").and_then(Value::as_array) {
        for window in windows {
            if let Some(train_batches) = window.get("train_batches").and_then(Value::as_array) {
                for batch in train_batches {
                    let has_batch_signal_cache =
                        add_signal_cache_stats_from_value(&mut total, batch.get("signal_cache"));
                    if !has_batch_signal_cache {
                        add_signal_cache_stats_from_value(
                            &mut total,
                            batch.pointer("/signal_batch_prewarm/report/cache_delta"),
                        );
                    }
                }
            }
            if !has_oos_signal_cache {
                add_signal_cache_stats_from_value(
                    &mut total,
                    window.pointer("/oos_market_feature_prewarm_report/cache_delta"),
                );
            }
        }
    }

    total
}

fn add_signal_cache_stats_from_value(
    total: &mut SignalDataCacheStats,
    value: Option<&Value>,
) -> bool {
    let Some(value) = value else {
        return false;
    };
    match serde_json::from_value::<SignalDataCacheStats>(value.clone()) {
        Ok(stats) => {
            add_signal_cache_stats(total, stats);
            true
        }
        Err(_) => false,
    }
}

async fn evaluate_discovery_candidates(
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

fn robustness_result_is_approved(result: &Value) -> bool {
    result
        .get("status")
        .and_then(Value::as_str)
        .map(|status| status == "approved_candidate")
        .unwrap_or(false)
}

pub async fn evaluate_optimization_robustness(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Json(req): Json<EvaluateRobustnessRequest>,
) -> impl IntoResponse {
    match evaluate_and_persist_robustness(&state.db, &task_id, req.gate_policy.as_ref()).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn evaluate_optimization_trial_robustness(
    State(state): State<Arc<AppState>>,
    Path((task_id, trial_id)): Path<(String, String)>,
    Json(req): Json<EvaluateRobustnessRequest>,
) -> impl IntoResponse {
    match evaluate_and_persist_robustness_for_trial(
        &state.db,
        &task_id,
        &trial_id,
        req.gate_policy.as_ref(),
        "Trial robustness gate evaluated",
    )
    .await
    {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
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

fn generate_trial_parameters(
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

async fn execute_pending_trials(
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

async fn execute_pending_trials_with_caches(
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

fn plan_factor_signal_batch_prewarm_requests(
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

async fn execute_pending_trials_with_caches_and_concurrency(
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

async fn execute_loaded_pending_trials_concurrently(
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

fn merge_trial_execution_join(
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

async fn execute_single_pending_trial(
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

fn add_signal_cache_stats(total: &mut SignalDataCacheStats, value: SignalDataCacheStats) {
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

fn add_backtest_cache_stats(total: &mut BacktestDataCacheStats, value: BacktestDataCacheStats) {
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

fn normalize_performance_gate(
    gate: Option<&OptimizationPerformanceGateRequest>,
    trial_limit: i64,
) -> Result<OptimizationPerformanceGatePolicy, String> {
    let min_completed_trials = gate
        .and_then(|gate| gate.min_completed_trials)
        .unwrap_or(trial_limit);
    if min_completed_trials < 0 {
        return Err("performance_gate.min_completed_trials must be non-negative".into());
    }
    let max_failed_trials = gate.and_then(|gate| gate.max_failed_trials).unwrap_or(0);
    if max_failed_trials < 0 {
        return Err("performance_gate.max_failed_trials must be non-negative".into());
    }
    let max_elapsed_ms = gate.and_then(|gate| gate.max_elapsed_ms);
    if matches!(max_elapsed_ms, Some(value) if value < 0) {
        return Err("performance_gate.max_elapsed_ms must be non-negative".into());
    }

    Ok(OptimizationPerformanceGatePolicy {
        min_completed_trials,
        max_failed_trials,
        max_elapsed_ms,
    })
}

fn evaluate_optimization_performance_gates(
    executed: i64,
    completed: i64,
    failed: i64,
    elapsed_ms: i64,
    policy: &OptimizationPerformanceGatePolicy,
) -> Value {
    let mut gates = vec![
        json!({
            "gate": "min_completed_trials",
            "passed": completed >= policy.min_completed_trials,
            "limit": policy.min_completed_trials,
            "actual": completed,
        }),
        json!({
            "gate": "max_failed_trials",
            "passed": failed <= policy.max_failed_trials,
            "limit": policy.max_failed_trials,
            "actual": failed,
        }),
        json!({
            "gate": "executed_trials",
            "passed": executed >= policy.min_completed_trials,
            "limit": policy.min_completed_trials,
            "actual": executed,
        }),
    ];
    if let Some(limit) = policy.max_elapsed_ms {
        gates.push(json!({
            "gate": "max_elapsed_ms",
            "passed": elapsed_ms <= limit,
            "limit": limit,
            "actual": elapsed_ms,
        }));
    }
    Value::Array(gates)
}

fn performance_gate_status(gates: &Value) -> &'static str {
    let passed = gates
        .as_array()
        .map(|items| {
            items
                .iter()
                .all(|item| item.get("passed").and_then(Value::as_bool).unwrap_or(false))
        })
        .unwrap_or(false);
    if passed {
        "passed"
    } else {
        "review_required"
    }
}

async fn persist_optimization_experiment_run(
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
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
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

    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'optimization_trial_batch_release_gate', 'optimization_task', $2,
                 $3, $4, $5, now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(task_id)
    .bind(&config)
    .bind(&metrics)
    .bind(experiment_status)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to insert optimization experiment_run: {}", error))?;

    Ok(experiment_run_id)
}

async fn promote_trial(
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

async fn evaluate_and_persist_robustness(
    db: &sqlx::PgPool,
    task_id: &str,
    gate_policy: Option<&Value>,
) -> Result<Value, String> {
    let task = sqlx::query_as::<_, (Option<String>, Option<String>)>(
        "SELECT best_trial_id, status
         FROM optimization_task
         WHERE optimization_task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load optimization task: {}", error))?
    .ok_or_else(|| "optimization task not found".to_string())?;

    let best_trial_id = task.0.as_deref().ok_or_else(|| {
        "optimization task has no best_trial_id; run completed trials first".to_string()
    })?;

    evaluate_and_persist_robustness_for_trial(
        db,
        task_id,
        best_trial_id,
        gate_policy,
        "Robustness gate evaluated",
    )
    .await
}

async fn evaluate_and_persist_robustness_for_trial(
    db: &sqlx::PgPool,
    task_id: &str,
    trial_id: &str,
    gate_policy: Option<&Value>,
    summary_prefix: &str,
) -> Result<Value, String> {
    let trial = sqlx::query_as::<_, (Decimal, Option<Value>, Option<Value>, Option<String>, i32)>(
        "SELECT score, metrics, constraint_violations, backtest_task_id, trial_index
         FROM optimization_trial
         WHERE optimization_task_id = $1 AND trial_id = $2 AND status = 'completed'",
    )
    .bind(task_id)
    .bind(trial_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load best optimization trial: {}", error))?
    .ok_or_else(|| "best optimization trial is not completed".to_string())?;

    let runner_up = sqlx::query_as::<_, (Decimal,)>(
        "SELECT score
         FROM optimization_trial
         WHERE optimization_task_id = $1
           AND trial_id <> $2
           AND status = 'completed'
           AND score IS NOT NULL
           AND (score < $3 OR (score = $3 AND trial_index > $4))
         ORDER BY score DESC NULLS LAST, trial_index ASC
         LIMIT 1",
    )
    .bind(task_id)
    .bind(trial_id)
    .bind(trial.0)
    .bind(trial.4)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load runner-up optimization trial: {}", error))?
    .map(|row| row.0);

    let policy = resolve_robustness_gate_policy(gate_policy);
    let analysis = if let Some(backtest_task_id) = trial.3.as_deref() {
        load_robustness_timeseries_analysis(db, backtest_task_id, &policy).await?
    } else {
        None
    };
    let raw_metrics = trial.1.unwrap_or_else(|| json!({}));
    let (metrics, metric_sources, missing_elite_metrics) =
        enrich_trial_metrics(db, trial.3.as_deref(), raw_metrics).await?;
    let evaluation = evaluate_robustness_gates_with_analysis(
        trial.0,
        runner_up,
        &metrics,
        trial.2.as_ref().unwrap_or(&json!([])),
        Some(&policy),
        analysis.as_ref(),
    );
    let gate_result_id = format!("gate-{}", Uuid::new_v4());

    sqlx::query(
        "INSERT INTO robustness_gate_result
           (gate_result_id, optimization_task_id, trial_id, gate_policy, gate_results,
            status, summary)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&gate_result_id)
    .bind(task_id)
    .bind(trial_id)
    .bind(&policy)
    .bind(&evaluation.gates)
    .bind(&evaluation.status)
    .bind(format!("{} as {}", summary_prefix, evaluation.status))
    .execute(db)
    .await
    .map_err(|error| format!("Failed to persist robustness gate result: {}", error))?;

    Ok(json!({
        "gate_result_id": gate_result_id,
        "optimization_task_id": task_id,
        "trial_id": trial_id,
        "status": evaluation.status,
        "gate_results": evaluation.gates,
        "metrics": metrics,
        "metric_sources": metric_sources,
        "missing_elite_metrics": missing_elite_metrics,
        "failure_attribution": build_robustness_failure_attribution(&evaluation.gates),
    }))
}

async fn build_and_persist_elite_validation_report(
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

async fn load_completed_trial_snapshots(
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

async fn enrich_trial_metrics(
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

async fn enrich_trial_metrics_from_backtest_result(
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

async fn derive_max_drawdown_duration_days(
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

const ELITE_REPORT_METRIC_KEYS: &[&str] = &[
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

fn existing_metric_sources(metrics: &Value) -> Map<String, Value> {
    let mut sources = Map::new();
    for key in ELITE_REPORT_METRIC_KEYS {
        if metric_has_number(metrics, key) {
            sources.insert((*key).to_string(), json!("optimization_trial.metrics"));
        }
    }
    sources
}

fn insert_metric_if_missing(
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

fn ensure_metrics_object(metrics: &mut Value) -> &mut Map<String, Value> {
    if !metrics.is_object() {
        *metrics = json!({});
    }
    metrics.as_object_mut().expect("metrics object")
}

fn metric_has_number(metrics: &Value, key: &str) -> bool {
    metrics
        .get(key)
        .and_then(value_as_f64)
        .map(|value| value.is_finite())
        .unwrap_or(false)
}

fn missing_elite_metrics(metrics: &Value) -> Vec<&'static str> {
    ELITE_REPORT_METRIC_KEYS
        .iter()
        .copied()
        .filter(|key| !metric_has_number(metrics, key))
        .collect()
}

async fn persist_elite_validation_report_experiment(
    db: &sqlx::PgPool,
    task_id: &str,
    config: &Value,
    metrics: &Value,
) -> Result<String, String> {
    let experiment_run_id = format!("exp-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO experiment_run
           (experiment_run_id, experiment_type, related_entity_type, related_entity_id,
            config, metrics, status, started_at, completed_at)
         VALUES ($1, 'professional_elite_validation_report', 'optimization_task', $2,
                 $3, $4, 'completed', now(), now())",
    )
    .bind(&experiment_run_id)
    .bind(task_id)
    .bind(config)
    .bind(metrics)
    .execute(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to insert elite validation experiment_run: {}",
            error
        )
    })?;

    Ok(experiment_run_id)
}

fn summarize_elite_validation_rows(rows: &[Value]) -> Value {
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

fn summarize_missing_elite_metric_counts(rows: &[Value]) -> Value {
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

fn elite_trial_report_order(
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

fn elite_gap_score(metrics: &Value) -> f64 {
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

fn build_parameter_plateau_analysis(
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

fn parameter_axis_plateau_summary(
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

fn is_plateau_stable_neighbor(candidate: EliteMetricProfile, peer_metrics: &Value) -> bool {
    let peer = EliteMetricProfile::from_metrics(peer_metrics);
    peer.annual_return >= candidate.annual_return - 0.01
        && peer.sharpe >= candidate.sharpe - 0.10
        && peer.sortino >= candidate.sortino - 0.15
        && peer.max_drawdown <= candidate.max_drawdown + 0.03
}

fn build_portfolio_correlation_contribution_score(
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

fn active_correlation_controls(parameters: &Value) -> Vec<Value> {
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

fn correlation_control_score(parameters: &Value) -> f64 {
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

fn summarize_trial_metric_group(trials: &[&CompletedTrialSnapshot]) -> Value {
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

fn correlation_peer_impact(controlled: &Value, uncontrolled: &Value) -> Value {
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

fn differing_parameter_keys(left: &Value, right: &Value) -> Vec<String> {
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

fn parameter_value(parameters: &Value, key: &str) -> Value {
    parameters.get(key).cloned().unwrap_or(Value::Null)
}

fn parameter_f64(parameters: &Value, key: &str) -> Option<f64> {
    parameters.get(key).and_then(value_as_f64)
}

fn parameter_i64(parameters: &Value, key: &str) -> Option<i64> {
    parameters.get(key).and_then(Value::as_i64)
}

fn parameter_str<'a>(parameters: &'a Value, key: &str) -> Option<&'a str> {
    parameters.get(key).and_then(Value::as_str)
}

fn compact_trial_metrics(trial: &CompletedTrialSnapshot) -> Value {
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

fn metric_number(metrics: &Value, name: &str) -> f64 {
    metrics.get(name).and_then(value_as_f64).unwrap_or(0.0)
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
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

async fn load_discovery_candidates(
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

fn discovery_candidate_order(
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

fn professional_candidate_gap_score(
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

fn positive_gap(limit: Decimal, actual: Decimal) -> Decimal {
    (limit - actual).max(Decimal::ZERO)
}

fn discovery_candidate_rank(candidate_type: CandidateType) -> u8 {
    match candidate_type {
        CandidateType::Professional => 0,
        CandidateType::ReviewRequired => 1,
        CandidateType::Defensive => 2,
        CandidateType::Research => 3,
    }
}

fn discovery_candidate_json(candidate: &DiscoveryCandidate) -> Value {
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

async fn load_robustness_timeseries_analysis(
    db: &sqlx::PgPool,
    backtest_task_id: &str,
    policy: &Value,
) -> Result<Option<RobustnessTimeSeriesAnalysis>, String> {
    let rows = sqlx::query_as::<_, (NaiveDate, f64, Option<f64>)>(
        "SELECT c.trade_date,
                c.portfolio_value::double precision,
                COALESCE(c.benchmark_value::double precision, i.close::double precision)
         FROM backtest_equity_curve c
         JOIN backtest_task t ON t.task_id = c.task_id
         LEFT JOIN market_index_daily_bar i
           ON i.symbol = t.benchmark_symbol
          AND i.trade_date = c.trade_date
         WHERE c.task_id = $1
         ORDER BY c.trade_date",
    )
    .bind(backtest_task_id)
    .fetch_all(db)
    .await
    .map_err(|error| {
        format!(
            "Failed to load backtest equity curve for robustness: {}",
            error
        )
    })?;

    if rows.len() < 3 {
        return Ok(None);
    }
    let points = rows
        .into_iter()
        .filter(|(_, portfolio_value, _)| portfolio_value.is_finite() && *portfolio_value > 0.0)
        .map(
            |(trade_date, portfolio_value, benchmark_value)| RobustnessDailyPoint {
                trade_date,
                portfolio_value,
                benchmark_value: benchmark_value.filter(|value| value.is_finite() && *value > 0.0),
            },
        )
        .collect::<Vec<_>>();
    if points.len() < 3 {
        return Ok(None);
    }

    let window_size = constraint_i64(Some(policy), "walk_forward_window_days")
        .unwrap_or(63)
        .max(2) as usize;
    let step_size = constraint_i64(Some(policy), "walk_forward_step_days")
        .unwrap_or(21)
        .max(1) as usize;
    let bootstrap_trials = constraint_i64(Some(policy), "bootstrap_trials")
        .unwrap_or(256)
        .clamp(1, 10_000) as usize;
    let bootstrap_seed = constraint_i64(Some(policy), "bootstrap_seed")
        .unwrap_or(42)
        .max(1) as u64;

    RobustnessTimeSeriesAnalysis::from_points(
        &points,
        window_size.min(points.len()),
        step_size,
        bootstrap_trials,
        bootstrap_seed,
    )
    .map(Some)
}

fn normalize_promote_request(
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

async fn load_execution_context(
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

fn build_optimization_trial_request(
    task: &OptimizationTaskExecutionContext,
    parameters: &Value,
) -> Result<OptimizationTrialBacktestRequest, String> {
    match trial_signal_source(task, parameters)?.as_deref() {
        Some("prediction") | Some("model_prediction") | Some("ml_prediction") => {
            build_prediction_trial_request(task, parameters)
                .map(OptimizationTrialBacktestRequest::Prediction)
        }
        Some("factor") | Some("factor_combo") | None => {
            build_factor_trial_request(task, parameters)
                .map(OptimizationTrialBacktestRequest::Factor)
        }
        Some(other) => Err(format!("unsupported signal_source: {}", other)),
    }
}

fn trial_signal_source(
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

fn build_factor_trial_request(
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
    })
}

fn optional_cost_model_from_maps(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<Option<CostModelReq>, String> {
    optional_struct_from_merged_maps(params, template, "cost_model")
}

fn optional_execution_rules_from_maps(
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

fn optional_struct_from_merged_maps<T>(
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

fn merged_object_from_maps(
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

fn build_prediction_trial_request(
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
    })
}

fn market_regime_request_from_value(
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

fn optional_usize_from_object(
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

struct ReusableTrial {
    backtest_task_id: Option<String>,
    score: Decimal,
    metrics: Value,
    constraint_violations: Value,
}

async fn find_reusable_trial(
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

async fn mark_trial_reused(
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

fn trial_reuse_key(
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

fn canonical_json(value: &Value) -> String {
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

fn score_trial(
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

fn professional_candidate_objective_score(
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

fn positive_decimal_gap(limit: Decimal, actual: Decimal) -> Decimal {
    (limit - actual).max(Decimal::ZERO)
}

fn score_trial_with_output(
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

impl RobustnessDailyPoint {
    #[cfg(test)]
    fn new(trade_date: &str, portfolio_value: f64, benchmark_value: Option<f64>) -> Self {
        Self {
            trade_date: NaiveDate::parse_from_str(trade_date, "%Y-%m-%d").expect("valid date"),
            portfolio_value,
            benchmark_value,
        }
    }
}

impl RobustnessTimeSeriesAnalysis {
    fn from_points(
        points: &[RobustnessDailyPoint],
        window_size: usize,
        step_size: usize,
        bootstrap_trials: usize,
        bootstrap_seed: u64,
    ) -> Result<Self, String> {
        Ok(Self {
            market_scenarios: build_market_scenario_analysis(points),
            walk_forward: build_walk_forward_analysis(points, window_size, step_size),
            bootstrap: build_bootstrap_analysis(points, bootstrap_trials, bootstrap_seed)?,
        })
    }
}

fn build_market_scenario_analysis(points: &[RobustnessDailyPoint]) -> Value {
    if points.len() < 2 {
        return json!({
            "scenario_count": 0,
            "scenarios": []
        });
    }
    let summary = summarize_points(points);
    let volatility =
        annualized_volatility(&daily_returns(points, |point| Some(point.portfolio_value)));
    let scenario = classify_market_scenario(&summary, volatility);
    json!({
        "scenario_count": 1,
        "scenarios": [{
            "scenario": scenario,
            "start_date": points.first().map(|point| point.trade_date),
            "end_date": points.last().map(|point| point.trade_date),
            "metrics": metric_summary_json(&summary),
            "annualized_volatility": volatility
        }]
    })
}

fn build_walk_forward_analysis(
    points: &[RobustnessDailyPoint],
    window_size: usize,
    step_size: usize,
) -> Value {
    if points.len() < 2 || window_size < 2 || step_size == 0 {
        return json!({
            "window_count": 0,
            "windows": [],
            "positive_excess_window_ratio": 0.0,
            "worst_window_drawdown": 0.0
        });
    }

    let mut windows = Vec::new();
    let mut annual_returns = Vec::new();
    let mut excess_returns = Vec::new();
    let mut sharpes = Vec::new();
    let mut sortinos = Vec::new();
    let mut calmars = Vec::new();
    let mut idx = 0;
    while idx + window_size <= points.len() {
        let slice = &points[idx..idx + window_size];
        let summary = summarize_points(slice);
        let volatility =
            annualized_volatility(&daily_returns(slice, |point| Some(point.portfolio_value)));
        annual_returns.push(summary.annual_return);
        if let Some(excess_return) = summary.excess_return {
            excess_returns.push(excess_return);
        }
        sharpes.push(summary.sharpe_ratio);
        sortinos.push(summary.sortino_ratio);
        calmars.push(summary.calmar_ratio);
        windows.push(json!({
            "window_index": windows.len() + 1,
            "start_date": slice.first().map(|point| point.trade_date),
            "end_date": slice.last().map(|point| point.trade_date),
            "scenario": classify_market_scenario(&summary, volatility),
            "metrics": metric_summary_json(&summary),
            "annualized_volatility": volatility
        }));
        idx += step_size;
    }

    let positive_excess_count = windows
        .iter()
        .filter(|window| {
            window["metrics"]["excess_return"]
                .as_f64()
                .map(|value| value > 0.0)
                .unwrap_or(false)
        })
        .count();
    let positive_annual_return_count = annual_returns
        .iter()
        .filter(|value| value.is_finite() && **value > 0.0)
        .count();
    let worst_window_drawdown = windows
        .iter()
        .filter_map(|window| window["metrics"]["max_drawdown"].as_f64())
        .fold(0.0_f64, f64::max);
    let scenario_count = windows
        .iter()
        .filter_map(|window| window["scenario"].as_str())
        .collect::<BTreeSet<_>>()
        .len();

    json!({
        "window_count": windows.len(),
        "window_size": window_size,
        "step_size": step_size,
        "positive_excess_window_ratio": if windows.is_empty() { 0.0 } else { positive_excess_count as f64 / windows.len() as f64 },
        "positive_annual_return_ratio": if windows.is_empty() { 0.0 } else { positive_annual_return_count as f64 / windows.len() as f64 },
        "worst_window_drawdown": worst_window_drawdown,
        "scenario_count": scenario_count,
        "annual_return": distribution_summary(&mut annual_returns),
        "excess_return": distribution_summary(&mut excess_returns),
        "sharpe_ratio": distribution_summary(&mut sharpes),
        "sortino_ratio": distribution_summary(&mut sortinos),
        "calmar_ratio": distribution_summary(&mut calmars),
        "windows": windows
    })
}

fn build_bootstrap_analysis(
    points: &[RobustnessDailyPoint],
    trials: usize,
    seed: u64,
) -> Result<Value, String> {
    if points.len() < 3 {
        return Err("bootstrap requires at least 3 equity points".into());
    }
    let returns = daily_returns(points, |point| Some(point.portfolio_value));
    if returns.is_empty() {
        return Err("bootstrap requires non-empty daily returns".into());
    }
    let trials = trials.clamp(1, 10_000);
    let mut rng = DeterministicRng::new(seed);
    let mut total_returns = Vec::with_capacity(trials);
    let mut sharpes = Vec::with_capacity(trials);
    let mut sortinos = Vec::with_capacity(trials);
    let mut calmars = Vec::with_capacity(trials);
    let mut drawdowns = Vec::with_capacity(trials);
    let mut positive = 0usize;

    for _ in 0..trials {
        let sample = (0..returns.len())
            .map(|_| returns[rng.gen_usize(returns.len())])
            .collect::<Vec<_>>();
        let summary = summarize_return_sample(&sample);
        if summary.total_return > 0.0 {
            positive += 1;
        }
        total_returns.push(summary.total_return);
        sharpes.push(summary.sharpe_ratio);
        sortinos.push(summary.sortino_ratio);
        calmars.push(summary.calmar_ratio);
        drawdowns.push(summary.max_drawdown);
    }

    Ok(json!({
        "trials": trials,
        "sample_size": returns.len(),
        "positive_return_probability": positive as f64 / trials as f64,
        "total_return": distribution_summary(&mut total_returns),
        "sharpe_ratio": distribution_summary(&mut sharpes),
        "sortino_ratio": distribution_summary(&mut sortinos),
        "calmar_ratio": distribution_summary(&mut calmars),
        "max_drawdown": distribution_summary(&mut drawdowns)
    }))
}

fn classify_market_scenario(
    summary: &RobustnessMetricSummary,
    annualized_volatility: f64,
) -> &'static str {
    let benchmark_return = summary.benchmark_return.unwrap_or(summary.total_return);
    if annualized_volatility >= 0.30 {
        "high_volatility"
    } else if benchmark_return <= -0.02 || summary.max_drawdown >= 0.20 {
        "bear"
    } else if benchmark_return >= 0.10 && summary.max_drawdown <= 0.15 {
        "bull"
    } else if annualized_volatility <= 0.12 && benchmark_return.abs() <= 0.05 {
        "sideways"
    } else {
        "mixed"
    }
}

fn summarize_points(points: &[RobustnessDailyPoint]) -> RobustnessMetricSummary {
    let portfolio_returns = daily_returns(points, |point| Some(point.portfolio_value));
    let benchmark_returns = daily_returns(points, |point| point.benchmark_value);
    let mut nav = points
        .iter()
        .map(|point| point.portfolio_value)
        .collect::<Vec<_>>();
    let total_return = ratio_return(
        points.first().map(|point| point.portfolio_value),
        points.last().map(|point| point.portfolio_value),
    );
    let benchmark_return = if benchmark_returns.is_empty() {
        None
    } else {
        ratio_return(
            points.first().and_then(|point| point.benchmark_value),
            points.last().and_then(|point| point.benchmark_value),
        )
        .into()
    };
    let sample = summarize_return_sample(&portfolio_returns);
    let max_drawdown = max_drawdown(&mut nav);
    RobustnessMetricSummary {
        total_return,
        annual_return: annualized_return(total_return, points.len().saturating_sub(1)),
        sharpe_ratio: sample.sharpe_ratio,
        sortino_ratio: sample.sortino_ratio,
        calmar_ratio: calmar_ratio(
            annualized_return(total_return, points.len().saturating_sub(1)),
            max_drawdown,
        ),
        max_drawdown,
        benchmark_return,
        excess_return: benchmark_return.map(|value| total_return - value),
    }
}

fn summarize_return_sample(returns: &[f64]) -> RobustnessMetricSummary {
    let total_return = returns.iter().fold(1.0, |acc, value| acc * (1.0 + value)) - 1.0;
    let volatility = annualized_volatility(returns);
    let annual_return = annualized_return(total_return, returns.len());
    let max_drawdown = drawdown_from_returns(returns);
    RobustnessMetricSummary {
        total_return,
        annual_return,
        sharpe_ratio: if volatility > 0.0 {
            annual_return / volatility
        } else {
            0.0
        },
        sortino_ratio: sortino_ratio(annual_return, returns),
        calmar_ratio: calmar_ratio(annual_return, max_drawdown),
        max_drawdown,
        benchmark_return: None,
        excess_return: None,
    }
}

fn metric_summary_json(summary: &RobustnessMetricSummary) -> Value {
    json!({
        "total_return": summary.total_return,
        "annual_return": summary.annual_return,
        "sharpe_ratio": summary.sharpe_ratio,
        "sortino_ratio": summary.sortino_ratio,
        "calmar_ratio": summary.calmar_ratio,
        "max_drawdown": summary.max_drawdown,
        "benchmark_return": summary.benchmark_return,
        "excess_return": summary.excess_return
    })
}

fn daily_returns<F>(points: &[RobustnessDailyPoint], value_fn: F) -> Vec<f64>
where
    F: Fn(&RobustnessDailyPoint) -> Option<f64>,
{
    points
        .windows(2)
        .filter_map(|window| {
            let prev = value_fn(&window[0])?;
            let next = value_fn(&window[1])?;
            if prev.is_finite() && next.is_finite() && prev > 0.0 {
                Some(next / prev - 1.0)
            } else {
                None
            }
        })
        .collect()
}

fn ratio_return(start: Option<f64>, end: Option<f64>) -> f64 {
    match (start, end) {
        (Some(start), Some(end)) if start.is_finite() && end.is_finite() && start > 0.0 => {
            end / start - 1.0
        }
        _ => 0.0,
    }
}

fn annualized_return(total_return: f64, periods: usize) -> f64 {
    if periods == 0 || total_return <= -1.0 {
        return 0.0;
    }
    (1.0 + total_return).powf(252.0 / periods as f64) - 1.0
}

fn annualized_volatility(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    variance.sqrt() * 252.0_f64.sqrt()
}

fn sortino_ratio(annual_return: f64, returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let downside_sum = returns
        .iter()
        .filter(|value| **value < 0.0)
        .map(|value| value * value)
        .sum::<f64>();
    if downside_sum <= 0.0 {
        return if annual_return > 0.0 { 999.0 } else { 0.0 };
    }
    let downside_deviation = (downside_sum / (returns.len() - 1) as f64).sqrt() * 252.0_f64.sqrt();
    if downside_deviation > 0.0 {
        annual_return / downside_deviation
    } else {
        0.0
    }
}

fn calmar_ratio(annual_return: f64, max_drawdown: f64) -> f64 {
    if max_drawdown > 0.0 {
        annual_return / max_drawdown
    } else if annual_return > 0.0 {
        999.0
    } else {
        0.0
    }
}

fn max_drawdown(nav: &mut [f64]) -> f64 {
    let mut peak = None::<f64>;
    let mut max_dd = 0.0;
    for value in nav
        .iter()
        .copied()
        .filter(|value| value.is_finite() && *value > 0.0)
    {
        let current_peak = peak.map(|peak| peak.max(value)).unwrap_or(value);
        peak = Some(current_peak);
        if current_peak > 0.0 {
            let drawdown = (current_peak - value) / current_peak;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }
    }
    max_dd
}

fn drawdown_from_returns(returns: &[f64]) -> f64 {
    let mut value = 1.0;
    let mut nav = Vec::with_capacity(returns.len() + 1);
    nav.push(value);
    for daily_return in returns {
        value *= 1.0 + daily_return;
        nav.push(value);
    }
    max_drawdown(&mut nav)
}

fn distribution_summary(values: &mut [f64]) -> Value {
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "p05": percentile(values, 0.05),
        "median": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "mean": if values.is_empty() { 0.0 } else { values.iter().sum::<f64>() / values.len() as f64 }
    })
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[idx.min(values.len() - 1)]
}

#[cfg(test)]
fn evaluate_robustness_gates(
    best_score: Decimal,
    runner_up_score: Option<Decimal>,
    metrics: &Value,
    constraint_violations: &Value,
    gate_policy: Option<&Value>,
) -> RobustnessEvaluation {
    evaluate_robustness_gates_with_analysis(
        best_score,
        runner_up_score,
        metrics,
        constraint_violations,
        gate_policy,
        None,
    )
}

fn evaluate_robustness_gates_with_analysis(
    best_score: Decimal,
    runner_up_score: Option<Decimal>,
    metrics: &Value,
    constraint_violations: &Value,
    gate_policy: Option<&Value>,
    analysis: Option<&RobustnessTimeSeriesAnalysis>,
) -> RobustnessEvaluation {
    let min_trade_count = constraint_i64(gate_policy, "min_trade_count").unwrap_or(1);
    let min_annual_return = constraint_decimal(gate_policy, "min_annual_return");
    let min_excess_return = constraint_decimal(gate_policy, "min_excess_return");
    let min_sharpe = constraint_decimal(gate_policy, "min_sharpe");
    let min_sortino = constraint_decimal(gate_policy, "min_sortino");
    let min_calmar = constraint_decimal(gate_policy, "min_calmar");
    let min_profit_factor = constraint_decimal(gate_policy, "min_profit_factor");
    let min_win_rate = constraint_decimal(gate_policy, "min_win_rate");
    let max_drawdown_duration_days = constraint_i64(gate_policy, "max_drawdown_duration_days");
    let max_drawdown =
        constraint_decimal(gate_policy, "max_drawdown").unwrap_or(Decimal::new(20, 2));
    let min_score_gap = constraint_decimal(gate_policy, "min_score_gap").unwrap_or(Decimal::ZERO);
    let min_walk_forward_windows =
        constraint_i64(gate_policy, "min_walk_forward_windows").unwrap_or(0);
    let min_positive_excess_window_ratio =
        constraint_f64(gate_policy, "min_positive_excess_window_ratio").unwrap_or(0.0);
    let min_positive_annual_return_window_ratio =
        constraint_f64(gate_policy, "min_positive_annual_return_window_ratio").unwrap_or(0.0);
    let min_walk_forward_median_sharpe =
        constraint_f64(gate_policy, "min_walk_forward_median_sharpe").unwrap_or(0.0);
    let min_walk_forward_median_calmar =
        constraint_f64(gate_policy, "min_walk_forward_median_calmar");
    let min_bootstrap_positive_return_probability =
        constraint_f64(gate_policy, "min_bootstrap_positive_return_probability").unwrap_or(0.0);
    let min_bootstrap_sharpe_p05 =
        constraint_f64(gate_policy, "min_bootstrap_sharpe_p05").unwrap_or(f64::NEG_INFINITY);
    let min_bootstrap_calmar_p05 = constraint_f64(gate_policy, "min_bootstrap_calmar_p05");
    let max_bootstrap_drawdown_p95 = constraint_f64(gate_policy, "max_bootstrap_drawdown_p95");
    let min_market_scenarios = constraint_i64(gate_policy, "min_market_scenarios").unwrap_or(1);
    let enforce_trial_constraint_violations =
        constraint_bool(gate_policy, "enforce_trial_constraint_violations").unwrap_or(true);

    let num_trades = metrics
        .get("num_trades")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let annual_return =
        decimal_from_json(metrics.get("annual_return_pct")).unwrap_or(Decimal::ZERO);
    let excess_return =
        decimal_from_json(metrics.get("excess_return_pct")).unwrap_or(Decimal::ZERO);
    let sharpe = decimal_from_json(metrics.get("sharpe_ratio")).unwrap_or(Decimal::ZERO);
    let sortino = decimal_from_json(metrics.get("sortino_ratio")).unwrap_or(Decimal::ZERO);
    let calmar = decimal_from_json(metrics.get("calmar_ratio")).unwrap_or(Decimal::ZERO);
    let profit_factor = decimal_from_json(metrics.get("profit_factor")).unwrap_or(Decimal::ZERO);
    let win_rate = decimal_from_json(metrics.get("win_rate_pct")).unwrap_or(Decimal::ZERO);
    let drawdown = decimal_from_json(metrics.get("max_drawdown_pct")).unwrap_or(Decimal::ZERO);
    let drawdown_duration_days = metrics
        .get("max_drawdown_duration_days")
        .and_then(Value::as_i64);
    let hard_violations = constraint_violations
        .as_array()
        .map(|items| !items.is_empty())
        .unwrap_or(false);
    let score_gap = runner_up_score.map(|runner_up| best_score - runner_up);

    let mut gates = vec![
        json!({
            "gate": "no_hard_constraint_violations",
            "passed": !enforce_trial_constraint_violations || !hard_violations,
            "enforced": enforce_trial_constraint_violations,
            "actual": constraint_violations,
        }),
        json!({
            "gate": "min_trade_count",
            "passed": num_trades >= min_trade_count,
            "limit": min_trade_count,
            "actual": num_trades,
        }),
        json!({
            "gate": "max_drawdown",
            "passed": drawdown < max_drawdown,
            "limit": max_drawdown,
            "actual": drawdown,
        }),
        json!({
            "gate": "score_gap_vs_runner_up",
            "passed": score_gap.map(|gap| gap >= min_score_gap).unwrap_or(true),
            "limit": min_score_gap,
            "actual": score_gap,
        }),
    ];
    if let Some(coverage) = metrics.get("effective_coverage") {
        gates.push(json!({
            "gate": "effective_coverage_start",
            "passed": true,
            "actual": coverage.get("effective_start_date").cloned().unwrap_or(Value::Null),
            "details": coverage,
        }));
    }
    if let Some(limit) = min_annual_return {
        gates.push(json!({
            "gate": "min_annual_return",
            "passed": annual_return >= limit,
            "limit": limit,
            "actual": annual_return,
        }));
    }
    if let Some(limit) = min_excess_return {
        gates.push(json!({
            "gate": "min_excess_return",
            "passed": excess_return > limit,
            "limit": limit,
            "actual": excess_return,
        }));
    }
    if let Some(limit) = min_sharpe {
        gates.push(json!({
            "gate": "min_sharpe",
            "passed": sharpe > limit,
            "limit": limit,
            "actual": sharpe,
        }));
    }
    if let Some(limit) = min_sortino {
        gates.push(json!({
            "gate": "min_sortino",
            "passed": sortino >= limit,
            "limit": limit,
            "actual": sortino,
        }));
    }
    if let Some(limit) = min_calmar {
        gates.push(json!({
            "gate": "min_calmar",
            "passed": calmar > limit,
            "limit": limit,
            "actual": calmar,
        }));
    }
    if let Some(limit) = min_profit_factor {
        gates.push(json!({
            "gate": "min_profit_factor",
            "passed": profit_factor > limit,
            "limit": limit,
            "actual": profit_factor,
        }));
    }
    if let Some(limit) = min_win_rate {
        gates.push(json!({
            "gate": "min_win_rate",
            "passed": win_rate >= limit,
            "limit": limit,
            "actual": win_rate,
        }));
    }
    if let Some(limit) = max_drawdown_duration_days {
        gates.push(json!({
            "gate": "max_drawdown_duration_days",
            "passed": drawdown_duration_days.map(|actual| actual <= limit).unwrap_or(false),
            "limit": limit,
            "actual": drawdown_duration_days,
        }));
    }
    if let Some(analysis) = analysis {
        let window_count = analysis.walk_forward["window_count"].as_i64().unwrap_or(0);
        let positive_excess_window_ratio = analysis.walk_forward["positive_excess_window_ratio"]
            .as_f64()
            .unwrap_or(0.0);
        let positive_annual_return_ratio = analysis.walk_forward["positive_annual_return_ratio"]
            .as_f64()
            .unwrap_or(0.0);
        let walk_forward_median_sharpe = analysis.walk_forward["sharpe_ratio"]["median"]
            .as_f64()
            .unwrap_or(0.0);
        let walk_forward_median_calmar = analysis.walk_forward["calmar_ratio"]["median"]
            .as_f64()
            .unwrap_or(0.0);
        let bootstrap_positive_return_probability = analysis.bootstrap
            ["positive_return_probability"]
            .as_f64()
            .unwrap_or(0.0);
        let bootstrap_sharpe_p05 = analysis.bootstrap["sharpe_ratio"]["p05"]
            .as_f64()
            .unwrap_or(f64::NEG_INFINITY);
        let bootstrap_calmar_p05 = analysis.bootstrap["calmar_ratio"]["p05"]
            .as_f64()
            .unwrap_or(0.0);
        let bootstrap_drawdown_p95 = analysis.bootstrap["max_drawdown"]["p95"]
            .as_f64()
            .unwrap_or(0.0);
        let market_scenario_count = analysis.walk_forward["scenario_count"]
            .as_i64()
            .or_else(|| analysis.market_scenarios["scenario_count"].as_i64())
            .unwrap_or(0);
        gates.push(json!({
            "gate": "walk_forward_min_window_count",
            "passed": window_count >= min_walk_forward_windows,
            "limit": min_walk_forward_windows,
            "actual": window_count,
            "details": analysis.walk_forward,
        }));
        gates.push(json!({
            "gate": "walk_forward_positive_excess_ratio",
            "passed": positive_excess_window_ratio >= min_positive_excess_window_ratio,
            "limit": min_positive_excess_window_ratio,
            "actual": positive_excess_window_ratio,
        }));
        gates.push(json!({
            "gate": "walk_forward_positive_annual_return_ratio",
            "passed": positive_annual_return_ratio >= min_positive_annual_return_window_ratio,
            "limit": min_positive_annual_return_window_ratio,
            "actual": positive_annual_return_ratio,
            "details": {
                "annual_return": analysis.walk_forward["annual_return"].clone(),
                "window_count": window_count
            },
        }));
        gates.push(json!({
            "gate": "walk_forward_median_sharpe",
            "passed": walk_forward_median_sharpe >= min_walk_forward_median_sharpe,
            "limit": min_walk_forward_median_sharpe,
            "actual": walk_forward_median_sharpe,
            "details": analysis.walk_forward["sharpe_ratio"].clone(),
        }));
        if let Some(limit) = min_walk_forward_median_calmar {
            gates.push(json!({
                "gate": "walk_forward_median_calmar",
                "passed": walk_forward_median_calmar >= limit,
                "limit": limit,
                "actual": walk_forward_median_calmar,
                "details": analysis.walk_forward["calmar_ratio"].clone(),
            }));
        }
        gates.push(json!({
            "gate": "bootstrap_positive_return_probability",
            "passed": bootstrap_positive_return_probability >= min_bootstrap_positive_return_probability,
            "limit": min_bootstrap_positive_return_probability,
            "actual": bootstrap_positive_return_probability,
            "details": analysis.bootstrap,
        }));
        gates.push(json!({
            "gate": "bootstrap_sharpe_p05",
            "passed": bootstrap_sharpe_p05 >= min_bootstrap_sharpe_p05,
            "limit": min_bootstrap_sharpe_p05,
            "actual": bootstrap_sharpe_p05,
            "details": analysis.bootstrap["sharpe_ratio"].clone(),
        }));
        if let Some(limit) = min_bootstrap_calmar_p05 {
            gates.push(json!({
                "gate": "bootstrap_calmar_p05",
                "passed": bootstrap_calmar_p05 >= limit,
                "limit": limit,
                "actual": bootstrap_calmar_p05,
                "details": analysis.bootstrap["calmar_ratio"].clone(),
            }));
        }
        if let Some(limit) = max_bootstrap_drawdown_p95 {
            gates.push(json!({
                "gate": "bootstrap_drawdown_p95",
                "passed": bootstrap_drawdown_p95 <= limit,
                "limit": limit,
                "actual": bootstrap_drawdown_p95,
                "details": analysis.bootstrap["max_drawdown"].clone(),
            }));
        }
        gates.push(json!({
            "gate": "market_scenario_coverage",
            "passed": market_scenario_count >= min_market_scenarios,
            "limit": min_market_scenarios,
            "actual": market_scenario_count,
            "details": analysis.market_scenarios,
        }));
    }
    let any_failed = gates.iter().any(|gate| gate["passed"] == false);
    let has_review_only_failure = gates
        .iter()
        .any(|gate| gate["gate"] == "score_gap_vs_runner_up" && gate["passed"] == false)
        && gates
            .iter()
            .filter(|gate| gate["gate"] != "score_gap_vs_runner_up")
            .all(|gate| gate["passed"] == true);
    let status = if !any_failed {
        "approved_candidate"
    } else if has_review_only_failure {
        "review_required"
    } else {
        "rejected"
    };

    RobustnessEvaluation {
        status: status.to_string(),
        gates: Value::Array(gates),
    }
}

fn build_robustness_failure_attribution(gates: &Value) -> Value {
    let gate_items = gates.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let failed_gates = gate_items
        .iter()
        .filter(|gate| gate["passed"] == false)
        .map(compact_gate_failure)
        .collect::<Vec<_>>();
    let walk_forward_windows = extract_walk_forward_windows(gate_items);
    let worst_walk_forward_windows = rank_weak_walk_forward_windows(&walk_forward_windows, 5);
    let weak_market_scenarios = rank_weak_market_scenarios(&walk_forward_windows, 5);
    let bootstrap_tail = extract_bootstrap_tail(gate_items);
    let primary_failure_modes = build_primary_failure_modes(&failed_gates);

    json!({
        "status": if failed_gates.is_empty() { "no_failed_gates" } else { "has_failed_gates" },
        "failed_gates": failed_gates,
        "primary_failure_modes": primary_failure_modes,
        "worst_walk_forward_windows": worst_walk_forward_windows,
        "weak_market_scenarios": weak_market_scenarios,
        "bootstrap_tail": bootstrap_tail,
    })
}

fn compact_gate_failure(gate: &Value) -> Value {
    json!({
        "gate": gate["gate"].clone(),
        "limit": gate.get("limit").cloned().unwrap_or(Value::Null),
        "actual": gate.get("actual").cloned().unwrap_or(Value::Null),
    })
}

fn build_primary_failure_modes(failed_gates: &[Value]) -> Vec<Value> {
    let mut modes = BTreeSet::new();
    for gate in failed_gates {
        match gate["gate"].as_str().unwrap_or_default() {
            "min_annual_return" => {
                modes.insert("annual_return_shortfall");
            }
            "min_excess_return" => {
                modes.insert("excess_return_shortfall");
            }
            "min_sharpe" => {
                modes.insert("sharpe_shortfall");
            }
            "min_sortino" => {
                modes.insert("sortino_shortfall");
            }
            "min_calmar" | "walk_forward_median_calmar" | "bootstrap_calmar_p05" => {
                modes.insert("calmar_shortfall");
            }
            "min_profit_factor" => {
                modes.insert("profit_factor_shortfall");
            }
            "max_drawdown" => {
                modes.insert("drawdown_excess");
            }
            "max_drawdown_duration_days" => {
                modes.insert("drawdown_duration_excess");
            }
            "bootstrap_drawdown_p95" => {
                modes.insert("bootstrap_drawdown_tail_risk");
            }
            "walk_forward_positive_excess_ratio" | "walk_forward_min_window_count" => {
                modes.insert("walk_forward_instability");
            }
            "walk_forward_positive_annual_return_ratio" | "walk_forward_median_sharpe" => {
                modes.insert("overfit_window_concentration");
            }
            "bootstrap_positive_return_probability" => {
                modes.insert("bootstrap_tail_risk");
            }
            "bootstrap_sharpe_p05" => {
                modes.insert("bootstrap_sharpe_tail_risk");
            }
            "market_scenario_coverage" => {
                modes.insert("market_scenario_coverage_gap");
            }
            "score_gap_vs_runner_up" => {
                modes.insert("runner_up_gap_too_small");
            }
            "no_hard_constraint_violations" => {
                modes.insert("hard_constraint_violation");
            }
            "min_trade_count" => {
                modes.insert("insufficient_trade_sample");
            }
            _ => {
                modes.insert("other_gate_failure");
            }
        }
    }
    modes
        .into_iter()
        .map(|mode| Value::String(mode.to_string()))
        .collect()
}

fn extract_walk_forward_windows(gate_items: &[Value]) -> Vec<Value> {
    gate_items
        .iter()
        .find(|gate| gate["gate"] == "walk_forward_min_window_count")
        .and_then(|gate| gate["details"]["windows"].as_array())
        .cloned()
        .unwrap_or_default()
}

fn rank_weak_walk_forward_windows(windows: &[Value], limit: usize) -> Vec<Value> {
    let mut ranked = windows.to_vec();
    ranked.sort_by(|left, right| {
        weak_window_score(right)
            .partial_cmp(&weak_window_score(left))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(limit);
    ranked
        .into_iter()
        .map(|window| {
            json!({
                "window_index": window["window_index"].clone(),
                "start_date": window["start_date"].clone(),
                "end_date": window["end_date"].clone(),
                "scenario": window["scenario"].clone(),
                "metrics": window["metrics"].clone(),
                "weakness_score": weak_window_score(&window),
            })
        })
        .collect()
}

fn weak_window_score(window: &Value) -> f64 {
    let annual_return = metric_f64(window, "annual_return").unwrap_or(0.0);
    let excess_return = metric_f64(window, "excess_return").unwrap_or(0.0);
    let sharpe = metric_f64(window, "sharpe_ratio").unwrap_or(0.0);
    let sortino = metric_f64(window, "sortino_ratio").unwrap_or(0.0);
    let drawdown = metric_f64(window, "max_drawdown").unwrap_or(0.0);

    positive_shortfall(0.15, annual_return) * 2.0
        + positive_shortfall(0.0, excess_return) * 3.0
        + positive_shortfall(1.0, sharpe)
        + positive_shortfall(1.5, sortino) * 0.5
        + drawdown
}

fn rank_weak_market_scenarios(windows: &[Value], limit: usize) -> Vec<Value> {
    let mut by_scenario: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for window in windows {
        let scenario = window["scenario"].as_str().unwrap_or("unknown").to_string();
        by_scenario.entry(scenario).or_default().push(window);
    }
    let mut scenarios = by_scenario
        .into_iter()
        .map(|(scenario, windows)| scenario_weakness_summary(&scenario, &windows))
        .collect::<Vec<_>>();
    scenarios.sort_by(|left, right| {
        right["weakness_score"]
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&left["weakness_score"].as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scenarios.truncate(limit);
    scenarios
}

fn scenario_weakness_summary(scenario: &str, windows: &[&Value]) -> Value {
    let count = windows.len().max(1);
    let avg_annual_return = average_metric(windows, "annual_return");
    let avg_excess_return = average_metric(windows, "excess_return");
    let avg_sharpe = average_metric(windows, "sharpe_ratio");
    let avg_sortino = average_metric(windows, "sortino_ratio");
    let worst_drawdown = windows
        .iter()
        .filter_map(|window| metric_f64(window, "max_drawdown"))
        .fold(0.0_f64, f64::max);
    let weakness_score = windows
        .iter()
        .map(|window| weak_window_score(window))
        .sum::<f64>()
        / count as f64;
    let negative_excess_window_count = windows
        .iter()
        .filter(|window| metric_f64(window, "excess_return").unwrap_or(0.0) < 0.0)
        .count();

    json!({
        "scenario": scenario,
        "window_count": windows.len(),
        "negative_excess_window_count": negative_excess_window_count,
        "avg_annual_return": avg_annual_return,
        "avg_excess_return": avg_excess_return,
        "avg_sharpe_ratio": avg_sharpe,
        "avg_sortino_ratio": avg_sortino,
        "worst_drawdown": worst_drawdown,
        "weakness_score": weakness_score,
    })
}

fn average_metric(windows: &[&Value], metric: &str) -> f64 {
    let values = windows
        .iter()
        .filter_map(|window| metric_f64(window, metric))
        .collect::<Vec<_>>();
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn extract_bootstrap_tail(gate_items: &[Value]) -> Value {
    gate_items
        .iter()
        .find(|gate| gate["gate"] == "bootstrap_positive_return_probability")
        .map(|gate| {
            json!({
                "positive_return_probability": gate["details"]["positive_return_probability"].clone(),
                "total_return": gate["details"]["total_return"].clone(),
                "sharpe_ratio": gate["details"]["sharpe_ratio"].clone(),
                "sortino_ratio": gate["details"]["sortino_ratio"].clone(),
                "calmar_ratio": gate["details"]["calmar_ratio"].clone(),
                "max_drawdown": gate["details"]["max_drawdown"].clone(),
            })
        })
        .unwrap_or_else(|| json!({}))
}

fn metric_f64(window: &Value, metric: &str) -> Option<f64> {
    value_as_f64(window.get("metrics")?.get(metric)?)
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn positive_shortfall(limit: f64, actual: f64) -> f64 {
    (limit - actual).max(0.0)
}

fn decimal_from_json(value: Option<&Value>) -> Option<Decimal> {
    match value {
        Some(Value::Number(number)) => number.as_f64().and_then(Decimal::from_f64_retain),
        Some(Value::String(value)) => value.parse().ok(),
        _ => None,
    }
}

fn constraint_decimal(constraints: Option<&Value>, name: &str) -> Option<Decimal> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_f64)
        .and_then(Decimal::from_f64_retain)
}

fn constraint_i64(constraints: Option<&Value>, name: &str) -> Option<i64> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_i64)
}

fn constraint_f64(constraints: Option<&Value>, name: &str) -> Option<f64> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_f64)
}

fn constraint_bool(constraints: Option<&Value>, name: &str) -> Option<bool> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_bool)
}

fn constraint_str<'a>(constraints: Option<&'a Value>, name: &str) -> Option<&'a str> {
    constraints
        .and_then(|value| value.get(name))
        .and_then(Value::as_str)
}

async fn mark_trial_running(db: &sqlx::PgPool, trial_id: &str) -> Result<(), String> {
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

async fn mark_trial_completed(
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

async fn mark_trial_failed(
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

async fn refresh_task_progress(db: &sqlx::PgPool, task_id: &str) -> Result<Option<String>, String> {
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

fn sample_parameter(name: &str, spec: &Value, rng: &mut DeterministicRng) -> Result<Value, String> {
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

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
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

    fn gen_usize(&mut self, upper_exclusive: usize) -> usize {
        ((self.next_u64() >> 32) as usize) % upper_exclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn report_trial(
        trial_id: &str,
        trial_index: i32,
        parameters: Value,
        metrics: Value,
    ) -> CompletedTrialSnapshot {
        CompletedTrialSnapshot {
            trial_id: trial_id.to_string(),
            trial_index,
            backtest_task_id: Some(format!("bt-{trial_id}")),
            score: None,
            metrics,
            metric_sources: json!({}),
            missing_elite_metrics: json!([]),
            constraint_violations: json!([]),
            parameters,
        }
    }

    #[test]
    fn random_search_trial_generation_is_seed_deterministic() {
        let search_space = json!({
            "top_n": {"type": "int", "min": 5, "max": 8},
            "rebalance": {"type": "choice", "values": ["5", "20"]},
            "max_position_pct": {"type": "float", "min": 0.05, "max": 0.10}
        });

        let first = generate_trial_parameters(&search_space, 42, 3).expect("first generation");
        let second = generate_trial_parameters(&search_space, 42, 3).expect("second generation");

        assert_eq!(first, second);
        assert_eq!(first.len(), 3);
        assert!(first.iter().all(|params| params.get("top_n").is_some()));
    }

    #[test]
    fn oos_walk_forward_plan_uses_non_overlapping_train_and_test_windows() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(16),
            search_profile: Some("phase7_db".to_string()),
            trial_batch_limit: Some(4),
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: Some(4),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(1),
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan = build_oos_discovery_plan(&req).expect("oos plan");

        assert_eq!(plan.validation_mode, "walk_forward");
        assert_eq!(plan.windows.len(), 2);
        assert!(plan.windows.iter().all(|window| {
            window.train_start <= window.train_end && window.train_end < window.test_start
        }));
        assert_eq!(
            plan.windows[1].train_start - plan.windows[0].train_start,
            Duration::days(365)
        );
    }

    #[test]
    fn oos_holdout_plan_uses_single_in_sample_out_of_sample_split() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20210101"
            })),
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: Some("holdout_80_20".to_string()),
            in_sample_ratio: Some(0.80),
            include_partial_last_window: None,
            plan_only: Some(true),
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan = build_oos_discovery_plan(&req).expect("holdout plan");

        assert_eq!(plan.windows.len(), 1);
        assert_eq!(plan.windows[0].validation_mode, "holdout_80_20");
        assert!(plan.windows[0].train_end < plan.windows[0].test_start);
        assert_eq!(plan.windows[0].test_end, plan.end_date);
    }

    #[test]
    fn stitched_oos_curve_compounds_only_test_window_returns() {
        let first = vec![
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                portfolio_value: 100.0,
                benchmark_value: Some(100.0),
            },
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2020, 1, 2).unwrap(),
                portfolio_value: 110.0,
                benchmark_value: Some(105.0),
            },
        ];
        let second = vec![
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
                portfolio_value: 200.0,
                benchmark_value: Some(100.0),
            },
            RobustnessDailyPoint {
                trade_date: NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
                portfolio_value: 220.0,
                benchmark_value: Some(110.0),
            },
        ];
        let mut stitched = Vec::new();

        append_stitched_oos_points(&mut stitched, &first);
        append_stitched_oos_points(&mut stitched, &second);

        assert_eq!(stitched.len(), 3);
        assert!((stitched.last().unwrap().portfolio_value - 1.21).abs() < 1e-12);
    }

    #[test]
    fn oos_execution_mode_accepts_background_aliases() {
        assert_eq!(
            normalize_oos_execution_mode(Some("background")).unwrap(),
            "background"
        );
        assert_eq!(
            normalize_oos_execution_mode(Some("async")).unwrap(),
            "background"
        );
        assert_eq!(normalize_oos_execution_mode(None).unwrap(), "inline");
        assert!(normalize_oos_execution_mode(Some("manual")).is_err());
    }

    #[test]
    fn oos_train_execution_defaults_to_shared_cache_even_when_concurrency_requested() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        assert_eq!(
            normalize_oos_train_cache_mode(None).unwrap(),
            "shared_window"
        );
        let policy =
            resolve_oos_train_execution_policy(&req, &LocalResourcePlan::for_machine(10, 32))
                .unwrap();
        assert_eq!(policy.trial_concurrency, 1);

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));
        assert_eq!(config["requested_trial_concurrency"], json!(4));
        assert_eq!(config["train_trial_concurrency"], json!(1));
        assert_eq!(config["train_cache_mode"], json!("shared_window"));
    }

    #[test]
    fn oos_train_execution_allows_explicit_isolated_parallel_cache() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: Some("per_trial_isolated".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        assert_eq!(
            normalize_oos_train_cache_mode(Some("isolated")).unwrap(),
            "per_trial_isolated"
        );
        let policy =
            resolve_oos_train_execution_policy(&req, &LocalResourcePlan::for_machine(10, 32))
                .unwrap();
        assert_eq!(policy.trial_concurrency, 4);

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));
        assert_eq!(config["requested_trial_concurrency"], json!(4));
        assert_eq!(config["train_trial_concurrency"], json!(4));
        assert_eq!(config["train_cache_mode"], json!("per_trial_isolated"));
    }

    #[test]
    fn oos_train_execution_auto_cache_prefers_shared_for_small_windows() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(8),
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: Some("auto".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let resource_plan = LocalResourcePlan::for_machine(10, 32);

        let policy = resolve_oos_train_execution_policy(&req, &resource_plan).unwrap();

        assert_eq!(policy.cache_mode, "shared_window");
        assert_eq!(policy.trial_concurrency, 1);
        assert_eq!(policy.requested_cache_mode, "auto");
    }

    #[test]
    fn oos_train_execution_auto_cache_uses_isolated_parallel_for_large_windows() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(96),
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(8),
            train_cache_mode: Some("auto".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let resource_plan = LocalResourcePlan::for_machine(10, 32);

        let policy = resolve_oos_train_execution_policy(&req, &resource_plan).unwrap();

        assert_eq!(policy.cache_mode, "per_trial_isolated");
        assert_eq!(policy.trial_concurrency, 8);
        assert_eq!(policy.requested_cache_mode, "auto");
    }

    #[test]
    fn oos_train_execution_accepts_explicit_shared_run_cache() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(8),
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: Some(4),
            train_cache_mode: Some("shared_run".to_string()),
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let resource_plan = LocalResourcePlan::for_machine(10, 32);

        assert_eq!(
            normalize_oos_train_cache_mode(Some("run_shared")).unwrap(),
            "shared_run"
        );
        let policy = resolve_oos_train_execution_policy(&req, &resource_plan).unwrap();

        assert_eq!(policy.requested_cache_mode, "shared_run");
        assert_eq!(policy.cache_mode, "shared_run");
        assert_eq!(policy.trial_concurrency, 1);

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));
        assert_eq!(config["requested_trial_concurrency"], json!(4));
        assert_eq!(config["trial_concurrency"], json!(1));
        assert_eq!(config["train_trial_concurrency"], json!(1));
        assert_eq!(config["train_cache_mode"], json!("shared_run"));
    }

    #[test]
    fn oos_train_batch_summary_labels_shared_run_cache_scope() {
        let policy = OosTrainExecutionPolicy {
            requested_cache_mode: "shared_run",
            cache_mode: "shared_run",
            requested_trial_concurrency: 4,
            trial_concurrency: 1,
        };
        let batch = annotate_oos_train_batch_cache_mode(
            json!({
                "executed": 2,
                "trial_concurrency": 1,
                "cache_mode": "shared_window",
            }),
            &policy,
        );

        assert_eq!(batch["cache_mode"], json!("shared_run"));
        assert_eq!(batch["cache_scope"], json!("run"));
        assert_eq!(batch["requested_cache_mode"], json!("shared_run"));
    }

    #[test]
    fn phase7_oos_profile_comparison_plan_defaults_to_ec_eu_ev_with_auto_cache() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(8),
            search_profile: None,
            trial_batch_limit: Some(2),
            trial_concurrency: Some(4),
            train_cache_mode: None,
            max_batches_per_window: Some(1),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: None,
            execution_mode: Some("background".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(3),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan =
            build_phase7_oos_profile_comparison_plan(&req, None, None).expect("comparison plan");

        let profiles = plan["profiles"].as_array().expect("profiles");
        assert_eq!(profiles.len(), 3);
        assert_eq!(profiles[0]["search_profile"], json!("phase7_ec"));
        assert_eq!(profiles[1]["search_profile"], json!("phase7_eu"));
        assert_eq!(profiles[2]["search_profile"], json!("phase7_ev"));
        assert!(profiles.iter().all(|profile| {
            profile["request"]["plan_only"] == json!(true)
                && profile["request"]["execution_mode"] == json!("inline")
                && profile["request"]["train_cache_mode"] == json!("auto")
                && profile["plan"]["window_count"] == json!(2)
                && profile["config"]["train_cache_mode"] == json!("shared_window")
        }));
    }

    #[test]
    fn phase7_oos_profile_comparison_launch_requests_are_bounded_background_smokes() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(64),
            search_profile: None,
            trial_batch_limit: Some(64),
            trial_concurrency: Some(8),
            train_cache_mode: None,
            max_batches_per_window: Some(16),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: Some("inline".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(20),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let requests = build_phase7_oos_profile_comparison_launch_requests(&req, None, None)
            .expect("launch reqs");

        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].search_profile.as_deref(), Some("phase7_ec"));
        assert_eq!(requests[1].search_profile.as_deref(), Some("phase7_eu"));
        assert_eq!(requests[2].search_profile.as_deref(), Some("phase7_ev"));
        assert!(requests.iter().all(|request| {
            request.plan_only == Some(false)
                && request.execution_mode.as_deref() == Some("background")
                && request.train_cache_mode.as_deref() == Some("auto")
                && request.max_trials_per_window == Some(8)
                && request.trial_batch_limit == Some(8)
                && request.max_batches_per_window == Some(1)
                && request.oos_top_n == Some(3)
        }));
    }

    #[test]
    fn phase7_oos_profile_comparison_plan_can_build_raw_stats_cache_pairs() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(64),
            search_profile: None,
            trial_batch_limit: Some(64),
            trial_concurrency: Some(8),
            train_cache_mode: None,
            max_batches_per_window: Some(16),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: Some("inline".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(20),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let plan = build_phase7_oos_profile_comparison_plan(
            &req,
            Some(vec!["phase7_er".to_string()]),
            Some(true),
        )
        .expect("paired comparison plan");

        let profiles = plan["profiles"].as_array().expect("profiles");
        assert_eq!(plan["profile_count"], json!(1));
        assert_eq!(plan["launch_count"], json!(2));
        assert_eq!(profiles.len(), 2);
        assert_eq!(
            plan["return_risk_cache_comparison"]["report_endpoint"],
            json!("/api/v1/quant/experiments/return-risk-cache-economics/report")
        );
        assert_eq!(profiles[0]["search_profile"], json!("phase7_er"));
        assert_eq!(
            profiles[0]["return_risk_feature_cache_mode"],
            json!("raw_matrix")
        );
        assert_eq!(
            profiles[0]["request"]["return_risk_feature_cache_mode"],
            json!("raw_matrix")
        );
        assert_eq!(
            profiles[1]["return_risk_feature_cache_mode"],
            json!("stats_matrix_experimental")
        );
        assert_eq!(
            profiles[1]["request"]["return_risk_feature_cache_mode"],
            json!("stats_matrix_experimental")
        );
    }

    #[test]
    fn phase7_oos_profile_comparison_launch_requests_can_build_raw_stats_cache_pairs() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(64),
            search_profile: None,
            trial_batch_limit: Some(64),
            trial_concurrency: Some(8),
            train_cache_mode: None,
            max_batches_per_window: Some(16),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: Some("inline".to_string()),
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: Some(20),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let requests = build_phase7_oos_profile_comparison_launch_requests(
            &req,
            Some(vec!["phase7_er".to_string()]),
            Some(true),
        )
        .expect("paired launch reqs");

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].search_profile.as_deref(), Some("phase7_er"));
        assert_eq!(requests[0].plan_only, Some(false));
        assert_eq!(requests[0].execution_mode.as_deref(), Some("background"));
        assert_eq!(
            requests[0]
                .backtest_template
                .as_ref()
                .and_then(|template| template.get("return_risk_feature_cache_mode")),
            Some(&json!("raw_matrix"))
        );
        assert_eq!(
            requests[1]
                .backtest_template
                .as_ref()
                .and_then(|template| template.get("return_risk_feature_cache_mode")),
            Some(&json!("stats_matrix_experimental"))
        );
    }

    #[test]
    fn oos_gate_report_rejects_weak_cost_capacity_perturbations() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20200101",
                "end_date": "20250101",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials_per_window: Some(16),
            search_profile: Some("phase7_db".to_string()),
            trial_batch_limit: Some(4),
            trial_concurrency: Some(2),
            train_cache_mode: None,
            max_batches_per_window: Some(4),
            train_window_days: Some(365 * 3),
            test_window_days: Some(365),
            step_days: Some(365),
            validation_mode: Some("walk_forward".to_string()),
            in_sample_ratio: None,
            include_partial_last_window: Some(false),
            plan_only: Some(true),
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: Some(1.2),
            min_positive_oos_window_ratio: Some(0.5),
            min_oos_window_count: Some(1),
            oos_top_n: Some(1),
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: Some(vec![OosCostCapacityPerturbationRequest {
                name: Some("cost_up".to_string()),
                cost_multiplier: Some(1.5),
                slippage_bps: Some(0.0002),
                impact_cost_coefficient: None,
                max_participation_rate: None,
                capacity_penalty_strength: None,
            }]),
            min_cost_capacity_perturbation_pass_ratio: Some(0.75),
            min_perturbed_oos_calmar: Some(1.0),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };
        let plan = build_oos_discovery_plan(&req).expect("plan");
        let window_results = vec![
            json!({
                "oos_metrics": {"annual_return_pct": 0.10},
                "train_robustness": {"status": "approved_candidate"},
                "cost_capacity_perturbations": [
                    {"name": "cost_up", "passed": true}
                ]
            }),
            json!({
                "oos_metrics": {"annual_return_pct": 0.08},
                "train_robustness": {"status": "approved_candidate"},
                "cost_capacity_perturbations": [
                    {"name": "cost_up", "passed": false}
                ]
            }),
        ];

        let gates = build_oos_gate_report(
            &req,
            &plan,
            &window_results,
            &json!({"calmar_ratio": 1.4}),
            &resolve_oos_final_promotion_gate_policy(&req, &plan.validation_mode),
        );

        let perturbation_gate = gates
            .as_array()
            .unwrap()
            .iter()
            .find(|gate| gate["gate"] == "cost_capacity_perturbation_pass_ratio")
            .expect("cost/capacity gate");
        assert_eq!(perturbation_gate["passed"], json!(false));
        assert_eq!(perturbation_gate["actual"], json!(0.5));
    }

    #[test]
    fn train_selection_cost_capacity_gate_is_policy_scoped() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };
        let train_policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "min_train_cost_capacity_perturbation_pass_ratio": 0.67,
            "min_train_perturbed_calmar": 0.50,
            "max_train_perturbed_drawdown_pct": 0.45
        });

        let gate = train_cost_capacity_perturbation_gate_config(&req, &train_policy);

        assert!(gate.enabled);
        assert_eq!(gate.min_pass_ratio, 0.67);
        assert_eq!(gate.min_perturbed_calmar, 0.50);
        assert_eq!(gate.max_perturbed_drawdown_pct, 0.45);
    }

    #[test]
    fn train_cost_capacity_stress_aware_selection_defaults_on_when_gate_enabled() {
        let gate = OosCostCapacityGateConfig {
            enabled: true,
            min_pass_ratio: 0.80,
            min_perturbed_calmar: 1.2,
            max_perturbed_drawdown_pct: 0.35,
        };

        assert!(train_cost_capacity_stress_aware_selection_enabled(
            &json!({}),
            &gate
        ));
        assert!(!train_cost_capacity_stress_aware_selection_enabled(
            &json!({"enable_train_cost_capacity_stress_aware_selection": false}),
            &gate
        ));
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_capacity_stress_calmar_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "capacity_stress_calmar_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "capacity_stress_calmar_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_cash_drag_fill_gap_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "cash_drag_fill_gap_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "cash_drag_fill_gap_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_capacity_stress_return_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "capacity_stress_return_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "capacity_stress_return_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_stress_fill_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "stress_fill_objective_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "stress_fill_objective_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_prediction_confidence_stress_fill_objective() {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "prediction_confidence_stress_fill_objective_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "prediction_confidence_stress_fill_objective_score_v1"
        );
    }

    #[test]
    fn train_cost_capacity_score_profile_accepts_prediction_confidence_stress_fill_quality_objective(
    ) {
        let policy = json!({
            "enable_train_cost_capacity_perturbation_gate": true,
            "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1"
        });

        assert_eq!(
            train_cost_capacity_stress_score_profile(&policy),
            "prediction_confidence_stress_fill_quality_score_v1"
        );
    }

    #[test]
    fn cash_drag_fill_gap_score_prefers_filled_candidate_when_capacity_ties() {
        let candidate = |trial_id: &str,
                         final_cash_weight: Decimal,
                         target_gap: Decimal,
                         expired_count: u64|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return: Decimal::new(28, 2),
                    excess_return: Decimal::new(10, 2),
                    sharpe: Decimal::new(18, 1),
                    sortino: Decimal::new(28, 1),
                    max_drawdown: Decimal::new(12, 2),
                    final_cash_weight,
                    max_execution_target_gap: target_gap,
                    execution_schedule_expired_count: expired_count,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut cashized_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        cashized_summary.min_calmar = Decimal::new(150, 2);
        cashized_summary.avg_calmar = Decimal::new(180, 2);
        cashized_summary.avg_sharpe = Decimal::new(160, 2);
        cashized_summary.min_annual_return = Decimal::new(12, 2);
        cashized_summary.max_drawdown = Decimal::new(12, 2);
        cashized_summary.max_final_cash_weight = Decimal::new(55, 2);
        cashized_summary.avg_final_cash_weight = Decimal::new(42, 2);
        cashized_summary.max_execution_target_gap = Decimal::new(18, 2);
        cashized_summary.total_execution_schedule_expired_count = 5;
        let mut filled_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        filled_summary.min_calmar = cashized_summary.min_calmar;
        filled_summary.avg_calmar = cashized_summary.avg_calmar;
        filled_summary.avg_sharpe = cashized_summary.avg_sharpe;
        filled_summary.min_annual_return = cashized_summary.min_annual_return;
        filled_summary.max_drawdown = cashized_summary.max_drawdown;
        filled_summary.max_final_cash_weight = Decimal::new(8, 2);
        filled_summary.avg_final_cash_weight = Decimal::new(5, 2);
        filled_summary.max_execution_target_gap = Decimal::new(3, 2);
        filled_summary.total_execution_schedule_expired_count = 0;
        let policy = json!({
            "train_stress_score_profile": "cash_drag_fill_gap_score_v1",
            "min_train_perturbed_calmar": 1.2,
            "max_train_final_cash_weight_pct": 0.20,
            "max_train_execution_target_gap_pct": 0.08
        });

        let cashized_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("cashized", Decimal::new(60, 2), Decimal::new(20, 2), 4),
            &cashized_summary,
            &policy,
        );
        let filled_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("filled", Decimal::new(6, 2), Decimal::new(3, 2), 0),
            &filled_summary,
            &policy,
        );

        assert!(
            filled_score > cashized_score,
            "filled_score={filled_score}, cashized_score={cashized_score}"
        );
    }

    #[test]
    fn train_cash_drag_fill_gap_gate_rejects_cashized_train_candidate() {
        let candidate = DiscoveryCandidate {
            trial_id: "cashized".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(28, 2),
                sharpe: Decimal::new(18, 1),
                final_cash_weight: Decimal::new(35, 2),
                max_execution_target_gap: Decimal::new(18, 2),
                execution_schedule_expired_count: 2,
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.max_final_cash_weight = Decimal::new(60, 2);
        summary.max_execution_target_gap = Decimal::new(21, 2);
        summary.max_execution_schedule_expired_count = 4;
        let policy = json!({
            "max_train_final_cash_weight_pct": 0.20,
            "max_train_execution_target_gap_pct": 0.08,
            "max_train_execution_schedule_expired_count": 0
        });

        let (robustness, passed) = attach_train_cash_drag_fill_gap_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], "rejected");
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| gate["gate"] == "train_final_cash_weight"
                && gate["passed"] == false
                && gate["actual"] == json!("0.60")));
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| gate["gate"] == "train_execution_target_gap" && gate["passed"] == false));
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |gate| gate["gate"] == "train_execution_schedule_expired_count"
                    && gate["passed"] == false
            ));
    }

    #[test]
    fn train_fill_ratio_gate_allows_intentional_cash_when_target_is_filled() {
        let candidate = DiscoveryCandidate {
            trial_id: "intentional_cash".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(18, 2),
                sharpe: Decimal::new(12, 1),
                final_cash_weight: Decimal::new(65, 2),
                final_target_gross_exposure: Decimal::new(35, 2),
                final_actual_gross_exposure: Decimal::new(35, 2),
                final_unfilled_target_gap: Decimal::ZERO,
                final_execution_fill_ratio: Decimal::ONE,
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.max_final_cash_weight = Decimal::new(70, 2);
        summary.max_final_unfilled_target_gap = Decimal::new(1, 2);
        summary.min_final_execution_fill_ratio = Decimal::new(98, 2);
        let policy = json!({
            "max_train_final_unfilled_target_gap_pct": 0.08,
            "min_train_final_execution_fill_ratio": 0.90
        });

        let (robustness, passed) = attach_train_cash_drag_fill_gap_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(passed);
        assert_eq!(robustness["status"], "approved_candidate");
        let gates = robustness["gate_results"].as_array().unwrap();
        assert!(gates.iter().any(
            |gate| gate["gate"] == "train_final_unfilled_target_gap" && gate["passed"] == true
        ));
        assert!(gates.iter().any(
            |gate| gate["gate"] == "train_final_execution_fill_ratio" && gate["passed"] == true
        ));
        assert!(!gates
            .iter()
            .any(|gate| gate["gate"] == "train_final_cash_weight"));
    }

    #[test]
    fn train_execution_quality_gate_rejects_zero_trade_low_exposure_candidate() {
        let candidate = DiscoveryCandidate {
            trial_id: "empty-exposure".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::ZERO,
                sharpe: Decimal::ZERO,
                num_trades: 0,
                final_cash_weight: Decimal::new(90, 2),
                final_actual_gross_exposure: Decimal::new(10, 2),
                final_unfilled_target_gap: Decimal::new(2, 2),
                final_execution_fill_ratio: Decimal::new(95, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_num_trades = 0;
        summary.max_final_cash_weight = Decimal::new(92, 2);
        summary.min_final_actual_gross_exposure = Decimal::new(8, 2);
        let policy = json!({
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.25,
            "max_train_final_cash_weight_pct": 0.75,
            "max_train_final_unfilled_target_gap_pct": 0.08,
            "min_train_final_execution_fill_ratio": 0.90
        });

        let (robustness, passed) = attach_train_cash_drag_fill_gap_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], "rejected");
        let gates = robustness["gate_results"].as_array().unwrap();
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "train_trade_count" && gate["passed"] == false));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "train_final_actual_gross_exposure"
                && gate["passed"] == false
                && gate["actual"] == json!("0.08")));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "train_final_cash_weight" && gate["passed"] == false));
    }

    #[test]
    fn capacity_stress_return_score_penalizes_low_trade_low_exposure_candidates() {
        let candidate = |trial_id: &str,
                         num_trades: u64,
                         final_cash_weight: Decimal,
                         final_actual_gross_exposure: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return: Decimal::new(20, 2),
                    excess_return: Decimal::new(8, 2),
                    sharpe: Decimal::new(12, 1),
                    sortino: Decimal::new(18, 1),
                    max_drawdown: Decimal::new(20, 2),
                    num_trades,
                    final_cash_weight,
                    final_actual_gross_exposure,
                    final_unfilled_target_gap: Decimal::new(2, 2),
                    final_execution_fill_ratio: Decimal::new(95, 2),
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut low_exposure_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        low_exposure_summary.min_calmar = Decimal::new(150, 2);
        low_exposure_summary.avg_calmar = Decimal::new(170, 2);
        low_exposure_summary.avg_sharpe = Decimal::new(120, 2);
        low_exposure_summary.min_annual_return = Decimal::new(8, 2);
        low_exposure_summary.max_drawdown = Decimal::new(14, 2);
        low_exposure_summary.min_num_trades = 0;
        low_exposure_summary.max_final_cash_weight = Decimal::new(90, 2);
        low_exposure_summary.min_final_actual_gross_exposure = Decimal::new(10, 2);
        let mut tradable_summary = low_exposure_summary.clone();
        tradable_summary.min_num_trades = 260;
        tradable_summary.max_final_cash_weight = Decimal::new(28, 2);
        tradable_summary.min_final_actual_gross_exposure = Decimal::new(70, 2);
        let policy = json!({
            "train_stress_score_profile": "capacity_stress_return_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.25,
            "max_train_final_cash_weight_pct": 0.75,
            "max_train_final_unfilled_target_gap_pct": 0.08,
            "min_train_final_execution_fill_ratio": 0.90
        });

        let low_exposure_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("low-exposure", 0, Decimal::new(90, 2), Decimal::new(10, 2)),
            &low_exposure_summary,
            &policy,
        );
        let tradable_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("tradable", 260, Decimal::new(28, 2), Decimal::new(72, 2)),
            &tradable_summary,
            &policy,
        );

        assert!(
            tradable_score > low_exposure_score,
            "tradable_score={tradable_score}, low_exposure_score={low_exposure_score}"
        );
    }

    #[test]
    fn stress_fill_objective_score_prefers_filled_exposure_over_cashized_return() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         num_trades: u64,
                         final_cash_weight: Decimal,
                         final_actual_gross_exposure: Decimal,
                         final_unfilled_target_gap: Decimal,
                         final_execution_fill_ratio: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(8, 2),
                    sharpe: Decimal::new(12, 1),
                    sortino: Decimal::new(18, 1),
                    max_drawdown: Decimal::new(20, 2),
                    num_trades,
                    final_cash_weight,
                    final_actual_gross_exposure,
                    final_unfilled_target_gap,
                    final_execution_fill_ratio,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut cashized_summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        cashized_summary.min_calmar = Decimal::new(160, 2);
        cashized_summary.avg_calmar = Decimal::new(180, 2);
        cashized_summary.avg_sharpe = Decimal::new(130, 2);
        cashized_summary.min_annual_return = Decimal::new(18, 2);
        cashized_summary.max_drawdown = Decimal::new(18, 2);
        cashized_summary.min_num_trades = 80;
        cashized_summary.max_final_cash_weight = Decimal::new(82, 2);
        cashized_summary.min_final_actual_gross_exposure = Decimal::new(18, 2);
        cashized_summary.max_final_unfilled_target_gap = Decimal::new(18, 2);
        cashized_summary.min_final_execution_fill_ratio = Decimal::new(78, 2);
        let mut filled_summary = cashized_summary.clone();
        filled_summary.min_annual_return = Decimal::new(11, 2);
        filled_summary.min_num_trades = 240;
        filled_summary.max_final_cash_weight = Decimal::new(22, 2);
        filled_summary.min_final_actual_gross_exposure = Decimal::new(78, 2);
        filled_summary.max_final_unfilled_target_gap = Decimal::new(2, 2);
        filled_summary.min_final_execution_fill_ratio = Decimal::new(98, 2);
        let policy = json!({
            "train_stress_score_profile": "stress_fill_objective_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let cashized_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "cashized-high-return",
                Decimal::new(30, 2),
                80,
                Decimal::new(82, 2),
                Decimal::new(18, 2),
                Decimal::new(18, 2),
                Decimal::new(78, 2),
            ),
            &cashized_summary,
            &policy,
        );
        let filled_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "filled-lower-return",
                Decimal::new(18, 2),
                240,
                Decimal::new(22, 2),
                Decimal::new(78, 2),
                Decimal::new(2, 2),
                Decimal::new(98, 2),
            ),
            &filled_summary,
            &policy,
        );

        assert!(
            filled_score > cashized_score,
            "filled_score={filled_score}, cashized_score={cashized_score}"
        );
    }

    #[test]
    fn prediction_confidence_stress_fill_objective_prefers_positive_prediction_gate() {
        let candidate = |trial_id: &str, parameters: Value| -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return: Decimal::new(16, 2),
                    excess_return: Decimal::new(8, 2),
                    sharpe: Decimal::new(12, 1),
                    sortino: Decimal::new(19, 1),
                    max_drawdown: Decimal::new(18, 2),
                    num_trades: 240,
                    final_cash_weight: Decimal::new(24, 2),
                    final_actual_gross_exposure: Decimal::new(76, 2),
                    final_unfilled_target_gap: Decimal::new(2, 2),
                    final_execution_fill_ratio: Decimal::new(98, 2),
                    ..CandidateMetrics::default()
                },
                parameters,
            }
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_calmar = Decimal::new(150, 2);
        summary.avg_calmar = Decimal::new(180, 2);
        summary.avg_sharpe = Decimal::new(130, 2);
        summary.min_annual_return = Decimal::new(10, 2);
        summary.max_drawdown = Decimal::new(18, 2);
        summary.min_num_trades = 220;
        summary.max_final_cash_weight = Decimal::new(26, 2);
        summary.min_final_actual_gross_exposure = Decimal::new(74, 2);
        summary.max_final_unfilled_target_gap = Decimal::new(3, 2);
        summary.min_final_execution_fill_ratio = Decimal::new(97, 2);
        let policy = json!({
            "train_stress_score_profile": "prediction_confidence_stress_fill_objective_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_trade_count": 50,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let unconfirmed_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate("unconfirmed", json!({})),
            &summary,
            &policy,
        );
        let confirmed_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "positive-confirmed",
                json!({
                    "prediction_confidence_gate_profile": "train_positive_raw_score_gate_v1",
                    "prediction_min_score": "0.01",
                    "prediction_min_percentile": "0.30",
                    "prediction_set_override_source": "train_window_ml_internal"
                }),
            ),
            &summary,
            &policy,
        );

        assert!(
            confirmed_score > unconfirmed_score,
            "confirmed_score={confirmed_score}, unconfirmed_score={unconfirmed_score}"
        );
    }

    #[test]
    fn prediction_confidence_stress_fill_quality_objective_prefers_left_tail_resilience() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         sharpe: Decimal,
                         sortino: Decimal,
                         max_drawdown: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(8, 2),
                    sharpe,
                    sortino,
                    max_drawdown,
                    num_trades: 260,
                    final_cash_weight: Decimal::new(24, 2),
                    final_actual_gross_exposure: Decimal::new(76, 2),
                    final_unfilled_target_gap: Decimal::new(2, 2),
                    final_execution_fill_ratio: Decimal::new(98, 2),
                    ..CandidateMetrics::default()
                },
                parameters: json!({
                    "prediction_confidence_gate_profile": "train_positive_raw_score_gate_v1",
                    "prediction_min_score": "0.01",
                    "prediction_min_percentile": "0.35",
                    "prediction_set_override_source": "train_window_ml_internal"
                }),
            }
        };
        let mut high_return_fragile = cost_capacity_perturbation_summary_from_counts(2, 3);
        high_return_fragile.min_calmar = Decimal::new(45, 2);
        high_return_fragile.avg_calmar = Decimal::new(80, 2);
        high_return_fragile.avg_sharpe = Decimal::new(40, 2);
        high_return_fragile.min_annual_return = Decimal::new(-3, 2);
        high_return_fragile.max_drawdown = Decimal::new(24, 2);
        high_return_fragile.min_num_trades = 240;
        high_return_fragile.max_final_cash_weight = Decimal::new(26, 2);
        high_return_fragile.min_final_actual_gross_exposure = Decimal::new(74, 2);
        high_return_fragile.max_final_unfilled_target_gap = Decimal::new(3, 2);
        high_return_fragile.min_final_execution_fill_ratio = Decimal::new(97, 2);
        let mut lower_return_resilient = high_return_fragile.clone();
        lower_return_resilient.passed_count = 3;
        lower_return_resilient.pass_ratio_ppm = 1_000_000;
        lower_return_resilient.min_calmar = Decimal::new(140, 2);
        lower_return_resilient.avg_calmar = Decimal::new(165, 2);
        lower_return_resilient.avg_sharpe = Decimal::new(120, 2);
        lower_return_resilient.min_annual_return = Decimal::new(9, 2);
        lower_return_resilient.max_drawdown = Decimal::new(16, 2);
        let policy = json!({
            "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_perturbed_calmar": 1.2,
            "min_train_trade_count": 100,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let fragile_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "high-return-fragile",
                Decimal::new(34, 2),
                Decimal::new(18, 1),
                Decimal::new(22, 1),
                Decimal::new(12, 2),
            ),
            &high_return_fragile,
            &policy,
        );
        let resilient_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "lower-return-resilient",
                Decimal::new(18, 2),
                Decimal::new(12, 1),
                Decimal::new(17, 1),
                Decimal::new(18, 2),
            ),
            &lower_return_resilient,
            &policy,
        );

        assert!(
            resilient_score > fragile_score,
            "resilient_score={resilient_score}, fragile_score={fragile_score}"
        );
    }

    #[test]
    fn capacity_stress_calmar_score_prefers_capacity_resilience_when_pass_ratio_ties() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         sharpe: Decimal,
                         max_drawdown: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(10, 2),
                    sharpe,
                    sortino: Decimal::new(18, 1),
                    max_drawdown,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut high_return_fragile = cost_capacity_perturbation_summary_from_counts(2, 3);
        high_return_fragile.min_calmar = Decimal::new(30, 2);
        high_return_fragile.avg_calmar = Decimal::new(75, 2);
        high_return_fragile.avg_sharpe = Decimal::new(90, 2);
        high_return_fragile.min_annual_return = Decimal::new(-5, 2);
        high_return_fragile.max_drawdown = Decimal::new(30, 2);
        let mut lower_return_resilient = cost_capacity_perturbation_summary_from_counts(2, 3);
        lower_return_resilient.min_calmar = Decimal::new(140, 2);
        lower_return_resilient.avg_calmar = Decimal::new(180, 2);
        lower_return_resilient.avg_sharpe = Decimal::new(160, 2);
        lower_return_resilient.min_annual_return = Decimal::new(7, 2);
        lower_return_resilient.max_drawdown = Decimal::new(18, 2);
        let policy = json!({
            "train_stress_score_profile": "capacity_stress_calmar_score_v1",
            "min_train_perturbed_calmar": 1.2,
            "capacity_stress_target_annual_return": 0.05
        });

        let fragile_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "high-return-fragile",
                Decimal::new(36, 2),
                Decimal::new(24, 1),
                Decimal::new(10, 2),
            ),
            &high_return_fragile,
            &policy,
        );
        let resilient_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "lower-return-resilient",
                Decimal::new(22, 2),
                Decimal::new(16, 1),
                Decimal::new(16, 2),
            ),
            &lower_return_resilient,
            &policy,
        );

        assert!(
            resilient_score > fragile_score,
            "resilient_score={resilient_score}, fragile_score={fragile_score}"
        );
    }

    #[test]
    fn capacity_stress_return_score_prefers_perturbed_return_resilience() {
        let candidate = |trial_id: &str,
                         annual_return: Decimal,
                         sharpe: Decimal,
                         max_drawdown: Decimal|
         -> DiscoveryCandidate {
            DiscoveryCandidate {
                trial_id: trial_id.to_string(),
                backtest_task_id: None,
                score: Some(Decimal::new(50, 0)),
                candidate_type: CandidateType::ReviewRequired,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics {
                    annual_return,
                    excess_return: Decimal::new(10, 2),
                    sharpe,
                    sortino: Decimal::new(18, 1),
                    max_drawdown,
                    ..CandidateMetrics::default()
                },
                parameters: json!({}),
            }
        };
        let mut high_base_low_stress = cost_capacity_perturbation_summary_from_counts(2, 3);
        high_base_low_stress.min_calmar = Decimal::new(125, 2);
        high_base_low_stress.avg_calmar = Decimal::new(170, 2);
        high_base_low_stress.avg_sharpe = Decimal::new(160, 2);
        high_base_low_stress.min_annual_return = Decimal::new(2, 2);
        high_base_low_stress.max_drawdown = Decimal::new(16, 2);
        let mut lower_base_high_stress = cost_capacity_perturbation_summary_from_counts(2, 3);
        lower_base_high_stress.min_calmar = Decimal::new(120, 2);
        lower_base_high_stress.avg_calmar = Decimal::new(165, 2);
        lower_base_high_stress.avg_sharpe = Decimal::new(155, 2);
        lower_base_high_stress.min_annual_return = Decimal::new(12, 2);
        lower_base_high_stress.max_drawdown = Decimal::new(17, 2);
        let policy = json!({
            "train_stress_score_profile": "capacity_stress_return_score_v1",
            "min_train_perturbed_calmar": 1.2,
            "capacity_stress_target_annual_return": 0.15
        });

        let fragile_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "high-base-low-stress",
                Decimal::new(34, 2),
                Decimal::new(24, 1),
                Decimal::new(10, 2),
            ),
            &high_base_low_stress,
            &policy,
        );
        let resilient_score = train_candidate_stress_adjusted_score_for_policy(
            &candidate(
                "lower-base-high-stress",
                Decimal::new(24, 2),
                Decimal::new(16, 1),
                Decimal::new(16, 2),
            ),
            &lower_base_high_stress,
            &policy,
        );

        assert!(
            resilient_score > fragile_score,
            "resilient_score={resilient_score}, fragile_score={fragile_score}"
        );
    }

    #[test]
    fn train_capacity_stress_return_gate_rejects_low_perturbed_annual_return() {
        let candidate = DiscoveryCandidate {
            trial_id: "stress-return-fragile".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(28, 2),
                sharpe: Decimal::new(18, 1),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_calmar = Decimal::new(140, 2);
        summary.avg_calmar = Decimal::new(170, 2);
        summary.min_annual_return = Decimal::new(2, 2);
        let policy = json!({
            "min_train_perturbed_annual_return": 0.05,
            "min_train_avg_perturbed_calmar": 1.2
        });

        let (robustness, passed) = attach_train_capacity_stress_return_gates_to_robustness(
            json!({"status": "approved_candidate", "gate_results": []}),
            &candidate,
            &summary,
            &policy,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], "rejected");
        assert!(robustness["gate_results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| gate["gate"] == "train_perturbed_annual_return"
                && gate["passed"] == false
                && gate["actual"] == json!("0.02")));
    }

    #[test]
    fn train_candidate_evaluation_order_prefers_stress_resilience() {
        let base_candidate = |trial_id: &str, annual_return: Decimal| DiscoveryCandidate {
            trial_id: trial_id.to_string(),
            backtest_task_id: None,
            score: None,
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::new(25, 2),
            metrics: CandidateMetrics {
                annual_return,
                excess_return: Decimal::new(10, 2),
                sharpe: Decimal::new(90, 2),
                sortino: Decimal::new(16, 1),
                max_drawdown: Decimal::new(25, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut fragile_summary = cost_capacity_perturbation_summary_from_counts(1, 3);
        fragile_summary.min_calmar = Decimal::new(36, 2);
        fragile_summary.max_drawdown = Decimal::new(9, 2);
        let mut resilient_summary = cost_capacity_perturbation_summary_from_counts(2, 3);
        resilient_summary.min_calmar = Decimal::new(125, 2);
        resilient_summary.max_drawdown = Decimal::new(12, 2);
        let fragile = OosTrainCandidateEvaluation {
            candidate: base_candidate("high-return-fragile", Decimal::new(32, 2)),
            robustness: json!({"status": "rejected"}),
            train_cost_capacity_perturbations: Vec::new(),
            stress_adjusted_score: train_candidate_stress_adjusted_score(
                &base_candidate("high-return-fragile", Decimal::new(32, 2)),
                &fragile_summary,
            ),
            stress_summary: fragile_summary,
            train_cost_gate_passed: false,
        };
        let resilient = OosTrainCandidateEvaluation {
            candidate: base_candidate("lower-return-resilient", Decimal::new(24, 2)),
            robustness: json!({"status": "approved_candidate"}),
            train_cost_capacity_perturbations: Vec::new(),
            stress_adjusted_score: train_candidate_stress_adjusted_score(
                &base_candidate("lower-return-resilient", Decimal::new(24, 2)),
                &resilient_summary,
            ),
            stress_summary: resilient_summary,
            train_cost_gate_passed: true,
        };
        let mut evaluations = vec![fragile, resilient];

        evaluations.sort_by(train_candidate_evaluation_order);

        assert_eq!(evaluations[0].candidate.trial_id, "lower-return-resilient");
        assert!(evaluations[0].stress_adjusted_score > evaluations[1].stress_adjusted_score);
    }

    #[test]
    fn train_cost_capacity_gate_report_rejects_weak_pass_ratio() {
        let gate = OosCostCapacityGateConfig {
            enabled: true,
            min_pass_ratio: 0.75,
            min_perturbed_calmar: 1.0,
            max_perturbed_drawdown_pct: 0.35,
        };
        let report = build_cost_capacity_pass_ratio_gate(
            "train_cost_capacity_perturbation_pass_ratio",
            1,
            2,
            &gate,
        );

        assert_eq!(report["passed"], json!(false));
        assert_eq!(report["actual"], json!(0.5));
        assert_eq!(report["limit"], json!(0.75));
    }

    #[test]
    fn train_cost_capacity_perturbations_default_when_train_gate_enabled_without_final_gate() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_fo".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };
        let train_gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fo"));

        let perturbations = resolved_train_cost_capacity_perturbations(&req, &train_gate_policy);

        assert_eq!(perturbations.len(), 3);
        assert_eq!(perturbations[0].name.as_deref(), Some("cost_up_150pct"));
        assert_eq!(
            perturbations[1].name.as_deref(),
            Some("impact_cost_2pct_participation_10pct")
        );
        assert_eq!(
            perturbations[2].name.as_deref(),
            Some("capacity_tight_participation_5pct")
        );
        assert!(
            resolved_oos_cost_capacity_perturbations(&req).is_empty(),
            "final/OOS perturbation gate should remain opt-in for final promotion"
        );
    }

    #[test]
    fn oos_experiment_config_reports_train_perturbations_independent_of_final_gate() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: Some(6),
            search_profile: Some("phase7_fo".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["enabled"],
            true
        );
        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["perturbations"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(config["cost_capacity_perturbation_gate"]["enabled"], false);
        assert_eq!(
            config["cost_capacity_perturbation_gate"]["perturbations"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn train_cost_capacity_gate_rejects_candidate_robustness() {
        let gate = OosCostCapacityGateConfig {
            enabled: true,
            min_pass_ratio: 0.75,
            min_perturbed_calmar: 1.0,
            max_perturbed_drawdown_pct: 0.35,
        };
        let (robustness, passed) = attach_train_cost_capacity_gate_to_robustness(
            json!({
                "status": "approved_candidate",
                "gate_results": []
            }),
            1,
            2,
            &gate,
        );

        assert!(!passed);
        assert_eq!(robustness["status"], json!("rejected"));
        assert_eq!(
            robustness["gate_results"][0]["gate"],
            json!("train_cost_capacity_perturbation_pass_ratio")
        );
        assert_eq!(robustness["gate_results"][0]["passed"], json!(false));
    }

    #[test]
    fn train_cost_capacity_overlay_persistence_fields_use_updated_status_and_gates() {
        let fields = train_cost_capacity_overlay_persistence_fields(&json!({
            "gate_result_id": "gate-train-cost",
            "status": "rejected",
            "gate_results": [
                {
                    "gate": "train_cost_capacity_perturbation_pass_ratio",
                    "passed": false
                }
            ]
        }))
        .expect("persistence fields");

        assert_eq!(fields.gate_result_id, "gate-train-cost");
        assert_eq!(fields.status, "rejected");
        assert_eq!(
            fields.gate_results[0]["gate"],
            json!("train_cost_capacity_perturbation_pass_ratio")
        );
    }

    #[test]
    fn random_search_rejects_invalid_search_space_item() {
        let search_space = json!({
            "top_n": {"type": "int", "min": 8, "max": 5}
        });

        let err = generate_trial_parameters(&search_space, 42, 1).unwrap_err();

        assert!(err.contains("top_n"));
    }

    #[test]
    fn random_search_choice_sampling_does_not_collapse_power_of_two_choices() {
        let search_space = json!({
            "prediction_blend_weight": {"type": "choice", "values": [0.0, 0.02, 0.05, 0.1]},
            "prediction_min_percentile": {"type": "choice", "values": [null, 0.1, 0.2, 0.3]},
            "prediction_min_score": {"type": "choice", "values": [null, -0.01, 0.0, 0.01]}
        });

        let trials = generate_trial_parameters(&search_space, 20260520, 16).expect("trial params");
        let unique_pairs = trials
            .iter()
            .map(|params| {
                format!(
                    "{}|{}|{}",
                    params
                        .get("prediction_blend_weight")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "missing".into()),
                    params
                        .get("prediction_min_percentile")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "missing".into()),
                    params
                        .get("prediction_min_score")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "missing".into())
                )
            })
            .collect::<BTreeSet<_>>();

        assert!(
            unique_pairs.len() >= 8,
            "expected broad choice coverage, got {unique_pairs:?}"
        );
    }

    #[test]
    fn trial_backtest_request_applies_parameter_overrides() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase3d_alpha_smoke",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "monthly",
                "max_position_pct": 0.10,
                "correlation_lookback_days": 60,
                "kelly_lookback_days": 60
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let mut params = json!({
            "top_n": 8,
            "rebalance": "5",
            "max_position_pct": 0.08,
            "max_pairwise_correlation": 0.65,
            "kelly_fraction": 0.25,
            "max_gross_exposure": 0.80,
            "score_direction": "ascending",
            "portfolio_method": "risk_budget",
            "risk_budget_lookback_days": 80,
            "capacity_penalty_strength": 0.75,
            "industry_max_weight_pct": 0.35,
            "capacity_risk_budget": "capacity_participation_strict_v1",
            "execution_impact_budget": "impact_turnover_20pct_v1",
            "style_risk_budget": "defensive_style_budget_v1",
            "candidate_risk_filter": "low_volatility_low_correlation_v1",
            "score_candidate_pool_size": 300,
            "universe_profile": "listed_non_st",
            "prediction_set_id": "pred-quality-growth-v1",
            "prediction_blend_weight": 0.35,
            "event_gate_combo_name": "phase7_event_window_earnings_v1",
            "event_gate_mode": "boost_positive",
            "event_gate_min_score": 0.0,
            "event_gate_boost_weight": 0.05,
            "event_gate_score_direction": "descending",
            "market_regime": "quality_crash_guard_v3",
            "portfolio_drawdown_reduce_start_pct": 0.10,
            "portfolio_drawdown_reduce_full_pct": 0.25,
            "portfolio_drawdown_min_exposure": 0.50,
            "portfolio_drawdown_peak_lookback_days": 252,
            "portfolio_drawdown_recovery_start_pct": 0.30,
            "portfolio_drawdown_recovery_full_pct": 0.70,
            "portfolio_drawdown_recovery_boost": 1.0,
            "portfolio_volatility_target_pct": 0.16,
            "portfolio_volatility_lookback_days": 60,
            "portfolio_volatility_min_exposure": 0.45,
            "portfolio_volatility_max_exposure": 1.0,
            "stop_loss_pct": 0.12,
            "take_profit_pct": null,
            "trailing_stop_pct": 0.18,
            "time_stop_days": "120",
            "reentry_cooldown_days": "10"
        });
        params["prediction_min_score"] = json!(0.0);
        params["execution_schedule_profile"] = json!("twap_5d_v1");
        params["execution_carry_policy"] = json!("roll_forward_v1");
        params["return_risk_feature_cache_mode"] = json!("stats_matrix_experimental");

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(req.strategy_version_id, "factor-combo-v1");
        assert_eq!(req.data_version_id, "perf-db-smoke-data-v1");
        assert_eq!(req.combo_name, "phase3d_alpha_smoke");
        assert_eq!(req.top_n, 8);
        assert_eq!(req.rebalance, "5");
        assert_eq!(req.max_position_pct, 0.08);
        assert_eq!(req.max_pairwise_correlation, Some(0.65));
        assert_eq!(req.correlation_lookback_days, 60);
        assert_eq!(req.kelly_fraction, 0.25);
        assert_eq!(req.kelly_lookback_days, 60);
        assert_eq!(req.max_gross_exposure, 0.80);
        assert_eq!(req.score_direction, "ascending");
        assert_eq!(req.portfolio_method, "risk_budget");
        assert_eq!(req.risk_budget_lookback_days, 80);
        assert_eq!(req.capacity_penalty_strength, 0.75);
        assert_eq!(req.industry_max_weight_pct, Some(0.35));
        assert_eq!(
            req.capacity_risk_budget.as_deref(),
            Some("capacity_participation_strict_v1")
        );
        assert_eq!(
            req.execution_impact_budget.as_deref(),
            Some("impact_turnover_20pct_v1")
        );
        assert_eq!(
            req.execution_rules
                .as_ref()
                .and_then(|rules| rules.execution_schedule_profile.as_deref()),
            Some("twap_5d_v1")
        );
        assert_eq!(
            req.execution_rules
                .as_ref()
                .and_then(|rules| rules.execution_carry_policy.as_deref()),
            Some("roll_forward_v1")
        );
        assert_eq!(
            req.style_risk_budget.as_deref(),
            Some("defensive_style_budget_v1")
        );
        assert_eq!(
            req.candidate_risk_filter.as_deref(),
            Some("low_volatility_low_correlation_v1")
        );
        assert_eq!(req.score_candidate_pool_size, Some(300));
        assert_eq!(req.universe_profile.as_deref(), Some("listed_non_st"));
        assert_eq!(
            req.prediction_set_id.as_deref(),
            Some("pred-quality-growth-v1")
        );
        assert_eq!(req.prediction_blend_weight, Some(0.35));
        assert_eq!(req.prediction_min_score, Some(0.0));
        assert_eq!(
            req.event_gate_combo_name.as_deref(),
            Some("phase7_event_window_earnings_v1")
        );
        assert_eq!(req.event_gate_mode.as_deref(), Some("boost_positive"));
        assert_eq!(req.event_gate_min_score, Some(0.0));
        assert_eq!(req.event_gate_boost_weight, Some(0.05));
        assert_eq!(
            req.event_gate_score_direction.as_deref(),
            Some("descending")
        );
        assert_eq!(
            req.market_regime.as_ref().and_then(|policy| policy.enabled),
            Some(true)
        );
        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_crash_guard_v3")
        );
        assert_eq!(req.portfolio_drawdown_reduce_start_pct, Some(0.10));
        assert_eq!(req.portfolio_drawdown_reduce_full_pct, Some(0.25));
        assert_eq!(req.portfolio_drawdown_min_exposure, Some(0.50));
        assert_eq!(req.portfolio_drawdown_peak_lookback_days, Some(252));
        assert_eq!(req.portfolio_drawdown_recovery_start_pct, Some(0.30));
        assert_eq!(req.portfolio_drawdown_recovery_full_pct, Some(0.70));
        assert_eq!(req.portfolio_drawdown_recovery_boost, Some(1.0));
        assert_eq!(req.portfolio_volatility_target_pct, Some(0.16));
        assert_eq!(req.portfolio_volatility_lookback_days, Some(60));
        assert_eq!(req.portfolio_volatility_min_exposure, Some(0.45));
        assert_eq!(req.portfolio_volatility_max_exposure, Some(1.0));
        assert_eq!(req.stop_loss_pct, Some(0.12));
        assert_eq!(req.take_profit_pct, None);
        assert_eq!(req.trailing_stop_pct, Some(0.18));
        assert_eq!(req.time_stop_days, Some(120));
        assert_eq!(req.reentry_cooldown_days, Some(10));
        assert_eq!(req.persistence_mode.as_deref(), Some("summary_only"));
        assert_eq!(
            req.return_risk_feature_cache_mode.as_deref(),
            Some("stats_matrix_experimental")
        );
        let coverage = req
            .effective_coverage
            .as_ref()
            .expect("optimization requests should auto-enable effective coverage");
        assert_eq!(coverage.enabled, Some(true));
        assert_eq!(coverage.mode.as_deref(), Some("adjust_start"));
        assert_eq!(coverage.min_rows, Some(8));
        assert_eq!(coverage.include_rebalance_warmup, Some(true));
        assert_eq!(coverage.warmup_trading_days, Some(19));
    }

    #[test]
    fn trial_backtest_request_allows_effective_coverage_override() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20160201",
                "end_date": "20260515",
                "benchmark": "000300.SH",
                "top_n": 20,
                "effective_coverage": {
                    "enabled": true,
                    "mode": "guard_only",
                    "min_rows": 120
                }
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 30,
            "effective_coverage": {
                "enabled": true,
                "mode": "adjust_start",
                "min_rows": 180
            }
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        let coverage = req.effective_coverage.expect("effective coverage");
        assert_eq!(coverage.enabled, Some(true));
        assert_eq!(coverage.mode.as_deref(), Some("adjust_start"));
        assert_eq!(coverage.min_rows, Some(180));
        assert_eq!(coverage.include_rebalance_warmup, Some(true));
        assert_eq!(coverage.warmup_trading_days, Some(19));
    }

    #[test]
    fn trial_backtest_request_passes_cost_and_execution_rules() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "cost_model": {
                    "commission_rate": 0.0003,
                    "min_commission": 5.0,
                    "tax_rate": 0.001,
                    "slippage_bps": 0.0001,
                    "cost_multiplier": 1.0,
                    "impact_cost_coefficient": 0.01
                },
                "execution_rules": {
                    "execution_timing": "next_open",
                    "execution_price": "next_open",
                    "max_participation_rate": 0.10
                }
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "cost_model": {
                "cost_multiplier": 1.5,
                "slippage_bps": 0.0002
            },
            "execution_rules": {
                "max_participation_rate": 0.05
            }
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        let cost = req.cost_model.expect("cost model should be propagated");
        assert_eq!(cost.commission_rate, Some(0.0003));
        assert_eq!(cost.min_commission, Some(5.0));
        assert_eq!(cost.tax_rate, Some(0.001));
        assert_eq!(cost.slippage_bps, Some(0.0002));
        assert_eq!(cost.cost_multiplier, Some(1.5));
        assert_eq!(cost.impact_cost_coefficient, Some(0.01));
        let rules = req
            .execution_rules
            .expect("execution rules should be propagated");
        assert_eq!(rules.execution_timing.as_deref(), Some("next_open"));
        assert_eq!(rules.execution_price.as_deref(), Some("next_open"));
        assert_eq!(rules.max_participation_rate, Some(0.05));
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_selector_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "phase7-professional-v1".into(),
            data_version_id: "full-market-2016-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20160304",
                "end_date": "20260515",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60"
            }),
            objective: json!({"type": "professional_candidate"}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_state_alpha_selector_v2",
            "portfolio_method": "risk_budget",
            "risk_budget_lookback_days": 160,
            "score_direction": "ascending",
            "max_position_pct": "0.15",
            "max_pairwise_correlation": "0.75",
            "candidate_risk_filter": "off",
            "risk_contribution_control": "soft_single_name_20pct_v1"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_state_alpha_selector_v2")
        );
        assert_eq!(
            req.risk_contribution_control.as_deref(),
            Some("soft_single_name_20pct_v1")
        );
    }

    #[test]
    fn trial_backtest_request_applies_rebalance_smoothing_overrides() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "monthly",
                "max_position_pct": 0.10
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "rebalance_hysteresis_pct": 0.02,
            "partial_rebalance_ratio": 0.75
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(req.rebalance_hysteresis_pct, Some(0.02));
        assert_eq!(req.partial_rebalance_ratio, Some(0.75));
    }

    #[test]
    fn trial_backtest_request_accepts_bear_window_market_regime_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_bear_window_guard_v1",
            "score_direction": "ascending"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_bear_window_guard_v1")
        );
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_switch_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_regime_alpha_switch_v1",
            "score_direction": "ascending"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_regime_alpha_switch_v1")
        );
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_sleeve_search_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_regime_alpha_switch_value_v1",
            "quality_regime_alpha_switch_recovery_v1",
            "quality_regime_alpha_switch_blend_v1",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_overlay_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_regime_alpha_overlay_value_05pct_v1",
            "quality_regime_alpha_overlay_value_10pct_v1",
            "quality_regime_alpha_overlay_blend_10pct_v1",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_state_sharpe_bridge_router_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_state_sharpe_bridge_router_v1",
            "quality_state_sharpe_bridge_router_v2",
            "quality_state_sharpe_bridge_router_v3",
            "quality_frontier_regime_bridge_router_v1",
            "quality_frontier_regime_bridge_router_v2",
            "quality_frontier_regime_bridge_router_v3",
            "quality_frontier_regime_bridge_router_v4",
            "quality_frontier_regime_bridge_router_v5",
            "quality_frontier_regime_bridge_router_v6",
            "quality_frontier_regime_bridge_router_v7",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_nonlinear_alpha_router_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_nonlinear_alpha_router_v1",
            "quality_nonlinear_alpha_router_v2",
            "quality_nonlinear_alpha_risk_memory_router_v1",
            "quality_nonlinear_alpha_risk_memory_router_v2",
            "quality_nonlinear_alpha_risk_memory_router_v3",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_regime_alpha_portfolio_sleeve_policies() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };

        for policy in [
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
            "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1",
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1",
            "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1",
        ] {
            let params = json!({
                "market_regime": policy,
                "score_direction": "ascending"
            });
            let req = build_factor_trial_request(&task, &params).expect("factor request");
            assert_eq!(
                req.market_regime
                    .as_ref()
                    .and_then(|policy| policy.policy.as_deref()),
                Some(policy)
            );
        }
    }

    #[test]
    fn trial_backtest_request_accepts_bear_position_market_regime_policy() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "factor-combo-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "60",
                "max_position_pct": 0.15,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "market_regime": "quality_bear_position_guard_v1",
            "score_direction": "ascending"
        });

        let req = build_factor_trial_request(&task, &params).expect("factor request");

        assert_eq!(
            req.market_regime
                .as_ref()
                .and_then(|policy| policy.policy.as_deref()),
            Some("quality_bear_position_guard_v1")
        );
    }

    #[test]
    fn optimization_trial_request_routes_model_prediction_source() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "prediction-strategy-v1".into(),
            data_version_id: "perf-db-smoke-data-v1".into(),
            backtest_template: json!({
                "signal_source": "model_prediction",
                "prediction_set_id": "pred-linear-v1",
                "start_date": "20250109",
                "end_date": "20250131",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "5",
                "max_position_pct": 0.08,
                "portfolio_method": "risk_budget"
            }),
            objective: json!({"type": "risk_adjusted", "maximize": true}),
            constraints: None,
        };
        let params = json!({
            "top_n": 10,
            "kelly_fraction": 0.25,
            "risk_budget_lookback_days": 120
        });

        let req = build_optimization_trial_request(&task, &params).expect("prediction request");

        match req {
            OptimizationTrialBacktestRequest::Prediction(req) => {
                assert_eq!(req.prediction_set_id, "pred-linear-v1");
                assert_eq!(req.strategy_version_id, "prediction-strategy-v1");
                assert_eq!(req.top_n, 10);
                assert_eq!(req.rebalance, "5");
                assert_eq!(req.max_position_pct, 0.08);
                assert_eq!(req.portfolio_method, "risk_budget");
                assert_eq!(req.kelly_fraction, 0.25);
                assert_eq!(req.risk_budget_lookback_days, 120);
            }
            OptimizationTrialBacktestRequest::Factor(_) => {
                panic!("expected model prediction request")
            }
        }
    }

    #[test]
    fn phase7_layered_request_builds_resource_limited_plan() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160405",
                "end_date": "20260511",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(7),
            search_profile: None,
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert!(bundle.plan.requested_trials > bundle.plan.planned_trials);
        assert_eq!(bundle.plan.planned_trials, 7);
        assert!(bundle.plan.truncated);
        assert_eq!(bundle.search_space["phase"], "7-D");
        assert_eq!(bundle.search_space["resource_plan"]["max_trials"], 7);
    }

    #[test]
    fn phase7_layered_request_can_include_prediction_candidates() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20250109",
                "end_date": "20250131",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![" pred-linear-v1 ".to_string(), "".to_string()]),
            max_trials: Some(40),
            search_profile: None,
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"][0],
            "pred-linear-v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["signal_source"] == "model_prediction"
                && trial.parameters["prediction_set_id"] == "pred-linear-v1"
        }));
    }

    #[test]
    fn professional_discovery_defaults_to_sharpe_stabilization_search_profile() {
        let req = Phase7ProfessionalDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: None,
            trial_batch_limit: Some(4),
            max_batches: Some(2),
            robustness_top_n: Some(3),
            robustness_gate_policy: None,
            stop_after_professional_candidate: Some(true),
            stop_after_robust_approval: Some(true),
        };
        let layered_req = phase7_discovery_layered_request(&req);
        let bundle = build_phase7_layered_plan_bundle(
            &layered_req,
            quant_common::phase7::LocalResourcePlan::for_machine(10, 32),
        );

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert_eq!(
            layered_req.constraints.as_ref().unwrap()["min_sortino"],
            1.5
        );
        let first_trial = bundle.plan.trials.first().unwrap();
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            first_trial.parameters["portfolio_drawdown_control"],
            "recover252_10_24_50_30_70"
        );
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(first_trial.parameters["stop_loss_pct"], "0.075");
        assert_eq!(first_trial.parameters["reentry_cooldown_days"], 30);
        assert_eq!(first_trial.parameters["rebalance_hysteresis_pct"], "0");
        assert_eq!(first_trial.parameters["partial_rebalance_ratio"], "1");
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_sharpe_stabilization_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_t".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 20
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_stabilization_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_u".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_crash_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_bear_window_stabilization_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_u2".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_bear_window_stabilization"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_window_guard_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_style_risk_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_v".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_style_risk_budget"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["style_risk_budget"] == "defensive_style_budget_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_second_alpha_source_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_w".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_second_alpha_source"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_value_recovery_confirm_v1"
                && trial.parameters["style_risk_budget"] == "liquidity_volatility_balanced_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
                && trial.parameters["stop_loss_pct"] == "0.075"
                && trial.parameters["reentry_cooldown_days"] == 30
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_residual_quality_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_ag".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_residual_quality"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_industry_residual_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_industry_residual_quality_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_residual_overlay_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ah".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_residual_overlay_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[1].parameters["combo_name"],
            "phase7_quality_residual_confirm_5pct_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_residual_confirm_10pct_v1"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["market_regime"] == "quality_bear_window_guard_v2" }));
    }

    #[test]
    fn phase7_layered_request_accepts_conditioned_second_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ak".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_conditioned_second_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_require_top40"
                && trial.parameters["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial.parameters["event_gate_min_score"] == "0.60"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "moneyflow_require_top40"
                && trial.parameters["event_gate_combo_name"] == "phase7_moneyflow_v1"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| trial.parameters["combo_name"] == "phase7_financial_quality_v1"));
    }

    #[test]
    fn phase7_layered_request_accepts_valuation_guard_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(9),
            search_profile: Some("phase7_al".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_valuation_guard_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 9);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial.parameters["event_gate_mode"] == "exclude_negative"
                && trial.parameters["event_gate_min_score"] == "0.40"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_mode"] == "require_positive"
                || trial.parameters["event_gate_combo_name"] == "phase7_moneyflow_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_conditioned_valuation_guard_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_am".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_conditioned_valuation_guard"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35_stress_only"
                && trial.parameters["event_gate_active_regimes"]
                    == json!(["bear", "high_volatility"])
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40_stress_only"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_routing_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_an".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_routing"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_sleeve_search_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ao".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_sleeve_search"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_value_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_recovery_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_switch_blend_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_overlay_search_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ap".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_overlay_search"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_value_05pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_value_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_blend_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_sleeve_allocation_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_aq".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_sleeve_allocation"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_low_risk_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ar".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_low_risk_sleeve"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_value_guard_sleeve_composition_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_as".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_value_guard_sleeve_composition"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_nearest_candidate_risk_model_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_at".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_nearest_candidate_risk_model"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial.parameters["portfolio_method"] == "min_variance"
                && trial.parameters["risk_budget_lookback_days"] == 180
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_regime_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_av".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_regime_sleeve"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_sleeve_weight_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_aw".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_sleeve_weight"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_sleeve_upper_bound_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ax".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_sleeve_upper_bound"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_regime_placement_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ay".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_regime_placement"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_window_decay_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_az".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_window_decay"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_surprise_nonlinear_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_ba".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_surprise_nonlinear"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial.parameters["event_gate_combo_name"] != "phase7_event_window_earnings_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_boost_pos_5pct_stress_only"
                && trial.parameters["event_gate_active_regimes"]
                    == json!(["bear", "high_volatility"])
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_quality_segment_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_bb".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_quality_segment"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 120
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_strength_segment_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(5),
            search_profile: Some("phase7_bc".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_strength_segment"
        );
        assert_eq!(bundle.plan.planned_trials, 5);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial.parameters["event_gate_mode"] == "require_positive"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_require_strong_p90"
                && trial.parameters["event_gate_min_score"] == "0.66"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_require_strong_p90"
                && trial.parameters["event_gate_min_score"] == "0.43"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_confirm_require_light_p50"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_earnings_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_strength_boost_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_bd".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_strength_boost"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial.parameters["event_gate_mode"] == "boost_positive"
                && trial.parameters["event_gate_boost_weight"] != "0"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_boost_strong_p75_5pct"
                && trial.parameters["event_gate_min_score"] == "0.38"
                && trial.parameters["event_gate_boost_weight"] == "0.05"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_boost_strong_p75_3pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial.parameters["event_gate_min_score"] == "0.35"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_confirm_boost_light_p50_3pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_earnings_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_legacy_alpha_revalidation_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_be".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_legacy_alpha_revalidation"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["legacy_revalidation_profile"]
                == "full_icir_v3_full_history_legacy_exact"
                && trial.parameters["combo_name"] == "full_icir_16f_v3"
                && trial.parameters["start_date"] == "20160201"
                && trial.parameters["end_date"] == "20260511"
                && trial.parameters["portfolio_method"] == "heuristic"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["legacy_revalidation_profile"]
                == "full_icir_v3_full_history_risk_budget"
                && trial.parameters["combo_name"] == "full_icir_16f_v3"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["legacy_revalidation_profile"]
                == "full_icir_v3_recent_window_diagnostic"
                && trial.parameters["start_date"] == "20230512"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_risk_shape_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bf".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_risk_shape"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_drawdown_control"] == "recover126_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_risk_control"] == "stop_loss_075_cooldown_45"
                && trial.parameters["reentry_cooldown_days"] == 45
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos12_corr65"
                && trial.parameters["max_position_pct"] == "0.12"
                && trial.parameters["max_pairwise_correlation"] == "0.65"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_position_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bg".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_position_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["risk_budget_shape_profile"] == "risk_budget_180" }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos14_corr70"
                && trial.parameters["max_position_pct"] == "0.14"
                && trial.parameters["max_pairwise_correlation"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "vol16_recover08_maxpos14_corr70"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_weak_window_repair_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bh".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_weak_window_repair"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "stress_event_window_boost_p75_3pct"
                && trial.parameters["event_gate_active_regimes"]
                    == json!(["bear", "high_volatility"])
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "stress_valuation_boost_p40_5pct"
                && trial.parameters["event_gate_min_score"] == "0.40"
                && trial.parameters["event_gate_boost_weight"] == "0.05"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_current_anchor_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bi".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_anchor_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert_eq!(
            bundle.plan.trials[0].parameters["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos14_corr65_vol20"
                && trial.parameters["portfolio_volatility_control"] == "vol120_20_60_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos15_corr70_vol20"
                && trial.parameters["max_position_pct"] == "0.15"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_return_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bn".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_return_recovery"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert_eq!(
            bundle.plan.trials[0].parameters["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_19_58_100"
                && trial.parameters["position_shape_profile"] == "maxpos14_corr65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial.parameters["event_gate_min_score"] == "0.35"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_risk_memory_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_bo".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_risk_memory_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["risk_budget_shape_profile"] == "risk_budget_150"
                && trial.parameters["risk_budget_lookback_days"] == 150
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"] == "maxpos145_corr65_rb150"
                && trial.parameters["max_position_pct"] == "0.145"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["risk_budget_lookback_days"] == 150
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_return_sharpe_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_bp".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_return_sharpe_router"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v1"
                && trial.parameters["position_shape_profile"] == "maxpos14_corr65"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v2"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_return_sharpe_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bq".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_return_sharpe_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["risk_budget_shape_profile"] == "bp_rb160_router_v3"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .all(|trial| { trial.parameters["combo_name"] == "phase7_financial_quality_v1" }));
    }

    #[test]
    fn phase7_layered_request_accepts_position_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_br".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_position_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial.parameters["position_shape_profile"] == "maxpos14_corr65_router_v4_rb160"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["position_shape_profile"] == "maxpos145_corr65_router_v3_rb160"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_moderate_position_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bs".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_moderate_position_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial.parameters["position_shape_profile"]
                    == "maxpos16_corr70_router_v4_vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["position_shape_profile"]
                    == "maxpos17_corr725_router_v3_vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_correlation_frontier_sharpe_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bt".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_correlation_frontier_sharpe_return"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos16_corr705_router_v4_vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos165_corr72_router_v4_vol120_17_52_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_correlation_threshold_sharpe_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bu".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_correlation_threshold_sharpe_return"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos16_corr706_router_v4_vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["position_shape_profile"]
                == "maxpos165_corr709_router_v4_vol120_17_52_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v4"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_soft_risk_frontier_sharpe_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_bv".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_soft_risk_frontier_sharpe_return"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial.parameters["market_regime"]
                    == "quality_event_window_return_sharpe_router_v3"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_selector_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bw".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_selector"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_selector_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_alpha_overlay_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bx".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_alpha_overlay_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["rebalance_hysteresis_pct"] == "0"
                && trial.parameters["partial_rebalance_ratio"] == "1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v3"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.005"
                && trial.parameters["partial_rebalance_ratio"] == "0.85"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_event_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_by".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_event_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_event_state_overlay_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["rebalance_hysteresis_pct"] == "0"
                && trial.parameters["partial_rebalance_ratio"] == "1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_event_state_overlay_selector_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_risk_memory_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bz".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_risk_memory"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["risk_budget_lookback_days"] == 160
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_risk_memory_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_ca".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_risk_memory_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v4"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v5"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_risk_memory_fine_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cb".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_risk_memory_fine_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v7"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v9"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_exposure_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cc".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_exposure_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v11"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_mixed_state_orthogonal_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cd".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_mixed_state_orthogonal_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_candidate_filter_alpha_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_ce".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_candidate_filter_alpha_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_soft_candidate_filter_alpha_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cf".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_soft_candidate_filter_alpha_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "soft_low_volatility_v1"
                && trial.parameters["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v3"
        }));
        assert!(bundle
            .plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["candidate_risk_filter"] == "off" }));
    }

    #[test]
    fn phase7_layered_request_accepts_sharpe_bridge_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cg".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_bridge_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_annual_sharpe_floor_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ch".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_annual_sharpe_floor_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["score_direction"] == "ascending"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_risk_memory_relaxed_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_ci".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_risk_memory_relaxed_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v18"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom35"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_sharpe_floor_auto_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ck".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_sharpe_floor_auto_discovery"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_portfolio_sharpe_control_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cj".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_portfolio_sharpe_control"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_sharpe_control"] == "roll_sharpe120_060_000_55"
                && trial.parameters["portfolio_sharpe_reduce_start"] == "0.60"
                && trial.parameters["portfolio_sharpe_lookback_days"] == 120
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_sharpe_control"]
                    == "bridge_roll_sharpe180_050_neg10_60"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_nonlinear_alpha_auto_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cl".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_nonlinear_alpha_auto_discovery"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_router_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_sharpe_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_nonlinear_sharpe_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_cm".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_nonlinear_sharpe_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial.parameters["max_position_pct"] == "0.16"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_confirmed_sharpe_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cn".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_confirmed_sharpe_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["prediction_blend_weight"] == "0.02"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["prediction_min_percentile"] == "0.20"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial.parameters["prediction_min_percentile"] == "0.30"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_boundary_return_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_co".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_boundary_return_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial.parameters["risk_budget_lookback_days"] == 180
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial.parameters["risk_budget_lookback_days"] == 170
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_boundary_event_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_cq".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_boundary_event_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_micro_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_cr".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_micro_frontier"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["candidate_risk_filter"] == "soft_low_volatility_v1"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["candidate_risk_filter"]
                    == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_sharpe_return_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cs".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_sharpe_return_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "valuation_exclude_bottom43"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_shape_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ct".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_shape_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 18
                && trial.parameters["rebalance"] == "55"
                && trial.parameters["skip_top_pct"] == "0.08"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 22
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_23_45_30_70"
                && trial.parameters["stop_loss_pct"] == "0.07"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_ultra_micro_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(15),
            search_profile: Some("phase7_cu".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_ultra_micro_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 15);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 20
                && trial.parameters["rebalance"] == "60"
                && trial.parameters["skip_top_pct"] == "0.10"
                && trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["risk_budget_lookback_days"] == 170
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 21
                && trial.parameters["rebalance"] == "60"
                && trial.parameters["skip_top_pct"] == "0.10"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 20
                && trial.parameters["rebalance"] == "62"
                && trial.parameters["portfolio_volatility_control"] == "vol120_143_467_100"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_annual_floor_micro_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cz".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_annual_floor_micro_lift"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v15"
                && trial.parameters["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial.parameters["risk_budget_lookback_days"] == 165
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial.parameters["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial.parameters["portfolio_sharpe_min_exposure"] == "0.68"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_near_miss_annual_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_da".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_near_miss_annual_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial.parameters["risk_budget_lookback_days"] == 165
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["max_position_pct"] == "0.145"
                && trial.parameters["max_pairwise_correlation"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_v14_corr70_annual_edge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(20),
            search_profile: Some("phase7_db".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_v14_corr70_annual_edge"
        );
        assert_eq!(bundle.plan.planned_trials, 20);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["portfolio_volatility_control"] == "vol120_1415_464_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial.parameters["max_pairwise_correlation"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["portfolio_sharpe_min_exposure"] == "0.68"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial.parameters["max_pairwise_correlation"] == "0.68"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["max_position_pct"] == "0.15"
                && trial.parameters["candidate_risk_filter"] == "off"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_robust_candidate_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_dj".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_robust_candidate"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["top_n"] == 50
                && trial.parameters["rebalance"] == "120"
                && trial.parameters["max_position_pct"] == "0.12"
                && trial.parameters["capacity_penalty_strength"] == "1.25"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["rebalance_hysteresis_pct"] == "0.01"
                && trial.parameters["partial_rebalance_ratio"] == "0.75"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_low_turnover_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_dn".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_low_turnover_alpha"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["candidate_risk_filter"]
                    == "soft_low_volatility_low_correlation_v1"
                && trial.parameters["rebalance"] == "180"
                && trial.parameters["capacity_penalty_strength"] == "1.5"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_capacity_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_dq".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_impact_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(18),
            search_profile: Some("phase7_dr".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_impact_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_schedule_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ds".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_schedule_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_5d_v1"
                && trial.parameters["execution_rules"]["execution_schedule_profile"] == "twap_5d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_patient_execution_schedule_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dt".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_patient_schedule_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["execution_rules"]["execution_schedule_profile"]
                    == "twap_20d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_daily_cap_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_du".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_daily_cap_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_daily_target_move_limit_pct"] == "0.05"
                && trial.parameters["execution_rules"]["execution_daily_target_move_limit_pct"]
                    == json!(0.05)
                && trial.parameters["execution_rules"]["execution_max_carry_days"] == 20
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_cash_drag_aware_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dv".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_cash_drag_aware_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
                && trial.parameters["execution_daily_target_move_limit_pct"] == "0.075"
                && trial.parameters["execution_max_carry_days"] == 40
                && trial.parameters["execution_rules"]["execution_daily_target_move_limit_pct"]
                    == json!(0.075)
        }));
    }

    #[test]
    fn phase7_dv_defaults_oos_train_score_to_cash_drag_fill_gap() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_dv".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_cash_weight_pct"],
            json!(0.20)
        );
        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["enabled"],
            true
        );
        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["min_pass_ratio"],
            json!(0.80)
        );
    }

    #[test]
    fn phase7_dy_defaults_oos_train_gate_to_fill_ratio_not_final_cash() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_dy".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert!(config["train_selection_gate_policy"]
            .get("max_train_final_cash_weight_pct")
            .is_none());
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_dz_defaults_oos_train_gate_to_rolling_carry_fill_ratio() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_dz".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert!(config["train_selection_gate_policy"]
            .get("max_train_execution_target_gap_pct")
            .is_none());
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ea_defaults_oos_train_gate_to_capacity_fill_frontier() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ea".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_eb_defaults_oos_train_gate_to_alpha_capacity_bridge() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eb".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_ec_defaults_oos_train_gate_to_alpha_capacity_return_frontier() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ec".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_ed_defaults_oos_train_gate_to_stress_fill_return_frontier() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ed".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_execution_schedule_expired_count"],
            json!(0)
        );
    }

    #[test]
    fn phase7_ee_defaults_oos_train_gate_to_stress_risk_budget() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ee".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "cash_drag_fill_gap_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
    }

    #[test]
    fn phase7_ef_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ef".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_perturbed_annual_return"],
            json!(0.05)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
    }

    #[test]
    fn phase7_eg_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eg".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_perturbed_annual_return"],
            json!(0.05)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_avg_perturbed_calmar"],
            json!(1.2)
        );
    }

    #[test]
    fn phase7_eh_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eh".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ei_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ei".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ej_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ej".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ek_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ek".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_el_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_el".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_em_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_em".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_en_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_en".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_eo_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eo".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_ep_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_ep".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_eq_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_eq".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_er_defaults_oos_train_gate_to_capacity_stress_return() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: Some("phase7_er".to_string()),
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: None,
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: Some(true),
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: Some(0.80),
            min_perturbed_oos_calmar: Some(1.2),
            max_perturbed_oos_drawdown_pct: Some(0.35),
        };

        let config =
            oos_walk_forward_experiment_config(&req, &json!({"validation_mode": "walk_forward"}));

        assert_eq!(
            config["train_cost_capacity_perturbation_gate"]["stress_aware_selection_score"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            config["train_selection_gate_policy"]["capacity_stress_target_annual_return"],
            json!(0.15)
        );
        assert_eq!(
            config["train_selection_gate_policy"]["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_layered_request_accepts_execution_feasible_fill_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dx".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_feasible_fill_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "fillable_gross_90_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_fill_ratio_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dy".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_fill_ratio_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_rolling_carry_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_dz".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_rolling_carry_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["execution_rules"]["execution_carry_policy"]
                    == "roll_forward_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_capacity_fill_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ea".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_fill_frontier"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
                && trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_alpha_capacity_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eb".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_alpha_capacity_bridge"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["max_gross_exposure"] == "1"
                && trial.parameters["cash_utilization"] == "fillable_gross_95_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["capacity_risk_budget"] == "capacity_participation_strict_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_alpha_capacity_return_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ec".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_alpha_capacity_return_frontier"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["max_gross_exposure"] == "1"
                && trial.parameters["execution_schedule_profile"] == "twap_10d_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["universe_profile"] == "listed_non_st"
                && trial.parameters["top_n"] == 60
                && trial.parameters["max_position_pct"] == "0.10"
                && trial.parameters["capacity_risk_budget"] == "capacity_participation_balanced_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_stress_fill_return_frontier_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ed".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_fill_return_frontier"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_execution_stress_risk_budget_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ee".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_risk_budget"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_soft_cap_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["execution_schedule_profile"] == "twap_10d_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_capacity_stress_return_gate_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ef".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_stress_return_gate"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_soft_cap_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_15d_v1"
                && trial.parameters["top_n"] == 100
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_low_impact_alpha_stress_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eg".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_low_impact_alpha_stress_return"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_soft_cap_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 100
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["rebalance"] == "180"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_stress_target_scaling_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eh".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_target_scaling_return"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_target_scale_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.05"
                && trial.parameters["max_gross_exposure"] == "0.80"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_stress_floor_scaling_return_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ei".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_floor_scaling_return"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_floor_35_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 100
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_stress_floor_return_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ej".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_stress_floor_return_recovery"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_pressure_headroom_floor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ek".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pressure_headroom_floor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 160
                && trial.parameters["max_position_pct"] == "0.05"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_alpha_headroom_floor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_el".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_alpha_headroom_floor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_blended_alpha_headroom_floor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_em".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_blended_alpha_headroom_floor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["max_position_pct"] == "0.06"
                && trial.parameters["max_gross_exposure"] == "0.90"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_anchor_stress_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_en".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_event_anchor_stress_bridge"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 80
                && trial.parameters["rebalance"] == "160"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_participation_aware_event_anchor_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eo".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_participation_aware_event_anchor"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"]
                == "soft_liquidity_low_volatility_low_correlation_v1"
                && trial.parameters["execution_rules"]["max_participation_rate"] == json!(0.10)
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["top_n"] == 120
                && trial.parameters["rebalance"] == "180"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_cl_anchor_fill_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ep".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_cl_anchor_fill_recovery"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ep should seed the CL high-Sharpe anchor first");
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_mixed_orthogonal_risk_memory_router_v3"
        );
        assert_eq!(
            first_trial.parameters["execution_cl_anchor_fill_recovery_profile"],
            "cl_fill_recovery_top120_rebalance180"
        );
        assert_eq!(first_trial.parameters["top_n"], 120);
        assert_eq!(first_trial.parameters["max_position_pct"], "0.06");
        assert_eq!(first_trial.parameters["max_gross_exposure"], "0.90");
        assert_eq!(
            first_trial.parameters["execution_rules"]["max_participation_rate"],
            json!(0.10)
        );
    }

    #[test]
    fn phase7_layered_request_accepts_capacity_aware_candidate_ranking_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eq".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_capacity_aware_candidate_ranking"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_eq should seed the CL high-Sharpe anchor first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_capacity_aware_candidate_ranking_profile"],
            "capacity_rank_top100_rebalance180"
        );
        assert_eq!(first_trial.parameters["top_n"], 100);
        assert_eq!(first_trial.parameters["max_position_pct"], "0.08");
        assert_eq!(
            first_trial.parameters["execution_rules"]["max_participation_rate"],
            json!(0.10)
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_capacity_ranking_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_er".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_capacity_ranking"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_er"));
        assert_eq!(
            gate_policy["enable_train_cost_capacity_perturbation_gate"],
            true
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_er should seed the EQ capacity anchor first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_capacity_aware_candidate_ranking_profile"],
            "capacity_rank_top100_rebalance180"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_alpha_first_low_impact_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_es".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_alpha_first_low_impact"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_es"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_es should seed alpha-first PIT capacity recovery first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert_eq!(
            first_trial.parameters["pit_capacity_recovery_profile"],
            "pit_alpha_first_event_top80_rebalance160"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_excess_return_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_et".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_excess_return_recovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_et"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_et should seed PIT excess-return recovery first");
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "relative_strength_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["pit_excess_return_recovery_profile"],
            "pit_excess_event_top80_rebalance120"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_bull_sleeve_cash_recovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_eu".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_bull_sleeve_cash_recovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_eu"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_eu should seed nearest-candidate recovery first");
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            first_trial.parameters["event_gate_profile"],
            "valuation_exclude_bottom40"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(first_trial.parameters["candidate_risk_filter"], "off");
        assert_eq!(first_trial.parameters["risk_contribution_control"], "off");
        assert_eq!(first_trial.parameters["cash_utilization"], "off");
        assert_eq!(first_trial.parameters["execution_impact_budget"], "off");
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
                && trial.parameters["cash_utilization"] == "off"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_return_first_fill_repair_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ey".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_return_first_fill_repair"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ey"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ey should seed return-first fill repair first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            first_trial.parameters["cash_utilization"],
            "stress_fill_gross_98_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_schedule_profile"],
            "twap_10d_v1"
        );
        assert_eq!(
            first_trial.parameters["return_first_fill_repair_profile"],
            "return_first_fill_anchor1_top60_stress_fill"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_pit_nonlinear_alpha_regime_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ez".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_nonlinear_alpha_regime_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ez"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ez should seed PIT nonlinear alpha rebuild first");
        assert_eq!(
            first_trial.parameters["pit_nonlinear_alpha_regime_rebuild_profile"],
            "pit_nonlinear_event_overlay_softcap_top100_rebalance180"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_state_alpha_overlay_selector_v1"
        );
        assert_eq!(
            first_trial.parameters["cash_utilization"],
            "stress_fill_gross_98_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_schedule_profile"],
            "twap_20d_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["market_regime"]
                    == "quality_nonlinear_alpha_risk_memory_router_v3"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_pit_quality_recovery_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fa".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_pit_quality_recovery_alpha"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fa"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fa should seed PIT quality recovery first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_recovery_acceleration_v1"
        );
        assert_eq!(first_trial.parameters["score_direction"], "descending");
        assert_eq!(
            first_trial.parameters["pit_quality_recovery_alpha_profile"],
            "pit_quality_recovery_top100_rebalance160"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["market_regime"]
                    == "quality_nonlinear_alpha_risk_memory_router_v3"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_post_return_curve_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fb".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_event_post_return_curve_alpha"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fb"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fb should seed PIT event post-return curve first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_event_post_return_curve_overlay_v1"
        );
        assert_eq!(
            first_trial.parameters["event_post_return_curve_alpha_profile"],
            "event_post_return_curve_top100_rebalance160"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_event_reaction_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fc".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_event_reaction_alpha"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fc"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fc should seed event reaction segment alpha first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_event_reaction_segments_overlay_v1"
        );
        assert_eq!(
            first_trial.parameters["event_reaction_alpha_profile"],
            "event_reaction_segments_top100_rebalance160"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_reaction_reversal_overlay_v1"
                && trial.parameters["event_reaction_alpha_profile"]
                    == "event_reaction_reversal_top100_rebalance160"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_broad_financial_feature_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_fg".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_broad_financial_feature_discovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fg"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_fg should seed broad FF financial combos first");
        assert_eq!(
            first_trial.parameters["combo_name"],
            "phase7_quality_cashflow_dividend_confirm_v1"
        );
        assert_eq!(
            first_trial.parameters["broad_financial_feature_discovery_profile"],
            "broad_ff_dual_confirm_ascending_top100"
        );
        assert_eq!(
            first_trial.parameters["candidate_ranking"],
            "alpha_first_low_impact_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_dividend_confirm_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_dividend_quality_v1"
                || trial.parameters["combo_name"] == "phase7_cashflow_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_broad_financial_stratified_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fh".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_broad_financial_feature_stratified_discovery"
        );
        let first_four_combos = bundle
            .plan
            .trials
            .iter()
            .take(4)
            .map(|trial| trial.parameters["combo_name"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        assert!(first_four_combos.contains(&"phase7_financial_quality_v1"));
        assert!(first_four_combos.contains(&"phase7_growth_recovery_v1"));
        assert!(first_four_combos.contains(&"phase7_industry_residual_quality_v1"));
        assert!(first_four_combos.contains(&"phase7_quality_relative_strength_v1"));
        assert!(bundle.plan.trials.iter().take(8).all(|trial| {
            trial.parameters["broad_financial_feature_sampling"] == "stratified_seed_v1"
        }));
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fh"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_dividend_quality_v1"
                || trial.parameters["combo_name"] == "phase7_cashflow_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_native_alpha_fusion_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_fj".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_native_alpha_fusion_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-p7-wf-wide-qgvrel-v1-201602-202605"])
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fj"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_four_families = bundle
            .plan
            .trials
            .iter()
            .take(4)
            .map(|trial| {
                trial.parameters["alpha_source_family"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert!(first_four_families.contains(&"broad_financial"));
        assert!(first_four_families.contains(&"event_surprise"));
        assert!(first_four_families.contains(&"event_reaction"));
        assert!(first_four_families.contains(&"prediction_confirmation"));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["native_alpha_fusion_profile"] == "broad_financial_core"
                && trial.parameters["alpha_source_family"] == "broad_financial"
        }));
        let event_surprise_trial = bundle
            .plan
            .trials
            .iter()
            .find(|trial| trial.parameters["alpha_source_family"] == "event_surprise")
            .expect("phase7_fj should include an event surprise trial");
        assert_eq!(
            event_surprise_trial.parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            event_surprise_trial.parameters["event_gate_combo_name"],
            "phase7_event_surprise_v1"
        );
        assert_ne!(
            event_surprise_trial.parameters["combo_name"],
            "phase7_quality_event_surprise_confirm_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["native_alpha_fusion_profile"] == "event_reaction_segments"
                && trial.parameters["alpha_source_family"] == "event_reaction"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["native_alpha_fusion_profile"] == "prediction_confirmation"
                && trial.parameters["alpha_source_family"] == "prediction_confirmation"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
        }));
        let prediction_trial = bundle
            .plan
            .trials
            .iter()
            .find(|trial| trial.parameters["alpha_source_family"] == "prediction_confirmation")
            .expect("phase7_fj should include a prediction confirmation trial");
        assert_eq!(
            prediction_trial.parameters["candidate_ranking"],
            "capacity_aware_alpha_liquidity_v1"
        );
        assert_eq!(
            prediction_trial.parameters["capacity_risk_budget"],
            "capacity_stress_participation_alpha_headroom_floor_70_v1"
        );
        assert_eq!(prediction_trial.parameters["max_position_pct"], "0.08");
        assert_eq!(
            prediction_trial.parameters["universe_profile"],
            "listed_non_st"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_trainable_alpha_admission_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_ft".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_trainable_alpha_admission_discovery"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ft"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_six_families = bundle
            .plan
            .trials
            .iter()
            .take(6)
            .map(|trial| {
                trial.parameters["alpha_source_family"]
                    .as_str()
                    .unwrap_or_default()
            })
            .collect::<BTreeSet<_>>();
        assert!(first_six_families.contains("broad_financial"));
        assert!(first_six_families.contains("residual_confirm"));
        assert!(first_six_families.contains("value_recovery"));
        assert!(bundle.plan.trials.iter().take(12).all(|trial| {
            let combo_name = trial.parameters["combo_name"].as_str().unwrap_or_default();
            quant_common::phase7::is_phase7_base_trainable_alpha(combo_name)
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_surprise_v1"
                || trial.parameters["combo_name"] == "phase7_event_window_earnings_v1"
                || trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
                || trial.parameters["combo_name"]
                    == "phase7_quality_event_reaction_segments_overlay_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_capacity_dual_objective_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fl".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_capacity_dual_objective"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-p7-wf-wide-qgvrel-v1-201602-202605"])
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fl"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_capacity_dual_objective_profile"]
                == "return_anchor_capacity_off"
                && trial.parameters["candidate_ranking"] == "off"
                && trial.parameters["capacity_risk_budget"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_capacity_dual_objective_profile"]
                == "capacity_aware_twap20_blended_headroom70"
                && trial.parameters["candidate_ranking"] == "capacity_aware_alpha_liquidity_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
                && trial.parameters["execution_schedule_profile"] == "twap_20d_v1"
                && trial.parameters["max_position_pct"] == "0.08"
                && trial.parameters["universe_profile"] == "listed_non_st"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_target_gross_signal_fidelity_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fm".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_target_gross_signal_fidelity"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fm"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_target_gross_signal_fidelity_profile"]
                == "gross65_signal_fidelity"
                && trial.parameters["max_gross_exposure"] == "0.65"
                && trial.parameters["candidate_ranking"] == "off"
                && trial.parameters["capacity_risk_budget"] == "off"
                && trial.parameters["cash_utilization"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_confidence_turnover_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fn".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_confidence_turnover_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FN should use PIT prediction as an overlay in seeds instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fn"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_turnover_profile"]
                == "return_anchor_confidence_off"
                && trial.parameters["rebalance_hysteresis_pct"] == "0"
                && trial.parameters["partial_rebalance_ratio"] == "1"
                && trial.parameters["execution_impact_budget"] == "off"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_turnover_profile"]
                == "confidence30_turnover_smooth"
                && trial.parameters["prediction_min_percentile"] == "0.30"
                && trial.parameters["rebalance"] == "160"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.01"
                && trial.parameters["partial_rebalance_ratio"] == "0.75"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_20pct_v1"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_confidence_alpha_lift_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fo".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_confidence_alpha_lift"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FO should use PIT prediction as an overlay in seeds instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fo"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_alpha_lift_profile"]
                == "confidence40_quality_nonlinear_alpha_lift"
                && trial.parameters["market_regime"]
                    == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["prediction_min_percentile"] == "0.40"
                && trial.parameters["rebalance"] == "200"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.02"
                && trial.parameters["partial_rebalance_ratio"] == "0.65"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_alpha_lift_family"] == "event_post_curve"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_confidence_alpha_lift_family"] == "event_reaction"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_reaction_segments_overlay_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_long_horizon_low_turnover_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_fp".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_long_horizon_low_turnover"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FP should use PIT prediction as an overlay in seeds instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fp"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_low_turnover_profile"]
                == "h60_confidence50_low_turnover_quality"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                && trial.parameters["prediction_min_percentile"] == "0.50"
                && trial.parameters["rebalance"] == "240"
                && trial.parameters["rebalance_hysteresis_pct"] == "0.03"
                && trial.parameters["partial_rebalance_ratio"] == "0.60"
                && trial.parameters["execution_impact_budget"] == "impact_turnover_15pct_v1"
                && trial.parameters["execution_carry_policy"] == "roll_forward_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_low_turnover_profile"]
                == "h60_confidence40_gross65_signal_fidelity"
                && trial.parameters["max_gross_exposure"] == "0.65"
        }));
    }

    #[test]
    fn phase7_layered_request_overrides_long_horizon_seed_prediction_set() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                " pred-phase7-fu-future-excess-h60-v1-202301-202501 ".to_string()
            ]),
            max_trials: Some(3),
            search_profile: Some("phase7_fp".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-phase7-fu-future-excess-h60-v1-202301-202501"])
        );
        assert_eq!(bundle.plan.trials.len(), 3);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_long_horizon_low_turnover_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-phase7-fu-future-excess-h60-v1-202301-202501"
                && trial.parameters["prediction_label_horizon_days"] == 60
                && trial.parameters["prediction_set_override"] == true
                && trial.parameters["prediction_set_override_source"] == "request"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_prediction_long_horizon_regime_alpha_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fr".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_long_horizon_regime_alpha"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FR should use H60 PIT prediction as an overlay in seeds"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fr"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(
            bundle.plan.trials.iter().all(|trial| {
                trial.parameters["prediction_long_horizon_regime_alpha_profile"].is_string()
                    && trial.parameters["prediction_set_id"]
                        == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                    && trial.parameters["prediction_label_horizon_days"] == 60
            }),
            "phase7_fr tiny smoke should not spill into generic non-H60 cartesian trials"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_regime_alpha_profile"]
                == "h60_value_recovery_regime_alpha"
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                && trial.parameters["prediction_label_horizon_days"] == 60
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["prediction_long_horizon_regime_alpha_family"] == "event_post_curve"
                && trial.parameters["combo_name"]
                    == "phase7_quality_event_post_return_curve_overlay_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_overrides_regime_alpha_seed_prediction_set() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "pred-phase7-fu-risk-adjusted-excess-h60-v1-202301-202501".to_string(),
            ]),
            max_trials: Some(8),
            search_profile: Some("phase7_fr".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-phase7-fu-risk-adjusted-excess-h60-v1-202301-202501"])
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_long_horizon_regime_alpha_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-phase7-fu-risk-adjusted-excess-h60-v1-202301-202501"
                && trial.parameters["prediction_label_horizon_days"] == 60
                && trial.parameters["prediction_set_override"] == true
                && trial.parameters["prediction_set_override_source"] == "request"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_h60_nonlinear_stress_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fw".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_h60_nonlinear_stress_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FW should keep H60 prediction as seed overlay instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fw"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_h60_nonlinear_stress_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605"
                && trial.parameters["prediction_label_horizon_days"] == 60
                && trial.parameters["signal_source"] == "factor_combo"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "relative_strength_alpha_liquidity_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_h120_low_impact_stress_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fz".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_prediction_h120_low_impact_stress_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FZ should keep H120 prediction as seed overlay instead of standalone model_prediction trials"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fz"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_h120_low_impact_stress_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-p7-wf-wide-qgvrel-h120-v1-201602-202605"
                && trial.parameters["prediction_label_horizon_days"] == 120
                && trial.parameters["signal_source"] == "factor_combo"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_ranking"] == "alpha_first_low_impact_v1"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
        }));
    }

    #[test]
    fn phase7_fz_defaults_train_gate_to_execution_quality_requirements() {
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fz"));

        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(gate_policy["min_train_trade_count"], json!(50));
        assert_eq!(
            gate_policy["min_train_final_actual_gross_exposure_pct"],
            json!(0.25)
        );
        assert_eq!(gate_policy["max_train_final_cash_weight_pct"], json!(0.75));
        assert_eq!(
            gate_policy["max_train_final_unfilled_target_gap_pct"],
            json!(0.08)
        );
        assert_eq!(
            gate_policy["min_train_final_execution_fill_ratio"],
            json!(0.90)
        );
    }

    #[test]
    fn phase7_layered_request_overrides_h60_nonlinear_stress_prediction_set() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec![
                "pred-p7fu-fex-h60-smk2-v1-20240902-20250324".to_string()
            ]),
            max_trials: Some(8),
            search_profile: Some("phase7_fw".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!(["pred-p7fu-fex-h60-smk2-v1-20240902-20250324"])
        );
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["prediction_h60_nonlinear_stress_profile"].is_string()
                && trial.parameters["prediction_set_id"]
                    == "pred-p7fu-fex-h60-smk2-v1-20240902-20250324"
                && trial.parameters["prediction_set_override"] == true
                && trial.parameters["prediction_set_override_source"] == "request"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_train_window_nonlinear_ranking_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["should-not-be-used-by-fx".to_string()]),
            max_trials: Some(8),
            search_profile: Some("phase7_fx".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_train_window_nonlinear_ranking_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "FX should ignore request prediction sets because it is a native train-window ranking profile"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fx"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["train_window_nonlinear_ranking_profile"].is_string()
                && trial.parameters["candidate_ranking"] == "nonlinear_regime_alpha_liquidity_v2"
                && trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"]
                == "capacity_stress_participation_alpha_headroom_floor_70_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_train_window_stress_fill_target_exposure_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["should-not-be-used-by-ga".to_string()]),
            max_trials: Some(8),
            search_profile: Some("phase7_ga".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_train_window_stress_fill_target_exposure"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "GA should ignore request prediction sets because it is native train-window construction"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ga"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(gate_policy["min_train_trade_count"], json!(50));
        assert_eq!(
            gate_policy["min_train_final_actual_gross_exposure_pct"],
            json!(0.25)
        );
        assert_eq!(gate_policy["max_train_final_cash_weight_pct"], json!(0.75));
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["train_window_stress_fill_target_exposure_profile"].is_string()
                && trial.parameters["candidate_ranking"] == "nonlinear_regime_alpha_liquidity_v2"
                && trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"]
                == "capacity_stress_participation_alpha_headroom_floor_70_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["capacity_risk_budget"]
                == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_train_window_ml_stress_fill_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: Some(vec!["must-not-override-train-window-ml".to_string()]),
            max_trials: Some(8),
            search_profile: Some("phase7_gb".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_train_window_ml_stress_fill_discovery"
        );
        assert_eq!(
            bundle.search_space["config"]["prediction_set_ids"],
            json!([]),
            "GB must not accept full-history or request-level prediction_set overrides"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_gb"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "prediction_confidence_stress_fill_quality_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_final_actual_gross_exposure_pct"],
            json!(0.35)
        );
        assert_eq!(gate_policy["max_train_final_cash_weight_pct"], json!(0.65));
        assert_eq!(
            gate_policy["min_train_final_execution_fill_ratio"],
            json!(0.95)
        );
        assert_eq!(bundle.plan.trials.len(), 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["train_window_ml_ranking_profile"].is_string()
                && trial.parameters["train_window_ml_feature_profile"]
                    == "phase7_gb_quality_value_recovery_low_impact_v2"
                && trial.parameters["train_window_ml_label_objective"]
                    == "regime_conditional_excess_return"
                && trial.parameters["train_window_ml_pit_policy"]
                    == "train-window rolling fit; no OOS labels"
                && trial.parameters["stress_fill_objective_profile"].is_string()
                && trial.parameters["signal_source"] == "factor_combo"
                && trial.parameters["portfolio_method"] == "stress_fill_aware_risk_budget"
                && trial.parameters["stress_fill_portfolio_construction"]
                    == "ml_score_capacity_correlation_risk_budget_target_exposure_v1"
                && trial.parameters["stress_fill_confidence_exposure"]
                    == "prediction_confidence_ascending_capacity_headroom_v1"
                && trial.parameters["cash_utilization"] == "stress_fill_gross_98_v1"
                && trial.parameters["prediction_confidence_gate_profile"]
                    == "train_positive_raw_score_gate_v1"
                && trial.parameters["train_window_ml_prediction_min_score"].is_string()
                && trial.parameters.get("prediction_min_score").is_none()
                && trial.parameters.get("prediction_set_id").is_none()
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["train_window_ml_ranking_profile"]
                == "nlq_ranker_rae_h45_bucket7_fill95"
                && trial.parameters["capacity_risk_budget"]
                    == "capacity_stress_participation_blended_alpha_headroom_floor_70_v1"
        }));
    }

    #[test]
    fn internal_train_window_ml_prediction_override_only_applies_to_ml_seeds() {
        let mut seed_trials = vec![
            json!({
                "signal_source": "factor_combo",
                "train_window_ml_ranking_profile": "nlq_ranker_rae_h45_bucket7_fill95",
                "stress_fill_objective_profile": "gb_quality",
                "train_window_ml_prediction_min_score": "0.01"
            }),
            json!({
                "signal_source": "factor_combo",
                "prediction_set_id": "existing-static-prediction"
            }),
        ];

        apply_internal_train_window_ml_prediction_set_to_seed_trials(
            &mut seed_trials,
            "p7gb-train-window-001",
        );

        assert_eq!(seed_trials[0]["prediction_set_id"], "p7gb-train-window-001");
        assert_eq!(seed_trials[0]["prediction_blend_weight"], "0.20");
        assert_eq!(seed_trials[0]["prediction_min_percentile"], "0.30");
        assert_eq!(seed_trials[0]["prediction_min_score"], "0.01");
        assert_eq!(
            seed_trials[0]["prediction_set_override_source"],
            "train_window_ml_internal"
        );
        assert_eq!(
            seed_trials[1]["prediction_set_id"],
            "existing-static-prediction"
        );
        assert!(seed_trials[1]
            .get("prediction_set_override_source")
            .is_none());
    }

    #[test]
    fn train_window_ml_oos_parameters_swap_to_test_prediction_set() {
        let train_parameters = json!({
            "signal_source": "factor_combo",
            "combo_name": "phase7_financial_quality_v1",
            "prediction_set_id": "p7gb-train-window-001",
            "prediction_set_override_source": "train_window_ml_internal"
        });
        let sets = TrainWindowMlPredictionSets {
            train_prediction_set_id: "p7gb-train-window-001".to_string(),
            test_prediction_set_id: "p7gb-test-window-001".to_string(),
            training_task_id: "train-p7gb-window-001".to_string(),
        };

        let oos_parameters =
            train_window_ml_oos_parameters(&train_parameters, Some(&sets)).expect("oos params");

        assert_eq!(oos_parameters["prediction_set_id"], "p7gb-test-window-001");
        assert_eq!(
            oos_parameters["train_window_ml_train_prediction_set_id"],
            "p7gb-train-window-001"
        );
        assert_eq!(
            oos_parameters["train_window_ml_training_task_id"],
            "train-p7gb-window-001"
        );
        assert_eq!(
            oos_parameters["prediction_set_override_source"],
            "train_window_ml_internal_oos"
        );
    }

    #[test]
    fn train_window_ml_factor_refs_use_materialized_pit_atomic_features() {
        let factors = phase7_train_window_ml_factor_refs_for_profile(
            phase7_train_window_ml_feature_profile(),
        );

        assert_eq!(
            phase7_train_window_ml_feature_profile(),
            "phase7_gb_quality_value_recovery_low_impact_v2"
        );
        assert!(factors.len() >= 45);
        assert!(factors
            .iter()
            .all(|factor| factor.factor_version == "1.0.0"));
        assert!(factors
            .iter()
            .all(|factor| !factor.factor_code.starts_with("phase7_")));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_roe_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_current_ratio_indrel_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_debt_to_assets_indrel_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_netprofit_margin_yoy_delta_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_roa_daily_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "val_pb_low_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "cf_ocf_to_profit_latest_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "amihud_20d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "val_pe_ttm_low_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "fin_roe_yoy_delta_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mkt_rel_mom_60d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "div_stability_4y_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "div_cash_sum_4y_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mf_net_amount_5d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mf_elg_net_amount_5d_std"));
        assert!(factors
            .iter()
            .any(|factor| factor.factor_code == "mf_small_sell_pressure_20d_std"));
    }

    #[test]
    fn phase7_layered_request_accepts_current_event_nonlinear_alpha_discovery_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_fs".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_current_event_nonlinear_alpha_discovery"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_fs"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["current_event_nonlinear_alpha_profile"].is_string()
                && trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] != "phase7_quality_event_surprise_confirm_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["current_event_nonlinear_alpha_family"] == "event_surprise"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_surprise_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["current_event_nonlinear_alpha_family"] == "event_window"
                && matches!(
                    trial.parameters["event_gate_combo_name"].as_str(),
                    Some("phase7_event_window_earnings_v1" | "phase7_event_window_earnings_40d_v1")
                )
        }));
        for trial in &bundle.plan.trials {
            let serialized = trial.parameters.to_string();
            assert!(!serialized.contains("event_reaction"));
            assert!(!serialized.contains("post_return_curve"));
        }
    }

    #[test]
    fn phase7_layered_request_accepts_oos_regime_alpha_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ev".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_oos_regime_alpha_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ev"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ev should seed OOS regime alpha rebuild first");
        assert_eq!(
            first_trial.parameters["oos_regime_alpha_rebuild_profile"],
            "oos_rebuild_quality_bull_top30_vol22"
        );
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(first_trial.parameters["candidate_risk_filter"], "off");
        assert_eq!(first_trial.parameters["cash_utilization"], "off");
        assert_eq!(first_trial.parameters["execution_impact_budget"], "off");
    }

    #[test]
    fn phase7_layered_request_accepts_oos_benchmark_excess_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ew".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_oos_benchmark_excess_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ew"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ew should seed OOS benchmark-excess rebuild first");
        assert_eq!(
            first_trial.parameters["oos_benchmark_excess_rebuild_profile"],
            "oos_excess_event_nonlinear_top100_vol24"
        );
        assert_eq!(
            first_trial.parameters["portfolio_volatility_control"],
            "vol120_24_70_100"
        );
        assert_eq!(
            first_trial.parameters["market_regime"],
            "quality_nonlinear_alpha_risk_memory_router_v3"
        );
        assert_eq!(first_trial.parameters["cash_utilization"], "off");
        assert_eq!(first_trial.parameters["execution_impact_budget"], "off");
    }

    #[test]
    fn phase7_layered_request_accepts_oos_execution_adaptive_rebuild_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(24),
            search_profile: Some("phase7_ex".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_execution_oos_execution_adaptive_rebuild"
        );
        let gate_policy =
            default_oos_train_selection_gate_policy_for_search_profile(Some("phase7_ex"));
        assert_eq!(
            gate_policy["train_stress_score_profile"],
            "capacity_stress_return_score_v1"
        );
        assert_eq!(
            gate_policy["min_train_cost_capacity_perturbation_pass_ratio"],
            json!(0.80)
        );
        let first_trial = bundle
            .plan
            .trials
            .first()
            .expect("phase7_ex should seed OOS execution-adaptive rebuild first");
        assert_eq!(
            first_trial.parameters["oos_execution_adaptive_rebuild_profile"],
            "oos_execution_adaptive_event_top100_vol24"
        );
        assert_eq!(
            first_trial.parameters["portfolio_sharpe_control"],
            "gentle_roll_sharpe180_025_neg20_70"
        );
        assert_eq!(
            first_trial.parameters["capacity_risk_budget"],
            "capacity_stress_participation_headroom_floor_70_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_schedule_profile"],
            "twap_10d_v1"
        );
        assert_eq!(
            first_trial.parameters["execution_carry_policy"],
            "roll_forward_v1"
        );
    }

    #[test]
    fn phase7_layered_request_accepts_return_alpha_sharpe_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(16),
            search_profile: Some("phase7_cv".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_return_alpha_sharpe_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 16);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial.parameters["risk_budget_lookback_days"] == 170
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_frontier_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cw".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_frontier_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v2"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_frontier_decomposition_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cx".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_frontier_decomposition"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v4"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_141_462_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v5"
                && trial.parameters["risk_contribution_control"] == "off"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_high_sharpe_return_micro_bridge_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cy".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_high_sharpe_return_micro_bridge"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_frontier_regime_bridge_router_v6"
                && trial.parameters["portfolio_volatility_control"] == "vol120_147_475_100"
                && trial.parameters["portfolio_sharpe_min_exposure"] == "0.70"
        }));
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["candidate_risk_filter"] == "off"
        }));
    }

    #[test]
    fn cache_stats_delta_reports_per_batch_increments_for_discovery_cache_reuse() {
        let before_signal = SignalDataCacheStats {
            combo_score_hits: 10,
            combo_score_misses: 4,
            prediction_score_hits: 2,
            prediction_score_misses: 1,
            return_history_hits: 100,
            return_history_covering_window_hits: 12,
            return_history_snapshot_hits: 7,
            average_amount_hits: 70,
            average_amount_symbol_hits: 28,
            average_amount_history_hits: 42,
            average_amount_history_covering_window_hits: 14,
            average_amount_history_snapshot_hits: 8,
            average_amount_symbol_misses: 11,
            average_amount_history_misses: 39,
            return_history_misses: 50,
            average_amount_misses: 50,
            ..Default::default()
        };
        let after_signal = SignalDataCacheStats {
            combo_score_hits: 13,
            combo_score_misses: 4,
            prediction_score_hits: 8,
            prediction_score_misses: 1,
            return_history_hits: 130,
            return_history_covering_window_hits: 20,
            return_history_snapshot_hits: 16,
            average_amount_hits: 95,
            average_amount_symbol_hits: 40,
            average_amount_history_hits: 55,
            average_amount_history_covering_window_hits: 20,
            average_amount_history_snapshot_hits: 18,
            average_amount_symbol_misses: 13,
            average_amount_history_misses: 42,
            return_history_misses: 55,
            average_amount_misses: 55,
            ..Default::default()
        };
        let signal_delta = signal_cache_stats_delta(before_signal, after_signal);

        assert_eq!(signal_delta.combo_score_hits, 3);
        assert_eq!(signal_delta.combo_score_misses, 0);
        assert_eq!(signal_delta.prediction_score_hits, 6);
        assert_eq!(signal_delta.prediction_score_misses, 0);
        assert_eq!(signal_delta.return_history_hits, 30);
        assert_eq!(signal_delta.return_history_covering_window_hits, 8);
        assert_eq!(signal_delta.return_history_snapshot_hits, 9);
        assert_eq!(signal_delta.average_amount_hits, 25);
        assert_eq!(signal_delta.average_amount_symbol_hits, 12);
        assert_eq!(signal_delta.average_amount_history_hits, 13);
        assert_eq!(signal_delta.average_amount_history_covering_window_hits, 6);
        assert_eq!(signal_delta.average_amount_history_snapshot_hits, 10);
        assert_eq!(signal_delta.return_history_misses, 5);
        assert_eq!(signal_delta.average_amount_misses, 5);
        assert_eq!(signal_delta.average_amount_symbol_misses, 2);
        assert_eq!(signal_delta.average_amount_history_misses, 3);

        let before_backtest = BacktestDataCacheStats {
            daily_bar_symbol_hits: 50,
            daily_bar_covering_window_hits: 10,
            daily_bar_snapshot_hits: 5,
            daily_bar_symbol_misses: 20,
            trading_profile_symbol_hits: 40,
            trading_profile_symbol_misses: 20,
            ..Default::default()
        };
        let after_backtest = BacktestDataCacheStats {
            daily_bar_symbol_hits: 80,
            daily_bar_covering_window_hits: 22,
            daily_bar_snapshot_hits: 9,
            daily_bar_symbol_misses: 22,
            trading_profile_symbol_hits: 70,
            trading_profile_symbol_misses: 22,
            ..Default::default()
        };
        let backtest_delta = backtest_cache_stats_delta(before_backtest, after_backtest);

        assert_eq!(backtest_delta.daily_bar_symbol_hits, 30);
        assert_eq!(backtest_delta.daily_bar_covering_window_hits, 12);
        assert_eq!(backtest_delta.daily_bar_snapshot_hits, 4);
        assert_eq!(backtest_delta.daily_bar_symbol_misses, 2);
        assert_eq!(backtest_delta.trading_profile_symbol_hits, 30);
        assert_eq!(backtest_delta.trading_profile_symbol_misses, 2);
    }

    #[test]
    fn aggregate_signal_cache_stats_collects_oos_cache_and_train_batches_without_double_counting() {
        let train_signal = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_hits: 2,
            persistent_return_risk_feature_matrix_return_values_loaded: 500_482,
            ..Default::default()
        };
        let train_prewarm = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_return_values_loaded: 999_999,
            ..Default::default()
        };
        let oos_signal = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_hits: 1,
            persistent_return_risk_feature_matrix_return_values_loaded: 250_000,
            ..Default::default()
        };
        let oos_prewarm = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_return_values_loaded: 888_888,
            ..Default::default()
        };
        let metrics = json!({
            "cache": {
                "oos_signal_cache": oos_signal,
            },
            "windows": [{
                "train_batches": [{
                    "signal_cache": train_signal,
                    "signal_batch_prewarm": {
                        "report": {
                            "cache_delta": train_prewarm,
                        }
                    }
                }],
                "oos_market_feature_prewarm_report": {
                    "cache_delta": oos_prewarm,
                }
            }]
        });

        let stats = aggregate_signal_cache_stats_from_experiment_metrics(&metrics);

        assert_eq!(stats.persistent_return_risk_feature_matrix_hits, 3);
        assert_eq!(
            stats.persistent_return_risk_feature_matrix_return_values_loaded,
            750_482
        );
    }

    #[test]
    fn return_risk_cache_economics_report_json_prefers_stats_for_steady_state_pair() {
        let raw = SignalDataCacheStats {
            persistent_return_risk_feature_matrix_hits: 2,
            persistent_return_risk_feature_matrix_return_values_loaded: 500_482,
            ..Default::default()
        };
        let stats = SignalDataCacheStats {
            persistent_return_risk_stats_feature_matrix_hits: 4,
            persistent_return_risk_stats_feature_matrix_misses: 0,
            persistent_return_risk_stats_feature_matrix_writes: 0,
            persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 8_512,
            persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 191_580,
            ..Default::default()
        };

        let report = return_risk_cache_economics_report_json("exp-raw", raw, "exp-stats", stats);

        assert_eq!(report["raw_experiment_run_id"], "exp-raw");
        assert_eq!(report["stats_experiment_run_id"], "exp-stats");
        assert_eq!(
            report["cache_economics"]["recommendation"],
            "PreferStatsMatrix"
        );
        assert_eq!(
            report["cache_economics"]["reason"],
            "stats_payload_below_raw_return_values"
        );
    }

    #[test]
    fn return_risk_cache_economics_report_rejects_non_completed_inputs() {
        assert!(ensure_completed_cache_economics_input("exp-ok", "raw", "completed").is_ok());

        let error = ensure_completed_cache_economics_input("exp-failed", "stats", "failed")
            .expect_err("failed experiment should not be reportable");
        assert!(error.contains("requires completed stats experiment exp-failed"));
        assert!(error.contains("status=failed"));
    }

    #[test]
    fn factor_batch_signal_prewarm_plan_extracts_only_valid_factor_requests() {
        let task = OptimizationTaskExecutionContext {
            strategy_version_id: "phase7-professional-v1".into(),
            data_version_id: "full-market-2016-v1".into(),
            backtest_template: json!({
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
                "start_date": "20200101",
                "end_date": "20230101",
                "benchmark": "000300.SH",
                "top_n": 20,
                "rebalance": "20",
                "max_position_pct": 0.08,
                "return_risk_feature_cache_mode": "stats_matrix_experimental"
            }),
            objective: json!({"type": "professional_candidate"}),
            constraints: None,
        };
        let pending_trials = vec![
            (
                "trial-factor-a".to_string(),
                1,
                json!({"top_n": 18, "market_regime": "quality_bear_window_guard_v2"}),
            ),
            (
                "trial-prediction".to_string(),
                2,
                json!({
                    "signal_source": "prediction",
                    "prediction_set_id": "pred-phase7-v1"
                }),
            ),
            (
                "trial-invalid".to_string(),
                3,
                json!({"top_n": {"bad": "shape"}}),
            ),
            (
                "trial-factor-b".to_string(),
                4,
                json!({"rebalance": "60", "score_candidate_pool_size": 600}),
            ),
        ];

        let plan = plan_factor_signal_batch_prewarm_requests(&task, &pending_trials);

        assert_eq!(plan.requested_trials, 4);
        assert_eq!(plan.factor_requests.len(), 2);
        assert_eq!(plan.prediction_trials, 1);
        assert_eq!(plan.invalid_trials, 1);
        assert_eq!(plan.factor_requests[0].top_n, 18);
        assert_eq!(
            plan.factor_requests[0]
                .market_regime
                .as_ref()
                .unwrap()
                .policy,
            Some("quality_bear_window_guard_v2".to_string())
        );
        assert_eq!(plan.factor_requests[1].rebalance, "60");
        assert_eq!(plan.factor_requests[1].score_candidate_pool_size, Some(600));
        assert_eq!(
            plan.factor_requests[0]
                .return_risk_feature_cache_mode
                .as_deref(),
            Some("stats_matrix_experimental")
        );
        assert_eq!(
            plan.factor_requests[1]
                .return_risk_feature_cache_mode
                .as_deref(),
            Some("stats_matrix_experimental")
        );
    }

    #[test]
    fn parallel_trial_cache_forks_start_from_shared_snapshot_without_parent_stats() {
        let parent_signal_cache = SignalDataCache::default();
        let parent_backtest_cache = BacktestDataCache::default();
        let signal_snapshot = parent_signal_cache.snapshot();
        let backtest_snapshot = parent_backtest_cache.snapshot();

        let signal_fork = SignalDataCache::from_snapshot(&signal_snapshot);
        let backtest_fork = BacktestDataCache::from_snapshot(&backtest_snapshot);

        assert_eq!(parent_signal_cache.stats().return_history_hits, 0);
        assert_eq!(parent_backtest_cache.stats().daily_bar_symbol_hits, 0);
        assert_eq!(signal_fork.stats().return_history_hits, 0);
        assert_eq!(backtest_fork.stats().daily_bar_symbol_hits, 0);
    }

    #[test]
    fn elite_validation_plateau_scores_stable_parameter_neighbors() {
        let candidate = report_trial(
            "candidate",
            1,
            json!({"top_n": 20, "rebalance": "60", "max_pairwise_correlation": "0.70"}),
            json!({
                "annual_return_pct": 0.1505,
                "excess_return_pct": 0.05,
                "sharpe_ratio": 1.02,
                "sortino_ratio": 1.733,
                "calmar_ratio": 0.715,
                "profit_factor": 1.20,
                "max_drawdown_pct": 0.2105,
                "max_drawdown_duration_days": 90,
                "num_trades": 260
            }),
        );
        let stable_neighbor = report_trial(
            "stable",
            2,
            json!({"top_n": 21, "rebalance": "60", "max_pairwise_correlation": "0.70"}),
            json!({
                "annual_return_pct": 0.147,
                "excess_return_pct": 0.04,
                "sharpe_ratio": 0.96,
                "sortino_ratio": 1.62,
                "calmar_ratio": 0.69,
                "profit_factor": 1.15,
                "max_drawdown_pct": 0.225,
                "max_drawdown_duration_days": 95,
                "num_trades": 255
            }),
        );
        let unstable_neighbor = report_trial(
            "unstable",
            3,
            json!({"top_n": 20, "rebalance": "55", "max_pairwise_correlation": "0.70"}),
            json!({
                "annual_return_pct": 0.120,
                "excess_return_pct": 0.01,
                "sharpe_ratio": 0.75,
                "sortino_ratio": 1.10,
                "calmar_ratio": 0.50,
                "profit_factor": 1.0,
                "max_drawdown_pct": 0.31,
                "max_drawdown_duration_days": 160,
                "num_trades": 240
            }),
        );
        let trials = vec![
            candidate.clone(),
            stable_neighbor.clone(),
            unstable_neighbor.clone(),
        ];

        let plateau = build_parameter_plateau_analysis(&candidate, &trials);

        assert_eq!(plateau["near_neighbor_count"], 2);
        assert_eq!(plateau["stable_neighbor_count"], 1);
        assert_eq!(plateau["single_axis"][0]["parameter"], "rebalance");
        assert_eq!(plateau["single_axis"][1]["parameter"], "top_n");
    }

    #[test]
    fn portfolio_correlation_contribution_scores_explicit_and_peer_controls() {
        let candidate = report_trial(
            "controlled",
            1,
            json!({
                "portfolio_method": "risk_budget",
                "max_pairwise_correlation": "0.70",
                "correlation_lookback_days": 120,
                "candidate_risk_filter": "low_volatility_low_correlation_v1",
                "risk_contribution_control": "soft_single_name_20pct_v1"
            }),
            json!({
                "annual_return_pct": 0.16,
                "excess_return_pct": 0.05,
                "sharpe_ratio": 1.05,
                "sortino_ratio": 1.70,
                "calmar_ratio": 0.75,
                "profit_factor": 1.30,
                "max_drawdown_pct": 0.22,
                "num_trades": 250
            }),
        );
        let uncontrolled = report_trial(
            "uncontrolled",
            2,
            json!({"portfolio_method": "heuristic"}),
            json!({
                "annual_return_pct": 0.17,
                "excess_return_pct": 0.06,
                "sharpe_ratio": 0.75,
                "sortino_ratio": 1.30,
                "calmar_ratio": 0.45,
                "profit_factor": 1.05,
                "max_drawdown_pct": 0.38,
                "num_trades": 260
            }),
        );
        let trials = vec![candidate.clone(), uncontrolled];

        let contribution = build_portfolio_correlation_contribution_score(&candidate, &trials);

        assert!(
            contribution["explicit_control_score"].as_f64().unwrap() > 0.85,
            "{contribution}"
        );
        assert!(
            contribution["contribution_score"].as_f64().unwrap() > 0.70,
            "{contribution}"
        );
        assert_eq!(contribution["active_controls"].as_array().unwrap().len(), 5);
    }

    #[test]
    fn elite_metric_enrichment_marks_sources_and_missing_fields() {
        let mut metrics = json!({
            "annual_return_pct": 0.16,
            "excess_return_pct": 0.02,
            "sharpe_ratio": 1.1,
            "sortino_ratio": 1.7,
            "max_drawdown_pct": 0.22,
            "num_trades": 320
        });
        let mut sources = existing_metric_sources(&metrics);

        insert_metric_if_missing(
            &mut metrics,
            &mut sources,
            "calmar_ratio",
            Some(json!(0.72)),
            "backtest_result.calmar_ratio",
        );

        let missing = missing_elite_metrics(&metrics);
        assert_eq!(
            sources["annual_return_pct"],
            json!("optimization_trial.metrics")
        );
        assert_eq!(
            sources["calmar_ratio"],
            json!("backtest_result.calmar_ratio")
        );
        assert!(!missing.contains(&"calmar_ratio"));
        assert!(missing.contains(&"profit_factor"));
        assert!(missing.contains(&"max_drawdown_duration_days"));
    }

    #[test]
    fn phase7_layered_request_accepts_return_distribution_repair_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(12),
            search_profile: Some("phase7_cp".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_return_distribution_repair"
        );
        assert_eq!(bundle.plan.planned_trials, 12);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters.get("prediction_set_id").is_none()
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial.parameters["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial.parameters["event_gate_profile"] == "event_window_10d_boost_p75_3pct"
                && trial.parameters["event_gate_combo_name"]
                    == "phase7_event_window_earnings_10d_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "residual_confirm_top40"
                && trial.parameters["event_gate_combo_name"]
                    == "phase7_quality_residual_confirm_10pct_v1"
                && trial.parameters["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_alpha_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bj".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_alpha_router"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_regime_alpha_overlay_value_05pct_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_state_position_risk_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bk".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_state_position_risk_router"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().all(|trial| {
            trial.parameters["combo_name"] == "phase7_financial_quality_v1"
                && trial.parameters["portfolio_method"] == "risk_budget"
                && trial.parameters["risk_budget_lookback_days"] == 180
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_position_risk_router_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bl".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_position_risk_router"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_position_guard_v3"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial.parameters["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_event_window_position_guard_v2"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_all_regime_event_sleeve_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: Some("phase7_bm".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_all_regime_event_sleeve"
        );
        assert_eq!(bundle.plan.planned_trials, 8);
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial.parameters["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_all_regime_event_window_sleeve_15pct_v1"
                && trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_volatility_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(4),
            search_profile: Some("phase7_ai".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_volatility_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 4);
        assert_eq!(
            bundle.plan.trials[0].parameters["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_20_60_100"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_regime_position_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_aj".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_regime_position_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v1"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["market_regime"] == "quality_bear_position_guard_v2"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_anti_overfit_sharpe_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_ab".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_anti_overfit_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            bundle.plan.trials[0].parameters["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_boost_pos_5pct"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
        assert!(!bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_earnings_v1"
                || trial.parameters["combo_name"] == "phase7_event_surprise_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_candidate_risk_filter_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_ac".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_candidate_risk_filter"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["candidate_risk_filter"],
            "off"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_risk_contribution_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(6),
            search_profile: Some("phase7_ad".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_risk_contribution"
        );
        assert_eq!(bundle.plan.planned_trials, 6);
        assert_eq!(
            bundle.plan.trials[0].parameters["risk_contribution_control"],
            "off"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial.parameters["combo_name"] == "phase7_financial_quality_v1"
        }));
    }

    #[test]
    fn phase7_layered_request_accepts_event_conditioned_profile() {
        let req = Phase7LayeredOptimizationRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: json!({"type": "professional_candidate", "benchmark": "000300.SH"}),
            constraints: None,
            walk_forward: None,
            backtest_template: Some(json!({
                "start_date": "20160201",
                "end_date": "20260515",
                "initial_capital": 1000000.0
            })),
            prediction_set_ids: None,
            max_trials: Some(10),
            search_profile: Some("phase7_ae".to_string()),
        };
        let resource_plan = quant_common::phase7::LocalResourcePlan::for_machine(10, 32);

        let bundle = build_phase7_layered_plan_bundle(&req, resource_plan);

        assert_eq!(
            bundle.search_space["search_profile"],
            "professional_event_conditioned_sharpe"
        );
        assert_eq!(bundle.plan.planned_trials, 10);
        assert_eq!(
            bundle.plan.trials[0].parameters["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_surprise_boost_pos_5pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial.parameters["event_gate_mode"] == "boost_positive"
        }));
        assert!(bundle.plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
                && trial.parameters["market_regime"] == "quality_bear_window_guard_v2"
        }));
    }

    #[test]
    fn professional_discovery_defaults_backtest_template_to_full_history_window() {
        let req = Phase7ProfessionalDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials: Some(8),
            search_profile: None,
            trial_batch_limit: Some(2),
            max_batches: Some(4),
            robustness_top_n: Some(3),
            robustness_gate_policy: None,
            stop_after_professional_candidate: Some(false),
            stop_after_robust_approval: Some(true),
        };

        let layered_req = phase7_discovery_layered_request(&req);

        let template = layered_req.backtest_template.expect("default template");
        assert_eq!(template["start_date"], "20160201");
        assert_eq!(template["end_date"], "20260515");
        assert_eq!(template["benchmark"], "000300.SH");
        assert_eq!(template["mode"], "standard");
        assert_eq!(template["effective_coverage"]["mode"], "adjust_start");
    }

    #[test]
    fn discovery_candidate_order_prefers_lower_professional_gap_over_raw_return() {
        let high_return_high_drawdown = DiscoveryCandidate {
            trial_id: "high-return".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(2, 1)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::new(75, 2),
            metrics: CandidateMetrics {
                annual_return: Decimal::new(19, 2),
                excess_return: Decimal::new(4, 2),
                sharpe: Decimal::new(60, 2),
                sortino: Decimal::new(11, 1),
                max_drawdown: Decimal::new(55, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let lower_return_better_risk = DiscoveryCandidate {
            trial_id: "better-risk".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(1, 1)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::new(25, 2),
            metrics: CandidateMetrics {
                annual_return: Decimal::new(151, 3),
                excess_return: Decimal::new(3, 2),
                sharpe: Decimal::new(85, 2),
                sortino: Decimal::new(13, 1),
                max_drawdown: Decimal::new(38, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({}),
        };
        let mut candidates = vec![high_return_high_drawdown, lower_return_better_risk];

        candidates.sort_by(discovery_candidate_order);

        assert_eq!(candidates[0].trial_id, "better-risk");
    }

    #[test]
    fn professional_discovery_only_approved_robustness_counts_as_found() {
        assert!(robustness_result_is_approved(&json!({
            "status": "approved_candidate"
        })));
        assert!(!robustness_result_is_approved(&json!({
            "status": "rejected"
        })));
        assert!(!robustness_result_is_approved(&json!({
            "status": "review_required"
        })));
    }

    #[test]
    fn risk_adjusted_score_records_constraint_violations() {
        let mut metrics = quant_backtest::metrics::BacktestMetrics::default();
        metrics.information_ratio = Decimal::new(8, 1);
        metrics.excess_return_pct = Decimal::new(12, 2);
        metrics.max_drawdown_pct = Decimal::new(25, 2);
        metrics.turnover = Decimal::new(12, 0);
        metrics.num_trades = 3;

        let scored = score_trial(
            &metrics,
            &json!({"type": "risk_adjusted", "maximize": true}),
            Some(&json!({
                "max_drawdown": 0.20,
                "max_turnover": 8.0,
                "min_trade_count": 10,
                "min_information_ratio": 1.0
            })),
        );

        assert!(scored.score < Decimal::new(8, 1));
        let violations = scored
            .constraint_violations
            .as_array()
            .expect("violations array");
        assert_eq!(violations.len(), 4);
        assert!(violations
            .iter()
            .any(|item| item["constraint"] == "min_trade_count"));
    }

    #[test]
    fn professional_candidate_score_prefers_sharpe_gap_after_annual_floor() {
        let mut high_return_low_sharpe = quant_backtest::metrics::BacktestMetrics::default();
        high_return_low_sharpe.annual_return_pct = Decimal::new(19, 2);
        high_return_low_sharpe.excess_return_pct = Decimal::new(400, 2);
        high_return_low_sharpe.sharpe_ratio = Decimal::new(82, 2);
        high_return_low_sharpe.sortino_ratio = Decimal::new(20, 1);
        high_return_low_sharpe.max_drawdown_pct = Decimal::new(24, 2);
        high_return_low_sharpe.num_trades = 2000;

        let mut lower_return_better_sharpe = quant_backtest::metrics::BacktestMetrics::default();
        lower_return_better_sharpe.annual_return_pct = Decimal::new(151, 3);
        lower_return_better_sharpe.excess_return_pct = Decimal::new(220, 2);
        lower_return_better_sharpe.sharpe_ratio = Decimal::new(95, 2);
        lower_return_better_sharpe.sortino_ratio = Decimal::new(17, 1);
        lower_return_better_sharpe.max_drawdown_pct = Decimal::new(23, 2);
        lower_return_better_sharpe.num_trades = 1800;

        let objective = json!({"type": "professional_candidate"});
        let constraints = json!({
            "min_annual_return": 0.15,
            "min_excess_return": 0.0,
            "min_sharpe": 1.0,
            "min_sortino": 1.5,
            "max_drawdown": 0.35,
            "min_trade_count": 1
        });

        let high_return_score =
            score_trial(&high_return_low_sharpe, &objective, Some(&constraints)).score;
        let better_sharpe_score =
            score_trial(&lower_return_better_sharpe, &objective, Some(&constraints)).score;

        assert!(
            better_sharpe_score > high_return_score,
            "professional objective should rank smaller Sharpe gap above raw return once annual floor is met"
        );
    }

    #[test]
    fn scoring_and_robustness_preserve_effective_coverage_metadata() {
        let mut metrics = quant_backtest::metrics::BacktestMetrics::default();
        metrics.annual_return_pct = Decimal::new(18, 2);
        metrics.excess_return_pct = Decimal::new(10, 2);
        metrics.sharpe_ratio = Decimal::new(8, 1);
        metrics.sortino_ratio = Decimal::new(18, 1);
        metrics.max_drawdown_pct = Decimal::new(30, 2);
        metrics.num_trades = 128;
        metrics.execution_schedule_expired_count = 2;
        metrics.max_execution_target_gap_pct = Decimal::new(12, 2);
        metrics.final_cash_weight_pct = Decimal::new(18, 2);
        let output = FactorBacktestRunOutput {
            signals_count: 10,
            metrics,
            trades: 128,
            equity_points: 512,
            effective_coverage: Some(crate::routes::backtest::EffectiveCoverageRunSummary {
                requested_start_date: NaiveDate::from_ymd_opt(2016, 2, 1).unwrap(),
                effective_start_date: NaiveDate::from_ymd_opt(2016, 3, 4).unwrap(),
                adjusted: true,
                mode: "adjust_start".to_string(),
                min_rows: 20,
                observed_rows: 3470,
                coverage_start_date: NaiveDate::from_ymd_opt(2016, 2, 1).unwrap(),
                warmup_start_date: Some(NaiveDate::from_ymd_opt(2016, 3, 4).unwrap()),
                warmup_trading_days: Some(19),
                combo_name: "phase7_financial_quality_v1".to_string(),
                version: "1.0.0".to_string(),
                universe_profile: Some("listed_non_st".to_string()),
            }),
            market_data_prewarm_report: None,
            market_feature_prewarm_report: None,
        };

        let scored = score_trial_with_output(
            &output,
            &json!({"type": "professional_candidate"}),
            Some(&json!({"min_trade_count": 1})),
        );

        assert_eq!(
            scored.metrics["effective_coverage"]["effective_start_date"],
            "2016-03-04"
        );
        assert_eq!(scored.metrics["execution_schedule_expired_count"], 2);
        assert_eq!(scored.metrics["max_execution_target_gap_pct"], "0.12");
        assert_eq!(scored.metrics["final_cash_weight_pct"], "0.18");
        let evaluation = evaluate_robustness_gates_with_analysis(
            scored.score,
            None,
            &scored.metrics,
            &scored.constraint_violations,
            Some(&json!({"min_trade_count": 1, "max_drawdown": 0.35})),
            None,
        );
        assert!(evaluation
            .gates
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| { gate["gate"] == "effective_coverage_start" && gate["passed"] == true }));
    }

    #[test]
    fn oos_window_execution_json_includes_market_data_prewarm_report() {
        let execution = OosWindowExecution {
            window: OosDiscoveryWindow {
                window_index: 1,
                validation_mode: "walk_forward".to_string(),
                train_start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                train_end: NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
                test_start: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                test_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            },
            train_optimization_task_id: "train-task-1".to_string(),
            train_execution_policy: OosTrainExecutionPolicy {
                requested_cache_mode: "shared_window",
                cache_mode: "shared_window",
                requested_trial_concurrency: 1,
                trial_concurrency: 1,
            },
            train_batches: Vec::new(),
            selected_candidate: DiscoveryCandidate {
                trial_id: "trial-1".to_string(),
                backtest_task_id: None,
                score: None,
                candidate_type: CandidateType::Professional,
                professional_gap_score: Decimal::ZERO,
                metrics: CandidateMetrics::default(),
                parameters: json!({}),
            },
            train_robustness: None,
            train_cost_capacity_perturbations: Vec::new(),
            oos_backtest_task_id: "oos-task-1".to_string(),
            oos_output: FactorBacktestRunOutput {
                signals_count: 0,
                metrics: quant_backtest::metrics::BacktestMetrics::default(),
                trades: 0,
                equity_points: 0,
                effective_coverage: None,
                market_data_prewarm_report: Some(
                    quant_backtest::runner::BacktestMarketDataPrewarmReport {
                        snapshot_key: quant_backtest::runner::BacktestMarketDataSnapshotKey::new(
                            "full-market-2016-v1",
                            "000300.SH",
                            NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                            NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                            &["AAA".to_string(), "BBB".to_string()],
                        ),
                        start_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                        end_date: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                        benchmark: "000300.SH".to_string(),
                        symbol_count: 2,
                        cache_delta: Default::default(),
                    },
                ),
                market_feature_prewarm_report: Some(
                    quant_backtest::signal_generator::MarketFeaturePrewarmReport {
                        snapshot_key:
                            quant_backtest::signal_generator::MarketFeatureSnapshotKey::new(
                                "full-market-2016-v1",
                                NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                                NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
                                NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                                NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                                60,
                                &["AAA".to_string(), "BBB".to_string()],
                            ),
                        feature_start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                        feature_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
                        symbol_count: 2,
                        lookback_days: 60,
                        return_risk_feature_cache_mode:
                            quant_backtest::signal_generator::ReturnRiskFeatureCacheMode::RawMatrix,
                        cache_delta: Default::default(),
                    },
                ),
            },
            oos_points: Vec::new(),
            cost_capacity_perturbations: Vec::new(),
        };

        let json = oos_window_execution_json(&execution, &json!({}));

        assert!(json.get("oos_market_data_prewarm_report").is_some());
        assert!(json.get("oos_market_feature_prewarm_report").is_some());
    }

    #[test]
    fn oos_window_execution_json_explains_gb_quality_train_stress_score() {
        let candidate = DiscoveryCandidate {
            trial_id: "trial-gb-quality".to_string(),
            backtest_task_id: None,
            score: Some(Decimal::new(50, 0)),
            candidate_type: CandidateType::ReviewRequired,
            professional_gap_score: Decimal::ZERO,
            metrics: CandidateMetrics {
                annual_return: Decimal::new(18, 2),
                excess_return: Decimal::new(8, 2),
                sharpe: Decimal::new(12, 1),
                sortino: Decimal::new(17, 1),
                max_drawdown: Decimal::new(18, 2),
                num_trades: 260,
                final_cash_weight: Decimal::new(24, 2),
                final_actual_gross_exposure: Decimal::new(76, 2),
                final_unfilled_target_gap: Decimal::new(2, 2),
                final_execution_fill_ratio: Decimal::new(98, 2),
                ..CandidateMetrics::default()
            },
            parameters: json!({
                "prediction_confidence_gate_profile": "train_positive_raw_score_gate_v1",
                "prediction_min_score": "0.01",
                "prediction_min_percentile": "0.35",
                "prediction_set_override_source": "train_window_ml_internal"
            }),
        };
        let mut summary = cost_capacity_perturbation_summary_from_counts(3, 3);
        summary.min_calmar = Decimal::new(140, 2);
        summary.avg_calmar = Decimal::new(165, 2);
        summary.avg_sharpe = Decimal::new(120, 2);
        summary.min_sortino = Decimal::new(160, 2);
        summary.min_annual_return = Decimal::new(9, 2);
        summary.max_drawdown = Decimal::new(16, 2);
        summary.min_num_trades = 240;
        summary.max_final_cash_weight = Decimal::new(26, 2);
        summary.min_final_actual_gross_exposure = Decimal::new(74, 2);
        summary.max_final_unfilled_target_gap = Decimal::new(3, 2);
        summary.min_final_execution_fill_ratio = Decimal::new(97, 2);
        let execution = OosWindowExecution {
            window: OosDiscoveryWindow {
                window_index: 1,
                validation_mode: "walk_forward".to_string(),
                train_start: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                train_end: NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
                test_start: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                test_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            },
            train_optimization_task_id: "train-task-1".to_string(),
            train_execution_policy: OosTrainExecutionPolicy {
                requested_cache_mode: "shared_window",
                cache_mode: "shared_window",
                requested_trial_concurrency: 1,
                trial_concurrency: 1,
            },
            train_batches: Vec::new(),
            selected_candidate: candidate,
            train_robustness: None,
            train_cost_capacity_perturbations: Vec::new(),
            oos_backtest_task_id: "oos-task-1".to_string(),
            oos_output: FactorBacktestRunOutput {
                signals_count: 0,
                metrics: quant_backtest::metrics::BacktestMetrics::default(),
                trades: 0,
                equity_points: 0,
                effective_coverage: None,
                market_data_prewarm_report: None,
                market_feature_prewarm_report: None,
            },
            oos_points: Vec::new(),
            cost_capacity_perturbations: Vec::new(),
        };
        let policy = json!({
            "train_stress_score_profile": "prediction_confidence_stress_fill_quality_score_v1",
            "capacity_stress_target_annual_return": 0.15,
            "min_train_perturbed_annual_return": 0.05,
            "min_train_perturbed_calmar": 1.2,
            "min_train_avg_perturbed_sharpe": 0.5,
            "min_train_perturbed_sortino": 1.0,
            "min_train_trade_count": 100,
            "min_train_final_actual_gross_exposure_pct": 0.35,
            "max_train_final_cash_weight_pct": 0.65,
            "max_train_final_unfilled_target_gap_pct": 0.06,
            "min_train_final_execution_fill_ratio": 0.95
        });

        let json = oos_window_execution_json_with_train_summary(&execution, &policy, &summary);
        let breakdown = json["train_stress_score_breakdown"]
            .as_object()
            .expect("train stress score breakdown");

        assert_eq!(
            breakdown["profile"],
            "prediction_confidence_stress_fill_quality_score_v1"
        );
        assert_eq!(breakdown["summary"]["pass_ratio"], 1.0);
        assert_eq!(breakdown["summary"]["min_sortino"], "1.60");
        assert!(breakdown["components"]["prediction_confidence_score"].is_string());
        assert!(breakdown["components"]["pass_ratio_bonus"].is_string());
        assert!(breakdown["components"]["annual_quality_bonus"].is_string());
        assert!(breakdown["components"]["calmar_quality_bonus"].is_string());
        assert!(breakdown["components"]["sharpe_quality_bonus"].is_string());
        assert!(breakdown["components"]["sortino_quality_bonus"].is_string());
        assert!(breakdown["penalties"]["drawdown_tail_penalty"].is_string());
        assert!(breakdown["penalties"]["min_annual_shortfall"].is_string());
        assert!(breakdown["penalties"]["calmar_shortfall"].is_string());
        assert!(breakdown["penalties"]["sharpe_shortfall"].is_string());
        assert!(breakdown["penalties"]["sortino_shortfall"].is_string());
        assert_eq!(
            json["train_stress_adjusted_score"],
            breakdown["total_score"]
        );
    }

    #[test]
    fn candidate_input_uses_best_trial_when_trial_id_is_blank() {
        let req = PromoteOptimizationRequest {
            trial_id: None,
            target_strategy_version: "factor-combo-v1-candidate".into(),
            candidate_name: "phase4c candidate".into(),
            promotion_mode: Some("create_candidate".into()),
            gate_policy: Some("default_v1".into()),
            freeze_after_approval: Some(false),
            reviewer: Some("gaocheng".into()),
            reason: "best completed trial".into(),
            notes: None,
        };

        let normalized = normalize_promote_request("trial-opt-demo-0001", req)
            .expect("normalized promote request");

        assert_eq!(normalized.trial_id, "trial-opt-demo-0001");
        assert_eq!(normalized.status, "candidate");
        assert_eq!(normalized.gate_policy, "default_v1");
    }

    #[test]
    fn trial_reuse_key_is_stable_for_same_template_and_parameters() {
        let template = json!({
            "combo_name": "phase3d_alpha_smoke",
            "version": "1.0.0",
            "start_date": "20250109",
            "end_date": "20250131"
        });
        let first = trial_reuse_key(
            "factor-combo-v1",
            "perf-db-smoke-data-v1",
            &template,
            &json!({"top_n": 5, "rebalance": "5"}),
        );
        let second = trial_reuse_key(
            "factor-combo-v1",
            "perf-db-smoke-data-v1",
            &template,
            &json!({"rebalance": "5", "top_n": 5}),
        );

        assert_eq!(first, second);
        assert!(first.contains("factor-combo-v1"));
        assert!(first.contains("phase3d_alpha_smoke"));
    }

    #[test]
    fn optimization_performance_gate_passes_completed_release_batch() {
        let policy = normalize_performance_gate(
            Some(&OptimizationPerformanceGateRequest {
                min_completed_trials: Some(200),
                max_failed_trials: Some(0),
                max_elapsed_ms: Some(60_000),
            }),
            200,
        )
        .expect("gate policy");

        let gates = evaluate_optimization_performance_gates(200, 200, 0, 42_000, &policy);

        assert_eq!(performance_gate_status(&gates), "passed");
        assert!(gates
            .as_array()
            .unwrap()
            .iter()
            .all(|gate| gate["passed"] == true));
    }

    #[test]
    fn optimization_performance_gate_requires_review_on_failures_or_shortfall() {
        let policy = normalize_performance_gate(None, 200).expect("default gate policy");

        let gates = evaluate_optimization_performance_gates(199, 198, 1, 42_000, &policy);

        assert_eq!(performance_gate_status(&gates), "review_required");
        assert_eq!(gates[0]["gate"], "min_completed_trials");
        assert_eq!(gates[0]["passed"], false);
        assert_eq!(gates[1]["gate"], "max_failed_trials");
        assert_eq!(gates[1]["passed"], false);
    }

    #[test]
    fn robustness_gate_approves_clean_dominant_trial() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(6, 1)),
            &json!({"num_trades": 12, "max_drawdown_pct": "0.08"}),
            &json!([]),
            Some(&json!({"min_trade_count": 5, "max_drawdown": 0.20})),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        assert!(evaluation
            .gates
            .as_array()
            .unwrap()
            .iter()
            .all(|gate| gate["passed"] == true));
    }

    #[test]
    fn robustness_gate_rejects_constraint_violations() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(4, 1),
            None,
            &json!({"num_trades": 2, "max_drawdown_pct": "0.25"}),
            &json!([{"constraint": "max_drawdown"}]),
            Some(&json!({"min_trade_count": 5, "max_drawdown": 0.20})),
        );

        assert_eq!(evaluation.status, "rejected");
    }

    #[test]
    fn robustness_gate_requires_review_when_score_gap_is_small() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(99, 2)),
            &json!({"num_trades": 12, "max_drawdown_pct": "0.08"}),
            &json!([]),
            Some(&json!({"min_trade_count": 5, "max_drawdown": 0.20, "min_score_gap": 0.05})),
        );

        assert_eq!(evaluation.status, "review_required");
    }

    #[test]
    fn professional_robustness_policy_encodes_core_targets() {
        let policy = default_professional_robustness_policy();

        assert_eq!(policy["min_annual_return"], 0.15);
        assert_eq!(policy["min_sharpe"], 1.0);
        assert_eq!(policy["min_sortino"], 1.5);
        assert_eq!(policy["max_drawdown"], 0.35);
        assert_eq!(policy["walk_forward_window_days"], 756);
        assert_eq!(policy["bootstrap_trials"], 512);
        assert_eq!(policy["min_positive_annual_return_window_ratio"], 0.60);
        assert_eq!(policy["min_walk_forward_median_sharpe"], 0.30);
        assert_eq!(policy["min_bootstrap_sharpe_p05"], 0.0);
    }

    #[test]
    fn professional_elite_robustness_policy_encodes_metric_matrix() {
        let policy = default_professional_elite_robustness_policy();

        assert_eq!(policy["candidate_tier"], "professional_elite");
        assert_eq!(policy["min_annual_return"], 0.15);
        assert_eq!(policy["min_sharpe"], 1.5);
        assert_eq!(policy["min_sortino"], 1.8);
        assert_eq!(policy["min_calmar"], 2.0);
        assert_eq!(policy["min_profit_factor"], 1.5);
        assert_eq!(policy["min_trade_count"], 200);
        assert_eq!(policy["max_drawdown_duration_days"], 126);
        assert_eq!(policy["bootstrap_trials"], 1000);
        assert_eq!(
            resolve_robustness_gate_policy(Some(&json!({"preset": "professional_elite"})))
                ["min_calmar"],
            2.0
        );
    }

    #[test]
    fn oos_train_selection_policy_is_separate_from_final_professional_gate() {
        let train_policy = default_oos_train_selection_gate_policy();
        let professional_policy = default_professional_robustness_policy();

        assert_eq!(train_policy["candidate_tier"], "oos_train_selection");
        assert_eq!(train_policy["min_annual_return"], 0.0);
        assert!(train_policy.get("min_excess_return").is_none());
        assert_eq!(train_policy["min_sharpe"], 0.0);
        assert_eq!(train_policy["min_walk_forward_windows"], 1);
        assert!(train_policy.get("min_sortino").is_none());
        assert!(train_policy.get("min_calmar").is_none());
        assert!(
            train_policy["min_sharpe"].as_f64().unwrap()
                < professional_policy["min_sharpe"].as_f64().unwrap()
        );
        assert!(
            train_policy["min_walk_forward_windows"].as_i64().unwrap()
                < professional_policy["min_walk_forward_windows"]
                    .as_i64()
                    .unwrap()
        );
    }

    #[test]
    fn oos_final_promotion_policy_keeps_strict_stitched_oos_gates() {
        let policy = default_oos_final_promotion_gate_policy("walk_forward");

        assert_eq!(policy["candidate_tier"], "oos_final_promotion");
        assert_eq!(policy["min_stitched_oos_calmar"], 1.2);
        assert_eq!(policy["min_positive_oos_window_ratio"], 0.60);
        assert_eq!(policy["min_oos_window_count"], 3);
        assert_eq!(policy["require_train_selection_approval"], true);
        assert_eq!(policy["require_no_train_test_overlap"], true);
    }

    #[test]
    fn oos_train_selection_policy_preserves_legacy_professional_override() {
        let req = Phase7OosWalkForwardDiscoveryRequest {
            strategy_version_id: "phase7-professional-v1".to_string(),
            data_version_id: "full-market-2016-v1".to_string(),
            objective: None,
            constraints: None,
            walk_forward: None,
            backtest_template: None,
            prediction_set_ids: None,
            max_trials_per_window: None,
            search_profile: None,
            trial_batch_limit: None,
            trial_concurrency: None,
            train_cache_mode: None,
            max_batches_per_window: None,
            train_window_days: None,
            test_window_days: None,
            step_days: None,
            validation_mode: None,
            in_sample_ratio: None,
            include_partial_last_window: None,
            plan_only: None,
            execution_mode: None,
            exhaustive_search: None,
            require_train_robustness_approval: None,
            train_robustness_gate_policy: Some(json!({"preset": "professional"})),
            train_selection_gate_policy: None,
            final_promotion_gate_policy: None,
            min_stitched_oos_calmar: None,
            min_positive_oos_window_ratio: None,
            min_oos_window_count: None,
            oos_top_n: None,
            enable_cost_capacity_perturbation_gate: None,
            cost_capacity_perturbations: None,
            min_cost_capacity_perturbation_pass_ratio: None,
            min_perturbed_oos_calmar: None,
            max_perturbed_oos_drawdown_pct: None,
        };

        let policy = resolve_oos_train_selection_gate_policy(&req);

        assert_eq!(policy["candidate_tier"], "professional_observation");
        assert_eq!(policy["min_sharpe"], 1.0);
        assert_eq!(policy["min_walk_forward_windows"], 4);
    }

    #[test]
    fn oos_train_selection_policy_ignores_final_objective_constraint_violations_by_default() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(9, 1)),
            &json!({
                "num_trades": 40,
                "annual_return_pct": "0.02",
                "excess_return_pct": "0.01",
                "sharpe_ratio": "0.20",
                "max_drawdown_pct": "0.10"
            }),
            &json!([
                {
                    "constraint": "min_annual_return",
                    "severity": "hard",
                    "limit": "0.15",
                    "actual": "0.02"
                },
                {
                    "constraint": "min_sharpe",
                    "severity": "hard",
                    "limit": "1.0",
                    "actual": "0.20"
                }
            ]),
            Some(&default_oos_train_selection_gate_policy()),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        assert!(evaluation.gates.as_array().unwrap().iter().any(|gate| {
            gate["gate"] == "no_hard_constraint_violations"
                && gate["passed"] == true
                && gate["enforced"] == false
        }));
    }

    #[test]
    fn oos_train_selection_policy_does_not_require_positive_train_excess_by_default() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(9, 1)),
            &json!({
                "num_trades": 80,
                "annual_return_pct": "0.14",
                "excess_return_pct": "-0.15",
                "sharpe_ratio": "1.02",
                "sortino_ratio": "1.51",
                "calmar_ratio": "2.27",
                "max_drawdown_pct": "0.06"
            }),
            &json!([
                {
                    "constraint": "min_annual_return",
                    "severity": "hard",
                    "limit": "0.15",
                    "actual": "0.14"
                },
                {
                    "constraint": "min_excess_return",
                    "severity": "hard",
                    "limit": "0.0",
                    "actual": "-0.15"
                }
            ]),
            Some(&default_oos_train_selection_gate_policy()),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        assert!(!evaluation
            .gates
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| { gate["gate"] == "min_excess_return" }));
    }

    #[test]
    fn oos_train_selection_best_effort_diagnostic_mode_is_opt_in() {
        let default_policy = default_oos_train_selection_gate_policy();
        let diagnostic_policy = merge_gate_policy(
            default_policy.clone(),
            &json!({"allow_best_effort_train_selection_for_diagnostics": true}),
        );

        assert!(!best_effort_train_selection_for_diagnostics_enabled(
            &default_policy
        ));
        assert!(best_effort_train_selection_for_diagnostics_enabled(
            &diagnostic_policy
        ));
    }

    #[test]
    fn professional_robustness_gate_rejects_low_sortino_candidate() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(6, 1)),
            &json!({
                "num_trades": 120,
                "annual_return_pct": "0.18",
                "excess_return_pct": "0.04",
                "sharpe_ratio": "1.10",
                "sortino_ratio": "1.00",
                "calmar_ratio": "0.90",
                "profit_factor": "1.20",
                "max_drawdown_pct": "0.20"
            }),
            &json!([]),
            Some(&default_professional_robustness_policy()),
        );

        assert_eq!(evaluation.status, "rejected");
        assert!(evaluation
            .gates
            .as_array()
            .expect("gates")
            .iter()
            .any(|gate| gate["gate"] == "min_sortino" && gate["passed"] == false));
    }

    #[test]
    fn elite_robustness_gate_rejects_low_calmar_and_profit_factor() {
        let evaluation = evaluate_robustness_gates(
            Decimal::new(10, 1),
            Some(Decimal::new(6, 1)),
            &json!({
                "num_trades": 250,
                "annual_return_pct": "0.18",
                "excess_return_pct": "0.04",
                "sharpe_ratio": "1.60",
                "sortino_ratio": "1.90",
                "calmar_ratio": "1.20",
                "profit_factor": "1.20",
                "max_drawdown_duration_days": 180,
                "max_drawdown_pct": "0.15"
            }),
            &json!([]),
            Some(&default_professional_elite_robustness_policy()),
        );

        assert_eq!(evaluation.status, "rejected");
        let gates = evaluation.gates.as_array().expect("gates");
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "min_calmar" && gate["passed"] == false));
        assert!(gates
            .iter()
            .any(|gate| { gate["gate"] == "min_profit_factor" && gate["passed"] == false }));
        assert!(gates.iter().any(|gate| {
            gate["gate"] == "max_drawdown_duration_days" && gate["passed"] == false
        }));
    }

    #[test]
    fn market_scenario_classifies_regime_from_benchmark_path() {
        let bull = RobustnessMetricSummary {
            total_return: 0.18,
            annual_return: 0.20,
            sharpe_ratio: 1.2,
            sortino_ratio: 1.8,
            calmar_ratio: 5.0,
            max_drawdown: 0.04,
            benchmark_return: Some(0.18),
            excess_return: Some(0.02),
        };
        let bear = RobustnessMetricSummary {
            total_return: -0.02,
            annual_return: -0.02,
            sharpe_ratio: -0.3,
            sortino_ratio: -0.2,
            calmar_ratio: -0.08,
            max_drawdown: 0.24,
            benchmark_return: Some(-0.18),
            excess_return: Some(0.16),
        };
        let high_vol = RobustnessMetricSummary {
            total_return: 0.01,
            annual_return: 0.01,
            sharpe_ratio: 0.1,
            sortino_ratio: 0.1,
            calmar_ratio: 0.1,
            max_drawdown: 0.10,
            benchmark_return: Some(0.01),
            excess_return: Some(0.0),
        };

        assert_eq!(classify_market_scenario(&bull, 0.18), "bull");
        assert_eq!(classify_market_scenario(&bear, 0.18), "bear");
        assert_eq!(classify_market_scenario(&high_vol, 0.32), "high_volatility");
    }

    #[test]
    fn walk_forward_analysis_records_window_degradation_and_scenarios() {
        let points = vec![
            RobustnessDailyPoint::new("2025-01-01", 100.0, Some(100.0)),
            RobustnessDailyPoint::new("2025-01-02", 103.0, Some(101.0)),
            RobustnessDailyPoint::new("2025-01-03", 106.0, Some(102.0)),
            RobustnessDailyPoint::new("2025-01-06", 101.0, Some(99.0)),
            RobustnessDailyPoint::new("2025-01-07", 98.0, Some(96.0)),
            RobustnessDailyPoint::new("2025-01-08", 104.0, Some(97.0)),
        ];
        let analysis = build_walk_forward_analysis(&points, 3, 2);

        assert_eq!(analysis["window_count"], 2);
        assert_eq!(analysis["windows"][0]["start_date"], "2025-01-01");
        assert_eq!(analysis["windows"][1]["scenario"], "bear");
        assert!(analysis["positive_excess_window_ratio"].as_f64().unwrap() > 0.0);
        assert!(analysis["worst_window_drawdown"].as_f64().unwrap() > 0.0);
        assert!(analysis["calmar_ratio"]["median"].is_number());
    }

    #[test]
    fn bootstrap_analysis_reports_distribution_and_gate_probability() {
        let points = vec![
            RobustnessDailyPoint::new("2025-01-01", 100.0, Some(100.0)),
            RobustnessDailyPoint::new("2025-01-02", 101.0, Some(100.4)),
            RobustnessDailyPoint::new("2025-01-03", 102.0, Some(100.8)),
            RobustnessDailyPoint::new("2025-01-06", 101.0, Some(100.2)),
            RobustnessDailyPoint::new("2025-01-07", 103.0, Some(100.5)),
        ];
        let analysis = build_bootstrap_analysis(&points, 64, 7).expect("bootstrap analysis");

        assert_eq!(analysis["trials"], 64);
        assert!(analysis["positive_return_probability"].as_f64().unwrap() > 0.50);
        assert!(analysis["total_return"]["p05"].is_number());
        assert!(analysis["sharpe_ratio"]["median"].is_number());
        assert!(analysis["calmar_ratio"]["median"].is_number());
        assert!(analysis["max_drawdown"]["p95"].is_number());
    }

    #[test]
    fn robustness_gate_includes_walk_forward_and_bootstrap_requirements() {
        let points = vec![
            RobustnessDailyPoint::new("2025-01-01", 100.0, Some(100.0)),
            RobustnessDailyPoint::new("2025-01-02", 101.0, Some(100.2)),
            RobustnessDailyPoint::new("2025-01-03", 102.0, Some(100.4)),
            RobustnessDailyPoint::new("2025-01-06", 103.0, Some(100.6)),
            RobustnessDailyPoint::new("2025-01-07", 104.0, Some(100.8)),
            RobustnessDailyPoint::new("2025-01-08", 105.0, Some(101.0)),
        ];
        let analysis = RobustnessTimeSeriesAnalysis::from_points(&points, 3, 3, 32, 11)
            .expect("timeseries analysis");

        let evaluation = evaluate_robustness_gates_with_analysis(
            Decimal::new(10, 1),
            Some(Decimal::new(7, 1)),
            &json!({"num_trades": 20, "max_drawdown_pct": "0.05"}),
            &json!([]),
            Some(&json!({
                "min_trade_count": 5,
                "max_drawdown": 0.20,
                "min_walk_forward_windows": 2,
                "min_positive_excess_window_ratio": 0.5,
                "min_bootstrap_positive_return_probability": 0.50
            })),
            Some(&analysis),
        );

        assert_eq!(evaluation.status, "approved_candidate");
        let gates = evaluation.gates.as_array().expect("gates");
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "walk_forward_min_window_count"));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "bootstrap_positive_return_probability"));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "market_scenario_coverage"));
    }

    #[test]
    fn robustness_gate_rejects_overfit_candidate_with_weak_window_distribution() {
        let analysis = RobustnessTimeSeriesAnalysis {
            market_scenarios: json!({
                "scenario_count": 3,
                "scenarios": []
            }),
            walk_forward: json!({
                "window_count": 8,
                "positive_excess_window_ratio": 0.75,
                "positive_annual_return_ratio": 0.50,
                "sharpe_ratio": {
                    "p05": -0.20,
                    "median": 0.10,
                    "p95": 1.40,
                    "mean": 0.35
                },
                "windows": []
            }),
            bootstrap: json!({
                "positive_return_probability": 0.95,
                "sharpe_ratio": {
                    "p05": -0.05,
                    "median": 0.80,
                    "p95": 1.40,
                    "mean": 0.78
                }
            }),
        };

        let evaluation = evaluate_robustness_gates_with_analysis(
            Decimal::new(10, 1),
            Some(Decimal::new(7, 1)),
            &json!({
                "num_trades": 120,
                "annual_return_pct": "0.18",
                "excess_return_pct": "0.40",
                "sharpe_ratio": "1.10",
                "sortino_ratio": "1.80",
                "max_drawdown_pct": "0.25"
            }),
            &json!([]),
            Some(&default_professional_robustness_policy()),
            Some(&analysis),
        );

        assert_eq!(evaluation.status, "rejected");
        let gates = evaluation.gates.as_array().expect("gates");
        assert!(gates.iter().any(|gate| {
            gate["gate"] == "walk_forward_positive_annual_return_ratio" && gate["passed"] == false
        }));
        assert!(gates.iter().any(|gate| {
            gate["gate"] == "walk_forward_median_sharpe" && gate["passed"] == false
        }));
        assert!(gates
            .iter()
            .any(|gate| gate["gate"] == "bootstrap_sharpe_p05" && gate["passed"] == false));
    }

    #[test]
    fn robustness_failure_attribution_ranks_failed_gates_and_weak_windows() {
        let gates = json!([
            {
                "gate": "min_sharpe",
                "passed": false,
                "limit": 1.0,
                "actual": 0.714
            },
            {
                "gate": "walk_forward_min_window_count",
                "passed": true,
                "limit": 2,
                "actual": 3,
                "details": {
                    "windows": [
                        {
                            "window_index": 1,
                            "start_date": "2016-03-04",
                            "end_date": "2019-03-04",
                            "scenario": "bear",
                            "metrics": {
                                "annual_return": 0.04,
                                "excess_return": -0.02,
                                "sharpe_ratio": 0.18,
                                "sortino_ratio": 0.40,
                                "max_drawdown": 0.34
                            }
                        },
                        {
                            "window_index": 2,
                            "start_date": "2019-06-03",
                            "end_date": "2022-06-03",
                            "scenario": "mixed",
                            "metrics": {
                                "annual_return": 0.13,
                                "excess_return": 0.03,
                                "sharpe_ratio": 0.52,
                                "sortino_ratio": 1.10,
                                "max_drawdown": 0.24
                            }
                        },
                        {
                            "window_index": 3,
                            "start_date": "2022-09-01",
                            "end_date": "2025-09-01",
                            "scenario": "bull",
                            "metrics": {
                                "annual_return": 0.22,
                                "excess_return": 0.08,
                                "sharpe_ratio": 0.92,
                                "sortino_ratio": 1.80,
                                "max_drawdown": 0.18
                            }
                        }
                    ]
                }
            },
            {
                "gate": "bootstrap_positive_return_probability",
                "passed": true,
                "limit": 0.70,
                "actual": 0.99,
                "details": {
                    "positive_return_probability": 0.99,
                    "total_return": {"p05": 0.18, "median": 3.0, "p95": 8.0},
                    "sharpe_ratio": {"p05": 0.20, "median": 0.70, "p95": 1.30},
                    "sortino_ratio": {"p05": 0.50, "median": 1.58, "p95": 2.40}
                }
            }
        ]);

        let attribution = build_robustness_failure_attribution(&gates);

        assert_eq!(attribution["failed_gates"][0]["gate"], "min_sharpe");
        assert_eq!(
            attribution["worst_walk_forward_windows"][0]["start_date"],
            "2016-03-04"
        );
        assert_eq!(attribution["weak_market_scenarios"][0]["scenario"], "bear");
        assert_eq!(attribution["bootstrap_tail"]["sharpe_ratio"]["p05"], 0.20);
        assert!(attribution["primary_failure_modes"]
            .as_array()
            .unwrap()
            .contains(&json!("sharpe_shortfall")));
    }
}
