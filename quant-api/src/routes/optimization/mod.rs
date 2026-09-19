//! Optimization API routes — create optimization tasks and parameter trials
//!
//! DDD Step 3b: split into a module tree under `routes/optimization/`.

mod diagnostics;
mod experiment_run;
mod param_search;
mod profile_registry;
mod robustness;
mod search_config;
mod wfa_engine;

pub use diagnostics::{
    get_sleeve_admission_diagnostics, report_alpha_source_diagnostics,
    report_feature_profile_readiness, report_main_business_diagnostics,
};
pub use experiment_run::{
    cleanup_stale_experiment_runs, cleanup_stale_optimization_tasks, create_optimization,
    generate_elite_validation_report, get_experiment_run, get_optimization,
    list_optimization_trials, promote_optimization_trial, run_phase7_professional_discovery,
};
pub use param_search::run_optimization_trials;
pub use robustness::{evaluate_optimization_robustness, evaluate_optimization_trial_robustness};
pub(crate) use search_config::{phase7_search_config, resolve_search_profile_name};
pub use wfa_engine::{
    create_phase7_layered_optimization, launch_phase7_oos_profile_comparison_smoke,
    plan_phase7_oos_profile_comparison, run_phase7_oos_walk_forward_discovery,
};

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use serde::{Deserialize, Serialize};
use serde_json::Value;

// Re-export submodule types so intra-crate references via `crate::routes::optimization::Foo`
// continue to resolve.
pub use diagnostics::*;
pub use experiment_run::*;
pub use param_search::*;
pub(crate) use profile_registry::*;
pub use robustness::*;
pub use wfa_engine::*;

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

#[derive(Debug, Clone, Deserialize)]
pub struct CleanupStaleBackgroundTasksReq {
    pub dry_run: Option<bool>,
    pub default_timeout_seconds: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct FeatureProfileReadinessRequest {
    pub feature_profile: String,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub min_day_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_rows: Option<i64>,
    #[serde(default)]
    pub min_p95_daily_row_ratio: Option<f64>,
    #[serde(default)]
    pub persist_report: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct AlphaSourceDiagnosticsRequest {
    pub combo_name: String,
    pub version: Option<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub alpha_admission_gate_id: Option<String>,
    #[serde(default)]
    pub universe_profile: Option<String>,
    #[serde(default)]
    pub min_day_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_rows: Option<i64>,
    #[serde(default)]
    pub min_p95_daily_row_ratio: Option<f64>,
    #[serde(default)]
    pub persist_report: Option<bool>,
    #[serde(default)]
    pub include_research_metrics: Option<bool>,
    #[serde(default)]
    pub return_horizons: Option<Vec<i64>>,
    #[serde(default)]
    pub bucket_count: Option<i64>,
    #[serde(default)]
    pub max_rank_ic_days: Option<i64>,
    #[serde(default)]
    pub include_exposure_regime_metrics: Option<bool>,
    #[serde(default)]
    pub max_exposure_regime_days: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct MainBusinessDiagnosticsRequest {
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub business_type: Option<String>,
    #[serde(default)]
    pub universe_profile: Option<String>,
    #[serde(default)]
    pub profiles: Option<Vec<String>>,
    #[serde(default)]
    pub min_day_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_rows: Option<i64>,
    #[serde(default)]
    pub min_p95_daily_row_ratio: Option<f64>,
    #[serde(default)]
    pub min_daily_coverage_ratio: Option<f64>,
    #[serde(default)]
    pub persist_report: Option<bool>,
    #[serde(default)]
    pub return_horizons: Option<Vec<i64>>,
    #[serde(default)]
    pub bucket_count: Option<i64>,
    #[serde(default)]
    pub max_rank_ic_days: Option<i64>,
    #[serde(default)]
    pub include_exposure_regime_metrics: Option<bool>,
    #[serde(default)]
    pub max_exposure_regime_days: Option<i64>,
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

pub(crate) fn default_random_seed() -> u64 {
    42
}

pub(crate) fn default_max_trials() -> usize {
    20
}

pub(crate) fn normalize_max_trials(value: usize) -> usize {
    value.clamp(1, 500)
}

pub(crate) fn normalize_limit(value: Option<i64>) -> i64 {
    value.unwrap_or(100).clamp(1, 500)
}

#[cfg(test)]
mod tests;
