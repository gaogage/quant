//! BC4 策略发现 / seed_generators：种子生成器子模块入口。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，按 7 子主题组织：
//! - breakthrough：突破/锚点种子
//! - regime：市场状态/风格守门
//! - event：事件驱动
//! - execution：执行/容量（含枢纽 return_distribution_repair）
//! - prediction：预测确认（含枢纽 prediction_confirmed_sharpe_bridge）
//! - v19_series：v19 当前活跃策略族
//! - sharpe_frontier：sharpe 桥/frontier（含枢纽 return_alpha_sharpe_bridge）
//!
//! 本文件承载：73+ with_*_seed builder + 7 公共 helper + SeedTrialGenerator trait 骨架。
//! 纯 move，零行为变更。

mod breakthrough;
mod regime;
mod event;
mod execution;
mod prediction;
mod v19_series;
mod sharpe_frontier;

// 子主题函数声明为 pub(crate)（对 strategy_discovery 可见）；
// 此处 pub(crate) use 重导出，让兄弟子模块通过 `use super::*` 互相可见。
// seed_generators 本身是 strategy_discovery 的私有子模块，pub(crate) 不会外泄。
pub(crate) use breakthrough::*;
pub(crate) use event::*;
pub(crate) use execution::*;
pub(crate) use prediction::*;
pub(crate) use regime::*;
pub(crate) use sharpe_frontier::*;
pub(crate) use v19_series::*;

use super::profiles::ScoreDirection;
use super::{decimal_f64, decimal_string, insert_execution_rule_value,
    is_phase7_base_trainable_alpha};

use rust_decimal::Decimal;
use serde_json::{json, Value};

// ============================================================
// 公共 helper
// ============================================================

// ---- 公共 helper ----

pub(crate) fn append_unique_seeds(seeds: &mut Vec<Value>, candidates: Vec<Value>) {
    for seed in candidates {
        if !seeds.contains(&seed) {
            seeds.push(seed);
        }
    }
}

pub(crate) fn add_turnover_smoothing_variants(seeds: &mut Vec<Value>, anchors: &[Value]) {
    for seed in anchors
        .iter()
        .filter(|seed| is_turnover_smoothing_anchor_seed(seed))
    {
        for (hysteresis_pct, partial_ratio) in
            [("0.01", "0.75"), ("0.02", "0.75"), ("0.01", "0.50")]
        {
            let mut smoothed = seed.clone();
            smoothed["rebalance_hysteresis_pct"] = json!(hysteresis_pct);
            smoothed["partial_rebalance_ratio"] = json!(partial_ratio);
            if !seeds.contains(&smoothed) {
                seeds.push(smoothed);
            }
        }
    }
}

pub(crate) fn is_turnover_smoothing_anchor_seed(seed: &Value) -> bool {
    seed["combo_name"] == "phase7_financial_quality_v1"
        && seed["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
        && seed["stop_loss_pct"] == "0.075"
        && seed["reentry_cooldown_days"] == 30
        && matches!(
            seed["portfolio_volatility_control"]
                .as_str()
                .unwrap_or("off"),
            "off" | "vol120_22_65_100" | "vol120_24_70_100"
        )
}

pub(crate) fn retarget_seed_combo(mut seed: Value, combo_name: &str) -> Value {
    seed["combo_name"] = json!(combo_name);
    seed
}

pub(crate) fn set_seed_score_direction(mut seed: Value, direction: ScoreDirection) -> Value {
    seed["score_direction"] = json!(direction.as_str());
    seed
}

pub(crate) fn reorder_seed_trials_by_string_field(
    seeds: Vec<Value>,
    field: &str,
    priority_values: &[&str],
) -> Vec<Value> {
    let mut remaining = seeds;
    let mut ordered = Vec::with_capacity(remaining.len());
    for priority_value in priority_values {
        if let Some(index) = remaining
            .iter()
            .position(|seed| seed.get(field).and_then(Value::as_str) == Some(*priority_value))
        {
            ordered.push(remaining.remove(index));
        }
    }
    ordered.extend(remaining);
    ordered
}

pub(crate) fn finalize_return_distribution_seed(seed: Value) -> Value {
    with_risk_contribution_control_seed(
        with_candidate_risk_filter_seed(seed, "off"),
        "soft_single_name_20pct_v1",
    )
}


// ============================================================
// with_*_seed builder
// ============================================================

pub(crate) fn with_event_window_gate_seed(
    seed: Value,
    profile_name: &str,
    mode: &str,
    boost_weight: &str,
) -> Value {
    with_event_combo_gate_seed(
        seed,
        profile_name,
        "phase7_event_window_earnings_v1",
        mode,
        boost_weight,
        ScoreDirection::Descending,
    )
}

pub(crate) fn with_event_surprise_gate_seed(
    seed: Value,
    profile_name: &str,
    mode: &str,
    boost_weight: &str,
) -> Value {
    with_event_combo_gate_seed(
        seed,
        profile_name,
        "phase7_event_surprise_v1",
        mode,
        boost_weight,
        ScoreDirection::Descending,
    )
}

pub(crate) fn with_event_combo_gate_seed(
    seed: Value,
    profile_name: &str,
    combo_name: &str,
    mode: &str,
    boost_weight: &str,
    score_direction: ScoreDirection,
) -> Value {
    with_event_combo_gate_seed_with_min_score(
        seed,
        profile_name,
        combo_name,
        mode,
        "0",
        boost_weight,
        score_direction,
    )
}

pub(crate) fn with_event_combo_gate_seed_with_min_score(
    mut seed: Value,
    profile_name: &str,
    combo_name: &str,
    mode: &str,
    min_score: &str,
    boost_weight: &str,
    score_direction: ScoreDirection,
) -> Value {
    seed["event_gate_profile"] = json!(profile_name);
    seed["event_gate_combo_name"] = json!(combo_name);
    seed["event_gate_version"] = json!("1.0.0");
    seed["event_gate_mode"] = json!(mode);
    seed["event_gate_min_score"] = json!(min_score);
    seed["event_gate_boost_weight"] = json!(boost_weight);
    seed["event_gate_score_direction"] = json!(score_direction.as_str());
    seed
}

pub(crate) fn with_event_combo_gate_seed_active_in(
    seed: Value,
    profile_name: &str,
    combo_name: &str,
    mode: &str,
    min_score: &str,
    score_direction: ScoreDirection,
    active_regimes: &[&str],
) -> Value {
    let mut seed = with_event_combo_gate_seed_with_min_score(
        seed,
        profile_name,
        combo_name,
        mode,
        min_score,
        "0",
        score_direction,
    );
    seed["event_gate_active_regimes"] = json!(active_regimes);
    seed
}

pub(crate) fn with_event_combo_gate_seed_active_in_with_boost(
    seed: Value,
    profile_name: &str,
    combo_name: &str,
    mode: &str,
    boost_weight: &str,
    score_direction: ScoreDirection,
    active_regimes: &[&str],
) -> Value {
    let mut seed = with_event_combo_gate_seed_with_min_score(
        seed,
        profile_name,
        combo_name,
        mode,
        "0",
        boost_weight,
        score_direction,
    );
    seed["event_gate_active_regimes"] = json!(active_regimes);
    seed
}

pub(crate) fn with_event_combo_gate_seed_active_in_with_min_score_and_boost(
    seed: Value,
    profile_name: &str,
    combo_name: &str,
    mode: &str,
    min_score: &str,
    boost_weight: &str,
    score_direction: ScoreDirection,
    active_regimes: &[&str],
) -> Value {
    let mut seed = with_event_combo_gate_seed_with_min_score(
        seed,
        profile_name,
        combo_name,
        mode,
        min_score,
        boost_weight,
        score_direction,
    );
    seed["event_gate_active_regimes"] = json!(active_regimes);
    seed
}

pub(crate) fn with_market_regime_seed(mut seed: Value, market_regime: &str) -> Value {
    seed["market_regime"] = json!(market_regime);
    seed
}

pub(crate) fn with_drawdown_profile_seed(
    mut seed: Value,
    profile_name: &str,
    reduce_start_pct: &str,
    reduce_full_pct: &str,
    min_exposure: &str,
    peak_lookback_days: usize,
    recovery_start_pct: &str,
    recovery_full_pct: &str,
    recovery_boost: &str,
) -> Value {
    seed["portfolio_drawdown_control"] = json!(profile_name);
    seed["portfolio_drawdown_reduce_start_pct"] = json!(reduce_start_pct);
    seed["portfolio_drawdown_reduce_full_pct"] = json!(reduce_full_pct);
    seed["portfolio_drawdown_min_exposure"] = json!(min_exposure);
    seed["portfolio_drawdown_peak_lookback_days"] = json!(peak_lookback_days);
    seed["portfolio_drawdown_recovery_start_pct"] = json!(recovery_start_pct);
    seed["portfolio_drawdown_recovery_full_pct"] = json!(recovery_full_pct);
    seed["portfolio_drawdown_recovery_boost"] = json!(recovery_boost);
    seed
}

pub(crate) fn with_style_risk_budget_seed(mut seed: Value, style_risk_budget: &str) -> Value {
    seed["style_risk_budget"] = json!(style_risk_budget);
    seed
}

pub(crate) fn with_portfolio_method_seed(
    mut seed: Value,
    portfolio_method: &str,
    risk_budget_lookback_days: usize,
) -> Value {
    seed["portfolio_method"] = json!(portfolio_method);
    seed["risk_budget_lookback_days"] = json!(risk_budget_lookback_days);
    seed
}

pub(crate) fn with_volatility_profile_seed(
    mut seed: Value,
    profile_name: &str,
    target_pct: &str,
    lookback_days: usize,
    min_exposure: &str,
    max_exposure: &str,
) -> Value {
    seed["portfolio_volatility_control"] = json!(profile_name);
    seed["portfolio_volatility_target_pct"] = json!(target_pct);
    seed["portfolio_volatility_lookback_days"] = json!(lookback_days);
    seed["portfolio_volatility_min_exposure"] = json!(min_exposure);
    seed["portfolio_volatility_max_exposure"] = json!(max_exposure);
    seed
}

pub(crate) fn with_sharpe_profile_seed(
    mut seed: Value,
    profile_name: &str,
    reduce_start: &str,
    reduce_full: &str,
    lookback_days: usize,
    min_exposure: &str,
) -> Value {
    seed["portfolio_sharpe_control"] = json!(profile_name);
    seed["portfolio_sharpe_reduce_start"] = json!(reduce_start);
    seed["portfolio_sharpe_reduce_full"] = json!(reduce_full);
    seed["portfolio_sharpe_lookback_days"] = json!(lookback_days);
    seed["portfolio_sharpe_min_exposure"] = json!(min_exposure);
    seed
}

pub(crate) fn with_sharpe_off_seed(mut seed: Value) -> Value {
    seed["portfolio_sharpe_control"] = json!("off");
    if let Some(object) = seed.as_object_mut() {
        object.remove("portfolio_sharpe_reduce_start");
        object.remove("portfolio_sharpe_reduce_full");
        object.remove("portfolio_sharpe_lookback_days");
        object.remove("portfolio_sharpe_min_exposure");
    }
    seed
}

pub(crate) fn with_candidate_risk_filter_seed(mut seed: Value, candidate_risk_filter: &str) -> Value {
    seed["candidate_risk_filter"] = json!(candidate_risk_filter);
    seed
}

pub(crate) fn with_candidate_ranking_seed(mut seed: Value, candidate_ranking: &str) -> Value {
    seed["candidate_ranking"] = json!(candidate_ranking);
    seed
}

pub(crate) fn with_risk_contribution_control_seed(mut seed: Value, risk_contribution_control: &str) -> Value {
    seed["risk_contribution_control"] = json!(risk_contribution_control);
    seed
}

pub(crate) fn with_capacity_risk_budget_seed(mut seed: Value, capacity_risk_budget: &str) -> Value {
    seed["capacity_risk_budget"] = json!(capacity_risk_budget);
    seed
}

pub(crate) fn with_cash_utilization_seed(mut seed: Value, cash_utilization: &str) -> Value {
    seed["cash_utilization"] = json!(cash_utilization);
    seed
}

pub(crate) fn with_execution_impact_budget_seed(mut seed: Value, execution_impact_budget: &str) -> Value {
    seed["execution_impact_budget"] = json!(execution_impact_budget);
    seed
}

pub(crate) fn with_execution_schedule_seed(mut seed: Value, execution_schedule_profile: &str) -> Value {
    seed["execution_schedule_profile"] = json!(execution_schedule_profile);
    insert_execution_rule_value(
        &mut seed,
        "execution_schedule_profile",
        json!(execution_schedule_profile),
    );
    seed
}

pub(crate) fn with_execution_schedule_control_seed(
    mut seed: Value,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
) -> Value {
    seed = with_execution_schedule_seed(seed, execution_schedule_profile);
    seed["execution_daily_target_move_limit_pct"] =
        json!(decimal_string(daily_target_move_limit_pct));
    seed["execution_max_carry_days"] = json!(max_carry_days);
    insert_execution_rule_value(
        &mut seed,
        "execution_daily_target_move_limit_pct",
        json!(decimal_f64(daily_target_move_limit_pct)),
    );
    insert_execution_rule_value(&mut seed, "execution_max_carry_days", json!(max_carry_days));
    seed
}

pub(crate) fn with_execution_carry_policy_seed(mut seed: Value, execution_carry_policy: &str) -> Value {
    let policy = execution_carry_policy.trim();
    if policy.is_empty() || policy == "expire" || policy == "off" || policy == "default" {
        if let Some(object) = seed.as_object_mut() {
            object.remove("execution_carry_policy");
            if let Some(Value::Object(rules)) = object.get_mut("execution_rules") {
                rules.remove("execution_carry_policy");
                if rules.is_empty() {
                    object.remove("execution_rules");
                }
            }
        }
        return seed;
    }
    seed["execution_carry_policy"] = json!(policy);
    insert_execution_rule_value(&mut seed, "execution_carry_policy", json!(policy));
    seed
}

pub(crate) fn with_prediction_confirmation_seed(
    mut seed: Value,
    prediction_set_id: &str,
    blend_weight: &str,
    min_percentile: Option<&str>,
) -> Value {
    seed["prediction_set_id"] = json!(prediction_set_id);
    seed["prediction_blend_weight"] = json!(blend_weight);
    if let Some(min_percentile) = min_percentile {
        seed["prediction_min_percentile"] = json!(min_percentile);
    } else if let Some(object) = seed.as_object_mut() {
        object.remove("prediction_min_percentile");
    }
    seed
}

pub(crate) fn with_stop_loss_cooldown_seed(
    mut seed: Value,
    profile_name: &str,
    stop_loss_pct: &str,
    cooldown_days: u32,
) -> Value {
    seed["position_risk_control"] = json!(profile_name);
    seed["stop_loss_pct"] = json!(stop_loss_pct);
    seed["reentry_cooldown_days"] = json!(cooldown_days);
    seed
}

pub(crate) fn with_rebalance_days_seed(mut seed: Value, rebalance_days: usize, profile_name: &str) -> Value {
    seed["rebalance"] = json!(rebalance_days.to_string());
    seed["rebalance_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_skip_top_seed(mut seed: Value, skip_top_pct: &str, profile_name: &str) -> Value {
    seed["skip_top_pct"] = json!(skip_top_pct);
    seed["skip_top_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_position_shape_seed(
    mut seed: Value,
    profile_name: &str,
    max_position_pct: &str,
    max_pairwise_correlation: &str,
) -> Value {
    seed["position_shape_profile"] = json!(profile_name);
    seed["max_position_pct"] = json!(max_position_pct);
    seed["max_pairwise_correlation"] = json!(max_pairwise_correlation);
    seed
}

pub(crate) fn with_execution_robustness_seed(
    mut seed: Value,
    profile_name: &str,
    capacity_penalty_strength: &str,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
) -> Value {
    seed["execution_robustness_profile"] = json!(profile_name);
    seed["capacity_penalty_strength"] = json!(capacity_penalty_strength);
    seed["rebalance_hysteresis_pct"] = json!(rebalance_hysteresis_pct);
    seed["partial_rebalance_ratio"] = json!(partial_rebalance_ratio);
    seed
}

pub(crate) fn with_risk_budget_lookback_seed(
    mut seed: Value,
    profile_name: &str,
    risk_budget_lookback_days: usize,
) -> Value {
    seed["risk_budget_shape_profile"] = json!(profile_name);
    seed["risk_budget_lookback_days"] = json!(risk_budget_lookback_days);
    seed
}

pub(crate) fn with_top_n_seed(mut seed: Value, top_n: usize, profile_name: &str) -> Value {
    seed["top_n"] = json!(top_n);
    seed["top_n_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_max_gross_exposure_seed(
    mut seed: Value,
    max_gross_exposure: &str,
    profile_name: &str,
) -> Value {
    seed["max_gross_exposure"] = json!(max_gross_exposure);
    seed["gross_exposure_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_event_sleeve_seed(mut seed: Value, market_regime: &str, sleeve_profile: &str) -> Value {
    seed["event_sleeve_profile"] = json!(sleeve_profile);
    seed["market_regime"] = json!(market_regime);
    seed
}

pub(crate) fn with_rebalance_smoothing_seed(
    mut seed: Value,
    profile_name: &str,
    hysteresis_pct: &str,
    partial_ratio: &str,
) -> Value {
    seed["rebalance_smoothing_profile"] = json!(profile_name);
    seed["rebalance_hysteresis_pct"] = json!(hysteresis_pct);
    seed["partial_rebalance_ratio"] = json!(partial_ratio);
    seed
}

pub(crate) fn with_capacity_fill_frontier_seed(
    seed: Value,
    top_n: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
) -> Value {
    let profile_name = format!(
        "capacity_fill_top{top_n}_maxpos{}_gross{}",
        max_position_pct.replace('.', ""),
        max_gross_exposure.replace('.', "")
    );
    let seed = with_top_n_seed(seed, top_n, &profile_name);
    let seed = with_position_shape_seed(seed, &profile_name, max_position_pct, "0.65");
    let seed = with_max_gross_exposure_seed(seed, max_gross_exposure, &profile_name);
    let seed = with_execution_robustness_seed(
        seed,
        &profile_name,
        capacity_penalty_strength,
        "0.04",
        "0.25",
    );
    let seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
    let seed = with_cash_utilization_seed(seed, "fillable_gross_95_v1");
    let seed = with_execution_schedule_control_seed(
        seed,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
    );
    let mut seed = with_execution_carry_policy_seed(seed, "roll_forward_v1");
    seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
    seed["capacity_fill_frontier_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_low_impact_alpha_stress_return_seed(
    seed: Value,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    universe_profile: &str,
) -> Value {
    let profile_name = format!(
        "low_impact_alpha_top{top_n}_rebalance{rebalance_days}_maxpos{}_gross{}",
        max_position_pct.replace('.', ""),
        max_gross_exposure.replace('.', "")
    );
    let seed = set_seed_score_direction(
        retarget_seed_combo(seed, combo_name),
        ScoreDirection::Ascending,
    );
    let seed = with_event_sleeve_seed(seed, market_regime, &profile_name);
    let seed = with_candidate_risk_filter_seed(seed, candidate_risk_filter);
    let seed = with_risk_contribution_control_seed(seed, risk_contribution_control);
    let seed = with_rebalance_days_seed(seed, rebalance_days, &profile_name);
    let seed = with_capacity_fill_frontier_seed(
        seed,
        top_n,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
    );
    let seed = with_cash_utilization_seed(seed, "stress_fill_gross_98_v1");
    let mut seed = with_execution_impact_budget_seed(seed, "impact_turnover_15pct_v1");
    seed["low_impact_alpha_profile"] = json!(profile_name);
    seed["universe_profile"] = json!(universe_profile);
    seed
}

pub(crate) fn with_event_anchor_execution_stress_seed(
    seed: Value,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    universe_profile: &str,
) -> Value {
    let profile_name = format!(
        "event_anchor_stress_top{top_n}_rebalance{rebalance_days}_maxpos{}_gross{}",
        max_position_pct.replace('.', ""),
        max_gross_exposure.replace('.', "")
    );
    let seed = with_candidate_risk_filter_seed(seed, "soft_low_volatility_low_correlation_v1");
    let seed = with_risk_contribution_control_seed(seed, "soft_single_name_15pct_v1");
    let seed = with_rebalance_days_seed(seed, rebalance_days, &profile_name);
    let seed = with_capacity_fill_frontier_seed(
        seed,
        top_n,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
    );
    let seed = with_cash_utilization_seed(seed, "stress_fill_gross_98_v1");
    let mut seed = with_execution_impact_budget_seed(seed, "impact_turnover_15pct_v1");
    seed["execution_event_stress_bridge_profile"] = json!(profile_name);
    seed["universe_profile"] = json!(universe_profile);
    seed
}

pub(crate) fn with_execution_participation_limit_seed(
    mut seed: Value,
    max_participation_rate: Decimal,
) -> Value {
    seed["execution_max_participation_rate"] = json!(decimal_string(max_participation_rate));
    insert_execution_rule_value(
        &mut seed,
        "max_participation_rate",
        json!(decimal_f64(max_participation_rate)),
    );
    seed
}

pub(crate) fn with_cl_anchor_fill_recovery_seed(
    seed: Value,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    max_participation_rate: Decimal,
    risk_contribution_control: &str,
) -> Value {
    let seed = with_event_anchor_execution_stress_seed(
        seed,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        "twap_20d_v1",
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        "listed_non_st",
    );
    let seed =
        with_candidate_risk_filter_seed(seed, "soft_liquidity_low_volatility_low_correlation_v1");
    let seed = with_risk_contribution_control_seed(seed, risk_contribution_control);
    let mut seed = with_execution_participation_limit_seed(seed, max_participation_rate);
    seed["execution_cl_anchor_fill_recovery_profile"] = json!(format!(
        "cl_fill_recovery_top{top_n}_rebalance{rebalance_days}"
    ));
    seed
}

pub(crate) fn with_capacity_aware_candidate_ranking_seed(seed: Value, profile_name: &str) -> Value {
    let seed = with_candidate_ranking_seed(seed, "capacity_aware_alpha_liquidity_v1");
    let mut seed = seed;
    seed["execution_capacity_aware_candidate_ranking_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_pit_alpha_first_low_impact_seed(
    seed: Value,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    universe_profile: &str,
    profile_name: &str,
) -> Value {
    let seed = with_low_impact_alpha_stress_return_seed(
        seed,
        combo_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        universe_profile,
    );
    let seed = with_candidate_ranking_seed(seed, "alpha_first_low_impact_v1");
    let mut seed = with_execution_impact_budget_seed(seed, "impact_turnover_20pct_v1");
    seed["pit_capacity_recovery_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_pit_excess_return_recovery_seed(
    seed: Value,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    universe_profile: &str,
    execution_impact_budget: &str,
    profile_name: &str,
) -> Value {
    let seed = with_low_impact_alpha_stress_return_seed(
        seed,
        combo_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        universe_profile,
    );
    let seed = with_candidate_ranking_seed(seed, "relative_strength_alpha_liquidity_v1");
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    seed["pit_excess_return_recovery_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_pit_nonlinear_alpha_regime_rebuild_seed(
    seed: Value,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    universe_profile: &str,
    candidate_ranking: &str,
    execution_impact_budget: &str,
    profile_name: &str,
) -> Value {
    let seed = with_low_impact_alpha_stress_return_seed(
        seed,
        combo_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        universe_profile,
    );
    let seed = with_candidate_ranking_seed(seed, candidate_ranking);
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    seed["pit_nonlinear_alpha_regime_rebuild_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_train_window_nonlinear_ranking_seed(
    seed: Value,
    combo_name: &str,
    score_direction: ScoreDirection,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    cash_utilization: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    execution_impact_budget: &str,
    profile_name: &str,
) -> Value {
    let seed = with_low_impact_alpha_stress_return_seed(
        seed,
        combo_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        "listed_non_st",
    );
    let seed = set_seed_score_direction(seed, score_direction);
    let seed = with_candidate_ranking_seed(seed, "nonlinear_regime_alpha_liquidity_v2");
    let seed = with_cash_utilization_seed(seed, cash_utilization);
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    if let Some(object) = seed.as_object_mut() {
        object.remove("prediction_set_id");
        object.remove("prediction_blend_weight");
        object.remove("prediction_min_percentile");
        object.remove("prediction_label_horizon_days");
    }
    seed["train_window_nonlinear_ranking_profile"] = json!(profile_name);
    seed["alpha_source_family"] = json!("train_window_native_nonlinear_ranking");
    seed
}

pub(crate) fn with_pit_quality_recovery_alpha_seed(
    seed: Value,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    universe_profile: &str,
    candidate_ranking: &str,
    execution_impact_budget: &str,
    profile_name: &str,
) -> Value {
    let seed = with_low_impact_alpha_stress_return_seed(
        seed,
        "phase7_quality_recovery_acceleration_v1",
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        universe_profile,
    );
    let seed = set_seed_score_direction(seed, ScoreDirection::Descending);
    let seed = with_candidate_ranking_seed(seed, candidate_ranking);
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    seed["pit_quality_recovery_alpha_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_event_post_return_curve_alpha_seed(
    seed: Value,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    universe_profile: &str,
    candidate_ranking: &str,
    execution_impact_budget: &str,
    profile_name: &str,
) -> Value {
    let seed = with_low_impact_alpha_stress_return_seed(
        seed,
        "phase7_quality_event_post_return_curve_overlay_v1",
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        universe_profile,
    );
    let seed = set_seed_score_direction(seed, ScoreDirection::Descending);
    let seed = with_candidate_ranking_seed(seed, candidate_ranking);
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    seed["event_post_return_curve_alpha_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_event_reaction_alpha_seed(
    seed: Value,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    universe_profile: &str,
    candidate_ranking: &str,
    execution_impact_budget: &str,
    profile_name: &str,
) -> Value {
    let seed = with_low_impact_alpha_stress_return_seed(
        seed,
        combo_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        universe_profile,
    );
    let seed = set_seed_score_direction(seed, ScoreDirection::Descending);
    let seed = with_candidate_ranking_seed(seed, candidate_ranking);
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    seed["event_reaction_alpha_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_broad_financial_feature_discovery_seed(
    seed: Value,
    combo_name: &str,
    score_direction: ScoreDirection,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    candidate_ranking: &str,
    execution_impact_budget: &str,
    profile_name: &str,
) -> Value {
    let seed = with_pit_nonlinear_alpha_regime_rebuild_seed(
        seed,
        combo_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        "listed_non_st",
        candidate_ranking,
        execution_impact_budget,
        profile_name,
    );
    let mut seed = set_seed_score_direction(seed, score_direction);
    seed["broad_financial_feature_discovery_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_v19_multi_alpha_sleeve_seed(
    seed: Value,
    sleeve_family: &str,
    combo_name: &str,
    score_direction: ScoreDirection,
    top_n: usize,
    rebalance_days: usize,
    market_regime: &str,
    candidate_ranking: &str,
) -> Value {
    let mut seed =
        tag_trainable_alpha_admission_seed(seed, "v19_p2_pit_sleeve_admission_v1", sleeve_family);
    if let Some(object) = seed.as_object_mut() {
        for key in [
            "prediction_set_id",
            "prediction_blend_weight",
            "prediction_min_score",
            "prediction_min_percentile",
            "train_window_ml_policy",
            "train_window_ml_profile",
            "train_window_ml_feature_profile",
        ] {
            object.remove(key);
        }
    }
    seed["multi_alpha_sleeve_profile"] = json!("v19_p2_pit_sleeve_admission_v1");
    seed["alpha_sleeve_family"] = json!(sleeve_family);
    seed["signal_source"] = json!("factor_combo");
    seed["combo_name"] = json!(combo_name);
    seed["version"] = json!("1.0.0");
    seed["score_direction"] = json!(score_direction.as_str());
    seed["top_n"] = json!(top_n);
    seed["rebalance"] = json!(rebalance_days.to_string());
    seed["market_regime"] = json!(market_regime);
    seed["candidate_ranking"] = json!(candidate_ranking);
    seed["portfolio_method"] = json!("risk_budget");
    seed["risk_budget_lookback_days"] = json!(165);
    seed["capacity_penalty_strength"] = json!("0.75");
    seed["capacity_risk_budget"] =
        json!("capacity_stress_participation_alpha_headroom_floor_70_v1");
    seed["cash_utilization"] = json!("stress_fill_gross_98_v1");
    seed["execution_impact_budget"] = json!("impact_turnover_15pct_v1");
    seed["execution_schedule_profile"] = json!("twap_20d_v1");
    seed["execution_carry_policy"] = json!("roll_forward_v1");
    insert_execution_rule_value(
        &mut seed,
        "execution_carry_policy",
        json!("roll_forward_v1"),
    );
    seed["candidate_risk_filter"] = json!("soft_liquidity_low_volatility_low_correlation_v1");
    seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
    seed["max_position_pct"] = json!("0.08");
    seed["max_gross_exposure"] = json!("1");
    seed["score_candidate_pool_size"] = json!(2200);
    seed["universe_profile"] = json!("listed_non_st");
    seed
}

pub(crate) fn with_v19_event_surprise_sleeve_gate_seed(
    seed: Value,
    variant_name: &str,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    market_regime: &str,
    candidate_ranking: &str,
) -> Value {
    let mut seed = with_v19_multi_alpha_sleeve_seed(
        seed,
        "event_surprise_sleeve_gate",
        combo_name,
        ScoreDirection::Descending,
        top_n,
        rebalance_days,
        market_regime,
        candidate_ranking,
    );
    if let Some(object) = seed.as_object_mut() {
        for key in [
            "multi_alpha_sleeve_profile",
            "alpha_sleeve_family",
            "event_gate_profile",
            "event_gate_combo_name",
            "event_gate_version",
            "event_gate_mode",
            "event_gate_min_score",
            "event_gate_boost_weight",
            "event_gate_score_direction",
            "event_gate_active_regimes",
        ] {
            object.remove(key);
        }
    }
    seed["event_surprise_sleeve_gate_profile"] =
        json!("v19_p311_fq_change_event_surprise_sleeve_gate_v1");
    seed["event_surprise_sleeve_gate_variant"] = json!(variant_name);
    seed["alpha_source_family"] = json!("event_surprise_sleeve_gate");
    seed
}

pub(crate) fn with_v19_supply_float_sleeve_seed(
    seed: Value,
    variant_name: &str,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    market_regime: &str,
    candidate_ranking: &str,
) -> Value {
    let mut seed = with_v19_multi_alpha_sleeve_seed(
        seed,
        "supply_float_sleeve",
        combo_name,
        ScoreDirection::Descending,
        top_n,
        rebalance_days,
        market_regime,
        candidate_ranking,
    );
    if let Some(object) = seed.as_object_mut() {
        for key in [
            "prediction_set_id",
            "prediction_blend_weight",
            "prediction_min_score",
            "prediction_min_percentile",
            "prediction_label_horizon_days",
            "train_window_ml_policy",
            "train_window_ml_profile",
            "train_window_ml_feature_profile",
            "multi_alpha_sleeve_profile",
            "alpha_sleeve_family",
            "event_gate_profile",
            "event_gate_combo_name",
            "event_gate_version",
            "event_gate_mode",
            "event_gate_min_score",
            "event_gate_boost_weight",
            "event_gate_score_direction",
            "event_gate_active_regimes",
        ] {
            object.remove(key);
        }
    }
    seed["supply_float_sleeve_profile"] = json!("v19_p312_fq_change_supply_float_sleeve_v1");
    seed["supply_float_sleeve_variant"] = json!(variant_name);
    seed["alpha_source_family"] = json!("supply_float_sleeve");
    seed
}

pub(crate) fn with_v19_unlock_pressure_sleeve_seed(
    seed: Value,
    variant_name: &str,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    market_regime: &str,
    candidate_ranking: &str,
) -> Value {
    let mut seed = with_v19_multi_alpha_sleeve_seed(
        seed,
        "unlock_pressure_sleeve",
        combo_name,
        ScoreDirection::Descending,
        top_n,
        rebalance_days,
        market_regime,
        candidate_ranking,
    );
    if let Some(object) = seed.as_object_mut() {
        for key in [
            "prediction_set_id",
            "prediction_blend_weight",
            "prediction_min_score",
            "prediction_min_percentile",
            "prediction_label_horizon_days",
            "train_window_ml_policy",
            "train_window_ml_profile",
            "train_window_ml_feature_profile",
            "multi_alpha_sleeve_profile",
            "alpha_sleeve_family",
            "event_gate_profile",
            "event_gate_combo_name",
            "event_gate_version",
            "event_gate_mode",
            "event_gate_min_score",
            "event_gate_boost_weight",
            "event_gate_score_direction",
            "event_gate_active_regimes",
        ] {
            object.remove(key);
        }
    }
    seed["unlock_pressure_sleeve_profile"] = json!("v19_p314_fq_change_unlock_pressure_sleeve_v1");
    seed["unlock_pressure_sleeve_variant"] = json!(variant_name);
    seed["unlock_pressure_sleeve_control"] = json!("phase7_financial_quality_change_v1");
    seed["sideways_regime_policy"] = json!("exclude");
    seed["oos_policy"] = json!("evaluation_only");
    seed["alpha_source_family"] = json!("unlock_pressure_sleeve");
    seed
}

pub(crate) fn with_v19_forecast_revision_sleeve_seed(
    seed: Value,
    variant_name: &str,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    market_regime: &str,
    candidate_ranking: &str,
) -> Value {
    let mut seed = with_v19_multi_alpha_sleeve_seed(
        seed,
        "forecast_revision_sleeve",
        combo_name,
        ScoreDirection::Descending,
        top_n,
        rebalance_days,
        market_regime,
        candidate_ranking,
    );
    if let Some(object) = seed.as_object_mut() {
        for key in [
            "prediction_set_id",
            "prediction_blend_weight",
            "prediction_min_score",
            "prediction_min_percentile",
            "prediction_label_horizon_days",
            "train_window_ml_policy",
            "train_window_ml_profile",
            "train_window_ml_feature_profile",
            "multi_alpha_sleeve_profile",
            "alpha_sleeve_family",
            "event_gate_profile",
            "event_gate_combo_name",
            "event_gate_version",
            "event_gate_mode",
            "event_gate_min_score",
            "event_gate_boost_weight",
            "event_gate_score_direction",
            "event_gate_active_regimes",
        ] {
            object.remove(key);
        }
    }
    seed["forecast_revision_sleeve_profile"] =
        json!("v19_p313_fq_change_forecast_revision_sleeve_v1");
    seed["forecast_revision_sleeve_variant"] = json!(variant_name);
    seed["alpha_source_family"] = json!("forecast_revision_sleeve");
    seed
}

pub(crate) fn with_v19_shareholder_structure_sleeve_seed(
    seed: Value,
    variant_name: &str,
    combo_name: &str,
    top_n: usize,
    rebalance_days: usize,
    market_regime: &str,
    candidate_ranking: &str,
) -> Value {
    let mut seed = with_v19_multi_alpha_sleeve_seed(
        seed,
        "shareholder_structure_sleeve",
        combo_name,
        ScoreDirection::Descending,
        top_n,
        rebalance_days,
        market_regime,
        candidate_ranking,
    );
    if let Some(object) = seed.as_object_mut() {
        for key in [
            "prediction_set_id",
            "prediction_blend_weight",
            "prediction_min_score",
            "prediction_min_percentile",
            "prediction_label_horizon_days",
            "train_window_ml_policy",
            "train_window_ml_profile",
            "train_window_ml_feature_profile",
            "multi_alpha_sleeve_profile",
            "alpha_sleeve_family",
            "event_gate_profile",
            "event_gate_combo_name",
            "event_gate_version",
            "event_gate_mode",
            "event_gate_min_score",
            "event_gate_boost_weight",
            "event_gate_score_direction",
            "event_gate_active_regimes",
        ] {
            object.remove(key);
        }
    }
    seed["shareholder_structure_sleeve_profile"] =
        json!("v19_p321e_fq_change_shareholder_structure_sleeve_v1");
    seed["shareholder_structure_sleeve_variant"] = json!(variant_name);
    seed["shareholder_structure_sleeve_control"] = json!("phase7_financial_quality_change_v1");
    seed["alpha_admission_gate_id"] = json!("shareholder_structure_low_fanout_strict_pit_gate_v1");
    seed["universe_profile"] = json!("main_chinext_non_st");
    seed["shareholder_structure_source_version"] = json!("p321d-shareholder-low-fanout-v1");
    seed["shareholder_structure_regime_policy"] = json!("evaluation_only_no_reweight");
    seed["oos_policy"] = json!("evaluation_only");
    seed["alpha_source_family"] = json!("shareholder_structure_sleeve");
    seed
}

pub(crate) fn with_v19_event_post_return_overlay_seed(
    seed: Value,
    variant_name: &str,
    top_n: usize,
    rebalance_days: usize,
    score_direction: ScoreDirection,
    market_regime: &str,
    candidate_ranking: &str,
    event_gate: Option<(&str, &str, &str, &str)>,
) -> Value {
    let mut seed = seed;
    if let Some(object) = seed.as_object_mut() {
        for key in [
            "prediction_set_id",
            "prediction_blend_weight",
            "prediction_min_score",
            "prediction_min_percentile",
            "prediction_label_horizon_days",
            "train_window_ml_policy",
            "train_window_ml_profile",
            "train_window_ml_feature_profile",
            "multi_alpha_sleeve_profile",
            "alpha_sleeve_family",
            "event_gate_profile",
            "event_gate_combo_name",
            "event_gate_version",
            "event_gate_mode",
            "event_gate_min_score",
            "event_gate_boost_weight",
            "event_gate_score_direction",
            "event_gate_active_regimes",
        ] {
            object.remove(key);
        }
    }
    seed["event_overlay_profile"] = json!("v19_p39_broad_base_event_post_return_overlay_v1");
    seed["event_overlay_variant"] = json!(variant_name);
    seed["alpha_source_family"] = json!("event_post_return_overlay");
    seed["broad_base_combo_name"] = json!("phase7_financial_quality_v1");
    seed["event_overlay_combo_name"] = json!("phase7_event_post_return_curve_20d_v1");
    seed["optional_overlay_combo_name"] =
        json!("phase7_quality_event_post_return_curve_overlay_v1");
    seed["signal_source"] = json!("factor_combo");
    seed["combo_name"] = json!("phase7_quality_event_post_return_curve_overlay_v1");
    seed["version"] = json!("1.0.0");
    seed["score_direction"] = json!(score_direction.as_str());
    seed["top_n"] = json!(top_n);
    seed["rebalance"] = json!(rebalance_days.to_string());
    seed["market_regime"] = json!(market_regime);
    seed["candidate_ranking"] = json!(candidate_ranking);
    seed["portfolio_method"] = json!("risk_budget");
    seed["risk_budget_lookback_days"] = json!(165);
    seed["capacity_penalty_strength"] = json!("0.75");
    seed["capacity_risk_budget"] =
        json!("capacity_stress_participation_alpha_headroom_floor_70_v1");
    seed["cash_utilization"] = json!("stress_fill_gross_98_v1");
    seed["execution_impact_budget"] = json!("impact_turnover_15pct_v1");
    seed["execution_schedule_profile"] = json!("twap_20d_v1");
    seed["execution_carry_policy"] = json!("roll_forward_v1");
    insert_execution_rule_value(
        &mut seed,
        "execution_carry_policy",
        json!("roll_forward_v1"),
    );
    seed["candidate_risk_filter"] = json!("soft_liquidity_low_volatility_low_correlation_v1");
    seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
    seed["max_position_pct"] = json!("0.08");
    seed["max_gross_exposure"] = json!("1");
    seed["score_candidate_pool_size"] = json!(2200);
    seed["universe_profile"] = json!("listed_non_st");
    seed["event_gate_profile"] = json!("off");

    if let Some((profile_name, mode, min_score, boost_weight)) = event_gate {
        seed = with_event_combo_gate_seed_with_min_score(
            seed,
            profile_name,
            "phase7_event_post_return_curve_20d_v1",
            mode,
            min_score,
            boost_weight,
            ScoreDirection::Descending,
        );
    }

    seed
}

pub(crate) fn with_v19_execution_repair_admission_seed(
    seed: Value,
    variant_name: &str,
    top_n: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    cash_utilization: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    execution_impact_budget: &str,
    score_candidate_pool_size: usize,
) -> Value {
    let seed = with_top_n_seed(seed, top_n, variant_name);
    let seed = with_position_shape_seed(seed, variant_name, max_position_pct, "0.75");
    let seed = with_max_gross_exposure_seed(seed, max_gross_exposure, variant_name);
    let seed = with_execution_robustness_seed(
        seed,
        variant_name,
        capacity_penalty_strength,
        "0.03",
        "0.50",
    );
    let seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
    let seed = with_cash_utilization_seed(seed, cash_utilization);
    let seed = with_execution_schedule_control_seed(
        seed,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
    );
    let seed = with_execution_carry_policy_seed(seed, "roll_forward_v1");
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
    seed["candidate_risk_filter"] = json!("soft_liquidity_low_volatility_low_correlation_v1");
    seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
    seed["v19_execution_repair_profile"] = json!("v19_p2_pit_execution_repair_v1");
    seed["v19_execution_repair_variant"] = json!(variant_name);
    seed
}

pub(crate) fn with_v19_train_window_ml_alpha_rebuild_seed_for_label(
    seed: Value,
    rebuild_profile: &str,
    label_family: &str,
    alpha_source_family: &str,
    ranking_profile: &str,
    label_objective: &str,
    label_horizon_days: usize,
    bucket_count: usize,
    min_samples_per_bucket: usize,
) -> Value {
    let sleeve_family = seed["alpha_sleeve_family"]
        .as_str()
        .unwrap_or("unknown_sleeve");
    let execution_variant = seed["v19_execution_repair_variant"]
        .as_str()
        .unwrap_or("bounded_execution");
    let profile_name = format!(
        "v19_ml_{}_{}_{}",
        label_family, sleeve_family, execution_variant
    );
    let mut seed = with_train_window_ml_stress_fill_seed_for_feature_profile(
        seed,
        &profile_name,
        ranking_profile,
        "phase7_gb_quality_value_recovery_low_impact_v6",
        label_horizon_days,
        bucket_count,
        "0.00",
        label_objective,
    );
    seed["train_window_ml_min_samples_per_bucket"] = json!(min_samples_per_bucket);
    seed["v19_alpha_rebuild_profile"] = json!(rebuild_profile);
    seed["v19_alpha_rebuild_label_family"] = json!(label_family);
    seed["alpha_source_family"] = json!(alpha_source_family);
    seed["stress_fill_objective_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_v19_train_window_ml_alpha_rebuild_seed(seed: Value) -> Value {
    with_v19_train_window_ml_alpha_rebuild_seed_for_label(
        seed,
        "v19_p3_train_window_ml_alpha_rebuild_v1",
        "quality_adjusted_risk_adjusted_excess_return",
        "v19_train_window_ml_alpha_rebuild",
        "nlq_ranker_qarae_h60_bucket10_v19_pit_v1",
        "quality_adjusted_risk_adjusted_excess_return",
        60,
        10,
        100,
    )
}

pub(crate) fn with_v19_train_window_ml_simple_excess_rebuild_seed(seed: Value) -> Value {
    with_v19_train_window_ml_alpha_rebuild_seed_for_label(
        seed,
        "v19_p3_train_window_ml_simple_excess_rebuild_v1",
        "future_excess_return_h45",
        "v19_train_window_ml_simple_excess_rebuild",
        "nlq_ranker_simple_excess_h45_bucket5_v19_pit_v1",
        "future_excess_return",
        45,
        5,
        50,
    )
}

pub(crate) fn with_v19_simple_excess_low_impact_execution_seed(
    seed: Value,
    variant_name: &str,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    cash_utilization: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    execution_impact_budget: &str,
    score_candidate_pool_size: usize,
    partial_rebalance_ratio: &str,
) -> Value {
    let seed = with_top_n_seed(seed, top_n, variant_name);
    let seed = with_rebalance_days_seed(seed, rebalance_days, variant_name);
    let seed = with_position_shape_seed(seed, variant_name, max_position_pct, "0.75");
    let seed = with_max_gross_exposure_seed(seed, max_gross_exposure, variant_name);
    let seed = with_execution_robustness_seed(
        seed,
        variant_name,
        capacity_penalty_strength,
        "0.05",
        partial_rebalance_ratio,
    );
    let seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
    let seed = with_cash_utilization_seed(seed, cash_utilization);
    let seed = with_execution_schedule_control_seed(
        seed,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
    );
    let seed = with_execution_carry_policy_seed(seed, "roll_forward_v1");
    let seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    let mut seed = with_candidate_ranking_seed(seed, "alpha_first_low_impact_v1");
    seed["candidate_risk_filter"] = json!("soft_liquidity_low_volatility_low_correlation_v1");
    seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
    seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
    seed["v19_execution_repair_profile"] = json!("v19_p2_pit_execution_repair_v1");
    seed["v19_execution_repair_variant"] = json!(variant_name);
    seed
}

pub(crate) fn with_v19_train_window_ml_simple_excess_low_impact_rebuild_seed(seed: Value) -> Value {
    with_v19_train_window_ml_alpha_rebuild_seed_for_label(
        seed,
        "v19_p3_train_window_ml_simple_excess_low_impact_rebuild_v1",
        "future_excess_return_h45_low_impact",
        "v19_train_window_ml_simple_excess_low_impact_rebuild",
        "nlq_ranker_simple_excess_h45_bucket5_low_impact_v19_pit_v1",
        "future_excess_return",
        45,
        5,
        50,
    )
}

pub(crate) fn with_v19_train_window_ml_h120_low_impact_rebuild_seed(seed: Value) -> Value {
    with_v19_train_window_ml_alpha_rebuild_seed_for_label(
        seed,
        "v19_p3_train_window_ml_h120_low_impact_rebuild_v1",
        "future_excess_return_h120_low_impact",
        "v19_train_window_ml_h120_low_impact_rebuild",
        "nlq_ranker_future_excess_h120_bucket5_low_impact_v19_pit_v1",
        "future_excess_return",
        120,
        5,
        50,
    )
}

pub(crate) fn with_v19_train_window_ml_rae_h120_residual_capacity_rebuild_seed(seed: Value) -> Value {
    let mut seed = with_v19_train_window_ml_alpha_rebuild_seed_for_label(
        seed,
        "v19_p3_3_train_window_ml_rae_h120_residual_capacity_rebuild_v1",
        "risk_adjusted_excess_return_h120_residual_capacity",
        "v19_train_window_ml_rae_h120_residual_capacity_rebuild",
        "nlq_ranker_rae_h120_bucket7_residual_capacity_v19_pit_v1",
        "risk_adjusted_excess_return",
        120,
        7,
        50,
    );
    seed["candidate_ranking"] = json!("capacity_aware_alpha_liquidity_v1");
    seed["v19_p3_3_objective_profile"] = json!("risk_adjusted_excess_h120_residual_capacity_v1");
    seed
}

pub(crate) fn with_v19_train_window_ml_event_sentiment_rebuild_seed(
    mut seed: Value,
    sleeve_family: &str,
) -> Value {
    seed["alpha_sleeve_family"] = json!(sleeve_family);
    let execution_variant = seed["v19_execution_repair_variant"]
        .as_str()
        .unwrap_or("bounded_execution");
    let profile_name = format!(
        "v19_ml_event_sentiment_h120_{}_{}",
        sleeve_family, execution_variant
    );
    let mut seed = with_train_window_ml_stress_fill_seed_for_feature_profile(
        seed,
        &profile_name,
        "nlq_ranker_event_sentiment_h120_bucket7_v19_pit_v1",
        "phase7_p4_event_sentiment_high_coverage_v1",
        120,
        7,
        "0.00",
        "risk_adjusted_excess_return",
    );
    seed["train_window_ml_min_samples_per_bucket"] = json!(50);
    seed["v19_alpha_rebuild_profile"] =
        json!("v19_p3_4_train_window_ml_event_sentiment_rebuild_v1");
    seed["v19_alpha_rebuild_label_family"] = json!("risk_adjusted_excess_event_sentiment_h120");
    seed["alpha_source_family"] = json!("v19_train_window_ml_event_sentiment_rebuild");
    seed["candidate_ranking"] = json!("capacity_aware_alpha_liquidity_v1");
    seed["v19_p3_4_objective_profile"] = json!("risk_adjusted_excess_event_sentiment_h120_v1");
    seed
}

pub(crate) fn with_prediction_capacity_dual_objective_seed(
    seed: Value,
    profile_name: &str,
    candidate_ranking: &str,
    top_n: usize,
    max_position_pct: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    partial_rebalance_ratio: &str,
    score_candidate_pool_size: usize,
    universe_profile: &str,
) -> Value {
    let seed = with_top_n_seed(seed, top_n, profile_name);
    let seed = with_position_shape_seed(seed, profile_name, max_position_pct, "0.75");
    let seed = with_candidate_ranking_seed(seed, candidate_ranking);
    let seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
    let seed =
        with_execution_robustness_seed(seed, profile_name, "1.50", "0.04", partial_rebalance_ratio);
    let seed = with_execution_schedule_control_seed(
        seed,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
    );
    let seed = with_execution_impact_budget_seed(seed, "impact_turnover_20pct_v1");
    let mut seed = with_cash_utilization_seed(seed, "stress_fill_gross_98_v1");
    seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
    seed["universe_profile"] = json!(universe_profile);
    seed["prediction_capacity_dual_objective_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_prediction_capacity_return_anchor_seed(mut seed: Value) -> Value {
    seed["candidate_ranking"] = json!("off");
    seed["capacity_risk_budget"] = json!("off");
    seed["prediction_capacity_dual_objective_profile"] = json!("return_anchor_capacity_off");
    seed
}

pub(crate) fn with_prediction_target_gross_signal_fidelity_seed(
    seed: Value,
    profile_name: &str,
    max_gross_exposure: &str,
) -> Value {
    let seed = with_max_gross_exposure_seed(seed, max_gross_exposure, profile_name);
    let seed = with_candidate_ranking_seed(seed, "off");
    let seed = with_capacity_risk_budget_seed(seed, "off");
    let mut seed = with_cash_utilization_seed(seed, "off");
    seed["prediction_target_gross_signal_fidelity_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_prediction_confidence_turnover_seed(
    seed: Value,
    profile_name: &str,
    blend_weight: &str,
    min_percentile: Option<&str>,
    rebalance_days: usize,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
    execution_impact_budget: &str,
    execution_carry_policy: &str,
) -> Value {
    let wide_prediction_set = "pred-p7-wf-wide-qgvrel-v1-201602-202605";
    with_prediction_confidence_turnover_seed_for_prediction_set(
        seed,
        profile_name,
        wide_prediction_set,
        blend_weight,
        min_percentile,
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        execution_impact_budget,
        execution_carry_policy,
    )
}

pub(crate) fn with_prediction_confidence_turnover_seed_for_prediction_set(
    seed: Value,
    profile_name: &str,
    prediction_set_id: &str,
    blend_weight: &str,
    min_percentile: Option<&str>,
    rebalance_days: usize,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
    execution_impact_budget: &str,
    execution_carry_policy: &str,
) -> Value {
    let seed =
        with_prediction_confirmation_seed(seed, prediction_set_id, blend_weight, min_percentile);
    let seed = with_rebalance_days_seed(seed, rebalance_days, profile_name);
    let seed = with_rebalance_smoothing_seed(
        seed,
        profile_name,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
    );
    let seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    let seed = with_execution_carry_policy_seed(seed, execution_carry_policy);
    let seed = with_candidate_ranking_seed(seed, "off");
    let seed = with_capacity_risk_budget_seed(seed, "off");
    let mut seed = with_cash_utilization_seed(seed, "off");
    seed["prediction_confidence_turnover_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_prediction_confidence_alpha_lift_seed(
    seed: Value,
    profile_name: &str,
    family: &str,
    combo_name: &str,
    score_direction: ScoreDirection,
    market_regime: &str,
    blend_weight: &str,
    min_percentile: &str,
    rebalance_days: usize,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
    execution_impact_budget: &str,
) -> Value {
    let seed = retarget_seed_combo(seed, combo_name);
    let seed = set_seed_score_direction(seed, score_direction);
    let seed = with_market_regime_seed(seed, market_regime);
    let mut seed = with_prediction_confidence_turnover_seed(
        seed,
        profile_name,
        blend_weight,
        Some(min_percentile),
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        execution_impact_budget,
        "roll_forward_v1",
    );
    seed["prediction_confidence_alpha_lift_profile"] = json!(profile_name);
    seed["prediction_confidence_alpha_lift_family"] = json!(family);
    seed
}

pub(crate) fn with_prediction_long_horizon_low_turnover_seed(
    seed: Value,
    profile_name: &str,
    blend_weight: &str,
    min_percentile: &str,
    rebalance_days: usize,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
    max_gross_exposure: Option<&str>,
) -> Value {
    let long_horizon_prediction_set = "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605";
    let seed = with_prediction_confidence_turnover_seed_for_prediction_set(
        seed,
        profile_name,
        long_horizon_prediction_set,
        blend_weight,
        Some(min_percentile),
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        "impact_turnover_15pct_v1",
        "roll_forward_v1",
    );
    let mut seed = if let Some(max_gross_exposure) = max_gross_exposure {
        with_max_gross_exposure_seed(seed, max_gross_exposure, profile_name)
    } else {
        seed
    };
    seed["prediction_long_horizon_low_turnover_profile"] = json!(profile_name);
    seed["prediction_label_horizon_days"] = json!(60);
    seed
}

pub(crate) fn with_prediction_long_horizon_regime_alpha_seed(
    seed: Value,
    profile_name: &str,
    family: &str,
    combo_name: &str,
    score_direction: ScoreDirection,
    market_regime: &str,
    blend_weight: &str,
    min_percentile: &str,
    rebalance_days: usize,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
    execution_impact_budget: &str,
) -> Value {
    let long_horizon_prediction_set = "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605";
    let seed = retarget_seed_combo(seed, combo_name);
    let seed = set_seed_score_direction(seed, score_direction);
    let seed = with_market_regime_seed(seed, market_regime);
    let mut seed = with_prediction_confidence_turnover_seed_for_prediction_set(
        seed,
        profile_name,
        long_horizon_prediction_set,
        blend_weight,
        Some(min_percentile),
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        execution_impact_budget,
        "roll_forward_v1",
    );
    seed["prediction_long_horizon_regime_alpha_profile"] = json!(profile_name);
    seed["prediction_long_horizon_regime_alpha_family"] = json!(family);
    seed["prediction_label_horizon_days"] = json!(60);
    seed
}

pub(crate) fn with_prediction_h60_nonlinear_stress_seed(
    seed: Value,
    profile_name: &str,
    label_family: &str,
    combo_name: &str,
    score_direction: ScoreDirection,
    market_regime: &str,
    candidate_ranking: &str,
    capacity_risk_budget: &str,
    cash_utilization: &str,
    top_n: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    blend_weight: &str,
    min_percentile: &str,
    rebalance_days: usize,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
    execution_impact_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    score_candidate_pool_size: usize,
) -> Value {
    let long_horizon_prediction_set = "pred-p7-wf-wide-qgvrel-h60-v1-201602-202605";
    let seed = retarget_seed_combo(seed, combo_name);
    let seed = set_seed_score_direction(seed, score_direction);
    let seed = with_market_regime_seed(seed, market_regime);
    let seed = with_prediction_confidence_turnover_seed_for_prediction_set(
        seed,
        profile_name,
        long_horizon_prediction_set,
        blend_weight,
        Some(min_percentile),
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        execution_impact_budget,
        "roll_forward_v1",
    );
    let seed = with_top_n_seed(seed, top_n, profile_name);
    let seed = with_position_shape_seed(seed, profile_name, max_position_pct, "0.75");
    let seed = with_max_gross_exposure_seed(seed, max_gross_exposure, profile_name);
    let seed = with_candidate_ranking_seed(seed, candidate_ranking);
    let seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
    let seed = with_cash_utilization_seed(seed, cash_utilization);
    let seed = with_execution_schedule_control_seed(
        seed,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
    );
    let seed = with_candidate_risk_filter_seed(seed, candidate_risk_filter);
    let seed = with_risk_contribution_control_seed(seed, risk_contribution_control);
    let mut seed = with_execution_carry_policy_seed(seed, "roll_forward_v1");
    seed["prediction_h60_nonlinear_stress_profile"] = json!(profile_name);
    seed["prediction_h60_label_family"] = json!(label_family);
    seed["prediction_label_horizon_days"] = json!(60);
    seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
    seed["universe_profile"] = json!("listed_non_st");
    seed
}

pub(crate) fn with_prediction_h120_low_impact_stress_seed(
    seed: Value,
    profile_name: &str,
    label_family: &str,
    combo_name: &str,
    score_direction: ScoreDirection,
    market_regime: &str,
    candidate_ranking: &str,
    capacity_risk_budget: &str,
    cash_utilization: &str,
    top_n: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    blend_weight: &str,
    min_percentile: &str,
    rebalance_days: usize,
    rebalance_hysteresis_pct: &str,
    partial_rebalance_ratio: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    score_candidate_pool_size: usize,
) -> Value {
    let seed = with_prediction_h60_nonlinear_stress_seed(
        seed,
        profile_name,
        label_family,
        combo_name,
        score_direction,
        market_regime,
        candidate_ranking,
        capacity_risk_budget,
        cash_utilization,
        top_n,
        max_position_pct,
        max_gross_exposure,
        blend_weight,
        min_percentile,
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        "impact_turnover_15pct_v1",
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        candidate_risk_filter,
        risk_contribution_control,
        score_candidate_pool_size,
    );
    let seed = with_prediction_confidence_turnover_seed_for_prediction_set(
        seed,
        profile_name,
        "pred-p7-wf-wide-qgvrel-h120-v1-201602-202605",
        blend_weight,
        Some(min_percentile),
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        "impact_turnover_15pct_v1",
        "roll_forward_v1",
    );
    let seed = with_candidate_ranking_seed(seed, candidate_ranking);
    let seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
    let mut seed = with_cash_utilization_seed(seed, cash_utilization);
    seed["prediction_h120_low_impact_stress_profile"] = json!(profile_name);
    seed["prediction_h120_label_family"] = json!(label_family);
    seed["prediction_label_horizon_days"] = json!(120);
    if let Some(object) = seed.as_object_mut() {
        object.remove("prediction_h60_nonlinear_stress_profile");
        object.remove("prediction_h60_label_family");
    }
    seed
}

pub(crate) fn with_train_window_stress_fill_target_exposure_seed(
    seed: Value,
    profile_name: &str,
    combo_name: &str,
    score_direction: ScoreDirection,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    cash_utilization: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    candidate_risk_filter: &str,
    risk_contribution_control: &str,
    market_regime: &str,
    execution_impact_budget: &str,
) -> Value {
    let mut seed = with_train_window_nonlinear_ranking_seed(
        seed,
        combo_name,
        score_direction,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        cash_utilization,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        candidate_risk_filter,
        risk_contribution_control,
        market_regime,
        execution_impact_budget,
        profile_name,
    );
    seed["train_window_stress_fill_target_exposure_profile"] = json!(profile_name);
    seed["alpha_source_family"] = json!("train_window_stress_fill_target_exposure");
    seed
}

pub(crate) fn with_train_window_ml_stress_fill_seed(
    seed: Value,
    profile_name: &str,
    ml_profile: &str,
    label_horizon_days: usize,
    bucket_count: usize,
    min_prediction_score: &str,
    label_objective: &str,
) -> Value {
    with_train_window_ml_stress_fill_seed_for_feature_profile(
        seed,
        profile_name,
        ml_profile,
        "phase7_gb_quality_value_recovery_low_impact_v5",
        label_horizon_days,
        bucket_count,
        min_prediction_score,
        label_objective,
    )
}

pub(crate) fn with_train_window_ml_stress_fill_seed_for_feature_profile(
    mut seed: Value,
    profile_name: &str,
    ml_profile: &str,
    feature_profile: &str,
    label_horizon_days: usize,
    bucket_count: usize,
    min_prediction_score: &str,
    label_objective: &str,
) -> Value {
    seed["train_window_ml_ranking_profile"] = json!(ml_profile);
    seed["train_window_ml_feature_profile"] = json!(feature_profile);
    seed["train_window_ml_label_objective"] = json!(label_objective);
    seed["train_window_ml_label_horizon_days"] = json!(label_horizon_days);
    seed["train_window_ml_bucket_count"] = json!(bucket_count);
    seed["train_window_ml_min_samples_per_bucket"] = json!(100);
    seed["train_window_ml_pit_policy"] = json!("train-window rolling fit; no OOS labels");
    seed["portfolio_method"] = json!("stress_fill_aware_risk_budget");
    seed["stress_fill_portfolio_construction"] =
        json!("ml_score_capacity_correlation_risk_budget_target_exposure_v1");
    seed["stress_fill_objective_profile"] = json!(profile_name);
    seed["stress_fill_confidence_exposure"] =
        json!("prediction_confidence_ascending_capacity_headroom_v1");
    seed["alpha_source_family"] = json!("train_window_ml_stress_fill");
    seed["prediction_confidence_gate_profile"] = json!("train_positive_raw_score_gate_v1");
    seed["train_window_ml_prediction_min_score"] = json!(min_prediction_score);
    if let Some(object) = seed.as_object_mut() {
        object.remove("prediction_set_id");
        object.remove("prediction_blend_weight");
        object.remove("prediction_min_percentile");
        object.remove("prediction_min_score");
        object.remove("prediction_label_horizon_days");
        object.remove("prediction_set_override");
        object.remove("prediction_set_override_source");
    }
    seed
}

pub(crate) fn with_current_event_nonlinear_alpha_seed(
    mut seed: Value,
    profile_name: &str,
    family: &str,
) -> Value {
    seed["current_event_nonlinear_alpha_profile"] = json!(profile_name);
    seed["current_event_nonlinear_alpha_family"] = json!(family);
    seed["alpha_source_family"] = json!(family);
    seed
}

pub(crate) fn with_bull_sleeve_cash_recovery_seed(
    seed: Value,
    combo_name: &str,
    score_direction: ScoreDirection,
    top_n: usize,
    rebalance_days: usize,
    max_position_pct: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    score_candidate_pool_size: usize,
    market_regime: &str,
    candidate_risk_filter: &str,
    universe_profile: &str,
    profile_name: &str,
) -> Value {
    let seed = set_seed_score_direction(retarget_seed_combo(seed, combo_name), score_direction);
    let seed = with_market_regime_seed(seed, market_regime);
    let seed = with_candidate_ranking_seed(seed, "capacity_aware_alpha_liquidity_v1");
    let seed = with_candidate_risk_filter_seed(seed, candidate_risk_filter);
    let seed = with_risk_contribution_control_seed(seed, "soft_single_name_20pct_v1");
    let seed = with_rebalance_days_seed(seed, rebalance_days, profile_name);
    let seed = with_capacity_fill_frontier_seed(
        seed,
        top_n,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
    );
    let seed = with_cash_utilization_seed(seed, "stress_fill_gross_98_v1");
    let mut seed = with_execution_impact_budget_seed(seed, "impact_turnover_20pct_v1");
    seed["universe_profile"] = json!(universe_profile);
    seed["bull_sleeve_cash_recovery_profile"] = json!(profile_name);
    seed
}

pub(crate) fn with_return_first_fill_repair_seed(
    seed: Value,
    top_n: usize,
    max_position_pct: &str,
    max_pairwise_correlation: &str,
    max_gross_exposure: &str,
    capacity_penalty_strength: &str,
    capacity_risk_budget: &str,
    cash_utilization: &str,
    execution_schedule_profile: &str,
    daily_target_move_limit_pct: Decimal,
    max_carry_days: usize,
    execution_impact_budget: &str,
    score_candidate_pool_size: usize,
    profile_name: &str,
) -> Value {
    let seed = with_candidate_ranking_seed(seed, "capacity_aware_alpha_liquidity_v1");
    let seed = with_candidate_risk_filter_seed(seed, "off");
    let seed = with_risk_contribution_control_seed(seed, "off");
    let seed = with_top_n_seed(seed, top_n, profile_name);
    let seed = with_position_shape_seed(
        seed,
        profile_name,
        max_position_pct,
        max_pairwise_correlation,
    );
    let seed = with_max_gross_exposure_seed(seed, max_gross_exposure, profile_name);
    let seed = with_execution_robustness_seed(
        seed,
        profile_name,
        capacity_penalty_strength,
        "0.02",
        "0.50",
    );
    let seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
    let seed = with_cash_utilization_seed(seed, cash_utilization);
    let seed = with_execution_schedule_control_seed(
        seed,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
    );
    let seed = with_execution_carry_policy_seed(seed, "roll_forward_v1");
    let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
    seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
    seed["return_first_fill_repair_profile"] = json!(profile_name);
    seed
}


// ============================================================
// SeedTrialGenerator trait 骨架（后续批次接入，当前仅声明）
// ============================================================

/// 种子试验生成器 trait：统一种子生成接口，后续批次将 professional_* 接入。
/// 当前为骨架声明，具体实现仍由各 pub(crate) fn professional_*_seed_trials 承载。
#[allow(dead_code)]
pub(crate) trait SeedTrialGenerator {
    /// 生成种子试验列表。
    fn generate_seed_trials(&self) -> Vec<serde_json::Value>;
}

