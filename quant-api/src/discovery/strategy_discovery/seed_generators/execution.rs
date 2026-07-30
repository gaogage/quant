//! BC4 策略发现 / seed_generators / execution：执行/容量 种子生成器。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。

use rust_decimal::Decimal;
use serde_json::{json, Value};

use super::super::profiles::ScoreDirection;
use super::*;

pub(crate) fn professional_execution_robust_candidate_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(anchor, "off"), "off");

    let mut seeds = Vec::new();
    for (
        top_n,
        rebalance_days,
        max_position_pct,
        max_pairwise_correlation,
        risk_budget_lookback_days,
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_min_exposure,
        capacity_penalty_strength,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        score_candidate_pool_size,
    ) in [
        (
            20,
            60,
            "0.15",
            "0.70",
            165,
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.66",
            "1",
            "0.01",
            "0.75",
            500,
        ),
        (
            20,
            80,
            "0.12",
            "0.70",
            165,
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.67",
            "1",
            "0.01",
            "0.75",
            500,
        ),
        (
            30,
            80,
            "0.10",
            "0.70",
            168,
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.67",
            "1",
            "0.01",
            "0.75",
            500,
        ),
        (
            30,
            80,
            "0.12",
            "0.70",
            168,
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_68",
            "0.68",
            "1.25",
            "0.01",
            "0.75",
            500,
        ),
        (
            30,
            120,
            "0.10",
            "0.68",
            168,
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_68",
            "0.68",
            "1.25",
            "0.02",
            "0.50",
            650,
        ),
        (
            50,
            120,
            "0.12",
            "0.70",
            180,
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_67",
            "0.67",
            "1.25",
            "0.02",
            "0.50",
            650,
        ),
        (
            50,
            120,
            "0.10",
            "0.68",
            180,
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.66",
            "1",
            "0.02",
            "0.50",
            650,
        ),
    ] {
        let seed = with_execution_robustness_seed(
            with_top_n_seed(
                with_rebalance_days_seed(
                    with_position_shape_seed(
                        with_sharpe_profile_seed(
                            with_risk_budget_lookback_seed(
                                with_volatility_profile_seed(
                                    base.clone(),
                                    vol_profile,
                                    target_pct,
                                    120,
                                    min_exposure,
                                    "1",
                                ),
                                &format!(
                                    "bp_rb{risk_budget_lookback_days}_execution_robust_candidate"
                                ),
                                risk_budget_lookback_days,
                            ),
                            sharpe_profile,
                            "0.50",
                            "-0.10",
                            180,
                            sharpe_min_exposure,
                        ),
                        &format!(
                            "maxpos{}_corr{}_execution_robust",
                            max_position_pct.replace('.', ""),
                            max_pairwise_correlation.replace('.', "")
                        ),
                        max_position_pct,
                        max_pairwise_correlation,
                    ),
                    rebalance_days,
                    &format!("rebalance{rebalance_days}_execution_robust"),
                ),
                top_n,
                &format!("top{top_n}_execution_robust"),
            ),
            &format!(
                "cap{}_hyst{}_partial{}_execution_robust",
                capacity_penalty_strength.replace('.', ""),
                rebalance_hysteresis_pct.replace('.', ""),
                partial_rebalance_ratio.replace('.', "")
            ),
            capacity_penalty_strength,
            rebalance_hysteresis_pct,
            partial_rebalance_ratio,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            "quality_mixed_state_risk_memory_router_v14",
            &format!("{vol_profile}_{sharpe_profile}_execution_robust_candidate"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_execution_low_turnover_alpha_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    for (
        market_regime,
        candidate_risk_filter,
        risk_contribution_control,
        top_n,
        rebalance_days,
        max_position_pct,
        max_pairwise_correlation,
        risk_budget_lookback_days,
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_min_exposure,
        capacity_penalty_strength,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        score_candidate_pool_size,
    ) in [
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            50,
            160,
            "0.10",
            "0.68",
            180,
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.67",
            "1.25",
            "0.02",
            "0.50",
            650,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            80,
            180,
            "0.08",
            "0.65",
            220,
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_68",
            "0.68",
            "1.5",
            "0.03",
            "0.35",
            650,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            50,
            180,
            "0.10",
            "0.70",
            220,
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_67",
            "0.67",
            "1.5",
            "0.03",
            "0.35",
            650,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            50,
            160,
            "0.10",
            "0.68",
            180,
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.67",
            "1.25",
            "0.02",
            "0.50",
            650,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "soft_low_volatility_v1",
            "soft_single_name_15pct_v1",
            30,
            120,
            "0.12",
            "0.70",
            180,
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.66",
            "1.25",
            "0.02",
            "0.50",
            500,
        ),
    ] {
        let base = with_risk_contribution_control_seed(
            with_candidate_risk_filter_seed(anchor.clone(), candidate_risk_filter),
            risk_contribution_control,
        );
        let seed = with_execution_robustness_seed(
            with_top_n_seed(
                with_rebalance_days_seed(
                    with_position_shape_seed(
                        with_sharpe_profile_seed(
                            with_risk_budget_lookback_seed(
                                with_volatility_profile_seed(
                                    base,
                                    vol_profile,
                                    target_pct,
                                    120,
                                    min_exposure,
                                    "1",
                                ),
                                &format!("bp_rb{risk_budget_lookback_days}_execution_low_turnover"),
                                risk_budget_lookback_days,
                            ),
                            sharpe_profile,
                            "0.50",
                            "-0.10",
                            180,
                            sharpe_min_exposure,
                        ),
                        &format!(
                            "maxpos{}_corr{}_execution_low_turnover",
                            max_position_pct.replace('.', ""),
                            max_pairwise_correlation.replace('.', "")
                        ),
                        max_position_pct,
                        max_pairwise_correlation,
                    ),
                    rebalance_days,
                    &format!("rebalance{rebalance_days}_execution_low_turnover"),
                ),
                top_n,
                &format!("top{top_n}_execution_low_turnover"),
            ),
            &format!(
                "cap{}_hyst{}_partial{}_execution_low_turnover",
                capacity_penalty_strength.replace('.', ""),
                rebalance_hysteresis_pct.replace('.', ""),
                partial_rebalance_ratio.replace('.', "")
            ),
            capacity_penalty_strength,
            rebalance_hysteresis_pct,
            partial_rebalance_ratio,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            market_regime,
            &format!("{vol_profile}_{sharpe_profile}_execution_low_turnover_alpha"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        seed["universe_profile"] = json!("listed_non_st");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_execution_capacity_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_execution_low_turnover_alpha_seed_trials() {
        for capacity_risk_budget in [
            "capacity_participation_balanced_v1",
            "capacity_participation_strict_v1",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_capacity_risk_budget_seed(
                    seed.clone(),
                    capacity_risk_budget,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_impact_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_execution_capacity_budget_seed_trials() {
        for execution_impact_budget in [
            "impact_turnover_30pct_v1",
            "impact_turnover_20pct_v1",
            "impact_turnover_15pct_v1",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_execution_impact_budget_seed(
                    seed.clone(),
                    execution_impact_budget,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_schedule_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_execution_impact_budget_seed_trials() {
        for execution_schedule_profile in ["twap_3d_v1", "twap_5d_v1", "twap_10d_v1"] {
            append_unique_seeds(
                &mut seeds,
                vec![with_execution_schedule_seed(
                    seed.clone(),
                    execution_schedule_profile,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_patient_schedule_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_execution_impact_budget_seed_trials() {
        for execution_schedule_profile in ["twap_10d_v1", "twap_15d_v1", "twap_20d_v1"] {
            append_unique_seeds(
                &mut seeds,
                vec![with_execution_schedule_seed(
                    seed.clone(),
                    execution_schedule_profile,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_daily_cap_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        ("twap_15d_v1", Decimal::new(5, 2), 20usize),
        ("twap_20d_v1", Decimal::new(35, 3), 30usize),
        ("twap_10d_v1", Decimal::new(25, 3), 15usize),
    ];
    for seed in professional_execution_impact_budget_seed_trials() {
        for (execution_schedule_profile, daily_target_move_limit_pct, max_carry_days) in controls {
            append_unique_seeds(
                &mut seeds,
                vec![with_execution_schedule_control_seed(
                    seed.clone(),
                    execution_schedule_profile,
                    daily_target_move_limit_pct,
                    max_carry_days,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_cash_drag_aware_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        ("twap_10d_v1", Decimal::new(8, 2), 30usize),
        ("twap_15d_v1", Decimal::new(75, 3), 40usize),
        ("twap_20d_v1", Decimal::new(6, 2), 45usize),
    ];
    for seed in professional_execution_impact_budget_seed_trials() {
        for (execution_schedule_profile, daily_target_move_limit_pct, max_carry_days) in controls {
            append_unique_seeds(
                &mut seeds,
                vec![with_execution_schedule_control_seed(
                    seed.clone(),
                    execution_schedule_profile,
                    daily_target_move_limit_pct,
                    max_carry_days,
                )],
            );
        }
    }
    append_unique_seeds(
        &mut seeds,
        professional_execution_daily_cap_budget_seed_trials(),
    );
    seeds
}

pub(crate) fn professional_execution_feasible_fill_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_execution_cash_drag_aware_budget_seed_trials() {
        for cash_utilization in ["fillable_gross_90_v1", "fillable_gross_95_v1"] {
            append_unique_seeds(
                &mut seeds,
                vec![with_cash_utilization_seed(seed.clone(), cash_utilization)],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_rolling_carry_budget_seed_trials() -> Vec<Value> {
    professional_execution_feasible_fill_budget_seed_trials()
        .into_iter()
        .map(|seed| with_execution_carry_policy_seed(seed, "roll_forward_v1"))
        .collect()
}

pub(crate) fn professional_execution_capacity_fill_frontier_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        (
            120usize,
            "0.06",
            "0.90",
            "2.0",
            "capacity_participation_strict_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            45usize,
            900usize,
        ),
        (
            160usize,
            "0.05",
            "0.80",
            "2.0",
            "capacity_participation_strict_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            60usize,
            1200usize,
        ),
        (
            80usize,
            "0.08",
            "1",
            "1.5",
            "capacity_participation_balanced_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            40usize,
            900usize,
        ),
    ];
    for seed in professional_execution_rolling_carry_budget_seed_trials() {
        for (
            top_n,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
        ) in controls
        {
            append_unique_seeds(
                &mut seeds,
                vec![with_capacity_fill_frontier_seed(
                    seed.clone(),
                    top_n,
                    max_position_pct,
                    max_gross_exposure,
                    capacity_penalty_strength,
                    capacity_risk_budget,
                    execution_schedule_profile,
                    daily_target_move_limit_pct,
                    max_carry_days,
                    score_candidate_pool_size,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_alpha_capacity_bridge_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        (
            80usize,
            "0.08",
            "1",
            "1.5",
            "capacity_participation_balanced_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            40usize,
            900usize,
            "impact_turnover_20pct_v1",
        ),
        (
            120usize,
            "0.06",
            "0.90",
            "2.0",
            "capacity_participation_strict_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            45usize,
            900usize,
            "impact_turnover_15pct_v1",
        ),
    ];
    for seed in professional_return_alpha_sharpe_bridge_seed_trials() {
        for (
            top_n,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            execution_impact_budget,
        ) in controls
        {
            let seed = with_capacity_fill_frontier_seed(
                seed.clone(),
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
            append_unique_seeds(
                &mut seeds,
                vec![with_execution_impact_budget_seed(
                    seed,
                    execution_impact_budget,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_execution_alpha_capacity_return_frontier_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        (
            80usize,
            "0.08",
            "1",
            "1.5",
            "capacity_participation_balanced_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            30usize,
            900usize,
            "impact_turnover_20pct_v1",
            None,
        ),
        (
            80usize,
            "0.08",
            "1",
            "1.25",
            "capacity_participation_balanced_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            40usize,
            900usize,
            "impact_turnover_30pct_v1",
            None,
        ),
        (
            60usize,
            "0.10",
            "1",
            "1.5",
            "capacity_participation_balanced_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            30usize,
            650usize,
            "impact_turnover_20pct_v1",
            Some("listed_non_st"),
        ),
        (
            100usize,
            "0.07",
            "0.95",
            "2.0",
            "capacity_participation_strict_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            45usize,
            1200usize,
            "impact_turnover_15pct_v1",
            Some("listed_non_st"),
        ),
    ];
    for seed in professional_return_alpha_sharpe_bridge_seed_trials() {
        for (
            top_n,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            execution_impact_budget,
            universe_profile,
        ) in controls
        {
            let seed = with_capacity_fill_frontier_seed(
                seed.clone(),
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
            let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
            if let Some(universe_profile) = universe_profile {
                seed["universe_profile"] = json!(universe_profile);
            }
            append_unique_seeds(&mut seeds, vec![seed]);
        }
    }
    seeds
}

pub(crate) fn professional_execution_stress_fill_return_frontier_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        (
            120usize,
            "0.06",
            "1",
            "2.0",
            "capacity_participation_strict_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            45usize,
            1200usize,
            "impact_turnover_15pct_v1",
            "listed_non_st",
        ),
        (
            160usize,
            "0.05",
            "1",
            "2.0",
            "capacity_participation_strict_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            60usize,
            1500usize,
            "impact_turnover_15pct_v1",
            "listed_non_st",
        ),
        (
            120usize,
            "0.06",
            "0.95",
            "1.5",
            "capacity_participation_balanced_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            45usize,
            1200usize,
            "impact_turnover_20pct_v1",
            "listed_non_st",
        ),
        (
            80usize,
            "0.08",
            "1",
            "1.5",
            "capacity_participation_balanced_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            30usize,
            1200usize,
            "impact_turnover_20pct_v1",
            "all",
        ),
    ];
    for seed in professional_return_alpha_sharpe_bridge_seed_trials() {
        for (
            top_n,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            execution_impact_budget,
            universe_profile,
        ) in controls
        {
            let seed = with_capacity_fill_frontier_seed(
                seed.clone(),
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
            let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
            seed["universe_profile"] = json!(universe_profile);
            append_unique_seeds(&mut seeds, vec![seed]);
        }
    }
    seeds
}

pub(crate) fn professional_execution_stress_risk_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        (
            80usize,
            "0.08",
            "1",
            "1.5",
            "capacity_stress_participation_soft_cap_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            30usize,
            1200usize,
            "impact_turnover_20pct_v1",
            "all",
        ),
        (
            60usize,
            "0.10",
            "1",
            "1.5",
            "capacity_stress_participation_soft_cap_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            30usize,
            900usize,
            "impact_turnover_20pct_v1",
            "listed_non_st",
        ),
        (
            100usize,
            "0.07",
            "0.95",
            "2.0",
            "capacity_stress_participation_soft_cap_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            45usize,
            1200usize,
            "impact_turnover_15pct_v1",
            "listed_non_st",
        ),
        (
            80usize,
            "0.08",
            "0.95",
            "2.0",
            "capacity_participation_strict_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            45usize,
            1200usize,
            "impact_turnover_15pct_v1",
            "listed_non_st",
        ),
    ];
    for seed in professional_return_alpha_sharpe_bridge_seed_trials() {
        for (
            top_n,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            execution_impact_budget,
            universe_profile,
        ) in controls
        {
            let seed = with_capacity_fill_frontier_seed(
                seed.clone(),
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
            let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
            seed["universe_profile"] = json!(universe_profile);
            append_unique_seeds(&mut seeds, vec![seed]);
        }
    }
    seeds
}

pub(crate) fn professional_execution_capacity_stress_return_gate_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        (
            100usize,
            "0.06",
            "0.95",
            "2.5",
            "capacity_stress_participation_soft_cap_v1",
            "twap_15d_v1",
            Decimal::new(6, 2),
            60usize,
            1500usize,
            "impact_turnover_15pct_v1",
            "listed_non_st",
        ),
        (
            120usize,
            "0.05",
            "0.90",
            "2.5",
            "capacity_participation_strict_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            60usize,
            1500usize,
            "impact_turnover_15pct_v1",
            "listed_non_st",
        ),
        (
            80usize,
            "0.08",
            "1",
            "2.0",
            "capacity_stress_participation_soft_cap_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            45usize,
            1200usize,
            "impact_turnover_20pct_v1",
            "all",
        ),
    ];
    for seed in professional_return_alpha_sharpe_bridge_seed_trials() {
        for (
            top_n,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            execution_impact_budget,
            universe_profile,
        ) in controls
        {
            let seed = with_capacity_fill_frontier_seed(
                seed.clone(),
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
            let mut seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
            seed["universe_profile"] = json!(universe_profile);
            append_unique_seeds(&mut seeds, vec![seed]);
        }
    }
    seeds
}

pub(crate) fn professional_execution_low_impact_alpha_stress_return_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    let controls = [
        (
            "phase7_quality_event_window_overlay_v1",
            100usize,
            180usize,
            "0.06",
            "0.95",
            "2.5",
            "capacity_stress_participation_soft_cap_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            60usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_value_recovery_confirm_v1",
            120usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_participation_strict_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "main_board_non_st",
        ),
        (
            "phase7_quality_residual_confirm_10pct_v1",
            120usize,
            220usize,
            "0.05",
            "0.90",
            "2.5",
            "capacity_stress_participation_soft_cap_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            60usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "listed_non_st",
        ),
        (
            "phase7_blend_quality_growth_v1",
            100usize,
            160usize,
            "0.06",
            "0.95",
            "2.0",
            "capacity_stress_participation_soft_cap_v1",
            "twap_15d_v1",
            Decimal::new(6, 2),
            60usize,
            1200usize,
            "soft_low_volatility_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
        (
            "phase7_financial_quality_v1",
            80usize,
            160usize,
            "0.08",
            "0.95",
            "2.0",
            "capacity_stress_participation_soft_cap_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            45usize,
            1200usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
    ];
    for (
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
    ) in controls
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_low_impact_alpha_stress_return_seed(
                anchor.clone(),
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
            )],
        );
    }
    seeds
}

pub(crate) fn professional_execution_stress_target_scaling_return_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    let controls = [
        (
            "phase7_quality_event_window_overlay_v1",
            100usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_target_scale_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.05",
            "0.80",
            "3.0",
            "capacity_stress_participation_target_scale_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            160usize,
            180usize,
            "0.04",
            "0.70",
            "3.0",
            "capacity_stress_participation_target_scale_v1",
            "twap_20d_v1",
            Decimal::new(4, 2),
            90usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_financial_quality_v1",
            120usize,
            160usize,
            "0.05",
            "0.80",
            "2.5",
            "capacity_stress_participation_target_scale_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
        (
            "phase7_quality_residual_confirm_10pct_v1",
            120usize,
            220usize,
            "0.05",
            "0.80",
            "3.0",
            "capacity_stress_participation_target_scale_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "listed_non_st",
        ),
    ];
    for (
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
    ) in controls
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_low_impact_alpha_stress_return_seed(
                anchor.clone(),
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
            )],
        );
    }
    seeds
}

pub(crate) fn professional_execution_stress_floor_scaling_return_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    let controls = [
        (
            "phase7_quality_event_window_overlay_v1",
            100usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_floor_35_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            100usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_floor_50_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.05",
            "0.80",
            "3.0",
            "capacity_stress_participation_floor_35_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_financial_quality_v1",
            100usize,
            160usize,
            "0.06",
            "0.80",
            "2.5",
            "capacity_stress_participation_floor_35_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
    ];
    for (
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
    ) in controls
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_low_impact_alpha_stress_return_seed(
                anchor.clone(),
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
            )],
        );
    }
    seeds
}

pub(crate) fn professional_execution_stress_floor_return_recovery_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    let controls = [
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            100usize,
            180usize,
            "0.06",
            "1.00",
            "2.5",
            "capacity_stress_participation_soft_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_financial_quality_v1",
            120usize,
            160usize,
            "0.05",
            "0.90",
            "2.0",
            "capacity_stress_participation_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
    ];
    for (
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
    ) in controls
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_low_impact_alpha_stress_return_seed(
                anchor.clone(),
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
            )],
        );
    }
    seeds
}

pub(crate) fn professional_execution_pressure_headroom_floor_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    let controls = [
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            160usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            90usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            160usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            90usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_financial_quality_v1",
            160usize,
            160usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            90usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
    ];
    for (
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
    ) in controls
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_low_impact_alpha_stress_return_seed(
                anchor.clone(),
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
            )],
        );
    }
    seeds
}

pub(crate) fn professional_execution_alpha_headroom_floor_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    let controls = [
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            160usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            90usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_financial_quality_v1",
            120usize,
            160usize,
            "0.05",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
    ];
    for (
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
    ) in controls
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_low_impact_alpha_stress_return_seed(
                anchor.clone(),
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
            )],
        );
    }
    seeds
}

pub(crate) fn professional_execution_blended_alpha_headroom_floor_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    let controls = [
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_blended_alpha_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            160usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            90usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "listed_non_st",
        ),
        (
            "phase7_financial_quality_v1",
            120usize,
            160usize,
            "0.05",
            "0.90",
            "2.5",
            "capacity_stress_participation_blended_alpha_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(5, 2),
            75usize,
            1500usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "listed_non_st",
        ),
    ];
    for (
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
    ) in controls
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_low_impact_alpha_stress_return_seed(
                anchor.clone(),
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
            )],
        );
    }
    seeds
}

pub(crate) fn professional_execution_event_anchor_stress_bridge_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let controls = [
        (
            80usize,
            160usize,
            "0.08",
            "0.95",
            "2.0",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(6, 2),
            75usize,
            1500usize,
        ),
        (
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(5, 2),
            90usize,
            1500usize,
        ),
        (
            160usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_headroom_floor_70_v1",
            Decimal::new(5, 2),
            90usize,
            1800usize,
        ),
    ];

    if let Some(anchor) = phase7_current_anchor_bg_trial4_seed() {
        for (
            top_n,
            rebalance_days,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
        ) in controls.iter().copied()
        {
            append_unique_seeds(
                &mut seeds,
                vec![with_event_anchor_execution_stress_seed(
                    anchor.clone(),
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
                )],
            );
        }
    }

    for seed in professional_return_distribution_repair_seed_trials()
        .into_iter()
        .take(2)
    {
        for (
            top_n,
            rebalance_days,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
        ) in controls.iter().copied().take(2)
        {
            append_unique_seeds(
                &mut seeds,
                vec![with_event_anchor_execution_stress_seed(
                    seed.clone(),
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
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_execution_participation_aware_event_anchor_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    let mut event_anchor_seeds = Vec::new();
    let mut high_sharpe_anchor_seeds = Vec::new();
    let controls = [
        (
            80usize,
            160usize,
            "0.08",
            "0.95",
            "2.0",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(6, 2),
            75usize,
            1800usize,
            Decimal::new(10, 2),
        ),
        (
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(5, 2),
            90usize,
            2200usize,
            Decimal::new(10, 2),
        ),
        (
            160usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_headroom_floor_70_v1",
            Decimal::new(5, 2),
            90usize,
            2200usize,
            Decimal::new(5, 2),
        ),
    ];

    if let Some(anchor) = phase7_current_anchor_bg_trial4_seed() {
        for (
            top_n,
            rebalance_days,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            max_participation_rate,
        ) in controls.iter().copied()
        {
            let seed = with_event_anchor_execution_stress_seed(
                anchor.clone(),
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
            let seed = with_candidate_risk_filter_seed(
                seed,
                "soft_liquidity_low_volatility_low_correlation_v1",
            );
            let mut seed = with_execution_participation_limit_seed(seed, max_participation_rate);
            seed["execution_participation_aware_anchor_profile"] = json!(format!(
                "event_anchor_participation_top{top_n}_rebalance{rebalance_days}"
            ));
            append_unique_seeds(&mut event_anchor_seeds, vec![seed]);
        }
    }

    for seed in professional_return_distribution_repair_seed_trials()
        .into_iter()
        .take(2)
    {
        for (
            top_n,
            rebalance_days,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            max_participation_rate,
        ) in controls.iter().copied().take(2)
        {
            let seed = with_event_anchor_execution_stress_seed(
                seed.clone(),
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
            let seed = with_candidate_risk_filter_seed(
                seed,
                "soft_liquidity_low_volatility_low_correlation_v1",
            );
            let mut seed = with_execution_participation_limit_seed(seed, max_participation_rate);
            seed["execution_participation_aware_anchor_profile"] = json!(format!(
                "event_anchor_participation_top{top_n}_rebalance{rebalance_days}"
            ));
            append_unique_seeds(&mut high_sharpe_anchor_seeds, vec![seed]);
        }
    }

    if let Some(seed) = event_anchor_seeds.first().cloned() {
        append_unique_seeds(&mut seeds, vec![seed]);
    }
    if let Some(seed) = high_sharpe_anchor_seeds.first().cloned() {
        append_unique_seeds(&mut seeds, vec![seed]);
    }
    for seed in event_anchor_seeds
        .into_iter()
        .skip(1)
        .chain(high_sharpe_anchor_seeds.into_iter().skip(1))
    {
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_execution_cl_anchor_fill_recovery_seed_trials() -> Vec<Value> {
    let high_sharpe_anchors = professional_return_distribution_repair_seed_trials();
    let Some(cl_anchor) = high_sharpe_anchors.first().cloned() else {
        return Vec::new();
    };
    let cm_anchor = high_sharpe_anchors.get(1).cloned();
    let mut seeds = Vec::new();

    for (
        anchor,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        max_participation_rate,
        risk_contribution_control,
    ) in [
        (
            cl_anchor.clone(),
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(8, 2),
            90usize,
            2400usize,
            Decimal::new(10, 2),
            "soft_single_name_20pct_v1",
        ),
        (
            cl_anchor.clone(),
            160usize,
            180usize,
            "0.05",
            "0.90",
            "3.0",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(6, 2),
            90usize,
            2600usize,
            Decimal::new(10, 2),
            "soft_single_name_15pct_v1",
        ),
        (
            cl_anchor,
            100usize,
            160usize,
            "0.07",
            "0.95",
            "2.0",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            Decimal::new(12, 2),
            "soft_single_name_20pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_cl_anchor_fill_recovery_seed(
                anchor,
                top_n,
                rebalance_days,
                max_position_pct,
                max_gross_exposure,
                capacity_penalty_strength,
                capacity_risk_budget,
                daily_target_move_limit_pct,
                max_carry_days,
                score_candidate_pool_size,
                max_participation_rate,
                risk_contribution_control,
            )],
        );
    }

    if let Some(cm_anchor) = cm_anchor {
        for (
            top_n,
            rebalance_days,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            max_participation_rate,
            risk_contribution_control,
        ) in [
            (
                120usize,
                180usize,
                "0.06",
                "0.90",
                "2.5",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                Decimal::new(8, 2),
                90usize,
                2400usize,
                Decimal::new(10, 2),
                "soft_single_name_20pct_v1",
            ),
            (
                160usize,
                180usize,
                "0.05",
                "0.90",
                "3.0",
                "capacity_stress_participation_headroom_floor_70_v1",
                Decimal::new(6, 2),
                90usize,
                2600usize,
                Decimal::new(10, 2),
                "soft_single_name_15pct_v1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_cl_anchor_fill_recovery_seed(
                    cm_anchor.clone(),
                    top_n,
                    rebalance_days,
                    max_position_pct,
                    max_gross_exposure,
                    capacity_penalty_strength,
                    capacity_risk_budget,
                    daily_target_move_limit_pct,
                    max_carry_days,
                    score_candidate_pool_size,
                    max_participation_rate,
                    risk_contribution_control,
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_execution_pit_alpha_first_low_impact_seed_trials() -> Vec<Value> {
    let Some(event_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let high_sharpe_anchors = professional_return_distribution_repair_seed_trials();
    let high_sharpe_anchor = high_sharpe_anchors.first().cloned();
    let mut seeds = Vec::new();

    for (
        seed,
        profile_name,
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
    ) in [
        (
            event_anchor.clone(),
            "pit_alpha_first_event_top80_rebalance160",
            "phase7_financial_quality_v1",
            80usize,
            160usize,
            "0.08",
            "0.95",
            "1.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            1800usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            "listed_non_st",
        ),
        (
            event_anchor,
            "pit_alpha_first_event_top100_rebalance180",
            "phase7_financial_quality_v1",
            100usize,
            180usize,
            "0.08",
            "0.90",
            "2.0",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(8, 2),
            90usize,
            2200usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            "listed_non_st",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_pit_alpha_first_low_impact_seed(
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
                profile_name,
            )],
        );
    }

    if let Some(high_sharpe_anchor) = high_sharpe_anchor {
        append_unique_seeds(
            &mut seeds,
            vec![with_pit_alpha_first_low_impact_seed(
                high_sharpe_anchor,
                "phase7_quality_event_window_overlay_v1",
                100,
                180,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(6, 2),
                90,
                1800,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_mixed_orthogonal_risk_memory_router_v3",
                "listed_non_st",
                "pit_alpha_first_cl_top100_rebalance180",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_execution_pit_excess_return_recovery_seed_trials() -> Vec<Value> {
    let Some(event_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let high_sharpe_anchors = professional_return_distribution_repair_seed_trials();
    let high_sharpe_anchor = high_sharpe_anchors.first().cloned();
    let mut seeds = Vec::new();

    for (
        seed,
        profile_name,
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
        execution_impact_budget,
    ) in [
        (
            event_anchor.clone(),
            "pit_excess_event_top80_rebalance120",
            "phase7_financial_quality_v1",
            80usize,
            120usize,
            "0.10",
            "1",
            "1.25",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "twap_15d_v1",
            Decimal::new(10, 2),
            60usize,
            2200usize,
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            "listed_non_st",
            "impact_turnover_20pct_v1",
        ),
        (
            event_anchor,
            "pit_excess_event_top100_rebalance160",
            "phase7_quality_event_window_overlay_v1",
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.5",
            "capacity_stress_participation_blended_alpha_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(8, 2),
            75usize,
            2400usize,
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "listed_non_st",
            "impact_turnover_20pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_pit_excess_return_recovery_seed(
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
                execution_impact_budget,
                profile_name,
            )],
        );
    }

    if let Some(high_sharpe_anchor) = high_sharpe_anchor {
        append_unique_seeds(
            &mut seeds,
            vec![with_pit_excess_return_recovery_seed(
                high_sharpe_anchor,
                "phase7_financial_quality_v1",
                120,
                160,
                "0.07",
                "0.95",
                "1.5",
                "capacity_stress_participation_alpha_headroom_floor_60_v1",
                "twap_20d_v1",
                Decimal::new(8, 2),
                75,
                2400,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "listed_non_st",
                "impact_turnover_15pct_v1",
                "pit_excess_cl_top120_rebalance160",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_execution_pit_nonlinear_alpha_regime_rebuild_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();

    if let Some(high_sharpe_anchor) = phase7_high_sharpe_boundary_base_seed() {
        for (
            profile_name,
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
            candidate_ranking,
            execution_impact_budget,
        ) in [
            (
                "pit_nonlinear_event_overlay_softcap_top100_rebalance180",
                "phase7_quality_event_window_overlay_v1",
                100usize,
                180usize,
                "0.06",
                "0.95",
                "2.0",
                "capacity_stress_participation_soft_cap_v1",
                "twap_20d_v1",
                Decimal::new(6, 2),
                75usize,
                2200usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_state_alpha_overlay_selector_v1",
                "alpha_first_low_impact_v1",
                "impact_turnover_15pct_v1",
            ),
            (
                "pit_nonlinear_quality_v14_top80_rebalance120",
                "phase7_financial_quality_v1",
                80usize,
                120usize,
                "0.10",
                "1",
                "1.25",
                "capacity_stress_participation_soft_cap_v1",
                "twap_15d_v1",
                Decimal::new(10, 2),
                60usize,
                2200usize,
                "soft_low_volatility_v1",
                "soft_single_name_20pct_v1",
                "quality_mixed_state_risk_memory_router_v14",
                "alpha_first_low_impact_v1",
                "impact_turnover_20pct_v1",
            ),
            (
                "pit_nonlinear_value_recovery_top100_rebalance160",
                "phase7_quality_value_recovery_confirm_v1",
                100usize,
                160usize,
                "0.08",
                "0.95",
                "1.50",
                "capacity_participation_balanced_v1",
                "twap_15d_v1",
                Decimal::new(8, 2),
                75usize,
                2200usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "capacity_aware_alpha_liquidity_v1",
                "impact_turnover_20pct_v1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_pit_nonlinear_alpha_regime_rebuild_seed(
                    high_sharpe_anchor.clone(),
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
                )],
            );
        }
    }

    if let Some(event_anchor) = phase7_current_event_window_15pct_risk_budget_180_anchor_seed() {
        for (
            profile_name,
            combo_name,
            top_n,
            rebalance_days,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            market_regime,
            candidate_ranking,
        ) in [
            (
                "pit_nonlinear_event_anchor_top80_alpha_headroom",
                "phase7_quality_event_window_overlay_v1",
                80usize,
                160usize,
                "0.08",
                "0.95",
                "1.50",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
                "alpha_first_low_impact_v1",
            ),
            (
                "pit_nonlinear_event_anchor_top120_capacity_rank",
                "phase7_financial_quality_v1",
                120usize,
                180usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "quality_mixed_orthogonal_risk_memory_router_v3",
                "capacity_aware_alpha_liquidity_v1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_pit_nonlinear_alpha_regime_rebuild_seed(
                    event_anchor.clone(),
                    combo_name,
                    top_n,
                    rebalance_days,
                    max_position_pct,
                    max_gross_exposure,
                    capacity_penalty_strength,
                    capacity_risk_budget,
                    "twap_20d_v1",
                    Decimal::new(8, 2),
                    90,
                    2400,
                    "soft_liquidity_low_volatility_low_correlation_v1",
                    "soft_single_name_20pct_v1",
                    market_regime,
                    "listed_non_st",
                    candidate_ranking,
                    "impact_turnover_15pct_v1",
                    profile_name,
                )],
            );
        }
    }

    for seed in professional_return_distribution_repair_seed_trials()
        .into_iter()
        .take(2)
    {
        append_unique_seeds(
            &mut seeds,
            vec![with_pit_nonlinear_alpha_regime_rebuild_seed(
                seed,
                "phase7_financial_quality_v1",
                120,
                160,
                "0.07",
                "0.95",
                "1.50",
                "capacity_stress_participation_alpha_headroom_floor_60_v1",
                "twap_20d_v1",
                Decimal::new(8, 2),
                75,
                2400,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "listed_non_st",
                "alpha_first_low_impact_v1",
                "impact_turnover_15pct_v1",
                "pit_nonlinear_cl_cm_alpha_headroom_top120",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_train_window_nonlinear_ranking_discovery_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();

    let anchors = [
        phase7_high_sharpe_boundary_base_seed(),
        phase7_current_event_window_15pct_risk_budget_180_anchor_seed(),
        phase7_current_anchor_bg_trial4_seed(),
    ];

    for anchor in anchors.into_iter().flatten() {
        for (
            profile_name,
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
        ) in [
            (
                "native_quality_low_impact_rebalance180_fill70",
                "phase7_financial_quality_v1",
                ScoreDirection::Ascending,
                100usize,
                180usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "stress_fill_gross_98_v1",
                "twap_20d_v1",
                Decimal::new(6, 2),
                90usize,
                2400usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "impact_turnover_15pct_v1",
            ),
            (
                "native_value_recovery_rebalance220_fill70",
                "phase7_quality_value_recovery_confirm_v1",
                ScoreDirection::Descending,
                100usize,
                220usize,
                "0.06",
                "0.90",
                "2.5",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "stress_fill_gross_98_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                110usize,
                2400usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_mixed_orthogonal_risk_memory_router_v3",
                "impact_turnover_15pct_v1",
            ),
            (
                "native_growth_recovery_rebalance180_fill60",
                "phase7_growth_recovery_v1",
                ScoreDirection::Descending,
                80usize,
                180usize,
                "0.08",
                "0.95",
                "1.5",
                "capacity_stress_participation_alpha_headroom_floor_60_v1",
                "fillable_gross_95_v1",
                "twap_15d_v1",
                Decimal::new(8, 2),
                90usize,
                2200usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_mixed_state_risk_memory_router_v14",
                "impact_turnover_20pct_v1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_train_window_nonlinear_ranking_seed(
                    anchor.clone(),
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
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_execution_pit_quality_recovery_alpha_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();

    if let Some(anchor) = phase7_high_sharpe_boundary_base_seed() {
        for (
            profile_name,
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
            candidate_ranking,
            execution_impact_budget,
        ) in [
            (
                "pit_quality_recovery_top100_rebalance160",
                100usize,
                160usize,
                "0.08",
                "0.95",
                "1.50",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_15d_v1",
                Decimal::new(8, 2),
                75usize,
                2200usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "alpha_first_low_impact_v1",
                "impact_turnover_15pct_v1",
            ),
            (
                "pit_quality_recovery_top80_rebalance120",
                80usize,
                120usize,
                "0.10",
                "1",
                "1.25",
                "capacity_stress_participation_soft_cap_v1",
                "twap_15d_v1",
                Decimal::new(10, 2),
                60usize,
                1800usize,
                "soft_low_volatility_v1",
                "soft_single_name_20pct_v1",
                "quality_mixed_state_risk_memory_router_v14",
                "alpha_first_low_impact_v1",
                "impact_turnover_20pct_v1",
            ),
            (
                "pit_quality_recovery_top120_capacity_rank",
                120usize,
                180usize,
                "0.06",
                "0.90",
                "2.00",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(6, 2),
                90usize,
                2400usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "capacity_aware_alpha_liquidity_v1",
                "impact_turnover_15pct_v1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_pit_quality_recovery_alpha_seed(
                    anchor.clone(),
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
                )],
            );
        }
    }

    if let Some(event_anchor) = phase7_current_event_window_15pct_risk_budget_180_anchor_seed() {
        append_unique_seeds(
            &mut seeds,
            vec![with_pit_quality_recovery_alpha_seed(
                event_anchor,
                100,
                160,
                "0.08",
                "0.95",
                "1.50",
                "capacity_stress_participation_soft_cap_v1",
                "twap_20d_v1",
                Decimal::new(8, 2),
                75,
                2200,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_state_alpha_overlay_selector_v1",
                "listed_non_st",
                "capacity_aware_alpha_liquidity_v1",
                "impact_turnover_15pct_v1",
                "pit_quality_recovery_event_anchor_top100",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_execution_event_post_return_curve_alpha_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();

    if let Some(anchor) = phase7_high_sharpe_boundary_base_seed() {
        for (
            profile_name,
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
            candidate_ranking,
            execution_impact_budget,
        ) in [
            (
                "event_post_return_curve_top100_rebalance160",
                100usize,
                160usize,
                "0.08",
                "0.95",
                "1.50",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_15d_v1",
                Decimal::new(8, 2),
                75usize,
                2200usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_event_window_return_sharpe_router_v4",
                "alpha_first_low_impact_v1",
                "impact_turnover_15pct_v1",
            ),
            (
                "event_post_return_curve_top80_rebalance120",
                80usize,
                120usize,
                "0.10",
                "1",
                "1.25",
                "capacity_stress_participation_soft_cap_v1",
                "twap_15d_v1",
                Decimal::new(10, 2),
                60usize,
                1800usize,
                "soft_low_volatility_v1",
                "soft_single_name_20pct_v1",
                "quality_event_window_return_sharpe_router_v3",
                "alpha_first_low_impact_v1",
                "impact_turnover_20pct_v1",
            ),
            (
                "event_post_return_curve_capacity_rank_top120",
                120usize,
                180usize,
                "0.06",
                "0.90",
                "2.00",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(6, 2),
                90usize,
                2400usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_event_window_return_sharpe_router_v4",
                "capacity_aware_alpha_liquidity_v1",
                "impact_turnover_15pct_v1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_event_post_return_curve_alpha_seed(
                    anchor.clone(),
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
                )],
            );
        }
    }

    if let Some(event_anchor) = phase7_current_event_window_15pct_risk_budget_180_anchor_seed() {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_post_return_curve_alpha_seed(
                event_anchor,
                100,
                180,
                "0.08",
                "0.95",
                "1.50",
                "capacity_stress_participation_soft_cap_v1",
                "twap_20d_v1",
                Decimal::new(8, 2),
                90,
                2200,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
                "listed_non_st",
                "capacity_aware_alpha_liquidity_v1",
                "impact_turnover_15pct_v1",
                "event_post_return_curve_event_anchor_top100",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_execution_event_reaction_alpha_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();

    if let Some(anchor) = phase7_high_sharpe_boundary_base_seed() {
        for (combo_name, profile_prefix) in [
            (
                "phase7_quality_event_reaction_segments_overlay_v1",
                "event_reaction_segments",
            ),
            (
                "phase7_quality_event_reaction_reversal_overlay_v1",
                "event_reaction_reversal",
            ),
        ] {
            for (
                profile_suffix,
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
                candidate_ranking,
                execution_impact_budget,
            ) in [
                (
                    "top100_rebalance160",
                    100usize,
                    160usize,
                    "0.08",
                    "0.95",
                    "1.50",
                    "capacity_stress_participation_alpha_headroom_floor_70_v1",
                    "twap_15d_v1",
                    Decimal::new(8, 2),
                    75usize,
                    2200usize,
                    "soft_liquidity_low_volatility_low_correlation_v1",
                    "soft_single_name_20pct_v1",
                    "quality_event_window_return_sharpe_router_v4",
                    "alpha_first_low_impact_v1",
                    "impact_turnover_15pct_v1",
                ),
                (
                    "top80_rebalance120",
                    80usize,
                    120usize,
                    "0.10",
                    "1",
                    "1.25",
                    "capacity_stress_participation_soft_cap_v1",
                    "twap_15d_v1",
                    Decimal::new(10, 2),
                    60usize,
                    1800usize,
                    "soft_low_volatility_v1",
                    "soft_single_name_20pct_v1",
                    "quality_event_window_return_sharpe_router_v3",
                    "alpha_first_low_impact_v1",
                    "impact_turnover_20pct_v1",
                ),
                (
                    "capacity_rank_top120",
                    120usize,
                    180usize,
                    "0.06",
                    "0.90",
                    "2.00",
                    "capacity_stress_participation_alpha_headroom_floor_70_v1",
                    "twap_20d_v1",
                    Decimal::new(6, 2),
                    90usize,
                    2400usize,
                    "soft_low_volatility_low_correlation_v1",
                    "soft_single_name_15pct_v1",
                    "quality_event_window_return_sharpe_router_v4",
                    "capacity_aware_alpha_liquidity_v1",
                    "impact_turnover_15pct_v1",
                ),
            ] {
                append_unique_seeds(
                    &mut seeds,
                    vec![with_event_reaction_alpha_seed(
                        anchor.clone(),
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
                        &format!("{profile_prefix}_{profile_suffix}"),
                    )],
                );
            }
        }
    }

    seeds
}

pub(crate) fn professional_execution_broad_financial_feature_discovery_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        profile_name,
        combo_name,
        score_direction,
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
        candidate_ranking,
        execution_impact_budget,
    ) in [
        (
            "broad_ff_dual_confirm_ascending_top100",
            "phase7_quality_cashflow_dividend_confirm_v1",
            ScoreDirection::Ascending,
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.50",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "impact_turnover_15pct_v1",
        ),
        (
            "broad_ff_cashflow_confirm_ascending_top100",
            "phase7_quality_cashflow_confirm_v1",
            ScoreDirection::Ascending,
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.50",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "impact_turnover_15pct_v1",
        ),
        (
            "broad_ff_dividend_confirm_ascending_top100",
            "phase7_quality_dividend_confirm_v1",
            ScoreDirection::Ascending,
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.50",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "impact_turnover_15pct_v1",
        ),
        (
            "broad_ff_financial_quality_ascending_top100",
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.50",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_mixed_state_risk_memory_router_v14",
            "alpha_first_low_impact_v1",
            "impact_turnover_15pct_v1",
        ),
        (
            "broad_ff_industry_residual_ascending_top120",
            "phase7_industry_residual_quality_v1",
            ScoreDirection::Ascending,
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            90usize,
            2400usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
            "impact_turnover_15pct_v1",
        ),
        (
            "broad_ff_growth_recovery_descending_top100",
            "phase7_growth_recovery_v1",
            ScoreDirection::Descending,
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.50",
            "capacity_participation_balanced_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
            "impact_turnover_20pct_v1",
        ),
        (
            "broad_ff_relative_strength_descending_top80",
            "phase7_quality_relative_strength_v1",
            ScoreDirection::Descending,
            80usize,
            120usize,
            "0.10",
            "1",
            "1.25",
            "capacity_stress_participation_soft_cap_v1",
            "twap_15d_v1",
            Decimal::new(10, 2),
            60usize,
            1800usize,
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            "quality_state_alpha_overlay_selector_v1",
            "alpha_first_low_impact_v1",
            "impact_turnover_20pct_v1",
        ),
        (
            "broad_ff_dividend_confirm_descending_top100",
            "phase7_quality_dividend_confirm_v1",
            ScoreDirection::Descending,
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.50",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "impact_turnover_15pct_v1",
        ),
        (
            "broad_ff_cashflow_confirm_descending_top100",
            "phase7_quality_cashflow_confirm_v1",
            ScoreDirection::Descending,
            100usize,
            160usize,
            "0.08",
            "0.95",
            "1.50",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            2200usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "impact_turnover_15pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_broad_financial_feature_discovery_seed(
                anchor.clone(),
                combo_name,
                score_direction,
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
                candidate_ranking,
                execution_impact_budget,
                profile_name,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_execution_broad_financial_feature_stratified_discovery_seed_trials() -> Vec<Value> {
    let priority_profiles = [
        "broad_ff_financial_quality_ascending_top100",
        "broad_ff_growth_recovery_descending_top100",
        "broad_ff_industry_residual_ascending_top120",
        "broad_ff_relative_strength_descending_top80",
        "broad_ff_dual_confirm_ascending_top100",
        "broad_ff_dividend_confirm_descending_top100",
        "broad_ff_cashflow_confirm_descending_top100",
        "broad_ff_cashflow_confirm_ascending_top100",
        "broad_ff_dividend_confirm_ascending_top100",
    ];
    let mut seeds = reorder_seed_trials_by_string_field(
        professional_execution_broad_financial_feature_discovery_seed_trials(),
        "broad_financial_feature_discovery_profile",
        &priority_profiles,
    );
    for seed in &mut seeds {
        seed["broad_financial_feature_sampling"] = json!("stratified_seed_v1");
    }
    seeds
}

pub(crate) fn tag_native_alpha_fusion_seed(
    mut seed: Value,
    native_profile: &str,
    alpha_source_family: &str,
) -> Value {
    seed["native_alpha_fusion_profile"] = json!(native_profile);
    seed["alpha_source_family"] = json!(alpha_source_family);
    seed
}

pub(crate) fn professional_execution_native_alpha_fusion_discovery_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();

    let broad_financial_seeds =
        professional_execution_broad_financial_feature_stratified_discovery_seed_trials()
            .into_iter()
            .take(4)
            .map(|seed| {
                tag_native_alpha_fusion_seed(seed, "broad_financial_core", "broad_financial")
            })
            .collect::<Vec<_>>();

    let event_surprise_seeds = phase7_high_sharpe_boundary_base_seed()
        .map(|anchor| {
            let seed = with_event_surprise_gate_seed(
                with_broad_financial_feature_discovery_seed(
                    anchor,
                    "phase7_financial_quality_v1",
                    ScoreDirection::Ascending,
                    100,
                    160,
                    "0.08",
                    "0.95",
                    "1.50",
                    "capacity_stress_participation_alpha_headroom_floor_70_v1",
                    "twap_15d_v1",
                    Decimal::new(8, 2),
                    75,
                    2200,
                    "soft_liquidity_low_volatility_low_correlation_v1",
                    "soft_single_name_20pct_v1",
                    "quality_state_alpha_overlay_selector_v1",
                    "alpha_first_low_impact_v1",
                    "impact_turnover_15pct_v1",
                    "event_surprise_gate_top100",
                ),
                "event_surprise_exclude_negative",
                "exclude_negative",
                "0",
            );
            tag_native_alpha_fusion_seed(seed, "event_surprise_gate", "event_surprise")
        })
        .into_iter()
        .collect::<Vec<_>>();

    let event_reaction_seeds = professional_execution_event_reaction_alpha_seed_trials()
        .into_iter()
        .map(|seed| {
            let combo_name = seed
                .get("combo_name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let native_profile =
                if combo_name == "phase7_quality_event_reaction_reversal_overlay_v1" {
                    "event_reaction_reversal"
                } else {
                    "event_reaction_segments"
                };
            tag_native_alpha_fusion_seed(seed, native_profile, "event_reaction")
        })
        .collect::<Vec<_>>();

    let prediction_confirmation_seeds =
        professional_native_alpha_fusion_prediction_confirmation_seed_trials()
            .into_iter()
            .take(4)
            .map(|seed| {
                tag_native_alpha_fusion_seed(
                    seed,
                    "prediction_confirmation",
                    "prediction_confirmation",
                )
            })
            .collect::<Vec<_>>();

    for source_group in [
        &broad_financial_seeds,
        &event_surprise_seeds,
        &event_reaction_seeds,
        &prediction_confirmation_seeds,
    ] {
        if let Some(seed) = source_group.first() {
            append_unique_seeds(&mut seeds, vec![seed.clone()]);
        }
    }

    for source_group in [
        &broad_financial_seeds,
        &event_surprise_seeds,
        &event_reaction_seeds,
        &prediction_confirmation_seeds,
    ] {
        append_unique_seeds(&mut seeds, source_group.iter().skip(1).cloned().collect());
    }

    seeds
}

pub(crate) fn professional_trainable_alpha_admission_discovery_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();

    let broad_financial_seeds =
        professional_execution_broad_financial_feature_stratified_discovery_seed_trials()
            .into_iter()
            .take(4)
            .map(|seed| {
                tag_trainable_alpha_admission_seed(
                    seed,
                    "broad_financial_stratified_core",
                    "broad_financial",
                )
            })
            .collect::<Vec<_>>();
    append_unique_seeds(&mut seeds, broad_financial_seeds);

    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return seeds;
    };
    for (profile, combo_name, score_direction, top_n, family, market_regime, candidate_ranking) in [
        (
            "residual_confirm_10pct_ascending_top100",
            "phase7_quality_residual_confirm_10pct_v1",
            ScoreDirection::Ascending,
            100usize,
            "residual_confirm",
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "value_recovery_descending_top100",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Descending,
            100usize,
            "value_recovery",
            "quality_state_alpha_overlay_selector_v1",
            "alpha_first_low_impact_v1",
        ),
        (
            "financial_quality_change_acceleration",
            "phase7_financial_quality_change_v1",
            ScoreDirection::Descending,
            100usize,
            "financial_quality_change",
            "quality_mixed_state_risk_memory_router_v14",
            "alpha_first_low_impact_v1",
        ),
        (
            "earnings_recovery_persistence",
            "phase7_earnings_recovery_persistence_v1",
            ScoreDirection::Descending,
            100usize,
            "earnings_recovery_persistence",
            "quality_mixed_state_risk_memory_router_v14",
            "alpha_first_low_impact_v1",
        ),
        (
            "residual_confirm_5pct_ascending_top120",
            "phase7_quality_residual_confirm_5pct_v1",
            ScoreDirection::Ascending,
            120usize,
            "residual_confirm",
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
        ),
        (
            "quality_moneyflow_pos_ascending_top100",
            "phase7_quality_moneyflow_pos_5pct_v1",
            ScoreDirection::Ascending,
            100usize,
            "moneyflow_quality",
            "quality_mixed_state_risk_memory_router_v14",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "moneyflow_congestion",
            "phase7_moneyflow_congestion_interaction_v1",
            ScoreDirection::Descending,
            100usize,
            "moneyflow_congestion",
            "quality_mixed_state_risk_memory_router_v14",
            "capacity_aware_alpha_liquidity_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![tag_trainable_alpha_admission_seed(
                with_broad_financial_feature_discovery_seed(
                    anchor.clone(),
                    combo_name,
                    score_direction,
                    top_n,
                    160,
                    "0.08",
                    "0.95",
                    "1.50",
                    "capacity_stress_participation_alpha_headroom_floor_70_v1",
                    "twap_15d_v1",
                    Decimal::new(8, 2),
                    75,
                    2200,
                    "soft_liquidity_low_volatility_low_correlation_v1",
                    "soft_single_name_20pct_v1",
                    market_regime,
                    candidate_ranking,
                    "impact_turnover_15pct_v1",
                    profile,
                ),
                profile,
                family,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_train_window_stress_fill_target_exposure_seed_trials() -> Vec<Value> {
    let anchors = [
        phase7_high_sharpe_boundary_base_seed(),
        phase7_current_event_window_15pct_risk_budget_180_anchor_seed(),
        phase7_current_anchor_bg_trial4_seed(),
    ];
    let mut seeds = Vec::new();

    for anchor in anchors.into_iter().flatten() {
        for (
            profile_name,
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
        ) in [
            (
                "native_quality_stress_fill_floor70_rebalance240",
                "phase7_financial_quality_v1",
                ScoreDirection::Ascending,
                120usize,
                240usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "stress_fill_gross_98_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "impact_turnover_15pct_v1",
            ),
            (
                "native_value_stress_fill_floor70_rebalance300",
                "phase7_quality_value_recovery_confirm_v1",
                ScoreDirection::Ascending,
                120usize,
                300usize,
                "0.06",
                "0.90",
                "2.5",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "stress_fill_gross_98_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                160usize,
                2600usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_mixed_orthogonal_risk_memory_router_v3",
                "impact_turnover_15pct_v1",
            ),
            (
                "native_growth_stress_fill_floor60_rebalance240",
                "phase7_growth_recovery_v1",
                ScoreDirection::Descending,
                100usize,
                240usize,
                "0.08",
                "0.90",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_60_v1",
                "stress_fill_gross_98_v1",
                "twap_20d_v1",
                Decimal::new(6, 2),
                140usize,
                2400usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_mixed_state_risk_memory_router_v14",
                "impact_turnover_15pct_v1",
            ),
            (
                "native_residual_stress_fill_blended_floor70_rebalance300",
                "phase7_quality_residual_confirm_10pct_v1",
                ScoreDirection::Ascending,
                140usize,
                300usize,
                "0.05",
                "0.90",
                "2.5",
                "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
                "stress_fill_gross_98_v1",
                "twap_20d_v1",
                Decimal::new(4, 2),
                180usize,
                2800usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_mixed_orthogonal_risk_memory_router_v3",
                "impact_turnover_15pct_v1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_train_window_stress_fill_target_exposure_seed(
                    anchor.clone(),
                    profile_name,
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
                )],
            );
        }
    }

    seeds
}

#[allow(dead_code)]
pub(crate) fn professional_ensemble_discovery_seed_trials() -> Vec<Value> {
    // Single seed: the 4-Model Ensemble with PIT routing.
    // The WFA executor detects this profile and runs multi-model training + routing.
    vec![json!({
        "profile": "ensemble_default",
        "label_objectives": [
            "asymmetric_excess_return",
            "quality_adjusted_excess_return",
            "quality_adjusted_risk_adjusted_excess_return",
            "future_return"
        ],
        "label_horizon_days": [60, 60, 60, 5],
        "model_max_pct": {"asym_bull": 0.07, "bear_q": 0.04, "bear_def": 0.07, "mr": 0.10},
        "pit_routing": {
            "mr_trigger": "prev_vol > 0.20",
            "bearq_trigger": "trailing_return_42d < -0.02",
            "deep_bear_trigger": "north_flow_zscore < -1.0",
            "default": "ew3_asym"
        },
        "top_n": 20,
        "rebalance_days": 20,
        "feature_profile": "phase7_gb_quality_value_recovery_low_impact_v5",
        "bucket_count": 10,
        "min_samples_per_bucket": 100
    })]
}

pub(crate) fn professional_train_window_ml_stress_fill_discovery_seed_trials() -> Vec<Value> {
    let anchors = [
        phase7_high_sharpe_boundary_base_seed(),
        phase7_current_event_window_15pct_risk_budget_180_anchor_seed(),
        phase7_current_anchor_bg_trial4_seed(),
    ];
    let mut seeds = Vec::new();

    for anchor in anchors.into_iter().flatten() {
        for (
            profile_name,
            ml_profile,
            label_horizon_days,
            bucket_count,
            combo_name,
            score_direction,
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
            min_prediction_score,
        ) in [
            (
                "gb_quality_rae45_fill95_floor70_rebalance240",
                "nlq_ranker_rae_h45_bucket7_fill95",
                45usize,
                7usize,
                "phase7_financial_quality_v1",
                ScoreDirection::Ascending,
                120usize,
                240usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "0.00",
            ),
            (
                "gb_value_recovery_rae45_fill95_floor70_rebalance300",
                "nlq_ranker_rae_h45_bucket7_fill95",
                45usize,
                7usize,
                "phase7_quality_value_recovery_confirm_v1",
                ScoreDirection::Ascending,
                120usize,
                300usize,
                "0.06",
                "0.90",
                "2.5",
                "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                160usize,
                2600usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_mixed_orthogonal_risk_memory_router_v3",
                "0.00",
            ),
            (
                "gb_growth_rae30_fill95_floor70_rebalance240",
                "nlq_ranker_rae_h30_bucket5_fill95",
                30usize,
                5usize,
                "phase7_growth_recovery_v1",
                ScoreDirection::Descending,
                100usize,
                240usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2400usize,
                "soft_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_mixed_state_risk_memory_router_v14",
                "0.01",
            ),
            (
                "gb_quality_rae60_bucket15_fill95_floor70_rebalance240",
                "nlq_ranker_rae_h60_bucket15_fill95",
                60usize,
                15usize,
                "phase7_financial_quality_v1",
                ScoreDirection::Ascending,
                120usize,
                240usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "0.00",
            ),
            (
                "gb_quality_rae45_fill95_floor85_rebalance240",
                "nlq_ranker_rae_h45_bucket7_fill95",
                45usize,
                7usize,
                "phase7_financial_quality_v1",
                ScoreDirection::Ascending,
                120usize,
                240usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_85_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "0.00",
            ),
            // === Regime-adaptive label seeds (方案4): future_return for growth ===
            (
                "gb_growth_desc_top40_future_return_rebalance240",
                "nlq_ranker_rae_h45_bucket7_fill95",
                45usize,
                7usize,
                "phase7_growth_recovery_v1",
                ScoreDirection::Descending,
                40usize,
                240usize,
                "0.05",
                "0.95",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "0.00",
            ),
            (
                "gb_growth_desc_top60_future_return_rebalance240",
                "nlq_ranker_rae_h45_bucket7_fill95",
                45usize,
                7usize,
                "phase7_growth_recovery_v1",
                ScoreDirection::Descending,
                60usize,
                240usize,
                "0.05",
                "0.95",
                "2.0",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "0.00",
            ),
            // === Concentrated quality seeds ===
            (
                "gb_quality_asc_top40_fill95_floor70_rebalance240",
                "nlq_ranker_rae_h45_bucket7_fill95",
                45usize,
                7usize,
                "phase7_financial_quality_v1",
                ScoreDirection::Ascending,
                40usize,
                240usize,
                "0.05",
                "0.95",
                "2.0",
                "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_20pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "0.00",
            ),
            (
                "gb_quality_rae45_bucket10_fill95_floor70_rebalance240",
                "nlq_ranker_rae_h45_bucket10_fill95",
                45usize,
                10usize,
                "phase7_financial_quality_v1",
                ScoreDirection::Ascending,
                120usize,
                240usize,
                "0.06",
                "0.90",
                "2.0",
                "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
                "twap_20d_v1",
                Decimal::new(5, 2),
                140usize,
                2600usize,
                "soft_liquidity_low_volatility_low_correlation_v1",
                "soft_single_name_15pct_v1",
                "quality_nonlinear_alpha_risk_memory_router_v3",
                "0.00",
            ),
        ] {
            let seed = with_train_window_stress_fill_target_exposure_seed(
                anchor.clone(),
                profile_name,
                combo_name,
                score_direction,
                top_n,
                rebalance_days,
                max_position_pct,
                max_gross_exposure,
                capacity_penalty_strength,
                capacity_risk_budget,
                "stress_fill_gross_98_v1",
                execution_schedule_profile,
                daily_target_move_limit_pct,
                max_carry_days,
                score_candidate_pool_size,
                candidate_risk_filter,
                risk_contribution_control,
                market_regime,
                "impact_turnover_15pct_v1",
            );
            append_unique_seeds(
                &mut seeds,
                vec![with_train_window_ml_stress_fill_seed(
                    seed,
                    profile_name,
                    ml_profile,
                    label_horizon_days,
                    bucket_count,
                    min_prediction_score,
                    if profile_name.contains("future_return") {
                        "future_excess_return"
                    } else {
                        "regime_conditional_excess_return"
                    },
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_current_event_nonlinear_alpha_discovery_seed_trials() -> Vec<Value> {
    let anchors = professional_return_distribution_repair_seed_trials();
    let nonlinear_anchor = anchors
        .iter()
        .find(|seed| seed["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3")
        .cloned()
        .or_else(|| anchors.first().cloned());
    let orthogonal_anchor = anchors
        .iter()
        .find(|seed| seed["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3")
        .cloned()
        .or_else(|| anchors.first().cloned());
    let Some(nonlinear_anchor) = nonlinear_anchor else {
        return Vec::new();
    };
    let orthogonal_anchor = orthogonal_anchor.unwrap_or_else(|| nonlinear_anchor.clone());
    let event_router_anchor = with_market_regime_seed(
        nonlinear_anchor.clone(),
        "quality_event_window_return_sharpe_router_v4",
    );
    let mut seeds = Vec::new();

    for (base, profile_name, min_score, boost_weight) in [
        (
            nonlinear_anchor.clone(),
            "cm_event_surprise_boost_p75",
            "0.35",
            "0.03",
        ),
        (
            nonlinear_anchor.clone(),
            "cm_event_surprise_boost_p90",
            "0.43",
            "0.05",
        ),
        (
            orthogonal_anchor.clone(),
            "cl_event_surprise_boost_p75",
            "0.35",
            "0.03",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_current_event_nonlinear_alpha_seed(
                with_event_combo_gate_seed_with_min_score(
                    base,
                    profile_name,
                    "phase7_event_surprise_v1",
                    "boost_positive",
                    min_score,
                    boost_weight,
                    ScoreDirection::Descending,
                ),
                profile_name,
                "event_surprise",
            )],
        );
    }

    append_unique_seeds(
        &mut seeds,
        vec![with_current_event_nonlinear_alpha_seed(
            with_event_combo_gate_seed_with_min_score(
                orthogonal_anchor.clone(),
                "cl_event_surprise_exclude_negative",
                "phase7_event_surprise_v1",
                "exclude_negative",
                "0",
                "0",
                ScoreDirection::Descending,
            ),
            "cl_event_surprise_exclude_negative",
            "event_surprise",
        )],
    );

    for (base, profile_name, combo_name, mode, min_score, boost_weight) in [
        (
            event_router_anchor.clone(),
            "cm_event_window_20d_boost_p75",
            "phase7_event_window_earnings_v1",
            "boost_positive",
            "0.38",
            "0.03",
        ),
        (
            orthogonal_anchor.clone(),
            "cl_event_window_40d_exclude_negative",
            "phase7_event_window_earnings_40d_v1",
            "exclude_negative",
            "0",
            "0",
        ),
        (
            nonlinear_anchor.clone(),
            "cm_event_window_40d_boost_p75",
            "phase7_event_window_earnings_40d_v1",
            "boost_positive",
            "0.38",
            "0.03",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_current_event_nonlinear_alpha_seed(
                with_event_combo_gate_seed_with_min_score(
                    base,
                    profile_name,
                    combo_name,
                    mode,
                    min_score,
                    boost_weight,
                    ScoreDirection::Descending,
                ),
                profile_name,
                "event_window",
            )],
        );
    }

    for (base, profile_name) in [
        (orthogonal_anchor, "cl_residual_confirm_router"),
        (nonlinear_anchor, "cm_residual_confirm_router"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_current_event_nonlinear_alpha_seed(
                set_seed_score_direction(
                    retarget_seed_combo(base, "phase7_quality_residual_confirm_10pct_v1"),
                    ScoreDirection::Descending,
                ),
                profile_name,
                "residual_confirm",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_execution_bull_sleeve_cash_recovery_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_event_window_sleeve_upper_bound_seed_trials()
        .into_iter()
        .take(5)
    {
        let mut seed = with_candidate_ranking_seed(seed, "capacity_aware_alpha_liquidity_v1");
        seed = with_candidate_risk_filter_seed(seed, "off");
        seed = with_risk_contribution_control_seed(seed, "off");
        seed = with_cash_utilization_seed(seed, "off");
        seed = with_execution_impact_budget_seed(seed, "off");
        seed["bull_sleeve_cash_recovery_profile"] =
            json!("nearest_candidate_return_first_execution_gated");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    let anchors = professional_execution_alpha_capacity_return_frontier_seed_trials();
    let Some(ec_anchor) = anchors.first().cloned() else {
        return seeds;
    };

    for (
        combo_name,
        score_direction,
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
        market_regime,
        candidate_risk_filter,
        universe_profile,
        profile_name,
    ) in [
        (
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            80usize,
            120usize,
            "0.08",
            "1",
            "1.25",
            "capacity_participation_balanced_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            45usize,
            1800usize,
            "quality_state_alpha_selector_v1",
            "soft_low_volatility_v1",
            "all",
            "bull_sleeve_quality_top80_rebalance120",
        ),
        (
            "phase7_quality_event_window_overlay_v1",
            ScoreDirection::Ascending,
            100usize,
            160usize,
            "0.07",
            "0.95",
            "1.5",
            "capacity_participation_balanced_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            60usize,
            2200usize,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "soft_low_volatility_low_correlation_v1",
            "listed_non_st",
            "bull_sleeve_event_overlay_top100_rebalance160",
        ),
        (
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Descending,
            80usize,
            120usize,
            "0.08",
            "1",
            "1.25",
            "capacity_participation_balanced_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            45usize,
            1800usize,
            "quality_state_alpha_selector_v2",
            "soft_low_volatility_v1",
            "all",
            "bull_sleeve_value_recovery_top80_rebalance120",
        ),
        (
            "phase7_growth_recovery_v1",
            ScoreDirection::Descending,
            100usize,
            160usize,
            "0.07",
            "0.95",
            "1.5",
            "capacity_participation_strict_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            60usize,
            2200usize,
            "quality_frontier_regime_bridge_router_v2",
            "soft_low_volatility_low_correlation_v1",
            "listed_non_st",
            "bull_sleeve_growth_recovery_top100_rebalance160",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_bull_sleeve_cash_recovery_seed(
                ec_anchor.clone(),
                combo_name,
                score_direction,
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
                market_regime,
                candidate_risk_filter,
                universe_profile,
                profile_name,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_execution_return_first_fill_repair_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for (anchor_index, seed) in professional_event_window_sleeve_upper_bound_seed_trials()
        .into_iter()
        .take(5)
        .enumerate()
    {
        for (
            variant_name,
            top_n,
            max_position_pct,
            max_pairwise_correlation,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            cash_utilization,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
            execution_impact_budget,
            score_candidate_pool_size,
        ) in [
            (
                "top60_stress_fill",
                60usize,
                "0.10",
                "0.75",
                "1",
                "0.75",
                "off",
                "stress_fill_gross_98_v1",
                "twap_10d_v1",
                Decimal::new(10, 2),
                30usize,
                "off",
                900usize,
            ),
            (
                "top80_balanced_fill",
                80usize,
                "0.08",
                "0.75",
                "1",
                "1.00",
                "capacity_participation_balanced_v1",
                "stress_fill_gross_98_v1",
                "twap_15d_v1",
                Decimal::new(75, 3),
                45usize,
                "impact_turnover_20pct_v1",
                1500usize,
            ),
        ] {
            let profile_name = format!(
                "return_first_fill_anchor{}_{}",
                anchor_index + 1,
                variant_name
            );
            append_unique_seeds(
                &mut seeds,
                vec![with_return_first_fill_repair_seed(
                    seed.clone(),
                    top_n,
                    max_position_pct,
                    max_pairwise_correlation,
                    max_gross_exposure,
                    capacity_penalty_strength,
                    capacity_risk_budget,
                    cash_utilization,
                    execution_schedule_profile,
                    daily_target_move_limit_pct,
                    max_carry_days,
                    execution_impact_budget,
                    score_candidate_pool_size,
                    &profile_name,
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_execution_oos_regime_alpha_rebuild_seed_trials() -> Vec<Value> {
    let anchors = professional_execution_bull_sleeve_cash_recovery_seed_trials();
    let Some(return_first_anchor) = anchors.first().cloned() else {
        return Vec::new();
    };
    let value_anchor = anchors.get(1).cloned();
    let event_anchor = anchors.get(4).cloned().or_else(|| anchors.get(3).cloned());

    let mut seeds = Vec::new();
    for (
        seed,
        combo_name,
        score_direction,
        market_regime,
        volatility_profile,
        volatility_target,
        volatility_min_exposure,
        drawdown_profile,
        drawdown_reduce_start,
        drawdown_reduce_full,
        drawdown_min_exposure,
        top_n,
        max_position_pct,
        max_pairwise_correlation,
        capacity_penalty_strength,
        profile_name,
    ) in [
        (
            return_first_anchor.clone(),
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            "quality_state_alpha_selector_v1",
            "vol120_22_65_100",
            "0.22",
            "0.65",
            "recover252_08_23_55_30_70",
            "0.08",
            "0.23",
            "0.55",
            30usize,
            "0.12",
            "0.75",
            "0.75",
            "oos_rebuild_quality_bull_top30_vol22",
        ),
        (
            value_anchor.unwrap_or_else(|| return_first_anchor.clone()),
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Descending,
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
            "vol120_22_65_100",
            "0.22",
            "0.65",
            "recover252_08_23_55_30_70",
            "0.08",
            "0.23",
            "0.55",
            40usize,
            "0.10",
            "0.75",
            "0.75",
            "oos_rebuild_value_recovery_top40_vol22",
        ),
        (
            event_anchor.unwrap_or_else(|| return_first_anchor.clone()),
            "phase7_quality_event_window_overlay_v1",
            ScoreDirection::Ascending,
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            "vol120_24_70_100",
            "0.24",
            "0.70",
            "recover252_08_24_55_30_70",
            "0.08",
            "0.24",
            "0.55",
            40usize,
            "0.10",
            "0.75",
            "1.00",
            "oos_rebuild_event_window_top40_vol24",
        ),
    ] {
        let mut seed =
            set_seed_score_direction(retarget_seed_combo(seed, combo_name), score_direction);
        seed = with_market_regime_seed(seed, market_regime);
        seed = with_volatility_profile_seed(
            seed,
            volatility_profile,
            volatility_target,
            120,
            volatility_min_exposure,
            "1",
        );
        seed = with_drawdown_profile_seed(
            seed,
            drawdown_profile,
            drawdown_reduce_start,
            drawdown_reduce_full,
            drawdown_min_exposure,
            252,
            "0.30",
            "0.70",
            "1",
        );
        seed = with_top_n_seed(seed, top_n, profile_name);
        seed = with_position_shape_seed(
            seed,
            profile_name,
            max_position_pct,
            max_pairwise_correlation,
        );
        seed = with_max_gross_exposure_seed(seed, "1", profile_name);
        seed["capacity_penalty_strength"] = json!(capacity_penalty_strength);
        seed["candidate_ranking"] = json!("capacity_aware_alpha_liquidity_v1");
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!("off");
        seed["cash_utilization"] = json!("off");
        seed["execution_impact_budget"] = json!("off");
        seed["oos_regime_alpha_rebuild_profile"] = json!(profile_name);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_execution_oos_benchmark_excess_rebuild_seed_trials() -> Vec<Value> {
    let anchors = professional_execution_oos_regime_alpha_rebuild_seed_trials();
    let Some(quality_anchor) = anchors.first().cloned() else {
        return Vec::new();
    };
    let value_anchor = anchors
        .get(1)
        .cloned()
        .unwrap_or_else(|| quality_anchor.clone());
    let event_anchor = anchors
        .get(2)
        .cloned()
        .unwrap_or_else(|| quality_anchor.clone());

    let mut seeds = Vec::new();
    for (
        seed,
        combo_name,
        score_direction,
        market_regime,
        volatility_profile,
        volatility_target,
        volatility_min_exposure,
        top_n,
        rebalance_days,
        max_position_pct,
        max_pairwise_correlation,
        capacity_penalty_strength,
        capacity_risk_budget,
        candidate_risk_filter,
        risk_contribution_control,
        universe_profile,
        score_candidate_pool_size,
        event_gate_profile,
        event_gate_min_score,
        profile_name,
    ) in [
        (
            event_anchor.clone(),
            "phase7_quality_event_window_overlay_v1",
            ScoreDirection::Descending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "vol120_24_70_100",
            "0.24",
            "0.70",
            100usize,
            120usize,
            "0.15",
            "0.75",
            "1.00",
            "capacity_participation_balanced_v1",
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            "listed_non_st",
            1800usize,
            "valuation_exclude_bottom45",
            "0.45",
            "oos_excess_event_nonlinear_top100_vol24",
        ),
        (
            event_anchor.clone(),
            "phase7_quality_event_window_overlay_v1",
            ScoreDirection::Descending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "vol120_26_75_100",
            "0.26",
            "0.75",
            80usize,
            90usize,
            "0.15",
            "0.75",
            "0.75",
            "capacity_participation_balanced_v1",
            "off",
            "off",
            "listed_non_st",
            1800usize,
            "valuation_exclude_bottom45",
            "0.45",
            "oos_excess_event_fill_recovery_top80_vol26",
        ),
        (
            value_anchor,
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Descending,
            "quality_state_alpha_selector_v1",
            "vol120_22_65_100",
            "0.22",
            "0.65",
            40usize,
            60usize,
            "0.10",
            "0.75",
            "0.75",
            "capacity_participation_balanced_v1",
            "off",
            "off",
            "all",
            1000usize,
            "valuation_exclude_bottom40",
            "0.40",
            "oos_excess_value_fill_top40_vol22",
        ),
        (
            quality_anchor,
            "phase7_quality_relative_strength_v1",
            ScoreDirection::Descending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "vol120_24_70_100",
            "0.24",
            "0.70",
            100usize,
            90usize,
            "0.15",
            "0.75",
            "1.00",
            "capacity_participation_balanced_v1",
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            "listed_non_st",
            1800usize,
            "valuation_exclude_bottom45",
            "0.45",
            "oos_excess_relative_strength_top100_vol24",
        ),
    ] {
        let mut seed =
            set_seed_score_direction(retarget_seed_combo(seed, combo_name), score_direction);
        seed = with_market_regime_seed(seed, market_regime);
        seed = with_volatility_profile_seed(
            seed,
            volatility_profile,
            volatility_target,
            120,
            volatility_min_exposure,
            "1",
        );
        seed = with_drawdown_profile_seed(
            seed,
            "recover252_10_24_50_30_70",
            "0.10",
            "0.24",
            "0.50",
            252,
            "0.30",
            "0.70",
            "1",
        );
        seed = with_top_n_seed(seed, top_n, profile_name);
        seed = with_rebalance_days_seed(seed, rebalance_days, profile_name);
        seed = with_position_shape_seed(
            seed,
            profile_name,
            max_position_pct,
            max_pairwise_correlation,
        );
        seed = with_max_gross_exposure_seed(seed, "1", profile_name);
        seed["capacity_penalty_strength"] = json!(capacity_penalty_strength);
        seed["capacity_risk_budget"] = json!(capacity_risk_budget);
        seed["candidate_ranking"] = json!("capacity_aware_alpha_liquidity_v1");
        seed["candidate_risk_filter"] = json!(candidate_risk_filter);
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["cash_utilization"] = json!("off");
        seed["execution_impact_budget"] = json!("off");
        seed["execution_schedule_profile"] = json!("immediate");
        seed["event_gate_profile"] = json!(event_gate_profile);
        seed["event_gate_min_score"] = json!(event_gate_min_score);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        seed["universe_profile"] = json!(universe_profile);
        seed["oos_benchmark_excess_rebuild_profile"] = json!(profile_name);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_execution_oos_execution_adaptive_rebuild_seed_trials() -> Vec<Value> {
    let anchors = professional_execution_oos_benchmark_excess_rebuild_seed_trials();
    let Some(event_anchor) = anchors.first().cloned() else {
        return Vec::new();
    };
    let fill_anchor = anchors
        .get(1)
        .cloned()
        .unwrap_or_else(|| event_anchor.clone());
    let value_anchor = anchors
        .get(2)
        .cloned()
        .unwrap_or_else(|| event_anchor.clone());

    let mut seeds = Vec::new();
    for (
        seed,
        sharpe_profile,
        sharpe_reduce_start,
        sharpe_reduce_full,
        sharpe_lookback_days,
        sharpe_min_exposure,
        capacity_risk_budget,
        cash_utilization,
        execution_impact_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        risk_contribution_control,
        profile_name,
    ) in [
        (
            event_anchor.clone(),
            "gentle_roll_sharpe180_025_neg20_70",
            "0.25",
            "-0.20",
            180usize,
            "0.70",
            "capacity_stress_participation_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "impact_turnover_20pct_v1",
            "twap_10d_v1",
            Decimal::new(8, 2),
            75usize,
            "soft_single_name_20pct_v1",
            "oos_execution_adaptive_event_top100_vol24",
        ),
        (
            event_anchor.clone(),
            "off",
            "0.00",
            "0.00",
            0usize,
            "1.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "impact_turnover_15pct_v1",
            "twap_15d_v1",
            Decimal::new(75, 3),
            90usize,
            "soft_single_name_20pct_v1",
            "oos_execution_adaptive_event_off_sharpe_twap15",
        ),
        (
            fill_anchor,
            "gentle_roll_sharpe120_030_neg20_70",
            "0.30",
            "-0.20",
            120usize,
            "0.70",
            "capacity_stress_participation_headroom_floor_70_v1",
            "fillable_gross_95_v1",
            "impact_turnover_20pct_v1",
            "twap_10d_v1",
            Decimal::new(10, 2),
            60usize,
            "off",
            "oos_execution_adaptive_fill_recovery_top80_vol26",
        ),
        (
            value_anchor,
            "off",
            "0.00",
            "0.00",
            0usize,
            "1.00",
            "capacity_participation_balanced_v1",
            "stress_fill_gross_98_v1",
            "off",
            "immediate",
            Decimal::new(10, 2),
            30usize,
            "off",
            "oos_execution_adaptive_value_control",
        ),
    ] {
        let mut seed = if sharpe_profile == "off" {
            with_sharpe_off_seed(seed)
        } else {
            with_sharpe_profile_seed(
                seed,
                sharpe_profile,
                sharpe_reduce_start,
                sharpe_reduce_full,
                sharpe_lookback_days,
                sharpe_min_exposure,
            )
        };
        seed = with_capacity_risk_budget_seed(seed, capacity_risk_budget);
        seed = with_cash_utilization_seed(seed, cash_utilization);
        seed = with_execution_impact_budget_seed(seed, execution_impact_budget);
        seed = with_execution_schedule_control_seed(
            seed,
            execution_schedule_profile,
            daily_target_move_limit_pct,
            max_carry_days,
        );
        seed = with_execution_carry_policy_seed(seed, "roll_forward_v1");
        seed = with_risk_contribution_control_seed(seed, risk_contribution_control);
        seed["oos_execution_adaptive_rebuild_profile"] = json!(profile_name);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_execution_capacity_aware_candidate_ranking_seed_trials() -> Vec<Value> {
    let high_sharpe_anchors = professional_return_distribution_repair_seed_trials();
    let Some(cl_anchor) = high_sharpe_anchors.first().cloned() else {
        return Vec::new();
    };
    let cm_anchor = high_sharpe_anchors.get(1).cloned();
    let mut seeds = Vec::new();

    for (
        anchor,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        max_participation_rate,
        risk_contribution_control,
    ) in [
        (
            cl_anchor.clone(),
            100usize,
            180usize,
            "0.08",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(8, 2),
            90usize,
            2200usize,
            Decimal::new(10, 2),
            "soft_single_name_20pct_v1",
        ),
        (
            cl_anchor.clone(),
            120usize,
            180usize,
            "0.06",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(8, 2),
            90usize,
            2200usize,
            Decimal::new(10, 2),
            "soft_single_name_20pct_v1",
        ),
        (
            cl_anchor,
            80usize,
            160usize,
            "0.08",
            "0.95",
            "2.0",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            Decimal::new(8, 2),
            75usize,
            1800usize,
            Decimal::new(12, 2),
            "soft_single_name_20pct_v1",
        ),
    ] {
        let seed = with_cl_anchor_fill_recovery_seed(
            anchor,
            top_n,
            rebalance_days,
            max_position_pct,
            max_gross_exposure,
            capacity_penalty_strength,
            capacity_risk_budget,
            daily_target_move_limit_pct,
            max_carry_days,
            score_candidate_pool_size,
            max_participation_rate,
            risk_contribution_control,
        );
        append_unique_seeds(
            &mut seeds,
            vec![with_capacity_aware_candidate_ranking_seed(
                seed,
                &format!("capacity_rank_top{top_n}_rebalance{rebalance_days}"),
            )],
        );
    }

    if let Some(cm_anchor) = cm_anchor {
        let seed = with_cl_anchor_fill_recovery_seed(
            cm_anchor,
            100,
            180,
            "0.08",
            "0.90",
            "2.5",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            Decimal::new(8, 2),
            90,
            2200,
            Decimal::new(10, 2),
            "soft_single_name_20pct_v1",
        );
        append_unique_seeds(
            &mut seeds,
            vec![with_capacity_aware_candidate_ranking_seed(
                seed,
                "capacity_rank_cm_top100_rebalance180",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_return_distribution_repair_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    let cl_anchor = with_risk_budget_lookback_seed(
        with_position_shape_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    with_event_combo_gate_seed_with_min_score(
                        return_anchor.clone(),
                        "valuation_exclude_bottom45",
                        "phase7_valuation_v1",
                        "exclude_negative",
                        "0.45",
                        "0",
                        ScoreDirection::Descending,
                    ),
                    "quality_mixed_orthogonal_risk_memory_router_v3",
                    "cl_high_sharpe_residual_risk_memory",
                ),
                "vol120_15_48_100",
                "0.15",
                120,
                "0.48",
                "1",
            ),
            "maxpos15_corr75_cl_orthogonal",
            "0.15",
            "0.75",
        ),
        "bp_rb180_cl_orthogonal",
        180,
    );
    let cm_anchor = with_risk_budget_lookback_seed(
        with_position_shape_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    with_event_combo_gate_seed_with_min_score(
                        return_anchor.clone(),
                        "valuation_exclude_bottom40",
                        "phase7_valuation_v1",
                        "exclude_negative",
                        "0.40",
                        "0",
                        ScoreDirection::Descending,
                    ),
                    "quality_nonlinear_alpha_risk_memory_router_v3",
                    "cm_return_sharpe_risk_memory",
                ),
                "vol120_15_48_100",
                "0.15",
                120,
                "0.48",
                "1",
            ),
            "maxpos15_corr75_cm_nonlinear",
            "0.15",
            "0.75",
        ),
        "bp_rb180_cm_nonlinear",
        180,
    );

    append_unique_seeds(
        &mut seeds,
        vec![
            finalize_return_distribution_seed(cl_anchor.clone()),
            finalize_return_distribution_seed(cm_anchor.clone()),
        ],
    );

    for (base, router) in [
        (
            cm_anchor.clone(),
            "quality_event_window_return_sharpe_router_v3",
        ),
        (
            cl_anchor.clone(),
            "quality_event_window_return_sharpe_router_v4",
        ),
    ] {
        for (
            profile_name,
            combo_name,
            mode,
            min_score,
            boost_weight,
            sharpe_profile,
            risk_budget_days,
        ) in [
            (
                "event_window_10d_boost_p75_3pct",
                "phase7_event_window_earnings_10d_v1",
                "boost_positive",
                "0.38",
                "0.03",
                "off",
                180,
            ),
            (
                "event_window_20d_boost_p75_3pct",
                "phase7_event_window_earnings_v1",
                "boost_positive",
                "0.38",
                "0.03",
                "roll_sharpe180_050_neg10_60",
                180,
            ),
            (
                "event_window_40d_exclude_negative",
                "phase7_event_window_earnings_40d_v1",
                "exclude_negative",
                "0.00",
                "0",
                "roll_sharpe120_050_neg10_60",
                160,
            ),
            (
                "event_window_40d_boost_p75_3pct",
                "phase7_event_window_earnings_40d_v1",
                "boost_positive",
                "0.38",
                "0.03",
                "roll_sharpe180_050_neg10_60",
                180,
            ),
        ] {
            let mut seed = with_risk_budget_lookback_seed(
                with_market_regime_seed(
                    with_event_combo_gate_seed_with_min_score(
                        base.clone(),
                        profile_name,
                        combo_name,
                        mode,
                        min_score,
                        boost_weight,
                        ScoreDirection::Descending,
                    ),
                    router,
                ),
                &format!("bp_rb{risk_budget_days}_{router}_{profile_name}"),
                risk_budget_days,
            );
            if sharpe_profile == "roll_sharpe180_050_neg10_60" {
                seed = with_sharpe_profile_seed(seed, sharpe_profile, "0.50", "-0.10", 180, "0.60");
            } else if sharpe_profile == "roll_sharpe120_050_neg10_60" {
                seed = with_sharpe_profile_seed(seed, sharpe_profile, "0.50", "-0.10", 120, "0.60");
            } else {
                seed["portfolio_sharpe_control"] = json!("off");
            }
            append_unique_seeds(&mut seeds, vec![finalize_return_distribution_seed(seed)]);
        }
    }

    for (base, regime, vol_profile, target_pct, min_exposure, risk_budget_days) in [
        (
            cl_anchor.clone(),
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
        ),
        (
            cm_anchor.clone(),
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
        ),
    ] {
        let residual_seed = with_sharpe_profile_seed(
            with_risk_budget_lookback_seed(
                with_volatility_profile_seed(
                    with_market_regime_seed(
                        with_event_combo_gate_seed_with_min_score(
                            base,
                            "residual_confirm_top40",
                            "phase7_quality_residual_confirm_10pct_v1",
                            "require_positive",
                            "0.40",
                            "0",
                            ScoreDirection::Descending,
                        ),
                        regime,
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("bp_rb{risk_budget_days}_{regime}_residual_confirm"),
                risk_budget_days,
            ),
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
        );
        append_unique_seeds(
            &mut seeds,
            vec![finalize_return_distribution_seed(residual_seed)],
        );
    }

    seeds.into_iter().take(12).collect()
}
