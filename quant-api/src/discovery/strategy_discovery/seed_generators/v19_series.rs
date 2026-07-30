//! BC4 策略发现 / seed_generators / v19_series：v19 当前活跃策略族 种子生成器。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。

use rust_decimal::Decimal;
use serde_json::{json, Value};

use super::super::profiles::ScoreDirection;
use super::*;

pub(crate) fn tag_trainable_alpha_admission_seed(
    mut seed: Value,
    profile: &str,
    alpha_source_family: &str,
) -> Value {
    if let Some(object) = seed.as_object_mut() {
        for key in [
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
    seed["trainable_alpha_admission_profile"] = json!(profile);
    seed["alpha_source_family"] = json!(alpha_source_family);
    seed
}

pub(crate) fn professional_v19_multi_alpha_sleeve_admission_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (sleeve_family, combo_name, score_direction, top_n, market_regime, candidate_ranking) in [
        (
            "quality_core",
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            120usize,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "valuation_guard",
            "phase7_valuation_v1",
            ScoreDirection::Descending,
            120usize,
            "quality_state_alpha_overlay_selector_v1",
            "alpha_first_low_impact_v1",
        ),
        (
            "growth_recovery",
            "phase7_growth_recovery_v1",
            ScoreDirection::Descending,
            100usize,
            "quality_mixed_state_risk_memory_router_v14",
            "alpha_first_low_impact_v1",
        ),
        (
            "relative_strength",
            "phase7_quality_relative_strength_v1",
            ScoreDirection::Descending,
            100usize,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "moneyflow_quality",
            "phase7_quality_moneyflow_pos_5pct_v1",
            ScoreDirection::Ascending,
            100usize,
            "quality_mixed_state_risk_memory_router_v14",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "cashflow_dividend_quality",
            "phase7_quality_cashflow_dividend_confirm_v1",
            ScoreDirection::Ascending,
            120usize,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "value_recovery",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Descending,
            100usize,
            "quality_state_alpha_overlay_selector_v1",
            "alpha_first_low_impact_v1",
        ),
        (
            "residual_quality",
            "phase7_quality_residual_confirm_10pct_v1",
            ScoreDirection::Ascending,
            120usize,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "blend_quality_growth",
            "phase7_blend_quality_growth_v1",
            ScoreDirection::Ascending,
            120usize,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            "recovery_tilt",
            "phase7_blend_recovery_tilt_v1",
            ScoreDirection::Ascending,
            120usize,
            "quality_state_alpha_overlay_selector_v1",
            "alpha_first_low_impact_v1",
        ),
    ] {
        if !is_phase7_base_trainable_alpha(combo_name) {
            continue;
        }
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_multi_alpha_sleeve_seed(
                anchor.clone(),
                sleeve_family,
                combo_name,
                score_direction,
                top_n,
                60,
                market_regime,
                candidate_ranking,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_v19_event_surprise_sleeve_gate_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    let market_regime = "quality_mixed_state_risk_memory_router_v14";
    let candidate_ranking = "alpha_first_low_impact_v1";

    for (variant_name, combo_name, top_n) in [
        (
            "fq_change_control",
            "phase7_financial_quality_change_v1",
            100usize,
        ),
        (
            "event_surprise_sleeve_05pct",
            "phase7_fq_change_event_surprise_sleeve_05pct_v1",
            100usize,
        ),
        (
            "event_surprise_sleeve_10pct",
            "phase7_fq_change_event_surprise_sleeve_10pct_v1",
            100usize,
        ),
        (
            "event_surprise_sleeve_15pct_boundary",
            "phase7_fq_change_event_surprise_sleeve_15pct_v1",
            100usize,
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_event_surprise_sleeve_gate_seed(
                anchor.clone(),
                variant_name,
                combo_name,
                top_n,
                60,
                market_regime,
                candidate_ranking,
            )],
        );
    }

    for (variant_name, gate_profile, mode, min_score, boost_weight) in [
        (
            "fq_change_event_surprise_boost_p75_3pct",
            "event_surprise_boost_p75_3pct",
            "boost_positive",
            "0.35",
            "0.03",
        ),
        (
            "fq_change_event_surprise_exclude_negative",
            "event_surprise_exclude_negative",
            "exclude_negative",
            "0",
            "0",
        ),
    ] {
        let seed = with_v19_event_surprise_sleeve_gate_seed(
            anchor.clone(),
            variant_name,
            "phase7_financial_quality_change_v1",
            100,
            60,
            market_regime,
            candidate_ranking,
        );
        append_unique_seeds(
            &mut seeds,
            vec![with_event_combo_gate_seed_with_min_score(
                seed,
                gate_profile,
                "phase7_event_surprise_v1",
                mode,
                min_score,
                boost_weight,
                ScoreDirection::Descending,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_v19_supply_float_sleeve_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    let market_regime = "quality_mixed_state_risk_memory_router_v14";
    let candidate_ranking = "alpha_first_low_impact_v1";

    for (variant_name, combo_name) in [
        ("fq_change_control", "phase7_financial_quality_change_v1"),
        (
            "supply_float_sleeve_05pct",
            "phase7_fq_change_supply_float_sleeve_05pct_v1",
        ),
        (
            "supply_float_sleeve_10pct",
            "phase7_fq_change_supply_float_sleeve_10pct_v1",
        ),
        (
            "supply_float_sleeve_15pct_boundary",
            "phase7_fq_change_supply_float_sleeve_15pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_supply_float_sleeve_seed(
                anchor.clone(),
                variant_name,
                combo_name,
                100,
                60,
                market_regime,
                candidate_ranking,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_v19_unlock_pressure_sleeve_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    let market_regime = "quality_mixed_state_risk_memory_router_v14";
    let candidate_ranking = "alpha_first_low_impact_v1";

    for (variant_name, combo_name) in [
        ("fq_change_control", "phase7_financial_quality_change_v1"),
        (
            "unlock_pressure_sleeve_05pct",
            "phase7_fq_change_unlock_pressure_sleeve_05pct_v1",
        ),
        (
            "unlock_pressure_sleeve_10pct",
            "phase7_fq_change_unlock_pressure_sleeve_10pct_v1",
        ),
        (
            "unlock_pressure_sleeve_15pct_boundary",
            "phase7_fq_change_unlock_pressure_sleeve_15pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_unlock_pressure_sleeve_seed(
                anchor.clone(),
                variant_name,
                combo_name,
                100,
                60,
                market_regime,
                candidate_ranking,
            )],
        );
    }

    for (variant_name, gate_profile, mode, min_score, boost_weight) in [
        (
            "unlock_pressure_exclude_sideways_gate",
            "unlock_pressure_exclude_sideways_gate",
            "boost_positive",
            "0.35",
            "0.03",
        ),
        (
            "unlock_pressure_exclude_sideways_negative_guard",
            "unlock_pressure_exclude_sideways_negative_guard",
            "exclude_negative",
            "0",
            "0",
        ),
    ] {
        let seed = with_v19_unlock_pressure_sleeve_seed(
            anchor.clone(),
            variant_name,
            "phase7_financial_quality_change_v1",
            100,
            60,
            market_regime,
            candidate_ranking,
        );
        append_unique_seeds(
            &mut seeds,
            vec![
                with_event_combo_gate_seed_active_in_with_min_score_and_boost(
                    seed,
                    gate_profile,
                    "phase7_unlock_supply_pressure_v1",
                    mode,
                    min_score,
                    boost_weight,
                    ScoreDirection::Descending,
                    &["bear", "bull", "mixed", "high_volatility"],
                ),
            ],
        );
    }

    seeds
}

pub(crate) fn professional_v19_forecast_revision_sleeve_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    let market_regime = "quality_mixed_state_risk_memory_router_v14";
    let candidate_ranking = "alpha_first_low_impact_v1";

    for (variant_name, combo_name) in [
        ("fq_change_control", "phase7_financial_quality_change_v1"),
        (
            "forecast_revision_sleeve_05pct",
            "phase7_fq_change_forecast_revision_sleeve_05pct_v1",
        ),
        (
            "forecast_revision_sleeve_10pct",
            "phase7_fq_change_forecast_revision_sleeve_10pct_v1",
        ),
        (
            "forecast_revision_sleeve_15pct_boundary",
            "phase7_fq_change_forecast_revision_sleeve_15pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_forecast_revision_sleeve_seed(
                anchor.clone(),
                variant_name,
                combo_name,
                100,
                60,
                market_regime,
                candidate_ranking,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_v19_shareholder_structure_sleeve_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    let market_regime = "off";
    let candidate_ranking = "alpha_first_low_impact_v1";

    for (variant_name, combo_name) in [
        ("fq_change_control", "phase7_financial_quality_change_v1"),
        (
            "shareholder_structure_sleeve_05pct",
            "phase7_fq_change_shareholder_structure_sleeve_05pct_v1",
        ),
        (
            "shareholder_structure_sleeve_10pct",
            "phase7_fq_change_shareholder_structure_sleeve_10pct_v1",
        ),
        (
            "shareholder_structure_sleeve_15pct_boundary",
            "phase7_fq_change_shareholder_structure_sleeve_15pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_shareholder_structure_sleeve_seed(
                anchor.clone(),
                variant_name,
                combo_name,
                100,
                60,
                market_regime,
                candidate_ranking,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_v19_event_post_return_overlay_admission_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (top_n, rebalance_days, market_regime, candidate_ranking) in [
        (
            100usize,
            160usize,
            "quality_event_window_return_sharpe_router_v4",
            "alpha_first_low_impact_v1",
        ),
        (
            120usize,
            180usize,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
        (
            80usize,
            120usize,
            "quality_mixed_state_risk_memory_router_v14",
            "alpha_first_low_impact_v1",
        ),
        (
            100usize,
            180usize,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
        ),
    ] {
        for (variant_name, event_gate) in [
            ("overlay_only", None),
            (
                "overlay_boost_positive_event_post_return",
                Some((
                    "event_post_return_boost_pos_5pct",
                    "boost_positive",
                    "0",
                    "0.05",
                )),
            ),
            (
                "overlay_exclude_negative_event_post_return",
                Some((
                    "event_post_return_exclude_negative",
                    "exclude_negative",
                    "0",
                    "0",
                )),
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_v19_event_post_return_overlay_seed(
                    anchor.clone(),
                    &format!("{variant_name}_top{top_n}_rebalance{rebalance_days}"),
                    top_n,
                    rebalance_days,
                    ScoreDirection::Descending,
                    market_regime,
                    candidate_ranking,
                    event_gate,
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_v19_execution_repair_admission_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for base_seed in professional_v19_multi_alpha_sleeve_admission_seed_trials() {
        for (
            variant_name,
            top_n,
            max_position_pct,
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
                "v19_exec_fill95_twap10_balanced_top80_pool900",
                80usize,
                "0.06",
                "0.95",
                "1.00",
                "capacity_participation_balanced_v1",
                "fillable_gross_95_v1",
                "twap_10d_v1",
                Decimal::new(10, 2),
                30usize,
                "impact_turnover_20pct_v1",
                900usize,
            ),
            (
                "v19_exec_stress98_twap15_balanced_top100_pool1200",
                100usize,
                "0.06",
                "1",
                "1.00",
                "capacity_participation_balanced_v1",
                "stress_fill_gross_98_v1",
                "twap_15d_v1",
                Decimal::new(75, 3),
                45usize,
                "impact_turnover_20pct_v1",
                1200usize,
            ),
            (
                "v19_exec_fill95_twap15_strict_top120_pool1500",
                120usize,
                "0.05",
                "0.95",
                "1.25",
                "capacity_participation_strict_v1",
                "fillable_gross_95_v1",
                "twap_15d_v1",
                Decimal::new(6, 2),
                60usize,
                "impact_turnover_30pct_v1",
                1500usize,
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_v19_execution_repair_admission_seed(
                    base_seed.clone(),
                    variant_name,
                    top_n,
                    max_position_pct,
                    max_gross_exposure,
                    capacity_penalty_strength,
                    capacity_risk_budget,
                    cash_utilization,
                    execution_schedule_profile,
                    daily_target_move_limit_pct,
                    max_carry_days,
                    execution_impact_budget,
                    score_candidate_pool_size,
                )],
            );
        }
    }
    seeds
}

pub(crate) fn professional_v19_train_window_ml_alpha_rebuild_seed_trials() -> Vec<Value> {
    professional_v19_execution_repair_admission_seed_trials()
        .into_iter()
        .map(with_v19_train_window_ml_alpha_rebuild_seed)
        .collect()
}

pub(crate) fn professional_v19_train_window_ml_simple_excess_rebuild_seed_trials() -> Vec<Value> {
    professional_v19_execution_repair_admission_seed_trials()
        .into_iter()
        .map(with_v19_train_window_ml_simple_excess_rebuild_seed)
        .collect()
}

pub(crate) fn professional_v19_train_window_ml_simple_excess_low_impact_rebuild_seed_trials() -> Vec<Value> {
    let Some(growth_seed) = professional_v19_multi_alpha_sleeve_admission_seed_trials()
        .into_iter()
        .find(|seed| seed["alpha_sleeve_family"] == "growth_recovery")
    else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        variant_name,
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
        execution_impact_budget,
        score_candidate_pool_size,
        partial_rebalance_ratio,
    ) in [
        (
            "v19_sxli_top80_rebalance120_twap15_partial35",
            80usize,
            120usize,
            "0.06",
            "0.95",
            "1.50",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            "twap_15d_v1",
            Decimal::new(5, 2),
            80usize,
            "impact_turnover_15pct_v1",
            1200usize,
            "0.35",
        ),
        (
            "v19_sxli_top80_rebalance160_twap20_partial25",
            80usize,
            160usize,
            "0.06",
            "0.95",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "twap_20d_v1",
            Decimal::new(4, 2),
            100usize,
            "impact_turnover_15pct_v1",
            1800usize,
            "0.25",
        ),
        (
            "v19_sxli_top100_rebalance160_twap20_partial35",
            100usize,
            160usize,
            "0.06",
            "0.95",
            "1.75",
            "capacity_participation_strict_v1",
            "stress_fill_gross_98_v1",
            "twap_20d_v1",
            Decimal::new(4, 2),
            100usize,
            "impact_turnover_15pct_v1",
            1800usize,
            "0.35",
        ),
        (
            "v19_sxli_top100_rebalance180_twap20_partial25",
            100usize,
            180usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "twap_20d_v1",
            Decimal::new(35, 3),
            120usize,
            "impact_turnover_15pct_v1",
            1800usize,
            "0.25",
        ),
        (
            "v19_sxli_top80_rebalance180_twap20_fillable_partial25",
            80usize,
            180usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            "twap_20d_v1",
            Decimal::new(35, 3),
            120usize,
            "impact_turnover_15pct_v1",
            1500usize,
            "0.25",
        ),
        (
            "v19_sxli_top100_rebalance120_twap15_balanced_partial35",
            100usize,
            120usize,
            "0.06",
            "0.95",
            "1.50",
            "capacity_participation_balanced_v1",
            "fillable_gross_95_v1",
            "twap_15d_v1",
            Decimal::new(5, 2),
            80usize,
            "impact_turnover_15pct_v1",
            1200usize,
            "0.35",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![
                with_v19_train_window_ml_simple_excess_low_impact_rebuild_seed(
                    with_v19_simple_excess_low_impact_execution_seed(
                        growth_seed.clone(),
                        variant_name,
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
                        execution_impact_budget,
                        score_candidate_pool_size,
                        partial_rebalance_ratio,
                    ),
                ),
            ],
        );
    }

    seeds
}

pub(crate) fn professional_v19_train_window_ml_h120_low_impact_rebuild_seed_trials() -> Vec<Value> {
    let base_seeds = professional_v19_multi_alpha_sleeve_admission_seed_trials();
    let mut seeds = Vec::new();

    for (
        sleeve_family,
        variant_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        cash_utilization,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        partial_rebalance_ratio,
    ) in [
        (
            "quality_core",
            "v19_h120_quality_top100_rebalance240_twap20_partial25",
            100usize,
            240usize,
            "0.06",
            "0.90",
            "1.75",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(35, 3),
            160usize,
            2400usize,
            "0.25",
        ),
        (
            "valuation_guard",
            "v19_h120_valuation_top100_rebalance240_twap20_partial35",
            100usize,
            240usize,
            "0.06",
            "0.90",
            "1.50",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            Decimal::new(4, 2),
            140usize,
            2200usize,
            "0.35",
        ),
        (
            "growth_recovery",
            "v19_h120_growth_top80_rebalance240_twap20_partial25",
            80usize,
            240usize,
            "0.06",
            "0.95",
            "1.75",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(35, 3),
            160usize,
            2400usize,
            "0.25",
        ),
        (
            "relative_strength",
            "v19_h120_relative_top80_rebalance300_twap20_partial35",
            80usize,
            300usize,
            "0.06",
            "0.95",
            "1.50",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            Decimal::new(35, 3),
            180usize,
            2200usize,
            "0.35",
        ),
        (
            "moneyflow_quality",
            "v19_h120_moneyflow_top100_rebalance300_twap20_partial25",
            100usize,
            300usize,
            "0.06",
            "0.90",
            "1.75",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(3, 2),
            180usize,
            2400usize,
            "0.25",
        ),
        (
            "cashflow_dividend_quality",
            "v19_h120_cashflow_top100_rebalance360_twap20_partial35",
            100usize,
            360usize,
            "0.06",
            "0.90",
            "1.50",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            Decimal::new(3, 2),
            200usize,
            2600usize,
            "0.35",
        ),
        (
            "value_recovery",
            "v19_h120_value_top100_rebalance300_twap20_partial25",
            100usize,
            300usize,
            "0.06",
            "0.90",
            "1.75",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(3, 2),
            180usize,
            2400usize,
            "0.25",
        ),
        (
            "residual_quality",
            "v19_h120_residual_top120_rebalance360_twap20_partial25",
            120usize,
            360usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(25, 3),
            220usize,
            2600usize,
            "0.25",
        ),
    ] {
        let Some(base_seed) = base_seeds
            .iter()
            .find(|seed| seed["alpha_sleeve_family"] == sleeve_family)
        else {
            continue;
        };
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_train_window_ml_h120_low_impact_rebuild_seed(
                with_v19_simple_excess_low_impact_execution_seed(
                    base_seed.clone(),
                    variant_name,
                    top_n,
                    rebalance_days,
                    max_position_pct,
                    max_gross_exposure,
                    capacity_penalty_strength,
                    capacity_risk_budget,
                    cash_utilization,
                    "twap_20d_v1",
                    daily_target_move_limit_pct,
                    max_carry_days,
                    "impact_turnover_15pct_v1",
                    score_candidate_pool_size,
                    partial_rebalance_ratio,
                ),
            )],
        );
    }

    seeds
}

pub(crate) fn professional_v19_train_window_ml_rae_h120_residual_capacity_rebuild_seed_trials() -> Vec<Value> {
    let base_seeds = professional_v19_multi_alpha_sleeve_admission_seed_trials();
    let mut seeds = Vec::new();

    for (
        sleeve_family,
        variant_name,
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
        partial_rebalance_ratio,
    ) in [
        (
            "residual_quality",
            "v19_rae_h120_residual_top120_rebalance360_twap20_partial25",
            120usize,
            360usize,
            "0.04",
            "0.85",
            "2.25",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "twap_20d_v1",
            Decimal::new(2, 2),
            240usize,
            3000usize,
            "0.25",
        ),
        (
            "value_recovery",
            "v19_rae_h120_value_top100_rebalance300_twap20_partial25",
            100usize,
            300usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "twap_20d_v1",
            Decimal::new(25, 3),
            220usize,
            2800usize,
            "0.25",
        ),
        (
            "relative_strength",
            "v19_rae_h120_relative_top80_rebalance300_twap20_partial25",
            80usize,
            300usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            "twap_20d_v1",
            Decimal::new(3, 2),
            200usize,
            2600usize,
            "0.25",
        ),
        (
            "moneyflow_quality",
            "v19_rae_h120_moneyflow_top100_rebalance240_twap20_partial25",
            100usize,
            240usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "twap_20d_v1",
            Decimal::new(3, 2),
            180usize,
            2800usize,
            "0.25",
        ),
        (
            "cashflow_dividend_quality",
            "v19_rae_h120_cashflow_top120_rebalance360_twap20_partial25",
            120usize,
            360usize,
            "0.04",
            "0.85",
            "2.25",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            "twap_20d_v1",
            Decimal::new(2, 2),
            240usize,
            3000usize,
            "0.25",
        ),
        (
            "blend_quality_growth",
            "v19_rae_h120_blend_top100_rebalance300_twap20_partial25",
            100usize,
            300usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            "twap_20d_v1",
            Decimal::new(25, 3),
            220usize,
            2800usize,
            "0.25",
        ),
    ] {
        let Some(base_seed) = base_seeds
            .iter()
            .find(|seed| seed["alpha_sleeve_family"] == sleeve_family)
        else {
            continue;
        };
        append_unique_seeds(
            &mut seeds,
            vec![
                with_v19_train_window_ml_rae_h120_residual_capacity_rebuild_seed(
                    with_v19_simple_excess_low_impact_execution_seed(
                        base_seed.clone(),
                        variant_name,
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
                        "impact_turnover_15pct_v1",
                        score_candidate_pool_size,
                        partial_rebalance_ratio,
                    ),
                ),
            ],
        );
    }

    seeds
}

pub(crate) fn professional_v19_train_window_ml_event_sentiment_rebuild_seed_trials() -> Vec<Value> {
    let base_seeds = professional_v19_multi_alpha_sleeve_admission_seed_trials();
    let mut seeds = Vec::new();

    for (
        base_family,
        sleeve_family,
        variant_name,
        top_n,
        rebalance_days,
        max_position_pct,
        max_gross_exposure,
        capacity_penalty_strength,
        capacity_risk_budget,
        cash_utilization,
        daily_target_move_limit_pct,
        max_carry_days,
        score_candidate_pool_size,
        partial_rebalance_ratio,
    ) in [
        (
            "quality_core",
            "event_sentiment_quality",
            "v19_evt_quality_top100_rebalance240_twap20_partial25",
            100usize,
            240usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(3, 2),
            180usize,
            2800usize,
            "0.25",
        ),
        (
            "quality_core",
            "event_sentiment_quality",
            "v19_evt_quality_top120_rebalance300_twap20_partial25",
            120usize,
            300usize,
            "0.04",
            "0.85",
            "2.25",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(25, 3),
            220usize,
            3000usize,
            "0.25",
        ),
        (
            "moneyflow_quality",
            "event_moneyflow_capacity",
            "v19_evt_moneyflow_top100_rebalance240_twap20_partial25",
            100usize,
            240usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(3, 2),
            180usize,
            3000usize,
            "0.25",
        ),
        (
            "moneyflow_quality",
            "event_moneyflow_capacity",
            "v19_evt_moneyflow_top120_rebalance300_twap20_partial25",
            120usize,
            300usize,
            "0.04",
            "0.85",
            "2.25",
            "capacity_participation_strict_v1",
            "fillable_gross_95_v1",
            Decimal::new(25, 3),
            220usize,
            3000usize,
            "0.25",
        ),
        (
            "residual_quality",
            "event_residual_quality",
            "v19_evt_residual_top100_rebalance300_twap20_partial25",
            100usize,
            300usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(25, 3),
            220usize,
            2800usize,
            "0.25",
        ),
        (
            "blend_quality_growth",
            "event_blend_recovery",
            "v19_evt_blend_top100_rebalance300_twap20_partial25",
            100usize,
            300usize,
            "0.05",
            "0.90",
            "2.00",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            Decimal::new(25, 3),
            220usize,
            2800usize,
            "0.25",
        ),
    ] {
        let Some(base_seed) = base_seeds
            .iter()
            .find(|seed| seed["alpha_sleeve_family"] == base_family)
        else {
            continue;
        };
        append_unique_seeds(
            &mut seeds,
            vec![with_v19_train_window_ml_event_sentiment_rebuild_seed(
                with_v19_simple_excess_low_impact_execution_seed(
                    base_seed.clone(),
                    variant_name,
                    top_n,
                    rebalance_days,
                    max_position_pct,
                    max_gross_exposure,
                    capacity_penalty_strength,
                    capacity_risk_budget,
                    cash_utilization,
                    "twap_20d_v1",
                    daily_target_move_limit_pct,
                    max_carry_days,
                    "impact_turnover_15pct_v1",
                    score_candidate_pool_size,
                    partial_rebalance_ratio,
                ),
                sleeve_family,
            )],
        );
    }

    seeds
}
