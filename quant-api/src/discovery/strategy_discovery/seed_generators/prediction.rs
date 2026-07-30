//! BC4 策略发现 / seed_generators / prediction：预测确认 种子生成器。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。

use rust_decimal::Decimal;
use serde_json::{json, Value};

use super::super::profiles::ScoreDirection;
use super::*;

pub(crate) fn professional_prediction_confirmed_sharpe_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let wide_prediction_set = "pred-p7-wf-wide-qgvrel-v1-201602-202605";
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        blend_weight,
        min_percentile,
    ) in [
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "0.02",
            None,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "0.05",
            Some("0.20"),
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "0.05",
            Some("0.20"),
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
            "0.05",
            Some("0.20"),
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "0.02",
            None,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "0.05",
            Some("0.20"),
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "0.02",
            None,
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            160,
            "0.08",
            Some("0.30"),
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            180,
            "0.05",
            Some("0.20"),
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "0.02",
            None,
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "0.05",
            Some("0.20"),
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            180,
            "0.08",
            Some("0.30"),
        ),
    ] {
        let seed = with_prediction_confirmation_seed(
            with_risk_budget_lookback_seed(
                with_position_shape_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            with_event_combo_gate_seed_with_min_score(
                                return_anchor.clone(),
                                valuation_profile,
                                "phase7_valuation_v1",
                                "exclude_negative",
                                valuation_min_score,
                                "0",
                                ScoreDirection::Descending,
                            ),
                            regime,
                            &format!(
                                "{regime}_{valuation_profile}_{vol_profile}_{blend_weight}_prediction_confirmed_bridge"
                            ),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("maxpos15_corr75_{regime}_{vol_profile}"),
                    "0.15",
                    "0.75",
                ),
                &format!(
                    "bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}_{vol_profile}"
                ),
                risk_budget_lookback_days,
            ),
            wide_prediction_set,
            blend_weight,
            min_percentile,
        );
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_contribution_control_seed(
                with_candidate_risk_filter_seed(seed, "off"),
                "soft_single_name_20pct_v1",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_native_alpha_fusion_prediction_confirmation_seed_trials() -> Vec<Value> {
    let wide_prediction_set = "pred-p7-wf-wide-qgvrel-v1-201602-202605";
    let mut seeds = Vec::new();

    if let Some(capacity_repair_seed) =
        professional_execution_pit_alpha_first_low_impact_seed_trials()
            .into_iter()
            .next()
    {
        let mut seed = with_prediction_confirmation_seed(
            with_capacity_aware_candidate_ranking_seed(
                capacity_repair_seed,
                "native_alpha_fusion_prediction_capacity_repair_top80",
            ),
            wide_prediction_set,
            "0.02",
            None,
        );
        seed["native_prediction_capacity_repair_profile"] =
            json!("prediction_capacity_repair_top80_rebalance160");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    append_unique_seeds(
        &mut seeds,
        professional_prediction_confirmed_sharpe_bridge_seed_trials(),
    );
    seeds
}

pub(crate) fn professional_prediction_capacity_dual_objective_seed_trials() -> Vec<Value> {
    let bridge_seeds = professional_prediction_confirmed_sharpe_bridge_seed_trials();
    let Some(return_anchor) = bridge_seeds.first().cloned() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_prediction_capacity_return_anchor_seed(
            return_anchor.clone(),
        )],
    );

    for (
        source_index,
        profile_name,
        candidate_ranking,
        top_n,
        max_position_pct,
        capacity_risk_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        partial_rebalance_ratio,
        score_candidate_pool_size,
    ) in [
        (
            1usize,
            "alpha_first_twap15_headroom70",
            "alpha_first_low_impact_v1",
            40usize,
            "0.10",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            "0.35",
            1800usize,
        ),
        (
            2usize,
            "capacity_aware_twap15_headroom70",
            "capacity_aware_alpha_liquidity_v1",
            40usize,
            "0.10",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            "0.35",
            1800usize,
        ),
        (
            3usize,
            "capacity_aware_twap20_blended_headroom70",
            "capacity_aware_alpha_liquidity_v1",
            60usize,
            "0.08",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            100usize,
            "0.25",
            2200usize,
        ),
    ] {
        let source_seed = bridge_seeds
            .get(source_index)
            .cloned()
            .unwrap_or_else(|| return_anchor.clone());
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_capacity_dual_objective_seed(
                source_seed,
                profile_name,
                candidate_ranking,
                top_n,
                max_position_pct,
                capacity_risk_budget,
                execution_schedule_profile,
                daily_target_move_limit_pct,
                max_carry_days,
                partial_rebalance_ratio,
                score_candidate_pool_size,
                "listed_non_st",
            )],
        );
    }

    for seed in bridge_seeds.into_iter().skip(4) {
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_capacity_dual_objective_seed(
                seed,
                "bridge_tail_light_capacity_neighbor",
                "alpha_first_low_impact_v1",
                40,
                "0.10",
                "capacity_stress_participation_alpha_headroom_floor_70_v1",
                "twap_15d_v1",
                Decimal::new(8, 2),
                75,
                "0.35",
                1800,
                "listed_non_st",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_prediction_target_gross_signal_fidelity_seed_trials() -> Vec<Value> {
    let bridge_seeds = professional_prediction_confirmed_sharpe_bridge_seed_trials();
    let Some(return_anchor) = bridge_seeds.first().cloned() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (profile_name, max_gross_exposure) in [
        ("gross100_return_anchor", "1"),
        ("gross65_signal_fidelity", "0.65"),
        ("gross50_signal_fidelity", "0.50"),
        ("gross35_signal_fidelity", "0.35"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_target_gross_signal_fidelity_seed(
                return_anchor.clone(),
                profile_name,
                max_gross_exposure,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_prediction_confidence_turnover_discovery_seed_trials() -> Vec<Value> {
    let bridge_seeds = professional_prediction_confirmed_sharpe_bridge_seed_trials();
    let Some(return_anchor) = bridge_seeds.first().cloned() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_prediction_confidence_turnover_seed(
            return_anchor.clone(),
            "return_anchor_confidence_off",
            "0.02",
            None,
            120,
            "0",
            "1",
            "off",
            "off",
        )],
    );

    for (
        source_index,
        profile_name,
        blend_weight,
        min_percentile,
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        execution_impact_budget,
    ) in [
        (
            1usize,
            "confidence20_turnover_smooth",
            "0.05",
            Some("0.20"),
            120usize,
            "0.005",
            "0.85",
            "impact_turnover_20pct_v1",
        ),
        (
            2usize,
            "confidence30_turnover_smooth",
            "0.05",
            Some("0.30"),
            160usize,
            "0.01",
            "0.75",
            "impact_turnover_20pct_v1",
        ),
        (
            3usize,
            "confidence40_low_turnover",
            "0.08",
            Some("0.40"),
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
    ] {
        let source_seed = bridge_seeds
            .get(source_index)
            .cloned()
            .unwrap_or_else(|| return_anchor.clone());
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_confidence_turnover_seed(
                source_seed,
                profile_name,
                blend_weight,
                min_percentile,
                rebalance_days,
                rebalance_hysteresis_pct,
                partial_rebalance_ratio,
                execution_impact_budget,
                "roll_forward_v1",
            )],
        );
    }

    for seed in bridge_seeds.into_iter().skip(4) {
        let blend_weight = seed
            .get("prediction_blend_weight")
            .and_then(Value::as_str)
            .unwrap_or("0.05")
            .to_string();
        let min_percentile = seed
            .get("prediction_min_percentile")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some("0.20".to_string()));
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_confidence_turnover_seed(
                seed,
                "bridge_tail_confidence20_turnover_smooth",
                &blend_weight,
                min_percentile.as_deref(),
                160,
                "0.01",
                "0.75",
                "impact_turnover_20pct_v1",
                "roll_forward_v1",
            )],
        );
    }

    seeds
}

pub(crate) fn professional_prediction_confidence_alpha_lift_seed_trials() -> Vec<Value> {
    let confidence_seeds = professional_prediction_confidence_turnover_discovery_seed_trials();
    let low_gap_anchor = confidence_seeds
        .iter()
        .find(|seed| seed["prediction_confidence_turnover_profile"] == "confidence40_low_turnover")
        .cloned()
        .or_else(|| confidence_seeds.first().cloned());
    let Some(low_gap_anchor) = low_gap_anchor else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    for (
        profile_name,
        family,
        combo_name,
        score_direction,
        market_regime,
        blend_weight,
        min_percentile,
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        execution_impact_budget,
    ) in [
        (
            "confidence40_quality_nonlinear_alpha_lift",
            "nonlinear_regime",
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "0.08",
            "0.40",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "confidence40_value_recovery_alpha_lift",
            "value_recovery",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "0.08",
            "0.40",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "confidence40_event_post_curve_lift",
            "event_post_curve",
            "phase7_quality_event_post_return_curve_overlay_v1",
            ScoreDirection::Descending,
            "quality_event_window_return_sharpe_router_v4",
            "0.08",
            "0.40",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "confidence40_event_reaction_segments_lift",
            "event_reaction",
            "phase7_quality_event_reaction_segments_overlay_v1",
            ScoreDirection::Descending,
            "quality_event_window_return_sharpe_router_v4",
            "0.08",
            "0.40",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "confidence40_event_reaction_reversal_lift",
            "event_reaction",
            "phase7_quality_event_reaction_reversal_overlay_v1",
            ScoreDirection::Descending,
            "quality_event_window_return_sharpe_router_v4",
            "0.08",
            "0.40",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "confidence35_return_recovery_lift",
            "value_recovery",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "0.06",
            "0.35",
            160usize,
            "0.01",
            "0.75",
            "impact_turnover_20pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_confidence_alpha_lift_seed(
                low_gap_anchor.clone(),
                profile_name,
                family,
                combo_name,
                score_direction,
                market_regime,
                blend_weight,
                min_percentile,
                rebalance_days,
                rebalance_hysteresis_pct,
                partial_rebalance_ratio,
                execution_impact_budget,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_prediction_long_horizon_low_turnover_seed_trials() -> Vec<Value> {
    let confidence_seeds = professional_prediction_confidence_turnover_discovery_seed_trials();
    let low_turnover_anchor = confidence_seeds
        .iter()
        .find(|seed| seed["prediction_confidence_turnover_profile"] == "confidence40_low_turnover")
        .cloned()
        .or_else(|| confidence_seeds.first().cloned());
    let Some(low_turnover_anchor) = low_turnover_anchor else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    for (
        profile_name,
        blend_weight,
        min_percentile,
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        max_gross_exposure,
    ) in [
        (
            "h60_confidence40_low_turnover_quality",
            "0.08",
            "0.40",
            200usize,
            "0.02",
            "0.65",
            None,
        ),
        (
            "h60_confidence50_low_turnover_quality",
            "0.10",
            "0.50",
            240usize,
            "0.03",
            "0.60",
            None,
        ),
        (
            "h60_confidence40_gross65_signal_fidelity",
            "0.08",
            "0.40",
            240usize,
            "0.02",
            "0.65",
            Some("0.65"),
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_long_horizon_low_turnover_seed(
                low_turnover_anchor.clone(),
                profile_name,
                blend_weight,
                min_percentile,
                rebalance_days,
                rebalance_hysteresis_pct,
                partial_rebalance_ratio,
                max_gross_exposure,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_prediction_long_horizon_regime_alpha_seed_trials() -> Vec<Value> {
    let confidence_seeds = professional_prediction_confidence_turnover_discovery_seed_trials();
    let low_turnover_anchor = confidence_seeds
        .iter()
        .find(|seed| seed["prediction_confidence_turnover_profile"] == "confidence40_low_turnover")
        .cloned()
        .or_else(|| confidence_seeds.first().cloned());
    let Some(low_turnover_anchor) = low_turnover_anchor else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    for (
        profile_name,
        family,
        combo_name,
        score_direction,
        market_regime,
        blend_weight,
        min_percentile,
        rebalance_days,
        rebalance_hysteresis_pct,
        partial_rebalance_ratio,
        execution_impact_budget,
    ) in [
        (
            "h60_value_recovery_regime_alpha",
            "value_recovery",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "0.08",
            "0.40",
            180usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "h60_event_post_curve_regime_alpha",
            "event_post_curve",
            "phase7_quality_event_post_return_curve_overlay_v1",
            ScoreDirection::Descending,
            "quality_event_window_return_sharpe_router_v4",
            "0.08",
            "0.40",
            180usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "h60_relative_strength_regime_alpha",
            "relative_strength",
            "phase7_quality_relative_strength_v1",
            ScoreDirection::Descending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "0.06",
            "0.35",
            160usize,
            "0.01",
            "0.75",
            "impact_turnover_20pct_v1",
        ),
        (
            "h60_value_recovery_confidence35_regime_alpha",
            "value_recovery",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "0.06",
            "0.35",
            160usize,
            "0.01",
            "0.75",
            "impact_turnover_20pct_v1",
        ),
        (
            "h60_value_recovery_confidence45_regime_alpha",
            "value_recovery",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "0.10",
            "0.45",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "h60_event_post_curve_confidence35_regime_alpha",
            "event_post_curve",
            "phase7_quality_event_post_return_curve_overlay_v1",
            ScoreDirection::Descending,
            "quality_event_window_return_sharpe_router_v4",
            "0.06",
            "0.35",
            160usize,
            "0.01",
            "0.75",
            "impact_turnover_20pct_v1",
        ),
        (
            "h60_event_post_curve_confidence45_regime_alpha",
            "event_post_curve",
            "phase7_quality_event_post_return_curve_overlay_v1",
            ScoreDirection::Descending,
            "quality_event_window_return_sharpe_router_v4",
            "0.10",
            "0.45",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
        (
            "h60_relative_strength_confidence45_regime_alpha",
            "relative_strength",
            "phase7_quality_relative_strength_v1",
            ScoreDirection::Descending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "0.10",
            "0.45",
            200usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_long_horizon_regime_alpha_seed(
                low_turnover_anchor.clone(),
                profile_name,
                family,
                combo_name,
                score_direction,
                market_regime,
                blend_weight,
                min_percentile,
                rebalance_days,
                rebalance_hysteresis_pct,
                partial_rebalance_ratio,
                execution_impact_budget,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_prediction_h60_nonlinear_stress_discovery_seed_trials() -> Vec<Value> {
    let confidence_seeds = professional_prediction_confidence_turnover_discovery_seed_trials();
    let low_turnover_anchor = confidence_seeds
        .iter()
        .find(|seed| seed["prediction_confidence_turnover_profile"] == "confidence40_low_turnover")
        .cloned()
        .or_else(|| confidence_seeds.first().cloned());
    let Some(low_turnover_anchor) = low_turnover_anchor else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    for (
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
        execution_impact_budget,
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        candidate_risk_filter,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "h60_fex_relative_strength_rank_fill70",
            "future_excess_return",
            "phase7_quality_relative_strength_v1",
            ScoreDirection::Descending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "relative_strength_alpha_liquidity_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            100usize,
            "0.06",
            "0.90",
            "0.10",
            "0.45",
            180usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            90usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2400usize,
        ),
        (
            "h60_fex_alpha_first_value_recovery_fill60",
            "future_excess_return",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "fillable_gross_95_v1",
            100usize,
            "0.08",
            "0.95",
            "0.08",
            "0.40",
            220usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2200usize,
        ),
        (
            "h60_raex_nonlinear_quality_rank_fill70",
            "risk_adjusted_excess_return",
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            120usize,
            "0.06",
            "0.90",
            "0.10",
            "0.45",
            220usize,
            "0.03",
            "0.60",
            "impact_turnover_15pct_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            90usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2400usize,
        ),
        (
            "h60_fex_event_post_curve_rank_fill70",
            "future_excess_return",
            "phase7_quality_event_post_return_curve_overlay_v1",
            ScoreDirection::Descending,
            "quality_event_window_return_sharpe_router_v4",
            "alpha_first_low_impact_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            100usize,
            "0.08",
            "0.90",
            "0.08",
            "0.40",
            180usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            90usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2400usize,
        ),
        (
            "h60_raex_growth_recovery_rank_fill60",
            "risk_adjusted_excess_return",
            "phase7_growth_recovery_v1",
            ScoreDirection::Descending,
            "quality_mixed_state_risk_memory_router_v14",
            "relative_strength_alpha_liquidity_v1",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "fillable_gross_95_v1",
            80usize,
            "0.08",
            "0.95",
            "0.08",
            "0.40",
            180usize,
            "0.02",
            "0.65",
            "impact_turnover_15pct_v1",
            "twap_15d_v1",
            Decimal::new(8, 2),
            75usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2200usize,
        ),
        (
            "h60_fex_residual_confirm_rank_fill70",
            "future_excess_return",
            "phase7_quality_residual_confirm_10pct_v1",
            ScoreDirection::Ascending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            120usize,
            "0.06",
            "0.90",
            "0.08",
            "0.40",
            220usize,
            "0.03",
            "0.60",
            "impact_turnover_15pct_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            90usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2400usize,
        ),
        (
            "h60_raex_moneyflow_quality_rank_fill60",
            "risk_adjusted_excess_return",
            "phase7_quality_moneyflow_pos_5pct_v1",
            ScoreDirection::Ascending,
            "quality_state_alpha_overlay_selector_v1",
            "alpha_first_low_impact_v1",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "fillable_gross_95_v1",
            100usize,
            "0.08",
            "0.95",
            "0.08",
            "0.40",
            260usize,
            "0.03",
            "0.60",
            "impact_turnover_15pct_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            90usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2200usize,
        ),
        (
            "h60_fex_quality_rank_signal_fidelity",
            "future_excess_return",
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "relative_strength_alpha_liquidity_v1",
            "capacity_stress_participation_blended_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            80usize,
            "0.08",
            "0.80",
            "0.10",
            "0.50",
            260usize,
            "0.03",
            "0.60",
            "impact_turnover_15pct_v1",
            "twap_20d_v1",
            Decimal::new(6, 2),
            100usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2400usize,
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_h60_nonlinear_stress_seed(
                low_turnover_anchor.clone(),
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
                execution_impact_budget,
                execution_schedule_profile,
                daily_target_move_limit_pct,
                max_carry_days,
                candidate_risk_filter,
                risk_contribution_control,
                score_candidate_pool_size,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_prediction_h120_low_impact_stress_discovery_seed_trials() -> Vec<Value> {
    let confidence_seeds = professional_prediction_confidence_turnover_discovery_seed_trials();
    let low_turnover_anchor = confidence_seeds
        .iter()
        .find(|seed| seed["prediction_confidence_turnover_profile"] == "confidence40_low_turnover")
        .cloned()
        .or_else(|| confidence_seeds.first().cloned());
    let Some(low_turnover_anchor) = low_turnover_anchor else {
        return Vec::new();
    };

    let mut seeds = Vec::new();
    for (
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
        execution_schedule_profile,
        daily_target_move_limit_pct,
        max_carry_days,
        candidate_risk_filter,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "h120_fex_quality_alpha_first_fill70",
            "future_excess_return",
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            100usize,
            "0.06",
            "0.90",
            "0.08",
            "0.40",
            240usize,
            "0.03",
            "0.60",
            "twap_20d_v1",
            Decimal::new(5, 2),
            140usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2400usize,
        ),
        (
            "h120_fex_value_recovery_alpha_first_fill70",
            "future_excess_return",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            100usize,
            "0.06",
            "0.90",
            "0.08",
            "0.40",
            300usize,
            "0.03",
            "0.60",
            "twap_20d_v1",
            Decimal::new(5, 2),
            160usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2400usize,
        ),
        (
            "h120_fex_growth_capacity_rank_fill60",
            "future_excess_return",
            "phase7_growth_recovery_v1",
            ScoreDirection::Descending,
            "quality_mixed_state_risk_memory_router_v14",
            "capacity_aware_alpha_liquidity_v1",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "fillable_gross_95_v1",
            80usize,
            "0.08",
            "0.95",
            "0.08",
            "0.35",
            240usize,
            "0.03",
            "0.60",
            "twap_20d_v1",
            Decimal::new(6, 2),
            140usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2200usize,
        ),
        (
            "h120_raex_quality_capacity_rank_fill70",
            "risk_adjusted_excess_return",
            "phase7_financial_quality_v1",
            ScoreDirection::Ascending,
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            120usize,
            "0.06",
            "0.90",
            "0.10",
            "0.45",
            300usize,
            "0.03",
            "0.60",
            "twap_20d_v1",
            Decimal::new(5, 2),
            160usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            2600usize,
        ),
        (
            "h120_raex_value_recovery_alpha_first_fill70",
            "risk_adjusted_excess_return",
            "phase7_quality_value_recovery_confirm_v1",
            ScoreDirection::Ascending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            100usize,
            "0.06",
            "0.90",
            "0.08",
            "0.40",
            300usize,
            "0.03",
            "0.60",
            "twap_20d_v1",
            Decimal::new(5, 2),
            160usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            2400usize,
        ),
        (
            "h120_fex_residual_confirm_alpha_first_fill70",
            "future_excess_return",
            "phase7_quality_residual_confirm_10pct_v1",
            ScoreDirection::Ascending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "alpha_first_low_impact_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            120usize,
            "0.06",
            "0.90",
            "0.08",
            "0.40",
            360usize,
            "0.04",
            "0.55",
            "twap_20d_v1",
            Decimal::new(5, 2),
            180usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            2600usize,
        ),
        (
            "h120_fex_relative_strength_capacity_rank_fill60",
            "future_excess_return",
            "phase7_quality_relative_strength_v1",
            ScoreDirection::Descending,
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "capacity_aware_alpha_liquidity_v1",
            "capacity_stress_participation_alpha_headroom_floor_60_v1",
            "fillable_gross_95_v1",
            80usize,
            "0.08",
            "0.95",
            "0.08",
            "0.35",
            240usize,
            "0.03",
            "0.60",
            "twap_20d_v1",
            Decimal::new(6, 2),
            140usize,
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            2200usize,
        ),
        (
            "h120_raex_cashflow_dividend_capacity_rank_fill70",
            "risk_adjusted_excess_return",
            "phase7_quality_cashflow_dividend_confirm_v1",
            ScoreDirection::Ascending,
            "quality_state_alpha_overlay_selector_v1",
            "capacity_aware_alpha_liquidity_v1",
            "capacity_stress_participation_alpha_headroom_floor_70_v1",
            "stress_fill_gross_98_v1",
            100usize,
            "0.06",
            "0.90",
            "0.08",
            "0.40",
            360usize,
            "0.04",
            "0.55",
            "twap_20d_v1",
            Decimal::new(5, 2),
            180usize,
            "soft_liquidity_low_volatility_low_correlation_v1",
            "soft_single_name_15pct_v1",
            2600usize,
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_prediction_h120_low_impact_stress_seed(
                low_turnover_anchor.clone(),
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
                execution_schedule_profile,
                daily_target_move_limit_pct,
                max_carry_days,
                candidate_risk_filter,
                risk_contribution_control,
                score_candidate_pool_size,
            )],
        );
    }

    seeds
}
