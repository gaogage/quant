//! BC4 策略发现 / seed_generators / sharpe_frontier：sharpe 桥/frontier 种子生成器。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。

use serde_json::{json, Value};

use super::super::profiles::ScoreDirection;
use super::*;

pub(crate) fn professional_risk_memory_bridge_seed_trials() -> Vec<Value> {
    let Some(high_sharpe_boundary) = phase7_current_anchor_high_sharpe_boundary_seed() else {
        return Vec::new();
    };
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return vec![high_sharpe_boundary];
    };

    let mut seeds = Vec::new();
    append_unique_seeds(
        &mut seeds,
        vec![return_anchor.clone(), high_sharpe_boundary.clone()],
    );

    for lookback_days in [120, 140, 150, 160, 170, 180] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                high_sharpe_boundary.clone(),
                &format!("risk_budget_{lookback_days}"),
                lookback_days,
            )],
        );
    }

    let valuation45 = with_event_combo_gate_seed_with_min_score(
        high_sharpe_boundary.clone(),
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    for lookback_days in [140, 150, 160] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                valuation45.clone(),
                &format!("valuation45_rb{lookback_days}"),
                lookback_days,
            )],
        );
    }

    for (position_profile, max_position_pct, correlation_profile, max_pairwise_correlation) in [
        ("maxpos145", "0.145", "corr65", "0.65"),
        ("maxpos15", "0.15", "corr65", "0.65"),
        ("maxpos14", "0.14", "corr675", "0.675"),
    ] {
        for lookback_days in [140, 150, 160] {
            append_unique_seeds(
                &mut seeds,
                vec![with_risk_budget_lookback_seed(
                    with_position_shape_seed(
                        high_sharpe_boundary.clone(),
                        &format!("{position_profile}_{correlation_profile}_rb{lookback_days}"),
                        max_position_pct,
                        max_pairwise_correlation,
                    ),
                    &format!("risk_budget_{lookback_days}"),
                    lookback_days,
                )],
            );
        }
    }

    seeds.into_iter().take(16).collect()
}

pub(crate) fn professional_position_sharpe_return_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, regime_suffix) in [
        ("quality_event_window_return_sharpe_router_v3", "router_v3"),
        ("quality_event_window_return_sharpe_router_v4", "router_v4"),
    ] {
        for (position_profile, max_position_pct, correlation_profile, max_pairwise_correlation) in [
            ("maxpos14", "0.14", "corr65", "0.65"),
            ("maxpos145", "0.145", "corr65", "0.65"),
            ("maxpos15", "0.15", "corr675", "0.675"),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_risk_budget_lookback_seed(
                    with_position_shape_seed(
                        with_volatility_profile_seed(
                            with_event_sleeve_seed(
                                valuation45.clone(),
                                regime,
                                &format!(
                                    "{regime_suffix}_{position_profile}_{correlation_profile}"
                                ),
                            ),
                            "vol120_16_50_100",
                            "0.16",
                            120,
                            "0.50",
                            "1",
                        ),
                        &format!("{position_profile}_{correlation_profile}_{regime_suffix}_rb160"),
                        max_position_pct,
                        max_pairwise_correlation,
                    ),
                    &format!("bp_rb160_{regime_suffix}_{position_profile}_{correlation_profile}"),
                    160,
                )],
            );
        }
    }

    for (lookback_days, position_profile, max_position_pct, max_pairwise_correlation) in [
        (150, "maxpos14", "0.14", "0.65"),
        (170, "maxpos14", "0.14", "0.65"),
        (180, "maxpos145", "0.145", "0.65"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                with_position_shape_seed(
                    with_event_sleeve_seed(
                        valuation45.clone(),
                        "quality_event_window_return_sharpe_router_v4",
                        &format!("router_v4_rb{lookback_days}"),
                    ),
                    &format!("{position_profile}_corr65_rb{lookback_days}"),
                    max_position_pct,
                    max_pairwise_correlation,
                ),
                &format!("bp_rb{lookback_days}_router_v4"),
                lookback_days,
            )],
        );
    }

    for (vol_profile, target_pct, min_exposure) in [
        ("vol120_17_52_100", "0.17", "0.52"),
        ("vol120_18_55_100", "0.18", "0.55"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                with_position_shape_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            valuation45.clone(),
                            "quality_event_window_return_sharpe_router_v4",
                            &format!("router_v4_maxpos14_corr65_{vol_profile}"),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("maxpos14_corr65_router_v4_{vol_profile}"),
                    "0.14",
                    "0.65",
                ),
                &format!("bp_rb160_router_v4_{vol_profile}"),
                160,
            )],
        );
    }

    append_unique_seeds(
        &mut seeds,
        vec![with_risk_budget_lookback_seed(
            with_position_shape_seed(
                with_volatility_profile_seed(
                    with_event_sleeve_seed(
                        valuation45,
                        "quality_event_window_return_sharpe_router_v3",
                        "router_v3_maxpos14_corr65_vol17",
                    ),
                    "vol120_17_52_100",
                    "0.17",
                    120,
                    "0.52",
                    "1",
                ),
                "maxpos14_corr65_router_v3_vol17",
                "0.14",
                "0.65",
            ),
            "bp_rb160_router_v3_vol17",
            160,
        )],
    );

    seeds.into_iter().take(12).collect()
}

pub(crate) fn professional_moderate_position_sharpe_return_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, regime_suffix) in [
        ("quality_event_window_return_sharpe_router_v3", "router_v3"),
        ("quality_event_window_return_sharpe_router_v4", "router_v4"),
    ] {
        for (vol_profile, target_pct, min_exposure) in [
            ("vol120_15_48_100", "0.15", "0.48"),
            ("vol120_16_50_100", "0.16", "0.50"),
        ] {
            for (
                position_profile,
                max_position_pct,
                correlation_profile,
                max_pairwise_correlation,
            ) in [
                ("maxpos16", "0.16", "corr70", "0.70"),
                ("maxpos16", "0.16", "corr725", "0.725"),
                ("maxpos17", "0.17", "corr725", "0.725"),
            ] {
                append_unique_seeds(
                    &mut seeds,
                    vec![with_risk_budget_lookback_seed(
                        with_position_shape_seed(
                            with_volatility_profile_seed(
                                with_event_sleeve_seed(
                                    valuation45.clone(),
                                    regime,
                                    &format!(
                                        "{regime_suffix}_{position_profile}_{correlation_profile}_{vol_profile}"
                                    ),
                                ),
                                vol_profile,
                                target_pct,
                                120,
                                min_exposure,
                                "1",
                            ),
                            &format!(
                                "{position_profile}_{correlation_profile}_{regime_suffix}_{vol_profile}"
                            ),
                            max_position_pct,
                            max_pairwise_correlation,
                        ),
                        &format!("bp_rb160_{regime_suffix}_{position_profile}_{correlation_profile}_{vol_profile}"),
                        160,
                    )],
                );
            }
        }
    }

    seeds.into_iter().take(12).collect()
}

pub(crate) fn professional_correlation_frontier_sharpe_return_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (
        position_profile,
        max_position_pct,
        correlation_profile,
        max_pairwise_correlation,
        vol_profile,
        target_pct,
        min_exposure,
    ) in [
        (
            "maxpos16",
            "0.16",
            "corr705",
            "0.705",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr71",
            "0.71",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr715",
            "0.715",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr72",
            "0.72",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr705",
            "0.705",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr71",
            "0.71",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr715",
            "0.715",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr72",
            "0.72",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr705",
            "0.705",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
        (
            "maxpos16",
            "0.16",
            "corr71",
            "0.71",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
        (
            "maxpos165",
            "0.165",
            "corr715",
            "0.715",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
        (
            "maxpos165",
            "0.165",
            "corr72",
            "0.72",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                with_position_shape_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            valuation45.clone(),
                            "quality_event_window_return_sharpe_router_v4",
                            &format!(
                                "router_v4_{position_profile}_{correlation_profile}_{vol_profile}"
                            ),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!(
                        "{}_{}_router_v4_{}",
                        position_profile, correlation_profile, vol_profile
                    ),
                    max_position_pct,
                    max_pairwise_correlation,
                ),
                &format!(
                    "bp_rb160_router_v4_{}_{}_{}",
                    position_profile, correlation_profile, vol_profile
                ),
                160,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_correlation_threshold_sharpe_return_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (
        position_profile,
        max_position_pct,
        correlation_profile,
        max_pairwise_correlation,
        vol_profile,
        target_pct,
        min_exposure,
    ) in [
        (
            "maxpos16",
            "0.16",
            "corr706",
            "0.706",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr707",
            "0.707",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr708",
            "0.708",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr709",
            "0.709",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr706",
            "0.706",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr707",
            "0.707",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr708",
            "0.708",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos165",
            "0.165",
            "corr709",
            "0.709",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "maxpos16",
            "0.16",
            "corr706",
            "0.706",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
        (
            "maxpos16",
            "0.16",
            "corr707",
            "0.707",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
        (
            "maxpos165",
            "0.165",
            "corr708",
            "0.708",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
        (
            "maxpos165",
            "0.165",
            "corr709",
            "0.709",
            "vol120_17_52_100",
            "0.17",
            "0.52",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                with_position_shape_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            valuation45.clone(),
                            "quality_event_window_return_sharpe_router_v4",
                            &format!(
                                "router_v4_{position_profile}_{correlation_profile}_{vol_profile}"
                            ),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!(
                        "{}_{}_router_v4_{}",
                        position_profile, correlation_profile, vol_profile
                    ),
                    max_position_pct,
                    max_pairwise_correlation,
                ),
                &format!(
                    "bp_rb160_router_v4_{}_{}_{}",
                    position_profile, correlation_profile, vol_profile
                ),
                160,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_soft_risk_frontier_sharpe_return_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure) in [
        (
            "quality_event_window_return_sharpe_router_v4",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_event_window_return_sharpe_router_v4",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_event_window_return_sharpe_router_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_event_window_return_sharpe_router_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
    ] {
        for (risk_contribution_control, candidate_risk_filter) in [
            ("soft_single_name_20pct_v1", "off"),
            ("soft_single_name_15pct_v1", "low_volatility_v1"),
            (
                "soft_single_name_20pct_v1",
                "low_volatility_low_correlation_v1",
            ),
        ] {
            let mut seed = with_risk_budget_lookback_seed(
                with_volatility_profile_seed(
                    with_event_sleeve_seed(
                        valuation45.clone(),
                        regime,
                        &format!("{regime}_{vol_profile}_{risk_contribution_control}_{candidate_risk_filter}"),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("bp_rb160_{regime}_{vol_profile}"),
                160,
            );
            seed["risk_contribution_control"] = json!(risk_contribution_control);
            seed["candidate_risk_filter"] = json!(candidate_risk_filter);
            append_unique_seeds(&mut seeds, vec![seed]);
        }
    }

    seeds.into_iter().take(12).collect()
}

pub(crate) fn professional_mixed_state_risk_memory_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure) in [
        (
            "quality_mixed_event_state_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v2",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}_mixed_risk_memory"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_mixed_risk_memory"),
            160,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_mixed_state_risk_memory_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure) in [
        (
            "quality_mixed_state_risk_memory_router_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v4",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v5",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v6",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v4",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v5",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}_mixed_risk_frontier"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_mixed_risk_frontier"),
            160,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_mixed_state_risk_memory_fine_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure) in [
        (
            "quality_mixed_state_risk_memory_router_v7",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v8",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v9",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v10",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v7",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v8",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v9",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v4",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}_mixed_risk_fine_frontier"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_mixed_risk_fine_frontier"),
            160,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_mixed_state_exposure_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure) in [
        (
            "quality_mixed_state_risk_memory_router_v4",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v11",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v12",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v13",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_state_risk_memory_router_v11",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v12",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}_mixed_exposure_frontier"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_mixed_exposure_frontier"),
            160,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_mixed_state_orthogonal_alpha_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure) in [
        (
            "quality_mixed_orthogonal_alpha_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_alpha_selector_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_alpha_selector_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}_mixed_orthogonal_alpha"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_mixed_orthogonal_alpha"),
            160,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_candidate_filter_alpha_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (
        regime,
        risk_contribution_control,
        candidate_risk_filter,
        vol_profile,
        target_pct,
        min_exposure,
    ) in [
        (
            "quality_state_alpha_overlay_selector_v1",
            "soft_single_name_20pct_v1",
            "low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_overlay_selector_v2",
            "soft_single_name_20pct_v1",
            "low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "soft_single_name_20pct_v1",
            "low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_alpha_selector_v2",
            "soft_single_name_20pct_v1",
            "low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_alpha_selector_v1",
            "soft_single_name_15pct_v1",
            "low_volatility_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "soft_single_name_20pct_v1",
            "off",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "soft_single_name_20pct_v1",
            "low_volatility_low_correlation_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "soft_single_name_20pct_v1",
            "off",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!(
                        "{regime}_{vol_profile}_{risk_contribution_control}_{candidate_risk_filter}_candidate_filter_alpha_bridge"
                    ),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_candidate_filter_alpha_bridge"),
            160,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["candidate_risk_filter"] = json!(candidate_risk_filter);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_soft_candidate_filter_alpha_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (
        regime,
        risk_contribution_control,
        candidate_risk_filter,
        vol_profile,
        target_pct,
        min_exposure,
    ) in [
        (
            "quality_state_alpha_overlay_selector_v1",
            "soft_single_name_20pct_v1",
            "soft_low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "soft_single_name_20pct_v1",
            "soft_low_volatility_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "soft_single_name_20pct_v1",
            "soft_low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_alpha_selector_v1",
            "soft_single_name_20pct_v1",
            "soft_low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_alpha_selector_v2",
            "soft_single_name_20pct_v1",
            "soft_low_volatility_low_correlation_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_alpha_selector_v3",
            "soft_single_name_20pct_v1",
            "soft_low_volatility_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "soft_single_name_20pct_v1",
            "soft_low_volatility_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "soft_single_name_20pct_v1",
            "off",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!(
                        "{regime}_{vol_profile}_{risk_contribution_control}_{candidate_risk_filter}_soft_candidate_filter_alpha_bridge"
                    ),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_soft_candidate_filter_alpha_bridge"),
            160,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["candidate_risk_filter"] = json!(candidate_risk_filter);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_sharpe_bridge_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, vol_profile, target_pct, min_exposure, risk_budget_lookback_days) in [
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
        ),
        (
            "quality_state_sharpe_bridge_router_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
        ),
        (
            "quality_state_sharpe_bridge_router_v3",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            160,
        ),
        (
            "quality_state_sharpe_bridge_router_v1",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}_sharpe_bridge_frontier"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!(
                "bp_rb{risk_budget_lookback_days}_{regime}_{vol_profile}_sharpe_bridge_frontier"
            ),
            risk_budget_lookback_days,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_annual_sharpe_floor_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        regime_suffix,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        position_shape,
        max_position_pct,
        max_pairwise_correlation,
        risk_contribution_control,
    ) in [
        (
            "quality_state_alpha_overlay_selector_v1",
            "overlay_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "bridge_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos16_corr75",
            "0.16",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "risk_memory_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            160,
            "maxpos16_corr75",
            "0.16",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "bridge_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
            "soft_single_name_20pct_v1",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "bridge_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            160,
            "maxpos16_corr75",
            "0.16",
            "0.75",
            "off",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
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
                            "{regime_suffix}_{valuation_profile}_{vol_profile}_annual_sharpe_floor"
                        ),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("{position_shape}_{regime_suffix}_{vol_profile}"),
                max_position_pct,
                max_pairwise_correlation,
            ),
            &format!(
                "bp_rb{risk_budget_lookback_days}_{regime_suffix}_{valuation_profile}_{vol_profile}"
            ),
            risk_budget_lookback_days,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_risk_memory_relaxed_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
    ) in [
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
        ),
        (
            "quality_mixed_state_risk_memory_router_v17",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
        ),
        (
            "quality_mixed_state_risk_memory_router_v17",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
        ),
        (
            "quality_mixed_state_risk_memory_router_v18",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
        ),
        (
            "quality_mixed_state_risk_memory_router_v18",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
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
                    &format!("{regime}_{valuation_profile}_{vol_profile}_relaxed_frontier"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}_{vol_profile}"),
            risk_budget_lookback_days,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_sharpe_floor_auto_discovery_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        position_shape,
        max_position_pct,
        max_pairwise_correlation,
    ) in [
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v18",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "maxpos15_corr75",
            "0.15",
            "0.75",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
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
                        &format!("{regime}_{valuation_profile}_{vol_profile}_sharpe_floor_auto"),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("{position_shape}_{regime}_{vol_profile}"),
                max_position_pct,
                max_pairwise_correlation,
            ),
            &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}_{vol_profile}"),
            risk_budget_lookback_days,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_nonlinear_alpha_auto_discovery_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        sharpe_profile,
        sharpe_start,
        sharpe_full,
        sharpe_lookback,
        sharpe_min_exposure,
    ) in [
        (
            "quality_nonlinear_alpha_router_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "off",
            "0",
            "0",
            0,
            "0",
        ),
        (
            "quality_nonlinear_alpha_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "off",
            "0",
            "0",
            0,
            "0",
        ),
        (
            "quality_nonlinear_alpha_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "off",
            "0",
            "0",
            0,
            "0",
        ),
        (
            "quality_nonlinear_alpha_router_v2",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "off",
            "0",
            "0",
            0,
            "0",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "roll_sharpe120_050_neg10_60",
            "0.50",
            "-0.10",
            120,
            "0.60",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
        ),
        (
            "quality_nonlinear_alpha_router_v1",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            180,
            "roll_sharpe180_060_000_60",
            "0.60",
            "0.00",
            180,
            "0.60",
        ),
        (
            "quality_nonlinear_alpha_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe120_060_000_55",
            "0.60",
            "0.00",
            120,
            "0.55",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v1",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "off",
            "0",
            "0",
            0,
            "0",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
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
                            "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_nonlinear"
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
            &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}_{vol_profile}"),
            risk_budget_lookback_days,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        if sharpe_profile != "off" {
            seed = with_sharpe_profile_seed(
                seed,
                sharpe_profile,
                sharpe_start,
                sharpe_full,
                sharpe_lookback,
                sharpe_min_exposure,
            );
        } else {
            seed["portfolio_sharpe_control"] = json!("off");
        }
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_nonlinear_sharpe_return_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        max_position_pct,
        max_pairwise_correlation,
        sharpe_profile,
        sharpe_start,
        sharpe_full,
        sharpe_lookback,
        sharpe_min_exposure,
    ) in [
        (
            "quality_nonlinear_alpha_risk_memory_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "0.15",
            "0.75",
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "0.15",
            "0.75",
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            160,
            "0.15",
            "0.75",
            "roll_sharpe120_050_neg10_60",
            "0.50",
            "-0.10",
            120,
            "0.60",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "0.15",
            "0.75",
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "0.15",
            "0.75",
            "roll_sharpe120_050_neg10_55",
            "0.50",
            "-0.10",
            120,
            "0.55",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "0.15",
            "0.75",
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "0.15",
            "0.75",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            160,
            "0.16",
            "0.75",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_17_52_100",
            "0.17",
            "0.52",
            180,
            "0.16",
            "0.75",
            "roll_sharpe180_060_000_60",
            "0.60",
            "0.00",
            180,
            "0.60",
        ),
        (
            "quality_nonlinear_alpha_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            180,
            "0.15",
            "0.75",
            "roll_sharpe120_060_000_55",
            "0.60",
            "0.00",
            120,
            "0.55",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
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
                            "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_bridge"
                        ),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("maxpos{max_position_pct}_corr{max_pairwise_correlation}_{regime}"),
                max_position_pct,
                max_pairwise_correlation,
            ),
            &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}_{vol_profile}"),
            risk_budget_lookback_days,
        );
        seed["rebalance_smoothing_profile"] = json!("off");
        seed["rebalance_hysteresis_pct"] = json!("0");
        seed["partial_rebalance_ratio"] = json!("1");
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        seed = with_sharpe_profile_seed(
            seed,
            sharpe_profile,
            sharpe_start,
            sharpe_full,
            sharpe_lookback,
            sharpe_min_exposure,
        );
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_high_sharpe_boundary_return_bridge_seed_trials() -> Vec<Value> {
    let Some(boundary) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        max_position_pct,
        max_pairwise_correlation,
    ) in [
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            170,
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom35",
            "0.35",
            "vol120_155_49_100",
            "0.155",
            "0.49",
            180,
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "0.15",
            "0.75",
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "0.15",
            "0.75",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "0.15",
            "0.75",
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "0.15",
            "0.75",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            170,
            "0.15",
            "0.75",
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_155_49_100",
            "0.155",
            "0.49",
            170,
            "0.16",
            "0.75",
        ),
    ] {
        let seed = with_risk_budget_lookback_seed(
            with_position_shape_seed(
                with_volatility_profile_seed(
                    with_event_sleeve_seed(
                        with_event_combo_gate_seed_with_min_score(
                            boundary.clone(),
                            valuation_profile,
                            "phase7_valuation_v1",
                            "exclude_negative",
                            valuation_min_score,
                            "0",
                            ScoreDirection::Descending,
                        ),
                        regime,
                        &format!("{regime}_{valuation_profile}_{vol_profile}_boundary_return_bridge"),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("{regime}_{vol_profile}_maxpos{max_position_pct}_corr{max_pairwise_correlation}"),
                max_position_pct,
                max_pairwise_correlation,
            ),
            &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}_{vol_profile}"),
            risk_budget_lookback_days,
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

pub(crate) fn professional_high_sharpe_micro_frontier_seed_trials() -> Vec<Value> {
    let Some(boundary) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        sharpe_profile,
        sharpe_start,
        sharpe_full,
        sharpe_lookback,
        sharpe_min_exposure,
        candidate_risk_filter,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            "soft_single_name_20pct_v1",
            400,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_155_49_100",
            "0.155",
            "0.49",
            180,
            "roll_sharpe120_050_neg10_60",
            "0.50",
            "-0.10",
            120,
            "0.60",
            "soft_low_volatility_v1",
            "off",
            800,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            170,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe120_050_neg10_60",
            "0.50",
            "-0.10",
            120,
            "0.60",
            "off",
            "soft_single_name_20pct_v1",
            400,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_155_49_100",
            "0.155",
            "0.49",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_low_volatility_low_correlation_v1",
            "off",
            800,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            "off",
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_low_volatility_v1",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe120_050_neg10_60",
            "0.50",
            "-0.10",
            120,
            "0.60",
            "soft_low_volatility_low_correlation_v1",
            "soft_single_name_20pct_v1",
            800,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "off",
            "soft_single_name_20pct_v1",
            400,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            170,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "off",
            "off",
            500,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe120_050_neg10_60",
            "0.50",
            "-0.10",
            120,
            "0.60",
            "off",
            "off",
            650,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
            170,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "soft_low_volatility_v1",
            "off",
            400,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_low_volatility_low_correlation_v1",
            "off",
            800,
        ),
    ] {
        let mut seed = with_sharpe_profile_seed(
            with_risk_budget_lookback_seed(
                with_volatility_profile_seed(
                    with_event_sleeve_seed(
                        with_event_combo_gate_seed_with_min_score(
                            boundary.clone(),
                            valuation_profile,
                            "phase7_valuation_v1",
                            "exclude_negative",
                            valuation_min_score,
                            "0",
                            ScoreDirection::Descending,
                        ),
                        regime,
                        &format!(
                            "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_micro"
                        ),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!(
                    "bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}_{vol_profile}"
                ),
                risk_budget_lookback_days,
            ),
            sharpe_profile,
            sharpe_start,
            sharpe_full,
            sharpe_lookback,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!(candidate_risk_filter);
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_return_alpha_sharpe_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        valuation_profile,
        valuation_min_score,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        sharpe_profile,
        sharpe_start,
        sharpe_full,
        sharpe_lookback,
        sharpe_min_exposure,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            170,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "off",
            500,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_mixed_orthogonal_risk_memory_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_60",
            "0.50",
            "-0.10",
            180,
            "0.60",
            "off",
            500,
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_state_sharpe_bridge_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "quality_nonlinear_alpha_risk_memory_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
    ] {
        let mut seed = with_sharpe_profile_seed(
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
                                "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_return_alpha_bridge"
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
            sharpe_profile,
            sharpe_start,
            sharpe_full,
            sharpe_lookback,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

pub(crate) fn professional_high_sharpe_return_micro_bridge_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
        regime,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
        sharpe_profile,
        sharpe_min_exposure,
        score_candidate_pool_size,
    ) in [
        (
            "quality_frontier_regime_bridge_router_v6",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_68",
            "0.68",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_68",
            "0.68",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "vol120_147_475_100",
            "0.147",
            "0.475",
            170,
            "roll_sharpe180_050_neg10_70",
            "0.70",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            170,
            "roll_sharpe180_050_neg10_70",
            "0.70",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "vol120_147_475_100",
            "0.147",
            "0.475",
            180,
            "roll_sharpe180_050_neg10_70",
            "0.70",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_72",
            "0.72",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_68",
            "0.68",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_68",
            "0.68",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "vol120_147_475_100",
            "0.147",
            "0.475",
            170,
            "roll_sharpe180_050_neg10_70",
            "0.70",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            170,
            "roll_sharpe180_050_neg10_70",
            "0.70",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "vol120_147_475_100",
            "0.147",
            "0.475",
            180,
            "roll_sharpe180_050_neg10_70",
            "0.70",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            180,
            "roll_sharpe180_050_neg10_72",
            "0.72",
            650,
        ),
    ] {
        let mut seed = with_sharpe_profile_seed(
            with_risk_budget_lookback_seed(
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
                            regime,
                            &format!(
                                "{regime}_valuation45_{vol_profile}_{sharpe_profile}_micro_bridge"
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
                &format!("bp_rb{risk_budget_lookback_days}_{regime}_valuation45"),
                risk_budget_lookback_days,
            ),
            sharpe_profile,
            "0.50",
            "-0.10",
            180,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!("off");
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}
