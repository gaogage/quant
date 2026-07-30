//! BC4 策略发现 / seed_generators / event：事件驱动 种子生成器。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。

use serde_json::{json, Value};

use super::super::profiles::ScoreDirection;
use super::*;

pub(crate) fn professional_event_conditioned_sharpe_seed_trials() -> Vec<Value> {
    let anchors = phase7_u2_exact_anchor_seed_trials();
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, anchors.clone());

    for seed in &anchors {
        append_unique_seeds(
            &mut seeds,
            vec![
                with_event_surprise_gate_seed(
                    seed.clone(),
                    "event_surprise_boost_pos_3pct",
                    "boost_positive",
                    "0.03",
                ),
                with_event_surprise_gate_seed(
                    seed.clone(),
                    "event_surprise_boost_pos_5pct",
                    "boost_positive",
                    "0.05",
                ),
                with_event_surprise_gate_seed(
                    seed.clone(),
                    "event_surprise_exclude_negative",
                    "exclude_negative",
                    "0",
                ),
                with_event_surprise_gate_seed(
                    seed.clone(),
                    "event_surprise_require_positive",
                    "require_positive",
                    "0",
                ),
                with_event_window_gate_seed(
                    seed.clone(),
                    "event_window_boost_pos_3pct",
                    "boost_positive",
                    "0.03",
                ),
                with_event_window_gate_seed(
                    seed.clone(),
                    "event_window_exclude_negative",
                    "exclude_negative",
                    "0",
                ),
                retarget_seed_combo(seed.clone(), "phase7_quality_event_surprise_confirm_v1"),
                retarget_seed_combo(seed.clone(), "phase7_quality_event_confirm_v1"),
            ],
        );
    }

    seeds
}

pub(crate) fn professional_second_alpha_source_seed_trials() -> Vec<Value> {
    let style_seeds = professional_style_risk_budget_seed_trials();
    let mut seeds = Vec::new();
    let combos = [
        "phase7_quality_value_recovery_confirm_v1",
        "phase7_financial_quality_v1",
        "phase7_quality_moneyflow_pos_5pct_v1",
        "phase7_blend_quality_growth_v1",
        "phase7_blend_recovery_tilt_v1",
    ];
    let direction_sweep_combos = [
        "phase7_quality_value_recovery_confirm_v1",
        "phase7_financial_quality_v1",
        "phase7_quality_moneyflow_pos_5pct_v1",
        "phase7_blend_quality_growth_v1",
        "phase7_blend_recovery_tilt_v1",
    ];

    if let Some(priority_seed) = style_seeds
        .iter()
        .find(|seed| {
            seed["market_regime"] == "quality_bear_window_guard_v2"
                && seed["style_risk_budget"] == "liquidity_volatility_balanced_v1"
                && seed["portfolio_volatility_control"] == "vol120_22_65_100"
                && seed["stop_loss_pct"] == "0.075"
                && seed["reentry_cooldown_days"] == 30
        })
        .or_else(|| style_seeds.first())
    {
        seeds.push(retarget_seed_combo(
            set_seed_score_direction(priority_seed.clone(), ScoreDirection::Descending),
            "phase7_quality_value_recovery_confirm_v1",
        ));
        for (profile_name, mode, boost_weight) in [
            ("event_window_boost_pos_5pct", "boost_positive", "0.05"),
            ("event_window_exclude_negative", "exclude_negative", "0"),
            ("event_window_require_positive", "require_positive", "0"),
        ] {
            seeds.push(with_event_window_gate_seed(
                retarget_seed_combo(
                    set_seed_score_direction(priority_seed.clone(), ScoreDirection::Descending),
                    "phase7_quality_value_recovery_confirm_v1",
                ),
                profile_name,
                mode,
                boost_weight,
            ));
        }
        seeds.push(retarget_seed_combo(
            set_seed_score_direction(priority_seed.clone(), ScoreDirection::Ascending),
            "phase7_quality_value_recovery_confirm_v1",
        ));
        for combo_name in direction_sweep_combos {
            seeds.push(retarget_seed_combo(
                set_seed_score_direction(priority_seed.clone(), ScoreDirection::Descending),
                combo_name,
            ));
            seeds.push(retarget_seed_combo(
                set_seed_score_direction(priority_seed.clone(), ScoreDirection::Ascending),
                combo_name,
            ));
        }
    }

    for seed in style_seeds {
        for combo_name in combos {
            if direction_sweep_combos.contains(&combo_name) {
                for direction in [ScoreDirection::Descending, ScoreDirection::Ascending] {
                    let alpha_seed = retarget_seed_combo(
                        set_seed_score_direction(seed.clone(), direction),
                        combo_name,
                    );
                    if !seeds.contains(&alpha_seed) {
                        seeds.push(alpha_seed);
                    }
                }
            } else {
                let alpha_seed = retarget_seed_combo(seed.clone(), combo_name);
                if !seeds.contains(&alpha_seed) {
                    seeds.push(alpha_seed);
                }
            }
        }
    }

    seeds
}

pub(crate) fn professional_residual_quality_seed_trials() -> Vec<Value> {
    let anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let style_seeds = professional_style_risk_budget_seed_trials();
    let mut seeds = Vec::new();

    if let Some(priority_seed) = anchor_seeds.first() {
        append_unique_seeds(
            &mut seeds,
            vec![
                retarget_seed_combo(
                    set_seed_score_direction(priority_seed.clone(), ScoreDirection::Ascending),
                    "phase7_industry_residual_quality_v1",
                ),
                retarget_seed_combo(
                    set_seed_score_direction(priority_seed.clone(), ScoreDirection::Descending),
                    "phase7_industry_residual_quality_v1",
                ),
                retarget_seed_combo(priority_seed.clone(), "phase7_financial_quality_v1"),
            ],
        );
    }

    for seed in style_seeds {
        for direction in [ScoreDirection::Ascending, ScoreDirection::Descending] {
            append_unique_seeds(
                &mut seeds,
                vec![retarget_seed_combo(
                    set_seed_score_direction(seed.clone(), direction),
                    "phase7_industry_residual_quality_v1",
                )],
            );
        }
        append_unique_seeds(
            &mut seeds,
            vec![retarget_seed_combo(seed, "phase7_financial_quality_v1")],
        );
    }

    seeds
}

pub(crate) fn professional_residual_overlay_sharpe_seed_trials() -> Vec<Value> {
    let anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let overlay_combos = [
        "phase7_quality_residual_confirm_5pct_v1",
        "phase7_quality_residual_confirm_10pct_v1",
    ];
    let mut seeds = Vec::new();

    if let Some(priority_seed) = anchor_seeds.first() {
        append_unique_seeds(&mut seeds, vec![priority_seed.clone()]);
        for combo_name in overlay_combos {
            append_unique_seeds(
                &mut seeds,
                vec![retarget_seed_combo(priority_seed.clone(), combo_name)],
            );
        }
        for combo_name in overlay_combos {
            append_unique_seeds(
                &mut seeds,
                vec![with_style_risk_budget_seed(
                    retarget_seed_combo(priority_seed.clone(), combo_name),
                    "liquidity_volatility_balanced_v1",
                )],
            );
        }
    }

    for seed in anchor_seeds.iter().skip(1) {
        append_unique_seeds(&mut seeds, vec![seed.clone()]);
        for combo_name in overlay_combos {
            append_unique_seeds(
                &mut seeds,
                vec![retarget_seed_combo(seed.clone(), combo_name)],
            );
        }
    }

    seeds
}

pub(crate) fn professional_conditioned_second_alpha_seed_trials() -> Vec<Value> {
    let anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let gate_specs = [
        (
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
        ),
        (
            "valuation_require_top40",
            "phase7_valuation_v1",
            "require_positive",
            "0.60",
        ),
        (
            "residual_require_top40",
            "phase7_industry_residual_quality_v1",
            "require_positive",
            "0.60",
        ),
        (
            "moneyflow_require_top40",
            "phase7_moneyflow_v1",
            "require_positive",
            "0.60",
        ),
    ];
    let mut seeds = Vec::new();

    for seed in &anchor_seeds {
        append_unique_seeds(&mut seeds, vec![seed.clone()]);
        for (profile_name, combo_name, mode, min_score) in gate_specs {
            append_unique_seeds(
                &mut seeds,
                vec![with_event_combo_gate_seed_with_min_score(
                    seed.clone(),
                    profile_name,
                    combo_name,
                    mode,
                    min_score,
                    "0",
                    ScoreDirection::Descending,
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_valuation_guard_sharpe_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol20_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_20_60_100",
        "0.20",
        120,
        "0.60",
        "1",
    );
    let vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let anchors = [vol22_anchor, vol20_anchor, vol18_anchor];
    let valuation_thresholds = [
        ("valuation_exclude_bottom40", "0.40"),
        ("valuation_exclude_bottom35", "0.35"),
        ("valuation_exclude_bottom30", "0.30"),
        ("valuation_exclude_bottom45", "0.45"),
        ("valuation_exclude_bottom25", "0.25"),
        ("valuation_exclude_bottom50", "0.50"),
    ];
    let mut seeds = Vec::new();

    for seed in &anchors {
        append_unique_seeds(&mut seeds, vec![seed.clone()]);
    }
    for (profile_name, min_score) in valuation_thresholds {
        for seed in &anchors {
            append_unique_seeds(
                &mut seeds,
                vec![with_event_combo_gate_seed_with_min_score(
                    seed.clone(),
                    profile_name,
                    "phase7_valuation_v1",
                    "exclude_negative",
                    min_score,
                    "0",
                    ScoreDirection::Descending,
                )],
            );
        }
    }

    seeds
}

pub(crate) fn professional_event_regime_sleeve_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            value40_anchor.clone(),
            "risk_budget",
            120,
        )],
    );
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(
                with_market_regime_seed(value40_anchor.clone(), policy),
                "risk_budget",
                120,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_window_sleeve_weight_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            value40_anchor.clone(),
            "risk_budget",
            120,
        )],
    );
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(
                with_market_regime_seed(value40_anchor.clone(), policy),
                "risk_budget",
                120,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_window_sleeve_upper_bound_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            value40_anchor.clone(),
            "risk_budget",
            120,
        )],
    );
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(
                with_market_regime_seed(value40_anchor.clone(), policy),
                "risk_budget",
                120,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_window_regime_placement_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            value40_anchor.clone(),
            "risk_budget",
            120,
        )],
    );
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(
                with_market_regime_seed(value40_anchor.clone(), policy),
                "risk_budget",
                120,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_window_decay_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            value40_anchor.clone(),
            "risk_budget",
            120,
        )],
    );
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(
                with_market_regime_seed(value40_anchor.clone(), policy),
                "risk_budget",
                120,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_quality_segment_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            value40_anchor.clone(),
            "risk_budget",
            120,
        )],
    );
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(
                with_market_regime_seed(value40_anchor.clone(), policy),
                "risk_budget",
                120,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_surprise_nonlinear_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let stress_regimes = ["bear", "high_volatility"];
    let mut seeds = Vec::new();

    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            value40_anchor.clone(),
            "risk_budget",
            120,
        )],
    );
    append_unique_seeds(
        &mut seeds,
        vec![with_portfolio_method_seed(
            retarget_seed_combo(
                value40_anchor.clone(),
                "phase7_quality_event_surprise_confirm_v1",
            ),
            "risk_budget",
            120,
        )],
    );

    for (profile_name, mode, boost_weight) in [
        (
            "event_surprise_boost_pos_3pct_stress_only",
            "boost_positive",
            "0.03",
        ),
        (
            "event_surprise_boost_pos_5pct_stress_only",
            "boost_positive",
            "0.05",
        ),
        (
            "event_surprise_exclude_negative_stress_only",
            "exclude_negative",
            "0",
        ),
        (
            "event_surprise_require_positive_stress_only",
            "require_positive",
            "0",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_combo_gate_seed_active_in_with_boost(
                with_portfolio_method_seed(value40_anchor.clone(), "risk_budget", 120),
                profile_name,
                "phase7_event_surprise_v1",
                mode,
                boost_weight,
                ScoreDirection::Descending,
                &stress_regimes,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_strength_segment_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let anchor = with_portfolio_method_seed(value40_anchor, "risk_budget", 120);
    let mut seeds = Vec::new();

    for (profile_name, combo_name, min_score) in [
        (
            "event_window_require_strong_p75",
            "phase7_event_window_earnings_v1",
            "0.38",
        ),
        (
            "event_window_require_strong_p90",
            "phase7_event_window_earnings_v1",
            "0.66",
        ),
        (
            "event_surprise_require_strong_p75",
            "phase7_event_surprise_v1",
            "0.35",
        ),
        (
            "event_surprise_require_strong_p90",
            "phase7_event_surprise_v1",
            "0.43",
        ),
        (
            "event_confirm_require_light_p50",
            "phase7_event_earnings_v1",
            "0.39",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_combo_gate_seed_with_min_score(
                anchor.clone(),
                profile_name,
                combo_name,
                "require_positive",
                min_score,
                "0",
                ScoreDirection::Descending,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_event_strength_boost_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(vol22_anchor, "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let value40_anchor = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let anchor = with_portfolio_method_seed(value40_anchor, "risk_budget", 120);
    let mut seeds = Vec::new();

    for (profile_name, combo_name, min_score, boost_weight) in [
        (
            "event_window_boost_strong_p75_3pct",
            "phase7_event_window_earnings_v1",
            "0.38",
            "0.03",
        ),
        (
            "event_window_boost_strong_p75_5pct",
            "phase7_event_window_earnings_v1",
            "0.38",
            "0.05",
        ),
        (
            "event_surprise_boost_strong_p75_3pct",
            "phase7_event_surprise_v1",
            "0.35",
            "0.03",
        ),
        (
            "event_surprise_boost_strong_p75_5pct",
            "phase7_event_surprise_v1",
            "0.35",
            "0.05",
        ),
        (
            "event_confirm_boost_light_p50_3pct",
            "phase7_event_earnings_v1",
            "0.39",
            "0.03",
        ),
        (
            "event_confirm_boost_light_p50_5pct",
            "phase7_event_earnings_v1",
            "0.39",
            "0.05",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_combo_gate_seed_with_min_score(
                anchor.clone(),
                profile_name,
                combo_name,
                "boost_positive",
                min_score,
                boost_weight,
                ScoreDirection::Descending,
            )],
        );
    }

    seeds
}

pub(crate) fn professional_mixed_state_event_alpha_seed_trials() -> Vec<Value> {
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
            "quality_state_alpha_overlay_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_event_state_overlay_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_mixed_event_state_selector_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_event_state_selector_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_mixed_event_state_overlay_selector_v2",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_event_window_return_sharpe_router_v4",
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
                    &format!("{regime}_{vol_profile}_no_smooth"),
                ),
                vol_profile,
                target_pct,
                120,
                min_exposure,
                "1",
            ),
            &format!("bp_rb160_{regime}_{vol_profile}_no_smooth"),
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

pub(crate) fn professional_high_sharpe_boundary_event_lift_seed_trials() -> Vec<Value> {
    let Some(boundary) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (regime, valuation_profile, valuation_min_score, vol_profile, target_pct, min_exposure) in [
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
        ),
        (
            "quality_all_regime_event_window_sleeve_05pct_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
        ),
        (
            "quality_all_regime_event_window_sleeve_05pct_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_all_regime_event_window_sleeve_10pct_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
        ),
        (
            "quality_all_regime_event_window_sleeve_10pct_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
        ),
        (
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_14_46_100",
            "0.14",
            "0.46",
        ),
        (
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
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
                        &format!(
                            "{regime}_{valuation_profile}_{vol_profile}_high_sharpe_event_lift"
                        ),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("{regime}_{vol_profile}_maxpos15_corr75"),
                "0.15",
                "0.75",
            ),
            &format!("bp_rb180_{regime}_{valuation_profile}_{vol_profile}"),
            180,
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

pub(crate) fn professional_event_position_risk_router_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(anchor.clone(), "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let mut seeds = Vec::new();

    for seed in [anchor.clone(), vol18_anchor.clone()] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(
                seed.clone(),
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            )],
        );
        for market_regime in [
            "quality_event_window_position_guard_v3",
            "quality_event_window_position_guard_v1",
            "quality_event_window_position_guard_v2",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), market_regime)],
            );
        }
    }

    seeds
}
