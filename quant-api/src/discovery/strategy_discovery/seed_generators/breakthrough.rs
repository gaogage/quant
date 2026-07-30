//! BC4 策略发现 / seed_generators / breakthrough：突破/锚点 种子生成器。
//!
//! R8 批次2 由 strategy_discovery/mod.rs 拆出，纯 move，零行为变更。

use serde_json::{json, Value};

use super::super::profiles::ScoreDirection;
use super::*;

pub(crate) fn professional_breakthrough_seed_trials() -> Vec<Value> {
    fn quality_seed(
        market_regime: &str,
        max_pairwise_correlation: &str,
        drawdown_profile: Option<(&str, &str, &str, &str, usize)>,
        recovery: Option<(&str, &str, &str)>,
    ) -> Value {
        let mut seed = json!({
            "market_regime": market_regime,
            "top_n": 20,
            "rebalance": "60",
            "score_direction": "ascending",
            "skip_top_pct": "0.10",
            "max_pairwise_correlation": max_pairwise_correlation,
            "correlation_lookback_days": 60,
            "kelly_fraction": "0",
            "kelly_lookback_days": 60,
            "max_position_pct": "0.15",
            "max_gross_exposure": "1",
            "portfolio_method": "risk_budget",
            "risk_budget_lookback_days": 120,
            "capacity_penalty_strength": "0.75",
            "industry_max_weight_pct": null,
            "score_candidate_pool_size": 500,
            "universe_profile": "all",
            "portfolio_drawdown_control": drawdown_profile
                .map(|profile| profile.0)
                .unwrap_or("off"),
            "portfolio_volatility_control": "off",
            "benchmark": "000300.SH",
            "signal_source": "factor_combo",
            "combo_name": "phase7_financial_quality_v1",
            "version": "1.0.0",
        });
        if let Some((_name, reduce_start, reduce_full, min_exposure, lookback_days)) =
            drawdown_profile
        {
            seed["portfolio_drawdown_reduce_start_pct"] = json!(reduce_start);
            seed["portfolio_drawdown_reduce_full_pct"] = json!(reduce_full);
            seed["portfolio_drawdown_min_exposure"] = json!(min_exposure);
            seed["portfolio_drawdown_peak_lookback_days"] = json!(lookback_days);
        }
        if let Some((recovery_start, recovery_full, boost)) = recovery {
            seed["portfolio_drawdown_recovery_start_pct"] = json!(recovery_start);
            seed["portfolio_drawdown_recovery_full_pct"] = json!(recovery_full);
            seed["portfolio_drawdown_recovery_boost"] = json!(boost);
        }
        seed
    }

    vec![
        quality_seed("off", "0.75", None, None),
        quality_seed("quality_crash_guard_v1", "0.75", None, None),
        quality_seed(
            "quality_crash_guard_v1",
            "0.75",
            Some(("recover252_10_25_50_30_70", "0.10", "0.25", "0.50", 252)),
            Some(("0.30", "0.70", "1")),
        ),
        quality_seed(
            "quality_crash_guard_v1",
            "0.75",
            Some(("recover252_10_27_50_30_70", "0.10", "0.27", "0.50", 252)),
            Some(("0.30", "0.70", "1")),
        ),
        quality_seed(
            "quality_crash_guard_v1",
            "0.75",
            Some(("rolling504_15_35_70", "0.15", "0.35", "0.70", 504)),
            None,
        ),
        quality_seed(
            "quality_crash_guard_v1",
            "0.90",
            Some(("recover252_10_27_50_30_70", "0.10", "0.27", "0.50", 252)),
            Some(("0.30", "0.70", "1")),
        ),
    ]
}

pub(crate) fn professional_risk_breakthrough_seed_trials() -> Vec<Value> {
    #[derive(Clone, Copy)]
    struct PositionRiskSeed<'a> {
        stop_loss_pct: Option<&'a str>,
        take_profit_pct: Option<&'a str>,
        trailing_stop_pct: Option<&'a str>,
        time_stop_days: Option<u32>,
        reentry_cooldown_days: Option<u32>,
    }

    fn risk_seed(
        combo_name: &str,
        market_regime: &str,
        top_n: usize,
        rebalance: usize,
        max_position_pct: &str,
        max_gross_exposure: &str,
        risk_budget_lookback_days: usize,
        capacity_penalty_strength: &str,
        industry_max_weight_pct: Option<&str>,
        drawdown_profile: (&str, &str, &str, &str, usize, &str, &str, &str),
        volatility_profile: Option<(&str, &str, usize, &str, &str)>,
        prediction_overlay: Option<(&str, &str, Option<&str>)>,
        position_risk: Option<PositionRiskSeed<'_>>,
    ) -> Value {
        let mut seed = json!({
            "market_regime": market_regime,
            "top_n": top_n,
            "rebalance": rebalance.to_string(),
            "score_direction": "ascending",
            "skip_top_pct": "0.10",
            "max_pairwise_correlation": "0.75",
            "correlation_lookback_days": 60,
            "kelly_fraction": "0",
            "kelly_lookback_days": 60,
            "max_position_pct": max_position_pct,
            "max_gross_exposure": max_gross_exposure,
            "portfolio_method": "risk_budget",
            "risk_budget_lookback_days": risk_budget_lookback_days,
            "capacity_penalty_strength": capacity_penalty_strength,
            "industry_max_weight_pct": industry_max_weight_pct,
            "score_candidate_pool_size": 500,
            "universe_profile": "all",
            "portfolio_drawdown_control": drawdown_profile.0,
            "portfolio_volatility_control": volatility_profile
                .map(|profile| profile.0)
                .unwrap_or("off"),
            "benchmark": "000300.SH",
            "signal_source": "factor_combo",
            "combo_name": combo_name,
            "version": "1.0.0",
        });
        seed["portfolio_drawdown_reduce_start_pct"] = json!(drawdown_profile.1);
        seed["portfolio_drawdown_reduce_full_pct"] = json!(drawdown_profile.2);
        seed["portfolio_drawdown_min_exposure"] = json!(drawdown_profile.3);
        seed["portfolio_drawdown_peak_lookback_days"] = json!(drawdown_profile.4);
        seed["portfolio_drawdown_recovery_start_pct"] = json!(drawdown_profile.5);
        seed["portfolio_drawdown_recovery_full_pct"] = json!(drawdown_profile.6);
        seed["portfolio_drawdown_recovery_boost"] = json!(drawdown_profile.7);
        if let Some(volatility_profile) = volatility_profile {
            seed["portfolio_volatility_target_pct"] = json!(volatility_profile.1);
            seed["portfolio_volatility_lookback_days"] = json!(volatility_profile.2);
            seed["portfolio_volatility_min_exposure"] = json!(volatility_profile.3);
            seed["portfolio_volatility_max_exposure"] = json!(volatility_profile.4);
        }
        if let Some((prediction_set_id, prediction_blend_weight, prediction_min_percentile)) =
            prediction_overlay
        {
            seed["prediction_set_id"] = json!(prediction_set_id);
            seed["prediction_blend_weight"] = json!(prediction_blend_weight);
            if let Some(prediction_min_percentile) = prediction_min_percentile {
                seed["prediction_min_percentile"] = json!(prediction_min_percentile);
            }
        }
        if let Some(position_risk) = position_risk {
            if let Some(stop_loss_pct) = position_risk.stop_loss_pct {
                seed["stop_loss_pct"] = json!(stop_loss_pct);
            }
            if let Some(take_profit_pct) = position_risk.take_profit_pct {
                seed["take_profit_pct"] = json!(take_profit_pct);
            }
            if let Some(trailing_stop_pct) = position_risk.trailing_stop_pct {
                seed["trailing_stop_pct"] = json!(trailing_stop_pct);
            }
            if let Some(time_stop_days) = position_risk.time_stop_days {
                seed["time_stop_days"] = json!(time_stop_days);
            }
            if let Some(reentry_cooldown_days) = position_risk.reentry_cooldown_days {
                seed["reentry_cooldown_days"] = json!(reentry_cooldown_days);
            }
        }
        seed
    }

    fn risk_seed_with_direction(
        combo_name: &str,
        market_regime: &str,
        score_direction: &str,
        top_n: usize,
        rebalance: usize,
        max_position_pct: &str,
        max_gross_exposure: &str,
        risk_budget_lookback_days: usize,
        capacity_penalty_strength: &str,
        industry_max_weight_pct: Option<&str>,
        drawdown_profile: (&str, &str, &str, &str, usize, &str, &str, &str),
        volatility_profile: Option<(&str, &str, usize, &str, &str)>,
        prediction_overlay: Option<(&str, &str, Option<&str>)>,
        position_risk: Option<PositionRiskSeed<'_>>,
    ) -> Value {
        let mut seed = risk_seed(
            combo_name,
            market_regime,
            top_n,
            rebalance,
            max_position_pct,
            max_gross_exposure,
            risk_budget_lookback_days,
            capacity_penalty_strength,
            industry_max_weight_pct,
            drawdown_profile,
            volatility_profile,
            prediction_overlay,
            position_risk,
        );
        seed["score_direction"] = json!(score_direction);
        seed
    }

    let recover_08_25 = (
        "recover252_08_25_60_25_75",
        "0.08",
        "0.25",
        "0.60",
        252,
        "0.25",
        "0.75",
        "1",
    );
    let recover_10_30 = (
        "recover252_10_30_55_25_75",
        "0.10",
        "0.30",
        "0.55",
        252,
        "0.25",
        "0.75",
        "1",
    );
    let recover_10_27 = (
        "recover252_10_27_50_30_70",
        "0.10",
        "0.27",
        "0.50",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let recover_10_25 = (
        "recover252_10_25_50_30_70",
        "0.10",
        "0.25",
        "0.50",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let recover_10_24 = (
        "recover252_10_24_50_30_70",
        "0.10",
        "0.24",
        "0.50",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let recover_10_26 = (
        "recover252_10_26_50_30_70",
        "0.10",
        "0.26",
        "0.50",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let recover_09_26 = (
        "recover252_09_26_50_30_70",
        "0.09",
        "0.26",
        "0.50",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let recover_09_24_45 = (
        "recover252_09_24_45_30_70",
        "0.09",
        "0.24",
        "0.45",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let recover_08_24_45 = (
        "recover252_08_24_45_30_70",
        "0.08",
        "0.24",
        "0.45",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let vol120_22 = ("vol120_22_65_100", "0.22", 120, "0.65", "1");
    let vol120_24 = ("vol120_24_70_100", "0.24", 120, "0.70", "1");
    let vol60_25 = ("vol60_25_75_100", "0.25", 60, "0.75", "1");
    let vol120_30 = ("vol120_30_85_100", "0.30", 120, "0.85", "1");
    let vol60_30 = ("vol60_30_90_100", "0.30", 60, "0.90", "1");
    let wide_prediction_set = "pred-p7-wf-wide-qgvrel-v1-201602-202605";
    let trailing_stop_18 = PositionRiskSeed {
        stop_loss_pct: None,
        take_profit_pct: None,
        trailing_stop_pct: Some("0.18"),
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_07 = PositionRiskSeed {
        stop_loss_pct: Some("0.07"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_07_cooldown_5 = PositionRiskSeed {
        stop_loss_pct: Some("0.07"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: Some(5),
    };
    let stop_loss_07_cooldown_10 = PositionRiskSeed {
        stop_loss_pct: Some("0.07"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: Some(10),
    };
    let stop_loss_07_cooldown_20 = PositionRiskSeed {
        stop_loss_pct: Some("0.07"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: Some(20),
    };
    let stop_loss_07_cooldown_30 = PositionRiskSeed {
        stop_loss_pct: Some("0.07"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: Some(30),
    };
    let stop_loss_065_cooldown_20 = PositionRiskSeed {
        stop_loss_pct: Some("0.065"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: Some(20),
    };
    let stop_loss_075_cooldown_20 = PositionRiskSeed {
        stop_loss_pct: Some("0.075"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: Some(20),
    };
    let stop_loss_075_cooldown_30 = PositionRiskSeed {
        stop_loss_pct: Some("0.075"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: Some(30),
    };
    let stop_loss_08 = PositionRiskSeed {
        stop_loss_pct: Some("0.08"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_085 = PositionRiskSeed {
        stop_loss_pct: Some("0.085"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_09 = PositionRiskSeed {
        stop_loss_pct: Some("0.09"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_095 = PositionRiskSeed {
        stop_loss_pct: Some("0.095"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_10 = PositionRiskSeed {
        stop_loss_pct: Some("0.10"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_11 = PositionRiskSeed {
        stop_loss_pct: Some("0.11"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_12 = PositionRiskSeed {
        stop_loss_pct: Some("0.12"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_13 = PositionRiskSeed {
        stop_loss_pct: Some("0.13"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };
    let stop_loss_14 = PositionRiskSeed {
        stop_loss_pct: Some("0.14"),
        take_profit_pct: None,
        trailing_stop_pct: None,
        time_stop_days: None,
        reentry_cooldown_days: None,
    };

    vec![
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.10",
            "0.90",
            180,
            "1",
            Some("0.20"),
            recover_08_25,
            Some(vol120_24),
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.12",
            "1",
            120,
            "0.75",
            Some("0.20"),
            recover_10_30,
            Some(vol120_24),
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            25,
            70,
            "0.10",
            "0.90",
            180,
            "1",
            Some("0.20"),
            recover_10_27,
            Some(vol60_25),
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.12",
            "1",
            120,
            "0.75",
            Some("0.20"),
            recover_10_27,
            Some(vol120_30),
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            Some("0.20"),
            recover_10_27,
            Some(vol60_30),
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.14",
            "1",
            120,
            "0.75",
            None,
            recover_10_26,
            None,
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_09_26,
            None,
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v2",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v3",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            Some((wide_prediction_set, "0.02", None)),
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            Some((wide_prediction_set, "0.05", Some("0.20"))),
            None,
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(trailing_stop_18),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_25,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_07),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_07_cooldown_5),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_07_cooldown_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_07_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            Some("0.20"),
            recover_10_24,
            None,
            None,
            Some(stop_loss_07_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            Some("0.25"),
            recover_10_24,
            None,
            None,
            Some(stop_loss_07_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            Some("0.30"),
            recover_10_24,
            None,
            None,
            Some(stop_loss_07_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_065_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            Some(vol120_24),
            None,
            Some(stop_loss_075_cooldown_30),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            Some(vol120_22),
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            Some(vol120_22),
            None,
            Some(stop_loss_075_cooldown_30),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_09_24_45,
            Some(vol120_24),
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.13",
            "0.90",
            120,
            "0.75",
            None,
            recover_08_24_45,
            Some(vol120_24),
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        {
            let mut seed = risk_seed(
                "phase7_financial_quality_v1",
                "quality_crash_guard_v1",
                20,
                60,
                "0.15",
                "1",
                120,
                "0.75",
                None,
                recover_10_24,
                Some(vol120_24),
                None,
                Some(stop_loss_075_cooldown_20),
            );
            seed["max_pairwise_correlation"] = json!("0.65");
            seed
        },
        {
            let mut seed = risk_seed(
                "phase7_financial_quality_v1",
                "quality_crash_guard_v1",
                20,
                60,
                "0.15",
                "1",
                120,
                "0.75",
                None,
                recover_10_24,
                Some(vol120_24),
                None,
                Some(stop_loss_075_cooldown_20),
            );
            seed["score_candidate_pool_size"] = json!(800);
            seed["universe_profile"] = json!("listed_non_st");
            seed
        },
        risk_seed(
            "phase7_quality_moneyflow_pos_5pct_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            Some(vol120_24),
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_07_cooldown_30),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_075_cooldown_30),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "0.90",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            Some(vol120_24),
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_09_24_45,
            None,
            None,
            Some(stop_loss_075_cooldown_20),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_08),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_085),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_09),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_24,
            None,
            None,
            Some(stop_loss_095),
        ),
        risk_seed(
            "phase7_quality_moneyflow_pos_5pct_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_value_quality_growth_rel_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_blend_quality_growth_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_blend_defensive_rel_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_blend_recovery_tilt_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed_with_direction(
            "phase7_value_quality_growth_rel_v1",
            "quality_crash_guard_v1",
            "descending",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed_with_direction(
            "phase7_blend_recovery_tilt_v1",
            "quality_crash_guard_v1",
            "descending",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.14",
            "1",
            120,
            "0.75",
            None,
            recover_09_26,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.13",
            "1",
            120,
            "0.75",
            None,
            recover_09_24_45,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.12",
            "0.90",
            120,
            "0.75",
            None,
            recover_08_24_45,
            None,
            None,
            Some(stop_loss_10),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_11),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_12),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_13),
        ),
        risk_seed(
            "phase7_financial_quality_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.15",
            "1",
            120,
            "0.75",
            None,
            recover_10_27,
            None,
            None,
            Some(stop_loss_14),
        ),
        risk_seed(
            "phase7_quality_moneyflow_pos_5pct_v1",
            "quality_crash_guard_v1",
            20,
            60,
            "0.10",
            "0.90",
            120,
            "1",
            Some("0.20"),
            recover_08_25,
            Some(vol120_24),
            None,
            None,
        ),
    ]
}

pub(crate) fn professional_sharpe_stabilization_seed_trials() -> Vec<Value> {
    let anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let base_seeds = professional_risk_breakthrough_seed_trials()
        .into_iter()
        .filter(|seed| {
            let is_trial29_neighborhood =
                seed["stop_loss_pct"] == "0.075" && seed.get("reentry_cooldown_days").is_some();
            let volatility_control = seed["portfolio_volatility_control"]
                .as_str()
                .unwrap_or("off");
            let drawdown_control = seed["portfolio_drawdown_control"].as_str().unwrap_or("");
            let combo_name = seed["combo_name"].as_str().unwrap_or("");

            is_trial29_neighborhood
                && (volatility_control != "off"
                    || seed["max_gross_exposure"] == "0.90"
                    || drawdown_control == "recover252_09_24_45_30_70"
                    || drawdown_control == "recover252_08_24_45_30_70"
                    || seed["reentry_cooldown_days"] == 30
                    || combo_name == "phase7_quality_moneyflow_pos_5pct_v1")
        })
        .collect::<Vec<_>>();

    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, anchor_seeds);
    append_unique_seeds(&mut seeds, base_seeds.clone());
    let smoothing_anchors = seeds.clone();
    add_turnover_smoothing_variants(&mut seeds, &smoothing_anchors);
    seeds
}

pub(crate) fn phase7_u2_exact_anchor_seed_trials() -> Vec<Value> {
    [
        ("vol120_22_65_100", "0.22", 120, "0.65", "1"),
        ("vol120_24_70_100", "0.24", 120, "0.70", "1"),
    ]
    .into_iter()
    .map(
        |(
            volatility_profile,
            volatility_target_pct,
            volatility_lookback_days,
            volatility_min_exposure,
            volatility_max_exposure,
        )| {
            json!({
                "market_regime": "quality_bear_window_guard_v2",
                "top_n": 20,
                "rebalance": "60",
                "score_direction": "ascending",
                "skip_top_pct": "0.10",
                "max_pairwise_correlation": "0.75",
                "correlation_lookback_days": 60,
                "kelly_fraction": "0",
                "kelly_lookback_days": 60,
                "max_position_pct": "0.15",
                "max_gross_exposure": "1",
                "portfolio_method": "risk_budget",
                "risk_budget_lookback_days": 120,
                "capacity_penalty_strength": "0.75",
                "industry_max_weight_pct": Value::Null,
                "style_risk_budget": "off",
                "candidate_risk_filter": "off",
                "risk_contribution_control": "off",
                "rebalance_hysteresis_pct": "0",
                "partial_rebalance_ratio": "1",
                "score_candidate_pool_size": 500,
                "universe_profile": "all",
                "portfolio_drawdown_control": "recover252_10_24_50_30_70",
                "portfolio_drawdown_reduce_start_pct": "0.10",
                "portfolio_drawdown_reduce_full_pct": "0.24",
                "portfolio_drawdown_min_exposure": "0.50",
                "portfolio_drawdown_peak_lookback_days": 252,
                "portfolio_drawdown_recovery_start_pct": "0.30",
                "portfolio_drawdown_recovery_full_pct": "0.70",
                "portfolio_drawdown_recovery_boost": "1",
                "portfolio_volatility_control": volatility_profile,
                "portfolio_volatility_target_pct": volatility_target_pct,
                "portfolio_volatility_lookback_days": volatility_lookback_days,
                "portfolio_volatility_min_exposure": volatility_min_exposure,
                "portfolio_volatility_max_exposure": volatility_max_exposure,
                "stop_loss_pct": "0.075",
                "reentry_cooldown_days": 30,
                "benchmark": "000300.SH",
                "signal_source": "factor_combo",
                "combo_name": "phase7_financial_quality_v1",
                "version": "1.0.0",
            })
        },
    )
    .collect()
}

pub(crate) fn phase7_high_sharpe_boundary_base_seed() -> Option<Value> {
    let base = json!({
        "market_regime": "quality_mixed_state_risk_memory_router_v14",
        "top_n": 20,
        "rebalance": "60",
        "score_direction": "ascending",
        "skip_top_pct": "0.10",
        "max_pairwise_correlation": "0.75",
        "correlation_lookback_days": 60,
        "kelly_fraction": "0",
        "kelly_lookback_days": 60,
        "max_position_pct": "0.15",
        "max_gross_exposure": "1",
        "portfolio_method": "risk_budget",
        "risk_budget_lookback_days": 180,
        "capacity_penalty_strength": "0.75",
        "industry_max_weight_pct": Value::Null,
        "style_risk_budget": "off",
        "candidate_risk_filter": "off",
        "risk_contribution_control": "soft_single_name_20pct_v1",
        "rebalance_smoothing_profile": "off",
        "rebalance_hysteresis_pct": "0",
        "partial_rebalance_ratio": "1",
        "score_candidate_pool_size": 500,
        "universe_profile": "all",
        "portfolio_drawdown_control": "recover252_08_22_45_30_70",
        "portfolio_drawdown_reduce_start_pct": "0.08",
        "portfolio_drawdown_reduce_full_pct": "0.22",
        "portfolio_drawdown_min_exposure": "0.45",
        "portfolio_drawdown_peak_lookback_days": 252,
        "portfolio_drawdown_recovery_start_pct": "0.30",
        "portfolio_drawdown_recovery_full_pct": "0.70",
        "portfolio_drawdown_recovery_boost": "1",
        "portfolio_sharpe_control": "off",
        "position_risk_control": "stop_loss_075_cooldown_30",
        "stop_loss_pct": "0.075",
        "reentry_cooldown_days": 30,
        "benchmark": "000300.SH",
        "signal_source": "factor_combo",
        "combo_name": "phase7_financial_quality_v1",
        "version": "1.0.0",
    });
    Some(with_volatility_profile_seed(
        with_event_combo_gate_seed_with_min_score(
            base,
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.45",
            "0",
            ScoreDirection::Descending,
        ),
        "vol120_14_46_100",
        "0.14",
        120,
        "0.46",
        "1",
    ))
}

pub(crate) fn professional_anti_overfit_sharpe_seed_trials() -> Vec<Value> {
    let anchors = phase7_u2_exact_anchor_seed_trials();
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, anchors.clone());

    for seed in &anchors {
        for market_regime in [
            "quality_bear_window_guard_v1",
            "quality_bear_window_guard_v2",
            "quality_crash_guard_v3",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), market_regime)],
            );
        }

        for drawdown_profile in [
            (
                "recover252_09_24_45_30_70",
                "0.09",
                "0.24",
                "0.45",
                252,
                "0.30",
                "0.70",
                "1",
            ),
            (
                "recover252_08_24_45_30_70",
                "0.08",
                "0.24",
                "0.45",
                252,
                "0.30",
                "0.70",
                "1",
            ),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_drawdown_profile_seed(
                    seed.clone(),
                    drawdown_profile.0,
                    drawdown_profile.1,
                    drawdown_profile.2,
                    drawdown_profile.3,
                    drawdown_profile.4,
                    drawdown_profile.5,
                    drawdown_profile.6,
                    drawdown_profile.7,
                )],
            );
        }

        append_unique_seeds(
            &mut seeds,
            vec![with_style_risk_budget_seed(
                seed.clone(),
                "liquidity_volatility_balanced_v1",
            )],
        );
        append_unique_seeds(
            &mut seeds,
            vec![
                with_event_window_gate_seed(
                    seed.clone(),
                    "event_window_boost_pos_5pct",
                    "boost_positive",
                    "0.05",
                ),
                with_event_window_gate_seed(
                    seed.clone(),
                    "event_window_exclude_negative",
                    "exclude_negative",
                    "0",
                ),
            ],
        );
        append_unique_seeds(
            &mut seeds,
            vec![
                retarget_seed_combo(seed.clone(), "phase7_quality_event_confirm_v1"),
                retarget_seed_combo(seed.clone(), "phase7_quality_event_surprise_confirm_v1"),
            ],
        );
    }

    seeds
}

pub(crate) fn professional_candidate_risk_filter_seed_trials() -> Vec<Value> {
    let anchors = phase7_u2_exact_anchor_seed_trials();
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, anchors.clone());

    for seed in &anchors {
        for candidate_risk_filter in ["low_volatility_v1", "low_volatility_low_correlation_v1"] {
            append_unique_seeds(
                &mut seeds,
                vec![with_candidate_risk_filter_seed(
                    seed.clone(),
                    candidate_risk_filter,
                )],
            );
        }
    }

    append_unique_seeds(&mut seeds, professional_anti_overfit_sharpe_seed_trials());
    seeds
}

pub(crate) fn professional_risk_contribution_seed_trials() -> Vec<Value> {
    let anchors = phase7_u2_exact_anchor_seed_trials();
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, anchors.clone());

    for seed in &anchors {
        for risk_contribution_control in ["soft_single_name_20pct_v1", "soft_single_name_15pct_v1"]
        {
            append_unique_seeds(
                &mut seeds,
                vec![with_risk_contribution_control_seed(
                    seed.clone(),
                    risk_contribution_control,
                )],
            );
        }
    }

    append_unique_seeds(&mut seeds, professional_anti_overfit_sharpe_seed_trials());
    seeds
}

pub(crate) fn phase7_current_event_window_15pct_anchor_seed() -> Option<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let vol22_anchor = anchor_seeds.drain(..).next()?;
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
    Some(with_portfolio_method_seed(
        with_market_regime_seed(
            value40_anchor,
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        ),
        "risk_budget",
        120,
    ))
}

pub(crate) fn phase7_current_event_window_15pct_risk_budget_180_anchor_seed() -> Option<Value> {
    phase7_current_event_window_15pct_anchor_seed()
        .map(|seed| with_risk_budget_lookback_seed(seed, "risk_budget_180", 180))
}

pub(crate) fn professional_current_anchor_risk_shape_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_event_window_15pct_anchor_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, vec![anchor.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![
            with_volatility_profile_seed(
                anchor.clone(),
                "vol120_20_60_100",
                "0.20",
                120,
                "0.60",
                "1",
            ),
            with_volatility_profile_seed(
                anchor.clone(),
                "vol120_16_50_100",
                "0.16",
                120,
                "0.50",
                "1",
            ),
            with_volatility_profile_seed(
                anchor.clone(),
                "vol252_16_50_100",
                "0.16",
                252,
                "0.50",
                "1",
            ),
            with_drawdown_profile_seed(
                anchor.clone(),
                "recover252_08_22_45_30_70",
                "0.08",
                "0.22",
                "0.45",
                252,
                "0.30",
                "0.70",
                "1",
            ),
            with_drawdown_profile_seed(
                anchor.clone(),
                "recover126_08_22_45_30_70",
                "0.08",
                "0.22",
                "0.45",
                126,
                "0.30",
                "0.70",
                "1",
            ),
            with_stop_loss_cooldown_seed(anchor.clone(), "stop_loss_070_cooldown_30", "0.07", 30),
            with_stop_loss_cooldown_seed(anchor.clone(), "stop_loss_075_cooldown_45", "0.075", 45),
            with_position_shape_seed(anchor.clone(), "maxpos12_corr65", "0.12", "0.65"),
            with_risk_budget_lookback_seed(anchor, "risk_budget_180", 180),
        ],
    );
    seeds
}

pub(crate) fn professional_current_anchor_position_frontier_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_event_window_15pct_risk_budget_180_anchor_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, vec![anchor.clone()]);

    let vol16 =
        with_volatility_profile_seed(anchor.clone(), "vol120_16_50_100", "0.16", 120, "0.50", "1");
    let recover08 = with_drawdown_profile_seed(
        anchor.clone(),
        "recover252_08_22_45_30_70",
        "0.08",
        "0.22",
        "0.45",
        252,
        "0.30",
        "0.70",
        "1",
    );
    let vol16_recover08 = with_drawdown_profile_seed(
        vol16.clone(),
        "recover252_08_22_45_30_70",
        "0.08",
        "0.22",
        "0.45",
        252,
        "0.30",
        "0.70",
        "1",
    );
    append_unique_seeds(
        &mut seeds,
        vec![vol16.clone(), recover08.clone(), vol16_recover08.clone()],
    );

    for (profile_name, max_position_pct, max_pairwise_correlation) in [
        ("maxpos14_corr70", "0.14", "0.70"),
        ("maxpos13_corr70", "0.13", "0.70"),
        ("maxpos14_corr65", "0.14", "0.65"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_position_shape_seed(
                anchor.clone(),
                profile_name,
                max_position_pct,
                max_pairwise_correlation,
            )],
        );
    }
    append_unique_seeds(
        &mut seeds,
        vec![
            with_position_shape_seed(vol16, "vol16_maxpos14_corr70", "0.14", "0.70"),
            with_position_shape_seed(recover08, "recover08_maxpos14_corr70", "0.14", "0.70"),
            with_position_shape_seed(
                vol16_recover08,
                "vol16_recover08_maxpos14_corr70",
                "0.14",
                "0.70",
            ),
        ],
    );

    seeds
}

pub(crate) fn phase7_current_anchor_bg_trial4_seed() -> Option<Value> {
    phase7_current_event_window_15pct_risk_budget_180_anchor_seed().map(|seed| {
        let vol16 =
            with_volatility_profile_seed(seed, "vol120_16_50_100", "0.16", 120, "0.50", "1");
        with_drawdown_profile_seed(
            vol16,
            "recover252_08_22_45_30_70",
            "0.08",
            "0.22",
            "0.45",
            252,
            "0.30",
            "0.70",
            "1",
        )
    })
}

pub(crate) fn phase7_current_anchor_high_sharpe_boundary_seed() -> Option<Value> {
    phase7_current_event_window_15pct_risk_budget_180_anchor_seed()
        .map(|seed| with_position_shape_seed(seed, "maxpos14_corr65", "0.14", "0.65"))
}

pub(crate) fn professional_current_anchor_weak_window_repair_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let stress_regimes = ["bear", "high_volatility"];
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, vec![anchor.clone()]);

    for (profile_name, combo_name, mode, min_score, boost_weight) in [
        (
            "stress_event_window_boost_p75_3pct",
            "phase7_event_window_earnings_v1",
            "boost_positive",
            "0.38",
            "0.03",
        ),
        (
            "stress_event_window_boost_p75_5pct",
            "phase7_event_window_earnings_v1",
            "boost_positive",
            "0.38",
            "0.05",
        ),
        (
            "stress_event_window_exclude_negative_p40",
            "phase7_event_window_earnings_v1",
            "exclude_negative",
            "0.40",
            "0",
        ),
        (
            "stress_valuation_boost_p40_3pct",
            "phase7_valuation_v1",
            "boost_positive",
            "0.40",
            "0.03",
        ),
        (
            "stress_valuation_boost_p40_5pct",
            "phase7_valuation_v1",
            "boost_positive",
            "0.40",
            "0.05",
        ),
        (
            "stress_valuation_exclude_p40",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
            "0",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![
                with_event_combo_gate_seed_active_in_with_min_score_and_boost(
                    anchor.clone(),
                    profile_name,
                    combo_name,
                    mode,
                    min_score,
                    boost_weight,
                    ScoreDirection::Descending,
                    &stress_regimes,
                ),
            ],
        );
    }

    for market_regime in [
        "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1",
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(anchor.clone(), market_regime)],
        );
    }

    seeds
}

pub(crate) fn professional_current_anchor_sharpe_return_bridge_seed_trials() -> Vec<Value> {
    let Some(boundary) = phase7_current_anchor_high_sharpe_boundary_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, vec![boundary.clone()]);

    let maxpos14_corr70 =
        with_position_shape_seed(boundary.clone(), "maxpos14_corr70", "0.14", "0.70");
    let maxpos15_corr70 =
        with_position_shape_seed(boundary.clone(), "maxpos15_corr70", "0.15", "0.70");
    append_unique_seeds(
        &mut seeds,
        vec![maxpos14_corr70.clone(), maxpos15_corr70.clone()],
    );

    for (base, base_name) in [
        (boundary.clone(), "maxpos14_corr65"),
        (maxpos14_corr70.clone(), "maxpos14_corr70"),
        (maxpos15_corr70.clone(), "maxpos15_corr70"),
    ] {
        let max_position_pct = base["max_position_pct"].as_str().unwrap_or("0.14");
        let max_pairwise_correlation = base["max_pairwise_correlation"].as_str().unwrap_or("0.65");
        for (suffix, profile_name, target_pct, min_exposure) in [
            ("vol20", "vol120_20_60_100", "0.20", "0.60"),
            ("vol22", "vol120_22_65_100", "0.22", "0.65"),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_volatility_profile_seed(
                    with_position_shape_seed(
                        base.clone(),
                        &format!("{base_name}_{suffix}"),
                        max_position_pct,
                        max_pairwise_correlation,
                    ),
                    profile_name,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                )],
            );
        }
    }

    append_unique_seeds(
        &mut seeds,
        vec![
            with_market_regime_seed(
                with_volatility_profile_seed(
                    with_position_shape_seed(
                        boundary.clone(),
                        "maxpos14_corr65_vol20_event125",
                        "0.14",
                        "0.65",
                    ),
                    "vol120_20_60_100",
                    "0.20",
                    120,
                    "0.60",
                    "1",
                ),
                "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1",
            ),
            with_market_regime_seed(
                with_volatility_profile_seed(
                    with_position_shape_seed(
                        maxpos14_corr70,
                        "maxpos14_corr70_vol20_event125",
                        "0.14",
                        "0.70",
                    ),
                    "vol120_20_60_100",
                    "0.20",
                    120,
                    "0.60",
                    "1",
                ),
                "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1",
            ),
            with_top_n_seed(maxpos15_corr70, 25, "top25_maxpos15_corr70"),
        ],
    );

    seeds.into_iter().take(10).collect()
}

pub(crate) fn professional_high_sharpe_return_recovery_seed_trials() -> Vec<Value> {
    let Some(boundary) = phase7_current_anchor_high_sharpe_boundary_seed() else {
        return Vec::new();
    };
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return vec![boundary];
    };

    let mut seeds = Vec::new();
    append_unique_seeds(&mut seeds, vec![boundary.clone(), return_anchor]);

    for (profile_name, target_pct, min_exposure) in [
        ("vol120_18_55_100", "0.18", "0.55"),
        ("vol120_19_58_100", "0.19", "0.58"),
        ("vol120_20_60_100", "0.20", "0.60"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_volatility_profile_seed(
                boundary.clone(),
                profile_name,
                target_pct,
                120,
                min_exposure,
                "1",
            )],
        );
    }

    for (market_regime, sleeve_profile) in [
        (
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1",
            "event_window_125pct",
        ),
        (
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            "event_window_15pct",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_sleeve_seed(
                boundary.clone(),
                market_regime,
                sleeve_profile,
            )],
        );
    }

    for (profile_name, min_score) in [
        ("valuation_exclude_bottom35", "0.35"),
        ("valuation_exclude_bottom40", "0.40"),
        ("valuation_exclude_bottom45", "0.45"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_combo_gate_seed_with_min_score(
                boundary.clone(),
                profile_name,
                "phase7_valuation_v1",
                "exclude_negative",
                min_score,
                "0",
                ScoreDirection::Descending,
            )],
        );
    }

    let maxpos145_corr65 =
        with_position_shape_seed(boundary.clone(), "maxpos145_corr65", "0.145", "0.65");
    append_unique_seeds(&mut seeds, vec![maxpos145_corr65.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_volatility_profile_seed(
            maxpos145_corr65,
            "vol120_19_58_100",
            "0.19",
            120,
            "0.58",
            "1",
        )],
    );

    append_unique_seeds(
        &mut seeds,
        vec![
            with_position_shape_seed(boundary.clone(), "maxpos15_corr65", "0.15", "0.65"),
            with_position_shape_seed(boundary.clone(), "maxpos14_corr675", "0.14", "0.675"),
            with_top_n_seed(boundary, 22, "top22_maxpos14_corr65"),
        ],
    );

    seeds.into_iter().take(12).collect()
}
