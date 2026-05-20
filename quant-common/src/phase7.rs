use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalResourcePlan {
    pub profile: String,
    pub cpu_count: usize,
    pub memory_gb: usize,
    pub reserved_cpus: usize,
    pub max_parallel_trials: usize,
    pub batch_size: usize,
    pub memory_budget_gb: usize,
    pub max_trials: usize,
    pub trial_timeout_ms: u64,
    pub batch_timeout_ms: u64,
    pub profile_sampling: bool,
}

impl LocalResourcePlan {
    pub fn for_machine(cpu_count: usize, memory_gb: usize) -> Self {
        let reserved_cpus = if cpu_count >= 6 { 2 } else { 1 };
        let max_parallel_trials = cpu_count.saturating_sub(reserved_cpus).max(1);
        let memory_budget_gb = ((memory_gb as f64) * 0.65).floor() as usize;
        let memory_budget_gb = memory_budget_gb.max(2);
        let max_trials = (max_parallel_trials * 8).clamp(40, 200);

        Self {
            profile: "local_mac".to_string(),
            cpu_count,
            memory_gb,
            reserved_cpus,
            max_parallel_trials,
            batch_size: max_parallel_trials,
            memory_budget_gb,
            max_trials,
            trial_timeout_ms: 900_000,
            batch_timeout_ms: 3_600_000,
            profile_sampling: true,
        }
    }

    pub fn local_mac() -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        let memory_gb = detect_memory_gb().unwrap_or(16);
        Self::for_machine(cpu_count, memory_gb)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboVersion {
    pub combo_name: String,
    pub version: String,
}

impl ComboVersion {
    pub fn new(combo_name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            combo_name: combo_name.into(),
            version: version.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlphaBlendSource {
    pub combo_name: String,
    pub version: String,
    pub weight: Decimal,
}

impl AlphaBlendSource {
    pub fn new(combo_name: impl Into<String>, version: impl Into<String>, weight: Decimal) -> Self {
        Self {
            combo_name: combo_name.into(),
            version: version.into(),
            weight,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlphaBlendProfile {
    pub profile_name: String,
    pub combo_name: String,
    pub version: String,
    pub description: String,
    pub sources: Vec<AlphaBlendSource>,
}

impl AlphaBlendProfile {
    pub fn combo_version(&self) -> ComboVersion {
        ComboVersion::new(self.combo_name.clone(), self.version.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortfolioDrawdownControlProfile {
    pub profile_name: String,
    pub reduce_start_pct: Option<Decimal>,
    pub reduce_full_pct: Option<Decimal>,
    pub min_exposure: Option<Decimal>,
    pub peak_lookback_days: Option<usize>,
    pub recovery_start_pct: Option<Decimal>,
    pub recovery_full_pct: Option<Decimal>,
    pub recovery_boost: Option<Decimal>,
}

impl PortfolioDrawdownControlProfile {
    pub fn off() -> Self {
        Self {
            profile_name: "off".to_string(),
            reduce_start_pct: None,
            reduce_full_pct: None,
            min_exposure: None,
            peak_lookback_days: None,
            recovery_start_pct: None,
            recovery_full_pct: None,
            recovery_boost: None,
        }
    }

    pub fn preserve(
        profile_name: impl Into<String>,
        reduce_start_pct: Decimal,
        reduce_full_pct: Decimal,
        min_exposure: Decimal,
        peak_lookback_days: Option<usize>,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            reduce_start_pct: Some(reduce_start_pct),
            reduce_full_pct: Some(reduce_full_pct),
            min_exposure: Some(min_exposure),
            peak_lookback_days,
            recovery_start_pct: None,
            recovery_full_pct: None,
            recovery_boost: None,
        }
    }

    pub fn recover(
        profile_name: impl Into<String>,
        reduce_start_pct: Decimal,
        reduce_full_pct: Decimal,
        min_exposure: Decimal,
        peak_lookback_days: Option<usize>,
        recovery_start_pct: Decimal,
        recovery_full_pct: Decimal,
        recovery_boost: Decimal,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            reduce_start_pct: Some(reduce_start_pct),
            reduce_full_pct: Some(reduce_full_pct),
            min_exposure: Some(min_exposure),
            peak_lookback_days,
            recovery_start_pct: Some(recovery_start_pct),
            recovery_full_pct: Some(recovery_full_pct),
            recovery_boost: Some(recovery_boost),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortfolioVolatilityControlProfile {
    pub profile_name: String,
    pub target_pct: Option<Decimal>,
    pub lookback_days: Option<usize>,
    pub min_exposure: Option<Decimal>,
    pub max_exposure: Option<Decimal>,
}

impl PortfolioVolatilityControlProfile {
    pub fn off() -> Self {
        Self {
            profile_name: "off".to_string(),
            target_pct: None,
            lookback_days: None,
            min_exposure: None,
            max_exposure: None,
        }
    }

    pub fn target(
        profile_name: impl Into<String>,
        target_pct: Decimal,
        lookback_days: usize,
        min_exposure: Decimal,
        max_exposure: Decimal,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            target_pct: Some(target_pct),
            lookback_days: Some(lookback_days),
            min_exposure: Some(min_exposure),
            max_exposure: Some(max_exposure),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PositionRiskControlProfile {
    pub profile_name: String,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
    pub trailing_stop_pct: Option<Decimal>,
    pub time_stop_days: Option<u32>,
    pub reentry_cooldown_days: Option<u32>,
}

impl PositionRiskControlProfile {
    pub fn off() -> Self {
        Self {
            profile_name: "off".to_string(),
            stop_loss_pct: None,
            take_profit_pct: None,
            trailing_stop_pct: None,
            time_stop_days: None,
            reentry_cooldown_days: None,
        }
    }

    pub fn stop_loss(profile_name: impl Into<String>, stop_loss_pct: Decimal) -> Self {
        Self {
            profile_name: profile_name.into(),
            stop_loss_pct: Some(stop_loss_pct),
            take_profit_pct: None,
            trailing_stop_pct: None,
            time_stop_days: None,
            reentry_cooldown_days: None,
        }
    }

    pub fn stop_loss_with_cooldown(
        profile_name: impl Into<String>,
        stop_loss_pct: Decimal,
        reentry_cooldown_days: u32,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            stop_loss_pct: Some(stop_loss_pct),
            take_profit_pct: None,
            trailing_stop_pct: None,
            time_stop_days: None,
            reentry_cooldown_days: Some(reentry_cooldown_days),
        }
    }

    pub fn trailing_stop(profile_name: impl Into<String>, trailing_stop_pct: Decimal) -> Self {
        Self {
            profile_name: profile_name.into(),
            stop_loss_pct: None,
            take_profit_pct: None,
            trailing_stop_pct: Some(trailing_stop_pct),
            time_stop_days: None,
            reentry_cooldown_days: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventGateProfile {
    pub profile_name: String,
    pub combo_name: Option<String>,
    pub version: Option<String>,
    pub mode: Option<String>,
    pub min_score: Option<Decimal>,
    pub boost_weight: Option<Decimal>,
    pub score_direction: Option<ScoreDirection>,
    pub active_regimes: Vec<String>,
}

impl EventGateProfile {
    pub fn off() -> Self {
        Self {
            profile_name: "off".to_string(),
            combo_name: None,
            version: None,
            mode: None,
            min_score: None,
            boost_weight: None,
            score_direction: None,
            active_regimes: vec![],
        }
    }

    pub fn event_window(
        profile_name: impl Into<String>,
        mode: impl Into<String>,
        min_score: Decimal,
        boost_weight: Decimal,
        score_direction: ScoreDirection,
    ) -> Self {
        Self::event_combo(
            profile_name,
            "phase7_event_window_earnings_v1",
            mode,
            min_score,
            boost_weight,
            score_direction,
        )
    }

    pub fn event_surprise(
        profile_name: impl Into<String>,
        mode: impl Into<String>,
        min_score: Decimal,
        boost_weight: Decimal,
        score_direction: ScoreDirection,
    ) -> Self {
        Self::event_combo(
            profile_name,
            "phase7_event_surprise_v1",
            mode,
            min_score,
            boost_weight,
            score_direction,
        )
    }

    pub fn event_combo(
        profile_name: impl Into<String>,
        combo_name: impl Into<String>,
        mode: impl Into<String>,
        min_score: Decimal,
        boost_weight: Decimal,
        score_direction: ScoreDirection,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            combo_name: Some(combo_name.into()),
            version: Some("1.0.0".to_string()),
            mode: Some(mode.into()),
            min_score: Some(min_score),
            boost_weight: Some(boost_weight),
            score_direction: Some(score_direction),
            active_regimes: vec![],
        }
    }

    pub fn active_in(mut self, regimes: &[&str]) -> Self {
        self.active_regimes = regimes.iter().map(|regime| (*regime).to_string()).collect();
        self
    }
}

pub fn phase7_alpha_blend_profiles() -> Vec<AlphaBlendProfile> {
    let source = |combo: &str, weight: Decimal| AlphaBlendSource::new(combo, "1.0.0", weight);
    vec![
        AlphaBlendProfile {
            profile_name: "balanced".to_string(),
            combo_name: "phase7_value_quality_growth_rel_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Balanced valuation, quality, growth/recovery, and relative-strength blend"
                    .to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(30, 2)),
                source("phase7_financial_quality_v1", Decimal::new(30, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(30, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "value_tilt".to_string(),
            combo_name: "phase7_blend_value_tilt_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Valuation-heavy blend for cheap-quality recovery candidates".to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(45, 2)),
                source("phase7_financial_quality_v1", Decimal::new(25, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(20, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_growth".to_string(),
            combo_name: "phase7_blend_quality_growth_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Quality and growth/recovery-led blend with valuation as guardrail"
                .to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(15, 2)),
                source("phase7_financial_quality_v1", Decimal::new(40, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(35, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "defensive_relative".to_string(),
            combo_name: "phase7_blend_defensive_rel_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Defensive quality blend with a larger relative-strength stabilizer"
                .to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(25, 2)),
                source("phase7_financial_quality_v1", Decimal::new(40, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(15, 2)),
                source("phase7_relative_strength_v1", Decimal::new(20, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "recovery_tilt".to_string(),
            combo_name: "phase7_blend_recovery_tilt_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Growth/recovery-heavy blend for earnings-repair regimes".to_string(),
            sources: vec![
                source("phase7_valuation_v1", Decimal::new(20, 2)),
                source("phase7_financial_quality_v1", Decimal::new(25, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(45, 2)),
                source("phase7_relative_strength_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_moneyflow_confirm_5pct".to_string(),
            combo_name: "phase7_quality_moneyflow_pos_5pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light moneyflow confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_moneyflow_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_confirm_5pct".to_string(),
            combo_name: "phase7_quality_event_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light earnings-event confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_earnings_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_surprise_confirm_5pct".to_string(),
            combo_name: "phase7_quality_event_surprise_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality with a light segmented event-surprise overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_surprise_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_event_window_overlay_5pct".to_string(),
            combo_name: "phase7_quality_event_window_overlay_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with optional sparse post-event window overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_event_window_earnings_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_residual_confirm_5pct".to_string(),
            combo_name: "phase7_quality_residual_confirm_5pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with a small industry-residual quality confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(95, 2)),
                source("phase7_industry_residual_quality_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_residual_confirm_10pct".to_string(),
            combo_name: "phase7_quality_residual_confirm_10pct_v1".to_string(),
            version: "1.0.0".to_string(),
            description: "Financial quality core with a moderate industry-residual quality confirmation overlay"
                .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(90, 2)),
                source("phase7_industry_residual_quality_v1", Decimal::new(10, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_value_recovery_confirm_5pct".to_string(),
            combo_name: "phase7_quality_value_recovery_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial quality core with valuation, earnings-recovery, and moneyflow confirmation"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(60, 2)),
                source("phase7_valuation_v1", Decimal::new(20, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(15, 2)),
                source("phase7_moneyflow_v1", Decimal::new(5, 2)),
            ],
        },
        AlphaBlendProfile {
            profile_name: "quality_value_recovery_event_confirm_5pct".to_string(),
            combo_name: "phase7_quality_value_recovery_event_confirm_v1".to_string(),
            version: "1.0.0".to_string(),
            description:
                "Financial quality core with valuation, earnings-recovery, and event confirmation"
                    .to_string(),
            sources: vec![
                source("phase7_financial_quality_v1", Decimal::new(60, 2)),
                source("phase7_valuation_v1", Decimal::new(20, 2)),
                source("phase7_growth_recovery_v1", Decimal::new(15, 2)),
                source("phase7_event_earnings_v1", Decimal::new(5, 2)),
            ],
        },
    ]
}

fn professional_breakthrough_seed_trials() -> Vec<Value> {
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

fn professional_risk_breakthrough_seed_trials() -> Vec<Value> {
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

fn professional_sharpe_stabilization_seed_trials() -> Vec<Value> {
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

fn append_unique_seeds(seeds: &mut Vec<Value>, candidates: Vec<Value>) {
    for seed in candidates {
        if !seeds.contains(&seed) {
            seeds.push(seed);
        }
    }
}

fn phase7_u2_exact_anchor_seed_trials() -> Vec<Value> {
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

fn add_turnover_smoothing_variants(seeds: &mut Vec<Value>, anchors: &[Value]) {
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

fn is_turnover_smoothing_anchor_seed(seed: &Value) -> bool {
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

fn professional_regime_stabilization_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_sharpe_stabilization_seed_trials() {
        let is_quality_mainline = seed["combo_name"] == "phase7_financial_quality_v1"
            && seed["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
            && seed["stop_loss_pct"] == "0.075"
            && seed["reentry_cooldown_days"] == 30;
        let volatility_control = seed["portfolio_volatility_control"]
            .as_str()
            .unwrap_or("off");
        let keeps_phase7_t_volatility_shape =
            volatility_control == "vol120_24_70_100" || volatility_control == "vol120_22_65_100";

        if !is_quality_mainline || !keeps_phase7_t_volatility_shape {
            continue;
        }

        for market_regime in [
            "quality_crash_guard_v1",
            "quality_crash_guard_v2",
            "quality_crash_guard_v3",
        ] {
            let mut regime_seed = seed.clone();
            regime_seed["market_regime"] = json!(market_regime);
            seeds.push(regime_seed);
        }
    }
    seeds
}

fn professional_bear_window_stabilization_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_regime_stabilization_seed_trials() {
        for market_regime in [
            "quality_bear_window_guard_v1",
            "quality_bear_window_guard_v2",
        ] {
            let mut bear_seed = seed.clone();
            bear_seed["market_regime"] = json!(market_regime);
            if !seeds.contains(&bear_seed) {
                seeds.push(bear_seed);
            }
        }
    }
    seeds
}

fn professional_style_risk_budget_seed_trials() -> Vec<Value> {
    let mut seeds = Vec::new();
    for seed in professional_bear_window_stabilization_seed_trials() {
        for style_risk_budget in [
            "liquidity_volatility_balanced_v1",
            "defensive_style_budget_v1",
        ] {
            let mut style_seed = seed.clone();
            style_seed["style_risk_budget"] = json!(style_risk_budget);
            if !seeds.contains(&style_seed) {
                seeds.push(style_seed);
            }
        }
    }
    seeds
}

fn retarget_seed_combo(mut seed: Value, combo_name: &str) -> Value {
    seed["combo_name"] = json!(combo_name);
    seed
}

fn set_seed_score_direction(mut seed: Value, direction: ScoreDirection) -> Value {
    seed["score_direction"] = json!(direction.as_str());
    seed
}

fn with_event_window_gate_seed(
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

fn with_event_surprise_gate_seed(
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

fn with_event_combo_gate_seed(
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

fn with_event_combo_gate_seed_with_min_score(
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

fn with_event_combo_gate_seed_active_in(
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

fn with_market_regime_seed(mut seed: Value, market_regime: &str) -> Value {
    seed["market_regime"] = json!(market_regime);
    seed
}

#[allow(clippy::too_many_arguments)]
fn with_drawdown_profile_seed(
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

fn with_style_risk_budget_seed(mut seed: Value, style_risk_budget: &str) -> Value {
    seed["style_risk_budget"] = json!(style_risk_budget);
    seed
}

fn with_portfolio_method_seed(
    mut seed: Value,
    portfolio_method: &str,
    risk_budget_lookback_days: usize,
) -> Value {
    seed["portfolio_method"] = json!(portfolio_method);
    seed["risk_budget_lookback_days"] = json!(risk_budget_lookback_days);
    seed
}

fn with_volatility_profile_seed(
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

fn with_candidate_risk_filter_seed(mut seed: Value, candidate_risk_filter: &str) -> Value {
    seed["candidate_risk_filter"] = json!(candidate_risk_filter);
    seed
}

fn with_risk_contribution_control_seed(mut seed: Value, risk_contribution_control: &str) -> Value {
    seed["risk_contribution_control"] = json!(risk_contribution_control);
    seed
}

fn professional_anti_overfit_sharpe_seed_trials() -> Vec<Value> {
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

fn professional_candidate_risk_filter_seed_trials() -> Vec<Value> {
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

fn professional_risk_contribution_seed_trials() -> Vec<Value> {
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

fn professional_event_conditioned_sharpe_seed_trials() -> Vec<Value> {
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

fn professional_second_alpha_source_seed_trials() -> Vec<Value> {
    let style_seeds = professional_style_risk_budget_seed_trials();
    let mut seeds = Vec::new();
    let combos = [
        "phase7_event_earnings_v1",
        "phase7_event_surprise_v1",
        "phase7_event_window_earnings_v1",
        "phase7_quality_event_window_overlay_v1",
        "phase7_quality_event_surprise_confirm_v1",
        "phase7_quality_value_recovery_confirm_v1",
        "phase7_quality_value_recovery_event_confirm_v1",
        "phase7_financial_quality_v1",
        "phase7_quality_moneyflow_pos_5pct_v1",
        "phase7_quality_event_confirm_v1",
        "phase7_blend_quality_growth_v1",
        "phase7_blend_recovery_tilt_v1",
    ];
    let direction_sweep_combos = [
        "phase7_quality_value_recovery_confirm_v1",
        "phase7_event_earnings_v1",
        "phase7_event_surprise_v1",
        "phase7_event_window_earnings_v1",
        "phase7_quality_event_window_overlay_v1",
        "phase7_quality_event_surprise_confirm_v1",
        "phase7_quality_event_confirm_v1",
        "phase7_quality_value_recovery_event_confirm_v1",
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
        for combo_name in [
            "phase7_event_earnings_v1",
            "phase7_event_surprise_v1",
            "phase7_event_window_earnings_v1",
            "phase7_quality_event_window_overlay_v1",
            "phase7_quality_event_surprise_confirm_v1",
            "phase7_quality_event_confirm_v1",
            "phase7_quality_value_recovery_event_confirm_v1",
        ] {
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

fn professional_residual_quality_seed_trials() -> Vec<Value> {
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

fn professional_residual_overlay_sharpe_seed_trials() -> Vec<Value> {
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

fn professional_conditioned_second_alpha_seed_trials() -> Vec<Value> {
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

fn professional_valuation_guard_sharpe_seed_trials() -> Vec<Value> {
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

fn professional_regime_conditioned_valuation_guard_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let stress_regimes = ["bear", "high_volatility"];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            vol22_anchor.clone(),
            "valuation_exclude_bottom35",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.35",
            "0",
            ScoreDirection::Descending,
        )],
    );
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_active_in(
            vol22_anchor,
            "valuation_exclude_bottom35_stress_only",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.35",
            ScoreDirection::Descending,
            &stress_regimes,
        )],
    );

    append_unique_seeds(&mut seeds, vec![vol18_anchor.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            vol18_anchor.clone(),
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
            "0",
            ScoreDirection::Descending,
        )],
    );
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_active_in(
            vol18_anchor,
            "valuation_exclude_bottom40_stress_only",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
            ScoreDirection::Descending,
            &stress_regimes,
        )],
    );

    seeds
}

fn professional_regime_alpha_routing_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let routed_vol22 =
        with_market_regime_seed(vol22_anchor.clone(), "quality_regime_alpha_switch_v1");
    let routed_vol18 =
        with_market_regime_seed(vol18_anchor.clone(), "quality_regime_alpha_switch_v1");
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone(), routed_vol22.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            routed_vol22,
            "valuation_exclude_bottom35",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.35",
            "0",
            ScoreDirection::Descending,
        )],
    );

    append_unique_seeds(&mut seeds, vec![vol18_anchor.clone(), routed_vol18.clone()]);
    append_unique_seeds(
        &mut seeds,
        vec![with_event_combo_gate_seed_with_min_score(
            routed_vol18,
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            "0.40",
            "0",
            ScoreDirection::Descending,
        )],
    );

    seeds
}

fn professional_regime_alpha_sleeve_search_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    let stress_policies = [
        "quality_regime_alpha_switch_value_v1",
        "quality_regime_alpha_switch_recovery_v1",
        "quality_regime_alpha_switch_blend_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in stress_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

fn professional_regime_alpha_overlay_search_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let overlay_policies = [
        "quality_regime_alpha_overlay_value_05pct_v1",
        "quality_regime_alpha_overlay_value_10pct_v1",
        "quality_regime_alpha_overlay_blend_10pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in overlay_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

fn professional_regime_alpha_sleeve_allocation_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
        "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

fn professional_low_risk_sleeve_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1",
    ];
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![vol22_anchor.clone()]);
    for policy in sleeve_policies {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), policy)],
        );
    }

    seeds
}

fn professional_value_guard_sleeve_composition_seed_trials() -> Vec<Value> {
    let mut anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let Some(mut vol22_anchor) = anchor_seeds.drain(..).next() else {
        return Vec::new();
    };
    vol22_anchor["event_gate_profile"] = json!("off");
    let mut vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    vol18_anchor["event_gate_profile"] = json!("off");

    let value35_vol22 = with_event_combo_gate_seed_with_min_score(
        vol22_anchor,
        "valuation_exclude_bottom35",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.35",
        "0",
        ScoreDirection::Descending,
    );
    let value40_vol18 = with_event_combo_gate_seed_with_min_score(
        vol18_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let sleeve_policies = [
        "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
    ];
    let guarded_anchors = [value35_vol22, value40_vol18];
    let mut seeds = Vec::new();

    for seed in &guarded_anchors {
        append_unique_seeds(&mut seeds, vec![seed.clone()]);
        for policy in sleeve_policies {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), policy)],
            );
        }
    }

    seeds
}

fn professional_nearest_candidate_risk_model_seed_trials() -> Vec<Value> {
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
    let nearest_candidate = with_market_regime_seed(
        value40_anchor.clone(),
        "quality_regime_alpha_portfolio_sleeve_value_15pct_v1",
    );
    let anchors = [value40_anchor, nearest_candidate];
    let mut seeds = Vec::new();

    for seed in &anchors {
        append_unique_seeds(
            &mut seeds,
            vec![with_portfolio_method_seed(seed.clone(), "risk_budget", 120)],
        );
        for lookback_days in [120, 180] {
            append_unique_seeds(
                &mut seeds,
                vec![with_portfolio_method_seed(
                    seed.clone(),
                    "min_variance",
                    lookback_days,
                )],
            );
        }
    }

    seeds
}

fn professional_event_regime_sleeve_seed_trials() -> Vec<Value> {
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

fn professional_event_window_sleeve_weight_seed_trials() -> Vec<Value> {
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

fn professional_event_window_sleeve_upper_bound_seed_trials() -> Vec<Value> {
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

fn professional_event_window_regime_placement_seed_trials() -> Vec<Value> {
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

fn professional_event_window_decay_seed_trials() -> Vec<Value> {
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

fn professional_volatility_sharpe_seed_trials() -> Vec<Value> {
    let anchor_seeds = phase7_u2_exact_anchor_seed_trials();
    let tighter_profiles = [
        ("vol120_20_60_100", "0.20", 120, "0.60", "1"),
        ("vol120_18_55_100", "0.18", 120, "0.55", "1"),
        ("vol252_16_45_100", "0.16", 252, "0.45", "1"),
    ];
    let mut seeds = Vec::new();

    for seed in &anchor_seeds {
        append_unique_seeds(&mut seeds, vec![seed.clone()]);
        for profile in tighter_profiles {
            append_unique_seeds(
                &mut seeds,
                vec![with_volatility_profile_seed(
                    seed.clone(),
                    profile.0,
                    profile.1,
                    profile.2,
                    profile.3,
                    profile.4,
                )],
            );
        }
    }

    seeds
}

fn professional_regime_position_sharpe_seed_trials() -> Vec<Value> {
    let mut anchors = phase7_u2_exact_anchor_seed_trials();
    let Some(vol22_anchor) = anchors.drain(..).next() else {
        return Vec::new();
    };
    let vol18_anchor = with_volatility_profile_seed(
        vol22_anchor.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let mut seeds = Vec::new();

    for market_regime in [
        "quality_bear_window_guard_v2",
        "quality_bear_position_guard_v1",
        "quality_bear_position_guard_v2",
        "quality_crash_guard_v3",
        "quality_risk_off_v1",
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol18_anchor.clone(), market_regime)],
        );
    }

    for market_regime in [
        "quality_bear_position_guard_v1",
        "quality_bear_position_guard_v2",
        "quality_bear_window_guard_v2",
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_market_regime_seed(vol22_anchor.clone(), market_regime)],
        );
    }

    seeds
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScoreDirection {
    Descending,
    Ascending,
}

impl ScoreDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Descending => "descending",
            Self::Ascending => "ascending",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayeredSearchConfig {
    pub market_regime_policies: Vec<String>,
    pub combo_versions: Vec<ComboVersion>,
    pub prediction_set_ids: Vec<String>,
    pub top_n: Vec<usize>,
    pub rebalance_days: Vec<usize>,
    pub score_directions: Vec<ScoreDirection>,
    pub skip_top_pct: Vec<Decimal>,
    pub max_pairwise_correlation: Vec<Decimal>,
    pub kelly_fraction: Vec<Decimal>,
    pub max_position_pct: Vec<Decimal>,
    pub max_gross_exposure: Vec<Decimal>,
    pub portfolio_methods: Vec<String>,
    pub risk_budget_lookback_days: Vec<usize>,
    pub capacity_penalty_strength: Vec<Decimal>,
    pub industry_max_weight_pct: Vec<Option<Decimal>>,
    pub style_risk_budget_profiles: Vec<String>,
    pub candidate_risk_filter_profiles: Vec<String>,
    pub risk_contribution_control_profiles: Vec<String>,
    pub rebalance_hysteresis_pct: Vec<Decimal>,
    pub partial_rebalance_ratio: Vec<Decimal>,
    pub score_candidate_pool_sizes: Vec<usize>,
    pub universe_profiles: Vec<String>,
    pub portfolio_drawdown_controls: Vec<PortfolioDrawdownControlProfile>,
    pub portfolio_volatility_controls: Vec<PortfolioVolatilityControlProfile>,
    pub position_risk_controls: Vec<PositionRiskControlProfile>,
    pub event_gate_profiles: Vec<EventGateProfile>,
    pub seed_trials: Vec<Value>,
    pub correlation_lookback_days: usize,
    pub kelly_lookback_days: usize,
    pub benchmark: String,
}

impl LayeredSearchConfig {
    pub fn local_professional_default() -> Self {
        let mut combo_versions = vec![
            ComboVersion::new("full_icir_16f_v3", "1.0.0"),
            ComboVersion::new("full_icir_16f_v2", "20260511"),
            ComboVersion::new("full_icir_16f", "1.0.0"),
            ComboVersion::new("full_eq_16f", "1.0.0"),
            ComboVersion::new("phase7_price_volume_expanded_v1", "1.0.0"),
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_industry_residual_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_valuation_v1", "1.0.0"),
            ComboVersion::new("phase7_moneyflow_v1", "1.0.0"),
            ComboVersion::new("phase7_event_earnings_v1", "1.0.0"),
            ComboVersion::new("phase7_event_surprise_v1", "1.0.0"),
            ComboVersion::new("phase7_event_window_earnings_v1", "1.0.0"),
        ];
        combo_versions.extend(
            phase7_alpha_blend_profiles()
                .into_iter()
                .map(|profile| profile.combo_version()),
        );

        Self {
            market_regime_policies: vec![
                "off".to_string(),
                "professional_default".to_string(),
                "drawdown_control_v1".to_string(),
                "drawdown_control_v2".to_string(),
                "quality_risk_off_v1".to_string(),
                "quality_crash_guard_v1".to_string(),
            ],
            combo_versions,
            prediction_set_ids: Vec::new(),
            top_n: vec![20, 30, 50, 80],
            rebalance_days: vec![5, 10, 20, 60],
            score_directions: vec![ScoreDirection::Descending, ScoreDirection::Ascending],
            skip_top_pct: vec![Decimal::ZERO, Decimal::new(5, 2), Decimal::new(10, 2)],
            max_pairwise_correlation: vec![
                Decimal::new(65, 2),
                Decimal::new(75, 2),
                Decimal::new(90, 2),
            ],
            kelly_fraction: vec![Decimal::ZERO, Decimal::new(25, 2), Decimal::new(50, 2)],
            max_position_pct: vec![Decimal::new(5, 2), Decimal::new(8, 2), Decimal::new(10, 2)],
            max_gross_exposure: vec![Decimal::new(80, 2), Decimal::ONE],
            portfolio_methods: vec!["heuristic".to_string(), "risk_budget".to_string()],
            risk_budget_lookback_days: vec![60, 120],
            capacity_penalty_strength: vec![Decimal::ZERO, Decimal::new(75, 2)],
            industry_max_weight_pct: vec![
                None,
                Some(Decimal::new(20, 2)),
                Some(Decimal::new(35, 2)),
            ],
            style_risk_budget_profiles: vec!["off".to_string()],
            candidate_risk_filter_profiles: vec!["off".to_string()],
            risk_contribution_control_profiles: vec!["off".to_string()],
            rebalance_hysteresis_pct: vec![Decimal::ZERO],
            partial_rebalance_ratio: vec![Decimal::ONE],
            score_candidate_pool_sizes: vec![0, 200, 500],
            universe_profiles: vec![
                "all".to_string(),
                "listed_non_st".to_string(),
                "main_board_non_st".to_string(),
            ],
            portfolio_drawdown_controls: vec![
                PortfolioDrawdownControlProfile::off(),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling252_10_25_50",
                    Decimal::new(10, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                    Some(252),
                ),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling252_12_30_60",
                    Decimal::new(12, 2),
                    Decimal::new(30, 2),
                    Decimal::new(60, 2),
                    Some(252),
                ),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling504_15_35_70",
                    Decimal::new(15, 2),
                    Decimal::new(35, 2),
                    Decimal::new(70, 2),
                    Some(504),
                ),
                PortfolioDrawdownControlProfile::recover(
                    "recover252_10_25_50_30_70",
                    Decimal::new(10, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                    Some(252),
                    Decimal::new(30, 2),
                    Decimal::new(70, 2),
                    Decimal::ONE,
                ),
                PortfolioDrawdownControlProfile::recover(
                    "recover252_10_27_50_30_70",
                    Decimal::new(10, 2),
                    Decimal::new(27, 2),
                    Decimal::new(50, 2),
                    Some(252),
                    Decimal::new(30, 2),
                    Decimal::new(70, 2),
                    Decimal::ONE,
                ),
                PortfolioDrawdownControlProfile::recover(
                    "recover252_12_30_60_30_70",
                    Decimal::new(12, 2),
                    Decimal::new(30, 2),
                    Decimal::new(60, 2),
                    Some(252),
                    Decimal::new(30, 2),
                    Decimal::new(70, 2),
                    Decimal::ONE,
                ),
            ],
            portfolio_volatility_controls: vec![
                PortfolioVolatilityControlProfile::off(),
                PortfolioVolatilityControlProfile::target(
                    "vol252_16_45_100",
                    Decimal::new(16, 2),
                    252,
                    Decimal::new(45, 2),
                    Decimal::ONE,
                ),
                PortfolioVolatilityControlProfile::target(
                    "vol120_18_50_100",
                    Decimal::new(18, 2),
                    120,
                    Decimal::new(50, 2),
                    Decimal::ONE,
                ),
                PortfolioVolatilityControlProfile::target(
                    "vol60_20_60_100",
                    Decimal::new(20, 2),
                    60,
                    Decimal::new(60, 2),
                    Decimal::ONE,
                ),
            ],
            position_risk_controls: vec![PositionRiskControlProfile::off()],
            event_gate_profiles: vec![EventGateProfile::off()],
            seed_trials: Vec::new(),
            correlation_lookback_days: 60,
            kelly_lookback_days: 60,
            benchmark: "000300.SH".to_string(),
        }
    }

    pub fn professional_breakthrough_default() -> Self {
        let mut config = Self::local_professional_default();
        config.market_regime_policies = vec![
            "off".to_string(),
            "quality_risk_off_v1".to_string(),
            "quality_crash_guard_v1".to_string(),
        ];
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_recovery_tilt_v1", "1.0.0"),
        ];
        config.top_n = vec![20, 30, 50];
        config.rebalance_days = vec![40, 60, 80];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.skip_top_pct = vec![Decimal::new(10, 2), Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2), Decimal::new(90, 2)];
        config.kelly_fraction = vec![Decimal::ZERO, Decimal::new(25, 2), Decimal::new(50, 2)];
        config.max_position_pct = vec![
            Decimal::new(8, 2),
            Decimal::new(10, 2),
            Decimal::new(12, 2),
            Decimal::new(15, 2),
        ];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.risk_budget_lookback_days = vec![60, 120];
        config.capacity_penalty_strength = vec![Decimal::ZERO, Decimal::new(75, 2)];
        config.industry_max_weight_pct =
            vec![None, Some(Decimal::new(20, 2)), Some(Decimal::new(35, 2))];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 800, 1200];
        config.universe_profiles = vec![
            "all".to_string(),
            "listed_non_st".to_string(),
            "main_board_non_st".to_string(),
        ];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::off(),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_25_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(25, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_27_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(27, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::off(),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_50_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol60_20_60_100",
                Decimal::new(20, 2),
                60,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::off()];
        config.seed_trials = professional_breakthrough_seed_trials();
        config
    }

    pub fn professional_risk_breakthrough_default() -> Self {
        let mut config = Self::professional_breakthrough_default();
        config.market_regime_policies = vec!["quality_crash_guard_v1".to_string()];
        config
            .market_regime_policies
            .push("quality_crash_guard_v2".to_string());
        config
            .market_regime_policies
            .push("quality_crash_guard_v3".to_string());
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_value_quality_growth_rel_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_value_tilt_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_defensive_rel_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_recovery_tilt_v1", "1.0.0"),
        ];
        config.top_n = vec![20, 25, 30];
        config.rebalance_days = vec![50, 60, 70, 80];
        config.skip_top_pct = vec![
            Decimal::new(10, 2),
            Decimal::new(12, 2),
            Decimal::new(15, 2),
        ];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(75, 2),
            Decimal::new(90, 2),
        ];
        config.kelly_fraction = vec![Decimal::ZERO, Decimal::new(25, 2)];
        config.max_position_pct =
            vec![Decimal::new(8, 2), Decimal::new(10, 2), Decimal::new(12, 2)];
        config.max_gross_exposure = vec![Decimal::new(80, 2), Decimal::new(90, 2), Decimal::ONE];
        config.risk_budget_lookback_days = vec![120, 180];
        config.capacity_penalty_strength =
            vec![Decimal::new(75, 2), Decimal::ONE, Decimal::new(125, 2)];
        config.industry_max_weight_pct = vec![
            Some(Decimal::new(20, 2)),
            Some(Decimal::new(25, 2)),
            Some(Decimal::new(30, 2)),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 800];
        config.universe_profiles = vec!["all".to_string(), "listed_non_st".to_string()];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_25_60_25_75",
                Decimal::new(8, 2),
                Decimal::new(25, 2),
                Decimal::new(60, 2),
                Some(252),
                Decimal::new(25, 2),
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_30_55_25_75",
                Decimal::new(10, 2),
                Decimal::new(30, 2),
                Decimal::new(55, 2),
                Some(252),
                Decimal::new(25, 2),
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_27_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(27, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_26_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(26, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_09_26_50_30_70",
                Decimal::new(9, 2),
                Decimal::new(26, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_09_24_45_30_70",
                Decimal::new(9, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_24_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::off(),
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol60_25_75_100",
                Decimal::new(25, 2),
                60,
                Decimal::new(75, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_30_85_100",
                Decimal::new(30, 2),
                120,
                Decimal::new(85, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol60_30_90_100",
                Decimal::new(30, 2),
                60,
                Decimal::new(90, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![
            PositionRiskControlProfile::off(),
            PositionRiskControlProfile::stop_loss("stop_loss_07", Decimal::new(7, 2)),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_5",
                Decimal::new(7, 2),
                5,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_10",
                Decimal::new(7, 2),
                10,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_20",
                Decimal::new(7, 2),
                20,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_07_cooldown_30",
                Decimal::new(7, 2),
                30,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_065_cooldown_20",
                Decimal::new(65, 3),
                20,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_20",
                Decimal::new(75, 3),
                20,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_30",
                Decimal::new(75, 3),
                30,
            ),
            PositionRiskControlProfile::stop_loss("stop_loss_08", Decimal::new(8, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_085", Decimal::new(85, 3)),
            PositionRiskControlProfile::stop_loss("stop_loss_09", Decimal::new(9, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_095", Decimal::new(95, 3)),
            PositionRiskControlProfile::stop_loss("stop_loss_10", Decimal::new(10, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_12", Decimal::new(12, 2)),
            PositionRiskControlProfile::stop_loss("stop_loss_14", Decimal::new(14, 2)),
            PositionRiskControlProfile::trailing_stop("trailing_stop_18", Decimal::new(18, 2)),
        ];
        config.seed_trials = professional_risk_breakthrough_seed_trials();
        config
    }

    pub fn professional_sharpe_stabilization_default() -> Self {
        let mut config = Self::professional_risk_breakthrough_default();
        config.rebalance_hysteresis_pct = vec![
            Decimal::ZERO,
            Decimal::new(5, 3),
            Decimal::new(1, 2),
            Decimal::new(2, 2),
        ];
        config.partial_rebalance_ratio =
            vec![Decimal::ONE, Decimal::new(75, 2), Decimal::new(50, 2)];
        config.seed_trials = professional_sharpe_stabilization_seed_trials();
        config
    }

    pub fn professional_regime_stabilization_default() -> Self {
        let mut config = Self::professional_sharpe_stabilization_default();
        config.market_regime_policies = vec![
            "quality_crash_guard_v1".to_string(),
            "quality_crash_guard_v2".to_string(),
            "quality_crash_guard_v3".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2), Decimal::new(90, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.risk_budget_lookback_days = vec![120];
        config.capacity_penalty_strength = vec![Decimal::new(75, 2)];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500];
        config.universe_profiles = vec!["all".to_string()];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_10_24_50_30_70",
            Decimal::new(10, 2),
            Decimal::new(24, 2),
            Decimal::new(50, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::stop_loss_with_cooldown(
            "stop_loss_075_cooldown_30",
            Decimal::new(75, 3),
            30,
        )];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_stabilization_seed_trials();
        config
    }

    pub fn professional_bear_window_stabilization_default() -> Self {
        let mut config = Self::professional_regime_stabilization_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
        ];
        config.seed_trials = professional_bear_window_stabilization_seed_trials();
        config
    }

    pub fn professional_style_risk_budget_default() -> Self {
        let mut config = Self::professional_bear_window_stabilization_default();
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
            "defensive_style_budget_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_style_risk_budget_seed_trials();
        config
    }

    pub fn professional_second_alpha_source_default() -> Self {
        let mut config = Self::professional_style_risk_budget_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_event_earnings_v1", "1.0.0"),
            ComboVersion::new("phase7_event_surprise_v1", "1.0.0"),
            ComboVersion::new("phase7_event_window_earnings_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_window_overlay_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_surprise_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_industry_residual_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_recovery_tilt_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_boost_pos_5pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_require_positive",
                "require_positive",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_second_alpha_source_seed_trials();
        config
    }

    pub fn professional_residual_quality_default() -> Self {
        let mut config = Self::professional_style_risk_budget_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_industry_residual_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
        ];
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_residual_quality_seed_trials();
        config
    }

    pub fn professional_residual_overlay_sharpe_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
        ];
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_residual_overlay_sharpe_seed_trials();
        config
    }

    pub fn professional_conditioned_second_alpha_default() -> Self {
        let mut config = Self::professional_volatility_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_require_top40",
                "phase7_valuation_v1",
                "require_positive",
                Decimal::new(60, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "residual_require_top40",
                "phase7_industry_residual_quality_v1",
                "require_positive",
                Decimal::new(60, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "moneyflow_require_top40",
                "phase7_moneyflow_v1",
                "require_positive",
                Decimal::new(60, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_conditioned_second_alpha_seed_trials();
        config
    }

    pub fn professional_valuation_guard_sharpe_default() -> Self {
        let mut config = Self::professional_volatility_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom25",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(25, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom30",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(30, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom50",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(50, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_valuation_guard_sharpe_seed_trials();
        config
    }

    pub fn professional_regime_conditioned_valuation_guard_default() -> Self {
        let mut config = Self::professional_valuation_guard_sharpe_default();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35_stress_only",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40_stress_only",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
        ];
        config.seed_trials = professional_regime_conditioned_valuation_guard_seed_trials();
        config
    }

    pub fn professional_regime_alpha_routing_default() -> Self {
        let mut config = Self::professional_regime_conditioned_valuation_guard_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_switch_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_regime_alpha_routing_seed_trials();
        config
    }

    pub fn professional_regime_alpha_sleeve_search_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_switch_value_v1".to_string(),
            "quality_regime_alpha_switch_recovery_v1".to_string(),
            "quality_regime_alpha_switch_blend_v1".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_alpha_sleeve_search_seed_trials();
        config
    }

    pub fn professional_regime_alpha_overlay_search_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_overlay_value_05pct_v1".to_string(),
            "quality_regime_alpha_overlay_value_10pct_v1".to_string(),
            "quality_regime_alpha_overlay_blend_10pct_v1".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_alpha_overlay_search_seed_trials();
        config
    }

    pub fn professional_regime_alpha_sleeve_allocation_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_blend_10pct_v1".to_string(),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_alpha_sleeve_allocation_seed_trials();
        config
    }

    pub fn professional_low_risk_sleeve_default() -> Self {
        let mut config = Self::professional_regime_alpha_routing_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_low_risk_sleeve_seed_trials();
        config
    }

    pub fn professional_value_guard_sleeve_composition_default() -> Self {
        let mut config = Self::professional_valuation_guard_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom35",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.seed_trials = professional_value_guard_sleeve_composition_seed_trials();
        config
    }

    pub fn professional_nearest_candidate_risk_model_default() -> Self {
        let mut config = Self::professional_value_guard_sleeve_composition_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
        ];
        config.portfolio_methods = vec!["risk_budget".to_string(), "min_variance".to_string()];
        config.risk_budget_lookback_days = vec![120, 180];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_18_55_100",
            Decimal::new(18, 2),
            120,
            Decimal::new(55, 2),
            Decimal::ONE,
        )];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_nearest_candidate_risk_model_seed_trials();
        config
    }

    pub fn professional_event_regime_sleeve_default() -> Self {
        let mut config = Self::professional_nearest_candidate_risk_model_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1".to_string(),
        ];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.risk_budget_lookback_days = vec![120];
        config.seed_trials = professional_event_regime_sleeve_seed_trials();
        config
    }

    pub fn professional_event_window_sleeve_weight_default() -> Self {
        let mut config = Self::professional_event_regime_sleeve_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_05pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_sleeve_weight_seed_trials();
        config
    }

    pub fn professional_event_window_sleeve_upper_bound_default() -> Self {
        let mut config = Self::professional_event_window_sleeve_weight_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_sleeve_upper_bound_seed_trials();
        config
    }

    pub fn professional_event_window_regime_placement_default() -> Self {
        let mut config = Self::professional_event_window_sleeve_upper_bound_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_regime_placement_seed_trials();
        config
    }

    pub fn professional_event_window_decay_default() -> Self {
        let mut config = Self::professional_event_window_regime_placement_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1".to_string(),
        ];
        config.seed_trials = professional_event_window_decay_seed_trials();
        config
    }

    pub fn professional_volatility_sharpe_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol252_16_45_100",
                Decimal::new(16, 2),
                252,
                Decimal::new(45, 2),
                Decimal::ONE,
            ),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_volatility_sharpe_seed_trials();
        config
    }

    pub fn professional_regime_position_sharpe_default() -> Self {
        let mut config = Self::professional_volatility_sharpe_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_bear_position_guard_v1".to_string(),
            "quality_bear_position_guard_v2".to_string(),
            "quality_crash_guard_v3".to_string(),
            "quality_risk_off_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
        ];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_regime_position_sharpe_seed_trials();
        config
    }

    pub fn professional_anti_overfit_sharpe_default() -> Self {
        let mut config = Self::professional_bear_window_stabilization_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_surprise_confirm_v1", "1.0.0"),
        ];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v1".to_string(),
            "quality_bear_window_guard_v2".to_string(),
            "quality_crash_guard_v3".to_string(),
        ];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.risk_budget_lookback_days = vec![120];
        config.capacity_penalty_strength = vec![Decimal::new(75, 2), Decimal::ONE];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec![
            "off".to_string(),
            "liquidity_volatility_balanced_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500];
        config.universe_profiles = vec!["all".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_10_24_50_30_70",
                Decimal::new(10, 2),
                Decimal::new(24, 2),
                Decimal::new(50, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_09_24_45_30_70",
                Decimal::new(9, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_24_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(24, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_22_65_100",
                Decimal::new(22, 2),
                120,
                Decimal::new(65, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_24_70_100",
                Decimal::new(24, 2),
                120,
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::stop_loss_with_cooldown(
            "stop_loss_075_cooldown_30",
            Decimal::new(75, 3),
            30,
        )];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_boost_pos_5pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_anti_overfit_sharpe_seed_trials();
        config
    }

    pub fn professional_candidate_risk_filter_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "low_volatility_v1".to_string(),
            "low_volatility_low_correlation_v1".to_string(),
        ];
        config.seed_trials = professional_candidate_risk_filter_seed_trials();
        config
    }

    pub fn professional_risk_contribution_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.risk_contribution_control_profiles = vec![
            "off".to_string(),
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_risk_contribution_seed_trials();
        config
    }

    pub fn professional_event_conditioned_sharpe_default() -> Self {
        let mut config = Self::professional_anti_overfit_sharpe_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_surprise_confirm_v1", "1.0.0"),
        ];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_3pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_5pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_require_positive",
                "require_positive",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_boost_pos_3pct",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_exclude_negative",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.seed_trials = professional_event_conditioned_sharpe_seed_trials();
        config
    }

    pub fn search_space_size(&self) -> usize {
        [
            self.market_regime_policies.len(),
            self.signal_candidate_count(),
            self.top_n.len(),
            self.rebalance_days.len(),
            self.score_directions.len(),
            self.skip_top_pct.len(),
            self.max_pairwise_correlation.len(),
            self.kelly_fraction.len(),
            self.max_position_pct.len(),
            self.max_gross_exposure.len(),
            self.portfolio_methods.len(),
            self.risk_budget_lookback_days.len(),
            self.capacity_penalty_strength.len(),
            self.industry_max_weight_pct.len(),
            self.style_risk_budget_profiles.len(),
            self.candidate_risk_filter_profiles.len(),
            self.risk_contribution_control_profiles.len(),
            self.rebalance_hysteresis_pct.len(),
            self.partial_rebalance_ratio.len(),
            self.score_candidate_pool_sizes.len(),
            self.universe_profiles.len(),
            self.portfolio_drawdown_controls.len(),
            self.portfolio_volatility_controls.len(),
            self.position_risk_controls.len(),
            self.event_gate_profiles.len(),
        ]
        .into_iter()
        .fold(1usize, |total, size| total.saturating_mul(size))
    }

    fn signal_candidate_count(&self) -> usize {
        self.combo_versions
            .len()
            .saturating_add(self.prediction_set_ids.len())
    }
}

#[derive(Debug, Clone)]
enum LayeredSignalCandidate<'a> {
    FactorCombo(&'a ComboVersion),
    ModelPrediction(&'a str),
}

impl LayeredSearchConfig {
    fn signal_candidate(&self, index: usize) -> Option<LayeredSignalCandidate<'_>> {
        if index < self.combo_versions.len() {
            self.combo_versions
                .get(index)
                .map(LayeredSignalCandidate::FactorCombo)
        } else {
            self.prediction_set_ids
                .get(index.saturating_sub(self.combo_versions.len()))
                .map(|value| LayeredSignalCandidate::ModelPrediction(value.as_str()))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayeredSearchTrial {
    pub trial_id: String,
    pub trial_index: usize,
    pub batch_index: usize,
    pub parameters: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayeredSearchPlan {
    pub requested_trials: usize,
    pub planned_trials: usize,
    pub truncated: bool,
    pub batch_size: usize,
    pub max_parallel_trials: usize,
    pub trials: Vec<LayeredSearchTrial>,
}

pub fn build_layered_search_plan(
    config: &LayeredSearchConfig,
    resource_plan: &LocalResourcePlan,
) -> LayeredSearchPlan {
    let cartesian_trials = config.search_space_size();
    let requested_trials = cartesian_trials.saturating_add(config.seed_trials.len());
    let max_trials = resource_plan.max_trials;
    let batch_size = resource_plan.batch_size.max(1);
    let mut trials = Vec::with_capacity(requested_trials.min(max_trials));

    for seed in config.seed_trials.iter().take(max_trials) {
        let trial_index = trials.len();
        trials.push(LayeredSearchTrial {
            trial_id: format!("phase7d-{:06}", trial_index + 1),
            trial_index,
            batch_index: trial_index / batch_size + 1,
            parameters: seed.clone(),
        });
    }

    let remaining_trials = max_trials.saturating_sub(trials.len());
    for source_index in selected_cartesian_indices(cartesian_trials, remaining_trials) {
        let Some(indices) = LayeredTrialIndices::from_flat_index(config, source_index) else {
            continue;
        };

        let market_regime_policy = &config.market_regime_policies[indices.market_regime_policy];
        let signal_candidate = config.signal_candidate(indices.signal_candidate);
        let top_n = config.top_n[indices.top_n];
        let rebalance = config.rebalance_days[indices.rebalance_days];
        let direction = config.score_directions[indices.score_direction];
        let skip_top_pct = config.skip_top_pct[indices.skip_top_pct];
        let max_pairwise_correlation =
            config.max_pairwise_correlation[indices.max_pairwise_correlation];
        let kelly_fraction = config.kelly_fraction[indices.kelly_fraction];
        let max_position_pct = config.max_position_pct[indices.max_position_pct];
        let max_gross_exposure = config.max_gross_exposure[indices.max_gross_exposure];
        let portfolio_method = &config.portfolio_methods[indices.portfolio_method];
        let risk_budget_lookback_days =
            config.risk_budget_lookback_days[indices.risk_budget_lookback_days];
        let capacity_penalty_strength =
            config.capacity_penalty_strength[indices.capacity_penalty_strength];
        let industry_max_weight_pct =
            config.industry_max_weight_pct[indices.industry_max_weight_pct];
        let style_risk_budget_profile =
            &config.style_risk_budget_profiles[indices.style_risk_budget_profile];
        let candidate_risk_filter_profile =
            &config.candidate_risk_filter_profiles[indices.candidate_risk_filter_profile];
        let risk_contribution_control_profile =
            &config.risk_contribution_control_profiles[indices.risk_contribution_control_profile];
        let rebalance_hysteresis_pct =
            config.rebalance_hysteresis_pct[indices.rebalance_hysteresis_pct];
        let partial_rebalance_ratio =
            config.partial_rebalance_ratio[indices.partial_rebalance_ratio];
        let score_candidate_pool_size =
            config.score_candidate_pool_sizes[indices.score_candidate_pool_size];
        let universe_profile = &config.universe_profiles[indices.universe_profile];
        let portfolio_drawdown_control =
            &config.portfolio_drawdown_controls[indices.portfolio_drawdown_control];
        let portfolio_volatility_control =
            &config.portfolio_volatility_controls[indices.portfolio_volatility_control];
        let position_risk_control = &config.position_risk_controls[indices.position_risk_control];
        let event_gate_profile = &config.event_gate_profiles[indices.event_gate_profile];

        let trial_index = trials.len();
        let mut parameters = serde_json::json!({
            "market_regime": market_regime_policy,
            "top_n": top_n,
            "rebalance": rebalance.to_string(),
            "score_direction": direction.as_str(),
            "skip_top_pct": decimal_string(skip_top_pct),
            "max_pairwise_correlation": decimal_string(max_pairwise_correlation),
            "correlation_lookback_days": config.correlation_lookback_days,
            "kelly_fraction": decimal_string(kelly_fraction),
            "kelly_lookback_days": config.kelly_lookback_days,
            "max_position_pct": decimal_string(max_position_pct),
            "max_gross_exposure": decimal_string(max_gross_exposure),
            "portfolio_method": portfolio_method,
            "risk_budget_lookback_days": risk_budget_lookback_days,
            "capacity_penalty_strength": decimal_string(capacity_penalty_strength),
            "industry_max_weight_pct": industry_max_weight_pct.map(decimal_string),
            "style_risk_budget": style_risk_budget_profile,
            "candidate_risk_filter": candidate_risk_filter_profile,
            "risk_contribution_control": risk_contribution_control_profile,
            "rebalance_hysteresis_pct": decimal_string(rebalance_hysteresis_pct),
            "partial_rebalance_ratio": decimal_string(partial_rebalance_ratio),
            "score_candidate_pool_size": score_candidate_pool_size,
            "universe_profile": universe_profile,
            "portfolio_drawdown_control": portfolio_drawdown_control.profile_name,
            "portfolio_volatility_control": portfolio_volatility_control.profile_name,
            "position_risk_control": position_risk_control.profile_name,
            "event_gate_profile": event_gate_profile.profile_name,
            "benchmark": config.benchmark,
        });
        if let (Some(start), Some(full), Some(min_exposure)) = (
            portfolio_drawdown_control.reduce_start_pct,
            portfolio_drawdown_control.reduce_full_pct,
            portfolio_drawdown_control.min_exposure,
        ) {
            parameters["portfolio_drawdown_reduce_start_pct"] =
                serde_json::json!(decimal_string(start));
            parameters["portfolio_drawdown_reduce_full_pct"] =
                serde_json::json!(decimal_string(full));
            parameters["portfolio_drawdown_min_exposure"] =
                serde_json::json!(decimal_string(min_exposure));
        }
        if let Some(lookback_days) = portfolio_drawdown_control.peak_lookback_days {
            parameters["portfolio_drawdown_peak_lookback_days"] = serde_json::json!(lookback_days);
        }
        if let (Some(start), Some(full)) = (
            portfolio_drawdown_control.recovery_start_pct,
            portfolio_drawdown_control.recovery_full_pct,
        ) {
            parameters["portfolio_drawdown_recovery_start_pct"] =
                serde_json::json!(decimal_string(start));
            parameters["portfolio_drawdown_recovery_full_pct"] =
                serde_json::json!(decimal_string(full));
        }
        if let Some(boost) = portfolio_drawdown_control.recovery_boost {
            parameters["portfolio_drawdown_recovery_boost"] =
                serde_json::json!(decimal_string(boost));
        }
        if let Some(target_pct) = portfolio_volatility_control.target_pct {
            parameters["portfolio_volatility_target_pct"] =
                serde_json::json!(decimal_string(target_pct));
        }
        if let Some(lookback_days) = portfolio_volatility_control.lookback_days {
            parameters["portfolio_volatility_lookback_days"] = serde_json::json!(lookback_days);
        }
        if let Some(min_exposure) = portfolio_volatility_control.min_exposure {
            parameters["portfolio_volatility_min_exposure"] =
                serde_json::json!(decimal_string(min_exposure));
        }
        if let Some(max_exposure) = portfolio_volatility_control.max_exposure {
            parameters["portfolio_volatility_max_exposure"] =
                serde_json::json!(decimal_string(max_exposure));
        }
        if let Some(stop_loss_pct) = position_risk_control.stop_loss_pct {
            parameters["stop_loss_pct"] = serde_json::json!(decimal_string(stop_loss_pct));
        }
        if let Some(take_profit_pct) = position_risk_control.take_profit_pct {
            parameters["take_profit_pct"] = serde_json::json!(decimal_string(take_profit_pct));
        }
        if let Some(trailing_stop_pct) = position_risk_control.trailing_stop_pct {
            parameters["trailing_stop_pct"] = serde_json::json!(decimal_string(trailing_stop_pct));
        }
        if let Some(time_stop_days) = position_risk_control.time_stop_days {
            parameters["time_stop_days"] = serde_json::json!(time_stop_days);
        }
        if let Some(reentry_cooldown_days) = position_risk_control.reentry_cooldown_days {
            parameters["reentry_cooldown_days"] = serde_json::json!(reentry_cooldown_days);
        }
        if let (Some(combo_name), Some(mode)) = (
            event_gate_profile.combo_name.as_ref(),
            event_gate_profile.mode.as_ref(),
        ) {
            parameters["event_gate_combo_name"] = serde_json::json!(combo_name);
            parameters["event_gate_version"] = serde_json::json!(event_gate_profile
                .version
                .clone()
                .unwrap_or_else(|| "1.0.0".to_string()));
            parameters["event_gate_mode"] = serde_json::json!(mode);
            if let Some(min_score) = event_gate_profile.min_score {
                parameters["event_gate_min_score"] = serde_json::json!(decimal_string(min_score));
            }
            if let Some(boost_weight) = event_gate_profile.boost_weight {
                parameters["event_gate_boost_weight"] =
                    serde_json::json!(decimal_string(boost_weight));
            }
            if let Some(score_direction) = event_gate_profile.score_direction {
                parameters["event_gate_score_direction"] =
                    serde_json::json!(score_direction.as_str());
            }
            if !event_gate_profile.active_regimes.is_empty() {
                parameters["event_gate_active_regimes"] =
                    serde_json::json!(event_gate_profile.active_regimes.clone());
            }
        }
        match signal_candidate {
            Some(LayeredSignalCandidate::FactorCombo(combo)) => {
                parameters["signal_source"] = serde_json::json!("factor_combo");
                parameters["combo_name"] = serde_json::json!(combo.combo_name.clone());
                parameters["version"] = serde_json::json!(combo.version.clone());
            }
            Some(LayeredSignalCandidate::ModelPrediction(prediction_set_id)) => {
                parameters["signal_source"] = serde_json::json!("model_prediction");
                parameters["prediction_set_id"] = serde_json::json!(prediction_set_id);
            }
            None => continue,
        }

        trials.push(LayeredSearchTrial {
            trial_id: format!("phase7d-{:06}", trial_index + 1),
            trial_index,
            batch_index: trial_index / batch_size + 1,
            parameters,
        });
    }

    LayeredSearchPlan {
        requested_trials,
        planned_trials: trials.len(),
        truncated: requested_trials > trials.len(),
        batch_size,
        max_parallel_trials: resource_plan.max_parallel_trials,
        trials,
    }
}

#[derive(Debug, Clone, Copy)]
struct LayeredTrialIndices {
    market_regime_policy: usize,
    signal_candidate: usize,
    top_n: usize,
    rebalance_days: usize,
    score_direction: usize,
    skip_top_pct: usize,
    max_pairwise_correlation: usize,
    kelly_fraction: usize,
    max_position_pct: usize,
    max_gross_exposure: usize,
    portfolio_method: usize,
    risk_budget_lookback_days: usize,
    capacity_penalty_strength: usize,
    industry_max_weight_pct: usize,
    style_risk_budget_profile: usize,
    candidate_risk_filter_profile: usize,
    risk_contribution_control_profile: usize,
    rebalance_hysteresis_pct: usize,
    partial_rebalance_ratio: usize,
    score_candidate_pool_size: usize,
    universe_profile: usize,
    portfolio_drawdown_control: usize,
    portfolio_volatility_control: usize,
    position_risk_control: usize,
    event_gate_profile: usize,
}

impl LayeredTrialIndices {
    fn from_flat_index(config: &LayeredSearchConfig, mut index: usize) -> Option<Self> {
        let market_regime_policy =
            take_axis_index(&mut index, config.market_regime_policies.len())?;
        let capacity_penalty_strength =
            take_axis_index(&mut index, config.capacity_penalty_strength.len())?;
        let industry_max_weight_pct =
            take_axis_index(&mut index, config.industry_max_weight_pct.len())?;
        let style_risk_budget_profile =
            take_axis_index(&mut index, config.style_risk_budget_profiles.len())?;
        let candidate_risk_filter_profile =
            take_axis_index(&mut index, config.candidate_risk_filter_profiles.len())?;
        let risk_contribution_control_profile =
            take_axis_index(&mut index, config.risk_contribution_control_profiles.len())?;
        let rebalance_hysteresis_pct =
            take_axis_index(&mut index, config.rebalance_hysteresis_pct.len())?;
        let partial_rebalance_ratio =
            take_axis_index(&mut index, config.partial_rebalance_ratio.len())?;
        let score_candidate_pool_size =
            take_axis_index(&mut index, config.score_candidate_pool_sizes.len())?;
        let universe_profile = take_axis_index(&mut index, config.universe_profiles.len())?;
        let portfolio_drawdown_control =
            take_axis_index(&mut index, config.portfolio_drawdown_controls.len())?;
        let portfolio_volatility_control =
            take_axis_index(&mut index, config.portfolio_volatility_controls.len())?;
        let position_risk_control =
            take_axis_index(&mut index, config.position_risk_controls.len())?;
        let event_gate_profile = take_axis_index(&mut index, config.event_gate_profiles.len())?;
        let risk_budget_lookback_days =
            take_axis_index(&mut index, config.risk_budget_lookback_days.len())?;
        let portfolio_method = take_axis_index(&mut index, config.portfolio_methods.len())?;
        let max_gross_exposure = take_axis_index(&mut index, config.max_gross_exposure.len())?;
        let max_position_pct = take_axis_index(&mut index, config.max_position_pct.len())?;
        let kelly_fraction = take_axis_index(&mut index, config.kelly_fraction.len())?;
        let max_pairwise_correlation =
            take_axis_index(&mut index, config.max_pairwise_correlation.len())?;
        let skip_top_pct = take_axis_index(&mut index, config.skip_top_pct.len())?;
        let score_direction = take_axis_index(&mut index, config.score_directions.len())?;
        let rebalance_days = take_axis_index(&mut index, config.rebalance_days.len())?;
        let top_n = take_axis_index(&mut index, config.top_n.len())?;
        let signal_candidate = take_axis_index(&mut index, config.signal_candidate_count())?;

        Some(Self {
            market_regime_policy,
            signal_candidate,
            top_n,
            rebalance_days,
            score_direction,
            skip_top_pct,
            max_pairwise_correlation,
            kelly_fraction,
            max_position_pct,
            max_gross_exposure,
            portfolio_method,
            risk_budget_lookback_days,
            capacity_penalty_strength,
            industry_max_weight_pct,
            style_risk_budget_profile,
            candidate_risk_filter_profile,
            risk_contribution_control_profile,
            rebalance_hysteresis_pct,
            partial_rebalance_ratio,
            score_candidate_pool_size,
            universe_profile,
            portfolio_drawdown_control,
            portfolio_volatility_control,
            position_risk_control,
            event_gate_profile,
        })
    }
}

fn take_axis_index(index: &mut usize, axis_len: usize) -> Option<usize> {
    if axis_len == 0 {
        return None;
    }
    let axis_index = *index % axis_len;
    *index /= axis_len;
    Some(axis_index)
}

fn selected_cartesian_indices(total: usize, max_trials: usize) -> Vec<usize> {
    if total == 0 || max_trials == 0 {
        return Vec::new();
    }
    if max_trials >= total {
        return (0..total).collect();
    }
    if max_trials == 1 {
        return vec![0];
    }

    let last = total - 1;
    (0..max_trials)
        .map(|idx| (idx * last + (max_trials - 1) / 2) / (max_trials - 1))
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateTargets {
    pub min_annual_return: Decimal,
    pub min_excess_return: Decimal,
    pub min_sharpe: Decimal,
    pub min_sortino: Decimal,
    pub max_drawdown: Decimal,
}

impl Default for CandidateTargets {
    fn default() -> Self {
        Self {
            min_annual_return: Decimal::new(15, 2),
            min_excess_return: Decimal::ZERO,
            min_sharpe: Decimal::ONE,
            min_sortino: Decimal::new(15, 1),
            max_drawdown: Decimal::new(35, 2),
        }
    }
}

impl CandidateTargets {
    pub fn classify(&self, metrics: &CandidateMetrics) -> CandidateType {
        if metrics.annual_return >= self.min_annual_return
            && metrics.excess_return > self.min_excess_return
            && metrics.sharpe > self.min_sharpe
            && metrics.sortino >= self.min_sortino
            && metrics.max_drawdown < self.max_drawdown
        {
            CandidateType::Professional
        } else if metrics.annual_return >= self.min_annual_return
            && metrics.excess_return > self.min_excess_return
        {
            CandidateType::ReviewRequired
        } else if metrics.annual_return >= Decimal::ZERO
            && metrics.max_drawdown <= Decimal::new(35, 2)
        {
            CandidateType::Defensive
        } else {
            CandidateType::Research
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidateType {
    #[serde(rename = "professional_candidate")]
    Professional,
    #[serde(rename = "review_required")]
    ReviewRequired,
    #[serde(rename = "defensive_candidate")]
    Defensive,
    #[serde(rename = "research_candidate")]
    Research,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateMetrics {
    pub annual_return: Decimal,
    pub excess_return: Decimal,
    pub sharpe: Decimal,
    pub sortino: Decimal,
    pub max_drawdown: Decimal,
    pub total_return: Decimal,
    pub benchmark_return: Decimal,
    pub turnover: Decimal,
    pub num_trades: u64,
}

impl Default for CandidateMetrics {
    fn default() -> Self {
        Self {
            annual_return: Decimal::ZERO,
            excess_return: Decimal::ZERO,
            sharpe: Decimal::ZERO,
            sortino: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            total_return: Decimal::ZERO,
            benchmark_return: Decimal::ZERO,
            turnover: Decimal::ZERO,
            num_trades: 0,
        }
    }
}

impl CandidateMetrics {
    pub fn from_optimization_metrics(metrics: &Value) -> Self {
        Self {
            annual_return: decimal_field(metrics, "annual_return_pct"),
            excess_return: decimal_field(metrics, "excess_return_pct"),
            sharpe: decimal_field(metrics, "sharpe_ratio"),
            sortino: decimal_field(metrics, "sortino_ratio"),
            max_drawdown: decimal_field(metrics, "max_drawdown_pct"),
            total_return: decimal_field(metrics, "total_return_pct"),
            benchmark_return: decimal_field(metrics, "benchmark_return_pct"),
            turnover: decimal_field(metrics, "turnover"),
            num_trades: metrics
                .get("num_trades")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| decimal_field(metrics, "num_trades").to_u64().unwrap_or(0)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateScreeningRow {
    pub candidate_id: String,
    pub candidate_type: CandidateType,
    pub metrics: CandidateMetrics,
    pub robustness_status: Option<String>,
    pub optimization_task_id: Option<String>,
    pub backtest_task_id: Option<String>,
    pub parameters: Value,
}

pub fn screen_optimization_results(
    results: &[Value],
    targets: &CandidateTargets,
) -> Vec<CandidateScreeningRow> {
    let mut rows: Vec<_> = results
        .iter()
        .map(|item| {
            let best_trial = item.get("best_trial").unwrap_or(&Value::Null);
            let metrics = CandidateMetrics::from_optimization_metrics(
                best_trial.get("metrics").unwrap_or(&Value::Null),
            );
            CandidateScreeningRow {
                candidate_id: item
                    .get("candidate_id")
                    .and_then(Value::as_str)
                    .or_else(|| item.get("optimization_task_id").and_then(Value::as_str))
                    .unwrap_or("unknown_candidate")
                    .to_string(),
                candidate_type: targets.classify(&metrics),
                metrics,
                robustness_status: item
                    .pointer("/robustness/data/status")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                optimization_task_id: item
                    .get("optimization_task_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                backtest_task_id: best_trial
                    .get("backtest_task_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                parameters: best_trial.get("parameters").cloned().unwrap_or(Value::Null),
            }
        })
        .collect();

    rows.sort_by(candidate_row_order);
    rows
}

fn candidate_row_order(
    left: &CandidateScreeningRow,
    right: &CandidateScreeningRow,
) -> std::cmp::Ordering {
    candidate_type_rank(left.candidate_type)
        .cmp(&candidate_type_rank(right.candidate_type))
        .then_with(|| right.metrics.annual_return.cmp(&left.metrics.annual_return))
        .then_with(|| right.metrics.sharpe.cmp(&left.metrics.sharpe))
        .then_with(|| right.metrics.sortino.cmp(&left.metrics.sortino))
        .then_with(|| right.metrics.excess_return.cmp(&left.metrics.excess_return))
        .then_with(|| left.metrics.max_drawdown.cmp(&right.metrics.max_drawdown))
}

fn candidate_type_rank(candidate_type: CandidateType) -> u8 {
    match candidate_type {
        CandidateType::Professional => 0,
        CandidateType::ReviewRequired => 1,
        CandidateType::Defensive => 2,
        CandidateType::Research => 3,
    }
}

fn decimal_field(value: &Value, field: &str) -> Decimal {
    value
        .get(field)
        .and_then(|field_value| match field_value {
            Value::String(raw) => raw.parse().ok(),
            Value::Number(number) => number.to_string().parse().ok(),
            _ => None,
        })
        .unwrap_or(Decimal::ZERO)
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn detect_memory_gb() -> Option<usize> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8(output.stdout).ok()?;
    let bytes: u64 = raw.trim().parse().ok()?;
    Some((bytes / 1024 / 1024 / 1024) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use serde_json::json;

    #[test]
    fn local_mac_resource_plan_reserves_headroom() {
        let plan = LocalResourcePlan::for_machine(10, 32);

        assert_eq!(plan.profile, "local_mac");
        assert_eq!(plan.cpu_count, 10);
        assert_eq!(plan.memory_gb, 32);
        assert_eq!(plan.max_parallel_trials, 8);
        assert_eq!(plan.memory_budget_gb, 20);
        assert_eq!(plan.max_trials, 64);
    }

    #[test]
    fn candidate_screening_marks_professional_candidate() {
        let metrics = CandidateMetrics {
            annual_return: Decimal::new(18, 2),
            excess_return: Decimal::new(5, 2),
            sharpe: Decimal::new(12, 1),
            sortino: Decimal::new(16, 1),
            max_drawdown: Decimal::new(20, 2),
            ..CandidateMetrics::default()
        };

        assert_eq!(
            CandidateTargets::default().classify(&metrics),
            CandidateType::Professional
        );
    }

    #[test]
    fn candidate_screening_requires_sortino_for_professional_candidate() {
        let metrics = CandidateMetrics {
            annual_return: Decimal::new(18, 2),
            excess_return: Decimal::new(5, 2),
            sharpe: Decimal::new(12, 1),
            sortino: Decimal::new(10, 1),
            max_drawdown: Decimal::new(20, 2),
            ..CandidateMetrics::default()
        };

        assert_eq!(
            CandidateTargets::default().classify(&metrics),
            CandidateType::ReviewRequired
        );
    }

    #[test]
    fn candidate_screening_keeps_low_return_result_as_defensive() {
        let metrics = CandidateMetrics {
            annual_return: Decimal::new(209, 4),
            excess_return: Decimal::new(-2937, 4),
            sharpe: Decimal::new(18, 2),
            max_drawdown: Decimal::new(23, 2),
            ..CandidateMetrics::default()
        };

        assert_eq!(
            CandidateTargets::default().classify(&metrics),
            CandidateType::Defensive
        );
    }

    #[test]
    fn candidate_screening_sorts_professional_before_defensive() {
        let results = vec![
            json!({
                "candidate_id": "weak",
                "best_trial": {
                    "metrics": {
                        "annual_return_pct": "0.02",
                        "excess_return_pct": "-0.10",
                        "sharpe_ratio": "0.10",
                        "sortino_ratio": "0.12",
                        "max_drawdown_pct": "0.20",
                        "num_trades": 30
                    }
                },
                "robustness": {"data": {"status": "approved_candidate"}}
            }),
            json!({
                "candidate_id": "strong",
                "best_trial": {
                    "metrics": {
                        "annual_return_pct": "0.16",
                        "excess_return_pct": "0.03",
                        "sharpe_ratio": "1.10",
                        "sortino_ratio": "1.60",
                        "max_drawdown_pct": "0.22",
                        "num_trades": 80
                    }
                },
                "robustness": {"data": {"status": "approved_candidate"}}
            }),
        ];

        let rows = screen_optimization_results(&results, &CandidateTargets::default());

        assert_eq!(rows[0].candidate_id, "strong");
        assert_eq!(rows[0].candidate_type, CandidateType::Professional);
        assert_eq!(rows[1].candidate_type, CandidateType::Defensive);
    }

    #[test]
    fn layered_search_plan_respects_local_resource_budget() {
        let config = LayeredSearchConfig {
            market_regime_policies: vec!["off".to_string(), "professional_default".to_string()],
            combo_versions: vec![
                ComboVersion::new("full_icir_16f_v3", "1.0.0"),
                ComboVersion::new("phase7_price_volume_expanded_v1", "1.0.0"),
                ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ],
            prediction_set_ids: Vec::new(),
            top_n: vec![20, 30],
            rebalance_days: vec![10, 20],
            score_directions: vec![ScoreDirection::Descending, ScoreDirection::Ascending],
            skip_top_pct: vec![Decimal::ZERO],
            max_pairwise_correlation: vec![Decimal::new(75, 2)],
            kelly_fraction: vec![Decimal::new(25, 2)],
            max_position_pct: vec![Decimal::new(8, 2)],
            max_gross_exposure: vec![Decimal::ONE],
            portfolio_methods: vec!["heuristic".to_string(), "risk_budget".to_string()],
            risk_budget_lookback_days: vec![60],
            capacity_penalty_strength: vec![Decimal::ZERO],
            industry_max_weight_pct: vec![None],
            style_risk_budget_profiles: vec!["off".to_string()],
            candidate_risk_filter_profiles: vec!["off".to_string()],
            risk_contribution_control_profiles: vec!["off".to_string()],
            rebalance_hysteresis_pct: vec![Decimal::ZERO],
            partial_rebalance_ratio: vec![Decimal::ONE],
            score_candidate_pool_sizes: vec![0, 200],
            universe_profiles: vec!["all".to_string(), "listed_non_st".to_string()],
            portfolio_drawdown_controls: vec![
                PortfolioDrawdownControlProfile::off(),
                PortfolioDrawdownControlProfile::preserve(
                    "rolling252_10_25_50",
                    Decimal::new(10, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                    Some(252),
                ),
            ],
            portfolio_volatility_controls: vec![
                PortfolioVolatilityControlProfile::off(),
                PortfolioVolatilityControlProfile::target(
                    "vol120_18_50_100",
                    Decimal::new(18, 2),
                    120,
                    Decimal::new(50, 2),
                    Decimal::ONE,
                ),
            ],
            position_risk_controls: vec![PositionRiskControlProfile::off()],
            event_gate_profiles: vec![EventGateProfile::off()],
            seed_trials: Vec::new(),
            correlation_lookback_days: 60,
            kelly_lookback_days: 60,
            benchmark: "000300.SH".to_string(),
        };
        let mut resource_plan = LocalResourcePlan::for_machine(4, 16);
        resource_plan.max_trials = 5;

        let plan = build_layered_search_plan(&config, &resource_plan);

        assert_eq!(plan.requested_trials, 1536);
        assert_eq!(plan.trials.len(), 5);
        assert!(plan.truncated);
        assert_eq!(plan.trials[0].trial_id, "phase7d-000001");
        assert_eq!(plan.trials[0].parameters["combo_name"], "full_icir_16f_v3");
        assert_eq!(plan.trials[0].parameters["market_regime"], "off");
        assert_eq!(plan.trials[0].parameters["universe_profile"], "all");
        assert!(plan.trials[0].parameters["industry_max_weight_pct"].is_null());
        assert_eq!(
            plan.trials[0].parameters["portfolio_drawdown_control"],
            "off"
        );
        assert_eq!(
            plan.trials[0].parameters["portfolio_volatility_control"],
            "off"
        );
        let market_regimes = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["market_regime"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(market_regimes.contains("professional_default"));
        assert_eq!(plan.trials[0].parameters["rebalance"], "10");
        assert_eq!(plan.trials[0].parameters["score_direction"], "descending");
    }

    #[test]
    fn default_layered_search_config_includes_professional_axes() {
        let config = LayeredSearchConfig::local_professional_default();

        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_price_volume_expanded_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_financial_quality_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_relative_strength_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_quality_relative_strength_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_growth_recovery_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_valuation_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_moneyflow_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_value_quality_growth_rel_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_quality_value_recovery_confirm_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_value_tilt_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_quality_growth_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_defensive_rel_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_blend_recovery_tilt_v1"));
        assert!(config.top_n.contains(&30));
        assert!(config.rebalance_days.contains(&20));
        assert!(config
            .score_directions
            .contains(&ScoreDirection::Descending));
        assert!(config.market_regime_policies.contains(&"off".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"professional_default".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"drawdown_control_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"drawdown_control_v2".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_risk_off_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_crash_guard_v1".to_string()));
        assert!(config.kelly_fraction.contains(&Decimal::new(25, 2)));
        assert!(config
            .max_pairwise_correlation
            .contains(&Decimal::new(75, 2)));
        assert!(config
            .portfolio_methods
            .contains(&"risk_budget".to_string()));
        assert!(config.risk_budget_lookback_days.contains(&120));
        assert!(config
            .capacity_penalty_strength
            .contains(&Decimal::new(75, 2)));
        assert!(config.score_candidate_pool_sizes.contains(&0));
        assert!(config.score_candidate_pool_sizes.contains(&500));
        assert!(config.universe_profiles.contains(&"all".to_string()));
        assert!(config
            .universe_profiles
            .contains(&"listed_non_st".to_string()));
        assert!(config
            .universe_profiles
            .contains(&"main_board_non_st".to_string()));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "rolling252_10_25_50"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "rolling252_12_30_60"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "rolling504_15_35_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_25_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_27_50_30_70"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol252_16_45_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_18_50_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol60_20_60_100"));
    }

    #[test]
    fn professional_breakthrough_config_focuses_on_high_return_neighborhood() {
        let config = LayeredSearchConfig::professional_breakthrough_default();

        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_financial_quality_v1"));
        assert!(config
            .combo_versions
            .iter()
            .any(|combo| combo.combo_name == "phase7_quality_moneyflow_pos_5pct_v1"));
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config.top_n.contains(&20));
        assert!(config.rebalance_days.contains(&60));
        assert!(config.max_position_pct.contains(&Decimal::new(15, 2)));
        assert!(config
            .portfolio_methods
            .contains(&"risk_budget".to_string()));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_25_50_30_70"));
        assert!(config.seed_trials.len() >= 3);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["risk_budget_lookback_days"] == 120
                && trial["capacity_penalty_strength"] == "0.75"
        }));
    }

    #[test]
    fn professional_risk_breakthrough_config_focuses_on_drawdown_sortino_neighborhood() {
        let config = LayeredSearchConfig::professional_risk_breakthrough_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_value_quality_growth_rel_v1"));
        assert!(combo_names.contains("phase7_blend_value_tilt_v1"));
        assert!(combo_names.contains("phase7_blend_quality_growth_v1"));
        assert!(combo_names.contains("phase7_blend_defensive_rel_v1"));
        assert!(combo_names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(combo_names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
        assert!(config
            .market_regime_policies
            .contains(&"quality_crash_guard_v1".to_string()));
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config.top_n.contains(&20));
        assert!(config.top_n.contains(&25));
        assert!(config.rebalance_days.contains(&60));
        assert!(config.max_position_pct.contains(&Decimal::new(10, 2)));
        assert!(config.max_gross_exposure.contains(&Decimal::new(90, 2)));
        assert!(config.risk_budget_lookback_days.contains(&180));
        assert!(config.capacity_penalty_strength.contains(&Decimal::ONE));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_08_25_60_25_75"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_30_55_25_75"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_26_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_10_24_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_09_26_50_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_09_24_45_30_70"));
        assert!(config
            .portfolio_drawdown_controls
            .iter()
            .any(|profile| profile.profile_name == "recover252_08_24_45_30_70"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_22_65_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_24_70_100"));
        assert!(config
            .portfolio_volatility_controls
            .iter()
            .any(|profile| profile.profile_name == "vol120_30_85_100"));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["portfolio_drawdown_control"] == "recover252_08_25_60_25_75"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["max_position_pct"] == "0.10"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_30_85_100"
                && trial["max_gross_exposure"] == "1"
                && trial["portfolio_volatility_min_exposure"] == "0.85"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "vol60_30_90_100"
                && trial["max_gross_exposure"] == "1"
                && trial["portfolio_volatility_min_exposure"] == "0.90"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["max_gross_exposure"] == "1"
                && trial["industry_max_weight_pct"].is_null()
                && trial.get("portfolio_volatility_target_pct").is_none()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_26_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.14"
                && trial["industry_max_weight_pct"].is_null()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v2"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"].is_null()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v3"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"].is_null()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["prediction_set_id"] == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial["prediction_blend_weight"] == "0.02"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["prediction_set_id"] == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial["prediction_blend_weight"] == "0.05"
                && trial["prediction_min_percentile"] == "0.20"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["trailing_stop_pct"] == "0.18"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.12"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_25_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.14"
        }));
        assert!(config
            .position_risk_controls
            .iter()
            .any(|profile| profile.profile_name == "stop_loss_12"
                && profile.stop_loss_pct == Some(Decimal::new(12, 2))));
        for (profile_name, cooldown_days) in [
            ("stop_loss_07_cooldown_5", 5),
            ("stop_loss_07_cooldown_10", 10),
            ("stop_loss_07_cooldown_20", 20),
            ("stop_loss_07_cooldown_30", 30),
        ] {
            assert!(config.position_risk_controls.iter().any(|profile| {
                profile.profile_name == profile_name
                    && profile.stop_loss_pct == Some(Decimal::new(7, 2))
                    && profile.reentry_cooldown_days == Some(cooldown_days)
            }));
        }
        for (profile_name, stop_loss_pct, cooldown_days) in [
            ("stop_loss_065_cooldown_20", Decimal::new(65, 3), 20),
            ("stop_loss_075_cooldown_20", Decimal::new(75, 3), 20),
            ("stop_loss_075_cooldown_30", Decimal::new(75, 3), 30),
        ] {
            assert!(config.position_risk_controls.iter().any(|profile| {
                profile.profile_name == profile_name
                    && profile.stop_loss_pct == Some(stop_loss_pct)
                    && profile.reentry_cooldown_days == Some(cooldown_days)
            }));
        }
        for (profile_name, stop_loss_pct) in [
            ("stop_loss_07", Decimal::new(7, 2)),
            ("stop_loss_08", Decimal::new(8, 2)),
            ("stop_loss_085", Decimal::new(85, 3)),
            ("stop_loss_09", Decimal::new(9, 2)),
            ("stop_loss_095", Decimal::new(95, 3)),
        ] {
            assert!(config.position_risk_controls.iter().any(|profile| {
                profile.profile_name == profile_name && profile.stop_loss_pct == Some(stop_loss_pct)
            }));
        }
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_drawdown_control"] == "recover252_09_24_45_30_70"
                && trial["max_position_pct"] == "0.13"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_drawdown_control"] == "recover252_08_24_45_30_70"
                && trial["max_gross_exposure"] == "0.90"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_moneyflow_pos_5pct_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_blend_defensive_rel_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_blend_recovery_tilt_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_27_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.07"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 10
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"] == "0.20"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["industry_max_weight_pct"] == "0.25"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.065"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.07"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_gross_exposure"] == "0.90"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_09_24_45_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["max_pairwise_correlation"] == "0.65"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_moneyflow_pos_5pct_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_09_24_45_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v1"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["max_position_pct"] == "0.15"
                && trial["stop_loss_pct"] == "0.09"
        }));
    }

    #[test]
    fn professional_sharpe_stabilization_profile_starts_from_u2_exact_anchor() {
        let risk_config = LayeredSearchConfig::professional_risk_breakthrough_default();
        let config = LayeredSearchConfig::professional_sharpe_stabilization_default();

        assert!(config.seed_trials.len() < risk_config.seed_trials.len());
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_drawdown_control"],
            "recover252_10_24_50_30_70"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(config.seed_trials[0]["stop_loss_pct"], "0.075");
        assert_eq!(config.seed_trials[0]["reentry_cooldown_days"], 30);
        assert_eq!(config.seed_trials[0]["rebalance_hysteresis_pct"], "0");
        assert_eq!(config.seed_trials[0]["partial_rebalance_ratio"], "1");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 20
        }));
        assert!(!config.seed_trials.iter().any(|trial| {
            trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "off"
                && trial["stop_loss_pct"] == "0.07"
        }));
    }

    #[test]
    fn professional_sharpe_stabilization_profile_adds_turnover_smoothing_axes() {
        let config = LayeredSearchConfig::professional_sharpe_stabilization_default();

        assert!(config.rebalance_hysteresis_pct.contains(&Decimal::ZERO));
        assert!(config
            .rebalance_hysteresis_pct
            .contains(&Decimal::new(1, 2)));
        assert!(config
            .rebalance_hysteresis_pct
            .contains(&Decimal::new(2, 2)));
        assert!(config.partial_rebalance_ratio.contains(&Decimal::ONE));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(75, 2)));
        assert!(config
            .partial_rebalance_ratio
            .contains(&Decimal::new(50, 2)));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["rebalance_hysteresis_pct"] == "0.01"
                && trial["partial_rebalance_ratio"] == "0.75"
        }));
    }

    #[test]
    fn professional_regime_stabilization_profile_focuses_on_state_triggered_risk_controls() {
        let config = LayeredSearchConfig::professional_regime_stabilization_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_crash_guard_v1".to_string(),
                "quality_crash_guard_v2".to_string(),
                "quality_crash_guard_v3".to_string(),
            ]
        );
        assert!(config.seed_trials.iter().all(|trial| {
            matches!(
                trial["market_regime"].as_str(),
                Some("quality_crash_guard_v1")
                    | Some("quality_crash_guard_v2")
                    | Some("quality_crash_guard_v3")
            )
        }));
        assert_eq!(config.max_gross_exposure, vec![Decimal::ONE]);
        assert!(config
            .position_risk_controls
            .iter()
            .any(|profile| profile.profile_name == "stop_loss_075_cooldown_30"));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v2"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_crash_guard_v3"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(!config
            .seed_trials
            .iter()
            .any(|trial| trial["max_pairwise_correlation"] == "0.65"));
    }

    #[test]
    fn professional_bear_window_stabilization_profile_targets_attribution_failure_window() {
        let config = LayeredSearchConfig::professional_bear_window_stabilization_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.max_gross_exposure, vec![Decimal::ONE]);
        assert_eq!(
            config.portfolio_volatility_controls.len(),
            2,
            "U2 keeps only Phase 7-T/U working volatility shapes"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_window_guard_v1"
                && trial["portfolio_volatility_control"] == "vol120_24_70_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
        }));
    }

    #[test]
    fn professional_style_risk_budget_profile_extends_u2_with_style_axes() {
        let config = LayeredSearchConfig::professional_style_risk_budget_default();

        assert_eq!(
            config.style_risk_budget_profiles,
            vec![
                "off".to_string(),
                "liquidity_volatility_balanced_v1".to_string(),
                "defensive_style_budget_v1".to_string(),
            ]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["style_risk_budget"] == "defensive_style_budget_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 6,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(4, 16)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["style_risk_budget"] == "liquidity_volatility_balanced_v1"
        }));
    }

    #[test]
    fn professional_residual_quality_profile_targets_industry_residual_quality_source() {
        let config = LayeredSearchConfig::professional_residual_quality_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_industry_residual_quality_v1"));
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert_eq!(
            config.style_risk_budget_profiles,
            vec![
                "off".to_string(),
                "liquidity_volatility_balanced_v1".to_string(),
            ]
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_industry_residual_quality_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["score_direction"] == "ascending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_industry_residual_quality_v1"
                && trial["score_direction"] == "descending"
        }));
    }

    #[test]
    fn phase7_residual_confirmation_blends_keep_quality_as_core_alpha() {
        let profiles = phase7_alpha_blend_profiles();
        let light = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_residual_confirm_5pct_v1")
            .expect("5pct residual confirmation profile");
        let medium = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_residual_confirm_10pct_v1")
            .expect("10pct residual confirmation profile");

        let light_sources = light
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<Vec<_>>();
        assert_eq!(
            light_sources,
            vec![
                ("phase7_financial_quality_v1", Decimal::new(95, 2)),
                ("phase7_industry_residual_quality_v1", Decimal::new(5, 2)),
            ]
        );

        let medium_sources = medium
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<Vec<_>>();
        assert_eq!(
            medium_sources,
            vec![
                ("phase7_financial_quality_v1", Decimal::new(90, 2)),
                ("phase7_industry_residual_quality_v1", Decimal::new(10, 2)),
            ]
        );
    }

    #[test]
    fn professional_residual_overlay_sharpe_profile_preserves_u2_anchor() {
        let config = LayeredSearchConfig::professional_residual_overlay_sharpe_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_5pct_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(!combo_names.contains("phase7_industry_residual_quality_v1"));
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v1".to_string(),
                "quality_bear_window_guard_v2".to_string(),
            ]
        );
        assert_eq!(
            config.style_risk_budget_profiles,
            vec![
                "off".to_string(),
                "liquidity_volatility_balanced_v1".to_string(),
            ]
        );
        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_residual_confirm_5pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_residual_confirm_10pct_v1"
                && trial["style_risk_budget"] == "liquidity_volatility_balanced_v1"
        }));
        assert!(!config
            .seed_trials
            .iter()
            .any(|trial| trial["combo_name"] == "phase7_industry_residual_quality_v1"));
    }

    #[test]
    fn professional_conditioned_second_alpha_profile_uses_gates_not_linear_overlay() {
        let config = LayeredSearchConfig::professional_conditioned_second_alpha_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "valuation_exclude_bottom40"
                && profile.combo_name.as_deref() == Some("phase7_valuation_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
                && profile.min_score == Some(Decimal::new(40, 2))
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "residual_require_top40"
                && profile.combo_name.as_deref() == Some("phase7_industry_residual_quality_v1")
                && profile.mode.as_deref() == Some("require_positive")
                && profile.min_score == Some(Decimal::new(60, 2))
        }));
        assert_eq!(
            config.seed_trials[0]["event_gate_profile"],
            Value::Null,
            "first seed must keep the U2 anchor as an unchanged control"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "valuation_require_top40"
                && trial["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial["event_gate_mode"] == "require_positive"
                && trial["event_gate_min_score"] == "0.60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "moneyflow_require_top40"
                && trial["event_gate_combo_name"] == "phase7_moneyflow_v1"
        }));
    }

    #[test]
    fn professional_valuation_guard_sharpe_profile_tunes_exclusion_thresholds_only() {
        let config = LayeredSearchConfig::professional_valuation_guard_sharpe_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert!(config.event_gate_profiles.iter().all(|profile| {
            profile.mode.as_deref() != Some("require_positive")
                && profile.combo_name.as_deref() != Some("phase7_moneyflow_v1")
                && profile.combo_name.as_deref() != Some("phase7_industry_residual_quality_v1")
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "valuation_exclude_bottom35"
                && profile.combo_name.as_deref() == Some("phase7_valuation_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
                && profile.min_score == Some(Decimal::new(35, 2))
        }));
        assert_eq!(
            config.seed_trials[0]["event_gate_profile"],
            Value::Null,
            "first seed must keep the U2 anchor as an unchanged control"
        );
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_min_score"] == "0.40"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom50"
                && trial["event_gate_mode"] == "exclude_negative"
        }));
        assert!(!config
            .seed_trials
            .iter()
            .any(|trial| trial["event_gate_mode"] == "require_positive"));
    }

    #[test]
    fn professional_regime_conditioned_valuation_guard_profile_scopes_guard_to_stress_regimes() {
        let config = LayeredSearchConfig::professional_regime_conditioned_valuation_guard_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "valuation_exclude_bottom35_stress_only"
                && profile.combo_name.as_deref() == Some("phase7_valuation_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
                && profile.min_score == Some(Decimal::new(35, 2))
                && profile.active_regimes.as_slice() == ["bear", "high_volatility"]
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom35_stress_only"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40_stress_only"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_min_score"] == "0.40"
        }));
    }

    #[test]
    fn professional_regime_alpha_routing_profile_seeds_regime_specific_alpha_trials() {
        let config = LayeredSearchConfig::professional_regime_alpha_routing_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn professional_regime_alpha_sleeve_search_profile_seeds_multiple_stress_alpha_sources() {
        let config = LayeredSearchConfig::professional_regime_alpha_sleeve_search_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_value_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_recovery_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_switch_blend_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_value_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_recovery_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_switch_blend_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
    }

    #[test]
    fn professional_regime_alpha_overlay_search_profile_seeds_small_stress_overlays() {
        let config = LayeredSearchConfig::professional_regime_alpha_overlay_search_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_overlay_value_05pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_overlay_value_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_overlay_blend_10pct_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_overlay_value_10pct_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "off"
        }));
    }

    #[test]
    fn professional_regime_alpha_sleeve_allocation_profile_seeds_portfolio_sleeves() {
        let config = LayeredSearchConfig::professional_regime_alpha_sleeve_allocation_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_blend_10pct_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "off"
        }));
    }

    #[test]
    fn professional_low_risk_sleeve_profile_seeds_low_risk_portfolio_sleeves() {
        let config = LayeredSearchConfig::professional_low_risk_sleeve_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1".to_string()));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "off"
        }));
    }

    #[test]
    fn professional_value_guard_sleeve_composition_profile_combines_fallback_components() {
        let config = LayeredSearchConfig::professional_value_guard_sleeve_composition_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string()));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config
            .seed_trials
            .iter()
            .all(|trial| trial["combo_name"] == "phase7_financial_quality_v1"));
    }

    #[test]
    fn professional_nearest_candidate_risk_model_profile_searches_risk_model_neighbors() {
        let config = LayeredSearchConfig::professional_nearest_candidate_risk_model_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v2".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string()
            ]
        );
        assert_eq!(
            config.portfolio_methods,
            vec!["risk_budget".to_string(), "min_variance".to_string()]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![120, 180]);
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["portfolio_method"] == "min_variance"
                && trial["risk_budget_lookback_days"] == 180
        }));
    }

    #[test]
    fn professional_event_regime_sleeve_profile_searches_event_sleeve_neighbors() {
        let config = LayeredSearchConfig::professional_event_regime_sleeve_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string()));
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_surprise_05pct_v1".to_string()
        ));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_surprise_10pct_v1"
        }));
    }

    #[test]
    fn professional_event_window_sleeve_weight_profile_searches_weight_neighbors() {
        let config = LayeredSearchConfig::professional_event_window_sleeve_weight_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_075pct_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string()));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
        }));
    }

    #[test]
    fn professional_event_window_sleeve_upper_bound_profile_searches_upper_bound_weight() {
        let config = LayeredSearchConfig::professional_event_window_sleeve_upper_bound_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config
            .market_regime_policies
            .contains(&"quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()));
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
    }

    #[test]
    fn professional_event_window_regime_placement_profile_searches_regime_routes() {
        let config = LayeredSearchConfig::professional_event_window_regime_placement_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1".to_string()
        ));
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1".to_string()
        ));
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
    }

    #[test]
    fn professional_event_window_decay_profile_searches_short_and_long_decay_variants() {
        let config = LayeredSearchConfig::professional_event_window_decay_default();

        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1".to_string()
        ));
        assert!(config.market_regime_policies.contains(
            &"quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1".to_string()
        ));
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
    }

    #[test]
    fn professional_volatility_sharpe_profile_searches_tighter_u2_vol_targets() {
        let config = LayeredSearchConfig::professional_volatility_sharpe_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_20_60_100"
                && trial["portfolio_volatility_target_pct"] == "0.20"
                && trial["portfolio_volatility_min_exposure"] == "0.60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_volatility_target_pct"] == "0.18"
                && trial["portfolio_volatility_min_exposure"] == "0.55"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol252_16_45_100"
                && trial["portfolio_volatility_target_pct"] == "0.16"
                && trial["portfolio_volatility_min_exposure"] == "0.45"
        }));
    }

    #[test]
    fn professional_regime_position_sharpe_profile_keeps_u2_alpha_and_sweeps_regime_overlay() {
        let config = LayeredSearchConfig::professional_regime_position_sharpe_default();

        assert_eq!(config.combo_versions.len(), 1);
        assert_eq!(
            config.combo_versions[0].combo_name,
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v2".to_string(),
                "quality_bear_position_guard_v1".to_string(),
                "quality_bear_position_guard_v2".to_string(),
                "quality_crash_guard_v3".to_string(),
                "quality_risk_off_v1".to_string(),
            ]
        );
        assert_eq!(
            config
                .portfolio_volatility_controls
                .iter()
                .map(|profile| profile.profile_name.as_str())
                .collect::<Vec<_>>(),
            vec!["vol120_18_55_100", "vol120_22_65_100"]
        );
        assert!(config.seed_trials.len() <= 8);
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_risk_off_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config
            .seed_trials
            .iter()
            .all(|trial| trial["combo_name"] == "phase7_financial_quality_v1"));
    }

    #[test]
    fn phase7_w_alpha_blend_adds_non_momentum_quality_value_recovery_source() {
        let profiles = phase7_alpha_blend_profiles();
        let profile = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_value_recovery_confirm_v1")
            .expect("Phase 7-W quality/value/recovery profile");

        let source_weights = profile
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(profile.profile_name, "quality_value_recovery_confirm_5pct");
        assert_eq!(
            source_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(60, 2))
        );
        assert_eq!(
            source_weights.get("phase7_valuation_v1"),
            Some(&Decimal::new(20, 2))
        );
        assert_eq!(
            source_weights.get("phase7_growth_recovery_v1"),
            Some(&Decimal::new(15, 2))
        );
        assert_eq!(
            source_weights.get("phase7_moneyflow_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert!(
            !source_weights.contains_key("phase7_relative_strength_v1"),
            "Phase 7-W should add a non-momentum second alpha source"
        );
    }

    #[test]
    fn phase7_event_alpha_blends_are_light_confirmation_overlays() {
        let profiles = phase7_alpha_blend_profiles();
        let quality_event = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_event_confirm_v1")
            .expect("quality/event profile");
        let quality_event_surprise = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_event_surprise_confirm_v1")
            .expect("quality/event-surprise profile");
        let quality_event_window = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_event_window_overlay_v1")
            .expect("quality/event-window overlay profile");
        let quality_value_recovery_event = profiles
            .iter()
            .find(|profile| profile.combo_name == "phase7_quality_value_recovery_event_confirm_v1")
            .expect("quality/value/recovery/event profile");

        let quality_event_weights = quality_event
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();
        let event_confirm_weights = quality_value_recovery_event
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();
        let event_window_weights = quality_event_window
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();
        let event_surprise_weights = quality_event_surprise
            .sources
            .iter()
            .map(|source| (source.combo_name.as_str(), source.weight))
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(
            quality_event_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(95, 2))
        );
        assert_eq!(
            quality_event_weights.get("phase7_event_earnings_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert_eq!(
            event_confirm_weights.get("phase7_event_earnings_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert_eq!(
            event_window_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(95, 2))
        );
        assert_eq!(
            event_window_weights.get("phase7_event_window_earnings_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert_eq!(
            event_surprise_weights.get("phase7_financial_quality_v1"),
            Some(&Decimal::new(95, 2))
        );
        assert_eq!(
            event_surprise_weights.get("phase7_event_surprise_v1"),
            Some(&Decimal::new(5, 2))
        );
        assert!(
            !event_confirm_weights.contains_key("phase7_relative_strength_v1"),
            "event confirmation profile should not reintroduce a momentum dependency"
        );
    }

    #[test]
    fn professional_event_conditioned_sharpe_profile_targets_event_surprise_gates() {
        let config = LayeredSearchConfig::professional_event_conditioned_sharpe_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(!combo_names.contains("phase7_event_earnings_v1"));
        assert!(!combo_names.contains("phase7_event_surprise_v1"));

        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_surprise_boost_pos_5pct"
                && profile.combo_name.as_deref() == Some("phase7_event_surprise_v1")
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_boost_pos_3pct"
                && profile.combo_name.as_deref() == Some("phase7_event_window_earnings_v1")
        }));

        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_boost_pos_5pct"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["combo_name"] == "phase7_financial_quality_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_exclude_negative"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
        }));
    }

    #[test]
    fn professional_second_alpha_source_profile_targets_phase7_w_search() {
        let config = LayeredSearchConfig::professional_second_alpha_source_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_event_earnings_v1"));
        assert!(combo_names.contains("phase7_event_surprise_v1"));
        assert!(combo_names.contains("phase7_event_window_earnings_v1"));
        assert!(combo_names.contains("phase7_quality_event_window_overlay_v1"));
        assert!(combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(config
            .style_risk_budget_profiles
            .contains(&"liquidity_volatility_balanced_v1".to_string()));
        assert!(config.score_directions.contains(&ScoreDirection::Ascending));
        assert!(config
            .score_directions
            .contains(&ScoreDirection::Descending));
        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_quality_value_recovery_confirm_v1"
        );
        assert_eq!(config.seed_trials[0]["score_direction"], "descending");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_value_recovery_confirm_v1"
                && trial["market_regime"] == "quality_bear_window_guard_v2"
                && trial["style_risk_budget"] == "liquidity_volatility_balanced_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_event_earnings_v1"
                && trial["score_direction"] == "descending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_event_earnings_v1"
                && trial["score_direction"] == "ascending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_event_surprise_v1"
                && trial["score_direction"] == "descending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_event_surprise_v1"
                && trial["score_direction"] == "ascending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_event_window_earnings_v1"
                && trial["score_direction"] == "descending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_event_window_earnings_v1"
                && trial["score_direction"] == "ascending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["score_direction"] == "descending"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial["score_direction"] == "ascending"
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 24,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(4, 16)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_earnings_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_earnings_v1"
                && trial.parameters["score_direction"] == "ascending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_surprise_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_surprise_v1"
                && trial.parameters["score_direction"] == "ascending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_window_earnings_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_event_window_earnings_v1"
                && trial.parameters["score_direction"] == "ascending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_window_overlay_v1"
                && trial.parameters["score_direction"] == "ascending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
                && trial.parameters["score_direction"] == "descending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
                && trial.parameters["score_direction"] == "ascending"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_value_recovery_confirm_v1"
        }));
        assert!(plan
            .trials
            .iter()
            .any(|trial| { trial.parameters["combo_name"] == "phase7_quality_event_confirm_v1" }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["combo_name"] == "phase7_quality_value_recovery_event_confirm_v1"
        }));
    }

    #[test]
    fn professional_second_alpha_source_search_can_generate_event_gate_trials() {
        let config = LayeredSearchConfig::professional_second_alpha_source_default();
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_boost_pos_5pct"
                && profile.combo_name.as_deref() == Some("phase7_event_window_earnings_v1")
                && profile.mode.as_deref() == Some("boost_positive")
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_exclude_negative"
                && profile.mode.as_deref() == Some("exclude_negative")
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 128,
                batch_size: 4,
                max_parallel_trials: 4,
                ..LocalResourcePlan::for_machine(8, 32)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_boost_pos_5pct"
                && trial.parameters["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial.parameters["event_gate_mode"] == "boost_positive"
                && trial.parameters["event_gate_boost_weight"] == "0.05"
        }));
        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["event_gate_profile"] == "event_window_exclude_negative"
                && trial.parameters["event_gate_mode"] == "exclude_negative"
        }));
    }

    #[test]
    fn professional_anti_overfit_sharpe_profile_generalizes_u2_without_date_fitting() {
        let config = LayeredSearchConfig::professional_anti_overfit_sharpe_default();

        assert_eq!(
            config.seed_trials[0]["combo_name"],
            "phase7_financial_quality_v1"
        );
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_bear_window_guard_v2"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_22_65_100"
        );
        assert_eq!(config.seed_trials[0]["rebalance_hysteresis_pct"], "0");
        assert_eq!(config.seed_trials[0]["partial_rebalance_ratio"], "1");

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_event_confirm_v1"));
        assert!(combo_names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(!combo_names.contains("phase7_event_earnings_v1"));
        assert!(!combo_names.contains("phase7_event_surprise_v1"));

        assert!(config
            .market_regime_policies
            .contains(&"quality_bear_window_guard_v1".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_bear_window_guard_v2".to_string()));
        assert!(config
            .market_regime_policies
            .contains(&"quality_crash_guard_v3".to_string()));
        assert_eq!(
            config.rebalance_hysteresis_pct,
            vec![Decimal::ZERO],
            "anti-overfit profile should not prioritize smoothing that already diluted returns"
        );
        assert_eq!(config.partial_rebalance_ratio, vec![Decimal::ONE]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );

        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_window_boost_pos_5pct"
                && profile.mode.as_deref() == Some("boost_positive")
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "event_window_boost_pos_5pct"
                && trial["event_gate_mode"] == "boost_positive"
        }));

        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_candidate_risk_filter_profile_adds_low_vol_low_corr_axis() {
        let config = LayeredSearchConfig::professional_candidate_risk_filter_default();

        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "low_volatility_v1".to_string(),
                "low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.seed_trials[0]["candidate_risk_filter"], "off",
            "Phase 7-AC should keep the exact anchor first for fair comparison"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
                && trial["rebalance_hysteresis_pct"] == "0"
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 6,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(8, 32)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
        }));
    }

    #[test]
    fn professional_risk_contribution_profile_adds_single_name_risk_axis() {
        let config = LayeredSearchConfig::professional_risk_contribution_default();

        assert_eq!(
            config.risk_contribution_control_profiles,
            vec![
                "off".to_string(),
                "soft_single_name_20pct_v1".to_string(),
                "soft_single_name_15pct_v1".to_string(),
            ]
        );
        assert_eq!(
            config.seed_trials[0]["risk_contribution_control"], "off",
            "Phase 7-AD should keep the exact anchor first for fair comparison"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));

        let plan = build_layered_search_plan(
            &config,
            &LocalResourcePlan {
                max_trials: 6,
                batch_size: 2,
                max_parallel_trials: 2,
                ..LocalResourcePlan::for_machine(8, 32)
            },
        );

        assert!(plan.trials.iter().any(|trial| {
            trial.parameters["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
    }

    #[test]
    fn alpha_blend_profiles_are_valid_weight_search_candidates() {
        let profiles = phase7_alpha_blend_profiles();
        let names = profiles
            .iter()
            .map(|profile| profile.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(profiles.len(), 13);
        assert!(names.contains("phase7_value_quality_growth_rel_v1"));
        assert!(names.contains("phase7_blend_value_tilt_v1"));
        assert!(names.contains("phase7_blend_quality_growth_v1"));
        assert!(names.contains("phase7_blend_defensive_rel_v1"));
        assert!(names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
        assert!(names.contains("phase7_quality_event_confirm_v1"));
        assert!(names.contains("phase7_quality_event_surprise_confirm_v1"));
        assert!(names.contains("phase7_quality_event_window_overlay_v1"));
        assert!(names.contains("phase7_quality_residual_confirm_5pct_v1"));
        assert!(names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(names.contains("phase7_quality_value_recovery_confirm_v1"));
        assert!(names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        for profile in profiles {
            let weight_sum = profile
                .sources
                .iter()
                .map(|source| source.weight)
                .sum::<Decimal>();
            assert_eq!(weight_sum, Decimal::ONE);
            assert!(profile.sources.len() >= 2);
        }
    }

    #[test]
    fn layered_search_can_plan_model_prediction_trials() {
        let mut config = LayeredSearchConfig::local_professional_default();
        config.market_regime_policies = vec!["off".to_string()];
        config.combo_versions = Vec::new();
        config.prediction_set_ids = vec!["pred-linear-v1".to_string()];
        config.top_n = vec![20];
        config.rebalance_days = vec![20];
        config.score_directions = vec![ScoreDirection::Descending];
        config.skip_top_pct = vec![Decimal::ZERO];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(10, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["heuristic".to_string()];
        config.risk_budget_lookback_days = vec![60];
        config.capacity_penalty_strength = vec![Decimal::ZERO];
        config.industry_max_weight_pct = vec![None];
        config.score_candidate_pool_sizes = vec![0];
        config.universe_profiles = vec!["listed_non_st".to_string()];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::off()];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::off()];
        config.position_risk_controls = vec![PositionRiskControlProfile::off()];
        let mut resource_plan = LocalResourcePlan::for_machine(4, 16);
        resource_plan.max_trials = 1;

        let plan = build_layered_search_plan(&config, &resource_plan);

        assert_eq!(plan.requested_trials, 1);
        assert_eq!(
            plan.trials[0].parameters["signal_source"],
            "model_prediction"
        );
        assert_eq!(
            plan.trials[0].parameters["prediction_set_id"],
            "pred-linear-v1"
        );
        assert!(plan.trials[0].parameters.get("combo_name").is_none());
    }

    #[test]
    fn truncated_layered_search_samples_across_major_axes() {
        let config = LayeredSearchConfig::local_professional_default();
        let mut resource_plan = LocalResourcePlan::for_machine(10, 32);
        resource_plan.max_trials = 8;

        let plan = build_layered_search_plan(&config, &resource_plan);
        let combo_names = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["combo_name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let directions = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["score_direction"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let top_ns = plan
            .trials
            .iter()
            .filter_map(|trial| trial.parameters["top_n"].as_u64())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(plan.requested_trials, 5_267_275_776);
        assert_eq!(plan.planned_trials, 8);
        assert!(plan
            .trials
            .iter()
            .any(|trial| trial.parameters["universe_profile"] == "main_board_non_st"));
        assert!(combo_names.len() > 1);
        assert!(directions.contains("ascending"));
        assert!(directions.contains("descending"));
        assert!(top_ns.len() > 1);
        assert!(plan
            .trials
            .iter()
            .any(|trial| trial.parameters["portfolio_method"] == "risk_budget"));
    }
}
