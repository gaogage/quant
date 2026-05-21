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
pub struct PortfolioSharpeControlProfile {
    pub profile_name: String,
    pub reduce_start: Option<Decimal>,
    pub reduce_full: Option<Decimal>,
    pub lookback_days: Option<usize>,
    pub min_exposure: Option<Decimal>,
}

impl PortfolioSharpeControlProfile {
    pub fn off() -> Self {
        Self {
            profile_name: "off".to_string(),
            reduce_start: None,
            reduce_full: None,
            lookback_days: None,
            min_exposure: None,
        }
    }

    pub fn reduce(
        profile_name: impl Into<String>,
        reduce_start: Decimal,
        reduce_full: Decimal,
        lookback_days: usize,
        min_exposure: Decimal,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            reduce_start: Some(reduce_start),
            reduce_full: Some(reduce_full),
            lookback_days: Some(lookback_days),
            min_exposure: Some(min_exposure),
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

fn with_event_combo_gate_seed_active_in_with_boost(
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

fn with_event_combo_gate_seed_active_in_with_min_score_and_boost(
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

fn with_sharpe_profile_seed(
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

fn with_candidate_risk_filter_seed(mut seed: Value, candidate_risk_filter: &str) -> Value {
    seed["candidate_risk_filter"] = json!(candidate_risk_filter);
    seed
}

fn with_risk_contribution_control_seed(mut seed: Value, risk_contribution_control: &str) -> Value {
    seed["risk_contribution_control"] = json!(risk_contribution_control);
    seed
}

fn with_prediction_confirmation_seed(
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

fn phase7_high_sharpe_boundary_base_seed() -> Option<Value> {
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

fn professional_event_quality_segment_seed_trials() -> Vec<Value> {
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

fn professional_event_surprise_nonlinear_seed_trials() -> Vec<Value> {
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

fn professional_event_strength_segment_seed_trials() -> Vec<Value> {
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

fn professional_event_strength_boost_seed_trials() -> Vec<Value> {
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

fn legacy_full_icir_v3_seed(
    profile_name: &str,
    start_date: &str,
    end_date: &str,
    portfolio_method: &str,
    kelly_fraction: &str,
) -> Value {
    let mut seed = json!({
        "legacy_revalidation_profile": profile_name,
        "market_regime": "off",
        "top_n": 30,
        "rebalance": "20",
        "score_direction": "descending",
        "skip_top_pct": "0.05",
        "max_pairwise_correlation": "0.75",
        "correlation_lookback_days": 60,
        "kelly_fraction": kelly_fraction,
        "kelly_lookback_days": 60,
        "max_position_pct": "0.08",
        "max_gross_exposure": "1",
        "portfolio_method": portfolio_method,
        "risk_budget_lookback_days": 120,
        "capacity_penalty_strength": "0",
        "industry_max_weight_pct": Value::Null,
        "style_risk_budget": "off",
        "candidate_risk_filter": "off",
        "risk_contribution_control": "off",
        "rebalance_hysteresis_pct": "0",
        "partial_rebalance_ratio": "1",
        "score_candidate_pool_size": 0,
        "universe_profile": "all",
        "portfolio_drawdown_control": "off",
        "portfolio_volatility_control": "off",
        "benchmark": "000300.SH",
        "signal_source": "factor_combo",
        "combo_name": "full_icir_16f_v3",
        "version": "1.0.0",
        "start_date": start_date,
        "end_date": end_date,
    });
    if portfolio_method == "risk_budget" {
        seed["kelly_fraction"] = json!("0");
        seed["portfolio_volatility_control"] = json!("vol120_18_55_100");
        seed["portfolio_volatility_target_pct"] = json!("0.18");
        seed["portfolio_volatility_lookback_days"] = json!(120);
        seed["portfolio_volatility_min_exposure"] = json!("0.55");
        seed["portfolio_volatility_max_exposure"] = json!("1");
        seed["capacity_penalty_strength"] = json!("0.75");
    }
    seed
}

fn professional_legacy_alpha_revalidation_seed_trials() -> Vec<Value> {
    vec![
        legacy_full_icir_v3_seed(
            "full_icir_v3_full_history_legacy_exact",
            "20160201",
            "20260511",
            "heuristic",
            "0.25",
        ),
        legacy_full_icir_v3_seed(
            "full_icir_v3_full_history_risk_budget",
            "20160201",
            "20260511",
            "risk_budget",
            "0",
        ),
        legacy_full_icir_v3_seed(
            "full_icir_v3_recent_window_diagnostic",
            "20230512",
            "20260511",
            "heuristic",
            "0.25",
        ),
    ]
}

fn phase7_current_event_window_15pct_anchor_seed() -> Option<Value> {
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

fn with_stop_loss_cooldown_seed(
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

fn with_rebalance_days_seed(mut seed: Value, rebalance_days: usize, profile_name: &str) -> Value {
    seed["rebalance"] = json!(rebalance_days.to_string());
    seed["rebalance_profile"] = json!(profile_name);
    seed
}

fn with_skip_top_seed(mut seed: Value, skip_top_pct: &str, profile_name: &str) -> Value {
    seed["skip_top_pct"] = json!(skip_top_pct);
    seed["skip_top_profile"] = json!(profile_name);
    seed
}

fn with_position_shape_seed(
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

fn with_risk_budget_lookback_seed(
    mut seed: Value,
    profile_name: &str,
    risk_budget_lookback_days: usize,
) -> Value {
    seed["risk_budget_shape_profile"] = json!(profile_name);
    seed["risk_budget_lookback_days"] = json!(risk_budget_lookback_days);
    seed
}

fn with_top_n_seed(mut seed: Value, top_n: usize, profile_name: &str) -> Value {
    seed["top_n"] = json!(top_n);
    seed["top_n_profile"] = json!(profile_name);
    seed
}

fn with_event_sleeve_seed(mut seed: Value, market_regime: &str, sleeve_profile: &str) -> Value {
    seed["event_sleeve_profile"] = json!(sleeve_profile);
    seed["market_regime"] = json!(market_regime);
    seed
}

fn phase7_current_event_window_15pct_risk_budget_180_anchor_seed() -> Option<Value> {
    phase7_current_event_window_15pct_anchor_seed()
        .map(|seed| with_risk_budget_lookback_seed(seed, "risk_budget_180", 180))
}

fn professional_current_anchor_risk_shape_seed_trials() -> Vec<Value> {
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

fn professional_current_anchor_position_frontier_seed_trials() -> Vec<Value> {
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

fn phase7_current_anchor_bg_trial4_seed() -> Option<Value> {
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

fn phase7_current_anchor_high_sharpe_boundary_seed() -> Option<Value> {
    phase7_current_event_window_15pct_risk_budget_180_anchor_seed()
        .map(|seed| with_position_shape_seed(seed, "maxpos14_corr65", "0.14", "0.65"))
}

fn professional_current_anchor_weak_window_repair_seed_trials() -> Vec<Value> {
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

fn professional_current_anchor_sharpe_return_bridge_seed_trials() -> Vec<Value> {
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

fn professional_high_sharpe_return_recovery_seed_trials() -> Vec<Value> {
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

fn professional_risk_memory_bridge_seed_trials() -> Vec<Value> {
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

fn professional_state_return_sharpe_router_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let Some(high_sharpe_boundary) = phase7_current_anchor_high_sharpe_boundary_seed() else {
        return vec![return_anchor];
    };
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        high_sharpe_boundary.clone(),
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let boundary_vol18 = with_volatility_profile_seed(
        high_sharpe_boundary.clone(),
        "vol120_18_55_100",
        "0.18",
        120,
        "0.55",
        "1",
    );
    let mut seeds = Vec::new();

    for (seed, regime, profile_name) in [
        (
            return_anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
            "return_anchor_event_window_15pct",
        ),
        (
            return_anchor.clone(),
            "quality_event_window_return_sharpe_router_v1",
            "return_anchor_router_v1",
        ),
        (
            return_anchor.clone(),
            "quality_event_window_return_sharpe_router_v2",
            "return_anchor_router_v2",
        ),
        (
            boundary_vol18.clone(),
            "quality_event_window_return_sharpe_router_v1",
            "boundary_vol18_router_v1",
        ),
        (
            boundary_vol18.clone(),
            "quality_event_window_return_sharpe_router_v2",
            "boundary_vol18_router_v2",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_event_sleeve_seed(seed, regime, profile_name)],
        );
    }

    for lookback_days in [160, 180] {
        for (regime, suffix) in [
            ("quality_event_window_return_sharpe_router_v1", "router_v1"),
            ("quality_event_window_return_sharpe_router_v2", "router_v2"),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_risk_budget_lookback_seed(
                    with_event_sleeve_seed(
                        valuation45.clone(),
                        regime,
                        &format!("valuation45_{suffix}"),
                    ),
                    &format!("valuation45_rb{lookback_days}_{suffix}"),
                    lookback_days,
                )],
            );
        }
    }

    append_unique_seeds(
        &mut seeds,
        vec![with_top_n_seed(
            with_event_sleeve_seed(
                boundary_vol18,
                "quality_event_window_return_sharpe_router_v2",
                "boundary_top22_router_v2",
            ),
            22,
            "top22_boundary_router_v2",
        )],
    );

    seeds.into_iter().take(10).collect()
}

fn professional_state_return_sharpe_frontier_seed_trials() -> Vec<Value> {
    let Some(return_anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let valuation40 = with_event_combo_gate_seed_with_min_score(
        return_anchor,
        "valuation_exclude_bottom40",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.40",
        "0",
        ScoreDirection::Descending,
    );
    let valuation45 = with_event_combo_gate_seed_with_min_score(
        valuation40.clone(),
        "valuation_exclude_bottom45",
        "phase7_valuation_v1",
        "exclude_negative",
        "0.45",
        "0",
        ScoreDirection::Descending,
    );
    let mut seeds = Vec::new();

    for (regime, suffix, source_seed) in [
        (
            "quality_event_window_return_sharpe_router_v1",
            "router_v1",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v2",
            "router_v2",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v4",
            "router_v4",
            valuation40.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v3",
            "valuation45_router_v3",
            valuation45.clone(),
        ),
        (
            "quality_event_window_return_sharpe_router_v4",
            "valuation45_router_v4",
            valuation45,
        ),
    ] {
        for (vol_profile, target_pct, min_exposure) in [
            ("vol120_16_50_100", "0.16", "0.50"),
            ("vol120_15_48_100", "0.15", "0.48"),
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_risk_budget_lookback_seed(
                    with_volatility_profile_seed(
                        with_event_sleeve_seed(
                            source_seed.clone(),
                            regime,
                            &format!("{suffix}_{vol_profile}"),
                        ),
                        vol_profile,
                        target_pct,
                        120,
                        min_exposure,
                        "1",
                    ),
                    &format!("bp_rb160_{suffix}"),
                    160,
                )],
            );
        }
    }

    for (lookback_days, regime, suffix) in [
        (
            150,
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
        ),
        (
            170,
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
        ),
        (
            180,
            "quality_event_window_return_sharpe_router_v3",
            "router_v3",
        ),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_risk_budget_lookback_seed(
                with_event_sleeve_seed(valuation40.clone(), regime, &format!("bp_{suffix}")),
                &format!("bp_rb{lookback_days}_{suffix}"),
                lookback_days,
            )],
        );
    }

    seeds.into_iter().take(12).collect()
}

fn professional_position_sharpe_return_bridge_seed_trials() -> Vec<Value> {
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

fn professional_moderate_position_sharpe_return_bridge_seed_trials() -> Vec<Value> {
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

fn professional_correlation_frontier_sharpe_return_seed_trials() -> Vec<Value> {
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

fn professional_correlation_threshold_sharpe_return_seed_trials() -> Vec<Value> {
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

fn professional_soft_risk_frontier_sharpe_return_seed_trials() -> Vec<Value> {
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

fn professional_regime_alpha_selector_seed_trials() -> Vec<Value> {
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
            "quality_state_alpha_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_selector_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
        ),
        (
            "quality_state_alpha_selector_v3",
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
            "quality_state_alpha_selector_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_state_alpha_selector_v2",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
        (
            "quality_state_alpha_selector_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
        ),
    ] {
        let mut seed = with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    valuation45.clone(),
                    regime,
                    &format!("{regime}_{vol_profile}"),
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
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn with_rebalance_smoothing_seed(
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

fn professional_regime_alpha_overlay_frontier_seed_trials() -> Vec<Value> {
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

    for (regime, vol_profile, target_pct, min_exposure, smoothing_profile, hysteresis, partial) in [
        (
            "quality_state_alpha_selector_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_overlay_selector_v2",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_overlay_selector_v3",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            "off",
            "0",
            "1",
        ),
        (
            "quality_state_alpha_selector_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
        (
            "quality_state_alpha_overlay_selector_v1",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
        (
            "quality_state_alpha_overlay_selector_v2",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
        (
            "quality_state_alpha_overlay_selector_v3",
            "vol120_16_50_100",
            "0.16",
            "0.50",
            "light_smooth",
            "0.005",
            "0.85",
        ),
    ] {
        let mut seed = with_rebalance_smoothing_seed(
            with_risk_budget_lookback_seed(
                with_volatility_profile_seed(
                    with_event_sleeve_seed(
                        valuation45.clone(),
                        regime,
                        &format!("{regime}_{vol_profile}_{smoothing_profile}"),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("bp_rb160_{regime}_{vol_profile}_{smoothing_profile}"),
                160,
            ),
            smoothing_profile,
            hysteresis,
            partial,
        );
        seed["risk_contribution_control"] = json!("soft_single_name_20pct_v1");
        seed["candidate_risk_filter"] = json!("off");
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_mixed_state_event_alpha_seed_trials() -> Vec<Value> {
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

fn professional_mixed_state_risk_memory_seed_trials() -> Vec<Value> {
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

fn professional_mixed_state_risk_memory_frontier_seed_trials() -> Vec<Value> {
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

fn professional_mixed_state_risk_memory_fine_frontier_seed_trials() -> Vec<Value> {
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

fn professional_mixed_state_exposure_frontier_seed_trials() -> Vec<Value> {
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

fn professional_mixed_state_orthogonal_alpha_seed_trials() -> Vec<Value> {
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

fn professional_candidate_filter_alpha_bridge_seed_trials() -> Vec<Value> {
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

fn professional_soft_candidate_filter_alpha_bridge_seed_trials() -> Vec<Value> {
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

fn professional_sharpe_bridge_frontier_seed_trials() -> Vec<Value> {
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

fn professional_annual_sharpe_floor_bridge_seed_trials() -> Vec<Value> {
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

fn professional_risk_memory_relaxed_frontier_seed_trials() -> Vec<Value> {
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

fn professional_sharpe_floor_auto_discovery_seed_trials() -> Vec<Value> {
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

fn professional_nonlinear_alpha_auto_discovery_seed_trials() -> Vec<Value> {
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

fn professional_nonlinear_sharpe_return_bridge_seed_trials() -> Vec<Value> {
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

fn professional_prediction_confirmed_sharpe_bridge_seed_trials() -> Vec<Value> {
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

fn professional_high_sharpe_boundary_return_bridge_seed_trials() -> Vec<Value> {
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

fn professional_high_sharpe_boundary_event_lift_seed_trials() -> Vec<Value> {
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

fn professional_high_sharpe_micro_frontier_seed_trials() -> Vec<Value> {
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

fn professional_v14_sharpe_return_lift_seed_trials() -> Vec<Value> {
    let Some(boundary) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    for (
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
            500,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            500,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_148_475_100",
            "0.148",
            "0.475",
            180,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "off",
            650,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            400,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "soft_single_name_20pct_v1",
            400,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_148_475_100",
            "0.148",
            "0.475",
            180,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "off",
            650,
        ),
        (
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "-0.10",
            180,
            "0.65",
            "off",
            400,
        ),
        (
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_142_465_100",
            "0.142",
            "0.465",
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
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "off",
            650,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
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
            400,
        ),
        (
            "valuation_exclude_bottom43",
            "0.43",
            "vol120_148_475_100",
            "0.148",
            "0.475",
            170,
            "roll_sharpe180_045_neg10_63",
            "0.45",
            "-0.10",
            180,
            "0.63",
            "soft_single_name_20pct_v1",
            650,
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
                        "quality_mixed_state_risk_memory_router_v14",
                        &format!(
                            "v14_{valuation_profile}_{vol_profile}_{sharpe_profile}_return_lift"
                        ),
                    ),
                    vol_profile,
                    target_pct,
                    120,
                    min_exposure,
                    "1",
                ),
                &format!("bp_rb{risk_budget_lookback_days}_v14_{valuation_profile}_{vol_profile}"),
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

fn professional_v14_shape_lift_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base = with_sharpe_profile_seed(
        with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    with_event_combo_gate_seed_with_min_score(
                        anchor,
                        "valuation_exclude_bottom45",
                        "phase7_valuation_v1",
                        "exclude_negative",
                        "0.45",
                        "0",
                        ScoreDirection::Descending,
                    ),
                    "quality_mixed_state_risk_memory_router_v14",
                    "v14_shape_lift_anchor",
                ),
                "vol120_142_465_100",
                "0.142",
                120,
                "0.465",
                "1",
            ),
            "bp_rb170_v14_shape_lift",
            170,
        ),
        "roll_sharpe180_050_neg10_65",
        "0.50",
        "-0.10",
        180,
        "0.65",
    );
    let base = with_candidate_risk_filter_seed(base, "off");

    let mut seeds = Vec::new();
    for (
        top_n,
        rebalance_days,
        skip_top_pct,
        risk_contribution_control,
        drawdown_profile,
        stop_loss_profile,
        smoothing_profile,
        score_candidate_pool_size,
    ) in [
        (
            20,
            60,
            "0.10",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            55,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            50,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            500,
        ),
        (
            20,
            55,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "hysteresis_1pct_partial_75",
            500,
        ),
        (
            22,
            55,
            "0.08",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            650,
        ),
        (
            22,
            50,
            "0.08",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            650,
        ),
        (
            20,
            50,
            "0.08",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            60,
            "0.08",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_075_cooldown_30",
            "hysteresis_1pct_partial_75",
            500,
        ),
        (
            20,
            55,
            "0.10",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_070_cooldown_30",
            "off",
            500,
        ),
        (
            22,
            60,
            "0.10",
            "off",
            "recover252_08_23_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            650,
        ),
        (
            20,
            55,
            "0.08",
            "soft_single_name_20pct_v1",
            "recover252_08_22_45_30_70",
            "stop_loss_075_cooldown_30",
            "off",
            500,
        ),
        (
            18,
            55,
            "0.10",
            "off",
            "recover252_08_22_45_30_70",
            "stop_loss_070_cooldown_30",
            "hysteresis_1pct_partial_75",
            500,
        ),
    ] {
        let mut seed = with_skip_top_seed(
            with_rebalance_days_seed(
                with_top_n_seed(base.clone(), top_n, &format!("top{top_n}_shape_lift")),
                rebalance_days,
                &format!("rebalance{rebalance_days}_shape_lift"),
            ),
            skip_top_pct,
            &format!("skip{}_shape_lift", skip_top_pct.replace('.', "")),
        );
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        seed = match drawdown_profile {
            "recover252_08_23_45_30_70" => with_drawdown_profile_seed(
                seed,
                "recover252_08_23_45_30_70",
                "0.08",
                "0.23",
                "0.45",
                252,
                "0.30",
                "0.70",
                "1",
            ),
            _ => seed,
        };
        seed = match stop_loss_profile {
            "stop_loss_070_cooldown_30" => {
                with_stop_loss_cooldown_seed(seed, "stop_loss_070_cooldown_30", "0.07", 30)
            }
            _ => seed,
        };
        seed = match smoothing_profile {
            "hysteresis_1pct_partial_75" => {
                with_rebalance_smoothing_seed(seed, "hysteresis_1pct_partial_75", "0.01", "0.75")
            }
            _ => seed,
        };
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_ultra_micro_lift_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base = with_sharpe_profile_seed(
        with_risk_budget_lookback_seed(
            with_volatility_profile_seed(
                with_event_sleeve_seed(
                    with_event_combo_gate_seed_with_min_score(
                        anchor,
                        "valuation_exclude_bottom45",
                        "phase7_valuation_v1",
                        "exclude_negative",
                        "0.45",
                        "0",
                        ScoreDirection::Descending,
                    ),
                    "quality_mixed_state_risk_memory_router_v14",
                    "v14_ultra_micro_lift_anchor",
                ),
                "vol120_142_465_100",
                "0.142",
                120,
                "0.465",
                "1",
            ),
            "bp_rb170_v14_ultra_micro_lift",
            170,
        ),
        "roll_sharpe180_050_neg10_65",
        "0.50",
        "-0.10",
        180,
        "0.65",
    );
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(base, "off"), "off");

    let mut seeds = Vec::new();
    for (
        top_n,
        rebalance_days,
        skip_top_pct,
        vol_profile,
        target_pct,
        min_exposure,
        risk_budget_lookback_days,
    ) in [
        (20, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (19, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (21, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 58, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 62, "0.10", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 60, "0.09", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 60, "0.11", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 60, "0.10", "vol120_141_462_100", "0.141", "0.462", 170),
        (20, 60, "0.10", "vol120_143_467_100", "0.143", "0.467", 170),
        (20, 60, "0.10", "vol120_144_47_100", "0.144", "0.47", 170),
        (19, 58, "0.09", "vol120_142_465_100", "0.142", "0.465", 170),
        (21, 62, "0.11", "vol120_142_465_100", "0.142", "0.465", 170),
        (20, 58, "0.10", "vol120_143_467_100", "0.143", "0.467", 170),
        (20, 62, "0.10", "vol120_143_467_100", "0.143", "0.467", 170),
        (20, 60, "0.10", "vol120_142_465_100", "0.142", "0.465", 180),
    ] {
        let mut seed = with_skip_top_seed(
            with_rebalance_days_seed(
                with_top_n_seed(
                    with_risk_budget_lookback_seed(
                        with_volatility_profile_seed(
                            base.clone(),
                            vol_profile,
                            target_pct,
                            120,
                            min_exposure,
                            "1",
                        ),
                        &format!("bp_rb{risk_budget_lookback_days}_v14_ultra_micro_lift"),
                        risk_budget_lookback_days,
                    ),
                    top_n,
                    &format!("top{top_n}_v14_ultra_micro_lift"),
                ),
                rebalance_days,
                &format!("rebalance{rebalance_days}_v14_ultra_micro_lift"),
            ),
            skip_top_pct,
            &format!("skip{}_v14_ultra_micro_lift", skip_top_pct.replace('.', "")),
        );
        seed["score_candidate_pool_size"] = json!(500);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_annual_floor_micro_lift_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(anchor, "off"), "off");

    let mut seeds = Vec::new();
    for (
        regime,
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_start,
        sharpe_min_exposure,
        risk_budget_lookback_days,
        score_candidate_pool_size,
    ) in [
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v17",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_65",
            "0.50",
            "0.65",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v18",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v16",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_68",
            "0.50",
            "0.68",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v17",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v14",
            "vol120_1405_461_100",
            "0.1405",
            "0.461",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
        ),
        (
            "quality_mixed_state_risk_memory_router_v15",
            "vol120_1405_461_100",
            "0.1405",
            "0.461",
            "roll_sharpe180_0475_neg10_66",
            "0.475",
            "0.66",
            165,
            650,
        ),
    ] {
        let seed = with_market_regime_seed(
            with_skip_top_seed(
                with_rebalance_days_seed(
                    with_top_n_seed(
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
                                &format!("bp_rb{risk_budget_lookback_days}_{regime}_annual_floor"),
                                risk_budget_lookback_days,
                            ),
                            sharpe_profile,
                            sharpe_start,
                            "-0.10",
                            180,
                            sharpe_min_exposure,
                        ),
                        20,
                        "top20_v14_annual_floor",
                    ),
                    60,
                    "rebalance60_v14_annual_floor",
                ),
                "0.10",
                "skip010_v14_annual_floor",
            ),
            regime,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            regime,
            &format!("{regime}_{vol_profile}_{sharpe_profile}_annual_floor_micro_lift"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_near_miss_annual_bridge_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(anchor, "off"), "off");

    let mut seeds = Vec::new();
    for (
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_start,
        sharpe_min_exposure,
        risk_budget_lookback_days,
        score_candidate_pool_size,
        max_position_pct,
        max_pairwise_correlation,
    ) in [
        (
            "vol120_1405_461_100",
            "0.1405",
            "0.461",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1408_4615_100",
            "0.1408",
            "0.4615",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1412_463_100",
            "0.1412",
            "0.463",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_141_462_100",
            "0.141",
            "0.462",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1412_463_100",
            "0.1412",
            "0.463",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1408_4615_100",
            "0.1408",
            "0.4615",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            160,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1412_463_100",
            "0.1412",
            "0.463",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            170,
            500,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            650,
            "0.15",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.145",
            "0.75",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.145",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            160,
            650,
            "0.145",
            "0.70",
        ),
    ] {
        let seed = with_position_shape_seed(
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
                    &format!("bp_rb{risk_budget_lookback_days}_v14_near_miss_annual_bridge"),
                    risk_budget_lookback_days,
                ),
                sharpe_profile,
                sharpe_start,
                "-0.10",
                180,
                sharpe_min_exposure,
            ),
            &format!(
                "maxpos{}_corr{}_v14_near_miss",
                max_position_pct.replace('.', ""),
                max_pairwise_correlation.replace('.', "")
            ),
            max_position_pct,
            max_pairwise_correlation,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            "quality_mixed_state_risk_memory_router_v14",
            &format!("{vol_profile}_{sharpe_profile}_v14_near_miss_annual_bridge"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_v14_corr70_annual_edge_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_high_sharpe_boundary_base_seed() else {
        return Vec::new();
    };
    let base =
        with_risk_contribution_control_seed(with_candidate_risk_filter_seed(anchor, "off"), "off");

    let mut seeds = Vec::new();
    for (
        vol_profile,
        target_pct,
        min_exposure,
        sharpe_profile,
        sharpe_start,
        sharpe_min_exposure,
        risk_budget_lookback_days,
        score_candidate_pool_size,
        max_position_pct,
        max_pairwise_correlation,
    ) in [
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1415_464_100",
            "0.1415",
            "0.464",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1418_4645_100",
            "0.1418",
            "0.4645",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1418_4645_100",
            "0.1418",
            "0.4645",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1422_4655_100",
            "0.1422",
            "0.4655",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1422_4655_100",
            "0.1422",
            "0.4655",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_66",
            "0.50",
            "0.66",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_143_467_100",
            "0.143",
            "0.467",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_68",
            "0.50",
            "0.68",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_68",
            "0.50",
            "0.68",
            165,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            650,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            650,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            168,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_1425_466_100",
            "0.1425",
            "0.466",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            168,
            500,
            "0.15",
            "0.70",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.68",
        ),
        (
            "vol120_142_465_100",
            "0.142",
            "0.465",
            "roll_sharpe180_050_neg10_67",
            "0.50",
            "0.67",
            165,
            500,
            "0.15",
            "0.72",
        ),
    ] {
        let seed = with_position_shape_seed(
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
                    &format!("bp_rb{risk_budget_lookback_days}_v14_corr70_annual_edge"),
                    risk_budget_lookback_days,
                ),
                sharpe_profile,
                sharpe_start,
                "-0.10",
                180,
                sharpe_min_exposure,
            ),
            &format!(
                "maxpos{}_corr{}_v14_corr70_edge",
                max_position_pct.replace('.', ""),
                max_pairwise_correlation.replace('.', "")
            ),
            max_position_pct,
            max_pairwise_correlation,
        );
        let mut seed = with_event_sleeve_seed(
            seed,
            "quality_mixed_state_risk_memory_router_v14",
            &format!("{vol_profile}_{sharpe_profile}_v14_corr70_annual_edge"),
        );
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_return_alpha_sharpe_bridge_seed_trials() -> Vec<Value> {
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

fn professional_regime_frontier_bridge_seed_trials() -> Vec<Value> {
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
        sharpe_min_exposure,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v1",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_15_48_100",
            "0.15",
            "0.48",
            160,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "soft_single_name_20pct_v1",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v2",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_142_465_100",
            "0.142",
            "0.465",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            180,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v3",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "off",
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
                                valuation_profile,
                                "phase7_valuation_v1",
                                "exclude_negative",
                                valuation_min_score,
                                "0",
                                ScoreDirection::Descending,
                            ),
                            regime,
                            &format!(
                                "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_frontier_bridge"
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
                &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}"),
                risk_budget_lookback_days,
            ),
            sharpe_profile,
            "0.50",
            "-0.10",
            180,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_regime_frontier_decomposition_seed_trials() -> Vec<Value> {
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
        sharpe_min_exposure,
        risk_contribution_control,
        score_candidate_pool_size,
    ) in [
        (
            "quality_frontier_regime_bridge_router_v4",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v4",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v4",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v5",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v5",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v5",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v6",
            "valuation_exclude_bottom40",
            "0.40",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "soft_single_name_20pct_v1",
            650,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_141_462_100",
            "0.141",
            "0.462",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_143_467_100",
            "0.143",
            "0.467",
            170,
            "roll_sharpe180_050_neg10_65",
            "0.65",
            "off",
            500,
        ),
        (
            "quality_frontier_regime_bridge_router_v7",
            "valuation_exclude_bottom45",
            "0.45",
            "vol120_145_47_100",
            "0.145",
            "0.47",
            180,
            "roll_sharpe180_050_neg10_60",
            "0.60",
            "off",
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
                                valuation_profile,
                                "phase7_valuation_v1",
                                "exclude_negative",
                                valuation_min_score,
                                "0",
                                ScoreDirection::Descending,
                            ),
                            regime,
                            &format!(
                                "{regime}_{valuation_profile}_{vol_profile}_{sharpe_profile}_frontier_decomp"
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
                &format!("bp_rb{risk_budget_lookback_days}_{regime}_{valuation_profile}"),
                risk_budget_lookback_days,
            ),
            sharpe_profile,
            "0.50",
            "-0.10",
            180,
            sharpe_min_exposure,
        );
        seed["candidate_risk_filter"] = json!("off");
        seed["risk_contribution_control"] = json!(risk_contribution_control);
        seed["score_candidate_pool_size"] = json!(score_candidate_pool_size);
        append_unique_seeds(&mut seeds, vec![seed]);
    }

    seeds
}

fn professional_high_sharpe_return_micro_bridge_seed_trials() -> Vec<Value> {
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

fn finalize_return_distribution_seed(seed: Value) -> Value {
    with_risk_contribution_control_seed(
        with_candidate_risk_filter_seed(seed, "off"),
        "soft_single_name_20pct_v1",
    )
}

fn professional_return_distribution_repair_seed_trials() -> Vec<Value> {
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

fn professional_state_alpha_router_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let vol18_anchor =
        with_volatility_profile_seed(anchor.clone(), "vol120_18_55_100", "0.18", 120, "0.55", "1");
    let seed_specs = [
        (
            anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        ),
        (
            anchor.clone(),
            "quality_regime_alpha_overlay_value_05pct_v1",
        ),
        (
            anchor.clone(),
            "quality_regime_alpha_overlay_blend_10pct_v1",
        ),
        (
            anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1",
        ),
        (
            anchor,
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        ),
        (
            vol18_anchor.clone(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1",
        ),
        (
            vol18_anchor.clone(),
            "quality_regime_alpha_overlay_blend_10pct_v1",
        ),
        (
            vol18_anchor,
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1",
        ),
    ];
    let mut seeds = Vec::new();

    for (seed, policy) in seed_specs {
        append_unique_seeds(&mut seeds, vec![with_market_regime_seed(seed, policy)]);
    }

    seeds
}

fn professional_state_position_risk_router_seed_trials() -> Vec<Value> {
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
            "quality_bear_position_guard_v3",
            "quality_bear_position_guard_v1",
            "quality_bear_position_guard_v2",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), market_regime)],
            );
        }
    }

    seeds
}

fn professional_event_position_risk_router_seed_trials() -> Vec<Value> {
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

fn professional_all_regime_event_sleeve_seed_trials() -> Vec<Value> {
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
            "quality_all_regime_event_window_sleeve_05pct_v1",
            "quality_all_regime_event_window_sleeve_10pct_v1",
            "quality_all_regime_event_window_sleeve_15pct_v1",
        ] {
            append_unique_seeds(
                &mut seeds,
                vec![with_market_regime_seed(seed.clone(), market_regime)],
            );
        }
    }

    seeds
}

fn professional_portfolio_sharpe_control_seed_trials() -> Vec<Value> {
    let Some(anchor) = phase7_current_anchor_bg_trial4_seed() else {
        return Vec::new();
    };
    let Some(high_sharpe_boundary) = phase7_current_anchor_high_sharpe_boundary_seed() else {
        return Vec::new();
    };
    let mut seeds = Vec::new();

    append_unique_seeds(&mut seeds, vec![anchor.clone(), high_sharpe_boundary]);

    for (profile_name, reduce_start, reduce_full, lookback_days, min_exposure) in [
        ("roll_sharpe120_060_000_55", "0.60", "0.00", 120, "0.55"),
        ("roll_sharpe120_050_neg10_60", "0.50", "-0.10", 120, "0.60"),
        ("roll_sharpe180_060_000_55", "0.60", "0.00", 180, "0.55"),
        ("roll_sharpe180_050_neg10_60", "0.50", "-0.10", 180, "0.60"),
    ] {
        append_unique_seeds(
            &mut seeds,
            vec![with_sharpe_profile_seed(
                anchor.clone(),
                profile_name,
                reduce_start,
                reduce_full,
                lookback_days,
                min_exposure,
            )],
        );
    }

    let bridge = with_market_regime_seed(anchor, "quality_state_sharpe_bridge_router_v2");
    append_unique_seeds(
        &mut seeds,
        vec![
            with_sharpe_profile_seed(
                bridge.clone(),
                "bridge_roll_sharpe120_050_neg10_60",
                "0.50",
                "-0.10",
                120,
                "0.60",
            ),
            with_sharpe_profile_seed(
                bridge,
                "bridge_roll_sharpe180_050_neg10_60",
                "0.50",
                "-0.10",
                180,
                "0.60",
            ),
        ],
    );

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
    pub portfolio_sharpe_controls: Vec<PortfolioSharpeControlProfile>,
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
            portfolio_sharpe_controls: vec![PortfolioSharpeControlProfile::off()],
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

    pub fn professional_event_quality_segment_default() -> Self {
        let mut config = Self::professional_event_window_decay_default();
        config.market_regime_policies = vec![
            "quality_bear_window_guard_v2".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_event_quality_segment_seed_trials();
        config
    }

    pub fn professional_event_surprise_nonlinear_default() -> Self {
        let mut config = Self::professional_event_window_decay_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_event_surprise_confirm_v1", "1.0.0"),
        ];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_3pct_stress_only",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_surprise(
                "event_surprise_boost_pos_5pct_stress_only",
                "boost_positive",
                Decimal::ZERO,
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_surprise(
                "event_surprise_exclude_negative_stress_only",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_surprise(
                "event_surprise_require_positive_stress_only",
                "require_positive",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
        ];
        config.seed_trials = professional_event_surprise_nonlinear_seed_trials();
        config
    }

    pub fn professional_event_strength_segment_default() -> Self {
        let mut config = Self::professional_event_surprise_nonlinear_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.market_regime_policies = vec!["quality_bear_window_guard_v2".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_require_strong_p75",
                "require_positive",
                Decimal::new(38, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_require_strong_p90",
                "require_positive",
                Decimal::new(66, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_require_strong_p75",
                "require_positive",
                Decimal::new(35, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_require_strong_p90",
                "require_positive",
                Decimal::new(43, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_confirm_require_light_p50",
                "phase7_event_earnings_v1",
                "require_positive",
                Decimal::new(39, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_event_strength_segment_seed_trials();
        config
    }

    pub fn professional_event_strength_boost_default() -> Self {
        let mut config = Self::professional_event_strength_segment_default();
        config.event_gate_profiles = vec![
            EventGateProfile::off(),
            EventGateProfile::event_window(
                "event_window_boost_strong_p75_3pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "event_window_boost_strong_p75_5pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_boost_strong_p75_3pct",
                "boost_positive",
                Decimal::new(35, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_surprise(
                "event_surprise_boost_strong_p75_5pct",
                "boost_positive",
                Decimal::new(35, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_confirm_boost_light_p50_3pct",
                "phase7_event_earnings_v1",
                "boost_positive",
                Decimal::new(39, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_confirm_boost_light_p50_5pct",
                "phase7_event_earnings_v1",
                "boost_positive",
                Decimal::new(39, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_event_strength_boost_seed_trials();
        config
    }

    pub fn professional_legacy_alpha_revalidation_default() -> Self {
        let mut config = Self::professional_event_window_sleeve_upper_bound_default();
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("full_icir_16f_v3", "1.0.0"),
        ];
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "off".to_string(),
        ];
        config.top_n = vec![20, 30];
        config.rebalance_days = vec![20, 60];
        config.score_directions = vec![ScoreDirection::Ascending, ScoreDirection::Descending];
        config.skip_top_pct = vec![Decimal::new(5, 2), Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.kelly_fraction = vec![Decimal::ZERO, Decimal::new(25, 2)];
        config.max_position_pct = vec![Decimal::new(8, 2), Decimal::new(15, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["heuristic".to_string(), "risk_budget".to_string()];
        config.risk_budget_lookback_days = vec![120];
        config.capacity_penalty_strength = vec![Decimal::ZERO, Decimal::new(75, 2)];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.score_candidate_pool_sizes = vec![0, 500];
        config.universe_profiles = vec!["all".to_string()];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::off(),
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
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::off(),
            PortfolioVolatilityControlProfile::target(
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![
            PositionRiskControlProfile::off(),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_30",
                Decimal::new(75, 3),
                30,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::off()];
        config.seed_trials = professional_event_window_sleeve_upper_bound_seed_trials();
        append_unique_seeds(
            &mut config.seed_trials,
            professional_legacy_alpha_revalidation_seed_trials(),
        );
        config
    }

    pub fn professional_current_anchor_risk_shape_default() -> Self {
        let mut config = Self::professional_event_window_sleeve_upper_bound_default();
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.market_regime_policies =
            vec!["quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.score_directions = vec![ScoreDirection::Ascending];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(75, 2)];
        config.kelly_fraction = vec![Decimal::ZERO];
        config.max_position_pct = vec![Decimal::new(12, 2), Decimal::new(15, 2)];
        config.max_gross_exposure = vec![Decimal::ONE];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.risk_budget_lookback_days = vec![120, 180];
        config.capacity_penalty_strength = vec![Decimal::new(75, 2)];
        config.industry_max_weight_pct = vec![None];
        config.style_risk_budget_profiles = vec!["off".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.score_candidate_pool_sizes = vec![500];
        config.universe_profiles = vec!["all".to_string()];
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
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
            PortfolioDrawdownControlProfile::recover(
                "recover126_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(126),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
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
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol252_16_50_100",
                Decimal::new(16, 2),
                252,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_30",
                Decimal::new(75, 3),
                30,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_070_cooldown_30",
                Decimal::new(7, 2),
                30,
            ),
            PositionRiskControlProfile::stop_loss_with_cooldown(
                "stop_loss_075_cooldown_45",
                Decimal::new(75, 3),
                45,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_current_anchor_risk_shape_seed_trials();
        config
    }

    pub fn professional_current_anchor_position_frontier_default() -> Self {
        let mut config = Self::professional_current_anchor_risk_shape_default();
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(70, 2),
            Decimal::new(75, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(12, 2),
            Decimal::new(13, 2),
            Decimal::new(14, 2),
            Decimal::new(15, 2),
        ];
        config.risk_budget_lookback_days = vec![180];
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
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
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
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.position_risk_controls = vec![PositionRiskControlProfile::stop_loss_with_cooldown(
            "stop_loss_075_cooldown_30",
            Decimal::new(75, 3),
            30,
        )];
        config.seed_trials = professional_current_anchor_position_frontier_seed_trials();
        config
    }

    pub fn professional_current_anchor_weak_window_repair_default() -> Self {
        let mut config = Self::professional_current_anchor_position_frontier_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_budget_lookback_days = vec![180];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_08_22_45_30_70",
            Decimal::new(8, 2),
            Decimal::new(22, 2),
            Decimal::new(45, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_16_50_100",
            Decimal::new(16, 2),
            120,
            Decimal::new(50, 2),
            Decimal::ONE,
        )];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_window(
                "stress_event_window_boost_p75_3pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_window(
                "stress_event_window_boost_p75_5pct",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_window(
                "stress_event_window_exclude_negative_p40",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "stress_valuation_boost_p40_3pct",
                "phase7_valuation_v1",
                "boost_positive",
                Decimal::new(40, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "stress_valuation_boost_p40_5pct",
                "phase7_valuation_v1",
                "boost_positive",
                Decimal::new(40, 2),
                Decimal::new(5, 2),
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
            EventGateProfile::event_combo(
                "stress_valuation_exclude_p40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            )
            .active_in(&["bear", "high_volatility"]),
        ];
        config.seed_trials = professional_current_anchor_weak_window_repair_seed_trials();
        config
    }

    pub fn professional_current_anchor_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_current_anchor_position_frontier_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(70, 2)];
        config.max_position_pct = vec![Decimal::new(14, 2), Decimal::new(15, 2)];
        config.top_n = vec![20, 25];
        config.risk_budget_lookback_days = vec![180];
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
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
                Some(252),
                Decimal::new(30, 2),
                Decimal::new(70, 2),
                Decimal::ONE,
            ),
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
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
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
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_current_anchor_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_high_sharpe_return_recovery_default() -> Self {
        let mut config = Self::professional_current_anchor_sharpe_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
        ];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(675, 3),
            Decimal::new(70, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(145, 3),
            Decimal::new(15, 2),
        ];
        config.top_n = vec![20, 22, 25];
        config.risk_budget_lookback_days = vec![180];
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
                "vol120_18_55_100",
                Decimal::new(18, 2),
                120,
                Decimal::new(55, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_19_58_100",
                Decimal::new(19, 2),
                120,
                Decimal::new(58, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_20_60_100",
                Decimal::new(20, 2),
                120,
                Decimal::new(60, 2),
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
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.seed_trials = professional_high_sharpe_return_recovery_seed_trials();
        config
    }

    pub fn professional_risk_memory_bridge_default() -> Self {
        let mut config = Self::professional_high_sharpe_return_recovery_default();
        config.risk_budget_lookback_days = vec![120, 140, 150, 160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(675, 3)];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(145, 3),
            Decimal::new(15, 2),
        ];
        config.top_n = vec![20];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_18_55_100",
            Decimal::new(18, 2),
            120,
            Decimal::new(55, 2),
            Decimal::ONE,
        )];
        config.event_gate_profiles = vec![
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
        ];
        config.seed_trials = professional_risk_memory_bridge_seed_trials();
        config
    }

    pub fn professional_state_return_sharpe_router_default() -> Self {
        let mut config = Self::professional_risk_memory_bridge_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_event_window_return_sharpe_router_v1".to_string(),
            "quality_event_window_return_sharpe_router_v2".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(65, 2), Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(14, 2), Decimal::new(15, 2)];
        config.top_n = vec![20, 22];
        config.portfolio_drawdown_controls = vec![
            PortfolioDrawdownControlProfile::recover(
                "recover252_08_22_45_30_70",
                Decimal::new(8, 2),
                Decimal::new(22, 2),
                Decimal::new(45, 2),
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
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
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
        ];
        config.seed_trials = professional_state_return_sharpe_router_seed_trials();
        config
    }

    pub fn professional_state_return_sharpe_frontier_default() -> Self {
        let mut config = Self::professional_state_return_sharpe_router_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v1".to_string(),
            "quality_event_window_return_sharpe_router_v2".to_string(),
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![150, 160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(15, 2),
            Decimal::new(16, 2),
        ];
        config.top_n = vec![20];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![
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
        ];
        config.seed_trials = professional_state_return_sharpe_frontier_seed_trials();
        config
    }

    pub fn professional_position_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_state_return_sharpe_frontier_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![150, 160, 170, 180];
        config.max_pairwise_correlation = vec![
            Decimal::new(65, 2),
            Decimal::new(675, 3),
            Decimal::new(70, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(14, 2),
            Decimal::new(145, 3),
            Decimal::new(15, 2),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
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
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_position_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_moderate_position_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_position_sharpe_return_bridge_default();
        config.max_pairwise_correlation = vec![
            Decimal::new(70, 2),
            Decimal::new(725, 3),
            Decimal::new(75, 2),
        ];
        config.max_position_pct = vec![
            Decimal::new(16, 2),
            Decimal::new(17, 2),
            Decimal::new(18, 2),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.seed_trials = professional_moderate_position_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_correlation_frontier_sharpe_return_default() -> Self {
        let mut config = Self::professional_moderate_position_sharpe_return_bridge_default();
        config.market_regime_policies =
            vec!["quality_event_window_return_sharpe_router_v4".to_string()];
        config.max_pairwise_correlation = vec![
            Decimal::new(705, 3),
            Decimal::new(71, 2),
            Decimal::new(715, 3),
            Decimal::new(72, 2),
        ];
        config.max_position_pct = vec![Decimal::new(16, 2), Decimal::new(165, 3)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.seed_trials = professional_correlation_frontier_sharpe_return_seed_trials();
        config
    }

    pub fn professional_correlation_threshold_sharpe_return_default() -> Self {
        let mut config = Self::professional_correlation_frontier_sharpe_return_default();
        config.max_pairwise_correlation = vec![
            Decimal::new(706, 3),
            Decimal::new(707, 3),
            Decimal::new(708, 3),
            Decimal::new(709, 3),
        ];
        config.seed_trials = professional_correlation_threshold_sharpe_return_seed_trials();
        config
    }

    pub fn professional_soft_risk_frontier_sharpe_return_default() -> Self {
        let mut config = Self::professional_state_return_sharpe_frontier_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec![
            "off".to_string(),
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "low_volatility_v1".to_string(),
            "low_volatility_low_correlation_v1".to_string(),
        ];
        config.seed_trials = professional_soft_risk_frontier_sharpe_return_seed_trials();
        config
    }

    pub fn professional_regime_alpha_selector_default() -> Self {
        let mut config = Self::professional_soft_risk_frontier_sharpe_return_default();
        config.market_regime_policies = vec![
            "quality_event_window_return_sharpe_router_v4".to_string(),
            "quality_state_alpha_selector_v1".to_string(),
            "quality_state_alpha_selector_v2".to_string(),
            "quality_state_alpha_selector_v3".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_regime_alpha_selector_seed_trials();
        config
    }

    pub fn professional_regime_alpha_overlay_frontier_default() -> Self {
        let mut config = Self::professional_regime_alpha_selector_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_selector_v3".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_alpha_overlay_selector_v2".to_string(),
            "quality_state_alpha_overlay_selector_v3".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO, Decimal::new(5, 3)];
        config.partial_rebalance_ratio = vec![Decimal::ONE, Decimal::new(85, 2)];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_regime_alpha_overlay_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_event_alpha_default() -> Self {
        let mut config = Self::professional_regime_alpha_overlay_frontier_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v2".to_string(),
            "quality_mixed_event_state_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_overlay_selector_v2".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.risk_budget_lookback_days = vec![160];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.rebalance_hysteresis_pct = vec![Decimal::ZERO];
        config.partial_rebalance_ratio = vec![Decimal::ONE];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_mixed_state_event_alpha_seed_trials();
        config
    }

    pub fn professional_mixed_state_risk_memory_default() -> Self {
        let mut config = Self::professional_mixed_state_event_alpha_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v2".to_string(),
            "quality_mixed_state_risk_memory_router_v3".to_string(),
        ];
        config.seed_trials = professional_mixed_state_risk_memory_seed_trials();
        config
    }

    pub fn professional_mixed_state_risk_memory_frontier_default() -> Self {
        let mut config = Self::professional_mixed_state_risk_memory_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v4".to_string(),
            "quality_mixed_state_risk_memory_router_v5".to_string(),
            "quality_mixed_state_risk_memory_router_v6".to_string(),
        ];
        config.seed_trials = professional_mixed_state_risk_memory_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_risk_memory_fine_frontier_default() -> Self {
        let mut config = Self::professional_mixed_state_risk_memory_frontier_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v4".to_string(),
            "quality_mixed_state_risk_memory_router_v7".to_string(),
            "quality_mixed_state_risk_memory_router_v8".to_string(),
            "quality_mixed_state_risk_memory_router_v9".to_string(),
            "quality_mixed_state_risk_memory_router_v10".to_string(),
        ];
        config.seed_trials = professional_mixed_state_risk_memory_fine_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_exposure_frontier_default() -> Self {
        let mut config = Self::professional_mixed_state_risk_memory_frontier_default();
        config.market_regime_policies = vec![
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_state_risk_memory_router_v4".to_string(),
            "quality_mixed_state_risk_memory_router_v11".to_string(),
            "quality_mixed_state_risk_memory_router_v12".to_string(),
            "quality_mixed_state_risk_memory_router_v13".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.seed_trials = professional_mixed_state_exposure_frontier_seed_trials();
        config
    }

    pub fn professional_mixed_state_orthogonal_alpha_default() -> Self {
        let mut config = Self::professional_mixed_state_exposure_frontier_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_alpha_selector_v1".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v2".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v2".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_residual_confirm_10pct_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_value_recovery_event_confirm_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_defensive_rel_v1", "1.0.0"),
        ];
        config.seed_trials = professional_mixed_state_orthogonal_alpha_seed_trials();
        config
    }

    pub fn professional_candidate_filter_alpha_bridge_default() -> Self {
        let mut config = Self::professional_mixed_state_orthogonal_alpha_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_alpha_overlay_selector_v2".to_string(),
            "quality_mixed_event_state_selector_v1".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v1".to_string(),
            "quality_mixed_orthogonal_alpha_selector_v2".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
        ];
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "low_volatility_v1".to_string(),
            "low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec![
            "soft_single_name_20pct_v1".to_string(),
            "soft_single_name_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_candidate_filter_alpha_bridge_seed_trials();
        config
    }

    pub fn professional_soft_candidate_filter_alpha_bridge_default() -> Self {
        let mut config = Self::professional_candidate_filter_alpha_bridge_default();
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "soft_low_volatility_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_soft_candidate_filter_alpha_bridge_seed_trials();
        config
    }

    pub fn professional_sharpe_bridge_frontier_default() -> Self {
        let mut config = Self::professional_soft_candidate_filter_alpha_bridge_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_sharpe_bridge_router_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_state_sharpe_bridge_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_sharpe_bridge_frontier_seed_trials();
        config
    }

    pub fn professional_annual_sharpe_floor_bridge_default() -> Self {
        let mut config = Self::professional_sharpe_bridge_frontier_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
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
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_annual_sharpe_floor_bridge_seed_trials();
        config
    }

    pub fn professional_risk_memory_relaxed_frontier_default() -> Self {
        let mut config = Self::professional_annual_sharpe_floor_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_mixed_state_risk_memory_router_v15".to_string(),
            "quality_mixed_state_risk_memory_router_v16".to_string(),
            "quality_mixed_state_risk_memory_router_v17".to_string(),
            "quality_mixed_state_risk_memory_router_v18".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
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
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.seed_trials = professional_risk_memory_relaxed_frontier_seed_trials();
        config
    }

    pub fn professional_sharpe_floor_auto_discovery_default() -> Self {
        let mut config = Self::professional_annual_sharpe_floor_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v16".to_string(),
            "quality_mixed_state_risk_memory_router_v18".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
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
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_sharpe_floor_auto_discovery_seed_trials();
        config
    }

    pub fn professional_nonlinear_alpha_auto_discovery_default() -> Self {
        let mut config = Self::professional_sharpe_floor_auto_discovery_default();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_router_v1".to_string(),
            "quality_nonlinear_alpha_router_v2".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::off(),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_060_000_55",
                Decimal::new(60, 2),
                Decimal::ZERO,
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_060_000_60",
                Decimal::new(60, 2),
                Decimal::ZERO,
                180,
                Decimal::new(60, 2),
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
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_nonlinear_alpha_auto_discovery_seed_trials();
        config
    }

    pub fn professional_nonlinear_sharpe_return_bridge_default() -> Self {
        let mut config = Self::professional_nonlinear_alpha_auto_discovery_default();
        config.market_regime_policies = vec![
            "quality_nonlinear_alpha_risk_memory_router_v2".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_nonlinear_alpha_router_v2".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_55",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_060_000_55",
                Decimal::new(60, 2),
                Decimal::ZERO,
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_060_000_60",
                Decimal::new(60, 2),
                Decimal::ZERO,
                180,
                Decimal::new(60, 2),
            ),
        ];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.seed_trials = professional_nonlinear_sharpe_return_bridge_seed_trials();
        config
    }

    pub fn professional_prediction_confirmed_sharpe_bridge_default() -> Self {
        let mut config = Self::professional_nonlinear_sharpe_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
            "quality_state_alpha_overlay_selector_v1".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_17_52_100",
                Decimal::new(17, 2),
                120,
                Decimal::new(52, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
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
            EventGateProfile::event_combo(
                "valuation_exclude_bottom45",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(45, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_prediction_confirmed_sharpe_bridge_seed_trials();
        config
    }

    pub fn professional_high_sharpe_boundary_return_bridge_default() -> Self {
        let mut config = Self::professional_prediction_confirmed_sharpe_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.prediction_set_ids = Vec::new();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_155_49_100",
                Decimal::new(155, 3),
                120,
                Decimal::new(49, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(16, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.seed_trials = professional_high_sharpe_boundary_return_bridge_seed_trials();
        config
    }

    pub fn professional_high_sharpe_boundary_event_lift_default() -> Self {
        let mut config = Self::professional_high_sharpe_boundary_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::off()];
        config.seed_trials = professional_high_sharpe_boundary_event_lift_seed_trials();
        config
    }

    pub fn professional_high_sharpe_micro_frontier_default() -> Self {
        let mut config = Self::professional_high_sharpe_boundary_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_mixed_state_risk_memory_router_v14".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
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
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_155_49_100",
                Decimal::new(155, 3),
                120,
                Decimal::new(49, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec![
            "off".to_string(),
            "soft_low_volatility_v1".to_string(),
            "soft_low_volatility_low_correlation_v1".to_string(),
        ];
        config.score_candidate_pool_sizes = vec![400, 500, 650, 800];
        config.seed_trials = professional_high_sharpe_micro_frontier_seed_trials();
        config
    }

    pub fn professional_v14_sharpe_return_lift_default() -> Self {
        let mut config = Self::professional_high_sharpe_micro_frontier_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
            EventGateProfile::event_combo(
                "valuation_exclude_bottom40",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "valuation_exclude_bottom43",
                "phase7_valuation_v1",
                "exclude_negative",
                Decimal::new(43, 2),
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
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_14_46_100",
                Decimal::new(14, 2),
                120,
                Decimal::new(46, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_148_475_100",
                Decimal::new(148, 3),
                120,
                Decimal::new(475, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_045_neg10_63",
                Decimal::new(45, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(63, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![400, 500, 650];
        config.seed_trials = professional_v14_sharpe_return_lift_seed_trials();
        config
    }

    pub fn professional_v14_shape_lift_default() -> Self {
        let mut config = Self::professional_v14_sharpe_return_lift_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![18, 20, 22];
        config.rebalance_days = vec![50, 55, 60];
        config.skip_top_pct = vec![Decimal::new(8, 2), Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![PortfolioVolatilityControlProfile::target(
            "vol120_142_465_100",
            Decimal::new(142, 3),
            120,
            Decimal::new(465, 3),
            Decimal::ONE,
        )];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::reduce(
            "roll_sharpe180_050_neg10_65",
            Decimal::new(50, 2),
            Decimal::new(-10, 2),
            180,
            Decimal::new(65, 2),
        )];
        config.risk_budget_lookback_days = vec![170];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_shape_lift_seed_trials();
        config
    }

    pub fn professional_v14_ultra_micro_lift_default() -> Self {
        let mut config = Self::professional_v14_shape_lift_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![19, 20, 21];
        config.rebalance_days = vec![58, 60, 62];
        config.skip_top_pct = vec![Decimal::new(9, 2), Decimal::new(10, 2), Decimal::new(11, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_144_47_100",
                Decimal::new(144, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![PortfolioSharpeControlProfile::reduce(
            "roll_sharpe180_050_neg10_65",
            Decimal::new(50, 2),
            Decimal::new(-10, 2),
            180,
            Decimal::new(65, 2),
        )];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500];
        config.seed_trials = professional_v14_ultra_micro_lift_seed_trials();
        config
    }

    pub fn professional_v14_annual_floor_micro_lift_default() -> Self {
        let mut config = Self::professional_v14_ultra_micro_lift_default();
        config.market_regime_policies = vec![
            "quality_mixed_state_risk_memory_router_v14".to_string(),
            "quality_mixed_state_risk_memory_router_v15".to_string(),
            "quality_mixed_state_risk_memory_router_v16".to_string(),
            "quality_mixed_state_risk_memory_router_v17".to_string(),
            "quality_mixed_state_risk_memory_router_v18".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_1405_461_100",
                Decimal::new(1405, 4),
                120,
                Decimal::new(461, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_0475_neg10_66",
                Decimal::new(475, 3),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_66",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_68",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(68, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![165, 170];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_annual_floor_micro_lift_seed_trials();
        config
    }

    pub fn professional_v14_near_miss_annual_bridge_default() -> Self {
        let mut config = Self::professional_v14_annual_floor_micro_lift_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_1405_461_100",
                Decimal::new(1405, 4),
                120,
                Decimal::new(461, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1408_4615_100",
                Decimal::new(1408, 4),
                120,
                Decimal::new(4615, 4),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1412_463_100",
                Decimal::new(1412, 4),
                120,
                Decimal::new(463, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1415_464_100",
                Decimal::new(1415, 4),
                120,
                Decimal::new(464, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_66",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_67",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(67, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 165, 170];
        config.max_pairwise_correlation = vec![Decimal::new(70, 2), Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(145, 3), Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_near_miss_annual_bridge_seed_trials();
        config
    }

    pub fn professional_v14_corr70_annual_edge_default() -> Self {
        let mut config = Self::professional_v14_near_miss_annual_bridge_default();
        config.market_regime_policies =
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_1415_464_100",
                Decimal::new(1415, 4),
                120,
                Decimal::new(464, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1418_4645_100",
                Decimal::new(1418, 4),
                120,
                Decimal::new(4645, 4),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1422_4655_100",
                Decimal::new(1422, 4),
                120,
                Decimal::new(4655, 4),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_1425_466_100",
                Decimal::new(1425, 4),
                120,
                Decimal::new(466, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_66",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(66, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_67",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(67, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_68",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(68, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![165, 168];
        config.max_pairwise_correlation = vec![
            Decimal::new(68, 2),
            Decimal::new(70, 2),
            Decimal::new(72, 2),
        ];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_v14_corr70_annual_edge_seed_trials();
        config
    }

    pub fn professional_return_alpha_sharpe_bridge_default() -> Self {
        let mut config = Self::professional_v14_ultra_micro_lift_default();
        config.market_regime_policies = vec![
            "quality_state_alpha_overlay_selector_v1".to_string(),
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
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
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_return_alpha_sharpe_bridge_seed_trials();
        config
    }

    pub fn professional_regime_frontier_bridge_default() -> Self {
        let mut config = Self::professional_return_alpha_sharpe_bridge_default();
        config.market_regime_policies = vec![
            "quality_frontier_regime_bridge_router_v1".to_string(),
            "quality_frontier_regime_bridge_router_v2".to_string(),
            "quality_frontier_regime_bridge_router_v3".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
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
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_142_465_100",
                Decimal::new(142, 3),
                120,
                Decimal::new(465, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_regime_frontier_bridge_seed_trials();
        config
    }

    pub fn professional_regime_frontier_decomposition_default() -> Self {
        let mut config = Self::professional_regime_frontier_bridge_default();
        config.market_regime_policies = vec![
            "quality_frontier_regime_bridge_router_v4".to_string(),
            "quality_frontier_regime_bridge_router_v5".to_string(),
            "quality_frontier_regime_bridge_router_v6".to_string(),
            "quality_frontier_regime_bridge_router_v7".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![
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
        ];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_141_462_100",
                Decimal::new(141, 3),
                120,
                Decimal::new(462, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_65",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(65, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles =
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_regime_frontier_decomposition_seed_trials();
        config
    }

    pub fn professional_high_sharpe_return_micro_bridge_default() -> Self {
        let mut config = Self::professional_regime_frontier_decomposition_default();
        config.market_regime_policies = vec![
            "quality_frontier_regime_bridge_router_v6".to_string(),
            "quality_frontier_regime_bridge_router_v7".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.score_directions = vec![ScoreDirection::Ascending];
        config.top_n = vec![20];
        config.rebalance_days = vec![60];
        config.skip_top_pct = vec![Decimal::new(10, 2)];
        config.portfolio_methods = vec!["risk_budget".to_string()];
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom45",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(45, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_143_467_100",
                Decimal::new(143, 3),
                120,
                Decimal::new(467, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_145_47_100",
                Decimal::new(145, 3),
                120,
                Decimal::new(47, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_147_475_100",
                Decimal::new(147, 3),
                120,
                Decimal::new(475, 3),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_68",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(68, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_70",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(70, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_72",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(72, 2),
            ),
        ];
        config.risk_budget_lookback_days = vec![170, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.risk_contribution_control_profiles = vec!["off".to_string()];
        config.score_candidate_pool_sizes = vec![500, 650];
        config.seed_trials = professional_high_sharpe_return_micro_bridge_seed_trials();
        config
    }

    pub fn professional_return_distribution_repair_default() -> Self {
        let mut config = Self::professional_high_sharpe_boundary_return_bridge_default();
        config.market_regime_policies = vec![
            "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
            "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v3".to_string(),
            "quality_event_window_return_sharpe_router_v4".to_string(),
        ];
        config.combo_versions = vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")];
        config.prediction_set_ids = Vec::new();
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::off(),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
        ];
        config.event_gate_profiles = vec![
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
                "event_window_10d_boost_p75_3pct",
                "phase7_event_window_earnings_10d_v1",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_window_20d_boost_p75_3pct",
                "phase7_event_window_earnings_v1",
                "boost_positive",
                Decimal::new(38, 2),
                Decimal::new(3, 2),
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "event_window_40d_exclude_negative",
                "phase7_event_window_earnings_40d_v1",
                "exclude_negative",
                Decimal::ZERO,
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
            EventGateProfile::event_combo(
                "residual_confirm_top40",
                "phase7_quality_residual_confirm_10pct_v1",
                "require_positive",
                Decimal::new(40, 2),
                Decimal::ZERO,
                ScoreDirection::Descending,
            ),
        ];
        config.risk_budget_lookback_days = vec![160, 180];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.risk_contribution_control_profiles = vec!["soft_single_name_20pct_v1".to_string()];
        config.candidate_risk_filter_profiles = vec!["off".to_string()];
        config.seed_trials = professional_return_distribution_repair_seed_trials();
        config
    }

    pub fn professional_state_alpha_router_default() -> Self {
        let mut config = Self::professional_current_anchor_weak_window_repair_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_regime_alpha_overlay_value_05pct_v1".to_string(),
            "quality_regime_alpha_overlay_blend_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string(),
            "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
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
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_state_alpha_router_seed_trials();
        config
    }

    pub fn professional_state_position_risk_router_default() -> Self {
        let mut config = Self::professional_state_alpha_router_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_bear_position_guard_v3".to_string(),
            "quality_bear_position_guard_v1".to_string(),
            "quality_bear_position_guard_v2".to_string(),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_position_pct = vec![Decimal::new(15, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2)];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_08_22_45_30_70",
            Decimal::new(8, 2),
            Decimal::new(22, 2),
            Decimal::new(45, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
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
        config.event_gate_profiles = vec![EventGateProfile::event_combo(
            "valuation_exclude_bottom40",
            "phase7_valuation_v1",
            "exclude_negative",
            Decimal::new(40, 2),
            Decimal::ZERO,
            ScoreDirection::Descending,
        )];
        config.seed_trials = professional_state_position_risk_router_seed_trials();
        config
    }

    pub fn professional_event_position_risk_router_default() -> Self {
        let mut config = Self::professional_state_position_risk_router_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_event_window_position_guard_v3".to_string(),
            "quality_event_window_position_guard_v1".to_string(),
            "quality_event_window_position_guard_v2".to_string(),
        ];
        config.seed_trials = professional_event_position_risk_router_seed_trials();
        config
    }

    pub fn professional_all_regime_event_sleeve_default() -> Self {
        let mut config = Self::professional_event_position_risk_router_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
            "quality_all_regime_event_window_sleeve_15pct_v1".to_string(),
        ];
        config.seed_trials = professional_all_regime_event_sleeve_seed_trials();
        config
    }

    pub fn professional_portfolio_sharpe_control_default() -> Self {
        let mut config = Self::professional_all_regime_event_sleeve_default();
        config.market_regime_policies = vec![
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            "quality_state_sharpe_bridge_router_v2".to_string(),
        ];
        config.portfolio_sharpe_controls = vec![
            PortfolioSharpeControlProfile::off(),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_060_000_55",
                Decimal::new(60, 2),
                Decimal::ZERO,
                120,
                Decimal::new(55, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe120_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                120,
                Decimal::new(60, 2),
            ),
            PortfolioSharpeControlProfile::reduce(
                "roll_sharpe180_050_neg10_60",
                Decimal::new(50, 2),
                Decimal::new(-10, 2),
                180,
                Decimal::new(60, 2),
            ),
        ];
        config.portfolio_drawdown_controls = vec![PortfolioDrawdownControlProfile::recover(
            "recover252_08_22_45_30_70",
            Decimal::new(8, 2),
            Decimal::new(22, 2),
            Decimal::new(45, 2),
            Some(252),
            Decimal::new(30, 2),
            Decimal::new(70, 2),
            Decimal::ONE,
        )];
        config.portfolio_volatility_controls = vec![
            PortfolioVolatilityControlProfile::target(
                "vol120_16_50_100",
                Decimal::new(16, 2),
                120,
                Decimal::new(50, 2),
                Decimal::ONE,
            ),
            PortfolioVolatilityControlProfile::target(
                "vol120_15_48_100",
                Decimal::new(15, 2),
                120,
                Decimal::new(48, 2),
                Decimal::ONE,
            ),
        ];
        config.risk_budget_lookback_days = vec![180];
        config.max_position_pct = vec![Decimal::new(15, 2), Decimal::new(14, 2)];
        config.max_pairwise_correlation = vec![Decimal::new(75, 2), Decimal::new(70, 2)];
        config.seed_trials = professional_portfolio_sharpe_control_seed_trials();
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
            self.portfolio_sharpe_controls.len(),
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
        let portfolio_sharpe_control =
            &config.portfolio_sharpe_controls[indices.portfolio_sharpe_control];
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
            "portfolio_sharpe_control": portfolio_sharpe_control.profile_name,
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
        if let (Some(start), Some(full), Some(min_exposure)) = (
            portfolio_sharpe_control.reduce_start,
            portfolio_sharpe_control.reduce_full,
            portfolio_sharpe_control.min_exposure,
        ) {
            parameters["portfolio_sharpe_reduce_start"] = serde_json::json!(decimal_string(start));
            parameters["portfolio_sharpe_reduce_full"] = serde_json::json!(decimal_string(full));
            parameters["portfolio_sharpe_min_exposure"] =
                serde_json::json!(decimal_string(min_exposure));
        }
        if let Some(lookback_days) = portfolio_sharpe_control.lookback_days {
            parameters["portfolio_sharpe_lookback_days"] = serde_json::json!(lookback_days);
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
    portfolio_sharpe_control: usize,
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
        let portfolio_sharpe_control =
            take_axis_index(&mut index, config.portfolio_sharpe_controls.len())?;
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
            portfolio_sharpe_control,
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
            portfolio_sharpe_controls: vec![PortfolioSharpeControlProfile::off()],
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
    fn professional_event_surprise_nonlinear_profile_keeps_search_narrow_and_stress_conditioned() {
        let config = LayeredSearchConfig::professional_event_surprise_nonlinear_default();

        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            combo_names,
            std::collections::BTreeSet::from([
                "phase7_financial_quality_v1",
                "phase7_quality_event_surprise_confirm_v1"
            ])
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(config.event_gate_profiles.len(), 5);
        assert!(config.event_gate_profiles.iter().all(|profile| {
            profile.combo_name.as_deref() != Some("phase7_event_window_earnings_v1")
                && (profile.profile_name == "off"
                    || profile.active_regimes.as_slice() == ["bear", "high_volatility"])
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_surprise_boost_pos_5pct_stress_only"
                && profile.combo_name.as_deref() == Some("phase7_event_surprise_v1")
                && profile.mode.as_deref() == Some("boost_positive")
                && profile.boost_weight == Some(Decimal::new(5, 2))
        }));
        assert!(config.event_gate_profiles.iter().any(|profile| {
            profile.profile_name == "event_surprise_exclude_negative_stress_only"
                && profile.combo_name.as_deref() == Some("phase7_event_surprise_v1")
                && profile.mode.as_deref() == Some("exclude_negative")
        }));
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_combo_name"] != "phase7_event_window_earnings_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["combo_name"] == "phase7_quality_event_surprise_confirm_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_require_positive_stress_only"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
    }

    #[test]
    fn professional_event_quality_segment_profile_keeps_ax_anchor_and_compares_event_quality() {
        let config = LayeredSearchConfig::professional_event_quality_segment_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_bear_window_guard_v2".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1".to_string(),
            ]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(config.seed_trials.len(), 4);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"]
                == "quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1"
        }));
    }

    #[test]
    fn professional_event_strength_segment_profile_uses_min_score_without_widening_grid() {
        let config = LayeredSearchConfig::professional_event_strength_segment_default();

        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config
                .event_gate_profiles
                .iter()
                .map(|profile| profile.profile_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "off",
                "event_window_require_strong_p75",
                "event_window_require_strong_p90",
                "event_surprise_require_strong_p75",
                "event_surprise_require_strong_p90",
                "event_confirm_require_light_p50",
            ]
        );
        assert_eq!(config.seed_trials.len(), 5);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 120
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_mode"] == "require_positive"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_require_strong_p75"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.38"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_require_strong_p90"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.66"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_require_strong_p75"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial["event_gate_min_score"] == "0.35"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_confirm_require_light_p50"
                && trial["event_gate_combo_name"] == "phase7_event_earnings_v1"
                && trial["event_gate_min_score"] == "0.39"
        }));
    }

    #[test]
    fn professional_event_strength_boost_profile_segments_without_hard_filtering() {
        let config = LayeredSearchConfig::professional_event_strength_boost_default();

        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120]);
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_bear_window_guard_v2".to_string()]
        );
        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.seed_trials.len(), 6);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_boost_weight"] != "0"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_boost_strong_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.38"
                && trial["event_gate_boost_weight"] == "0.03"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_window_boost_strong_p75_5pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_min_score"] == "0.38"
                && trial["event_gate_boost_weight"] == "0.05"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_surprise_boost_strong_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_surprise_v1"
                && trial["event_gate_min_score"] == "0.35"
                && trial["event_gate_boost_weight"] == "0.03"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "event_confirm_boost_light_p50_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_earnings_v1"
                && trial["event_gate_min_score"] == "0.39"
                && trial["event_gate_boost_weight"] == "0.03"
        }));
    }

    #[test]
    fn professional_legacy_alpha_revalidation_profile_keeps_current_anchor_and_retests_legacy() {
        let config = LayeredSearchConfig::professional_legacy_alpha_revalidation_default();

        assert_eq!(
            config.combo_versions,
            vec![
                ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
                ComboVersion::new("full_icir_16f_v3", "1.0.0"),
            ]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "off".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["legacy_revalidation_profile"] == "full_icir_v3_full_history_legacy_exact"
                && trial["combo_name"] == "full_icir_16f_v3"
                && trial["start_date"] == "20160201"
                && trial["end_date"] == "20260511"
                && trial["portfolio_method"] == "heuristic"
                && trial["kelly_fraction"] == "0.25"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["legacy_revalidation_profile"] == "full_icir_v3_full_history_risk_budget"
                && trial["combo_name"] == "full_icir_16f_v3"
                && trial["start_date"] == "20160201"
                && trial["end_date"] == "20260511"
                && trial["portfolio_method"] == "risk_budget"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["legacy_revalidation_profile"] == "full_icir_v3_recent_window_diagnostic"
                && trial["start_date"] == "20230512"
                && trial["end_date"] == "20260511"
        }));
    }

    #[test]
    fn professional_current_anchor_risk_shape_profile_keeps_ax_anchor_and_sweeps_risk_shape() {
        let config = LayeredSearchConfig::professional_current_anchor_risk_shape_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![120, 180]);
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["stop_loss_pct"].is_string()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_20_60_100"
                && trial["portfolio_volatility_target_pct"] == "0.20"
                && trial["portfolio_volatility_min_exposure"] == "0.60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_volatility_target_pct"] == "0.16"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_drawdown_control"] == "recover126_08_22_45_30_70"
                && trial["portfolio_drawdown_peak_lookback_days"] == 126
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_risk_control"] == "stop_loss_075_cooldown_45"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 45
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos12_corr65"
                && trial["max_position_pct"] == "0.12"
                && trial["max_pairwise_correlation"] == "0.65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "risk_budget_180"
                && trial["risk_budget_lookback_days"] == 180
        }));
    }

    #[test]
    fn professional_current_anchor_position_frontier_profile_interpolates_position_risk() {
        let config = LayeredSearchConfig::professional_current_anchor_position_frontier_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string()]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.max_position_pct,
            vec![
                Decimal::new(12, 2),
                Decimal::new(13, 2),
                Decimal::new(14, 2),
                Decimal::new(15, 2),
            ]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(65, 2),
                Decimal::new(70, 2),
                Decimal::new(75, 2),
            ]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["stop_loss_pct"] == "0.075"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "risk_budget_180"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
                && trial["portfolio_drawdown_control"] == "recover252_10_24_50_30_70"
                && trial["max_position_pct"] == "0.15"
                && trial["max_pairwise_correlation"] == "0.75"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos14_corr70"
                && trial["max_position_pct"] == "0.14"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "vol16_recover08_maxpos14_corr70"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
        }));
    }

    #[test]
    fn professional_current_anchor_weak_window_repair_profile_keeps_bg_anchor_and_uses_stress_only_repairs(
    ) {
        let config = LayeredSearchConfig::professional_current_anchor_weak_window_repair_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_15pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1".to_string(),
            ]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["market_regime"]
                    == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "stress_event_window_boost_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_min_score"] == "0.38"
                && trial["event_gate_boost_weight"] == "0.03"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "stress_valuation_boost_p40_5pct"
                && trial["event_gate_combo_name"] == "phase7_valuation_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_min_score"] == "0.40"
                && trial["event_gate_boost_weight"] == "0.05"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "stress_valuation_exclude_p40"
                && trial["event_gate_mode"] == "exclude_negative"
                && trial["event_gate_active_regimes"] == json!(["bear", "high_volatility"])
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_15pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
        }));
    }

    #[test]
    fn professional_current_anchor_sharpe_return_bridge_profile_starts_from_high_sharpe_boundary() {
        let config =
            LayeredSearchConfig::professional_current_anchor_sharpe_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.max_pairwise_correlation,
            vec![Decimal::new(65, 2), Decimal::new(70, 2)]
        );
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(14, 2), Decimal::new(15, 2)]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert_eq!(
            config.seed_trials[0]["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert_eq!(config.seed_trials[0]["max_position_pct"], "0.14");
        assert_eq!(config.seed_trials[0]["max_pairwise_correlation"], "0.65");
        assert_eq!(
            config.seed_trials[0]["portfolio_volatility_control"],
            "vol120_18_55_100"
        );
        assert_eq!(
            config.seed_trials[0]["portfolio_drawdown_control"],
            "recover252_10_24_50_30_70"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos14_corr65_vol20"
                && trial["portfolio_volatility_control"] == "vol120_20_60_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos14_corr65_vol22"
                && trial["portfolio_volatility_control"] == "vol120_22_65_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos15_corr70_vol20"
                && trial["max_position_pct"] == "0.15"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
                && trial["position_shape_profile"] == "maxpos14_corr65_vol20_event125"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_return_recovery_profile_bridges_boundary_without_wide_grid() {
        let config = LayeredSearchConfig::professional_high_sharpe_return_recovery_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
            ]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(65, 2),
                Decimal::new(675, 3),
                Decimal::new(70, 2)
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert_eq!(
            config.seed_trials[0]["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert_eq!(
            config.seed_trials[1]["portfolio_drawdown_control"],
            "recover252_08_22_45_30_70"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_19_58_100"
                && trial["position_shape_profile"] == "maxpos14_corr65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1"
                && trial["event_sleeve_profile"] == "event_window_125pct"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial["event_gate_min_score"] == "0.35"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos145_corr65"
                && trial["max_position_pct"] == "0.145"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_risk_memory_bridge_profile_searches_middle_lookbacks() {
        let config = LayeredSearchConfig::professional_risk_memory_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.risk_budget_lookback_days,
            vec![120, 140, 150, 160, 170, 180]
        );
        assert_eq!(
            config.portfolio_volatility_controls[0].profile_name,
            "vol120_18_55_100"
        );
        assert_eq!(config.seed_trials.len(), 16);
        assert_eq!(
            config.seed_trials[0]["portfolio_drawdown_control"],
            "recover252_08_22_45_30_70"
        );
        assert_eq!(
            config.seed_trials[1]["position_shape_profile"],
            "maxpos14_corr65"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "risk_budget_140"
                && trial["risk_budget_lookback_days"] == 140
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos145_corr65_rb150"
                && trial["risk_budget_lookback_days"] == 150
                && trial["max_position_pct"] == "0.145"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_budget_shape_profile"] == "valuation45_rb150"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_state_return_sharpe_router_profile_bridges_return_and_boundary() {
        let config = LayeredSearchConfig::professional_state_return_sharpe_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_event_window_return_sharpe_router_v1".to_string(),
                "quality_event_window_return_sharpe_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v1"
                && trial["position_shape_profile"] == "maxpos14_corr65"
                && trial["max_position_pct"] == "0.14"
                && trial["max_pairwise_correlation"] == "0.65"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v2"
                && trial["risk_budget_lookback_days"] == 160
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["risk_budget_lookback_days"] == 180
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_state_return_sharpe_frontier_profile_sweeps_bp_neighborhood() {
        let config = LayeredSearchConfig::professional_state_return_sharpe_frontier_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![150, 160, 170, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v1".to_string(),
                "quality_event_window_return_sharpe_router_v2".to_string(),
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["risk_budget_shape_profile"] == "bp_rb160_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_position_sharpe_return_bridge_profile_blends_bq_and_boundary_shapes() {
        let config = LayeredSearchConfig::professional_position_sharpe_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![150, 160, 170, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(65, 2),
                Decimal::new(675, 3),
                Decimal::new(70, 2),
            ]
        );
        assert_eq!(
            config.max_position_pct,
            vec![
                Decimal::new(14, 2),
                Decimal::new(145, 3),
                Decimal::new(15, 2),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["position_shape_profile"] == "maxpos14_corr65_router_v4_rb160"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["position_shape_profile"] == "maxpos145_corr65_router_v3_rb160"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["risk_budget_shape_profile"] == "bp_rb170_router_v4"
                && trial["max_pairwise_correlation"] == "0.65"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_moderate_position_sharpe_return_bridge_profile_keeps_return_room() {
        let config =
            LayeredSearchConfig::professional_moderate_position_sharpe_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(70, 2),
                Decimal::new(725, 3),
                Decimal::new(75, 2),
            ]
        );
        assert_eq!(
            config.max_position_pct,
            vec![
                Decimal::new(16, 2),
                Decimal::new(17, 2),
                Decimal::new(18, 2),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["position_shape_profile"] == "maxpos16_corr70_router_v4_vol120_16_50_100"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["position_shape_profile"] == "maxpos17_corr725_router_v3_vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_correlation_frontier_sharpe_return_profile_sweeps_narrow_corr_band() {
        let config = LayeredSearchConfig::professional_correlation_frontier_sharpe_return_default();

        assert_eq!(
            config.market_regime_policies,
            vec!["quality_event_window_return_sharpe_router_v4".to_string()]
        );
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(705, 3),
                Decimal::new(71, 2),
                Decimal::new(715, 3),
                Decimal::new(72, 2),
            ]
        );
        assert_eq!(
            config.max_position_pct,
            vec![Decimal::new(16, 2), Decimal::new(165, 3)]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos16_corr705_router_v4_vol120_16_50_100"
                && trial["max_pairwise_correlation"] == "0.705"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos165_corr72_router_v4_vol120_17_52_100"
                && trial["max_position_pct"] == "0.165"
                && trial["max_pairwise_correlation"] == "0.72"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_correlation_threshold_sharpe_return_profile_sweeps_jump_boundary() {
        let config =
            LayeredSearchConfig::professional_correlation_threshold_sharpe_return_default();

        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(706, 3),
                Decimal::new(707, 3),
                Decimal::new(708, 3),
                Decimal::new(709, 3),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos16_corr706_router_v4_vol120_16_50_100"
                && trial["max_pairwise_correlation"] == "0.706"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["position_shape_profile"] == "maxpos165_corr709_router_v4_vol120_17_52_100"
                && trial["max_pairwise_correlation"] == "0.709"
        }));
        assert!(config.seed_trials.iter().all(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_soft_risk_frontier_sharpe_return_profile_adds_soft_risk_controls() {
        let config = LayeredSearchConfig::professional_soft_risk_frontier_sharpe_return_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec![
                "off".to_string(),
                "soft_single_name_20pct_v1".to_string(),
                "soft_single_name_15pct_v1".to_string(),
            ]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "low_volatility_v1".to_string(),
                "low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["candidate_risk_filter"] == "off"
                && trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["risk_contribution_control"] == "soft_single_name_15pct_v1"
                && trial["candidate_risk_filter"] == "low_volatility_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_regime_alpha_selector_profile_changes_alpha_mix_not_dates() {
        let config = LayeredSearchConfig::professional_regime_alpha_selector_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_event_window_return_sharpe_router_v4".to_string(),
                "quality_state_alpha_selector_v1".to_string(),
                "quality_state_alpha_selector_v2".to_string(),
                "quality_state_alpha_selector_v3".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_event_window_return_sharpe_router_v4"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["candidate_risk_filter"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_selector_v2"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_selector_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_regime_alpha_overlay_frontier_profile_adds_selector_overlay_and_smoothing() {
        let config = LayeredSearchConfig::professional_regime_alpha_overlay_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_selector_v3".to_string(),
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_state_alpha_overlay_selector_v2".to_string(),
                "quality_state_alpha_overlay_selector_v3".to_string(),
            ]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![160]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.rebalance_hysteresis_pct,
            vec![Decimal::ZERO, Decimal::new(5, 3)]
        );
        assert_eq!(
            config.partial_rebalance_ratio,
            vec![Decimal::ONE, Decimal::new(85, 2)]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["rebalance_hysteresis_pct"] == "0"
                && trial["partial_rebalance_ratio"] == "1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["rebalance_hysteresis_pct"] == "0.005"
                && trial["partial_rebalance_ratio"] == "0.85"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_event_alpha_profile_routes_mixed_state_without_date_fitting() {
        let config = LayeredSearchConfig::professional_mixed_state_event_alpha_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v2".to_string(),
                "quality_mixed_event_state_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_overlay_selector_v2".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![160]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.rebalance_hysteresis_pct, vec![Decimal::ZERO]);
        assert_eq!(config.partial_rebalance_ratio, vec![Decimal::ONE]);
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_event_state_overlay_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["rebalance_hysteresis_pct"] == "0"
                && trial["partial_rebalance_ratio"] == "1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_event_state_overlay_selector_v2"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_risk_memory_profile_tightens_mixed_state_only() {
        let config = LayeredSearchConfig::professional_mixed_state_risk_memory_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v2".to_string(),
                "quality_mixed_state_risk_memory_router_v3".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 160
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_risk_memory_frontier_profile_bridges_return_and_sharpe() {
        let config = LayeredSearchConfig::professional_mixed_state_risk_memory_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v4".to_string(),
                "quality_mixed_state_risk_memory_router_v5".to_string(),
                "quality_mixed_state_risk_memory_router_v6".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v5"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_risk_memory_fine_frontier_profile_scans_near_boundary() {
        let config =
            LayeredSearchConfig::professional_mixed_state_risk_memory_fine_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v4".to_string(),
                "quality_mixed_state_risk_memory_router_v7".to_string(),
                "quality_mixed_state_risk_memory_router_v8".to_string(),
                "quality_mixed_state_risk_memory_router_v9".to_string(),
                "quality_mixed_state_risk_memory_router_v10".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v7"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v9"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v4"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_exposure_frontier_profile_isolates_exposure_axis() {
        let config = LayeredSearchConfig::professional_mixed_state_exposure_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_event_state_selector_v1".to_string(),
                "quality_mixed_state_risk_memory_router_v4".to_string(),
                "quality_mixed_state_risk_memory_router_v11".to_string(),
                "quality_mixed_state_risk_memory_router_v12".to_string(),
                "quality_mixed_state_risk_memory_router_v13".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v11"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_event_state_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_mixed_state_orthogonal_alpha_profile_changes_mixed_alpha_source() {
        let config = LayeredSearchConfig::professional_mixed_state_orthogonal_alpha_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_alpha_selector_v1".to_string(),
                "quality_mixed_orthogonal_alpha_selector_v2".to_string(),
                "quality_mixed_orthogonal_alpha_selector_v3".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v1".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v2".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        let combo_names = config
            .combo_versions
            .iter()
            .map(|combo| combo.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(combo_names.contains("phase7_financial_quality_v1"));
        assert!(combo_names.contains("phase7_quality_residual_confirm_10pct_v1"));
        assert!(combo_names.contains("phase7_quality_value_recovery_event_confirm_v1"));
        assert!(combo_names.contains("phase7_blend_defensive_rel_v1"));
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_candidate_filter_alpha_bridge_profile_crosses_filter_and_alpha_axes() {
        let config = LayeredSearchConfig::professional_candidate_filter_alpha_bridge_default();

        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "low_volatility_v1".to_string(),
                "low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec![
                "soft_single_name_20pct_v1".to_string(),
                "soft_single_name_15pct_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["candidate_risk_filter"] == "low_volatility_low_correlation_v1"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v1"
                && trial["candidate_risk_filter"] == "low_volatility_v1"
                && trial["risk_contribution_control"] == "soft_single_name_15pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["candidate_risk_filter"] == "off"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_soft_candidate_filter_alpha_bridge_profile_relaxes_hard_filter() {
        let config = LayeredSearchConfig::professional_soft_candidate_filter_alpha_bridge_default();

        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "soft_low_volatility_v1".to_string(),
                "soft_low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["candidate_risk_filter"] == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_alpha_selector_v3"
                && trial["candidate_risk_filter"] == "soft_low_volatility_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["candidate_risk_filter"] == "off"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_sharpe_bridge_frontier_profile_centers_bx_anchor_without_filtering() {
        let config = LayeredSearchConfig::professional_sharpe_bridge_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_state_sharpe_bridge_router_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_state_sharpe_bridge_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["candidate_risk_filter"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_annual_sharpe_floor_bridge_profile_bridges_risk_memory_without_date_fitting() {
        let config = LayeredSearchConfig::professional_annual_sharpe_floor_bridge_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom35"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial["risk_contribution_control"] == "off"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_risk_memory_relaxed_frontier_profile_tests_internal_mixed_risk_boundary() {
        let config = LayeredSearchConfig::professional_risk_memory_relaxed_frontier_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_mixed_state_risk_memory_router_v15".to_string(),
                "quality_mixed_state_risk_memory_router_v16".to_string(),
                "quality_mixed_state_risk_memory_router_v17".to_string(),
                "quality_mixed_state_risk_memory_router_v18".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v18"
                && trial["event_gate_profile"] == "valuation_exclude_bottom35"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_sharpe_floor_auto_discovery_profile_bridges_sharpe_boundary_and_annual_floor() {
        let config = LayeredSearchConfig::professional_sharpe_floor_auto_discovery_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v16".to_string(),
                "quality_mixed_state_risk_memory_router_v18".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_prediction_confirmed_sharpe_bridge_profile_adds_weak_ml_confirmation_without_date_fitting(
    ) {
        let config = LayeredSearchConfig::professional_prediction_confirmed_sharpe_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_state_alpha_overlay_selector_v1".to_string(),
            ]
        );
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.prediction_set_ids,
            Vec::<String>::new(),
            "CN uses prediction as a weak blend/filter over factor_combo seeds, not standalone model_prediction"
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["prediction_set_id"] == "pred-p7-wf-wide-qgvrel-v1-201602-202605"
                && trial["prediction_blend_weight"] == "0.02"
                && trial.get("prediction_min_percentile").is_none()
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial["prediction_blend_weight"] == "0.05"
                && trial["prediction_min_percentile"] == "0.20"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial["prediction_blend_weight"] == "0.08"
                && trial["prediction_min_percentile"] == "0.30"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["prediction_set_id"],
                "pred-p7-wf-wide-qgvrel-v1-201602-202605"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_boundary_return_bridge_profile_targets_real_boundary_without_prediction(
    ) {
        let config = LayeredSearchConfig::professional_high_sharpe_boundary_return_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(
            config.portfolio_volatility_controls,
            vec![
                PortfolioVolatilityControlProfile::target(
                    "vol120_14_46_100",
                    Decimal::new(14, 2),
                    120,
                    Decimal::new(46, 2),
                    Decimal::ONE,
                ),
                PortfolioVolatilityControlProfile::target(
                    "vol120_145_47_100",
                    Decimal::new(145, 3),
                    120,
                    Decimal::new(47, 2),
                    Decimal::ONE,
                ),
                PortfolioVolatilityControlProfile::target(
                    "vol120_15_48_100",
                    Decimal::new(15, 2),
                    120,
                    Decimal::new(48, 2),
                    Decimal::ONE,
                ),
                PortfolioVolatilityControlProfile::target(
                    "vol120_155_49_100",
                    Decimal::new(155, 3),
                    120,
                    Decimal::new(49, 2),
                    Decimal::ONE,
                ),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial["risk_budget_lookback_days"] == 180
                && trial["max_position_pct"] == "0.15"
                && trial["max_pairwise_correlation"] == "0.75"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert!(seed.get("prediction_set_id").is_none());
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(
                seed["risk_contribution_control"],
                "soft_single_name_20pct_v1"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_boundary_event_lift_profile_adds_small_sleeves_without_date_fitting(
    ) {
        let config = LayeredSearchConfig::professional_high_sharpe_boundary_event_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            let regime = trial["market_regime"].as_str().unwrap_or_default();
            trial["combo_name"] == "phase7_financial_quality_v1"
                && matches!(
                    regime,
                    "quality_mixed_state_risk_memory_router_v14"
                        | "quality_all_regime_event_window_sleeve_05pct_v1"
                        | "quality_all_regime_event_window_sleeve_10pct_v1"
                        | "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
                )
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_high_sharpe_micro_frontier_profile_searches_near_cl_cm_v14_without_overfit_stacking(
    ) {
        let config = LayeredSearchConfig::professional_high_sharpe_micro_frontier_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_mixed_state_risk_memory_router_v14".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.score_candidate_pool_sizes, vec![400, 500, 650, 800]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec![
                "off".to_string(),
                "soft_low_volatility_v1".to_string(),
                "soft_low_volatility_low_correlation_v1".to_string(),
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 16);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial["candidate_risk_filter"] == "soft_low_volatility_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
                && trial["candidate_risk_filter"] == "soft_low_volatility_low_correlation_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_14_46_100"
                && trial["risk_contribution_control"] == "off"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert!(seed.get("prediction_set_id").is_none());
            let regime = seed["market_regime"].as_str().unwrap_or_default();
            assert!(
                regime == "quality_mixed_orthogonal_risk_memory_router_v3"
                    || regime == "quality_nonlinear_alpha_risk_memory_router_v3"
                    || regime == "quality_mixed_state_risk_memory_router_v14"
            );
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_sharpe_return_lift_profile_keeps_sharpe_anchor_and_only_lifts_return_gently(
    ) {
        let config = LayeredSearchConfig::professional_v14_sharpe_return_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![400, 500, 650]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial["risk_contribution_control"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom43"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_148_475_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_045_neg10_63"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_shape_lift_profile_varies_position_shape_without_alpha_or_date_overfit() {
        let config = LayeredSearchConfig::professional_v14_shape_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![18, 20, 22]);
        assert_eq!(config.rebalance_days, vec![50, 55, 60]);
        assert_eq!(
            config.skip_top_pct,
            vec![Decimal::new(8, 2), Decimal::new(10, 2)]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 18
                && trial["rebalance"] == "55"
                && trial["skip_top_pct"] == "0.08"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 22
                && trial["rebalance"] == "50"
                && trial["portfolio_drawdown_control"] == "recover252_08_23_45_30_70"
                && trial["stop_loss_pct"] == "0.07"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20
                && trial["rebalance"] == "55"
                && trial["rebalance_smoothing_profile"] == "hysteresis_1pct_partial_75"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_ultra_micro_lift_profile_only_perturbs_the_high_sharpe_anchor() {
        let config = LayeredSearchConfig::professional_v14_ultra_micro_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![19, 20, 21]);
        assert_eq!(config.rebalance_days, vec![58, 60, 62]);
        assert_eq!(
            config.skip_top_pct,
            vec![Decimal::new(9, 2), Decimal::new(10, 2), Decimal::new(11, 2)]
        );
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![500]);
        assert_eq!(config.seed_trials.len(), 15);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20
                && trial["rebalance"] == "60"
                && trial["skip_top_pct"] == "0.10"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 19
                && trial["rebalance"] == "60"
                && trial["skip_top_pct"] == "0.10"
                && trial["portfolio_volatility_control"] == "vol120_142_465_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20 && trial["rebalance"] == "58" && trial["skip_top_pct"] == "0.10"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["top_n"] == 20 && trial["rebalance"] == "60" && trial["skip_top_pct"] == "0.09"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(
                seed["portfolio_sharpe_control"],
                "roll_sharpe180_050_neg10_65"
            );
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert_eq!(seed["score_candidate_pool_size"], 500);
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_return_alpha_sharpe_bridge_profile_combines_return_alpha_with_sharpe_controls()
    {
        let config = LayeredSearchConfig::professional_return_alpha_sharpe_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_state_alpha_overlay_selector_v1".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![500, 650]);
        assert_eq!(config.seed_trials.len(), 16);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_alpha_overlay_selector_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["risk_budget_lookback_days"] == 170
                && trial["risk_contribution_control"] == "off"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_regime_frontier_bridge_profile_connects_high_sharpe_and_return_frontiers() {
        let config = LayeredSearchConfig::professional_regime_frontier_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_frontier_regime_bridge_router_v1".to_string(),
                "quality_frontier_regime_bridge_router_v2".to_string(),
                "quality_frontier_regime_bridge_router_v3".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 170, 180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string(), "soft_single_name_20pct_v1".to_string()]
        );
        assert_eq!(config.score_candidate_pool_sizes, vec![500, 650]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v3"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["score_candidate_pool_size"] == 500
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_regime_frontier_decomposition_profile_splits_risk_axes_without_overfit() {
        let config = LayeredSearchConfig::professional_regime_frontier_decomposition_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_frontier_regime_bridge_router_v4".to_string(),
                "quality_frontier_regime_bridge_router_v5".to_string(),
                "quality_frontier_regime_bridge_router_v6".to_string(),
                "quality_frontier_regime_bridge_router_v7".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 170, 180]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v4"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v5"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_141_462_100"
                && trial["risk_contribution_control"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v6"
                && trial["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial["score_candidate_pool_size"] == 500
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v7"
                && trial["portfolio_volatility_control"] == "vol120_145_47_100"
                && trial["score_candidate_pool_size"] == 650
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_high_sharpe_return_micro_bridge_profile_relaxes_only_exposure_and_vol() {
        let config = LayeredSearchConfig::professional_high_sharpe_return_micro_bridge_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_frontier_regime_bridge_router_v6".to_string(),
                "quality_frontier_regime_bridge_router_v7".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![170, 180]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v6"
                && trial["portfolio_volatility_control"] == "vol120_147_475_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_70"
                && trial["portfolio_sharpe_min_exposure"] == "0.70"
                && trial["risk_budget_lookback_days"] == 170
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_frontier_regime_bridge_router_v7"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_72"
                && trial["score_candidate_pool_size"] == 650
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_annual_floor_micro_lift_profile_bridges_only_the_high_sharpe_edge() {
        let config = LayeredSearchConfig::professional_v14_annual_floor_micro_lift_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_state_risk_memory_router_v14".to_string(),
                "quality_mixed_state_risk_memory_router_v15".to_string(),
                "quality_mixed_state_risk_memory_router_v16".to_string(),
                "quality_mixed_state_risk_memory_router_v17".to_string(),
                "quality_mixed_state_risk_memory_router_v18".to_string(),
            ]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.skip_top_pct, vec![Decimal::new(10, 2)]);
        assert_eq!(config.risk_budget_lookback_days, vec![165, 170]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v15"
                && trial["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_0475_neg10_66"
                && trial["risk_budget_lookback_days"] == 165
                && trial["score_candidate_pool_size"] == 650
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v16"
                && trial["portfolio_volatility_control"] == "vol120_143_467_100"
                && trial["portfolio_sharpe_min_exposure"] == "0.68"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_near_miss_annual_bridge_profile_only_lifts_the_v14_near_miss() {
        let config = LayeredSearchConfig::professional_v14_near_miss_annual_bridge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.skip_top_pct, vec![Decimal::new(10, 2)]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 165, 170]);
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 16);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_state_risk_memory_router_v14"
                && trial["portfolio_volatility_control"] == "vol120_1405_461_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial["risk_budget_lookback_days"] == 165
                && trial["score_candidate_pool_size"] == 500
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_1415_464_100"
                && trial["portfolio_sharpe_min_exposure"] == "0.67"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["max_position_pct"] == "0.145"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["top_n"], 20);
            assert_eq!(seed["rebalance"], "60");
            assert_eq!(seed["skip_top_pct"], "0.10");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_v14_corr70_annual_edge_profile_extends_only_the_corr70_near_miss() {
        let config = LayeredSearchConfig::professional_v14_corr70_annual_edge_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(
            config.market_regime_policies,
            vec!["quality_mixed_state_risk_memory_router_v14".to_string()]
        );
        assert_eq!(config.prediction_set_ids, Vec::<String>::new());
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.top_n, vec![20]);
        assert_eq!(config.rebalance_days, vec![60]);
        assert_eq!(config.skip_top_pct, vec![Decimal::new(10, 2)]);
        assert_eq!(config.risk_budget_lookback_days, vec![165, 168]);
        assert_eq!(
            config.max_pairwise_correlation,
            vec![
                Decimal::new(68, 2),
                Decimal::new(70, 2),
                Decimal::new(72, 2)
            ]
        );
        assert_eq!(
            config.risk_contribution_control_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(
            config.candidate_risk_filter_profiles,
            vec!["off".to_string()]
        );
        assert_eq!(config.seed_trials.len(), 20);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_1415_464_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_66"
                && trial["risk_budget_lookback_days"] == 165
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["portfolio_sharpe_min_exposure"] == "0.68"
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["risk_budget_lookback_days"] == 168
                && trial["max_pairwise_correlation"] == "0.70"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_volatility_control"] == "vol120_142_465_100"
                && trial["max_pairwise_correlation"] == "0.68"
        }));
        for seed in &config.seed_trials {
            assert_eq!(seed["signal_source"], "factor_combo");
            assert_eq!(seed["combo_name"], "phase7_financial_quality_v1");
            assert_eq!(
                seed["market_regime"],
                "quality_mixed_state_risk_memory_router_v14"
            );
            assert_eq!(seed["event_gate_profile"], "valuation_exclude_bottom45");
            assert_eq!(seed["portfolio_method"], "risk_budget");
            assert_eq!(seed["top_n"], 20);
            assert_eq!(seed["rebalance"], "60");
            assert_eq!(seed["skip_top_pct"], "0.10");
            assert_eq!(seed["max_position_pct"], "0.15");
            assert_eq!(seed["candidate_risk_filter"], "off");
            assert_eq!(seed["risk_contribution_control"], "off");
            assert!(seed.get("prediction_set_id").is_none());
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
            assert!(!serialized.contains("event_window"));
            assert!(!serialized.contains("prediction"));
        }
    }

    #[test]
    fn professional_return_distribution_repair_profile_combines_event_decay_and_orthogonal_alpha_without_date_fitting(
    ) {
        let config = LayeredSearchConfig::professional_return_distribution_repair_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![160, 180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v3".to_string(),
                "quality_event_window_return_sharpe_router_v4".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config.seed_trials.iter().all(|trial| {
            trial.get("prediction_set_id").is_none()
                && trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_contribution_control"] == "soft_single_name_20pct_v1"
                && trial["candidate_risk_filter"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_mixed_orthogonal_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom45"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v3"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["risk_budget_lookback_days"] == 180
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v3"
                && trial["event_gate_profile"] == "event_window_10d_boost_p75_3pct"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_10d_v1"
                && trial["event_gate_mode"] == "boost_positive"
                && trial["event_gate_min_score"] == "0.38"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_return_sharpe_router_v4"
                && trial["event_gate_profile"] == "event_window_40d_exclude_negative"
                && trial["event_gate_combo_name"] == "phase7_event_window_earnings_40d_v1"
                && trial["event_gate_mode"] == "exclude_negative"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["event_gate_profile"] == "residual_confirm_top40"
                && trial["event_gate_combo_name"] == "phase7_quality_residual_confirm_10pct_v1"
                && trial["event_gate_min_score"] == "0.40"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
                && trial["portfolio_sharpe_lookback_days"] == 180
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_state_alpha_router_profile_keeps_current_anchor_and_routes_second_alpha() {
        let config = LayeredSearchConfig::professional_state_alpha_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_regime_alpha_overlay_value_05pct_v1".to_string(),
                "quality_regime_alpha_overlay_blend_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1".to_string(),
                "quality_regime_alpha_portfolio_sleeve_value_10pct_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_overlay_value_05pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_overlay_blend_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_lowrisk_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_regime_alpha_portfolio_sleeve_value_10pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn professional_state_position_risk_router_profile_keeps_anchor_and_sweeps_position_regime() {
        let config = LayeredSearchConfig::professional_state_position_risk_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(config.max_position_pct, vec![Decimal::new(15, 2)]);
        assert_eq!(config.max_pairwise_correlation, vec![Decimal::new(75, 2)]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_bear_position_guard_v3".to_string(),
                "quality_bear_position_guard_v1".to_string(),
                "quality_bear_position_guard_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert_eq!(
            config.seed_trials[0]["market_regime"],
            "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1"
        );
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_bear_position_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_event_position_risk_router_profile_keeps_event_sleeve_and_position_guard() {
        let config = LayeredSearchConfig::professional_event_position_risk_router_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_event_window_position_guard_v3".to_string(),
                "quality_event_window_position_guard_v1".to_string(),
                "quality_event_window_position_guard_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_position_guard_v3"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_event_window_position_guard_v2"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_all_regime_event_sleeve_profile_mixes_event_flow_across_states() {
        let config = LayeredSearchConfig::professional_all_regime_event_sleeve_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(config.risk_budget_lookback_days, vec![180]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_05pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_10pct_v1".to_string(),
                "quality_all_regime_event_window_sleeve_15pct_v1".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config.seed_trials.iter().all(|trial| {
            trial["combo_name"] == "phase7_financial_quality_v1"
                && trial["portfolio_method"] == "risk_budget"
                && trial["risk_budget_lookback_days"] == 180
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_drawdown_control"] == "recover252_08_22_45_30_70"
                && trial["stop_loss_pct"] == "0.075"
                && trial["reentry_cooldown_days"] == 30
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_05pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_all_regime_event_window_sleeve_15pct_v1"
                && trial["portfolio_volatility_control"] == "vol120_18_55_100"
        }));
    }

    #[test]
    fn professional_portfolio_sharpe_control_profile_adds_self_risk_axis_without_date_fitting() {
        let config = LayeredSearchConfig::professional_portfolio_sharpe_control_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 8);
        assert!(config
            .portfolio_sharpe_controls
            .iter()
            .any(
                |profile| profile.profile_name == "roll_sharpe120_060_000_55"
                    && profile.reduce_start == Some(Decimal::new(60, 2))
                    && profile.reduce_full == Some(Decimal::ZERO)
                    && profile.min_exposure == Some(Decimal::new(55, 2))
            ));
        let sharpe_seed = config
            .seed_trials
            .iter()
            .find(|trial| trial["portfolio_sharpe_control"] == "roll_sharpe120_060_000_55")
            .expect("rolling Sharpe seed");
        assert_eq!(sharpe_seed["portfolio_sharpe_reduce_start"], "0.60");
        assert_eq!(sharpe_seed["portfolio_sharpe_reduce_full"], "0.00");
        assert_eq!(sharpe_seed["portfolio_sharpe_lookback_days"], 120);
        assert_eq!(sharpe_seed["portfolio_sharpe_min_exposure"], "0.55");
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["portfolio_sharpe_control"] == "bridge_roll_sharpe180_050_neg10_60"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_nonlinear_alpha_auto_discovery_profile_combines_new_alpha_and_rolling_sharpe() {
        let config = LayeredSearchConfig::professional_nonlinear_alpha_auto_discovery_default();

        assert_eq!(
            config.combo_versions,
            vec![ComboVersion::new("phase7_financial_quality_v1", "1.0.0")]
        );
        assert_eq!(config.score_directions, vec![ScoreDirection::Ascending]);
        assert_eq!(config.portfolio_methods, vec!["risk_budget".to_string()]);
        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_nonlinear_alpha_router_v1".to_string(),
                "quality_nonlinear_alpha_router_v2".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v1".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 12);
        assert!(config
            .portfolio_sharpe_controls
            .iter()
            .any(
                |profile| profile.profile_name == "roll_sharpe180_050_neg10_65"
                    && profile.reduce_start == Some(Decimal::new(50, 2))
                    && profile.reduce_full == Some(Decimal::new(-10, 2))
                    && profile.min_exposure == Some(Decimal::new(65, 2))
            ));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_router_v1"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_sharpe_control"] == "off"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v1"
                && trial["portfolio_volatility_control"] == "vol120_15_48_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_65"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
    }

    #[test]
    fn professional_nonlinear_sharpe_return_bridge_profile_targets_cl_boundary() {
        let config = LayeredSearchConfig::professional_nonlinear_sharpe_return_bridge_default();

        assert_eq!(
            config.market_regime_policies,
            vec![
                "quality_nonlinear_alpha_risk_memory_router_v2".to_string(),
                "quality_nonlinear_alpha_risk_memory_router_v3".to_string(),
                "quality_mixed_orthogonal_risk_memory_router_v3".to_string(),
                "quality_state_sharpe_bridge_router_v2".to_string(),
                "quality_nonlinear_alpha_router_v2".to_string(),
            ]
        );
        assert_eq!(config.seed_trials.len(), 10);
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_nonlinear_alpha_risk_memory_router_v2"
                && trial["event_gate_profile"] == "valuation_exclude_bottom40"
                && trial["portfolio_volatility_control"] == "vol120_16_50_100"
                && trial["portfolio_sharpe_control"] == "roll_sharpe180_050_neg10_60"
        }));
        assert!(config.seed_trials.iter().any(|trial| {
            trial["market_regime"] == "quality_state_sharpe_bridge_router_v2"
                && trial["portfolio_volatility_control"] == "vol120_17_52_100"
                && trial["max_position_pct"] == "0.16"
        }));
        for seed in &config.seed_trials {
            let serialized = seed.to_string();
            assert!(!serialized.contains("2017"));
            assert!(!serialized.contains("2020"));
        }
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
