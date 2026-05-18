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
    fn risk_seed(
        combo_name: &str,
        market_regime: &str,
        top_n: usize,
        rebalance: usize,
        max_position_pct: &str,
        max_gross_exposure: &str,
        risk_budget_lookback_days: usize,
        capacity_penalty_strength: &str,
        drawdown_profile: (&str, &str, &str, &str, usize, &str, &str, &str),
        volatility_profile: (&str, &str, usize, &str, &str),
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
            "industry_max_weight_pct": "0.20",
            "score_candidate_pool_size": 500,
            "universe_profile": "all",
            "portfolio_drawdown_control": drawdown_profile.0,
            "portfolio_volatility_control": volatility_profile.0,
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
        seed["portfolio_volatility_target_pct"] = json!(volatility_profile.1);
        seed["portfolio_volatility_lookback_days"] = json!(volatility_profile.2);
        seed["portfolio_volatility_min_exposure"] = json!(volatility_profile.3);
        seed["portfolio_volatility_max_exposure"] = json!(volatility_profile.4);
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
    let vol120_24 = ("vol120_24_70_100", "0.24", 120, "0.70", "1");
    let vol60_25 = ("vol60_25_75_100", "0.25", 60, "0.75", "1");
    let vol120_30 = ("vol120_30_85_100", "0.30", 120, "0.85", "1");
    let vol60_30 = ("vol60_30_85_100", "0.30", 60, "0.85", "1");

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
            recover_08_25,
            vol120_24,
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
            recover_10_30,
            vol120_24,
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
            recover_10_27,
            vol60_25,
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
            recover_10_27,
            vol120_30,
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
            recover_10_27,
            vol60_30,
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
            recover_08_25,
            vol120_24,
        ),
    ]
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
    pub score_candidate_pool_sizes: Vec<usize>,
    pub universe_profiles: Vec<String>,
    pub portfolio_drawdown_controls: Vec<PortfolioDrawdownControlProfile>,
    pub portfolio_volatility_controls: Vec<PortfolioVolatilityControlProfile>,
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
            ComboVersion::new("phase7_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_relative_strength_v1", "1.0.0"),
            ComboVersion::new("phase7_growth_recovery_v1", "1.0.0"),
            ComboVersion::new("phase7_valuation_v1", "1.0.0"),
            ComboVersion::new("phase7_moneyflow_v1", "1.0.0"),
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
        config.seed_trials = professional_breakthrough_seed_trials();
        config
    }

    pub fn professional_risk_breakthrough_default() -> Self {
        let mut config = Self::professional_breakthrough_default();
        config.market_regime_policies = vec!["quality_crash_guard_v1".to_string()];
        config.combo_versions = vec![
            ComboVersion::new("phase7_financial_quality_v1", "1.0.0"),
            ComboVersion::new("phase7_quality_moneyflow_pos_5pct_v1", "1.0.0"),
            ComboVersion::new("phase7_blend_quality_growth_v1", "1.0.0"),
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
        ];
        config.portfolio_volatility_controls = vec![
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
                "vol60_30_85_100",
                Decimal::new(30, 2),
                60,
                Decimal::new(85, 2),
                Decimal::ONE,
            ),
        ];
        config.seed_trials = professional_risk_breakthrough_seed_trials();
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
            self.score_candidate_pool_sizes.len(),
            self.universe_profiles.len(),
            self.portfolio_drawdown_controls.len(),
            self.portfolio_volatility_controls.len(),
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
        let score_candidate_pool_size =
            config.score_candidate_pool_sizes[indices.score_candidate_pool_size];
        let universe_profile = &config.universe_profiles[indices.universe_profile];
        let portfolio_drawdown_control =
            &config.portfolio_drawdown_controls[indices.portfolio_drawdown_control];
        let portfolio_volatility_control =
            &config.portfolio_volatility_controls[indices.portfolio_volatility_control];

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
            "score_candidate_pool_size": score_candidate_pool_size,
            "universe_profile": universe_profile,
            "portfolio_drawdown_control": portfolio_drawdown_control.profile_name,
            "portfolio_volatility_control": portfolio_volatility_control.profile_name,
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
    score_candidate_pool_size: usize,
    universe_profile: usize,
    portfolio_drawdown_control: usize,
    portfolio_volatility_control: usize,
}

impl LayeredTrialIndices {
    fn from_flat_index(config: &LayeredSearchConfig, mut index: usize) -> Option<Self> {
        let market_regime_policy =
            take_axis_index(&mut index, config.market_regime_policies.len())?;
        let capacity_penalty_strength =
            take_axis_index(&mut index, config.capacity_penalty_strength.len())?;
        let industry_max_weight_pct =
            take_axis_index(&mut index, config.industry_max_weight_pct.len())?;
        let score_candidate_pool_size =
            take_axis_index(&mut index, config.score_candidate_pool_sizes.len())?;
        let universe_profile = take_axis_index(&mut index, config.universe_profiles.len())?;
        let portfolio_drawdown_control =
            take_axis_index(&mut index, config.portfolio_drawdown_controls.len())?;
        let portfolio_volatility_control =
            take_axis_index(&mut index, config.portfolio_volatility_controls.len())?;
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
            score_candidate_pool_size,
            universe_profile,
            portfolio_drawdown_control,
            portfolio_volatility_control,
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
    }

    #[test]
    fn alpha_blend_profiles_are_valid_weight_search_candidates() {
        let profiles = phase7_alpha_blend_profiles();
        let names = profiles
            .iter()
            .map(|profile| profile.combo_name.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(profiles.len(), 6);
        assert!(names.contains("phase7_value_quality_growth_rel_v1"));
        assert!(names.contains("phase7_blend_value_tilt_v1"));
        assert!(names.contains("phase7_blend_quality_growth_v1"));
        assert!(names.contains("phase7_blend_defensive_rel_v1"));
        assert!(names.contains("phase7_blend_recovery_tilt_v1"));
        assert!(names.contains("phase7_quality_moneyflow_pos_5pct_v1"));
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

        assert_eq!(plan.requested_trials, 3_197_988_864);
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
