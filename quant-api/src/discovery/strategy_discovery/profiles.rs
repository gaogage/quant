//! BC4 策略发现 / 搜索空间配置类型层：资源计划、alpha 源角色、控制 profile。
//!
//! 由 phase7.rs 拆出（DDD 重构 R8 批次1），纯类型定义，零行为变更。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Phase7AlphaSourceRole {
    BaseTrainable,
    OptionalOverlayOnly,
    EventGateOnly,
    Excluded,
}

impl Phase7AlphaSourceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BaseTrainable => "base_trainable",
            Self::OptionalOverlayOnly => "optional_overlay_only",
            Self::EventGateOnly => "event_gate_only",
            Self::Excluded => "excluded",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Phase7AlphaSourceAdmission {
    pub combo_name: &'static str,
    pub role: Phase7AlphaSourceRole,
    pub reason: &'static str,
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
pub struct CostCapacityStressProfile {
    pub profile_name: String,
    pub cost_multiplier: Option<Decimal>,
    pub slippage_bps: Option<Decimal>,
    pub impact_cost_coefficient: Option<Decimal>,
    pub max_participation_rate: Option<Decimal>,
}

impl CostCapacityStressProfile {
    pub fn off() -> Self {
        Self {
            profile_name: "off".to_string(),
            cost_multiplier: None,
            slippage_bps: None,
            impact_cost_coefficient: None,
            max_participation_rate: None,
        }
    }

    pub fn cost_up(
        profile_name: impl Into<String>,
        cost_multiplier: Decimal,
        slippage_bps: Decimal,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            cost_multiplier: Some(cost_multiplier),
            slippage_bps: Some(slippage_bps),
            impact_cost_coefficient: None,
            max_participation_rate: None,
        }
    }

    pub fn impact_limited(
        profile_name: impl Into<String>,
        impact_cost_coefficient: Decimal,
        max_participation_rate: Decimal,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            cost_multiplier: None,
            slippage_bps: None,
            impact_cost_coefficient: Some(impact_cost_coefficient),
            max_participation_rate: Some(max_participation_rate),
        }
    }

    pub fn participation_limited(
        profile_name: impl Into<String>,
        max_participation_rate: Decimal,
    ) -> Self {
        Self {
            profile_name: profile_name.into(),
            cost_multiplier: None,
            slippage_bps: None,
            impact_cost_coefficient: None,
            max_participation_rate: Some(max_participation_rate),
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

pub(super) fn detect_memory_gb() -> Option<usize> {
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
