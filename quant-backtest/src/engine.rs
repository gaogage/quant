//! 回测引擎 — 多标的、A 股规则、三种模式
//!
//! fast:   无明细记录，仅净值曲线 + 指标
//! standard: 全量记录（交易 + 持仓 + 净值）
//! audit:  standard + 可复现 hash 校验

use chrono::NaiveDate;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

use super::portfolio::{DailyPosition, FeeConfig, Portfolio, Trade};
use quant_common::trading_rules::round_down_to_lot;

// ─── Attribution types ────────────────────────────────────────────

/// 组合暴露记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioExposure {
    pub trade_date: NaiveDate,
    pub exposure_type: String,
    pub exposure_name: String,
    pub net_exposure: Decimal,
    pub gross_exposure: Decimal,
}

/// 组合归因记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioAttribution {
    pub trade_date: NaiveDate,
    pub attribution_type: String,
    pub attribution_name: String,
    pub contribution: Decimal,
}

/// 目标组合记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioTarget {
    pub trade_date: NaiveDate,
    pub symbol: String,
    pub target_weight: Decimal,
    pub target_quantity: Option<Decimal>,
    pub reason: Option<String>,
}

/// 约束违反记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintViolation {
    pub trade_date: NaiveDate,
    pub constraint_name: String,
    pub limit_value: Decimal,
    pub actual_value: Decimal,
    pub severity: String,
}

// ─── Market data snapshot for one day ─────────────────────────────

#[derive(Debug, Clone)]
pub struct MarketDay {
    pub date: NaiveDate,
    pub open: HashMap<String, Decimal>,
    pub close: HashMap<String, Decimal>,
    pub pre_close: HashMap<String, Decimal>,
    pub amount: HashMap<String, Decimal>,
    pub suspended: HashSet<String>,
    pub up_limit: HashMap<String, Decimal>,
    pub down_limit: HashMap<String, Decimal>,
    pub benchmark_close: Decimal,
    pub benchmark_pre_close: Decimal,
}

// ─── Strategy signal ──────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StrategySignal {
    pub date: NaiveDate,
    pub target_weights: HashMap<String, Decimal>,
}

// ─── Config ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub initial_capital: Decimal,
    pub benchmark: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub fee_config: FeeConfig,
    pub mode: BacktestMode,
    pub max_position_pct: Decimal,
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub research_dataset_id: Option<String>,
    pub feature_set_version_id: Option<String>,
    pub prediction_set_id: Option<String>,
    pub portfolio_policy_id: Option<String>,
    /// Immutable strategy/request parameter snapshot used to reproduce and audit a task.
    #[serde(default)]
    pub parameters: Value,
    pub symbols: Vec<String>,
    pub rebalance_frequency: String,
    pub execution_timing: ExecutionTiming,
    pub execution_price: ExecutionPrice,
    #[serde(default)]
    pub execution_schedule_profile: ExecutionScheduleProfile,
    #[serde(default)]
    pub execution_carry_policy: ExecutionCarryPolicy,
    #[serde(default)]
    pub execution_daily_target_move_limit_pct: Option<Decimal>,
    #[serde(default)]
    pub execution_max_carry_days: Option<usize>,
    pub max_participation_rate: Option<Decimal>,
    /// Database persistence depth. Summary-only keeps result/equity curve for discovery and
    /// robustness, while skipping heavy detail tables.
    #[serde(default)]
    pub persistence_mode: BacktestPersistenceMode,
    /// Risk control configuration
    #[serde(default)]
    pub risk_control: RiskControlConfig,
}

/// Per-position risk management
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RiskControlConfig {
    /// Hard stop-loss: sell if loss exceeds this % from buy price (e.g. 0.10 = 10%)
    #[serde(default)]
    pub stop_loss_pct: Option<Decimal>,
    /// Take-profit: sell if gain exceeds this % from buy price
    #[serde(default)]
    pub take_profit_pct: Option<Decimal>,
    /// Trailing stop: sell if price drops this % from peak since purchase
    #[serde(default)]
    pub trailing_stop_pct: Option<Decimal>,
    /// Time stop: sell if held for more than N trading days without reaching profit target
    #[serde(default)]
    pub time_stop_days: Option<u32>,
    /// Re-entry cooldown after a position-level risk exit. A value of 1 blocks the exit day and
    /// the next calendar day, then allows buying again.
    #[serde(default)]
    pub reentry_cooldown_days: Option<u32>,
    /// Portfolio-level drawdown where target exposure starts to scale down.
    #[serde(default)]
    pub portfolio_drawdown_reduce_start_pct: Option<Decimal>,
    /// Portfolio-level drawdown where target exposure reaches the configured floor.
    #[serde(default)]
    pub portfolio_drawdown_reduce_full_pct: Option<Decimal>,
    /// Minimum gross exposure multiplier after portfolio drawdown control is fully active.
    #[serde(default)]
    pub portfolio_drawdown_min_exposure: Option<Decimal>,
    /// Optional number of recent equity points used for the portfolio drawdown high-water mark.
    #[serde(default)]
    pub portfolio_drawdown_peak_lookback_days: Option<usize>,
    /// Recovery ratio where drawdown control starts restoring target exposure after a trough.
    #[serde(default)]
    pub portfolio_drawdown_recovery_start_pct: Option<Decimal>,
    /// Recovery ratio where drawdown control applies the full configured recovery boost.
    #[serde(default)]
    pub portfolio_drawdown_recovery_full_pct: Option<Decimal>,
    /// Maximum fraction of the gap between reduced exposure and 100% exposure to restore.
    #[serde(default)]
    pub portfolio_drawdown_recovery_boost: Option<Decimal>,
    /// Annualized portfolio volatility target where target exposure starts scaling down.
    #[serde(default)]
    pub portfolio_volatility_target_pct: Option<Decimal>,
    /// Number of recent daily returns used for realized portfolio volatility.
    #[serde(default)]
    pub portfolio_volatility_lookback_days: Option<usize>,
    /// Minimum gross exposure multiplier after volatility targeting is fully active.
    #[serde(default)]
    pub portfolio_volatility_min_exposure: Option<Decimal>,
    /// Maximum gross exposure multiplier after volatility targeting. Defaults to 100%, no leverage.
    #[serde(default)]
    pub portfolio_volatility_max_exposure: Option<Decimal>,
    /// Rolling annualized Sharpe where target exposure starts scaling down.
    #[serde(default)]
    pub portfolio_sharpe_reduce_start: Option<Decimal>,
    /// Rolling annualized Sharpe where target exposure reaches the configured floor.
    #[serde(default)]
    pub portfolio_sharpe_reduce_full: Option<Decimal>,
    /// Number of recent daily returns used for rolling Sharpe control.
    #[serde(default)]
    pub portfolio_sharpe_lookback_days: Option<usize>,
    /// Minimum gross exposure multiplier after rolling Sharpe control is fully active.
    #[serde(default)]
    pub portfolio_sharpe_min_exposure: Option<Decimal>,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_capital: Decimal::new(1_000_000, 0),
            benchmark: "000300.SH".into(),
            start_date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            fee_config: FeeConfig::default(),
            mode: BacktestMode::Standard,
            max_position_pct: Decimal::new(100, 2), // default 100%
            strategy_version_id: "debug-strategy".into(),
            data_version_id: "debug-data".into(),
            research_dataset_id: None,
            feature_set_version_id: None,
            prediction_set_id: None,
            portfolio_policy_id: None,
            parameters: serde_json::json!({}),
            symbols: Vec::new(),
            rebalance_frequency: "daily".into(),
            execution_timing: ExecutionTiming::NextOpen,
            execution_price: ExecutionPrice::Open,
            execution_schedule_profile: ExecutionScheduleProfile::Immediate,
            execution_carry_policy: ExecutionCarryPolicy::Expire,
            execution_daily_target_move_limit_pct: None,
            execution_max_carry_days: None,
            max_participation_rate: None,
            persistence_mode: BacktestPersistenceMode::Full,
            risk_control: RiskControlConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BacktestMode {
    Fast,
    Standard,
    Audit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum BacktestPersistenceMode {
    #[default]
    Full,
    SummaryOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTiming {
    NextOpen,
    SameCloseDebug,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPrice {
    Open,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ExecutionScheduleProfile {
    #[default]
    Immediate,
    Twap3dV1,
    Twap5dV1,
    Twap10dV1,
    Twap15dV1,
    Twap20dV1,
}

impl ExecutionScheduleProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "" | "off" | "immediate" => Ok(Self::Immediate),
            "twap_3d_v1" | "twap3d_v1" | "twap_3d" => Ok(Self::Twap3dV1),
            "twap_5d_v1" | "twap5d_v1" | "twap_5d" => Ok(Self::Twap5dV1),
            "twap_10d_v1" | "twap10d_v1" | "twap_10d" => Ok(Self::Twap10dV1),
            "twap_15d_v1" | "twap15d_v1" | "twap_15d" => Ok(Self::Twap15dV1),
            "twap_20d_v1" | "twap20d_v1" | "twap_20d" => Ok(Self::Twap20dV1),
            other => Err(format!("unsupported execution_schedule_profile: {}", other)),
        }
    }

    fn execution_days(self) -> usize {
        match self {
            Self::Immediate => 1,
            Self::Twap3dV1 => 3,
            Self::Twap5dV1 => 5,
            Self::Twap10dV1 => 10,
            Self::Twap15dV1 => 15,
            Self::Twap20dV1 => 20,
        }
    }

    fn is_immediate(self) -> bool {
        self == Self::Immediate
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Twap3dV1 => "twap_3d_v1",
            Self::Twap5dV1 => "twap_5d_v1",
            Self::Twap10dV1 => "twap_10d_v1",
            Self::Twap15dV1 => "twap_15d_v1",
            Self::Twap20dV1 => "twap_20d_v1",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ExecutionCarryPolicy {
    #[default]
    Expire,
    RollForwardV1,
}

impl ExecutionCarryPolicy {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "" | "expire" | "expire_v1" | "drop" | "drop_on_ttl" => Ok(Self::Expire),
            "roll_forward_v1" | "roll-forward-v1" | "rolling_carry_v1" | "continue_v1" => {
                Ok(Self::RollForwardV1)
            }
            other => Err(format!("unsupported execution_carry_policy: {}", other)),
        }
    }
}

// ─── Result ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestOutput {
    pub config: BacktestConfig,
    pub metrics: super::metrics::BacktestMetrics,
    pub equity_curve: Vec<(NaiveDate, Decimal)>,
    pub benchmark_curve: Vec<(NaiveDate, Decimal)>,
    pub trades: Vec<Trade>,
    pub daily_positions: Vec<DailyPosition>,
    pub targets: Vec<PortfolioTarget>,
    pub exposures: Vec<PortfolioExposure>,
    pub attributions: Vec<PortfolioAttribution>,
    pub violations: Vec<ConstraintViolation>,
    pub reproducibility_hash: Option<String>,
}

// ─── Engine ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PendingExecutionSchedule {
    source_signal_date: NaiveDate,
    final_target_weights: HashMap<String, Decimal>,
    remaining_steps: usize,
    elapsed_steps: usize,
}

pub struct BacktestEngine {
    config: BacktestConfig,
    pub portfolio: Portfolio,
    equity_curve: Vec<(NaiveDate, Decimal)>,
    benchmark_curve: Vec<(NaiveDate, Decimal)>,
    daily_positions: Vec<DailyPosition>,
    targets: Vec<PortfolioTarget>,
    exposures: Vec<PortfolioExposure>,
    attributions: Vec<PortfolioAttribution>,
    violations: Vec<ConstraintViolation>,
    /// Track per-symbol buy date and peak price for risk control
    position_buy_date: HashMap<String, NaiveDate>,
    position_peak_price: HashMap<String, Decimal>,
    position_risk_cooldown_remaining: HashMap<String, u32>,
    pending_execution_schedule: Option<PendingExecutionSchedule>,
    execution_schedule_expired_count: usize,
    execution_schedule_roll_forward_count: usize,
    max_execution_target_gap_pct: Decimal,
    latest_target_gross_exposure_pct: Decimal,
    warm_prev_close: HashMap<String, (NaiveDate, Decimal)>,
    symbol_suspended_until: HashMap<String, NaiveDate>,
    adj_blacklisted: HashSet<String>,
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig) -> Self {
        let portfolio = Portfolio::new(
            config.initial_capital,
            config.fee_config.clone(),
            config.max_position_pct,
        );
        Self {
            config,
            portfolio,
            equity_curve: Vec::new(),
            benchmark_curve: Vec::new(),
            daily_positions: Vec::new(),
            targets: Vec::new(),
            exposures: Vec::new(),
            attributions: Vec::new(),
            violations: Vec::new(),
            position_buy_date: HashMap::new(),
            position_peak_price: HashMap::new(),
            position_risk_cooldown_remaining: HashMap::new(),
            pending_execution_schedule: None,
            execution_schedule_expired_count: 0,
            execution_schedule_roll_forward_count: 0,
            max_execution_target_gap_pct: Decimal::zero(),
            latest_target_gross_exposure_pct: Decimal::zero(),
            warm_prev_close: HashMap::new(),
            symbol_suspended_until: HashMap::new(),
            adj_blacklisted: HashSet::new(),
        }
    }

    pub fn process_day(&mut self, market: &MarketDay, signal: Option<&StrategySignal>) {
        info!(
            "process_day: date={} close_symbols={} has_signal={} hold_count={}",
            market.date,
            market.close.len(),
            signal.is_some(),
            self.portfolio.holdings.len()
        );

        // 记录停牌状态
        for sym in &market.suspended {
            self.symbol_suspended_until.insert(sym.clone(), market.date);
        }

        // 数据完整性: 检测非停牌期的价格跳变 (>50% = adj_factor数据异常)
        for (sym, close) in &market.close {
            if let Some((prev_date, prev_close)) = self.warm_prev_close.get(sym) {
                if !prev_close.is_zero() {
                    let was_suspended = self
                        .symbol_suspended_until
                        .get(sym)
                        .map(|until| *until >= *prev_date)
                        .unwrap_or(false);
                    if !was_suspended && !self.adj_blacklisted.contains(sym) {
                        let ret = (*close - *prev_close) / *prev_close;
                        if ret.abs() > Decimal::from_f64_retain(0.5).unwrap_or(Decimal::ONE) {
                            warn!(
                                "adj_factor异常: {} {}→{} 复权价跳变{:.0}%({}→{}) 加入黑名单排除",
                                sym,
                                prev_date,
                                market.date,
                                ret * Decimal::from(100u32),
                                prev_close,
                                close
                            );
                            self.adj_blacklisted.insert(sym.clone());
                        }
                    }
                }
            }
            self.warm_prev_close
                .insert(sym.clone(), (market.date, *close));
        }

        // 过滤adj_factor黑名单股票(仅影响mark_to_market,持仓会在下个调仓日自然卖出)
        let clean_close: HashMap<String, Decimal> = market
            .close
            .iter()
            .filter(|(s, _)| !self.adj_blacklisted.contains(*s))
            .map(|(s, v)| (s.clone(), *v))
            .collect();

        let previous_value = self.equity_curve.last().map(|(_, value)| *value);
        self.portfolio.mark_to_market(&clean_close);
        self.update_position_risk_state(market);
        self.execute_position_risk_controls(market);

        if self.config.execution_schedule_profile.is_immediate() {
            if let Some(sig) = signal {
                self.execute_rebalance(sig, market);
            }
        } else {
            if let Some(sig) = signal {
                self.start_pending_execution_schedule(sig);
            }
            if let Some(scheduled_signal) = self.pending_execution_schedule_signal(market.date) {
                self.execute_rebalance(&scheduled_signal, market);
                self.advance_pending_execution_schedule();
            }
        }

        self.equity_curve
            .push((market.date, self.portfolio.total_value()));

        let bm_val = if self.benchmark_curve.is_empty() {
            self.config.initial_capital
        } else {
            let prev = self.benchmark_curve.last().unwrap().1;
            if market.benchmark_pre_close.is_zero() {
                prev
            } else {
                prev * market.benchmark_close / market.benchmark_pre_close
            }
        };
        self.benchmark_curve.push((market.date, bm_val));

        if self.config.mode != BacktestMode::Fast {
            self.daily_positions
                .extend(self.portfolio.snapshot(market.date));

            // Portfolio exposure — per-symbol weights
            let tv = self.portfolio.total_value();
            if !tv.is_zero() {
                for (sym, h) in &self.portfolio.holdings {
                    let w = h.market_value() / tv;
                    self.exposures.push(PortfolioExposure {
                        trade_date: market.date,
                        exposure_type: "weight".into(),
                        exposure_name: sym.clone(),
                        net_exposure: w,
                        gross_exposure: w,
                    });
                }
            }

            // Portfolio attribution — daily return contribution
            if let Some(prev_tv) = previous_value {
                if !prev_tv.is_zero() {
                    self.attributions.push(PortfolioAttribution {
                        trade_date: market.date,
                        attribution_type: "total".into(),
                        attribution_name: "portfolio_return".into(),
                        contribution: (tv - prev_tv) / prev_tv,
                    });
                }
            }
        }

        self.advance_position_risk_cooldowns();
        self.portfolio.end_of_day();
    }

    fn execution_price_for(&self, market: &MarketDay, symbol: &str) -> Decimal {
        match self.config.execution_price {
            ExecutionPrice::Open => market.open.get(symbol).copied().unwrap_or_default(),
            ExecutionPrice::Close => market.close.get(symbol).copied().unwrap_or_default(),
        }
    }

    fn start_pending_execution_schedule(&mut self, signal: &StrategySignal) {
        let total_steps = self
            .config
            .execution_schedule_profile
            .execution_days()
            .max(1);
        self.pending_execution_schedule = Some(PendingExecutionSchedule {
            source_signal_date: signal.date,
            final_target_weights: signal.target_weights.clone(),
            remaining_steps: total_steps,
            elapsed_steps: 0,
        });
    }

    fn current_portfolio_weight(&self, symbol: &str, total_value: Decimal) -> Decimal {
        if total_value.is_zero() {
            return Decimal::zero();
        }
        self.portfolio
            .holdings
            .get(symbol)
            .map(|holding| holding.market_value() / total_value)
            .unwrap_or_default()
    }

    fn pending_execution_schedule_signal(&self, trade_date: NaiveDate) -> Option<StrategySignal> {
        let pending = self.pending_execution_schedule.as_ref()?;
        let total_value = self.portfolio.total_value();
        let step_fraction = Decimal::ONE / Decimal::from(pending.remaining_steps.max(1) as u64);
        let mut symbols = pending
            .final_target_weights
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        symbols.extend(self.portfolio.holdings.keys().cloned());

        let mut target_weights = HashMap::new();
        for symbol in symbols {
            let current_weight = self.current_portfolio_weight(&symbol, total_value);
            let final_weight = pending
                .final_target_weights
                .get(&symbol)
                .copied()
                .unwrap_or_default();
            let scheduled_weight = current_weight + (final_weight - current_weight) * step_fraction;
            let scheduled_weight = self.cap_daily_target_move(current_weight, scheduled_weight);
            if !scheduled_weight.is_zero() || self.portfolio.holdings.contains_key(&symbol) {
                target_weights.insert(
                    symbol,
                    scheduled_weight.clamp(Decimal::zero(), Decimal::ONE),
                );
            }
        }

        Some(StrategySignal {
            date: pending.source_signal_date.min(trade_date),
            target_weights,
        })
    }

    fn cap_daily_target_move(&self, current_weight: Decimal, scheduled_weight: Decimal) -> Decimal {
        let Some(limit) = self
            .config
            .execution_daily_target_move_limit_pct
            .filter(|limit| *limit > Decimal::zero())
        else {
            return scheduled_weight;
        };
        let lower = if current_weight > limit {
            current_weight - limit
        } else {
            Decimal::zero()
        };
        let upper = (current_weight + limit).min(Decimal::ONE);
        scheduled_weight.clamp(lower, upper)
    }

    fn execution_gap_tolerance() -> Decimal {
        Decimal::new(5, 4)
    }

    fn pending_execution_gap(&self) -> Decimal {
        let Some(pending) = self.pending_execution_schedule.as_ref() else {
            return Decimal::zero();
        };
        let total_value = self.portfolio.total_value();
        if total_value.is_zero() {
            return Decimal::zero();
        }
        let mut symbols = pending
            .final_target_weights
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        symbols.extend(self.portfolio.holdings.keys().cloned());

        symbols
            .into_iter()
            .map(|symbol| {
                let current_weight = self.current_portfolio_weight(&symbol, total_value);
                let final_weight = pending
                    .final_target_weights
                    .get(&symbol)
                    .copied()
                    .unwrap_or_default();
                if final_weight >= current_weight {
                    final_weight - current_weight
                } else {
                    current_weight - final_weight
                }
            })
            .sum::<Decimal>()
    }

    fn advance_pending_execution_schedule(&mut self) {
        let gap = self.pending_execution_gap();
        let should_clear = gap <= Self::execution_gap_tolerance();
        let total_steps = self
            .config
            .execution_schedule_profile
            .execution_days()
            .max(1);
        let mut clear_pending = should_clear;
        let mut abandon_expired = false;
        let mut roll_forward = false;
        {
            let Some(pending) = self.pending_execution_schedule.as_mut() else {
                return;
            };
            pending.elapsed_steps += 1;
            let carry_expired = self
                .config
                .execution_max_carry_days
                .is_some_and(|max_carry_days| pending.elapsed_steps >= max_carry_days.max(1));
            if carry_expired && !should_clear {
                match self.config.execution_carry_policy {
                    ExecutionCarryPolicy::Expire => {
                        abandon_expired = true;
                        clear_pending = true;
                    }
                    ExecutionCarryPolicy::RollForwardV1 => {
                        roll_forward = true;
                        pending.elapsed_steps = 0;
                        pending.remaining_steps = total_steps;
                        clear_pending = false;
                    }
                }
            } else if carry_expired {
                clear_pending = true;
            } else if !clear_pending && pending.remaining_steps > 1 {
                pending.remaining_steps -= 1;
            }
        }
        if abandon_expired && gap > Self::execution_gap_tolerance() {
            self.execution_schedule_expired_count += 1;
            self.max_execution_target_gap_pct = self.max_execution_target_gap_pct.max(gap);
        }
        if roll_forward {
            self.execution_schedule_roll_forward_count += 1;
        }
        if clear_pending {
            self.pending_execution_schedule = None;
        }
    }

    fn cap_order_amount(
        &mut self,
        market: &MarketDay,
        symbol: &str,
        desired_amount: Decimal,
    ) -> Decimal {
        let Some(max_rate) = self.config.max_participation_rate else {
            return desired_amount;
        };
        let Some(liquidity_amount) = market.amount.get(symbol).copied() else {
            self.violations.push(ConstraintViolation {
                trade_date: market.date,
                constraint_name: "liquidity_amount_missing".into(),
                limit_value: Decimal::zero(),
                actual_value: desired_amount,
                severity: "warning".into(),
            });
            return desired_amount;
        };
        if liquidity_amount.is_zero() || max_rate.is_zero() {
            self.violations.push(ConstraintViolation {
                trade_date: market.date,
                constraint_name: "participation_rate".into(),
                limit_value: Decimal::zero(),
                actual_value: desired_amount,
                severity: "hard".into(),
            });
            return Decimal::zero();
        }

        // bar 表 amount 为 Tushare 千元单位，与 participation_rate_for 同口径换算到元，
        // 否则参与率 cap 被压 1000 倍，买单不足一手被整手规则全部跳过。
        let cap = liquidity_amount * Decimal::from(1000u32) * max_rate;
        if desired_amount > cap {
            self.violations.push(ConstraintViolation {
                trade_date: market.date,
                constraint_name: "participation_rate".into(),
                limit_value: max_rate,
                actual_value: desired_amount / (liquidity_amount * Decimal::from(1000u32)),
                severity: "hard".into(),
            });
            cap
        } else {
            desired_amount
        }
    }

    fn participation_rate_for(
        &self,
        market: &MarketDay,
        symbol: &str,
        order_amount: Decimal,
    ) -> Decimal {
        market
            .amount
            .get(symbol)
            .copied()
            .filter(|amount| !amount.is_zero())
            // bar 表 amount 为 Tushare 千元单位,order_amount 为元——此前直接相除
            // 参与率被放大 1000 倍(小盘股虚算至 300%+,冲击成本 9%/边,2026-08-20
            // 定位:含成本回测全灭的根因)。统一换算到元再求比值。
            .map(|amount| order_amount / (amount * Decimal::from(1000u32)))
            .unwrap_or_default()
    }

    fn annotate_last_trade(
        &mut self,
        target_weight: Decimal,
        executed_weight: Decimal,
        reason: &str,
    ) {
        if let Some(trade) = self.portfolio.trades.last_mut() {
            trade.signal_type = Some("rebalance".into());
            trade.event_reason = Some(reason.into());
            trade.target_weight = Some(target_weight);
            trade.executed_weight = Some(executed_weight);
        }
    }

    fn rebalance_target_reason(&self, base_reason: &str) -> String {
        if self.config.execution_schedule_profile.is_immediate() {
            base_reason.into()
        } else {
            format!(
                "{}_execution_schedule_{}",
                base_reason,
                self.config.execution_schedule_profile.as_str()
            )
        }
    }

    fn rebalance_trade_reason(&self, side: &str) -> String {
        if self.config.execution_schedule_profile.is_immediate() {
            format!("rebalance_{}", side)
        } else {
            format!(
                "execution_schedule_{}_{}",
                self.config.execution_schedule_profile.as_str(),
                side
            )
        }
    }

    fn annotate_last_trade_reason(&mut self, reason: &str) {
        if let Some(trade) = self.portfolio.trades.last_mut() {
            trade.signal_type = Some("risk_control".into());
            trade.event_reason = Some(reason.into());
        }
    }

    fn update_position_metadata_after_buy(
        &mut self,
        symbol: &str,
        trade_date: NaiveDate,
        price: Decimal,
    ) {
        self.position_buy_date
            .entry(symbol.to_string())
            .or_insert(trade_date);
        self.position_peak_price
            .entry(symbol.to_string())
            .and_modify(|peak| *peak = (*peak).max(price))
            .or_insert(price);
    }

    fn cleanup_closed_position_metadata(&mut self, symbol: &str) {
        if !self.portfolio.holdings.contains_key(symbol) {
            self.position_buy_date.remove(symbol);
            self.position_peak_price.remove(symbol);
        }
    }

    fn start_position_risk_cooldown(&mut self, symbol: &str) {
        let Some(days) = self.config.risk_control.reentry_cooldown_days else {
            return;
        };
        if days == 0 {
            return;
        }
        self.position_risk_cooldown_remaining
            .insert(symbol.to_string(), days.saturating_add(1));
    }

    fn is_position_risk_cooling_down(&self, symbol: &str) -> bool {
        self.position_risk_cooldown_remaining
            .get(symbol)
            .is_some_and(|remaining_days| *remaining_days > 0)
    }

    fn advance_position_risk_cooldowns(&mut self) {
        for remaining_days in self.position_risk_cooldown_remaining.values_mut() {
            *remaining_days = remaining_days.saturating_sub(1);
        }
        self.position_risk_cooldown_remaining
            .retain(|_, remaining_days| *remaining_days > 0);
    }

    fn update_position_risk_state(&mut self, market: &MarketDay) {
        let held_symbols = self
            .portfolio
            .holdings
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        self.position_buy_date
            .retain(|symbol, _| held_symbols.contains(symbol));
        self.position_peak_price
            .retain(|symbol, _| held_symbols.contains(symbol));

        for symbol in held_symbols {
            if let Some(price) = market.close.get(&symbol).copied() {
                self.position_peak_price
                    .entry(symbol)
                    .and_modify(|peak| *peak = (*peak).max(price))
                    .or_insert(price);
            }
        }
    }

    fn position_risk_exit_reason(
        &self,
        symbol: &str,
        price: Decimal,
        trade_date: NaiveDate,
    ) -> Option<&'static str> {
        let holding = self.portfolio.holdings.get(symbol)?;
        let risk_control = &self.config.risk_control;

        if let Some(stop_loss) = risk_control.stop_loss_pct {
            if stop_loss > Decimal::zero() && price <= holding.avg_cost * (Decimal::ONE - stop_loss)
            {
                return Some("risk_stop_loss");
            }
        }
        if let Some(take_profit) = risk_control.take_profit_pct {
            if take_profit > Decimal::zero()
                && price >= holding.avg_cost * (Decimal::ONE + take_profit)
            {
                return Some("risk_take_profit");
            }
        }
        if let Some(trailing_stop) = risk_control.trailing_stop_pct {
            if trailing_stop > Decimal::zero() {
                if let Some(peak_price) = self.position_peak_price.get(symbol).copied() {
                    if peak_price > Decimal::zero()
                        && price <= peak_price * (Decimal::ONE - trailing_stop)
                    {
                        return Some("risk_trailing_stop");
                    }
                }
            }
        }
        if let Some(time_stop_days) = risk_control.time_stop_days {
            if let Some(buy_date) = self.position_buy_date.get(symbol).copied() {
                if (trade_date - buy_date).num_days() >= i64::from(time_stop_days) {
                    return Some("risk_time_stop");
                }
            }
        }

        None
    }

    fn execute_position_risk_controls(&mut self, market: &MarketDay) {
        let mut exits = self
            .portfolio
            .holdings
            .iter()
            .filter_map(|(symbol, holding)| {
                if holding.sellable_quantity.is_zero() || market.suspended.contains(symbol) {
                    return None;
                }
                let price = self.execution_price_for(market, symbol);
                if price.is_zero() {
                    return None;
                }
                if let Some(down_limit) = market.down_limit.get(symbol) {
                    if price <= *down_limit {
                        return None;
                    }
                }
                self.position_risk_exit_reason(symbol, price, market.date)
                    .map(|reason| (symbol.clone(), holding.sellable_quantity, price, reason))
            })
            .collect::<Vec<_>>();
        exits.sort_by(|left, right| left.0.cmp(&right.0));

        for (symbol, quantity, price, reason) in exits {
            let desired_amount = quantity * price;
            let capped_amount = self.cap_order_amount(market, &symbol, desired_amount);
            if capped_amount.is_zero() {
                continue;
            }
            let capped_quantity = (capped_amount / price).floor().min(quantity);
            if capped_quantity.is_zero() {
                continue;
            }
            let participation_rate = self.participation_rate_for(market, &symbol, capped_amount);
            if self
                .portfolio
                .sell_with_cost(
                    market.date,
                    &symbol,
                    capped_quantity,
                    price,
                    participation_rate,
                    market.down_limit.get(&symbol).copied(),
                )
                .is_some()
            {
                self.annotate_last_trade_reason(reason);
                self.cleanup_closed_position_metadata(&symbol);
                self.start_position_risk_cooldown(&symbol);
            }
        }
    }

    fn portfolio_equity_window_values(&self, current_value: Decimal) -> Vec<Decimal> {
        let lookback_days = self
            .config
            .risk_control
            .portfolio_drawdown_peak_lookback_days
            .filter(|days| *days > 0);
        let start_index = lookback_days
            .map(|days| self.equity_curve.len().saturating_sub(days))
            .unwrap_or_default();
        let mut values: Vec<Decimal> = self
            .equity_curve
            .iter()
            .skip(start_index)
            .map(|(_, value)| *value)
            .collect();
        values.push(current_value);
        values
    }

    fn portfolio_equity_peak(&self, current_value: Decimal) -> Decimal {
        self.portfolio_equity_window_values(current_value)
            .into_iter()
            .fold(current_value, |peak, value| peak.max(value))
    }

    fn portfolio_equity_trough_since_peak(
        &self,
        current_value: Decimal,
        peak_value: Decimal,
    ) -> Decimal {
        let values = self.portfolio_equity_window_values(current_value);
        let peak_index = values
            .iter()
            .rposition(|value| *value == peak_value)
            .unwrap_or_default();
        values[peak_index..]
            .iter()
            .copied()
            .fold(peak_value, |trough, value| trough.min(value))
    }

    fn portfolio_drawdown_recovery_exposure_scale(
        &self,
        current_value: Decimal,
        peak_value: Decimal,
        base_scale: Decimal,
    ) -> Decimal {
        let risk_control = &self.config.risk_control;
        let (Some(start), Some(full)) = (
            risk_control.portfolio_drawdown_recovery_start_pct,
            risk_control.portfolio_drawdown_recovery_full_pct,
        ) else {
            return base_scale;
        };
        if base_scale >= Decimal::ONE || peak_value <= current_value || full <= start {
            return base_scale;
        }

        let trough_value = self.portfolio_equity_trough_since_peak(current_value, peak_value);
        if trough_value >= peak_value || current_value <= trough_value {
            return base_scale;
        }

        let recovery = (current_value - trough_value) / (peak_value - trough_value);
        if recovery <= start {
            return base_scale;
        }

        let progress = if recovery >= full {
            Decimal::ONE
        } else {
            (recovery - start) / (full - start)
        };
        let boost = risk_control
            .portfolio_drawdown_recovery_boost
            .unwrap_or(Decimal::ONE)
            .clamp(Decimal::zero(), Decimal::ONE);
        (base_scale + (Decimal::ONE - base_scale) * progress * boost)
            .clamp(Decimal::zero(), Decimal::ONE)
    }

    fn portfolio_drawdown_exposure_scale(
        &self,
        current_value: Decimal,
        peak_value: Decimal,
    ) -> Decimal {
        let risk_control = &self.config.risk_control;
        let (Some(start), Some(full), Some(min_exposure)) = (
            risk_control.portfolio_drawdown_reduce_start_pct,
            risk_control.portfolio_drawdown_reduce_full_pct,
            risk_control.portfolio_drawdown_min_exposure,
        ) else {
            return Decimal::ONE;
        };
        if peak_value <= Decimal::zero() || current_value >= peak_value || full <= start {
            return Decimal::ONE;
        }

        let min_exposure = min_exposure.clamp(Decimal::zero(), Decimal::ONE);
        if min_exposure >= Decimal::ONE {
            return Decimal::ONE;
        }

        let drawdown = (peak_value - current_value) / peak_value;
        let base_scale = if drawdown <= start {
            Decimal::ONE
        } else if drawdown >= full {
            min_exposure
        } else {
            let progress = (drawdown - start) / (full - start);
            Decimal::ONE - (Decimal::ONE - min_exposure) * progress
        };
        self.portfolio_drawdown_recovery_exposure_scale(current_value, peak_value, base_scale)
    }

    fn portfolio_recent_returns(
        &self,
        current_value: Decimal,
        lookback_days: usize,
    ) -> Vec<Decimal> {
        let start_index = self.equity_curve.len().saturating_sub(lookback_days);
        let mut values: Vec<Decimal> = self
            .equity_curve
            .iter()
            .skip(start_index)
            .map(|(_, value)| *value)
            .collect();
        values.push(current_value);
        values
            .windows(2)
            .filter_map(|window| {
                let previous = window[0];
                if previous.is_zero() {
                    None
                } else {
                    Some((window[1] - previous) / previous)
                }
            })
            .collect()
    }

    fn portfolio_volatility_exposure_scale(&self, current_value: Decimal) -> Decimal {
        let risk_control = &self.config.risk_control;
        let Some(target_volatility) = risk_control.portfolio_volatility_target_pct else {
            return Decimal::ONE;
        };
        if target_volatility <= Decimal::zero() {
            return Decimal::ONE;
        }

        let lookback_days = risk_control
            .portfolio_volatility_lookback_days
            .unwrap_or(60)
            .max(2);
        let returns = self.portfolio_recent_returns(current_value, lookback_days);
        if returns.len() < lookback_days {
            return Decimal::ONE;
        }

        let min_exposure = risk_control
            .portfolio_volatility_min_exposure
            .unwrap_or(Decimal::new(50, 2))
            .clamp(Decimal::zero(), Decimal::ONE);
        let max_exposure = risk_control
            .portfolio_volatility_max_exposure
            .unwrap_or(Decimal::ONE)
            .clamp(min_exposure, Decimal::ONE);

        let daily_returns: Vec<f64> = returns
            .iter()
            .filter_map(|value| value.to_f64())
            .filter(|value| value.is_finite())
            .collect();
        if daily_returns.len() < lookback_days {
            return Decimal::ONE;
        }

        let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
        let variance = daily_returns
            .iter()
            .map(|value| {
                let diff = value - mean;
                diff * diff
            })
            .sum::<f64>()
            / (daily_returns.len() - 1) as f64;
        let realized_volatility = variance.sqrt() * 252.0_f64.sqrt();
        if !realized_volatility.is_finite() || realized_volatility <= f64::EPSILON {
            return max_exposure;
        }

        let Some(target_volatility) = target_volatility.to_f64() else {
            return Decimal::ONE;
        };
        let raw_scale = target_volatility / realized_volatility;
        Decimal::from_f64(raw_scale)
            .unwrap_or(Decimal::ONE)
            .clamp(min_exposure, max_exposure)
    }

    fn portfolio_sharpe_exposure_scale(&self, current_value: Decimal) -> Decimal {
        let risk_control = &self.config.risk_control;
        let (Some(start), Some(full), Some(min_exposure)) = (
            risk_control.portfolio_sharpe_reduce_start,
            risk_control.portfolio_sharpe_reduce_full,
            risk_control.portfolio_sharpe_min_exposure,
        ) else {
            return Decimal::ONE;
        };
        if full >= start {
            return Decimal::ONE;
        }

        let lookback_days = risk_control
            .portfolio_sharpe_lookback_days
            .unwrap_or(120)
            .max(2);
        let returns = self.portfolio_recent_returns(current_value, lookback_days);
        if returns.len() < lookback_days {
            return Decimal::ONE;
        }

        let min_exposure = min_exposure.clamp(Decimal::zero(), Decimal::ONE);
        if min_exposure >= Decimal::ONE {
            return Decimal::ONE;
        }

        let daily_returns: Vec<f64> = returns
            .iter()
            .filter_map(|value| value.to_f64())
            .filter(|value| value.is_finite())
            .collect();
        if daily_returns.len() < lookback_days {
            return Decimal::ONE;
        }

        let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
        let variance = daily_returns
            .iter()
            .map(|value| {
                let diff = value - mean;
                diff * diff
            })
            .sum::<f64>()
            / (daily_returns.len() - 1) as f64;
        let std_dev = variance.sqrt();
        let rolling_sharpe = if std_dev <= f64::EPSILON {
            if mean < 0.0 {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }
        } else {
            mean / std_dev * 252.0_f64.sqrt()
        };
        if !rolling_sharpe.is_finite() {
            return if rolling_sharpe.is_sign_negative() {
                min_exposure
            } else {
                Decimal::ONE
            };
        }

        let Some(start) = start.to_f64() else {
            return Decimal::ONE;
        };
        let Some(full) = full.to_f64() else {
            return Decimal::ONE;
        };
        if rolling_sharpe >= start {
            return Decimal::ONE;
        }
        if rolling_sharpe <= full {
            return min_exposure;
        }

        let progress = (start - rolling_sharpe) / (start - full);
        let raw_scale = 1.0 - (1.0 - min_exposure.to_f64().unwrap_or(0.0)) * progress;
        Decimal::from_f64(raw_scale)
            .unwrap_or(Decimal::ONE)
            .clamp(min_exposure, Decimal::ONE)
    }

    fn portfolio_risk_exposure_scale(
        &self,
        current_value: Decimal,
        peak_value: Decimal,
    ) -> (Decimal, Decimal, Decimal, Decimal) {
        let drawdown_scale = self.portfolio_drawdown_exposure_scale(current_value, peak_value);
        let volatility_scale = self.portfolio_volatility_exposure_scale(current_value);
        let sharpe_scale = self.portfolio_sharpe_exposure_scale(current_value);
        let exposure_scale = drawdown_scale.min(volatility_scale).min(sharpe_scale);
        (
            drawdown_scale,
            volatility_scale,
            sharpe_scale,
            exposure_scale,
        )
    }

    fn execute_rebalance(&mut self, signal: &StrategySignal, market: &MarketDay) {
        let total_value = self.portfolio.total_value();
        let equity_peak = self.portfolio_equity_peak(total_value);
        let (drawdown_scale, volatility_scale, sharpe_scale, exposure_scale) =
            self.portfolio_risk_exposure_scale(total_value, equity_peak);
        info!(
            "rebalance: total_value={} targets={} close_symbols={} drawdown_scale={} volatility_scale={} sharpe_scale={} exposure_scale={}",
            total_value,
            signal.target_weights.len(),
            market.close.len(),
            drawdown_scale,
            volatility_scale,
            sharpe_scale,
            exposure_scale
        );
        let mut to_sell: Vec<(String, Decimal, Decimal)> = Vec::new();
        let mut to_buy: Vec<(String, Decimal, Decimal, Decimal)> = Vec::new();

        // Phase 1: 卖出不在信号中的持仓
        let symbols_to_remove: Vec<String> = self
            .portfolio
            .holdings
            .keys()
            .filter(|s| !signal.target_weights.contains_key(*s))
            .cloned()
            .collect();
        let mut symbols_to_remove = symbols_to_remove;
        symbols_to_remove.sort();

        for sym in &symbols_to_remove {
            if let Some(h) = self.portfolio.holdings.get(sym) {
                let qty = h.sellable_quantity;
                if !qty.is_zero() {
                    to_sell.push((sym.clone(), qty, Decimal::zero()));
                }
            }
        }

        // Phase 2: 按目标权重计算买卖
        let mut signal_targets = signal.target_weights.iter().collect::<Vec<_>>();
        signal_targets.sort_by_key(|(left, _)| *left);
        let mut target_gross_exposure = Decimal::zero();
        for (sym, raw_target_w) in signal_targets {
            let target_w = (*raw_target_w * exposure_scale).clamp(Decimal::zero(), Decimal::ONE);
            target_gross_exposure += target_w;
            let target_amount = total_value * target_w;
            let current_mv = self
                .portfolio
                .holdings
                .get(sym)
                .map(|h| h.market_value())
                .unwrap_or_default();

            let price = self.execution_price_for(market, sym);
            if price.is_zero() || market.suspended.contains(sym) {
                continue;
            }

            // 目标持仓量记录:A股/ETF 按100股向下取整,与买入执行(1484)一致。
            // 卖出场景目标量可能含零头,但记录取整反映"A股持仓应为100倍数"的真实约束。
            let target_quantity = if price.is_zero() {
                None
            } else {
                Some(round_down_to_lot(
                    target_amount / price,
                    quant_common::trading_rules::LOT_SIZE,
                ))
            };
            self.targets.push(PortfolioTarget {
                trade_date: market.date,
                symbol: sym.clone(),
                target_weight: target_w,
                target_quantity,
                reason: Some(if exposure_scale >= Decimal::ONE {
                    self.rebalance_target_reason("rebalance_signal")
                } else if sharpe_scale < drawdown_scale.min(volatility_scale) {
                    self.rebalance_target_reason("rebalance_signal_portfolio_sharpe_scaled")
                } else if volatility_scale < drawdown_scale {
                    self.rebalance_target_reason("rebalance_signal_portfolio_volatility_scaled")
                } else if drawdown_scale < Decimal::ONE {
                    self.rebalance_target_reason("rebalance_signal_portfolio_drawdown_scaled")
                } else {
                    self.rebalance_target_reason("rebalance_signal_portfolio_risk_scaled")
                }),
            });

            let diff = target_amount - current_mv;
            if diff.is_zero() {
                continue;
            }

            let ul = market.up_limit.get(sym).copied();
            let dl = market.down_limit.get(sym).copied();

            if diff > Decimal::zero() {
                // 买入 — 涨停不买
                if let Some(ul_price) = ul {
                    if price >= ul_price {
                        continue;
                    }
                }
                if self.is_position_risk_cooling_down(sym) {
                    self.violations.push(ConstraintViolation {
                        trade_date: market.date,
                        constraint_name: "position_risk_reentry_cooldown".into(),
                        limit_value: Decimal::from(
                            self.config
                                .risk_control
                                .reentry_cooldown_days
                                .unwrap_or_default(),
                        ),
                        actual_value: Decimal::from(
                            self.position_risk_cooldown_remaining
                                .get(sym)
                                .copied()
                                .unwrap_or_default(),
                        ),
                        severity: "hard".into(),
                    });
                    continue;
                }
                to_buy.push((sym.clone(), diff, price, target_w));
            } else {
                // 卖出 — 跌停不卖
                if let Some(dl_price) = dl {
                    if price <= dl_price {
                        continue;
                    }
                }
                if let Some(h) = self.portfolio.holdings.get(sym) {
                    let sell_qty = (-diff / price).min(h.sellable_quantity);
                    if !sell_qty.is_zero() {
                        to_sell.push((sym.clone(), sell_qty, target_w));
                    }
                }
            }
        }
        self.latest_target_gross_exposure_pct =
            target_gross_exposure.clamp(Decimal::zero(), Decimal::ONE);

        // Phase 3: 先卖后买
        for (sym, qty, target_w) in &to_sell {
            let price = self.execution_price_for(market, sym);
            // 跌停不卖
            if let Some(dl) = market.down_limit.get(sym) {
                if price <= *dl {
                    continue;
                }
            }
            let desired_amount = *qty * price;
            let capped_amount = self.cap_order_amount(market, sym, desired_amount);
            if capped_amount.is_zero() {
                continue;
            }
            let capped_qty = (capped_amount / price).floor().min(*qty);
            if capped_qty.is_zero() {
                continue;
            }
            let participation_rate = self.participation_rate_for(market, sym, capped_qty * price);
            if self
                .portfolio
                .sell_with_cost(
                    market.date,
                    sym,
                    capped_qty,
                    price,
                    participation_rate,
                    market.down_limit.get(sym).copied(),
                )
                .is_some()
            {
                let executed_weight = (capped_qty * price) / total_value;
                let reason = self.rebalance_trade_reason("sell");
                self.annotate_last_trade(*target_w, executed_weight, &reason);
                self.cleanup_closed_position_metadata(sym);
            }
        }
        for (sym, amount, price, target_w) in &to_buy {
            let capped_amount = self.cap_order_amount(market, sym, *amount);
            if capped_amount.is_zero() {
                continue;
            }
            // A股/ETF 买入必须按100股/份向下取整(1手=100)。
            // 卖出允许零头清仓不取整(见下方 sell 段),仅买入取整。
            let qty =
                round_down_to_lot(capped_amount / price, quant_common::trading_rules::LOT_SIZE);
            if !qty.is_zero() {
                // Pre-check for constraint violations
                let participation_rate = self.participation_rate_for(market, sym, qty * *price);
                let effective_slippage = (self.portfolio.fee_config.slippage_bps
                    + participation_rate * self.portfolio.fee_config.impact_cost_coefficient)
                    * self.portfolio.fee_config.cost_multiplier;
                let slippage_price = *price * (Decimal::ONE + effective_slippage);
                let buy_amount = qty * slippage_price;
                let commission = (buy_amount
                    * self.portfolio.fee_config.commission_rate
                    * self.portfolio.fee_config.cost_multiplier)
                    .max(
                        self.portfolio.fee_config.min_commission
                            * self.portfolio.fee_config.cost_multiplier,
                    );
                let total_cost = buy_amount + commission;

                // Cash check
                if self.portfolio.cash < total_cost {
                    self.violations.push(ConstraintViolation {
                        trade_date: market.date,
                        constraint_name: "cash_insufficient".into(),
                        limit_value: total_cost,
                        actual_value: self.portfolio.cash,
                        severity: "hard".into(),
                    });
                    continue;
                }

                // Position limit check
                let max_pct = self.portfolio.max_position_pct;
                if !max_pct.is_zero() {
                    let after_mv = self
                        .portfolio
                        .holdings
                        .get(sym)
                        .map(|h| h.market_value())
                        .unwrap_or_default()
                        + buy_amount;
                    let ratio = after_mv / self.portfolio.total_value();
                    if ratio > max_pct {
                        self.violations.push(ConstraintViolation {
                            trade_date: market.date,
                            constraint_name: "position_limit".into(),
                            limit_value: max_pct,
                            actual_value: ratio,
                            severity: "hard".into(),
                        });
                        continue;
                    }
                }

                if self
                    .portfolio
                    .buy_with_cost(
                        market.date,
                        sym,
                        qty,
                        *price,
                        participation_rate,
                        market.up_limit.get(sym).copied(),
                    )
                    .is_some()
                {
                    let executed_weight = (qty * *price) / total_value;
                    let reason = self.rebalance_trade_reason("buy");
                    self.annotate_last_trade(*target_w, executed_weight, &reason);
                    self.update_position_metadata_after_buy(sym, market.date, *price);
                }
            }
        }
    }

    pub fn finalize(self) -> BacktestOutput {
        let nav: Vec<Decimal> = self.equity_curve.iter().map(|(_, v)| *v).collect();
        let bm_nav: Vec<Decimal> = self.benchmark_curve.iter().map(|(_, v)| *v).collect();
        let mut metrics =
            super::metrics::BacktestMetrics::compute(&nav, &bm_nav, self.config.initial_capital);
        metrics.num_trades = self.portfolio.trades.len();
        metrics.execution_schedule_expired_count = self.execution_schedule_expired_count;
        metrics.execution_schedule_roll_forward_count = self.execution_schedule_roll_forward_count;
        metrics.max_execution_target_gap_pct = self.max_execution_target_gap_pct;
        let final_value = self.portfolio.total_value();
        metrics.final_cash_weight_pct = if final_value.is_zero() {
            Decimal::zero()
        } else {
            self.portfolio.cash / final_value
        };
        metrics.final_target_gross_exposure_pct = self.latest_target_gross_exposure_pct;
        metrics.final_actual_gross_exposure_pct = if final_value.is_zero() {
            Decimal::zero()
        } else {
            self.portfolio
                .holdings
                .values()
                .map(|holding| holding.market_value())
                .sum::<Decimal>()
                / final_value
        };
        metrics.final_unfilled_target_gap_pct = (metrics.final_target_gross_exposure_pct
            - metrics.final_actual_gross_exposure_pct)
            .max(Decimal::zero());
        metrics.final_execution_fill_ratio = if metrics.final_target_gross_exposure_pct.is_zero() {
            Decimal::ONE
        } else {
            (metrics.final_actual_gross_exposure_pct / metrics.final_target_gross_exposure_pct)
                .clamp(Decimal::zero(), Decimal::ONE)
        };
        let total_traded_amount = self
            .portfolio
            .trades
            .iter()
            .map(|trade| trade.amount)
            .sum::<Decimal>();
        let average_nav = if nav.is_empty() {
            Decimal::zero()
        } else {
            nav.iter().copied().sum::<Decimal>() / Decimal::from(nav.len())
        };
        metrics.turnover = if average_nav.is_zero() {
            Decimal::zero()
        } else {
            total_traded_amount / average_nav
        };

        // Compute win_rate from trade P&L (FIFO matched buy-sell pairs per symbol)
        // Buy queue: (quantity, cost, commission)
        let mut buy_queue: HashMap<String, Vec<(Decimal, Decimal, Decimal)>> = HashMap::new();
        let mut winning_trades: usize = 0;
        let mut completed_trades: usize = 0;
        let mut gross_profit = Decimal::zero();
        let mut gross_loss_abs = Decimal::zero();
        for trade in &self.portfolio.trades {
            if trade.side == super::portfolio::TradeSide::Buy {
                buy_queue.entry(trade.symbol.clone()).or_default().push((
                    trade.quantity,
                    trade.amount,
                    trade.commission,
                ));
            } else {
                let mut remaining = trade.quantity;
                let mut sell_gross = trade.amount;
                let mut sell_comm = trade.commission;
                let mut sell_tax = trade.tax;
                if let Some(queue) = buy_queue.get_mut(&trade.symbol) {
                    while !remaining.is_zero() && !queue.is_empty() {
                        let (bought_qty, bought_cost, bought_comm) = queue[0];
                        // 本段开始前的剩余卖出股数：sell 侧金额分摊须与 buy 侧对称
                        // （剩余金额 × matched/剩余股数）。此前误用总 trade.quantity 做
                        // 分母，跨多 lot 卖出从第二段起 sell_gross 已被递减、再按总量
                        // 折减 = 双重折减，P&L 系统性偏低（win_rate/profit_factor 失真）。
                        let remaining_before = remaining;
                        let matched_qty = if remaining >= bought_qty {
                            bought_qty
                        } else {
                            remaining
                        };
                        // Proportional allocation
                        let cost_basis = if bought_qty.is_zero() {
                            Decimal::zero()
                        } else {
                            bought_cost * matched_qty / bought_qty
                        };
                        let buy_comm = if bought_qty.is_zero() {
                            Decimal::zero()
                        } else {
                            bought_comm * matched_qty / bought_qty
                        };
                        let sell_gross_alloc = if remaining_before.is_zero() {
                            Decimal::zero()
                        } else {
                            sell_gross * matched_qty / remaining_before
                        };
                        let sell_comm_alloc = if remaining_before.is_zero() {
                            Decimal::zero()
                        } else {
                            sell_comm * matched_qty / remaining_before
                        };
                        let sell_tax_alloc = if remaining_before.is_zero() {
                            Decimal::zero()
                        } else {
                            sell_tax * matched_qty / remaining_before
                        };
                        // P&L = sell net - buy total
                        let pnl = sell_gross_alloc
                            - sell_comm_alloc
                            - sell_tax_alloc
                            - cost_basis
                            - buy_comm;
                        if pnl > Decimal::zero() {
                            winning_trades += 1;
                            gross_profit += pnl;
                        } else if pnl < Decimal::zero() {
                            gross_loss_abs += Decimal::zero() - pnl;
                        }
                        completed_trades += 1;
                        if remaining >= bought_qty {
                            queue.remove(0);
                            remaining -= bought_qty;
                            sell_gross -= sell_gross_alloc;
                            sell_comm -= sell_comm_alloc;
                            sell_tax -= sell_tax_alloc;
                        } else {
                            queue[0] = (
                                bought_qty - matched_qty,
                                bought_cost - cost_basis,
                                bought_comm - buy_comm,
                            );
                            remaining = Decimal::zero();
                        }
                    }
                }
            }
        }
        metrics.win_rate_pct = if completed_trades > 0 {
            Decimal::from(winning_trades) / Decimal::from(completed_trades)
        } else {
            Decimal::zero()
        };
        metrics.profit_factor = if gross_loss_abs.is_zero() {
            if gross_profit > Decimal::zero() {
                Decimal::new(999, 0)
            } else {
                Decimal::zero()
            }
        } else {
            gross_profit / gross_loss_abs
        };

        metrics.calmar_ratio = if metrics.max_drawdown_pct.is_zero() {
            if metrics.annual_return_pct > Decimal::zero() {
                Decimal::new(999, 0)
            } else {
                Decimal::zero()
            }
        } else {
            metrics.annual_return_pct / metrics.max_drawdown_pct
        };
        metrics.annualized_volatility = metrics.annual_volatility_pct;

        let hash = if self.config.mode == BacktestMode::Audit {
            Some(self.compute_hash())
        } else {
            None
        };

        BacktestOutput {
            config: self.config,
            metrics,
            equity_curve: self.equity_curve,
            benchmark_curve: self.benchmark_curve,
            trades: self.portfolio.trades,
            daily_positions: self.daily_positions,
            targets: self.targets,
            exposures: self.exposures,
            attributions: self.attributions,
            violations: self.violations,
            reproducibility_hash: hash,
        }
    }

    fn compute_hash(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        for (d, v) in &self.equity_curve {
            d.to_string().hash(&mut h);
            v.to_string().hash(&mut h);
        }
        format!("{:x}", h.finish())
    }
}

// ─── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    // ─── ExecutionScheduleProfile 解析(纯函数) ──────────────────

    #[test]
    fn execution_schedule_profile_parse_aliases_and_default() {
        use ExecutionScheduleProfile::*;
        // off/空串/immediate → Immediate
        for s in ["", "off", "immediate"] {
            assert_eq!(
                ExecutionScheduleProfile::parse(s).unwrap(),
                Immediate,
                "{}",
                s
            );
        }
        // 带与不带 _v1 后缀的别名等价
        for (a, b) in [
            ("twap_3d_v1", "twap3d_v1"),
            ("twap_5d_v1", "twap5d_v1"),
            ("twap_10d_v1", "twap10d_v1"),
            ("twap_15d_v1", "twap15d_v1"),
            ("twap_20d_v1", "twap20d_v1"),
        ] {
            assert_eq!(
                ExecutionScheduleProfile::parse(a).unwrap(),
                ExecutionScheduleProfile::parse(b).unwrap(),
                "{} 与 {} 应等价",
                a,
                b
            );
        }
        // 未知值 → Err(防配置静默降级)
        assert!(ExecutionScheduleProfile::parse("twap_7d").is_err());
        assert!(
            ExecutionScheduleProfile::parse("TWAP_3D_V1").is_err(),
            "大小写敏感"
        );
    }

    #[test]
    fn execution_schedule_profile_roundtrip_and_days() {
        // as_str 输出可再次 parse 回自身(配置序列化往返)
        for p in [
            ExecutionScheduleProfile::Immediate,
            ExecutionScheduleProfile::Twap3dV1,
            ExecutionScheduleProfile::Twap5dV1,
            ExecutionScheduleProfile::Twap10dV1,
            ExecutionScheduleProfile::Twap15dV1,
            ExecutionScheduleProfile::Twap20dV1,
        ] {
            assert_eq!(ExecutionScheduleProfile::parse(p.as_str()).unwrap(), p);
            assert!(p.execution_days() >= 1);
        }
        assert_eq!(ExecutionScheduleProfile::Immediate.execution_days(), 1);
        assert_eq!(ExecutionScheduleProfile::Twap20dV1.execution_days(), 20);
        assert!(ExecutionScheduleProfile::Immediate.is_immediate());
        assert!(!ExecutionScheduleProfile::Twap3dV1.is_immediate());
    }

    fn market(date: &str, close: (&str, &str), pre_close: (&str, &str)) -> MarketDay {
        let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        let mut m = MarketDay {
            date,
            open: HashMap::from([(close.0.into(), d(close.1))]),
            close: HashMap::from([(close.0.into(), d(close.1))]),
            pre_close: HashMap::from([(pre_close.0.into(), d(pre_close.1))]),
            amount: HashMap::from([(close.0.into(), d("100000000"))]),
            suspended: HashSet::new(),
            up_limit: HashMap::new(),
            down_limit: HashMap::new(),
            benchmark_close: d("1.0"),
            benchmark_pre_close: d("1.0"),
        };
        let pc = d(pre_close.1);
        m.up_limit.insert(pre_close.0.into(), pc * d("1.1"));
        m.down_limit.insert(pre_close.0.into(), pc * d("0.9"));
        m
    }

    fn signal(sym: &str, weight: &str) -> StrategySignal {
        StrategySignal {
            date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            target_weights: HashMap::from([(sym.into(), d(weight))]),
        }
    }

    fn signal_empty() -> StrategySignal {
        StrategySignal {
            date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            target_weights: HashMap::new(),
        }
    }

    fn signal_dated(date: &str, sym: &str, weight: &str) -> StrategySignal {
        StrategySignal {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            target_weights: HashMap::from([(sym.into(), d(weight))]),
        }
    }

    fn signal_dated_empty(date: &str) -> StrategySignal {
        StrategySignal {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            target_weights: HashMap::new(),
        }
    }

    fn eng() -> BacktestEngine {
        let c = BacktestConfig {
            max_position_pct: d("1.01"), // 101% — allow full buy with slippage
            ..Default::default()
        };
        BacktestEngine::new(c)
    }

    /// 四费全零（佣金/最低佣金/印花税/滑点）——隔离费用影响的测试配置。
    fn zero_fee_config() -> FeeConfig {
        FeeConfig {
            min_commission: Decimal::zero(),
            commission_rate: Decimal::zero(),
            tax_rate: Decimal::zero(),
            slippage_bps: Decimal::zero(),
            ..Default::default()
        }
    }

    // ─── Basic ────────────────────────────────────────────────

    #[test]
    fn test_empty_no_days() {
        let config = BacktestConfig {
            start_date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            ..Default::default()
        };
        let engine = BacktestEngine::new(config);
        let output = engine.finalize();
        assert!(output.equity_curve.is_empty());
    }

    #[test]
    fn test_no_signal() {
        let mut e = eng();
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "9.9")), None);
        let o = e.finalize();
        assert_eq!(o.equity_curve.len(), 1);
        assert_eq!(o.trades.len(), 0);
    }

    #[test]
    fn test_simple_buy() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.95")),
        );
        let o = e.finalize();
        assert_eq!(o.trades.len(), 1);
        assert!(o.trades[0].quantity > Decimal::zero());
    }

    #[test]
    fn execution_schedule_splits_rebalance_and_continues_without_new_signal() {
        let config = BacktestConfig {
            max_position_pct: d("1.01"),
            execution_schedule_profile: ExecutionScheduleProfile::Twap5dV1,
            ..Default::default()
        };
        let mut e = BacktestEngine::new(config);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.95")),
        );

        assert_eq!(e.portfolio.trades.len(), 1);
        let first_trade_amount = e.portfolio.trades[0].amount;
        assert!(
            first_trade_amount < d("250000"),
            "first TWAP slice should be materially smaller than full target amount, got {}",
            first_trade_amount
        );
        assert!(e.pending_execution_schedule.is_some());

        e.process_day(&market("2024-01-03", ("A", "10"), ("A", "10")), None);

        assert_eq!(e.portfolio.trades.len(), 2);
        assert!(e.portfolio.trades[1].amount > Decimal::zero());
        assert_eq!(
            e.portfolio.trades[1].event_reason.as_deref(),
            Some("execution_schedule_twap_5d_v1_buy")
        );
    }

    #[test]
    fn patient_execution_schedule_uses_smaller_initial_twap_slice() {
        let config = BacktestConfig {
            max_position_pct: d("1.01"),
            execution_schedule_profile: ExecutionScheduleProfile::Twap15dV1,
            ..Default::default()
        };
        let mut e = BacktestEngine::new(config);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.95")),
        );

        assert_eq!(e.portfolio.trades.len(), 1);
        assert!(
            e.portfolio.trades[0].amount < d("80000"),
            "15-day TWAP should only execute a small initial slice, got {}",
            e.portfolio.trades[0].amount
        );
        assert_eq!(
            e.portfolio.trades[0].event_reason.as_deref(),
            Some("execution_schedule_twap_15d_v1_buy")
        );
    }

    #[test]
    fn execution_schedule_caps_daily_target_move_and_expires_carry() {
        let config = BacktestConfig {
            max_position_pct: d("1.01"),
            execution_schedule_profile: ExecutionScheduleProfile::Twap5dV1,
            execution_daily_target_move_limit_pct: Some(d("0.05")),
            execution_max_carry_days: Some(2),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(config);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.95")),
        );

        assert_eq!(e.portfolio.trades.len(), 1);
        assert!(
            e.portfolio.trades[0].amount < d("60000"),
            "daily target move cap should limit first slice, got {}",
            e.portfolio.trades[0].amount
        );
        assert!(e.pending_execution_schedule.is_some());

        e.process_day(&market("2024-01-03", ("A", "10"), ("A", "10")), None);

        assert_eq!(e.portfolio.trades.len(), 2);
        assert!(e.pending_execution_schedule.is_none());

        let output = e.finalize();
        assert_eq!(output.metrics.execution_schedule_expired_count, 1);
        assert!(output.metrics.max_execution_target_gap_pct > Decimal::zero());
        assert!(output.metrics.final_cash_weight_pct > d("0.80"));
    }

    #[test]
    fn execution_schedule_roll_forward_policy_continues_pending_target_after_carry_ttl() {
        let config = BacktestConfig {
            max_position_pct: d("1.01"),
            execution_schedule_profile: ExecutionScheduleProfile::Twap5dV1,
            execution_daily_target_move_limit_pct: Some(d("0.05")),
            execution_max_carry_days: Some(2),
            execution_carry_policy: ExecutionCarryPolicy::RollForwardV1,
            ..Default::default()
        };
        let mut e = BacktestEngine::new(config);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.95")),
        );
        e.process_day(&market("2024-01-03", ("A", "10"), ("A", "10")), None);

        assert!(e.pending_execution_schedule.is_some());
        assert_eq!(e.execution_schedule_expired_count, 0);
        assert_eq!(e.execution_schedule_roll_forward_count, 1);

        e.process_day(&market("2024-01-04", ("A", "10"), ("A", "10")), None);
        assert!(e.portfolio.trades.len() >= 3);

        let output = e.finalize();
        assert_eq!(output.metrics.execution_schedule_expired_count, 0);
        assert_eq!(output.metrics.execution_schedule_roll_forward_count, 1);
        assert!(output.metrics.final_actual_gross_exposure_pct > d("0.10"));
    }

    #[test]
    fn execution_fill_metrics_separate_intentional_cash_from_unfilled_capacity() {
        let mut intended_cash = eng();
        intended_cash.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.35")),
        );

        let intended_cash_output = intended_cash.finalize();
        assert!(intended_cash_output.metrics.final_cash_weight_pct > d("0.60"));
        assert_eq!(
            intended_cash_output.metrics.final_target_gross_exposure_pct,
            d("0.35")
        );
        assert!(intended_cash_output.metrics.final_actual_gross_exposure_pct > d("0.34"));
        assert!(
            intended_cash_output.metrics.final_unfilled_target_gap_pct < d("0.02"),
            "intentional cash should not be treated as unfilled execution gap"
        );
        assert!(intended_cash_output.metrics.final_execution_fill_ratio > d("0.97"));

        let capacity_config = BacktestConfig {
            max_position_pct: d("1.01"),
            max_participation_rate: Some(d("0.10")),
            ..Default::default()
        };
        let mut capacity_limited = BacktestEngine::new(capacity_config);
        let mut thin_market = market("2024-01-02", ("A", "10"), ("A", "9.9"));
        // bar amount 为 Tushare 千元单位：100 千元 = 10 万元成交额，10% cap = 1 万元，
        // 远低于 80% 目标仓位需求（100 万 NAV），构造"想吃吃不下"的容量受限场景。
        thin_market.amount.insert("A".into(), d("100"));

        capacity_limited.process_day(&thin_market, Some(&signal("A", "0.80")));

        let capacity_output = capacity_limited.finalize();
        assert_eq!(
            capacity_output.metrics.final_target_gross_exposure_pct,
            d("0.80")
        );
        assert!(capacity_output.metrics.final_actual_gross_exposure_pct < d("0.02"));
        assert!(capacity_output.metrics.final_unfilled_target_gap_pct > d("0.78"));
        assert!(capacity_output.metrics.final_execution_fill_ratio < d("0.03"));
    }

    #[test]
    fn test_simple_sell() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.95")),
        );
        e.process_day(
            &market("2024-01-03", ("A", "11"), ("A", "10")),
            Some(&signal_empty()),
        );
        let o = e.finalize();
        let has_sell = o
            .trades
            .iter()
            .any(|t| matches!(t.side, crate::portfolio::TradeSide::Sell));
        assert!(has_sell, "Should have at least one sell trade");
    }

    // ─── Edge cases ───────────────────────────────────────────

    #[test]
    fn test_limit_up_blocks_buy() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "11"), ("A", "10")), // close=11 == up_limit=11
            Some(&signal("A", "0.95")),
        );
        assert_eq!(e.finalize().trades.len(), 0);
    }

    #[test]
    fn test_limit_down_blocks_sell() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.95")),
        );
        e.process_day(
            &market("2024-01-03", ("A", "9"), ("A", "10")),
            Some(&signal_empty()),
        ); // close=9 == down_limit=9
        let o = e.finalize();
        let sells = o
            .trades
            .iter()
            .filter(|t| matches!(t.side, crate::portfolio::TradeSide::Sell))
            .count();
        assert_eq!(sells, 0, "Limit-down should block sell");
    }

    #[test]
    fn test_suspension_skips() {
        let mut e = eng();
        let mut m = market("2024-01-02", ("A", "10"), ("A", "10"));
        m.suspended.insert("A".into());
        e.process_day(&m, Some(&signal("A", "0.95")));
        assert_eq!(e.finalize().trades.len(), 0);
    }

    #[test]
    fn test_t1_settlement() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal_dated("2024-01-02", "A", "0.95")),
        );
        e.process_day(
            &market("2024-01-03", ("A", "10.5"), ("A", "10")),
            Some(&signal_dated_empty("2024-01-03")),
        );
        let o = e.finalize();
        let sells = o
            .trades
            .iter()
            .filter(|t| matches!(t.side, crate::portfolio::TradeSide::Sell))
            .count();
        assert!(sells > 0, "T+1 should allow sell on day 2");
    }

    // ─── Costs ────────────────────────────────────────────────

    #[test]
    fn test_commission_min() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "1.0"), ("A", "1.0")),
            Some(&signal("A", "0.01")), // very small buy
        );
        let o = e.finalize();
        assert!(o.trades[0].commission >= d("5.0"), "Min commission 5 CNY");
    }

    #[test]
    fn test_stamp_tax() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.95")),
        );
        e.process_day(
            &market("2024-01-03", ("A", "10"), ("A", "10")),
            Some(&signal_empty()),
        );
        let o = e.finalize();
        let sell = o
            .trades
            .iter()
            .find(|t| matches!(t.side, crate::portfolio::TradeSide::Sell))
            .unwrap();
        assert!(sell.tax > Decimal::zero(), "Sell should have stamp tax");
    }

    #[test]
    fn test_slippage() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.95")),
        );
        let o = e.finalize();
        let t = &o.trades[0];
        assert!(t.slippage > Decimal::zero(), "Should have slippage");
    }

    // ─── Reproducibility ──────────────────────────────────────

    #[test]
    fn test_audit_hash() {
        let c = BacktestConfig {
            mode: BacktestMode::Audit,
            ..Default::default()
        };
        let mut e1 = BacktestEngine::new(c.clone());
        let mut e2 = BacktestEngine::new(c);
        let m = market("2024-01-02", ("A", "10"), ("A", "9.9"));
        let s = signal("A", "0.95");
        e1.process_day(&m, Some(&s));
        e2.process_day(&m, Some(&s));
        assert_eq!(
            e1.finalize().reproducibility_hash,
            e2.finalize().reproducibility_hash
        );
    }

    #[test]
    fn test_standard_no_hash() {
        let c = BacktestConfig {
            mode: BacktestMode::Standard,
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.95")),
        );
        assert!(e.finalize().reproducibility_hash.is_none());
    }

    #[test]
    fn test_fast_mode_no_positions() {
        let c = BacktestConfig {
            mode: BacktestMode::Fast,
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.95")),
        );
        let o = e.finalize();
        assert!(
            o.daily_positions.is_empty(),
            "Fast mode should not record positions"
        );
        assert!(!o.trades.is_empty(), "Fast mode should still record trades");
    }

    // ─── Position limit ───────────────────────────────────────

    #[test]
    fn test_position_limit_blocks() {
        let c = BacktestConfig {
            max_position_pct: d("0.20"),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.6")), // 60% > 20%
        );
        let o = e.finalize();
        assert_eq!(o.trades.len(), 0);
        assert!(
            !o.violations.is_empty(),
            "Should record constraint violation"
        );
        assert_eq!(o.violations[0].constraint_name, "position_limit");
    }

    #[test]
    fn portfolio_drawdown_control_scales_exposure_between_thresholds() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.05")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.15")),
                portfolio_drawdown_min_exposure: Some(d("0.40")),
                ..Default::default()
            },
            ..Default::default()
        };
        let e = BacktestEngine::new(c);

        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("100"), d("100")),
            Decimal::ONE
        );
        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("90"), d("100")),
            d("0.70")
        );
        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("80"), d("100")),
            d("0.40")
        );
    }

    #[test]
    fn portfolio_drawdown_control_can_use_rolling_peak_to_restore_exposure() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.05")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.15")),
                portfolio_drawdown_min_exposure: Some(d("0.40")),
                portfolio_drawdown_peak_lookback_days: Some(2),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("80")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), d("90")));

        let rolling_peak = e.portfolio_equity_peak(d("90"));

        assert_eq!(rolling_peak, d("90"));
        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("90"), rolling_peak),
            Decimal::ONE
        );
    }

    #[test]
    fn portfolio_drawdown_control_recovers_exposure_after_trough() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.05")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.15")),
                portfolio_drawdown_min_exposure: Some(d("0.40")),
                portfolio_drawdown_recovery_start_pct: Some(d("0.30")),
                portfolio_drawdown_recovery_full_pct: Some(d("0.70")),
                portfolio_drawdown_recovery_boost: Some(Decimal::ONE),
                portfolio_drawdown_peak_lookback_days: Some(252),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("80")));

        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("90"), d("100")),
            d("0.85")
        );
    }

    #[test]
    fn portfolio_drawdown_control_reduces_rebalance_targets() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.05")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.15")),
                portfolio_drawdown_min_exposure: Some(d("0.40")),
                ..Default::default()
            },
            fee_config: FeeConfig {
                min_commission: Decimal::zero(),
                commission_rate: Decimal::zero(),
                slippage_bps: Decimal::zero(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("1000000")));
        e.portfolio.cash = d("900000");

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "1.0")),
        );
        let o = e.finalize();

        assert_eq!(o.targets[0].target_weight, d("0.70"));
        assert_eq!(o.trades[0].event_reason.as_deref(), Some("rebalance_buy"));
        assert!(o.trades[0].executed_weight.unwrap() <= d("0.71"));
    }

    #[test]
    fn position_stop_loss_sells_without_rebalance_signal() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                stop_loss_pct: Some(d("0.05")),
                ..Default::default()
            },
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );
        assert!(e.position_buy_date.contains_key("A"));
        assert_eq!(e.position_peak_price.get("A"), Some(&d("10")));

        e.process_day(&market("2024-01-03", ("A", "9.4"), ("A", "10")), None);
        let o = e.finalize();

        assert!(o.trades.iter().any(|trade| {
            trade.side == crate::portfolio::TradeSide::Sell
                && trade.symbol == "A"
                && trade.event_reason.as_deref() == Some("risk_stop_loss")
        }));
    }

    #[test]
    fn position_risk_reentry_cooldown_blocks_buy_until_cooldown_expires() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                stop_loss_pct: Some(d("0.05")),
                reentry_cooldown_days: Some(1),
                ..Default::default()
            },
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal_dated("2024-01-02", "A", "0.50")),
        );
        e.process_day(
            &market("2024-01-03", ("A", "9.4"), ("A", "10")),
            Some(&signal_dated("2024-01-03", "A", "0.50")),
        );
        e.process_day(
            &market("2024-01-04", ("A", "9.5"), ("A", "9.4")),
            Some(&signal_dated("2024-01-04", "A", "0.50")),
        );
        e.process_day(
            &market("2024-01-05", ("A", "9.6"), ("A", "9.5")),
            Some(&signal_dated("2024-01-05", "A", "0.50")),
        );
        let o = e.finalize();

        let buys = o
            .trades
            .iter()
            .filter(|trade| trade.side == crate::portfolio::TradeSide::Buy)
            .map(|trade| trade.trade_date)
            .collect::<Vec<_>>();
        let sells = o
            .trades
            .iter()
            .filter(|trade| trade.side == crate::portfolio::TradeSide::Sell)
            .collect::<Vec<_>>();

        assert_eq!(sells.len(), 1);
        assert_eq!(sells[0].event_reason.as_deref(), Some("risk_stop_loss"));
        assert_eq!(
            buys,
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            ]
        );
        assert!(o.violations.iter().any(|violation| {
            violation.trade_date == NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()
                && violation.constraint_name == "position_risk_reentry_cooldown"
        }));
        assert!(o.violations.iter().any(|violation| {
            violation.trade_date == NaiveDate::from_ymd_opt(2024, 1, 4).unwrap()
                && violation.constraint_name == "position_risk_reentry_cooldown"
        }));
    }

    #[test]
    fn position_trailing_stop_uses_peak_price_since_buy() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                trailing_stop_pct: Some(d("0.08")),
                ..Default::default()
            },
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );
        e.process_day(&market("2024-01-03", ("A", "12"), ("A", "10")), None);
        assert_eq!(e.position_peak_price.get("A"), Some(&d("12")));

        e.process_day(&market("2024-01-04", ("A", "10.9"), ("A", "12")), None);
        let o = e.finalize();

        assert!(o.trades.iter().any(|trade| {
            trade.side == crate::portfolio::TradeSide::Sell
                && trade.symbol == "A"
                && trade.event_reason.as_deref() == Some("risk_trailing_stop")
        }));
    }

    #[test]
    fn portfolio_volatility_control_waits_for_full_lookback() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_volatility_target_pct: Some(d("0.15")),
                portfolio_volatility_lookback_days: Some(3),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("102")));

        assert_eq!(
            e.portfolio_volatility_exposure_scale(d("101")),
            Decimal::ONE
        );
    }

    #[test]
    fn portfolio_volatility_control_reduces_high_realized_volatility() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_volatility_target_pct: Some(d("0.10")),
                portfolio_volatility_lookback_days: Some(3),
                portfolio_volatility_min_exposure: Some(d("0.30")),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("112")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), d("88")));

        assert_eq!(e.portfolio_volatility_exposure_scale(d("105")), d("0.30"));
    }

    #[test]
    fn portfolio_sharpe_control_reduces_weak_rolling_return_quality() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_sharpe_reduce_start: Some(d("0.60")),
                portfolio_sharpe_reduce_full: Some(d("0.00")),
                portfolio_sharpe_lookback_days: Some(3),
                portfolio_sharpe_min_exposure: Some(d("0.40")),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("99")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), d("98")));

        assert_eq!(e.portfolio_sharpe_exposure_scale(d("97")), d("0.40"));
    }

    #[test]
    fn portfolio_sharpe_control_waits_for_full_lookback() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_sharpe_reduce_start: Some(d("0.60")),
                portfolio_sharpe_reduce_full: Some(d("0.00")),
                portfolio_sharpe_lookback_days: Some(3),
                portfolio_sharpe_min_exposure: Some(d("0.40")),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("99")));

        assert_eq!(e.portfolio_sharpe_exposure_scale(d("98")), Decimal::ONE);
    }

    #[test]
    fn portfolio_volatility_control_respects_max_exposure_without_leverage() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_volatility_target_pct: Some(d("0.20")),
                portfolio_volatility_lookback_days: Some(3),
                portfolio_volatility_min_exposure: Some(d("0.20")),
                portfolio_volatility_max_exposure: Some(d("0.80")),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("100.1")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), d("100.2")));

        assert_eq!(e.portfolio_volatility_exposure_scale(d("100.3")), d("0.80"));
    }

    #[test]
    fn portfolio_risk_control_uses_more_conservative_scale() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.05")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.15")),
                portfolio_drawdown_min_exposure: Some(d("0.40")),
                portfolio_volatility_target_pct: Some(d("0.10")),
                portfolio_volatility_lookback_days: Some(3),
                portfolio_volatility_min_exposure: Some(d("0.50")),
                portfolio_sharpe_reduce_start: Some(d("0.60")),
                portfolio_sharpe_reduce_full: Some(d("0.00")),
                portfolio_sharpe_lookback_days: Some(3),
                portfolio_sharpe_min_exposure: Some(d("0.30")),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("112")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), d("88")));

        let (drawdown_scale, volatility_scale, sharpe_scale, exposure_scale) =
            e.portfolio_risk_exposure_scale(d("90"), d("100"));

        assert_eq!(drawdown_scale, d("0.70"));
        assert_eq!(volatility_scale, d("0.50"));
        assert_eq!(sharpe_scale, d("0.30"));
        assert_eq!(exposure_scale, d("0.30"));
    }

    #[test]
    fn portfolio_volatility_control_reduces_rebalance_targets() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                portfolio_volatility_target_pct: Some(d("0.10")),
                portfolio_volatility_lookback_days: Some(3),
                portfolio_volatility_min_exposure: Some(d("0.50")),
                ..Default::default()
            },
            fee_config: FeeConfig {
                min_commission: Decimal::zero(),
                commission_rate: Decimal::zero(),
                slippage_bps: Decimal::zero(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("1000000")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("1120000")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), d("880000")));
        e.portfolio.cash = d("1050000");

        e.process_day(
            &market("2024-01-04", ("A", "10"), ("A", "10")),
            Some(&signal("A", "1.0")),
        );
        let o = e.finalize();

        assert_eq!(o.targets[0].target_weight, d("0.50"));
        assert_eq!(
            o.targets[0].reason.as_deref(),
            Some("rebalance_signal_portfolio_volatility_scaled")
        );
        assert!(o.trades[0].executed_weight.unwrap() <= d("0.51"));
    }

    #[test]
    fn rebalance_targets_are_recorded_in_deterministic_symbol_order() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            fee_config: FeeConfig {
                min_commission: Decimal::zero(),
                commission_rate: Decimal::zero(),
                slippage_bps: Decimal::zero(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        let mut m = market("2024-01-02", ("B", "10"), ("B", "10"));
        m.open.insert("A".into(), d("10"));
        m.close.insert("A".into(), d("10"));
        m.pre_close.insert("A".into(), d("10"));
        m.amount.insert("A".into(), d("100000000"));
        m.up_limit.insert("A".into(), d("11"));
        m.down_limit.insert("A".into(), d("9"));
        let signal = StrategySignal {
            date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            target_weights: HashMap::from([("B".into(), d("0.40")), ("A".into(), d("0.40"))]),
        };

        e.process_day(&m, Some(&signal));
        let o = e.finalize();

        assert_eq!(o.targets[0].symbol, "A");
        assert_eq!(o.targets[1].symbol, "B");
    }

    #[test]
    fn test_participation_rate_caps_buy_order() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            max_participation_rate: Some(d("0.10")),
            fee_config: FeeConfig {
                min_commission: Decimal::zero(),
                commission_rate: Decimal::zero(),
                slippage_bps: Decimal::zero(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        let mut m = market("2024-01-02", ("A", "10"), ("A", "10"));
        // bar 表 amount 为 Tushare 千元单位：1000 千元 = 100 万元成交额，
        // 10% 参与率上限即 10 万元（f01af33 千元→元换算后的口径）
        m.amount.insert("A".into(), d("1000"));

        e.process_day(&m, Some(&signal("A", "0.95")));
        let o = e.finalize();

        assert_eq!(o.trades.len(), 1);
        // 数量视角断言：cap 10 万元 / 价 10 元 = 10000 股（成交额含滑点会略超 cap，
        // 故不直接断言 amount）
        assert!(o.trades[0].quantity <= d("10000"));
        assert!(o
            .violations
            .iter()
            .any(|v| v.constraint_name == "participation_rate"));
    }

    // ─── ExecutionCarryPolicy 解析(纯函数) ─────────────────────

    #[test]
    fn execution_carry_policy_parse_aliases_and_unknown() {
        // 空串/expire 族别名 → Expire（TTL 到期丢弃残余目标）
        for s in ["", "expire", "expire_v1", "drop", "drop_on_ttl"] {
            assert_eq!(
                ExecutionCarryPolicy::parse(s).unwrap(),
                ExecutionCarryPolicy::Expire,
                "{} 应解析为 Expire",
                s
            );
        }
        // roll forward 族别名 → 到期后延续未完成目标
        for s in [
            "roll_forward_v1",
            "roll-forward-v1",
            "rolling_carry_v1",
            "continue_v1",
        ] {
            assert_eq!(
                ExecutionCarryPolicy::parse(s).unwrap(),
                ExecutionCarryPolicy::RollForwardV1,
                "{} 应解析为 RollForwardV1",
                s
            );
        }
        // 前后空白应被 trim 掉
        assert_eq!(
            ExecutionCarryPolicy::parse(" expire ").unwrap(),
            ExecutionCarryPolicy::Expire
        );
        assert_eq!(
            ExecutionCarryPolicy::parse(" roll_forward_v1 ").unwrap(),
            ExecutionCarryPolicy::RollForwardV1
        );
        // 未知值 → Err（防配置静默降级），且大小写敏感
        assert!(ExecutionCarryPolicy::parse("expire_v2").is_err());
        assert!(ExecutionCarryPolicy::parse("ROLL_FORWARD_V1").is_err());
    }

    // ─── adj_factor 复权跳变黑名单 ─────────────────────────────

    #[test]
    fn adj_factor_price_jump_blacklists_symbol_from_valuation() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        // day1: 10 元建仓，作为后续跳变检测的 warm prev close 基准
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );
        let equity_after_buy = e.portfolio.total_value();

        // day2: 非停牌期价格 +150%（>50% 阈值）→ 判定复权数据异常，加入黑名单
        e.process_day(&market("2024-01-03", ("A", "25"), ("A", "10")), None);

        assert!(
            e.adj_blacklisted.contains("A"),
            "跳变超 50% 的标的应进入 adj_factor 黑名单"
        );
        // 黑名单标的从 mark_to_market 剔除：净值维持买入日水平，不被异常价污染
        assert_eq!(
            e.equity_curve.last().unwrap().1,
            equity_after_buy,
            "被拉黑标的的异常天价不应影响组合估值"
        );
    }

    // ─── 持仓级风控: take_profit / time_stop ─────────────────────

    #[test]
    fn position_take_profit_sells_without_rebalance_signal() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                take_profit_pct: Some(d("0.10")),
                ..Default::default()
            },
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );
        // day2: +16%（开盘 11.6 >= 成本 10 * 1.10）触发止盈卖出（无调仓信号）
        e.process_day(&market("2024-01-03", ("A", "11.6"), ("A", "10")), None);
        let o = e.finalize();

        assert!(o.trades.iter().any(|trade| {
            trade.side == crate::portfolio::TradeSide::Sell
                && trade.symbol == "A"
                && trade.event_reason.as_deref() == Some("risk_take_profit")
        }));
        // 止盈应清空全部持仓：买入总量与卖出总量对平
        let bought: Decimal = o
            .trades
            .iter()
            .filter(|t| matches!(t.side, crate::portfolio::TradeSide::Buy))
            .map(|t| t.quantity)
            .sum();
        let sold: Decimal = o
            .trades
            .iter()
            .filter(|t| matches!(t.side, crate::portfolio::TradeSide::Sell))
            .map(|t| t.quantity)
            .sum();
        assert_eq!(bought, sold, "止盈卖出应清空全部持仓");
    }

    #[test]
    fn position_time_stop_exits_after_holding_threshold() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                time_stop_days: Some(2),
                ..Default::default()
            },
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );
        // 持有 1 个自然日：未达阈值不触发
        e.process_day(&market("2024-01-03", ("A", "10.2"), ("A", "10")), None);
        assert!(
            e.portfolio.holdings.contains_key("A"),
            "持有 1 天未到 time_stop 阈值不应卖出"
        );

        // 持有满 2 个自然日：触发时间止损
        e.process_day(&market("2024-01-04", ("A", "10.1"), ("A", "10.2")), None);
        let o = e.finalize();

        assert!(o.trades.iter().any(|trade| {
            trade.side == crate::portfolio::TradeSide::Sell
                && trade.symbol == "A"
                && trade.event_reason.as_deref() == Some("risk_time_stop")
        }));
    }

    #[test]
    fn position_risk_reentry_cooldown_zero_days_allows_immediate_reentry() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                stop_loss_pct: Some(d("0.05")),
                reentry_cooldown_days: Some(0),
                ..Default::default()
            },
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal_dated("2024-01-02", "A", "0.50")),
        );
        // day2: 触发止损，但 cooldown=0 不建立冷却映射 → 同日调仓信号可立即回补
        e.process_day(
            &market("2024-01-03", ("A", "9.4"), ("A", "10")),
            Some(&signal_dated("2024-01-03", "A", "0.50")),
        );
        let o = e.finalize();

        assert!(o
            .trades
            .iter()
            .any(|trade| { trade.event_reason.as_deref() == Some("risk_stop_loss") }));
        assert!(
            o.trades.iter().any(|trade| {
                trade.side == crate::portfolio::TradeSide::Buy
                    && trade.trade_date == NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()
            }),
            "cooldown=0 时止损当日即可回补"
        );
        assert!(o
            .violations
            .iter()
            .all(|v| v.constraint_name != "position_risk_reentry_cooldown"));
    }

    // ─── 流动性/现金约束边界 ──────────────────────────────────

    #[test]
    fn participation_cap_without_liquidity_amount_warns_and_allows_full_order() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            max_participation_rate: Some(d("0.10")),
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        let mut m = market("2024-01-02", ("A", "10"), ("A", "10"));
        // 成交额字段缺失：无法核算参与率上限
        m.amount.remove("A");

        e.process_day(&m, Some(&signal("A", "0.95")));
        let o = e.finalize();

        assert_eq!(
            o.trades.len(),
            1,
            "缺 liquidity amount 时应降级为全量下单而非拒单"
        );
        // 95% 目标权重按 100 万本金全额成交（未被参与率截断）
        assert_eq!(o.trades[0].quantity, d("95000"));
        assert!(o
            .violations
            .iter()
            .any(|v| v.constraint_name == "liquidity_amount_missing" && v.severity == "warning"));
    }

    #[test]
    fn insufficient_cash_records_violation_and_skips_buy() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        // day1: 半仓买入 A，锁定持仓市值
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );
        // 模拟大额赎回：现金仅剩 1000 元
        e.portfolio.cash = d("1000");

        // day2: A 维持 0.99 + 新买 B 0.45 —— B 目标金额约 22.5 万远超现金
        let mut m = market("2024-01-03", ("B", "10"), ("B", "10"));
        m.open.insert("A".into(), d("10"));
        m.close.insert("A".into(), d("10"));
        m.pre_close.insert("A".into(), d("10"));
        m.amount.insert("A".into(), d("100000000"));
        m.up_limit.insert("A".into(), d("11"));
        m.down_limit.insert("A".into(), d("9"));
        let sig = StrategySignal {
            date: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            target_weights: HashMap::from([("A".into(), d("0.99")), ("B".into(), d("0.45"))]),
        };
        e.process_day(&m, Some(&sig));
        let o = e.finalize();

        assert_eq!(
            o.trades
                .iter()
                .filter(|t| t.side == crate::portfolio::TradeSide::Buy && t.symbol == "B")
                .count(),
            0,
            "现金不足的买单应被跳过"
        );
        assert!(o
            .violations
            .iter()
            .any(|v| v.constraint_name == "cash_insufficient" && v.severity == "hard"));
    }

    // ─── 基准曲线边界 ────────────────────────────────────────

    #[test]
    fn benchmark_curve_carries_previous_value_when_pre_close_is_zero() {
        let mut e = eng();
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "9.9")), None);

        // day2: 基准 pre_close 缺失（0）→ 无法计算涨跌，曲线沿用前值
        let mut m = market("2024-01-03", ("A", "10"), ("A", "10"));
        m.benchmark_close = d("1.2");
        m.benchmark_pre_close = Decimal::ZERO;
        e.process_day(&m, None);

        let o = e.finalize();
        assert_eq!(o.benchmark_curve.len(), 2);
        assert_eq!(
            o.benchmark_curve[1].1, o.benchmark_curve[0].1,
            "基准前收盘缺失时曲线应保持前值而非除零"
        );
    }

    // ─── 组合级风控边界条件 ──────────────────────────────────

    #[test]
    fn portfolio_volatility_control_returns_max_exposure_for_flat_equity() {
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_volatility_target_pct: Some(d("0.15")),
                portfolio_volatility_lookback_days: Some(3),
                portfolio_volatility_max_exposure: Some(d("0.90")),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        for (day, value) in [
            ("2024-01-01", "100"),
            ("2024-01-02", "100"),
            ("2024-01-03", "100"),
        ] {
            e.equity_curve.push((
                NaiveDate::parse_from_str(day, "%Y-%m-%d").unwrap(),
                d(value),
            ));
        }

        // 已实现波动率为 0（净值纹丝不动）：目标波动率无从缩放，直接放开到上限
        assert_eq!(
            e.portfolio_volatility_exposure_scale(d("100")),
            d("0.90"),
            "零波动应返回 max_exposure 而非放大杠杆"
        );
    }

    #[test]
    fn portfolio_sharpe_control_uses_mean_sign_for_zero_variance_returns() {
        let base = RiskControlConfig {
            portfolio_sharpe_reduce_start: Some(d("0.60")),
            portfolio_sharpe_reduce_full: Some(d("0.00")),
            portfolio_sharpe_lookback_days: Some(3),
            portfolio_sharpe_min_exposure: Some(d("0.40")),
            ..Default::default()
        };

        // 恒定 +1% 收益（零方差 + 正均值）→ Sharpe = +∞ → 维持满仓
        let up_config = BacktestConfig {
            risk_control: base.clone(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(up_config);
        for (day, value) in [
            ("2024-01-01", "100"),
            ("2024-01-02", "101"),
            ("2024-01-03", "102.01"),
        ] {
            e.equity_curve.push((
                NaiveDate::parse_from_str(day, "%Y-%m-%d").unwrap(),
                d(value),
            ));
        }
        assert_eq!(
            e.portfolio_sharpe_exposure_scale(d("103.0301")),
            Decimal::ONE,
            "零方差正收益视为无限 Sharpe，不降杠杆"
        );

        // 恒定 -1% 收益（零方差 + 负均值）→ Sharpe = -∞ → 压到下限
        let down_config = BacktestConfig {
            risk_control: base,
            ..Default::default()
        };
        let mut e = BacktestEngine::new(down_config);
        for (day, value) in [
            ("2024-01-01", "100"),
            ("2024-01-02", "99"),
            ("2024-01-03", "98.01"),
        ] {
            e.equity_curve.push((
                NaiveDate::parse_from_str(day, "%Y-%m-%d").unwrap(),
                d(value),
            ));
        }
        assert_eq!(
            e.portfolio_sharpe_exposure_scale(d("97.0299")),
            d("0.40"),
            "零方差负收益视为负无限 Sharpe，压到 min_exposure"
        );
    }

    #[test]
    fn portfolio_sharpe_control_rejects_inverted_thresholds() {
        // full(0.60) >= start(0.00) 属无效配置：直接放行 100%，不做缩放
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_sharpe_reduce_start: Some(d("0.00")),
                portfolio_sharpe_reduce_full: Some(d("0.60")),
                portfolio_sharpe_lookback_days: Some(3),
                portfolio_sharpe_min_exposure: Some(d("0.40")),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        for (day, value) in [
            ("2024-01-01", "100"),
            ("2024-01-02", "99"),
            ("2024-01-03", "98"),
        ] {
            e.equity_curve.push((
                NaiveDate::parse_from_str(day, "%Y-%m-%d").unwrap(),
                d(value),
            ));
        }

        assert_eq!(
            e.portfolio_sharpe_exposure_scale(d("97")),
            Decimal::ONE,
            "阈值倒挂时不应触发任何缩放"
        );
    }

    #[test]
    fn portfolio_drawdown_control_keeps_full_exposure_on_invalid_floor_or_window() {
        // 下限抬到 100%：控制形同虚设，任何回撤都保持满仓
        let floored = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.05")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.15")),
                portfolio_drawdown_min_exposure: Some(Decimal::ONE),
                ..Default::default()
            },
            ..Default::default()
        };
        let e = BacktestEngine::new(floored);
        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("80"), d("100")),
            Decimal::ONE,
            "min_exposure >= 1 时应直接放行"
        );

        // full <= start 同属无效窗口：直接放行
        let inverted = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.15")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.05")),
                portfolio_drawdown_min_exposure: Some(d("0.40")),
                ..Default::default()
            },
            ..Default::default()
        };
        let e = BacktestEngine::new(inverted);
        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("80"), d("100")),
            Decimal::ONE,
            "full <= start 时应直接放行"
        );
    }

    #[test]
    fn portfolio_drawdown_recovery_waits_until_recovery_start_threshold() {
        // 回撤修复未过 start 阈值（recovery = 0.1 <= 0.30）：不提前恢复暴露
        let c = BacktestConfig {
            risk_control: RiskControlConfig {
                portfolio_drawdown_reduce_start_pct: Some(d("0.05")),
                portfolio_drawdown_reduce_full_pct: Some(d("0.15")),
                portfolio_drawdown_min_exposure: Some(d("0.40")),
                portfolio_drawdown_recovery_start_pct: Some(d("0.30")),
                portfolio_drawdown_recovery_full_pct: Some(d("0.70")),
                portfolio_drawdown_recovery_boost: Some(Decimal::ONE),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("80")));

        // 回撤 18% 已越过 reduce_full(15%) → 基础 scale 压到 0.40；
        // 自谷底仅反弹 10%（recovery 0.1 < start 0.3）→ 无恢复加成
        assert_eq!(
            e.portfolio_drawdown_exposure_scale(d("82"), d("100")),
            d("0.40")
        );
    }

    #[test]
    fn portfolio_recent_returns_skips_zero_equity_points() {
        let mut e = eng();
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), Decimal::ZERO));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), d("100")));
        e.equity_curve
            .push((NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), d("110")));

        // 首个净值点为 0：其收益率(除零)被跳过，只保留有效区间的收益
        let returns = e.portfolio_recent_returns(d("121"), 3);
        assert_eq!(returns, vec![d("0.10"), d("0.1")]);
    }

    // ─── finalize 指标: FIFO 分批匹配 ─────────────────────────

    #[test]
    fn finalize_win_rate_uses_fifo_lots_across_partial_sells() {
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        // 两批不同成本建仓：50,000 股 @10 + 32,500 股 @12
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );
        e.process_day(
            &market("2024-01-03", ("A", "12"), ("A", "12")),
            Some(&signal("A", "0.90")),
        );
        // day3: 目标降回 0.50 → 部分卖出 37,019 股 @13（只吃掉第一批 lot 的一部分）
        e.process_day(
            &market("2024-01-04", ("A", "13"), ("A", "13")),
            Some(&signal("A", "0.50")),
        );
        // day4: 清仓卖出剩余两段 lot（第一批残余 + 第二批全部）
        e.process_day(
            &market("2024-01-05", ("A", "13"), ("A", "13")),
            Some(&signal_empty()),
        );
        let o = e.finalize();

        // 三段 FIFO 匹配（37,019 + 12,981 + 32,500）全部盈利（13 > 10/12）
        assert_eq!(
            o.metrics.win_rate_pct,
            Decimal::ONE,
            "三段 FIFO 匹配应全部计入 win_rate"
        );
        // 无亏损交易：profit_factor 走 999 哨兵值
        assert_eq!(o.metrics.profit_factor, Decimal::new(999, 0));
    }

    // ─── 数据完整性 / 流动性约束的防御分支 ────────────────────

    #[test]
    fn suspension_gap_price_jump_does_not_blacklist_symbol() {
        let mut e = eng();

        // day1: 建立 warm prev close 基准（A=10 元）
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "10")), None);

        // day2: A 停牌——close 里无报价, 仅记录停牌状态
        let mut m2 = market("2024-01-03", ("A", "10"), ("A", "10"));
        m2.open.remove("A");
        m2.close.remove("A");
        m2.pre_close.remove("A");
        m2.amount.remove("A");
        m2.up_limit.clear();
        m2.down_limit.clear();
        m2.suspended.insert("A".into());
        e.process_day(&m2, None);

        // day3: 复牌 25 元（相对停牌前 +150%）——停牌缺口不是复权数据异常, 不应拉黑
        e.process_day(&market("2024-01-04", ("A", "25"), ("A", "10")), None);
        assert!(
            !e.adj_blacklisted.contains("A"),
            "停牌缺口导致的价格跳变应豁免 adj_factor 黑名单"
        );

        // day4: 非停牌期再跳变（25→60, +140%）——检测器本身仍在工作, 应拉黑
        e.process_day(&market("2024-01-05", ("A", "60"), ("A", "25")), None);
        assert!(
            e.adj_blacklisted.contains("A"),
            "复牌后的第二次跳变（非停牌缺口）仍应进入黑名单"
        );
    }

    #[test]
    fn position_risk_sell_blocked_by_zero_liquidity_keeps_holding() {
        // 止损触发但当日成交额为 0：参与率 cap 把卖单压到 0, 风控卖出被跳过、持仓保留
        let c = BacktestConfig {
            max_position_pct: d("1.01"),
            risk_control: RiskControlConfig {
                stop_loss_pct: Some(d("0.03")),
                ..Default::default()
            },
            max_participation_rate: Some(d("0.10")),
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(c);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.50")),
        );

        // day2: 开盘 9.6（<= 止损线 10*0.97=9.7, 且 > 跌停价 9）触发止损；
        // 但 amount=0（千元单位口径下的零成交额）→ cap 压零 → 卖单整体跳过
        let mut m2 = market("2024-01-03", ("A", "9.6"), ("A", "10"));
        m2.amount.insert("A".into(), Decimal::ZERO);
        e.process_day(&m2, None);
        let o = e.finalize();

        assert!(
            !o.trades
                .iter()
                .any(|t| matches!(t.side, crate::portfolio::TradeSide::Sell)),
            "零成交额下参与率 cap 为 0, 止损卖单应被跳过"
        );
        assert!(
            o.daily_positions
                .iter()
                .any(|p| p.symbol == "A" && p.quantity > Decimal::zero()),
            "卖单被跳过后持仓应保留在每日持仓快照中"
        );
        assert!(
            o.violations
                .iter()
                .any(|v| v.constraint_name == "participation_rate"
                    && v.severity == "hard"
                    && v.actual_value == d("480000")),
            "应有 participation_rate 硬约束违规记录（期望卖出金额 50,000 股 × 9.6 = 480,000）"
        );
    }

    #[test]
    fn signal_symbol_without_market_price_is_skipped_from_targets_and_trades() {
        // 信号含 B 但当日行情完全无 B 的报价（非停牌）：无价格 → 无法定价,
        // 既不记录 target 也不产生交易；A 正常成交
        let mut e = eng();
        let m = market("2024-01-02", ("A", "10"), ("A", "10"));
        let sig = StrategySignal {
            date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            target_weights: HashMap::from([("A".into(), d("0.50")), ("B".into(), d("0.40"))]),
        };
        e.process_day(&m, Some(&sig));
        let o = e.finalize();

        assert!(
            !o.targets.iter().any(|t| t.symbol == "B"),
            "无报价的信号标的不应记录 target"
        );
        assert!(
            !o.trades.iter().any(|t| t.symbol == "B"),
            "无报价的信号标的不应产生交易"
        );
        assert!(
            o.targets
                .iter()
                .any(|t| t.symbol == "A" && t.target_weight == d("0.50")),
            "有报价的 A 应正常记录 target"
        );
        assert!(
            o.trades
                .iter()
                .any(|t| t.symbol == "A" && t.quantity > Decimal::zero()),
            "有报价的 A 应正常买入"
        );
    }

    #[test]
    fn zero_daily_target_move_limit_disables_twap_cap() {
        // 防御：execution_daily_target_move_limit_pct=Some(0) 视为未启用——
        // TWAP 首日切片不被封顶。手算：Twap5d 首片 = 100 万 × 0.95 / 5 = 19 万
        let config = BacktestConfig {
            max_position_pct: d("1.01"),
            execution_schedule_profile: ExecutionScheduleProfile::Twap5dV1,
            execution_daily_target_move_limit_pct: Some(Decimal::ZERO),
            fee_config: zero_fee_config(),
            ..Default::default()
        };
        let mut e = BacktestEngine::new(config);

        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "9.9")),
            Some(&signal("A", "0.95")),
        );

        assert_eq!(e.portfolio.trades.len(), 1);
        // 用股数断言（engine 侧 qty 计算不含滑点）：19 万 / 10 元 = 19,000 股整手；
        // 若 cap 生效（对照测试 limit=0.05）首片会被压到 5 万市值 ≈ 5,000 股以下
        assert_eq!(
            e.portfolio.trades[0].quantity,
            d("19000"),
            "limit=0 应禁用单日权重移动 cap, TWAP 首片足额执行"
        );
        assert!(
            e.portfolio.trades[0].amount > d("189000"),
            "首片名义金额应约 19 万（含微量冲击滑点）, 实得 {}",
            e.portfolio.trades[0].amount
        );
        assert!(
            e.pending_execution_schedule.is_some(),
            "TWAP 日程应继续挂起"
        );
    }
}
