//! 回测引擎 — 多标的、A 股规则、三种模式
//!
//! fast:   无明细记录，仅净值曲线 + 指标
//! standard: 全量记录（交易 + 持仓 + 净值）
//! audit:  standard + 可复现 hash 校验

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::info;

use super::portfolio::{DailyPosition, FeeConfig, Portfolio, Trade};

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
    pub close: HashMap<String, Decimal>,
    pub pre_close: HashMap<String, Decimal>,
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
    /// Risk control configuration
    #[serde(default)]
    pub risk_control: RiskControlConfig,
}

/// Per-position risk management
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for RiskControlConfig {
    fn default() -> Self {
        Self {
            stop_loss_pct: None,
            take_profit_pct: None,
            trailing_stop_pct: None,
            time_stop_days: None,
        }
    }
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

// ─── Result ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestOutput {
    pub config: BacktestConfig,
    pub metrics: super::metrics::BacktestMetrics,
    pub equity_curve: Vec<(NaiveDate, Decimal)>,
    pub benchmark_curve: Vec<(NaiveDate, Decimal)>,
    pub trades: Vec<Trade>,
    pub daily_positions: Vec<DailyPosition>,
    pub exposures: Vec<PortfolioExposure>,
    pub attributions: Vec<PortfolioAttribution>,
    pub violations: Vec<ConstraintViolation>,
    pub reproducibility_hash: Option<String>,
}

// ─── Engine ───────────────────────────────────────────────────────

pub struct BacktestEngine {
    config: BacktestConfig,
    pub portfolio: Portfolio,
    equity_curve: Vec<(NaiveDate, Decimal)>,
    benchmark_curve: Vec<(NaiveDate, Decimal)>,
    daily_positions: Vec<DailyPosition>,
    exposures: Vec<PortfolioExposure>,
    attributions: Vec<PortfolioAttribution>,
    violations: Vec<ConstraintViolation>,
    /// Track per-symbol buy date and peak price for risk control
    position_buy_date: HashMap<String, NaiveDate>,
    position_peak_price: HashMap<String, Decimal>,
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
            exposures: Vec::new(),
            attributions: Vec::new(),
            violations: Vec::new(),
            position_buy_date: HashMap::new(),
            position_peak_price: HashMap::new(),
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

        self.portfolio.mark_to_market(&market.close);

        if let Some(sig) = signal {
            self.execute_rebalance(sig, market);
        }

        self.equity_curve.push((market.date, self.portfolio.total_value()));

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
            self.daily_positions.extend(self.portfolio.snapshot(market.date));

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
            if let Some((_, prev_tv)) = self.equity_curve.last() {
                if !prev_tv.is_zero() {
                    self.attributions.push(PortfolioAttribution {
                        trade_date: market.date,
                        attribution_type: "total".into(),
                        attribution_name: "portfolio_return".into(),
                        contribution: (tv - *prev_tv) / *prev_tv,
                    });
                }
            }
        }

        self.portfolio.end_of_day();
    }

    fn execute_rebalance(&mut self, signal: &StrategySignal, market: &MarketDay) {
        let total_value = self.portfolio.total_value();
        info!(
            "rebalance: total_value={} targets={} close_symbols={}",
            total_value,
            signal.target_weights.len(),
            market.close.len()
        );
        let mut to_sell: Vec<(String, Decimal)> = Vec::new();
        let mut to_buy: Vec<(String, Decimal, Decimal)> = Vec::new();

        // Phase 1: 卖出不在信号中的持仓
        let symbols_to_remove: Vec<String> = self
            .portfolio.holdings.keys()
            .filter(|s| !signal.target_weights.contains_key(*s))
            .cloned()
            .collect();

        for sym in &symbols_to_remove {
            if let Some(h) = self.portfolio.holdings.get(sym) {
                let qty = h.sellable_quantity;
                if !qty.is_zero() {
                    to_sell.push((sym.clone(), qty));
                }
            }
        }

        // Phase 2: 按目标权重计算买卖
        for (sym, target_w) in &signal.target_weights {
            let target_amount = total_value * target_w;
            let current_mv = self
                .portfolio.holdings.get(sym)
                .map(|h| h.market_value())
                .unwrap_or_default();

            let diff = target_amount - current_mv;
            if diff.is_zero() {
                continue;
            }

            let price = market.close.get(sym).copied().unwrap_or_default();
            if price.is_zero() || market.suspended.contains(sym) {
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
                to_buy.push((sym.clone(), diff, price));
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
                        to_sell.push((sym.clone(), sell_qty));
                    }
                }
            }
        }

        // Phase 3: 先卖后买
        for (sym, qty) in &to_sell {
            let price = market.close.get(sym).copied().unwrap_or_default();
            // 跌停不卖
            if let Some(dl) = market.down_limit.get(sym) {
                if price <= *dl {
                    continue;
                }
            }
            self.portfolio.sell(market.date, sym, *qty, price);
        }
        for (sym, amount, price) in &to_buy {
            let qty = (amount / price).floor();
            if !qty.is_zero() {
                // Pre-check for constraint violations
                let slippage_price = *price * (Decimal::ONE + self.portfolio.fee_config.slippage_bps);
                let buy_amount = qty * slippage_price;
                let commission = (buy_amount * self.portfolio.fee_config.commission_rate)
                    .max(self.portfolio.fee_config.min_commission);
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
                    let after_mv = self.portfolio.holdings.get(sym)
                        .map(|h| h.market_value()).unwrap_or_default() + buy_amount;
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

                self.portfolio.buy(market.date, sym, qty, *price);
            }
        }
    }

    pub fn finalize(self) -> BacktestOutput {
        let nav: Vec<Decimal> = self.equity_curve.iter().map(|(_, v)| *v).collect();
        let bm_nav: Vec<Decimal> = self.benchmark_curve.iter().map(|(_, v)| *v).collect();
        let mut metrics = super::metrics::BacktestMetrics::compute(
            &nav, &bm_nav, self.config.initial_capital,
        );
        metrics.num_trades = self.portfolio.trades.len();

        // Compute win_rate from trade P&L (FIFO matched buy-sell pairs per symbol)
        // Buy queue: (quantity, cost, commission)
        let mut buy_queue: HashMap<String, Vec<(Decimal, Decimal, Decimal)>> = HashMap::new();
        let mut winning_trades: usize = 0;
        let mut completed_trades: usize = 0;
        for trade in &self.portfolio.trades {
            if trade.side == super::portfolio::TradeSide::Buy {
                buy_queue.entry(trade.symbol.clone())
                    .or_default()
                    .push((trade.quantity, trade.amount, trade.commission));
            } else {
                let mut remaining = trade.quantity;
                let mut sell_gross = trade.amount;
                let mut sell_comm = trade.commission;
                let mut sell_tax = trade.tax;
                if let Some(queue) = buy_queue.get_mut(&trade.symbol) {
                    while !remaining.is_zero() && !queue.is_empty() {
                        let (bought_qty, bought_cost, bought_comm) = queue[0];
                        let matched_qty = if remaining >= bought_qty { bought_qty } else { remaining };
                        // Proportional allocation
                        let cost_basis = if bought_qty.is_zero() { Decimal::zero() }
                            else { bought_cost * matched_qty / bought_qty };
                        let buy_comm = if bought_qty.is_zero() { Decimal::zero() }
                            else { bought_comm * matched_qty / bought_qty };
                        let sell_gross_alloc = if trade.quantity.is_zero() { Decimal::zero() }
                            else { sell_gross * matched_qty / trade.quantity };
                        let sell_comm_alloc = if trade.quantity.is_zero() { Decimal::zero() }
                            else { sell_comm * matched_qty / trade.quantity };
                        let sell_tax_alloc = if trade.quantity.is_zero() { Decimal::zero() }
                            else { sell_tax * matched_qty / trade.quantity };
                        // P&L = sell net - buy total
                        let pnl = sell_gross_alloc - sell_comm_alloc - sell_tax_alloc
                                - cost_basis - buy_comm;
                        if pnl > Decimal::zero() { winning_trades += 1; }
                        completed_trades += 1;
                        if remaining >= bought_qty {
                            queue.remove(0);
                            remaining -= bought_qty;
                            sell_gross -= sell_gross_alloc;
                            sell_comm -= sell_comm_alloc;
                            sell_tax -= sell_tax_alloc;
                        } else {
                            queue[0] = (bought_qty - matched_qty, bought_cost - cost_basis, bought_comm - buy_comm);
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

        metrics.calmar_ratio = if metrics.max_drawdown_pct.is_zero() {
            Decimal::zero()
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

    fn d(s: &str) -> Decimal { Decimal::from_str(s).unwrap() }

    fn market(date: &str, close: (&str, &str), pre_close: (&str, &str)) -> MarketDay {
        let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        let mut m = MarketDay {
            date,
            close: HashMap::from([(close.0.into(), d(close.1))]),
            pre_close: HashMap::from([(pre_close.0.into(), d(pre_close.1))]),
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
        let mut c = BacktestConfig::default();
        c.max_position_pct = d("1.01"); // 101% — allow full buy with slippage
        BacktestEngine::new(c)
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
    fn test_simple_sell() {
        let mut e = eng();
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "9.9")), Some(&signal("A", "0.95")));
        e.process_day(&market("2024-01-03", ("A", "11"), ("A", "10")), Some(&signal_empty()));
        let o = e.finalize();
        let has_sell = o.trades.iter().any(|t| matches!(t.side, crate::portfolio::TradeSide::Sell));
        assert!(has_sell, "Should have at least one sell trade");
    }

    // ─── Edge cases ───────────────────────────────────────────

    #[test]
    fn test_limit_up_blocks_buy() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "11"), ("A", "10")),  // close=11 == up_limit=11
            Some(&signal("A", "0.95")),
        );
        assert_eq!(e.finalize().trades.len(), 0);
    }

    #[test]
    fn test_limit_down_blocks_sell() {
        let mut e = eng();
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "10")), Some(&signal("A", "0.95")));
        e.process_day(&market("2024-01-03", ("A", "9"), ("A", "10")), Some(&signal_empty()));  // close=9 == down_limit=9
        let o = e.finalize();
        let sells = o.trades.iter().filter(|t| matches!(t.side, crate::portfolio::TradeSide::Sell)).count();
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
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "10")), Some(&signal_dated("2024-01-02", "A", "0.95")));
        e.process_day(&market("2024-01-03", ("A", "10.5"), ("A", "10")), Some(&signal_dated_empty("2024-01-03")));
        let o = e.finalize();
        let sells = o.trades.iter().filter(|t| matches!(t.side, crate::portfolio::TradeSide::Sell)).count();
        assert!(sells > 0, "T+1 should allow sell on day 2");
    }

    // ─── Costs ────────────────────────────────────────────────

    #[test]
    fn test_commission_min() {
        let mut e = eng();
        e.process_day(
            &market("2024-01-02", ("A", "1.0"), ("A", "1.0")),
            Some(&signal("A", "0.01")),  // very small buy
        );
        let o = e.finalize();
        assert!(o.trades[0].commission >= d("5.0"), "Min commission 5 CNY");
    }

    #[test]
    fn test_stamp_tax() {
        let mut e = eng();
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "10")), Some(&signal("A", "0.95")));
        e.process_day(&market("2024-01-03", ("A", "10"), ("A", "10")), Some(&signal_empty()));
        let o = e.finalize();
        let sell = o.trades.iter()
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
        let mut c = BacktestConfig::default();
        c.mode = BacktestMode::Audit;
        let mut e1 = BacktestEngine::new(c.clone());
        let mut e2 = BacktestEngine::new(c);
        let m = market("2024-01-02", ("A", "10"), ("A", "9.9"));
        let s = signal("A", "0.95");
        e1.process_day(&m, Some(&s));
        e2.process_day(&m, Some(&s));
        assert_eq!(e1.finalize().reproducibility_hash, e2.finalize().reproducibility_hash);
    }

    #[test]
    fn test_standard_no_hash() {
        let mut c = BacktestConfig::default();
        c.mode = BacktestMode::Standard;
        let mut e = BacktestEngine::new(c);
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "10")), Some(&signal("A", "0.95")));
        assert!(e.finalize().reproducibility_hash.is_none());
    }

    #[test]
    fn test_fast_mode_no_positions() {
        let mut c = BacktestConfig::default();
        c.mode = BacktestMode::Fast;
        let mut e = BacktestEngine::new(c);
        e.process_day(&market("2024-01-02", ("A", "10"), ("A", "10")), Some(&signal("A", "0.95")));
        let o = e.finalize();
        assert!(o.daily_positions.is_empty(), "Fast mode should not record positions");
        assert!(!o.trades.is_empty(), "Fast mode should still record trades");
    }

    // ─── Position limit ───────────────────────────────────────

    #[test]
    fn test_position_limit_blocks() {
        let mut c = BacktestConfig::default();
        c.max_position_pct = d("0.20");
        let mut e = BacktestEngine::new(c);
        e.process_day(
            &market("2024-01-02", ("A", "10"), ("A", "10")),
            Some(&signal("A", "0.6")), // 60% > 20%
        );
        let o = e.finalize();
        assert_eq!(o.trades.len(), 0);
        assert!(!o.violations.is_empty(), "Should record constraint violation");
        assert_eq!(o.violations[0].constraint_name, "position_limit");
    }
}
