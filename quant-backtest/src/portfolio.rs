//! 组合管理 — 多标的、A 股规则（T+1、涨跌停、费用）
//!
//! 从 valentina 重构，新增：滑点模型、可配置费用率、涨跌停价格过滤。

use chrono::NaiveDate;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Holding ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Holding {
    pub symbol: String,
    pub quantity: Decimal,
    pub avg_cost: Decimal,
    pub current_price: Decimal,
    /// T+1 可卖数量（当天买的不可卖）
    pub sellable_quantity: Decimal,
}

impl Holding {
    pub fn new(symbol: String, quantity: Decimal, price: Decimal) -> Self {
        Self {
            symbol,
            quantity,
            avg_cost: price,
            current_price: price,
            sellable_quantity: Decimal::zero(),
        }
    }
    pub fn market_value(&self) -> Decimal {
        self.quantity * self.current_price
    }
    pub fn unrealized_pnl(&self) -> Decimal {
        self.market_value() - self.quantity * self.avg_cost
    }
    pub fn update_price(&mut self, price: Decimal) {
        self.current_price = price;
    }
}

// ─── Trade ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_date: NaiveDate,
    pub symbol: String,
    pub side: TradeSide,
    pub quantity: Decimal,
    pub price: Decimal,
    pub amount: Decimal,
    pub commission: Decimal,
    pub tax: Decimal,
    pub slippage: Decimal,
    /// 过户费（沪市双边，深市为 0）
    pub transfer_fee: Decimal,
    pub signal_type: Option<String>,
    pub event_reason: Option<String>,
    pub target_weight: Option<Decimal>,
    pub executed_weight: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

// ─── Position (daily snapshot) ────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyPosition {
    pub date: NaiveDate,
    pub symbol: String,
    pub quantity: Decimal,
    pub available_quantity: Decimal,
    pub avg_cost: Decimal,
    pub close_price: Decimal,
    pub market_value: Decimal,
    pub weight: Decimal,
    pub unrealized_pnl: Decimal,
    pub target_weight: Option<Decimal>,
}

// ─── Fee config ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeConfig {
    /// 手续费率 (e.g. 0.0003 = 万三)
    pub commission_rate: Decimal,
    /// 最低手续费
    pub min_commission: Decimal,
    /// 印花税率 (卖出 0.0005 = 万五)
    pub tax_rate: Decimal,
    /// 滑点 bps (e.g. 0.0001 = 1bp)
    pub slippage_bps: Decimal,
    /// 成本压力倍数，用于成本上浮敏感性测试
    pub cost_multiplier: Decimal,
    /// 冲击成本系数，非线性模型：impact = coeff * participation_rate^exponent
    pub impact_cost_coefficient: Decimal,
    /// 冲击成本指数（平方根=0.5，线性=1.0），默认 0.5
    pub impact_cost_exponent: Decimal,
    /// 过户费率（沪市双边，默认 0.00001 = 万0.1），深市不征
    pub transfer_fee_rate: Decimal,
}

impl Default for FeeConfig {
    fn default() -> Self {
        Self {
            commission_rate: Decimal::new(3, 4), // 0.0003 = 万三
            min_commission: Decimal::new(5, 0),  // 5 元
            tax_rate: Decimal::new(5, 4),        // 0.0005 = 万五
            slippage_bps: Decimal::new(1, 4),    // 0.0001 = 1bp
            cost_multiplier: Decimal::ONE,
            // 平方根冲击模型：impact = 0.05 * sqrt(participation_rate)
            // participation_rate=10% 时冲击≈1.6%，超线性增长更贴近大单真实冲击
            impact_cost_coefficient: Decimal::new(5, 2),  // 0.05
            impact_cost_exponent: Decimal::new(5, 1),     // 0.5（平方根）
            transfer_fee_rate: Decimal::new(1, 5),        // 0.00001 = 万0.1（沪市双边）
        }
    }
}

// ─── Portfolio ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Portfolio {
    pub initial_capital: Decimal,
    pub cash: Decimal,
    pub holdings: HashMap<String, Holding>,
    pub trades: Vec<Trade>,
    pub fee_config: FeeConfig,
    /// 单标的最大仓位比例
    pub max_position_pct: Decimal,
}

impl Portfolio {
    pub fn new(initial_capital: Decimal, fee_config: FeeConfig, max_position_pct: Decimal) -> Self {
        Self {
            initial_capital,
            cash: initial_capital,
            holdings: HashMap::new(),
            trades: Vec::new(),
            fee_config,
            max_position_pct,
        }
    }

    pub fn total_value(&self) -> Decimal {
        self.cash
            + self
                .holdings
                .values()
                .map(|h| h.market_value())
                .sum::<Decimal>()
    }

    pub fn num_positions(&self) -> usize {
        self.holdings.len()
    }

    pub fn mark_to_market(&mut self, prices: &HashMap<String, Decimal>) {
        for (symbol, price) in prices {
            if let Some(h) = self.holdings.get_mut(symbol) {
                h.update_price(*price);
            }
        }
    }

    /// 获取当日持仓快照
    pub fn snapshot(&self, date: NaiveDate) -> Vec<DailyPosition> {
        let tv = self.total_value();
        self.holdings
            .iter()
            .map(|(sym, h)| DailyPosition {
                date,
                symbol: sym.clone(),
                quantity: h.quantity,
                available_quantity: h.sellable_quantity,
                avg_cost: h.avg_cost,
                close_price: h.current_price,
                market_value: h.market_value(),
                weight: if tv.is_zero() {
                    Decimal::zero()
                } else {
                    h.market_value() / tv
                },
                unrealized_pnl: h.unrealized_pnl(),
                target_weight: None,
            })
            .collect()
    }

    /// 买入：含手续费 + 滑点
    /// 返回实际成交价
    pub fn buy(
        &mut self,
        date: NaiveDate,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
    ) -> Option<Decimal> {
        self.buy_with_cost(date, symbol, quantity, price, Decimal::zero(), None)
    }

    /// 买入：含手续费、滑点和按参与率估算的冲击成本
    /// 返回实际成交价
    pub fn buy_with_cost(
        &mut self,
        date: NaiveDate,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
        participation_rate: Decimal,
        up_limit: Option<Decimal>,
    ) -> Option<Decimal> {
        if quantity.is_zero() {
            return None;
        }
        let slippage = self.effective_slippage(participation_rate);
        // 滑点价钳制涨停价:买入实际成交价不得超 up_limit(涨停封板无对手盘,
        // 即使有滑点也只能到涨停价)。无 up_limit(None)不钳制。
        let raw_slippage_price = price * (Decimal::ONE + slippage);
        let slippage_price = match up_limit {
            Some(ul) if ul > Decimal::ZERO => raw_slippage_price.min(ul),
            _ => raw_slippage_price,
        };
        let amount = quantity * slippage_price;
        let commission =
            (amount * self.effective_commission_rate()).max(self.effective_min_commission());
        let transfer = self.transfer_fee(symbol, amount);
        let total = amount + commission + transfer;
        if self.cash < total {
            return None;
        }
        // 仓位上限检查
        if !self.max_position_pct.is_zero() {
            let after_mv = self
                .holdings
                .get(symbol)
                .map(|h| h.market_value())
                .unwrap_or_default()
                + amount;
            if after_mv / self.total_value() > self.max_position_pct {
                return None;
            }
        }
        self.cash -= total;
        self.holdings
            .entry(symbol.to_string())
            .and_modify(|h| {
                let tc = h.quantity * h.avg_cost + amount;
                h.quantity += quantity;
                h.avg_cost = if h.quantity.is_zero() {
                    Decimal::zero()
                } else {
                    tc / h.quantity
                };
                h.current_price = price;
            })
            .or_insert_with(|| Holding::new(symbol.to_string(), quantity, price));
        self.trades.push(Trade {
            trade_date: date,
            symbol: symbol.to_string(),
            side: TradeSide::Buy,
            quantity,
            price,
            amount,
            commission,
            tax: Decimal::zero(),
            slippage: amount - quantity * price,
            transfer_fee: transfer,
            signal_type: None,
            event_reason: None,
            target_weight: None,
            executed_weight: None,
        });
        Some(slippage_price)
    }

    /// 卖出：含手续费 + 印花税 + 滑点
    /// 返回实际成交价
    pub fn sell(
        &mut self,
        date: NaiveDate,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
    ) -> Option<Decimal> {
        self.sell_with_cost(date, symbol, quantity, price, Decimal::zero(), None)
    }

    /// 卖出：含手续费、印花税、滑点和按参与率估算的冲击成本
    /// 返回实际成交价
    pub fn sell_with_cost(
        &mut self,
        date: NaiveDate,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
        participation_rate: Decimal,
        down_limit: Option<Decimal>,
    ) -> Option<Decimal> {
        if quantity.is_zero() {
            return None;
        }
        let holding = self.holdings.get(symbol)?;
        if holding.sellable_quantity < quantity {
            return None;
        }
        let slippage = self.effective_slippage(participation_rate);
        // 滑点价钳制跌停价:卖出实际成交价不得低于 down_limit(跌停封板无对手盘,
        // 即使有滑点也只能到跌停价)。无 down_limit(None)不钳制。
        let raw_slippage_price = price * (Decimal::ONE - slippage);
        let slippage_price = match down_limit {
            Some(dl) if dl > Decimal::ZERO => raw_slippage_price.max(dl),
            _ => raw_slippage_price,
        };
        let amount = quantity * slippage_price;
        let commission =
            (amount * self.effective_commission_rate()).max(self.effective_min_commission());
        let tax = amount * self.effective_tax_rate();
        let transfer = self.transfer_fee(symbol, amount);
        let net = amount - commission - tax - transfer;
        self.cash += net;
        if let Some(h) = self.holdings.get_mut(symbol) {
            h.quantity -= quantity;
            h.sellable_quantity -= quantity;
            if h.quantity.is_zero() {
                self.holdings.remove(symbol);
            }
        }
        self.trades.push(Trade {
            trade_date: date,
            symbol: symbol.to_string(),
            side: TradeSide::Sell,
            quantity,
            price,
            amount,
            commission,
            tax,
            slippage: quantity * price - amount,
            transfer_fee: transfer,
            signal_type: None,
            event_reason: None,
            target_weight: None,
            executed_weight: None,
        });
        Some(slippage_price)
    }

    /// 日终结算：T+1 解冻
    pub fn end_of_day(&mut self) {
        for h in self.holdings.values_mut() {
            h.sellable_quantity = h.quantity;
        }
    }

    fn effective_slippage(&self, participation_rate: Decimal) -> Decimal {
        // 非线性冲击成本模型：impact = coeff * participation_rate^exponent
        // 默认 exponent=0.5（平方根），大单冲击成本超线性增长，更贴近真实市场冲击。
        // 用 f64 powf（参照 metrics.rs:121 的 Decimal sqrt 模式），避免 Decimal 无原生 sqrt。
        // coeff=0 或 participation_rate=0 时无冲击（向后兼容旧配置）。
        let impact = if participation_rate.is_zero() || self.fee_config.impact_cost_coefficient.is_zero() {
            Decimal::ZERO
        } else {
            let pr_f = participation_rate.to_f64().unwrap_or(0.0).max(0.0);
            let exp = self.fee_config.impact_cost_exponent.to_f64().unwrap_or(0.5);
            let coeff = self.fee_config.impact_cost_coefficient;
            Decimal::from_f64(pr_f.powf(exp)).map(|v| coeff * v).unwrap_or(Decimal::ZERO)
        };
        let base = self.fee_config.slippage_bps + impact;
        base * self.fee_config.cost_multiplier
    }

    /// 过户费：沪市（.SH 后缀）双边征收，深市不征。
    /// 受 cost_multiplier 影响（成本敏感性测试统一）。
    fn transfer_fee(&self, symbol: &str, amount: Decimal) -> Decimal {
        if symbol.ends_with(".SH") {
            amount * self.fee_config.transfer_fee_rate * self.fee_config.cost_multiplier
        } else {
            Decimal::ZERO
        }
    }

    fn effective_commission_rate(&self) -> Decimal {
        self.fee_config.commission_rate * self.fee_config.cost_multiplier
    }

    fn effective_tax_rate(&self) -> Decimal {
        self.fee_config.tax_rate * self.fee_config.cost_multiplier
    }

    fn effective_min_commission(&self) -> Decimal {
        self.fee_config.min_commission * self.fee_config.cost_multiplier
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_portfolio() -> Portfolio {
        Portfolio::new(Decimal::new(1_000_000, 0), FeeConfig::default(), Decimal::ZERO)
    }

    /// 平方根冲击模型：impact = 0.05 * sqrt(participation_rate)
    /// participation_rate=0.1 时 impact ≈ 0.05 * 0.3162 ≈ 0.01581
    #[test]
    fn effective_slippage_sqrt_model() {
        let p = default_portfolio();
        let slippage = p.effective_slippage(Decimal::new(1, 1)); // 0.1
        // slippage = slippage_bps(0.0001) + 0.05 * sqrt(0.1)
        let expected_impact = Decimal::from_f64(0.1_f64.sqrt()).map(|v| Decimal::new(5, 2) * v).unwrap();
        let expected = Decimal::new(1, 4) + expected_impact;
        assert!(
            (slippage - expected).abs() < Decimal::new(1, 8),
            "expected {expected}, got {slippage}"
        );
    }

    /// 非线性：participation_rate 翻倍，冲击增量小于线性（sqrt 凹性）
    #[test]
    fn effective_slippage_sqrt_sublinear() {
        let p = default_portfolio();
        let s_low = p.effective_slippage(Decimal::new(5, 2));  // 0.05
        let s_high = p.effective_slippage(Decimal::new(1, 1)); // 0.10（参与率翻倍）
        let impact_low = s_low - Decimal::new(1, 4);
        let impact_high = s_high - Decimal::new(1, 4);
        // 线性模型 impact_high/impact_low = 2.0；平方根模型 = sqrt(2) ≈ 1.414
        let ratio = impact_high / impact_low;
        assert!(ratio < Decimal::new(18, 1), "sqrt 模型应亚线性，ratio={ratio} < 1.8"); // < 1.8
        assert!(ratio > Decimal::new(14, 1), "ratio 应接近 sqrt(2)≈1.414，got {ratio}");
    }

    /// coeff=0 时无冲击（向后兼容旧配置）
    #[test]
    fn impact_zero_when_coeff_zero() {
        let mut p = default_portfolio();
        p.fee_config.impact_cost_coefficient = Decimal::ZERO;
        let slippage = p.effective_slippage(Decimal::new(1, 1));
        assert_eq!(slippage, Decimal::new(1, 4)); // 仅 slippage_bps
    }

    /// participation_rate=0 时无冲击
    #[test]
    fn impact_zero_when_no_participation() {
        let p = default_portfolio();
        let slippage = p.effective_slippage(Decimal::ZERO);
        assert_eq!(slippage, Decimal::new(1, 4));
    }

    /// 过户费只对沪市（.SH）征收
    #[test]
    fn transfer_fee_sh_only() {
        let p = default_portfolio();
        let amount = Decimal::new(100_000, 0);
        // 沪市征过户费 = 100000 * 0.00001 = 1.0
        assert_eq!(p.transfer_fee("600000.SH", amount), Decimal::new(1, 0));
        // 深市不征
        assert_eq!(p.transfer_fee("000001.SZ", amount), Decimal::ZERO);
    }

    /// buy_with_cost 扣过户费（沪市）
    #[test]
    fn buy_sh_includes_transfer_fee() {
        let p = Portfolio::new(
            Decimal::new(1_000_000, 0),
            FeeConfig::default(),
            Decimal::ZERO,
        );
        let mut p = p;
        let date = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        // 买 100 股 600000.SH @ 10 元，participation_rate=0 → slippage=slippage_bps=0.0001
        // slippage_price = 10 * 1.0001 = 10.001，amount = 1000.1
        // 过户费 = 1000.1 * 0.00001 = 0.010001（沪市）
        let fill = p.buy_with_cost(date, "600000.SH", Decimal::new(100, 0), Decimal::new(10, 0), Decimal::ZERO, None);
        assert!(fill.is_some());
        let trade = p.trades.last().unwrap();
        assert_eq!(trade.transfer_fee, Decimal::new(10001, 6)); // 0.010001
        assert_eq!(trade.side, TradeSide::Buy);
    }

    /// buy_with_cost 深市无过户费
    #[test]
    fn buy_sz_no_transfer_fee() {
        let p = Portfolio::new(
            Decimal::new(1_000_000, 0),
            FeeConfig::default(),
            Decimal::ZERO,
        );
        let mut p = p;
        let date = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        let fill = p.buy_with_cost(date, "000001.SZ", Decimal::new(100, 0), Decimal::new(10, 0), Decimal::ZERO, None);
        assert!(fill.is_some());
        let trade = p.trades.last().unwrap();
        assert_eq!(trade.transfer_fee, Decimal::ZERO);
    }
}
