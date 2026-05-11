//! 组合管理 — 多标的、A 股规则（T+1、涨跌停、费用）
//!
//! 从 valentina 重构，新增：滑点模型、可配置费用率、涨跌停价格过滤。

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
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
        Self { symbol, quantity, avg_cost: price, current_price: price, sellable_quantity: Decimal::zero() }
    }
    pub fn market_value(&self) -> Decimal { self.quantity * self.current_price }
    pub fn unrealized_pnl(&self) -> Decimal { self.market_value() - self.quantity * self.avg_cost }
    pub fn update_price(&mut self, price: Decimal) { self.current_price = price; }
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
    pub signal_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide { Buy, Sell }

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
}

impl Default for FeeConfig {
    fn default() -> Self {
        Self {
            commission_rate: Decimal::new(3, 4),  // 0.0003 = 万三
            min_commission: Decimal::new(5, 0),   // 5 元
            tax_rate: Decimal::new(5, 4),          // 0.0005 = 万五
            slippage_bps: Decimal::new(1, 4),      // 0.0001 = 1bp
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
        self.cash + self.holdings.values().map(|h| h.market_value()).sum::<Decimal>()
    }

    pub fn num_positions(&self) -> usize { self.holdings.len() }

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
        self.holdings.iter().map(|(sym, h)| {
            DailyPosition {
                date,
                symbol: sym.clone(),
                quantity: h.quantity,
                available_quantity: h.sellable_quantity,
                avg_cost: h.avg_cost,
                close_price: h.current_price,
                market_value: h.market_value(),
                weight: if tv.is_zero() { Decimal::zero() } else { h.market_value() / tv },
                unrealized_pnl: h.unrealized_pnl(),
                target_weight: None,
            }
        }).collect()
    }

    /// 买入：含手续费 + 滑点
    /// 返回实际成交价
    pub fn buy(&mut self, date: NaiveDate, symbol: &str, quantity: Decimal, price: Decimal) -> Option<Decimal> {
        if quantity.is_zero() { return None; }
        let slippage_price = price * (Decimal::ONE + self.fee_config.slippage_bps);
        let amount = quantity * slippage_price;
        let commission = (amount * self.fee_config.commission_rate).max(self.fee_config.min_commission);
        let total = amount + commission;
        if self.cash < total { return None; }
        // 仓位上限检查
        if !self.max_position_pct.is_zero() {
            let after_mv = self.holdings.get(symbol).map(|h| h.market_value()).unwrap_or_default() + amount;
            if after_mv / self.total_value() > self.max_position_pct { return None; }
        }
        self.cash -= total;
        self.holdings.entry(symbol.to_string())
            .and_modify(|h| {
                let tc = h.quantity * h.avg_cost + amount;
                h.quantity += quantity;
                h.avg_cost = if h.quantity.is_zero() { Decimal::zero() } else { tc / h.quantity };
                h.current_price = price;
            })
            .or_insert_with(|| Holding::new(symbol.to_string(), quantity, price));
        self.trades.push(Trade {
            trade_date: date, symbol: symbol.to_string(), side: TradeSide::Buy,
            quantity, price, amount, commission, tax: Decimal::zero(),
            slippage: amount - quantity * price, signal_type: None,
        });
        Some(slippage_price)
    }

    /// 卖出：含手续费 + 印花税 + 滑点
    /// 返回实际成交价
    pub fn sell(&mut self, date: NaiveDate, symbol: &str, quantity: Decimal, price: Decimal) -> Option<Decimal> {
        if quantity.is_zero() { return None; }
        let holding = self.holdings.get(symbol)?;
        if holding.sellable_quantity < quantity { return None; }
        let slippage_price = price * (Decimal::ONE - self.fee_config.slippage_bps);
        let amount = quantity * slippage_price;
        let commission = (amount * self.fee_config.commission_rate).max(self.fee_config.min_commission);
        let tax = amount * self.fee_config.tax_rate;
        let net = amount - commission - tax;
        self.cash += net;
        if let Some(h) = self.holdings.get_mut(symbol) {
            h.quantity -= quantity;
            h.sellable_quantity -= quantity;
            if h.quantity.is_zero() { self.holdings.remove(symbol); }
        }
        self.trades.push(Trade {
            trade_date: date, symbol: symbol.to_string(), side: TradeSide::Sell,
            quantity, price, amount, commission, tax,
            slippage: quantity * price - amount, signal_type: None,
        });
        Some(slippage_price)
    }

    /// 日终结算：T+1 解冻
    pub fn end_of_day(&mut self) {
        for h in self.holdings.values_mut() {
            h.sellable_quantity = h.quantity;
        }
    }
}
