//! 组合管理模块
//!
//! 多标的组合：持仓、现金、交易记录和估值。
//! 从 valentina 的 Portfolio 重构：支持多标的、A 股规则。

use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 单标的持仓
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Holding {
    pub symbol: String,
    pub quantity: Decimal,
    pub avg_cost: Decimal,
    pub current_price: Decimal,
    /// T+1 卖出限制：当天买入的不可卖
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

    pub fn profit(&self) -> Decimal {
        self.market_value() - self.quantity * self.avg_cost
    }

    pub fn update_price(&mut self, price: Decimal) {
        self.current_price = price;
    }
}

/// 交易记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_date: chrono::NaiveDate,
    pub symbol: String,
    pub side: TradeSide,
    pub quantity: Decimal,
    pub price: Decimal,
    pub amount: Decimal,
    pub commission: Decimal,
    pub tax: Decimal,
    pub slippage: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// 组合管理器
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Portfolio {
    pub initial_capital: Decimal,
    pub cash: Decimal,
    pub holdings: HashMap<String, Holding>,
    pub trades: Vec<Trade>,
}

impl Portfolio {
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            initial_capital,
            cash: initial_capital,
            holdings: HashMap::new(),
            trades: Vec::new(),
        }
    }

    /// 总资产 = 现金 + 持仓市值
    pub fn total_value(&self) -> Decimal {
        let hv: Decimal = self.holdings.values().map(|h| h.market_value()).sum();
        self.cash + hv
    }

    /// 更新所有持仓的市价
    pub fn mark_to_market(&mut self, prices: &HashMap<String, Decimal>) {
        for (symbol, price) in prices {
            if let Some(h) = self.holdings.get_mut(symbol) {
                h.update_price(*price);
            }
        }
    }

    /// A 股买入：含手续费
    pub fn buy(
        &mut self,
        date: chrono::NaiveDate,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
    ) -> bool {
        let amount = quantity * price;
        let commission = amount * Decimal::new(3, 10000); // 0.03%
        let total = amount + commission;
        if self.cash < total {
            return false;
        }
        self.cash -= total;
        self.holdings
            .entry(symbol.to_string())
            .and_modify(|h| {
                let total_cost = h.quantity * h.avg_cost + amount;
                h.quantity += quantity;
                h.avg_cost = if h.quantity.is_zero() {
                    Decimal::zero()
                } else {
                    total_cost / h.quantity
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
            slippage: Decimal::zero(),
        });
        true
    }

    /// A 股卖出：含手续费 + 印花税
    pub fn sell(
        &mut self,
        date: chrono::NaiveDate,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
    ) -> bool {
        let holding = match self.holdings.get(symbol) {
            Some(h) => h,
            None => return false,
        };
        if holding.sellable_quantity < quantity {
            return false;
        }
        let amount = quantity * price;
        let commission = amount * Decimal::new(3, 10000); // 0.03%
        let tax = amount * Decimal::new(5, 10000); // 0.05% 印花税
        let net = amount - commission - tax;
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
            slippage: Decimal::zero(),
        });
        true
    }

    /// 每日结算：T+1 解冻
    pub fn end_of_day(&mut self) {
        for h in self.holdings.values_mut() {
            h.sellable_quantity = h.quantity;
        }
    }
}
