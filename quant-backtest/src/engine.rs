//! 回测引擎
//!
//! 多标的、A 股规则、基准跟踪、任务生命周期。

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::portfolio::{Portfolio, TradeSide};
use super::metrics::BacktestMetrics;

/// 回测配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub initial_capital: Decimal,
    pub benchmark: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    /// 手续费率 (0.03%)
    pub commission_rate: Decimal,
    /// 印花税率 (0.05% 卖出)
    pub tax_rate: Decimal,
    /// 滑点 (bps)
    pub slippage_bps: Decimal,
    /// 单标的最大仓位比例
    pub max_position_pct: Decimal,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_capital: Decimal::new(1000000, 0),
            benchmark: "000300.SH".into(),
            start_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
            commission_rate: Decimal::new(3, 10000),
            tax_rate: Decimal::new(5, 10000),
            slippage_bps: Decimal::new(1, 10000),
            max_position_pct: Decimal::new(10, 100), // 10%
        }
    }
}

/// 回测日频数据快照
#[derive(Debug, Clone)]
pub struct DailySnapshot {
    pub date: NaiveDate,
    pub prices: HashMap<String, Decimal>,
    pub pre_close: HashMap<String, Decimal>,
    pub is_trading: HashMap<String, bool>,
}

/// 回测结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub config: BacktestConfig,
    pub metrics: BacktestMetrics,
    pub equity_curve: Vec<(NaiveDate, Decimal)>,
    pub benchmark_curve: Vec<(NaiveDate, Decimal)>,
    pub trades: Vec<super::portfolio::Trade>,
    pub daily_positions: Vec<DailyPosition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyPosition {
    pub date: NaiveDate,
    pub positions: HashMap<String, (Decimal, Decimal)>, // (quantity, market_value)
    pub cash: Decimal,
}

/// 回测引擎
pub struct BacktestEngine {
    config: BacktestConfig,
    portfolio: Portfolio,
    equity_curve: Vec<(NaiveDate, Decimal)>,
    benchmark_curve: Vec<(NaiveDate, Decimal)>,
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig) -> Self {
        let portfolio = Portfolio::new(config.initial_capital);
        Self {
            config,
            portfolio,
            equity_curve: Vec::new(),
            benchmark_curve: Vec::new(),
        }
    }

    /// 获取当前组合的可变引用（供策略调用）
    pub fn portfolio_mut(&mut self) -> &mut Portfolio {
        &mut self.portfolio
    }

    /// 获取当前组合
    pub fn portfolio(&self) -> &Portfolio {
        &self.portfolio
    }

    /// 记录每日快照
    pub fn record_day(&mut self, date: NaiveDate, benchmark_value: Decimal) {
        let tv = self.portfolio.total_value();
        self.equity_curve.push((date, tv));
        self.benchmark_curve.push((date, benchmark_value));
        self.portfolio.end_of_day();
    }

    /// 计算最终指标
    pub fn finalize(self) -> BacktestResult {
        let nav: Vec<Decimal> = self.equity_curve.iter().map(|(_, v)| *v).collect();
        let bm_nav: Vec<Decimal> = self.benchmark_curve.iter().map(|(_, v)| *v).collect();
        let metrics = BacktestMetrics::compute(
            &nav,
            &bm_nav,
            self.config.initial_capital,
        );

        BacktestResult {
            config: self.config,
            metrics,
            equity_curve: self.equity_curve,
            benchmark_curve: self.benchmark_curve,
            trades: self.portfolio.trades,
            daily_positions: Vec::new(), // TODO
        }
    }
}
