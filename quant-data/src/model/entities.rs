//! 数据库实体定义
//!
//! 字段与 05-表结构设计.md 中的新 Quant 表结构对齐

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// 股票基本信息 (market_stock)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStock {
    pub symbol: String,
    pub name: String,
    pub exchange: String,
    pub market: Option<String>,
    pub industry: Option<String>,
    pub list_status: String,
    pub list_date: Option<NaiveDate>,
    pub delist_date: Option<NaiveDate>,
    pub is_st: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 交易日历 (market_trade_calendar)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTradeCalendar {
    pub exchange: String,
    pub trade_date: NaiveDate,
    pub is_open: bool,
    pub pre_trade_date: Option<NaiveDate>,
}

/// 股票日线 (market_stock_daily_bar)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockDailyBar {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub pre_close: Option<Decimal>,
    pub change_pct: Option<Decimal>,
    pub volume: Decimal,
    pub amount: Decimal,
}

/// 指数日线 (market_index_daily_bar)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketIndexDailyBar {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub pre_close: Option<Decimal>,
    pub change_pct: Option<Decimal>,
    pub volume: Decimal,
    pub amount: Decimal,
}

/// 复权因子 (market_adjustment_factor)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketAdjustmentFactor {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub adj_factor: Decimal,
}

/// 数据版本 (data_version)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataVersion {
    pub data_version_id: String,
    pub description: Option<String>,
    pub data_date: NaiveDate,
    pub stock_count: i32,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub created_at: DateTime<Utc>,
}

/// 回测任务 (backtest_task)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestTask {
    pub task_id: String,
    pub data_version_id: String,
    pub strategy_version_id: String,
    pub universe: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub initial_capital: Decimal,
    pub benchmark_symbol: String,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}
