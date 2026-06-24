//! 数据库实体定义
//!
//! 字段与 05-表结构设计.md 中的新 Quant 表结构对齐

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

/// 股票每日基础/估值数据 (market_stock_daily_basic)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockDailyBasic {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub pe_ttm: Option<Decimal>,
    pub pb: Option<Decimal>,
    pub ps_ttm: Option<Decimal>,
    pub dv_ttm: Option<Decimal>,
    pub total_share: Option<Decimal>,
    pub float_share: Option<Decimal>,
    pub free_share: Option<Decimal>,
    pub total_mv: Option<Decimal>,
    pub circ_mv: Option<Decimal>,
}

/// 股票每日资金流向数据 (market_stock_moneyflow)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockMoneyflow {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub buy_sm_vol: Option<Decimal>,
    pub buy_sm_amount: Option<Decimal>,
    pub sell_sm_vol: Option<Decimal>,
    pub sell_sm_amount: Option<Decimal>,
    pub buy_md_vol: Option<Decimal>,
    pub buy_md_amount: Option<Decimal>,
    pub sell_md_vol: Option<Decimal>,
    pub sell_md_amount: Option<Decimal>,
    pub buy_lg_vol: Option<Decimal>,
    pub buy_lg_amount: Option<Decimal>,
    pub sell_lg_vol: Option<Decimal>,
    pub sell_lg_amount: Option<Decimal>,
    pub buy_elg_vol: Option<Decimal>,
    pub buy_elg_amount: Option<Decimal>,
    pub sell_elg_vol: Option<Decimal>,
    pub sell_elg_amount: Option<Decimal>,
    pub net_mf_vol: Option<Decimal>,
    pub net_mf_amount: Option<Decimal>,
}

/// 个股融资融券明细 (market_stock_margin_detail)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockMarginDetail {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub name: Option<String>,
    pub rzye: Option<Decimal>,
    pub rqye: Option<Decimal>,
    pub rzmre: Option<Decimal>,
    pub rqyl: Option<Decimal>,
    pub rzche: Option<Decimal>,
    pub rqchl: Option<Decimal>,
    pub rqmcl: Option<Decimal>,
    pub rzrqye: Option<Decimal>,
    pub available_at: NaiveDate,
    pub source_published_at: Option<DateTime<Utc>>,
    pub raw_payload: Value,
}

/// 业绩预告 (market_stock_forecast)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockForecast {
    pub symbol: String,
    pub ann_date: NaiveDate,
    pub end_date: NaiveDate,
    pub forecast_type: String,
    pub p_change_min: Option<Decimal>,
    pub p_change_max: Option<Decimal>,
    pub net_profit_min: Option<Decimal>,
    pub net_profit_max: Option<Decimal>,
    pub first_ann_date: NaiveDate,
    pub available_at: NaiveDate,
    pub summary: Option<String>,
    pub change_reason: Option<String>,
    pub raw_payload: Value,
}

/// 业绩快报 (market_stock_express)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockExpress {
    pub symbol: String,
    pub ann_date: NaiveDate,
    pub end_date: NaiveDate,
    pub revenue: Option<Decimal>,
    pub n_income: Option<Decimal>,
    pub yoy_sales: Option<Decimal>,
    pub yoy_dedu_np: Option<Decimal>,
    pub diluted_eps: Option<Decimal>,
    pub diluted_roe: Option<Decimal>,
    pub is_audit: Option<i32>,
    pub available_at: NaiveDate,
    pub perf_summary: Option<String>,
    pub remark: Option<String>,
    pub raw_payload: Value,
}

/// 财报披露日期 (market_stock_disclosure_date)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockDisclosureDate {
    pub symbol: String,
    pub end_date: NaiveDate,
    pub ann_date: NaiveDate,
    pub pre_date: Option<NaiveDate>,
    pub actual_date: Option<NaiveDate>,
    pub modify_date: Option<NaiveDate>,
    pub available_at: NaiveDate,
    pub raw_payload: Value,
}

/// 现金流量表 (market_stock_cashflow)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockCashflow {
    pub symbol: String,
    pub ann_date: NaiveDate,
    pub f_ann_date: Option<NaiveDate>,
    pub end_date: NaiveDate,
    pub available_at: NaiveDate,
    pub net_profit: Option<Decimal>,
    pub n_cashflow_act: Option<Decimal>,
    pub c_cash_equ_end_period: Option<Decimal>,
    pub raw_payload: Value,
}

/// 分红送股 (market_stock_dividend)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockDividend {
    pub symbol: String,
    pub end_date: NaiveDate,
    pub ann_date: NaiveDate,
    pub div_proc: String,
    pub available_at: NaiveDate,
    pub cash_div: Option<Decimal>,
    pub cash_div_tax: Option<Decimal>,
    pub record_date: Option<NaiveDate>,
    pub ex_date: Option<NaiveDate>,
    pub pay_date: Option<NaiveDate>,
    pub imp_ann_date: Option<NaiveDate>,
    pub raw_payload: Value,
}

/// 股票回购 (market_stock_repurchase)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockRepurchase {
    pub symbol: String,
    pub ann_date: NaiveDate,
    pub end_date: NaiveDate,
    pub proc: String,
    pub available_at: NaiveDate,
    pub exp_date: Option<NaiveDate>,
    pub vol: Option<Decimal>,
    pub amount: Option<Decimal>,
    pub high_limit: Option<Decimal>,
    pub low_limit: Option<Decimal>,
    pub raw_payload: Value,
}

/// 限售股解禁 (market_stock_share_float)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockShareFloat {
    pub symbol: String,
    pub ann_date: NaiveDate,
    pub float_date: NaiveDate,
    pub available_at: NaiveDate,
    pub float_share: Option<Decimal>,
    pub float_ratio: Option<Decimal>,
    pub holder_name: String,
    pub share_type: String,
    pub raw_payload: Value,
}

/// 主营业务构成 (market_stock_main_business)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockMainBusiness {
    pub symbol: String,
    pub end_date: NaiveDate,
    pub available_at: NaiveDate,
    pub business_type: String,
    pub bz_item: String,
    pub bz_code: String,
    pub bz_sales: Option<Decimal>,
    pub bz_profit: Option<Decimal>,
    pub bz_cost: Option<Decimal>,
    pub curr_type: String,
    pub update_flag: String,
    pub source_row_hash: String,
    pub raw_payload: Value,
}

/// 期货日线行情 (market_futures_daily)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFuturesDaily {
    pub ts_code: String,
    pub trade_date: NaiveDate,
    pub pre_close: Option<Decimal>,
    pub pre_settle: Option<Decimal>,
    pub open: Option<Decimal>,
    pub high: Option<Decimal>,
    pub low: Option<Decimal>,
    pub close: Option<Decimal>,
    pub settle: Option<Decimal>,
    pub change1: Option<Decimal>,
    pub change2: Option<Decimal>,
    pub vol: Option<Decimal>,
    pub amount: Option<Decimal>,
    pub oi: Option<Decimal>,
    pub oi_chg: Option<Decimal>,
    pub delv_settle: Option<Decimal>,
    pub available_at: NaiveDate,
    pub source_published_at: Option<DateTime<Utc>>,
    pub raw_payload: Value,
}

/// 期货仓单日报 (market_futures_warehouse_receipt)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFuturesWarehouseReceipt {
    pub trade_date: NaiveDate,
    pub symbol: String,
    pub exchange: String,
    pub fut_name: Option<String>,
    pub warehouse: String,
    pub wh_id: Option<String>,
    pub pre_vol: Option<Decimal>,
    pub vol: Option<Decimal>,
    pub vol_chg: Option<Decimal>,
    pub area: Option<String>,
    pub year: Option<String>,
    pub grade: Option<String>,
    pub brand: Option<String>,
    pub place: Option<String>,
    pub pd: Option<Decimal>,
    pub is_ct: Option<String>,
    pub unit: Option<String>,
    pub available_at: NaiveDate,
    pub source_published_at: Option<DateTime<Utc>>,
    pub raw_payload: Value,
}

/// 期货每日成交持仓排名 (market_futures_holding_rank)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFuturesHoldingRank {
    pub trade_date: NaiveDate,
    pub symbol: String,
    pub exchange: String,
    pub broker: String,
    pub vol: Option<Decimal>,
    pub vol_chg: Option<Decimal>,
    pub long_hld: Option<Decimal>,
    pub long_chg: Option<Decimal>,
    pub short_hld: Option<Decimal>,
    pub short_chg: Option<Decimal>,
    pub available_at: NaiveDate,
    pub source_published_at: Option<DateTime<Utc>>,
    pub raw_payload: Value,
}

/// PIT 行业成员历史 (market_stock_industry_membership_pit)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStockIndustryMembershipPit {
    pub classification_source: String,
    pub industry_level: String,
    pub index_code: String,
    pub index_name: String,
    pub industry_code: String,
    pub industry_name: String,
    pub parent_code: String,
    pub symbol: String,
    pub symbol_name: String,
    pub in_date: NaiveDate,
    pub out_date: Option<NaiveDate>,
    pub available_at: NaiveDate,
    pub exit_available_at: Option<NaiveDate>,
    pub is_new: String,
    pub raw_payload: Value,
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
