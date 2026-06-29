//! 共享建仓模块 —— 回放与实盘走同一套选股→建仓→盯市链路。
//! 唯一差异:价格源(PriceSource)。选股统一为"读某 task_id 的 backtest_position 当日截面"
//! (实盘 task_id 由 generate_paper_signals_for_all 的 run-factor 产生并传入,
//!  回放 task_id 用 sc.equity_curve_task_id)。
//!
//! NAV 恒等式:current_nav = 持仓市值 + cash - margin_amount(经 trading::update_current_nav)
//!
//! 绩效口径(spec §2.1/§3.4):回放绩效基于真实持仓盯市后的 current_nav 复利,
//! 不再用 backtest_equity_curve 的收益率累乘。rebalance_account 做目标持仓驱动增量调仓。

#![allow(dead_code, unused_imports)]

use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::routes::scheduler::{MvoWeightCache, StrategyConfig};
use quant_data::tushare::client::TushareClient;

/// 价格源(回放/实盘唯一差异)
pub enum PriceSource {
    /// 回放:日终收盘价(market_stock_daily_bar_adj)
    EodClose,
    /// 实盘:盘中实时价(Tushare)
    Intraday,
}

/// 选股结果(当日某 A 股个股的截面持仓)
pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub market_value: Decimal,
}

/// 统一选股:读指定 task_id 的 backtest_position 当日截面。
/// task_id 来源:实盘=run-factor 产生;回放=sc.equity_curve_task_id。
pub async fn select_positions(
    db: &PgPool,
    task_id: &str,
    date: NaiveDate,
) -> Result<Vec<Position>, String> {
    let rows = sqlx::query_as::<_, (String, Decimal, Decimal)>(
        "SELECT symbol, COALESCE(quantity,0), COALESCE(market_value,0)
         FROM backtest_position
         WHERE task_id = $1 AND position_date = $2
           AND quantity > 0 AND market_value > 0
         ORDER BY market_value DESC",
    )
    .bind(task_id)
    .bind(date)
    .fetch_all(db)
    .await
    .map_err(|e| format!("select_positions: {}", e))?;
    Ok(rows
        .into_iter()
        .map(|(symbol, quantity, market_value)| Position {
            symbol,
            quantity,
            market_value,
        })
        .collect())
}

// rebalance_account 与 mark_to_market 由 Task 5 在此追加
