//! 数据仓储层
//!
//! 使用 SQLx 裸 SQL 查询（时序/批量走 SQLx + TimescaleDB）。
//! 遵循 05-表结构设计.md 中的表结构。

use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{debug, info};

use crate::model::entities::{MarketStock, MarketStockDailyBar, MarketTradeCalendar, MarketAdjustmentFactor};

// ─── market_stock ────────────────────────────────────────────────

pub async fn upsert_stock(pool: &PgPool, stock: &MarketStock) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock (symbol, name, exchange, market, industry, list_status, list_date, delist_date, is_st)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (symbol) DO UPDATE SET
             name = EXCLUDED.name,
             market = EXCLUDED.market,
             industry = EXCLUDED.industry,
             list_status = EXCLUDED.list_status,
             list_date = EXCLUDED.list_date,
             delist_date = EXCLUDED.delist_date,
             is_st = EXCLUDED.is_st,
             updated_at = now()"#,
    )
    .bind(&stock.symbol)
    .bind(&stock.name)
    .bind(&stock.exchange)
    .bind(&stock.market)
    .bind(&stock.industry)
    .bind(&stock.list_status)
    .bind(stock.list_date)
    .bind(stock.delist_date)
    .bind(stock.is_st)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_stocks_batch(
    pool: &PgPool,
    stocks: &[MarketStock],
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for stock in stocks {
        upsert_stock(pool, stock).await?;
        count += 1;
    }
    debug!("批量 upsert {} 条 stock_basic", count);
    Ok(count)
}

pub async fn count_stocks(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM market_stock")
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

// ─── market_stock_daily_bar ──────────────────────────────────────

pub async fn upsert_daily_bar(
    pool: &PgPool,
    bar: &MarketStockDailyBar,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock_daily_bar
             (symbol, trade_date, open, high, low, close, pre_close, pct_change,
              volume, amount, source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           ON CONFLICT (symbol, trade_date) DO UPDATE SET
             open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
             close = EXCLUDED.close, pre_close = EXCLUDED.pre_close,
             pct_change = EXCLUDED.pct_change,
             volume = EXCLUDED.volume, amount = EXCLUDED.amount,
             source = EXCLUDED.source, data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&bar.symbol)
    .bind(bar.trade_date)
    .bind(bar.open)
    .bind(bar.high)
    .bind(bar.low)
    .bind(bar.close)
    .bind(bar.pre_close)
    .bind(bar.change_pct)
    .bind(bar.volume)
    .bind(bar.amount)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_daily_bars_batch(
    pool: &PgPool,
    bars: &[MarketStockDailyBar],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for bar in bars {
        upsert_daily_bar(pool, bar, data_version_id, source).await?;
        count += 1;
    }
    info!("批量 upsert {} 条日线", count);
    Ok(count)
}

pub async fn get_daily_date_range(
    pool: &PgPool,
    symbol: &str,
) -> Result<(Option<NaiveDate>, Option<NaiveDate>), sqlx::Error> {
    let row: Option<(Option<NaiveDate>, Option<NaiveDate>)> = sqlx::query_as(
        "SELECT MIN(trade_date), MAX(trade_date) FROM market_stock_daily_bar WHERE symbol = $1",
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or((None, None)))
}

// ─── market_trade_calendar ───────────────────────────────────────

pub async fn upsert_trade_calendar(
    pool: &PgPool,
    cal: &MarketTradeCalendar,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_trade_calendar (exchange, trade_date, is_open, pre_trade_date)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (exchange, trade_date) DO UPDATE SET
             is_open = EXCLUDED.is_open,
             pre_trade_date = EXCLUDED.pre_trade_date"#,
    )
    .bind(&cal.exchange)
    .bind(cal.trade_date)
    .bind(cal.is_open)
    .bind(cal.pre_trade_date)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn bulk_upsert_calendars(
    pool: &PgPool,
    cals: &[MarketTradeCalendar],
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for cal in cals {
        upsert_trade_calendar(pool, cal).await?;
        count += 1;
    }
    info!("批量 upsert {} 条交易日历", count);
    Ok(count)
}

// ─── market_adjustment_factor ────────────────────────────────────

pub async fn upsert_adj_factors_batch(
    pool: &PgPool,
    factors: &[MarketAdjustmentFactor],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for f in factors {
        sqlx::query(
            r#"INSERT INTO market_adjustment_factor (symbol, trade_date, adj_factor, source, data_version_id)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (symbol, trade_date) DO UPDATE SET
                 adj_factor = EXCLUDED.adj_factor,
                 source = EXCLUDED.source,
                 data_version_id = EXCLUDED.data_version_id"#,
        )
        .bind(&f.symbol)
        .bind(f.trade_date)
        .bind(f.adj_factor)
        .bind(source)
        .bind(data_version_id)
        .execute(pool)
        .await?;
        count += 1;
    }
    info!("批量 upsert {} 条复权因子", count);
    Ok(count)
}

// ─── data_version ─────────────────────────────────────────────────

/// 创建数据版本记录（同步开始前调用）
pub async fn create_data_version(
    pool: &PgPool,
    dv_id: &str,
    name: &str,
    source: &str,
    tables: &[&str],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO data_version (data_version_id, name, source, start_date, end_date, tables, snapshot_hash)
           VALUES ($1, $2, $3, $4, $5, $6, '')
           ON CONFLICT (data_version_id) DO NOTHING"#,
    )
    .bind(dv_id)
    .bind(name)
    .bind(source)
    .bind(start_date)
    .bind(end_date)
    .bind(tables)
    .execute(pool)
    .await?;
    Ok(())
}

// ─── data_sync_task ──────────────────────────────────────────────

pub async fn create_sync_task(
    pool: &PgPool,
    task_id: &str,
    task_type: &str,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO data_sync_task (task_id, task_type, source, status) VALUES ($1, $2, 'tushare', $3)",
    )
    .bind(task_id)
    .bind(task_type)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_sync_task(
    pool: &PgPool,
    task_id: &str,
    status: &str,
    total: i32,
    success: i32,
    failed: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE data_sync_task
           SET status = $2, total_count = $3, success_count = $4, failed_count = $5,
               progress = CASE WHEN $3 > 0 THEN ($4 * 100 / $3) ELSE 0 END,
               completed_at = CASE WHEN $2 IN ('completed','partial','failed') THEN now() ELSE completed_at END
           WHERE task_id = $1"#,
    )
    .bind(task_id)
    .bind(status)
    .bind(total)
    .bind(success)
    .bind(failed)
    .execute(pool)
    .await?;
    Ok(())
}
