//! 数据同步服务 — Tushare → 标准化 → PostgreSQL

use chrono::{Datelike, Duration, NaiveDate};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;
use std::future::Future;
use std::time::Duration as StdDuration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::model::entities::{
    MarketAdjustmentFactor, MarketIndexDailyBar, MarketStock, MarketStockCashflow,
    MarketStockDailyBar, MarketStockDailyBasic, MarketStockDisclosureDate, MarketStockDividend,
    MarketStockExpress, MarketStockForecast, MarketStockMoneyflow, MarketStockRepurchase,
    MarketTradeCalendar,
};
use crate::repository;
use crate::tushare::client::TushareClient;

// ─── JSON helpers ────────────────────────────────────────────────
use serde_json::Map;

fn get_str(m: &Map<String, Value>, key: &str) -> String {
    m.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}
fn get_opt_str(m: &Map<String, Value>, key: &str) -> Option<String> {
    m.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}
fn get_f64(m: &Map<String, Value>, key: &str) -> Option<f64> {
    m.get(key).and_then(|v| v.as_f64())
}
fn get_i64(m: &Map<String, Value>, key: &str) -> Option<i64> {
    m.get(key).and_then(|v| v.as_i64())
}
fn to_decimal(v: Option<f64>) -> Decimal {
    v.and_then(|v| Decimal::from_f64_retain(v))
        .unwrap_or_default()
}
fn to_opt_decimal(v: Option<f64>) -> Option<Decimal> {
    v.and_then(Decimal::from_f64_retain)
}
fn to_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y%m%d").ok()
}

fn quarter_end_dates_in_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    if start > end {
        return Vec::new();
    }

    let mut periods = Vec::new();
    for year in start.year()..=end.year() {
        for (month, day) in [(3, 31), (6, 30), (9, 30), (12, 31)] {
            if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
                if date >= start && date <= end {
                    periods.push(date);
                }
            }
        }
    }
    periods
}

fn raw_payload(item: &Map<String, Value>) -> Value {
    Value::Object(item.clone())
}

fn list_or_stock_symbols<'a>(symbols: &'a [String], fallback: &'a [String]) -> &'a [String] {
    if symbols.is_empty() {
        fallback
    } else {
        symbols
    }
}

fn date_in_range(date: NaiveDate, start: NaiveDate, end: NaiveDate) -> bool {
    date >= start && date <= end
}

fn financial_sync_attempt_window() -> (NaiveDate, NaiveDate) {
    (
        NaiveDate::from_ymd_opt(1900, 1, 1).expect("valid financial sync attempt start"),
        NaiveDate::from_ymd_opt(9999, 12, 31).expect("valid financial sync attempt end"),
    )
}

fn tushare_symbol_call_timeout() -> StdDuration {
    const DEFAULT_TIMEOUT_SECS: u64 = 20;
    const MAX_TIMEOUT_SECS: u64 = 120;
    let secs = std::env::var("PHASE7_TUSHARE_SYMBOL_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.clamp(1, MAX_TIMEOUT_SECS))
        .unwrap_or(DEFAULT_TIMEOUT_SECS);
    StdDuration::from_secs(secs)
}

async fn bounded_tushare_symbol_call<T, E, F>(
    source: &str,
    symbol: &str,
    timeout: StdDuration,
    future: F,
) -> Result<T, String>
where
    E: std::fmt::Display,
    F: Future<Output = Result<T, E>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!(
            "{} request timed out after {}s for {}",
            source,
            timeout.as_secs_f64(),
            symbol
        )),
    }
}

fn append_symbol_error(existing: Option<String>, message: String) -> String {
    match existing {
        Some(existing) => format!("{}; {}", existing, message),
        None => message,
    }
}

// ─── sync_stock_basic ────────────────────────────────────────────

pub async fn sync_stock_basic(
    pool: &PgPool,
    client: &TushareClient,
    data_version_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = if data_version_id.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        data_version_id.to_string()
    };
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "stock_basic",
        "tushare",
        None,
        None,
        None,
        "running",
        None,
    )
    .await?;
    let mut all = Vec::new();

    for ex in &["SSE", "SZSE"] {
        info!("拉取 {} 股票基本信息...", ex);
        match client.stock_basic(Some(ex), Some("L")).await {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    for item in &maps {
                        all.push(MarketStock {
                            symbol: get_str(item, "ts_code"),
                            name: get_str(item, "name"),
                            exchange: ex.to_string(),
                            market: get_opt_str(item, "market"),
                            industry: get_opt_str(item, "industry"),
                            list_status: {
                                let s = get_str(item, "list_status");
                                if s.is_empty() {
                                    "L".to_string()
                                } else {
                                    s
                                }
                            },
                            list_date: to_date(&get_str(item, "list_date")),
                            delist_date: to_date(&get_str(item, "delist_date")),
                            is_st: get_str(item, "is_st") == "1",
                            created_at: chrono::Utc::now(),
                            updated_at: chrono::Utc::now(),
                        });
                    }
                }
            }
            Err(e) => {
                error!("拉取 {} 失败: {}", ex, e);
                repository::update_sync_task(
                    pool,
                    &task_id,
                    "failed",
                    all.len() as i32,
                    all.len() as i32,
                    1,
                )
                .await?;
                return Err(e.into());
            }
        }
    }

    let total = all.len();
    info!("共拉取 {} 只股票", total);
    let count = repository::upsert_stocks_batch(pool, &all).await?;
    repository::update_sync_task(pool, &task_id, "completed", total as i32, count as i32, 0)
        .await?;
    info!("stock_basic 同步完成: {}/{}", count, total);
    Ok(count)
}

// ─── sync_daily_bars ─────────────────────────────────────────────

pub async fn sync_daily_bars(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "daily",
        "tushare",
        Some(symbols),
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "daily bars sync",
        "tushare",
        &["market_stock_daily_bar"],
        s,
        e,
    )
    .await?;

    let total = symbols.len();
    let mut synced: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut failed: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut total_rows = 0usize;

    // Iterate by month — Tushare batch returns one day when ts_codes are specified.
    // Monthly chunks avoid the single-day limitation while staying within API limits.
    let months = months_in_range(s, e);
    info!("Syncing {} symbols across {} months", total, months.len());

    for (m_start, m_end) in &months {
        let sd = m_start.format("%Y%m%d").to_string();
        let ed = m_end.format("%Y%m%d").to_string();

        for chunk in symbols.chunks(500) {
            let chunk_vec: Vec<String> = chunk.to_vec();
            let mut offset = 0usize;
            let page_limit = 4000usize; // Tushare daily API page size

            // Paginated fetch: loop with offset until last page
            'page: loop {
                let mut retries = 0;
                let mut row_count = 0usize;
                while retries <= 3 {
                    match client
                        .daily_batch(
                            &chunk_vec,
                            Some(&sd),
                            Some(&ed),
                            Some(page_limit),
                            Some(offset),
                        )
                        .await
                    {
                        Ok(resp) => {
                            if let Some(data) = resp.data {
                                let maps = data.to_maps();
                                row_count = maps.len();

                                if row_count > 0 {
                                    let bars: Vec<MarketStockDailyBar> = maps
                                        .iter()
                                        .filter_map(|item| {
                                            let ts_code = get_str(item, "ts_code");
                                            let symbol = ts_code.to_string();
                                            Some(MarketStockDailyBar {
                                                symbol,
                                                trade_date: to_date(&get_str(item, "trade_date"))?,
                                                open: to_decimal(get_f64(item, "open")),
                                                high: to_decimal(get_f64(item, "high")),
                                                low: to_decimal(get_f64(item, "low")),
                                                close: to_decimal(get_f64(item, "close")),
                                                pre_close: get_f64(item, "pre_close")
                                                    .and_then(|v| Decimal::from_f64_retain(v)),
                                                change_pct: get_f64(item, "pct_chg").and_then(
                                                    |v| Decimal::from_f64_retain(v / 100.0),
                                                ),
                                                volume: to_decimal(get_f64(item, "vol")),
                                                amount: to_decimal(get_f64(item, "amount")),
                                            })
                                        })
                                        .collect();

                                    if !bars.is_empty() {
                                        total_rows += bars.len();
                                        repository::upsert_daily_bars_batch(
                                            pool, &bars, dv_id, "tushare",
                                        )
                                        .await?;
                                        for s in chunk {
                                            synced.insert(s.clone());
                                        }
                                    }
                                }
                            }
                            break; // page succeeded
                        }
                        Err(e) => {
                            retries += 1;
                            if retries > 3 {
                                warn!(
                                    "Batch daily failed after 3 retries for chunk offset {}: {}",
                                    offset, e
                                );
                                for s in chunk {
                                    failed.insert(s.clone());
                                }
                            } else {
                                let delay = std::time::Duration::from_millis(
                                    2000 * 2u64.pow(retries as u32 - 1),
                                );
                                warn!("Batch daily retry {}/3 after {:?}: {}", retries, delay, e);
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }

                if row_count < page_limit {
                    break 'page; // last page, exit pagination loop
                }
                offset += page_limit;
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        }
        if total_rows % 50000 == 0 {
            info!("Daily sync: {} rows", total_rows);
        }
    }

    // Remove failed symbols from synced set
    for s in &failed {
        synced.remove(s);
    }
    let ok = synced.len() as i32;
    let fail = failed.len() as i32;

    repository::update_sync_task(
        pool,
        &task_id,
        if fail > 0 { "partial" } else { "completed" },
        total as i32,
        ok,
        fail,
    )
    .await?;
    info!(
        "Daily sync done: {} rows, ok={}/{}, fail={}",
        total_rows, ok, total, fail
    );
    Ok(total_rows)
}

/// Generate (start, end) tuples for each month in a date range
fn months_in_range(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    use chrono::Datelike;
    let mut result = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let year = cursor.year();
        let month = cursor.month();
        let month_end =
            NaiveDate::from_ymd_opt(year, month, days_in_month(year, month)).unwrap_or(cursor);
        let actual_end = month_end.min(end);
        result.push((cursor, actual_end));
        // Move to first day of next month
        cursor = NaiveDate::from_ymd_opt(
            if month == 12 { year + 1 } else { year },
            if month == 12 { 1 } else { month + 1 },
            1,
        )
        .unwrap_or(end + chrono::Duration::days(1));
    }
    result
}

/// Split a date range into yearly chunks. Each chunk is at most one calendar year.
/// This reduces Tushare API calls by ~12× compared to monthly chunks.
fn years_in_range(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    use chrono::Datelike;
    let mut result = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let year = cursor.year();
        // End of this year or `end`, whichever is earlier
        let year_end = NaiveDate::from_ymd_opt(year, 12, 31).unwrap_or(cursor);
        let actual_end = year_end.min(end);
        result.push((cursor, actual_end));
        // Move to first day of next year
        cursor = NaiveDate::from_ymd_opt(year + 1, 1, 1)
            .unwrap_or(end + chrono::Duration::days(1));
    }
    result
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

// ─── sync_daily_basic ────────────────────────────────────────────

fn daily_basic_row_from_map(item: &Map<String, Value>) -> Option<MarketStockDailyBasic> {
    Some(MarketStockDailyBasic {
        symbol: get_str(item, "ts_code"),
        trade_date: to_date(&get_str(item, "trade_date"))?,
        pe_ttm: to_opt_decimal(get_f64(item, "pe_ttm")),
        pb: to_opt_decimal(get_f64(item, "pb")),
        ps_ttm: to_opt_decimal(get_f64(item, "ps_ttm")),
        dv_ttm: to_opt_decimal(get_f64(item, "dv_ttm")),
        total_mv: to_opt_decimal(get_f64(item, "total_mv")),
        circ_mv: to_opt_decimal(get_f64(item, "circ_mv")),
    })
}

pub async fn sync_daily_basic(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "daily_basic",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "daily basic valuation sync",
        "tushare",
        &["market_stock_daily_basic"],
        s,
        e,
    )
    .await?;

    let page_limit = 6000usize;
    let mut total_rows = 0usize;
    let mut ok = 0usize;
    let mut failed = 0usize;

    if symbols.is_empty() {
        let months = months_in_range(s, e);
        for (m_start, m_end) in &months {
            let sd = m_start.format("%Y%m%d").to_string();
            let ed = m_end.format("%Y%m%d").to_string();
            let mut offset = 0usize;

            loop {
                match client
                    .daily_basic(
                        None,
                        None,
                        Some(&sd),
                        Some(&ed),
                        Some(page_limit),
                        Some(offset),
                    )
                    .await
                {
                    Ok(resp) => {
                        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                        let row_count = maps.len();
                        let rows: Vec<MarketStockDailyBasic> =
                            maps.iter().filter_map(daily_basic_row_from_map).collect();
                        if !rows.is_empty() {
                            total_rows += rows.len();
                            repository::upsert_daily_basic_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                        }
                        if row_count < page_limit {
                            break;
                        }
                        offset += page_limit;
                    }
                    Err(error) => {
                        warn!("daily_basic {}..{} failed: {}", sd, ed, error);
                        let mut day = *m_start;
                        let mut fallback_failed = false;
                        while day <= *m_end {
                            let trade_date = day.format("%Y%m%d").to_string();
                            let mut offset = 0usize;
                            loop {
                                match client
                                    .daily_basic(
                                        None,
                                        Some(&trade_date),
                                        None,
                                        None,
                                        Some(page_limit),
                                        Some(offset),
                                    )
                                    .await
                                {
                                    Ok(resp) => {
                                        let maps = resp
                                            .data
                                            .map(|data| data.to_maps())
                                            .unwrap_or_default();
                                        let row_count = maps.len();
                                        let rows: Vec<MarketStockDailyBasic> = maps
                                            .iter()
                                            .filter_map(daily_basic_row_from_map)
                                            .collect();
                                        if !rows.is_empty() {
                                            total_rows += rows.len();
                                            repository::upsert_daily_basic_batch(
                                                pool, &rows, dv_id, "tushare",
                                            )
                                            .await?;
                                        }
                                        if row_count < page_limit {
                                            break;
                                        }
                                        offset += page_limit;
                                    }
                                    Err(day_error) => {
                                        warn!("daily_basic {} failed: {}", trade_date, day_error);
                                        failed += 1;
                                        fallback_failed = true;
                                        break;
                                    }
                                }
                            }
                            day += Duration::days(1);
                        }
                        if !fallback_failed {
                            info!("daily_basic {}..{} recovered by daily fallback", sd, ed);
                        }
                        break;
                    }
                }
            }
            ok += 1;
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                months.len() as i32,
                ok as i32,
                failed as i32,
            )
            .await?;
            if total_rows % 500_000 < page_limit {
                info!("daily_basic range sync: {} rows", total_rows);
            }
            if ok % 12 == 0 {
                repository::update_sync_task(
                    pool,
                    &task_id,
                    "running",
                    months.len() as i32,
                    ok as i32,
                    failed as i32,
                )
                .await?;
            }
        }

        repository::update_sync_task(
            pool,
            &task_id,
            if failed > 0 { "partial" } else { "completed" },
            months.len() as i32,
            ok as i32,
            failed as i32,
        )
        .await?;
    } else {
        for symbol in symbols {
            let mut offset = 0usize;
            let mut symbol_failed = false;
            loop {
                match client
                    .daily_basic(
                        Some(symbol),
                        None,
                        Some(start),
                        Some(end),
                        Some(page_limit),
                        Some(offset),
                    )
                    .await
                {
                    Ok(resp) => {
                        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                        let row_count = maps.len();
                        let rows: Vec<MarketStockDailyBasic> =
                            maps.iter().filter_map(daily_basic_row_from_map).collect();
                        if !rows.is_empty() {
                            total_rows += rows.len();
                            repository::upsert_daily_basic_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                        }
                        if row_count < page_limit {
                            break;
                        }
                        offset += page_limit;
                    }
                    Err(error) => {
                        warn!("{} daily_basic failed: {}", symbol, error);
                        failed += 1;
                        symbol_failed = true;
                        break;
                    }
                }
            }
            if !symbol_failed {
                ok += 1;
            }
            if ok % 100 == 0 {
                repository::update_sync_task(
                    pool,
                    &task_id,
                    "running",
                    symbols.len() as i32,
                    ok as i32,
                    failed as i32,
                )
                .await?;
            }
        }

        repository::update_sync_task(
            pool,
            &task_id,
            if failed > 0 { "partial" } else { "completed" },
            symbols.len() as i32,
            ok as i32,
            failed as i32,
        )
        .await?;
    }

    info!(
        "daily_basic 同步完成: rows={}, ok={}, failed={}",
        total_rows, ok, failed
    );
    Ok(total_rows)
}

// ─── sync_fund_basic (ETF/LOF 基本信息) ─────────────────────────

/// 同步基金/ETF 基本信息到 market_stock 表。
/// 从 Tushare fund_basic 接口拉取名称、类型、管理人。
pub async fn sync_fund_basic(
    pool: &PgPool,
    client: &TushareClient,
) -> Result<usize, String> {
    // E: 交易所 ETF, L: LOF
    let markets = ["E", "L"];
    let mut total = 0usize;

    for market in &markets {
        let resp = client
            .fund_basic(Some(market))
            .await
            .map_err(|e| format!("fund_basic {}: {}", market, e))?;

        let maps = resp.data.map(|d| d.to_maps()).unwrap_or_default();
        for item in &maps {
            let ts_code = item["ts_code"].as_str().unwrap_or("");
            let name = item["name"].as_str().unwrap_or(ts_code);
            let fund_type = item["fund_type"].as_str().unwrap_or("");
            let status = item["status"].as_str().unwrap_or("");

            if ts_code.is_empty() || status == "D" { continue; } // skip delisted

            sqlx::query(
                "INSERT INTO market_stock (symbol, name, exchange, list_status, is_st)
                 VALUES ($1, $2, 'SSE', 'L', false)
                 ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name",
            )
            .bind(ts_code)
            .bind(format!("{} ({})", name, fund_type))
            .execute(pool)
            .await
            .map_err(|e| format!("insert failed: {}", e))?;
            total += 1;
        }
    }

    Ok(total)
}

// ─── sync_fund_daily (ETF/LOF 基金日线) ──────────────────────────

pub async fn sync_fund_daily(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool, &task_id, "fund_daily", "tushare",
        Some(symbols), Some(s), Some(e), "running", None,
    )
    .await?;

    repository::create_data_version(
        pool, dv_id, "fund daily bars sync", "tushare",
        &["market_stock_daily_bar"], s, e,
    )
    .await?;

    let mut total_rows = 0usize;
    // Use yearly chunks instead of monthly — reduces API calls ~12× (240→20 for 2006-2026)
    let years = years_in_range(s, e);
    info!("Syncing {} fund symbols across {} yearly chunks ({} to {})", symbols.len(), years.len(), start, end);

    // Rate-limit tracking: max 4000 calls/hr, target ~3000 calls/hr = 50 calls/min
    let mut calls_this_minute = 0u32;
    let max_calls_per_minute = 45u32; // conservative: 45 × 60 = 2700/hr, well under 4000 limit

    for (y_start, y_end) in &years {
        let sd = y_start.format("%Y%m%d").to_string();
        let ed = y_end.format("%Y%m%d").to_string();

        for symbol in symbols {
            // Rate-limit: pause 1.5s if we've made too many calls this minute
            if calls_this_minute >= max_calls_per_minute {
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
                calls_this_minute = 0;
            }

            match client
                .fund_daily(Some(symbol), None, Some(&sd), Some(&ed))
                .await
            {
                Ok(resp) => {
                    calls_this_minute += 1;
                    if let Some(data) = resp.data {
                        let maps = data.to_maps();
                        if maps.is_empty() {
                            continue;
                        }

                        let bars: Vec<MarketStockDailyBar> = maps
                            .iter()
                            .filter_map(|item| {
                                let ts_code = get_str(item, "ts_code");
                                Some(MarketStockDailyBar {
                                    symbol: ts_code.to_string(),
                                    trade_date: to_date(&get_str(item, "trade_date"))?,
                                    open: to_decimal(get_f64(item, "open")),
                                    high: to_decimal(get_f64(item, "high")),
                                    low: to_decimal(get_f64(item, "low")),
                                    close: to_decimal(get_f64(item, "close")),
                                    pre_close: get_f64(item, "pre_close")
                                        .and_then(|v| Decimal::from_f64_retain(v)),
                                    change_pct: get_f64(item, "pct_chg").and_then(
                                        |v| Decimal::from_f64_retain(v / 100.0),
                                    ),
                                    volume: to_decimal(get_f64(item, "vol")),
                                    amount: to_decimal(get_f64(item, "amount")),
                                })
                            })
                            .collect();

                        if !bars.is_empty() {
                            total_rows += bars.len();
                            repository::upsert_daily_bars_batch(
                                pool, &bars, dv_id, "tushare",
                            )
                            .await?;
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    // Rate-limit hit: wait and retry once
                    if err_str.contains("40203") || err_str.contains("4000") {
                        warn!("fund_daily rate-limit, waiting 5s for {}: {}", symbol, err_str);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        calls_this_minute = 0;
                        // Retry once
                        match client
                            .fund_daily(Some(symbol), None, Some(&sd), Some(&ed))
                            .await
                        {
                            Ok(resp) => {
                                calls_this_minute += 1;
                                if let Some(data) = resp.data {
                                    let maps = data.to_maps();
                                    if maps.is_empty() { continue; }
                                    let bars: Vec<MarketStockDailyBar> = maps
                                        .iter()
                                        .filter_map(|item| {
                                            let ts_code = get_str(item, "ts_code");
                                            Some(MarketStockDailyBar {
                                                symbol: ts_code.to_string(),
                                                trade_date: to_date(&get_str(item, "trade_date"))?,
                                                open: to_decimal(get_f64(item, "open")),
                                                high: to_decimal(get_f64(item, "high")),
                                                low: to_decimal(get_f64(item, "low")),
                                                close: to_decimal(get_f64(item, "close")),
                                                pre_close: get_f64(item, "pre_close")
                                                    .and_then(|v| Decimal::from_f64_retain(v)),
                                                change_pct: get_f64(item, "pct_chg").and_then(
                                                    |v| Decimal::from_f64_retain(v / 100.0),
                                                ),
                                                volume: to_decimal(get_f64(item, "vol")),
                                                amount: to_decimal(get_f64(item, "amount")),
                                            })
                                        })
                                        .collect();
                                    if !bars.is_empty() {
                                        total_rows += bars.len();
                                        repository::upsert_daily_bars_batch(pool, &bars, dv_id, "tushare").await?;
                                    }
                                }
                            }
                            Err(e2) => {
                                warn!("fund_daily retry also failed for {} in {}-{}: {}", symbol, sd, ed, e2);
                            }
                        }
                    } else {
                        warn!("fund_daily failed for {} in {}-{}: {}", symbol, sd, ed, e);
                    }
                }
            }
        }
    }

    repository::update_sync_task(
        pool, &task_id, "completed",
        total_rows as i32, total_rows as i32, 0,
    )
    .await?;
    info!("fund_daily 同步完成: {} symbols, {} rows, {} yearly chunks", symbols.len(), total_rows, years.len());
    Ok(total_rows)
}

// ─── sync_moneyflow ─────────────────────────────────────────────

fn moneyflow_row_from_map(item: &Map<String, Value>) -> Option<MarketStockMoneyflow> {
    Some(MarketStockMoneyflow {
        symbol: get_str(item, "ts_code"),
        trade_date: to_date(&get_str(item, "trade_date"))?,
        buy_sm_vol: to_opt_decimal(get_f64(item, "buy_sm_vol")),
        buy_sm_amount: to_opt_decimal(get_f64(item, "buy_sm_amount")),
        sell_sm_vol: to_opt_decimal(get_f64(item, "sell_sm_vol")),
        sell_sm_amount: to_opt_decimal(get_f64(item, "sell_sm_amount")),
        buy_md_vol: to_opt_decimal(get_f64(item, "buy_md_vol")),
        buy_md_amount: to_opt_decimal(get_f64(item, "buy_md_amount")),
        sell_md_vol: to_opt_decimal(get_f64(item, "sell_md_vol")),
        sell_md_amount: to_opt_decimal(get_f64(item, "sell_md_amount")),
        buy_lg_vol: to_opt_decimal(get_f64(item, "buy_lg_vol")),
        buy_lg_amount: to_opt_decimal(get_f64(item, "buy_lg_amount")),
        sell_lg_vol: to_opt_decimal(get_f64(item, "sell_lg_vol")),
        sell_lg_amount: to_opt_decimal(get_f64(item, "sell_lg_amount")),
        buy_elg_vol: to_opt_decimal(get_f64(item, "buy_elg_vol")),
        buy_elg_amount: to_opt_decimal(get_f64(item, "buy_elg_amount")),
        sell_elg_vol: to_opt_decimal(get_f64(item, "sell_elg_vol")),
        sell_elg_amount: to_opt_decimal(get_f64(item, "sell_elg_amount")),
        net_mf_vol: to_opt_decimal(get_f64(item, "net_mf_vol")),
        net_mf_amount: to_opt_decimal(get_f64(item, "net_mf_amount")),
    })
}

pub async fn sync_moneyflow(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "moneyflow",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "daily moneyflow sync",
        "tushare",
        &["market_stock_moneyflow"],
        s,
        e,
    )
    .await?;

    let page_limit = 6000usize;
    let mut total_rows = 0usize;
    let mut ok = 0usize;
    let mut failed = 0usize;

    if symbols.is_empty() {
        let months = months_in_range(s, e);
        for (m_start, m_end) in &months {
            let sd = m_start.format("%Y%m%d").to_string();
            let ed = m_end.format("%Y%m%d").to_string();
            let mut offset = 0usize;

            loop {
                match client
                    .moneyflow(
                        None,
                        None,
                        Some(&sd),
                        Some(&ed),
                        Some(page_limit),
                        Some(offset),
                    )
                    .await
                {
                    Ok(resp) => {
                        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                        let row_count = maps.len();
                        let rows: Vec<MarketStockMoneyflow> =
                            maps.iter().filter_map(moneyflow_row_from_map).collect();
                        if !rows.is_empty() {
                            total_rows += rows.len();
                            repository::upsert_moneyflow_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                        }
                        if row_count < page_limit {
                            break;
                        }
                        offset += page_limit;
                    }
                    Err(error) => {
                        warn!("moneyflow {}..{} failed: {}", sd, ed, error);
                        let mut day = *m_start;
                        let mut fallback_failed = false;
                        while day <= *m_end {
                            let trade_date = day.format("%Y%m%d").to_string();
                            let mut offset = 0usize;
                            loop {
                                match client
                                    .moneyflow(
                                        None,
                                        Some(&trade_date),
                                        None,
                                        None,
                                        Some(page_limit),
                                        Some(offset),
                                    )
                                    .await
                                {
                                    Ok(resp) => {
                                        let maps = resp
                                            .data
                                            .map(|data| data.to_maps())
                                            .unwrap_or_default();
                                        let row_count = maps.len();
                                        let rows: Vec<MarketStockMoneyflow> = maps
                                            .iter()
                                            .filter_map(moneyflow_row_from_map)
                                            .collect();
                                        if !rows.is_empty() {
                                            total_rows += rows.len();
                                            repository::upsert_moneyflow_batch(
                                                pool, &rows, dv_id, "tushare",
                                            )
                                            .await?;
                                        }
                                        if row_count < page_limit {
                                            break;
                                        }
                                        offset += page_limit;
                                    }
                                    Err(day_error) => {
                                        warn!("moneyflow {} failed: {}", trade_date, day_error);
                                        failed += 1;
                                        fallback_failed = true;
                                        break;
                                    }
                                }
                            }
                            day += Duration::days(1);
                        }
                        if !fallback_failed {
                            info!("moneyflow {}..{} recovered by daily fallback", sd, ed);
                        }
                        break;
                    }
                }
            }
            ok += 1;
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                months.len() as i32,
                ok as i32,
                failed as i32,
            )
            .await?;
            if total_rows % 500_000 < page_limit {
                info!("moneyflow range sync: {} rows", total_rows);
            }
            if ok % 12 == 0 {
                repository::update_sync_task(
                    pool,
                    &task_id,
                    "running",
                    months.len() as i32,
                    ok as i32,
                    failed as i32,
                )
                .await?;
            }
        }

        repository::update_sync_task(
            pool,
            &task_id,
            if failed > 0 { "partial" } else { "completed" },
            months.len() as i32,
            ok as i32,
            failed as i32,
        )
        .await?;
    } else {
        for symbol in symbols {
            let mut offset = 0usize;
            let mut symbol_failed = false;
            loop {
                match client
                    .moneyflow(
                        Some(symbol),
                        None,
                        Some(start),
                        Some(end),
                        Some(page_limit),
                        Some(offset),
                    )
                    .await
                {
                    Ok(resp) => {
                        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                        let row_count = maps.len();
                        let rows: Vec<MarketStockMoneyflow> =
                            maps.iter().filter_map(moneyflow_row_from_map).collect();
                        if !rows.is_empty() {
                            total_rows += rows.len();
                            repository::upsert_moneyflow_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                        }
                        if row_count < page_limit {
                            break;
                        }
                        offset += page_limit;
                    }
                    Err(error) => {
                        warn!("{} moneyflow failed: {}", symbol, error);
                        failed += 1;
                        symbol_failed = true;
                        break;
                    }
                }
            }
            if !symbol_failed {
                ok += 1;
            }
            if ok % 100 == 0 {
                repository::update_sync_task(
                    pool,
                    &task_id,
                    "running",
                    symbols.len() as i32,
                    ok as i32,
                    failed as i32,
                )
                .await?;
            }
        }

        repository::update_sync_task(
            pool,
            &task_id,
            if failed > 0 { "partial" } else { "completed" },
            symbols.len() as i32,
            ok as i32,
            failed as i32,
        )
        .await?;
    }

    info!(
        "moneyflow 同步完成: rows={}, ok={}, failed={}",
        total_rows, ok, failed
    );
    Ok(total_rows)
}

// ─── sync_moneyflow_hsgt (沪深港通北向资金) ─────────────────────

pub async fn sync_moneyflow_hsgt(
    pool: &PgPool,
    client: &TushareClient,
    start: &str,
    end: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    let mut total = 0usize;

    // 按季度分批查询，避免 Tushare 单次分页限制（~300 行/页）
    let mut batch_start = s;
    while batch_start <= e {
        let batch_end = (batch_start + chrono::Duration::days(92)).min(e);
        let start_str = batch_start.format("%Y%m%d").to_string();
        let end_str = batch_end.format("%Y%m%d").to_string();

        let resp = client.moneyflow_hsgt(None, Some(&start_str), Some(&end_str)).await?;
        if let Some(data) = resp.data {
            for item in data.items {
                let date = item.first().and_then(|v| v.as_str()).unwrap_or("");
                if date.is_empty() {
                    continue;
                }
                let nf = item.get(1).and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let sf = item.get(2).and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let nb = item.get(3).and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let sb = item.get(4).and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let d = NaiveDate::parse_from_str(date, "%Y%m%d")?;
                sqlx::query(
                    "INSERT INTO market_moneyflow_hsgt (trade_date, north_flow, south_flow, north_balance, south_balance)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (trade_date) DO UPDATE SET north_flow=EXCLUDED.north_flow, south_flow=EXCLUDED.south_flow",
                )
                .bind(d).bind(nf).bind(sf).bind(nb).bind(sb)
                .execute(pool).await?;
                total += 1;
            }
        }
        info!(?batch_start, ?batch_end, batch_rows = total, "HSGT 同步进度");
        batch_start = batch_end + chrono::Duration::days(1);
    }
    Ok(total)
}

// ─── sync_margin (融资融券) ────────────────────────────────────────

pub async fn sync_margin(
    pool: &PgPool,
    client: &TushareClient,
    start: &str,
    end: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    let mut total = 0usize;

    // 按季度分批查询
    let mut batch_start = s;
    while batch_start <= e {
        let batch_end = (batch_start + chrono::Duration::days(92)).min(e);
        let start_str = batch_start.format("%Y%m%d").to_string();
        let end_str = batch_end.format("%Y%m%d").to_string();

        let resp = client.margin(None, Some(&start_str), Some(&end_str)).await?;
        if let Some(data) = resp.data {
            for item in data.items {
                let date = item.first().and_then(|v| v.as_str()).unwrap_or("");
                if date.is_empty() {
                    continue;
                }
                let exchange = item.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let rzye = item.get(2).and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))).unwrap_or(0.0);
                let rqye = item.get(3).and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))).unwrap_or(0.0);
                let rzrqye = item.get(4).and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))).unwrap_or(0.0);
                let d = NaiveDate::parse_from_str(date, "%Y%m%d")?;
                sqlx::query(
                    "INSERT INTO market_margin (trade_date, exchange, rzye, rqye, rzrqye)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (trade_date, exchange) DO UPDATE SET rzye=EXCLUDED.rzye, rqye=EXCLUDED.rqye, rzrqye=EXCLUDED.rzrqye",
                )
                .bind(d).bind(exchange).bind(rzye).bind(rqye).bind(rzrqye)
                .execute(pool).await?;
                total += 1;
            }
        }
        info!(?batch_start, ?batch_end, batch_rows = total, "Margin 同步进度");
        batch_start = batch_end + chrono::Duration::days(1);
    }
    Ok(total)
}

// ─── sync_trade_calendar ─────────────────────────────────────────

pub async fn sync_trade_calendar(
    pool: &PgPool,
    client: &TushareClient,
    exchange: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = Uuid::new_v4().to_string();
    sync_trade_calendar_with_task(pool, client, exchange, &task_id).await
}

pub async fn sync_trade_calendar_with_task(
    pool: &PgPool,
    client: &TushareClient,
    exchange: &str,
    task_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    repository::create_sync_task_with_context(
        pool,
        task_id,
        "trade_cal",
        "tushare",
        None,
        None,
        None,
        "running",
        None,
    )
    .await?;
    let resp = client.trade_cal(exchange, None, None).await?;
    let mut cals = Vec::new();

    if let Some(data) = resp.data {
        let maps = data.to_maps();
        for item in &maps {
            cals.push(MarketTradeCalendar {
                exchange: exchange.to_string(),
                trade_date: to_date(&get_str(item, "cal_date"))
                    .unwrap_or(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
                is_open: get_i64(item, "is_open").map(|v| v == 1).unwrap_or(false),
                pre_trade_date: {
                    let s = get_str(item, "pretrade_date");
                    if s.is_empty() {
                        None
                    } else {
                        to_date(&s)
                    }
                },
            });
        }
    }

    let count = repository::bulk_upsert_calendars(pool, &cals).await?;
    repository::update_sync_task(pool, task_id, "completed", count as i32, count as i32, 0).await?;
    info!("交易日历同步完成: {} 条", count);
    Ok(count)
}

// ─── sync_adj_factor ─────────────────────────────────────────────

/// 同步复权因子（逐只拉取）
pub async fn sync_adj_factor(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "adj_factor",
        "tushare",
        Some(symbols),
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "adj factor sync",
        "tushare",
        &["market_adjustment_factor"],
        s,
        e,
    )
    .await?;

    let total = symbols.len();
    let mut ok = 0usize;
    let mut fail = 0usize;

    for sym in symbols {
        match client.adj_factor(sym, Some(start), Some(end)).await {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    let factors: Vec<MarketAdjustmentFactor> = maps
                        .iter()
                        .filter_map(|item| {
                            Some(MarketAdjustmentFactor {
                                symbol: sym.clone(),
                                trade_date: to_date(&get_str(item, "trade_date"))?,
                                adj_factor: to_decimal(get_f64(item, "adj_factor")),
                            })
                        })
                        .collect();

                    if !factors.is_empty() {
                        repository::upsert_adj_factors_batch(pool, &factors, dv_id, "tushare")
                            .await?;
                        ok += 1;
                    }
                }
                if ok % 200 == 0 {
                    info!("复权因子进度: {}/{}", ok, total);
                }
            }
            Err(e) => {
                warn!("{} 复权因子失败: {}", sym, e);
                fail += 1;
            }
        }
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if fail > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        fail as i32,
    )
    .await?;
    info!("复权因子同步完成: ok={}/{}, fail={}", ok, total, fail);
    Ok(ok)
}

// ─── sync_fund_adj ───────────────────────────────────────────────

/// 同步基金(ETF/LOF)复权因子 — 用 Tushare `fund_adj` 接口（股票 adj_factor 接口对基金无数据）。
/// 复权因子写入同一张 market_adjustment_factor 表，复权视图自动生效。
pub async fn sync_fund_adj(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool, &task_id, "fund_adj", "tushare",
        Some(symbols), Some(s), Some(e), "running", None,
    )
    .await?;
    repository::create_data_version(
        pool, dv_id, "fund adj sync", "tushare",
        &["market_adjustment_factor"], s, e,
    )
    .await?;

    let total = symbols.len();
    let mut ok = 0usize;
    let mut fail = 0usize;

    for sym in symbols {
        match client.fund_adj(sym, Some(start), Some(end)).await {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    let factors: Vec<MarketAdjustmentFactor> = maps
                        .iter()
                        .filter_map(|item| {
                            Some(MarketAdjustmentFactor {
                                symbol: sym.clone(),
                                trade_date: to_date(&get_str(item, "trade_date"))?,
                                adj_factor: to_decimal(get_f64(item, "adj_factor")),
                            })
                        })
                        .collect();
                    if !factors.is_empty() {
                        repository::upsert_adj_factors_batch(pool, &factors, dv_id, "tushare")
                            .await?;
                        ok += 1;
                    }
                }
            }
            Err(e) => {
                warn!("{} 基金复权因子失败: {}", sym, e);
                fail += 1;
            }
        }
    }

    repository::update_sync_task(
        pool, &task_id,
        if fail > 0 { "partial" } else { "completed" },
        total as i32, ok as i32, fail as i32,
    )
    .await?;
    info!("基金复权因子同步完成: ok={}/{}, fail={}", ok, total, fail);
    Ok(ok)
}

// ─── sync_index_daily ────────────────────────────────────────────

/// 同步指数日线行情
pub async fn sync_index_daily(
    pool: &PgPool,
    client: &TushareClient,
    index_codes: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "index_daily",
        "tushare",
        Some(index_codes),
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "index daily sync",
        "tushare",
        &["market_index_daily_bar"],
        s,
        e,
    )
    .await?;

    let total = index_codes.len();
    let mut ok = 0usize;
    let mut fail = 0usize;

    for idx in index_codes {
        match client.index_daily(idx, Some(start), Some(end)).await {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    let bars: Vec<MarketIndexDailyBar> = maps
                        .iter()
                        .filter_map(|item| {
                            Some(MarketIndexDailyBar {
                                symbol: idx.clone(),
                                trade_date: to_date(&get_str(item, "trade_date"))?,
                                open: to_decimal(get_f64(item, "open")),
                                high: to_decimal(get_f64(item, "high")),
                                low: to_decimal(get_f64(item, "low")),
                                close: to_decimal(get_f64(item, "close")),
                                pre_close: get_f64(item, "pre_close")
                                    .and_then(|v| Decimal::from_f64_retain(v)),
                                change_pct: get_f64(item, "pct_chg")
                                    .and_then(|v| Decimal::from_f64_retain(v / 100.0)),
                                volume: to_decimal(get_f64(item, "vol")),
                                amount: to_decimal(get_f64(item, "amount")),
                            })
                        })
                        .collect();

                    if !bars.is_empty() {
                        repository::upsert_index_daily_bars_batch(pool, &bars, dv_id, "tushare")
                            .await?;
                        ok += 1;
                    }
                }
            }
            Err(e) => {
                warn!("{} 指数日线失败: {}", idx, e);
                fail += 1;
            }
        }
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if fail > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        fail as i32,
    )
    .await?;
    info!("指数日线同步完成: ok={}/{}, fail={}", ok, total, fail);
    Ok(ok)
}

// ─── run_quality_check ───────────────────────────────────────────

/// 数据质量检查：缺失/重复/异常值
pub async fn run_quality_check(
    pool: &PgPool,
    symbols: &[String],
    start: &str,
    end: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use serde_json::json;

    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    let check_id = format!("qc-{}", Uuid::new_v4());

    // 获取预期交易日
    let expected_days: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_trade_calendar
         WHERE exchange = 'SSE' AND is_open = true
         AND trade_date >= $1 AND trade_date <= $2",
    )
    .bind(s)
    .bind(e)
    .fetch_one(pool)
    .await?;

    let sym_count = symbols.len() as i64;
    let expected_records = expected_days * sym_count;

    // 实际记录数
    let actual_records: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_stock_daily_bar
         WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3",
    )
    .bind(symbols)
    .bind(s)
    .bind(e)
    .fetch_one(pool)
    .await?;

    let missing = (expected_records - actual_records).max(0);

    // 重复检查
    let dup_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (
            SELECT symbol, trade_date, COUNT(*) as cnt
            FROM market_stock_daily_bar
            WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
            GROUP BY symbol, trade_date
            HAVING COUNT(*) > 1
        ) dups",
    )
    .bind(symbols)
    .bind(s)
    .bind(e)
    .fetch_one(pool)
    .await?;

    // 异常值检查 (open/high/low/close = 0)
    let outlier_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_stock_daily_bar
         WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
         AND (open <= 0 OR high <= 0 OR low <= 0 OR close <= 0)",
    )
    .bind(symbols)
    .bind(s)
    .bind(e)
    .fetch_one(pool)
    .await?;

    // 质量评分
    let score = if actual_records > 0 {
        let dup_penalty = dup_count as f64 / actual_records as f64 * 20.0;
        let outlier_penalty = outlier_count as f64 / actual_records as f64 * 30.0;
        let missing_penalty = missing as f64 / expected_records as f64 * 50.0;
        (100.0 - dup_penalty - outlier_penalty - missing_penalty).max(0.0)
    } else {
        0.0
    };

    let issues = json!({
        "expected_records": expected_records,
        "actual_records": actual_records,
        "missing": missing,
        "duplicates": dup_count,
        "outliers": outlier_count,
    });

    sqlx::query(
        r#"INSERT INTO data_quality_check
           (check_id, table_name, check_type, check_date,
            total_records, missing_count, duplicate_count, outlier_count,
            quality_score, issues)
           VALUES ($1, 'market_stock_daily_bar', 'completeness', $2,
                   $3, $4, $5, $6, $7, $8)"#,
    )
    .bind(&check_id)
    .bind(s)
    .bind(actual_records)
    .bind(missing)
    .bind(dup_count)
    .bind(outlier_count)
    .bind(rust_decimal::Decimal::from_f64_retain(score))
    .bind(&issues)
    .execute(pool)
    .await?;

    Ok(json!({
        "check_id": check_id,
        "symbols": sym_count,
        "expected_records": expected_records,
        "actual_records": actual_records,
        "missing": missing,
        "duplicates": dup_count,
        "outliers": outlier_count,
        "quality_score": score,
    }))
}

// ─── sync_financial_data ──────────────────────────────────────────

/// 同步财务数据（利润表 + 资产负债表 + 财务指标）
/// 按 ts_code 逐个拉取，避免单次请求数据量过大
pub async fn sync_financial_data(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let task_id = Uuid::new_v4().to_string();
    sync_financial_data_with_task(pool, client, symbols, &task_id).await
}

pub async fn sync_financial_data_with_task(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    task_id: &str,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    repository::create_sync_task_with_context(
        pool,
        task_id,
        "financial",
        "tushare",
        Some(symbols),
        None,
        None,
        "running",
        None,
    )
    .await?;

    let mut stmt_count = 0usize;
    let mut ind_count = 0usize;
    let mut ok = 0usize;
    let mut failed = 0usize;
    let total = symbols.len();
    let call_timeout = tushare_symbol_call_timeout();

    for (i, sym) in symbols.iter().enumerate() {
        if i % 100 == 0 {
            info!(
                "财务数据同步: {}/{} ({} {} {})",
                i, total, stmt_count, ind_count, sym
            );
        }
        let symbol_stmt_start = stmt_count;
        let symbol_ind_start = ind_count;
        let mut symbol_failed = false;
        let mut symbol_error: Option<String> = None;

        // 利润表
        match bounded_tushare_symbol_call(
            "financial/income",
            sym,
            call_timeout,
            client.income(sym, None, None),
        )
        .await
        {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    for item in &maps {
                        let end_date = to_date(&get_str(item, "end_date"));
                        let ann_date = to_date(&get_str(item, "ann_date"));
                        if end_date.is_none() {
                            continue;
                        }
                        let ed = end_date.unwrap();
                        let ad = ann_date.unwrap_or(ed);

                        for (field, val) in item.iter() {
                            if field == "ts_code"
                                || field == "end_date"
                                || field == "ann_date"
                                || field == "f_ann_date"
                                || field == "report_type"
                                || field == "comp_type"
                            {
                                continue;
                            }
                            if let Some(v) = val.as_f64() {
                                sqlx::query(
                                    "INSERT INTO market_financial_statement (ts_code, ann_date, end_date, statement_type, field_name, field_value)
                                 VALUES ($1, $2, $3, 'income', $4, $5)
                                 ON CONFLICT (ts_code, end_date, statement_type, field_name, report_type) DO UPDATE SET field_value = $5",
                                )
                                .bind(sym).bind(ad).bind(ed).bind(field).bind(v)
                                .execute(pool).await?;
                                stmt_count += 1;
                            }
                        }
                    }
                }
            }
            Err(error) => {
                warn!("{} financial income failed: {}", sym, error);
                symbol_failed = true;
                symbol_error = Some(error);
            }
        }

        // 资产负债表 — 只取关键字段减少数据量
        match bounded_tushare_symbol_call(
            "financial/balancesheet",
            sym,
            call_timeout,
            client.balancesheet(sym, None, None),
        )
        .await
        {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    const BS_FIELDS: &[&str] = &[
                        "total_assets",
                        "total_liab",
                        "total_hldr_eqy_inc_min_int",
                        "total_cur_assets",
                        "total_cur_liab",
                        "money_cap",
                        "inventories",
                        "accounts_receiv",
                        "fix_assets",
                    ];
                    for item in &maps {
                        let end_date = to_date(&get_str(item, "end_date"));
                        let ann_date = to_date(&get_str(item, "ann_date"));
                        if end_date.is_none() {
                            continue;
                        }
                        let ed = end_date.unwrap();
                        let ad = ann_date.unwrap_or(ed);

                        for field in BS_FIELDS {
                            if let Some(v) = item.get(*field).and_then(|v| v.as_f64()) {
                                sqlx::query(
                                    "INSERT INTO market_financial_statement (ts_code, ann_date, end_date, statement_type, field_name, field_value)
                                 VALUES ($1, $2, $3, 'balance', $4, $5)
                                 ON CONFLICT (ts_code, end_date, statement_type, field_name, report_type) DO UPDATE SET field_value = $5",
                                )
                                .bind(sym).bind(ad).bind(ed).bind(*field).bind(v)
                                .execute(pool).await?;
                                stmt_count += 1;
                            }
                        }
                    }
                }
            }
            Err(error) => {
                warn!("{} financial balancesheet failed: {}", sym, error);
                symbol_failed = true;
                symbol_error = Some(append_symbol_error(symbol_error, error));
            }
        }

        // 财务指标
        match bounded_tushare_symbol_call(
            "financial/fina_indicator",
            sym,
            call_timeout,
            client.fina_indicator(sym, None, None),
        )
        .await
        {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    for item in &maps {
                        let end_date = to_date(&get_str(item, "end_date"));
                        let ann_date = to_date(&get_str(item, "ann_date"));
                        if end_date.is_none() {
                            continue;
                        }
                        let ed = end_date.unwrap();
                        let ad = ann_date.unwrap_or(ed);

                        sqlx::query(
                            "INSERT INTO market_financial_indicator (ts_code, ann_date, end_date,
                         eps, roe, roa, gross_margin, netprofit_margin, debt_to_assets, current_ratio, quick_ratio)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                         ON CONFLICT (ts_code, end_date) DO UPDATE SET
                         eps=$4, roe=$5, roa=$6, gross_margin=$7, netprofit_margin=$8,
                         debt_to_assets=$9, current_ratio=$10, quick_ratio=$11",
                        )
                        .bind(sym).bind(ad).bind(ed)
                        .bind(item.get("eps").and_then(|v| v.as_f64()))
                        .bind(item.get("roe").and_then(|v| v.as_f64()))
                        .bind(item.get("roa").and_then(|v| v.as_f64()))
                        .bind(item.get("gross_margin").and_then(|v| v.as_f64()))
                        .bind(item.get("netprofit_margin").and_then(|v| v.as_f64()))
                        .bind(item.get("debt_to_assets").and_then(|v| v.as_f64()))
                        .bind(item.get("current_ratio").and_then(|v| v.as_f64()))
                        .bind(item.get("quick_ratio").and_then(|v| v.as_f64()))
                        .execute(pool).await?;
                        ind_count += 1;
                    }
                }
            }
            Err(error) => {
                warn!("{} financial indicator failed: {}", sym, error);
                symbol_failed = true;
                symbol_error = Some(append_symbol_error(symbol_error, error));
            }
        }
        let symbol_rows = stmt_count - symbol_stmt_start + ind_count - symbol_ind_start;
        let (attempt_start, attempt_end) = financial_sync_attempt_window();
        repository::upsert_sync_attempt(
            pool,
            "financial",
            sym,
            attempt_start,
            attempt_end,
            task_id,
            if symbol_failed { "failed" } else { "completed" },
            symbol_rows as i64,
            symbol_error.as_deref(),
        )
        .await?;
        if symbol_failed {
            failed += 1;
        } else {
            ok += 1;
        }

        // 速率控制 — 每只股票 ~0.3s，避免被限流
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    repository::update_sync_task(
        pool,
        task_id,
        if failed > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        failed as i32,
    )
    .await?;
    info!(
        "财务数据同步完成: {} statements + {} indicators, ok={}, failed={}",
        stmt_count, ind_count, ok, failed
    );
    Ok((stmt_count, ind_count))
}

// ─── sync_forecast ───────────────────────────────────────────────

fn forecast_row_from_map(item: &Map<String, Value>) -> Option<MarketStockForecast> {
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let end_date = to_date(&get_str(item, "end_date"))?;
    let first_ann_date = to_date(&get_str(item, "first_ann_date")).unwrap_or(ann_date);
    Some(MarketStockForecast {
        symbol: get_str(item, "ts_code"),
        ann_date,
        end_date,
        forecast_type: get_str(item, "type"),
        p_change_min: to_opt_decimal(get_f64(item, "p_change_min")),
        p_change_max: to_opt_decimal(get_f64(item, "p_change_max")),
        net_profit_min: to_opt_decimal(get_f64(item, "net_profit_min")),
        net_profit_max: to_opt_decimal(get_f64(item, "net_profit_max")),
        first_ann_date,
        available_at: first_ann_date,
        summary: get_opt_str(item, "summary"),
        change_reason: get_opt_str(item, "change_reason"),
        raw_payload: raw_payload(item),
    })
}

pub async fn sync_forecast(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "forecast",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "earnings forecast sync",
        "tushare",
        &["market_stock_forecast"],
        s,
        e,
    )
    .await?;

    let fallback_symbols = repository::list_listed_stock_symbols(pool).await?;
    let symbols = list_or_stock_symbols(symbols, &fallback_symbols);
    let total = symbols.len();
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 2000usize;

    for symbol in symbols {
        let mut offset = 0usize;
        let mut symbol_failed = false;
        loop {
            match client
                .forecast(
                    Some(symbol),
                    None,
                    Some(start),
                    Some(end),
                    None,
                    None,
                    Some(page_limit),
                    Some(offset),
                )
                .await
            {
                Ok(resp) => {
                    let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                    let row_count = maps.len();
                    let rows: Vec<MarketStockForecast> =
                        maps.iter().filter_map(forecast_row_from_map).collect();
                    if !rows.is_empty() {
                        total_rows +=
                            repository::upsert_forecast_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                    }
                    if row_count < page_limit {
                        break;
                    }
                    offset += page_limit;
                }
                Err(error) => {
                    warn!("{} forecast failed: {}", symbol, error);
                    failed += 1;
                    symbol_failed = true;
                    break;
                }
            }
        }
        if !symbol_failed {
            ok += 1;
        }
        if ok % 100 == 0 {
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                total as i32,
                ok as i32,
                failed as i32,
            )
            .await?;
        }
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if failed > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        failed as i32,
    )
    .await?;
    info!(
        "forecast 同步完成: rows={}, ok={}, failed={}",
        total_rows, ok, failed
    );
    Ok(total_rows)
}

// ─── sync_express ────────────────────────────────────────────────

fn express_row_from_map(item: &Map<String, Value>) -> Option<MarketStockExpress> {
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let end_date = to_date(&get_str(item, "end_date"))?;
    Some(MarketStockExpress {
        symbol: get_str(item, "ts_code"),
        ann_date,
        end_date,
        revenue: to_opt_decimal(get_f64(item, "revenue")),
        n_income: to_opt_decimal(get_f64(item, "n_income")),
        yoy_sales: to_opt_decimal(get_f64(item, "yoy_sales")),
        yoy_dedu_np: to_opt_decimal(get_f64(item, "yoy_dedu_np")),
        diluted_eps: to_opt_decimal(get_f64(item, "diluted_eps")),
        diluted_roe: to_opt_decimal(get_f64(item, "diluted_roe")),
        is_audit: get_i64(item, "is_audit").map(|v| v as i32),
        available_at: ann_date,
        perf_summary: get_opt_str(item, "perf_summary"),
        remark: get_opt_str(item, "remark"),
        raw_payload: raw_payload(item),
    })
}

pub async fn sync_express(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "express",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "earnings express sync",
        "tushare",
        &["market_stock_express"],
        s,
        e,
    )
    .await?;

    let fallback_symbols = repository::list_listed_stock_symbols(pool).await?;
    let symbols = list_or_stock_symbols(symbols, &fallback_symbols);
    let total = symbols.len();
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 2000usize;

    for symbol in symbols {
        let mut offset = 0usize;
        let mut symbol_failed = false;
        loop {
            match client
                .express(
                    symbol,
                    None,
                    Some(start),
                    Some(end),
                    None,
                    Some(page_limit),
                    Some(offset),
                )
                .await
            {
                Ok(resp) => {
                    let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                    let row_count = maps.len();
                    let rows: Vec<MarketStockExpress> =
                        maps.iter().filter_map(express_row_from_map).collect();
                    if !rows.is_empty() {
                        total_rows +=
                            repository::upsert_express_batch(pool, &rows, dv_id, "tushare").await?;
                    }
                    if row_count < page_limit {
                        break;
                    }
                    offset += page_limit;
                }
                Err(error) => {
                    warn!("{} express failed: {}", symbol, error);
                    failed += 1;
                    symbol_failed = true;
                    break;
                }
            }
        }
        if !symbol_failed {
            ok += 1;
        }
        if ok % 100 == 0 {
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                total as i32,
                ok as i32,
                failed as i32,
            )
            .await?;
        }
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if failed > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        failed as i32,
    )
    .await?;
    info!(
        "express 同步完成: rows={}, ok={}, failed={}",
        total_rows, ok, failed
    );
    Ok(total_rows)
}

// ─── sync_disclosure_date ───────────────────────────────────────

fn disclosure_date_row_from_map(item: &Map<String, Value>) -> Option<MarketStockDisclosureDate> {
    let end_date = to_date(&get_str(item, "end_date"))?;
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let actual_date = to_date(&get_str(item, "actual_date"));
    Some(MarketStockDisclosureDate {
        symbol: get_str(item, "ts_code"),
        end_date,
        ann_date,
        pre_date: to_date(&get_str(item, "pre_date")),
        actual_date,
        modify_date: to_date(&get_str(item, "modify_date")),
        available_at: actual_date.unwrap_or(ann_date),
        raw_payload: raw_payload(item),
    })
}

pub async fn sync_disclosure_date(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "disclosure_date",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "financial disclosure date sync",
        "tushare",
        &["market_stock_disclosure_date"],
        s,
        e,
    )
    .await?;

    let symbol_filter = if symbols.is_empty() {
        None
    } else {
        Some(
            symbols
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
        )
    };
    let periods = quarter_end_dates_in_range(s, e);
    let total = periods.len();
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 3000usize;

    for period in &periods {
        let mut offset = 0usize;
        let mut period_failed = false;
        loop {
            let period_str = period.format("%Y%m%d").to_string();
            match client
                .disclosure_date(
                    None,
                    Some(&period_str),
                    None,
                    None,
                    None,
                    Some(page_limit),
                    Some(offset),
                )
                .await
            {
                Ok(resp) => {
                    let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                    let row_count = maps.len();
                    let rows: Vec<MarketStockDisclosureDate> = maps
                        .iter()
                        .filter_map(disclosure_date_row_from_map)
                        .filter(|row| {
                            symbol_filter
                                .as_ref()
                                .map(|filter| filter.contains(&row.symbol))
                                .unwrap_or(true)
                        })
                        .collect();
                    if !rows.is_empty() {
                        total_rows +=
                            repository::upsert_disclosure_date_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                    }
                    if row_count < page_limit {
                        break;
                    }
                    offset += page_limit;
                }
                Err(error) => {
                    warn!(
                        "disclosure_date {} failed: {}",
                        period.format("%Y%m%d"),
                        error
                    );
                    failed += 1;
                    period_failed = true;
                    break;
                }
            }
        }
        if !period_failed {
            ok += 1;
        }
        if ok % 8 == 0 {
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                total as i32,
                ok as i32,
                failed as i32,
            )
            .await?;
        }
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if failed > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        failed as i32,
    )
    .await?;
    info!(
        "disclosure_date 同步完成: rows={}, ok={}, failed={}",
        total_rows, ok, failed
    );
    Ok(total_rows)
}

// ─── sync_cashflow ──────────────────────────────────────────────

fn cashflow_row_from_map(item: &Map<String, Value>) -> Option<MarketStockCashflow> {
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let end_date = to_date(&get_str(item, "end_date"))?;
    let f_ann_date = to_date(&get_str(item, "f_ann_date"));
    Some(MarketStockCashflow {
        symbol: get_str(item, "ts_code"),
        ann_date,
        f_ann_date,
        end_date,
        available_at: f_ann_date.unwrap_or(ann_date),
        net_profit: to_opt_decimal(get_f64(item, "net_profit")),
        n_cashflow_act: to_opt_decimal(get_f64(item, "n_cashflow_act")),
        c_cash_equ_end_period: to_opt_decimal(get_f64(item, "c_cash_equ_end_period")),
        raw_payload: raw_payload(item),
    })
}

pub async fn sync_cashflow(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "cashflow",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "cashflow statement sync",
        "tushare",
        &["market_stock_cashflow"],
        s,
        e,
    )
    .await?;

    let fallback_symbols = repository::list_listed_stock_symbols(pool).await?;
    let symbols = list_or_stock_symbols(symbols, &fallback_symbols);
    let total = symbols.len();
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 2000usize;
    let call_timeout = tushare_symbol_call_timeout();

    for symbol in symbols {
        let mut offset = 0usize;
        let mut symbol_failed = false;
        let mut symbol_rows = 0usize;
        let mut symbol_error: Option<String> = None;
        loop {
            match bounded_tushare_symbol_call(
                "cashflow",
                symbol,
                call_timeout,
                client.cashflow(
                    symbol,
                    Some(start),
                    Some(end),
                    Some(page_limit),
                    Some(offset),
                ),
            )
            .await
            {
                Ok(resp) => {
                    let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                    let row_count = maps.len();
                    let rows: Vec<MarketStockCashflow> =
                        maps.iter().filter_map(cashflow_row_from_map).collect();
                    if !rows.is_empty() {
                        let saved =
                            repository::upsert_cashflow_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                        symbol_rows += saved;
                        total_rows += saved;
                    }
                    if row_count < page_limit {
                        break;
                    }
                    offset += page_limit;
                }
                Err(error) => {
                    warn!("{} cashflow failed: {}", symbol, error);
                    symbol_error = Some(error);
                    failed += 1;
                    symbol_failed = true;
                    break;
                }
            }
        }
        repository::upsert_sync_attempt(
            pool,
            "cashflow",
            symbol,
            s,
            e,
            &task_id,
            if symbol_failed { "failed" } else { "completed" },
            symbol_rows as i64,
            symbol_error.as_deref(),
        )
        .await?;
        if !symbol_failed {
            ok += 1;
        }
        if ok % 100 == 0 {
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                total as i32,
                ok as i32,
                failed as i32,
            )
            .await?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if failed > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        failed as i32,
    )
    .await?;
    info!(
        "cashflow 同步完成: rows={}, ok={}, failed={}",
        total_rows, ok, failed
    );
    Ok(total_rows)
}

// ─── sync_dividend ──────────────────────────────────────────────

fn dividend_row_from_map(item: &Map<String, Value>) -> Option<MarketStockDividend> {
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let end_date = to_date(&get_str(item, "end_date"))?;
    let imp_ann_date = to_date(&get_str(item, "imp_ann_date"));
    Some(MarketStockDividend {
        symbol: get_str(item, "ts_code"),
        end_date,
        ann_date,
        div_proc: get_str(item, "div_proc"),
        available_at: imp_ann_date.unwrap_or(ann_date),
        cash_div: to_opt_decimal(get_f64(item, "cash_div")),
        cash_div_tax: to_opt_decimal(get_f64(item, "cash_div_tax")),
        record_date: to_date(&get_str(item, "record_date")),
        ex_date: to_date(&get_str(item, "ex_date")),
        pay_date: to_date(&get_str(item, "pay_date")),
        imp_ann_date,
        raw_payload: raw_payload(item),
    })
}

pub async fn sync_dividend(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "dividend",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "dividend sync",
        "tushare",
        &["market_stock_dividend"],
        s,
        e,
    )
    .await?;

    let fallback_symbols = repository::list_listed_stock_symbols(pool).await?;
    let symbols = list_or_stock_symbols(symbols, &fallback_symbols);
    let total = symbols.len();
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 2000usize;
    let call_timeout = tushare_symbol_call_timeout();

    for symbol in symbols {
        let mut offset = 0usize;
        let mut symbol_failed = false;
        let mut symbol_rows = 0usize;
        let mut symbol_error: Option<String> = None;
        loop {
            match bounded_tushare_symbol_call(
                "dividend",
                symbol,
                call_timeout,
                client.dividend(
                    symbol,
                    None,
                    None,
                    None,
                    None,
                    Some(page_limit),
                    Some(offset),
                ),
            )
            .await
            {
                Ok(resp) => {
                    let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                    let row_count = maps.len();
                    let rows: Vec<MarketStockDividend> = maps
                        .iter()
                        .filter_map(dividend_row_from_map)
                        .filter(|row| date_in_range(row.available_at, s, e))
                        .collect();
                    if !rows.is_empty() {
                        let saved =
                            repository::upsert_dividend_batch(pool, &rows, dv_id, "tushare")
                                .await?;
                        symbol_rows += saved;
                        total_rows += saved;
                    }
                    if row_count < page_limit {
                        break;
                    }
                    offset += page_limit;
                }
                Err(error) => {
                    warn!("{} dividend failed: {}", symbol, error);
                    symbol_error = Some(error);
                    failed += 1;
                    symbol_failed = true;
                    break;
                }
            }
        }
        repository::upsert_sync_attempt(
            pool,
            "dividend",
            symbol,
            s,
            e,
            &task_id,
            if symbol_failed { "failed" } else { "completed" },
            symbol_rows as i64,
            symbol_error.as_deref(),
        )
        .await?;
        if !symbol_failed {
            ok += 1;
        }
        if ok % 100 == 0 {
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                total as i32,
                ok as i32,
                failed as i32,
            )
            .await?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if failed > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        failed as i32,
    )
    .await?;
    info!(
        "dividend 同步完成: rows={}, ok={}, failed={}",
        total_rows, ok, failed
    );
    Ok(total_rows)
}

// ─── sync_repurchase ────────────────────────────────────────────

fn repurchase_row_from_map(item: &Map<String, Value>) -> Option<MarketStockRepurchase> {
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let end_date = to_date(&get_str(item, "end_date")).unwrap_or(ann_date);
    Some(MarketStockRepurchase {
        symbol: get_str(item, "ts_code"),
        ann_date,
        end_date,
        proc: get_str(item, "proc"),
        available_at: ann_date,
        exp_date: to_date(&get_str(item, "exp_date")),
        vol: to_opt_decimal(get_f64(item, "vol")),
        amount: to_opt_decimal(get_f64(item, "amount")),
        high_limit: to_opt_decimal(get_f64(item, "high_limit")),
        low_limit: to_opt_decimal(get_f64(item, "low_limit")),
        raw_payload: raw_payload(item),
    })
}

pub async fn sync_repurchase(
    pool: &PgPool,
    client: &TushareClient,
    symbols: &[String],
    start: &str,
    end: &str,
    dv_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = dv_id.to_string();
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "repurchase",
        "tushare",
        if symbols.is_empty() {
            None
        } else {
            Some(symbols)
        },
        Some(s),
        Some(e),
        "running",
        None,
    )
    .await?;
    repository::create_data_version(
        pool,
        dv_id,
        "repurchase sync",
        "tushare",
        &["market_stock_repurchase"],
        s,
        e,
    )
    .await?;

    let symbol_filter = if symbols.is_empty() {
        None
    } else {
        Some(
            symbols
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
        )
    };
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 2000usize;
    let mut offset = 0usize;

    loop {
        match client
            .repurchase(None, Some(start), Some(end), Some(page_limit), Some(offset))
            .await
        {
            Ok(resp) => {
                let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                let row_count = maps.len();
                let rows: Vec<MarketStockRepurchase> = maps
                    .iter()
                    .filter_map(repurchase_row_from_map)
                    .filter(|row| {
                        symbol_filter
                            .as_ref()
                            .map(|filter| filter.contains(&row.symbol))
                            .unwrap_or(true)
                    })
                    .collect();
                if !rows.is_empty() {
                    total_rows +=
                        repository::upsert_repurchase_batch(pool, &rows, dv_id, "tushare").await?;
                }
                if row_count < page_limit {
                    break;
                }
                offset += page_limit;
            }
            Err(error) => {
                warn!("repurchase {}..{} failed: {}", start, end, error);
                failed += 1;
                break;
            }
        }
    }

    repository::update_sync_task(
        pool,
        &task_id,
        if failed > 0 { "partial" } else { "completed" },
        1,
        if failed > 0 { 0 } else { 1 },
        failed as i32,
    )
    .await?;
    info!(
        "repurchase 同步完成: rows={}, failed={}",
        total_rows, failed
    );
    Ok(total_rows)
}

// ─── sync_namechange (ST 历史 PIT 合规) ──────────────────────────

/// 同步股票名称变更历史，构建 PIT 合规的 ST 判断数据。
/// 从 Tushare namechange API 获取所有名称变更记录，提取 ST 期间。
pub async fn sync_namechange(
    pool: &PgPool,
    client: &TushareClient,
) -> Result<usize, String> {
    // 获取 1990 年至今的所有名称变更
    let resp = client
        .namechange(None, Some("19900101"), None)
        .await
        .map_err(|e| format!("namechange API 调用失败: {}", e))?;

    let maps = resp.data.map(|d| d.to_maps()).unwrap_or_default();
    let mut total = 0usize;
    let mut st_changes = 0usize;

    for item in &maps {
        let ts_code = item["ts_code"].as_str().unwrap_or("");
        let name = item["name"].as_str().unwrap_or("");
        let start_date_str = item["start_date"].as_str().unwrap_or("");
        let end_date_str = item["end_date"].as_str().or(Some("")).unwrap_or("");
        let reason = item["change_reason"].as_str().unwrap_or("");

        if ts_code.is_empty() || start_date_str.is_empty() {
            continue;
        }

        let start_date = NaiveDate::parse_from_str(start_date_str, "%Y%m%d")
            .unwrap_or_else(|_| NaiveDate::from_ymd_opt(1990, 1, 1).unwrap());
        let end_date = if end_date_str.is_empty() || end_date_str == "None" {
            None
        } else {
            NaiveDate::parse_from_str(end_date_str, "%Y%m%d").ok()
        };

        // 判断该名称是否为 ST（名称含 ST 但不是退市）
        let is_st = name.contains("ST") && !name.starts_with("退市");

        sqlx::query(
            r#"INSERT INTO market_stock_name_history (symbol, name, start_date, end_date, change_reason, is_st)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT DO NOTHING"#,
        )
        .bind(ts_code)
        .bind(name)
        .bind(start_date)
        .bind(end_date)
        .bind(reason)
        .bind(is_st)
        .execute(pool)
        .await
        .map_err(|e| format!("插入名称变更失败 {}: {}", ts_code, e))?;

        total += 1;
        if is_st {
            st_changes += 1;
        }
    }

    info!(total, st_changes, "ST 名称变更历史同步完成");

    // 同时更新 market_stock 的 is_st 标记（当前状态）
    let updated = sqlx::query(
        r#"UPDATE market_stock ms SET is_st = true
           FROM (
               SELECT DISTINCT symbol FROM market_stock_name_history
               WHERE is_st = true
                 AND (end_date IS NULL OR end_date >= CURRENT_DATE)
           ) st
           WHERE ms.symbol = st.symbol"#,
    )
    .execute(pool)
    .await
    .map_err(|e| format!("更新当前ST标记失败: {}", e))?;

    info!(updated = updated.rows_affected(), "已更新 market_stock.is_st 当前状态");

    Ok(total)
}

// ─── sync_suspension (停牌数据同步) ──────────────────────────

/// 同步股票停牌/复牌信息。
/// 从 Tushare suspend_d API 获取当日停牌股票列表。
pub async fn sync_suspension(
    pool: &PgPool,
    client: &TushareClient,
    trade_date: &str,
) -> Result<usize, String> {
    let resp = client
        .suspend_d(Some(trade_date), None, None, None)
        .await
        .map_err(|e| format!("suspend_d API: {}", e))?;

    let maps = resp.data.map(|d| d.to_maps()).unwrap_or_default();
    let mut total = 0usize;

    // 先清除当日旧数据
    let d = NaiveDate::parse_from_str(trade_date, "%Y%m%d")
        .map_err(|e| format!("日期解析: {}", e))?;
    sqlx::query("DELETE FROM market_stock_suspension WHERE trade_date = $1")
        .bind(d)
        .execute(pool)
        .await
        .map_err(|e| format!("清除旧数据: {}", e))?;

    for item in &maps {
        let ts_code = item["ts_code"].as_str().unwrap_or("");
        let s_type = item["suspend_type"].as_str().unwrap_or("");

        if ts_code.is_empty() { continue; }

        sqlx::query(
            "INSERT INTO market_stock_suspension (symbol, trade_date, suspend_type)
             VALUES ($1, $2, $3) ON CONFLICT (symbol, trade_date) DO NOTHING",
        )
        .bind(ts_code)
        .bind(d)
        .bind(s_type)
        .execute(pool)
        .await
        .map_err(|e| format!("insert: {}", e))?;
        total += 1;
    }

    // 更新 market_stock 的 is_suspended 标记
    let updated = sqlx::query(
        "UPDATE market_stock SET is_suspended = true
         WHERE symbol IN (SELECT symbol FROM market_stock_suspension WHERE trade_date = $1 AND suspend_type = 'S')",
    )
    .bind(d)
    .execute(pool)
    .await
    .map_err(|e| format!("更新停牌标记: {}", e))?;

    info!(total, updated = updated.rows_affected(), date = trade_date, "停牌数据同步完成");
    Ok(total)
}

/// 获取指定日期停牌的股票列表 (用于选股过滤)
pub async fn get_suspended_symbols(
    pool: &PgPool,
    trade_date: NaiveDate,
) -> Result<Vec<String>, String> {
    let rows = sqlx::query_as::<_, (String,)>(
        "SELECT symbol FROM market_stock_suspension WHERE trade_date = $1 AND suspend_type = 'S'",
    )
    .bind(trade_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("查询停牌: {}", e))?;
    Ok(rows.into_iter().map(|(s,)| s).collect())
}

// ─── sync_limit_list (涨跌停数据同步) ──────────────────────────

pub async fn sync_limit_list(
    pool: &PgPool,
    client: &TushareClient,
    trade_date: &str,
) -> Result<usize, String> {
    let resp = client
        .limit_list_d(Some(trade_date), None, None, None)
        .await
        .map_err(|e| format!("limit_list_d API: {}", e))?;

    let maps = resp.data.map(|d| d.to_maps()).unwrap_or_default();
    let mut total = 0usize;

    let d = NaiveDate::parse_from_str(trade_date, "%Y%m%d")
        .map_err(|e| format!("日期解析: {}", e))?;

    // 清除当日旧数据
    sqlx::query("DELETE FROM market_stock_limit WHERE trade_date = $1")
        .bind(d).execute(pool).await.map_err(|e| format!("清除: {}", e))?;

    for item in &maps {
        let ts_code = item["ts_code"].as_str().unwrap_or("");
        if ts_code.is_empty() { continue; }

        sqlx::query(
            "INSERT INTO market_stock_limit (symbol, trade_date) VALUES ($1, $2) ON CONFLICT (symbol, trade_date) DO NOTHING",
        )
        .bind(ts_code).bind(d).execute(pool).await.map_err(|e| format!("insert: {}", e))?;
        total += 1;
    }

    info!(total, date = trade_date, "涨跌停数据同步完成");
    Ok(total)
}

/// 同步涨跌停数据（日期范围，一次API调用）
pub async fn sync_limit_list_range(
    pool: &PgPool,
    client: &TushareClient,
    start_date: &str,
    end_date: &str,
) -> Result<usize, String> {
    let resp = client
        .limit_list_d(None, None, Some(start_date), Some(end_date))
        .await
        .map_err(|e| format!("limit_list_d range API: {}", e))?;

    let maps = resp.data.map(|d| d.to_maps()).unwrap_or_default();
    let mut total = 0usize;

    for item in &maps {
        let ts_code = item["ts_code"].as_str().unwrap_or("");
        let trade_date_str = item["trade_date"].as_str().unwrap_or("");
        if ts_code.is_empty() || trade_date_str.is_empty() { continue; }

        let d = NaiveDate::parse_from_str(trade_date_str, "%Y%m%d")
            .map_err(|_| "日期解析".to_string())?;

        sqlx::query(
            "INSERT INTO market_stock_limit (symbol, trade_date) VALUES ($1, $2) ON CONFLICT (symbol, trade_date) DO NOTHING",
        )
        .bind(ts_code).bind(d).execute(pool).await.map_err(|e| format!("insert: {}", e))?;
        total += 1;
    }

    info!(total, start = start_date, end = end_date, "涨跌停范围同步完成");
    Ok(total)
}

/// 获取 PIT 合规的 ST 股票列表（用于回测过滤条件）。
/// 参数 `as_of_date`: 回测时间点，仅返回该日期之前已进入 ST 的股票。
pub async fn get_st_symbols_at_date(
    pool: &PgPool,
    as_of_date: NaiveDate,
) -> Result<Vec<String>, String> {
    let rows = sqlx::query_as!(
        MarketStSymbol,
        r#"SELECT DISTINCT symbol FROM market_stock_name_history
           WHERE is_st = true
             AND start_date <= $1
             AND (end_date IS NULL OR end_date >= $1)"#,
        as_of_date,
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("查询 ST 列表失败: {}", e))?;
    Ok(rows.into_iter().map(|r| r.symbol).collect())
}

/// 获取 PIT 合规的非 ST 主板股票列表（用于回测 universe）。
/// 排除：ST 股票、创业板（300xxx.SZ）、科创板（688xxx.SH）
pub async fn get_pit_main_board_non_st_symbols(
    pool: &PgPool,
    as_of_date: NaiveDate,
) -> Result<Vec<String>, String> {
    let rows = sqlx::query_as!(
        MarketStSymbol,
        r#"SELECT ms.symbol FROM market_stock ms
           WHERE ms.list_status = 'L'
             AND ms.list_date <= $1
             AND ms.symbol NOT LIKE '300%SZ'
             AND ms.symbol NOT LIKE '301%SZ'
             AND ms.symbol NOT LIKE '688%SH'
             AND ms.symbol NOT IN (
                 SELECT symbol FROM market_stock_name_history
                 WHERE is_st = true
                   AND start_date <= $1
                   AND (end_date IS NULL OR end_date >= $1)
             )
           ORDER BY ms.symbol"#,
        as_of_date,
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("查询主板非ST列表失败: {}", e))?;
    Ok(rows.into_iter().map(|r| r.symbol).collect())
}

// 内部辅助结构体
struct MarketStSymbol {
    symbol: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn months_in_range_splits_by_calendar_month() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 30).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 3, 2).unwrap();

        let months = months_in_range(start, end);

        assert_eq!(
            months,
            vec![
                (
                    NaiveDate::from_ymd_opt(2026, 1, 30).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 1, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn financial_sync_attempt_window_uses_postgres_safe_dates() {
        let (start, end) = financial_sync_attempt_window();

        assert_eq!(start, NaiveDate::from_ymd_opt(1900, 1, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(9999, 12, 31).unwrap());
    }

    #[tokio::test]
    async fn bounded_tushare_symbol_call_times_out_instead_of_waiting_forever() {
        let result = bounded_tushare_symbol_call(
            "cashflow",
            "000001.SZ",
            std::time::Duration::from_millis(1),
            async {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                Ok::<(), &'static str>(())
            },
        )
        .await;

        let error = result.expect_err("pending request should time out");
        assert!(error.contains("cashflow"));
        assert!(error.contains("000001.SZ"));
        assert!(error.contains("timed out"));
    }

    #[tokio::test]
    async fn bounded_tushare_symbol_call_preserves_upstream_error_message() {
        let result = bounded_tushare_symbol_call(
            "dividend",
            "000002.SZ",
            std::time::Duration::from_secs(10),
            async { Err::<(), _>("HTTP error: error sending request for url") },
        )
        .await;

        assert_eq!(
            result.expect_err("upstream error should be returned"),
            "HTTP error: error sending request for url"
        );
    }

    #[test]
    fn daily_basic_row_maps_valuation_fields() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("trade_date".to_string(), json!("20260511"));
        item.insert("pe_ttm".to_string(), json!(6.8));
        item.insert("pb".to_string(), json!(0.72));
        item.insert("ps_ttm".to_string(), json!(1.15));
        item.insert("dv_ttm".to_string(), json!(4.2));
        item.insert("total_mv".to_string(), json!(123456.7));
        item.insert("circ_mv".to_string(), json!(98765.4));

        let row = daily_basic_row_from_map(&item).expect("daily basic row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(
            row.trade_date,
            NaiveDate::from_ymd_opt(2026, 5, 11).unwrap()
        );
        assert_eq!(row.pe_ttm, Decimal::from_f64_retain(6.8));
        assert_eq!(row.pb, Decimal::from_f64_retain(0.72));
        assert_eq!(row.ps_ttm, Decimal::from_f64_retain(1.15));
        assert_eq!(row.dv_ttm, Decimal::from_f64_retain(4.2));
        assert_eq!(row.total_mv, Decimal::from_f64_retain(123456.7));
        assert_eq!(row.circ_mv, Decimal::from_f64_retain(98765.4));
    }

    #[test]
    fn moneyflow_row_maps_fund_flow_fields() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("trade_date".to_string(), json!("20260511"));
        item.insert("buy_sm_amount".to_string(), json!(123.4));
        item.insert("sell_sm_amount".to_string(), json!(100.1));
        item.insert("buy_lg_amount".to_string(), json!(456.7));
        item.insert("sell_lg_amount".to_string(), json!(321.2));
        item.insert("buy_elg_amount".to_string(), json!(789.0));
        item.insert("sell_elg_amount".to_string(), json!(654.3));
        item.insert("net_mf_amount".to_string(), json!(293.5));

        let row = moneyflow_row_from_map(&item).expect("moneyflow row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(
            row.trade_date,
            NaiveDate::from_ymd_opt(2026, 5, 11).unwrap()
        );
        assert_eq!(row.buy_sm_amount, Decimal::from_f64_retain(123.4));
        assert_eq!(row.sell_sm_amount, Decimal::from_f64_retain(100.1));
        assert_eq!(row.buy_lg_amount, Decimal::from_f64_retain(456.7));
        assert_eq!(row.sell_lg_amount, Decimal::from_f64_retain(321.2));
        assert_eq!(row.buy_elg_amount, Decimal::from_f64_retain(789.0));
        assert_eq!(row.sell_elg_amount, Decimal::from_f64_retain(654.3));
        assert_eq!(row.net_mf_amount, Decimal::from_f64_retain(293.5));
    }

    #[test]
    fn cashflow_row_maps_pit_available_at_from_f_ann_date() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000333.SZ"));
        item.insert("ann_date".to_string(), json!("20260430"));
        item.insert("f_ann_date".to_string(), json!("20260429"));
        item.insert("end_date".to_string(), json!("20260331"));
        item.insert("net_profit".to_string(), json!(12450000000.0));
        item.insert("n_cashflow_act".to_string(), json!(14529165000.0));
        item.insert("c_cash_equ_end_period".to_string(), json!(76253016000.0));

        let row = cashflow_row_from_map(&item).expect("cashflow row");

        assert_eq!(row.symbol, "000333.SZ");
        assert_eq!(row.ann_date, NaiveDate::from_ymd_opt(2026, 4, 30).unwrap());
        assert_eq!(
            row.f_ann_date,
            Some(NaiveDate::from_ymd_opt(2026, 4, 29).unwrap())
        );
        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2026, 3, 31).unwrap());
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 4, 29).unwrap()
        );
        assert_eq!(row.net_profit, Decimal::from_f64_retain(12450000000.0));
        assert_eq!(row.n_cashflow_act, Decimal::from_f64_retain(14529165000.0));
        assert_eq!(
            row.c_cash_equ_end_period,
            Decimal::from_f64_retain(76253016000.0)
        );
        assert_eq!(row.raw_payload["ts_code"], json!("000333.SZ"));
    }

    #[test]
    fn cashflow_row_falls_back_to_ann_date_when_f_ann_date_missing() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("ann_date".to_string(), json!("20260425"));
        item.insert("end_date".to_string(), json!("20260331"));

        let row = cashflow_row_from_map(&item).expect("cashflow row");

        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 4, 25).unwrap()
        );
    }

    #[test]
    fn dividend_row_maps_pit_available_at_from_imp_ann_date() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000333.SZ"));
        item.insert("end_date".to_string(), json!("20251231"));
        item.insert("ann_date".to_string(), json!("20260331"));
        item.insert("div_proc".to_string(), json!("实施"));
        item.insert("cash_div".to_string(), json!(0.0));
        item.insert("cash_div_tax".to_string(), json!(3.8));
        item.insert("record_date".to_string(), json!("20260512"));
        item.insert("ex_date".to_string(), json!("20260513"));
        item.insert("pay_date".to_string(), json!("20260513"));
        item.insert("imp_ann_date".to_string(), json!("20260506"));

        let row = dividend_row_from_map(&item).expect("dividend row");

        assert_eq!(row.symbol, "000333.SZ");
        assert_eq!(row.div_proc, "实施");
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 5, 6).unwrap()
        );
        assert_eq!(row.cash_div_tax, Decimal::from_f64_retain(3.8));
        assert_eq!(
            row.ex_date,
            Some(NaiveDate::from_ymd_opt(2026, 5, 13).unwrap())
        );
    }

    #[test]
    fn dividend_row_falls_back_to_ann_date_when_imp_ann_date_missing() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("600000.SH"));
        item.insert("end_date".to_string(), json!("20251231"));
        item.insert("ann_date".to_string(), json!("20260331"));
        item.insert("div_proc".to_string(), json!("预案"));

        let row = dividend_row_from_map(&item).expect("dividend row");

        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 3, 31).unwrap()
        );
    }

    #[test]
    fn repurchase_row_maps_announcement_available_at() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("600010.SH"));
        item.insert("ann_date".to_string(), json!("20260525"));
        item.insert("end_date".to_string(), json!("20260521"));
        item.insert("proc".to_string(), json!("完成"));
        item.insert("vol".to_string(), json!(62586400.0));
        item.insert("amount".to_string(), json!(152003311.0));
        item.insert("high_limit".to_string(), json!(2.72));
        item.insert("low_limit".to_string(), json!(1.79));

        let row = repurchase_row_from_map(&item).expect("repurchase row");

        assert_eq!(row.symbol, "600010.SH");
        assert_eq!(row.proc, "完成");
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 5, 25).unwrap()
        );
        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2026, 5, 21).unwrap());
        assert_eq!(row.amount, Decimal::from_f64_retain(152003311.0));
        assert_eq!(row.high_limit, Decimal::from_f64_retain(2.72));
    }

    #[test]
    fn repurchase_row_falls_back_end_date_to_ann_date() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("ann_date".to_string(), json!("20260525"));
        item.insert("proc".to_string(), json!("预案"));

        let row = repurchase_row_from_map(&item).expect("repurchase row");

        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2026, 5, 25).unwrap());
    }

    #[test]
    fn quarter_end_dates_in_range_includes_supported_report_periods() {
        let start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();

        let periods = quarter_end_dates_in_range(start, end);

        assert_eq!(
            periods,
            vec![
                NaiveDate::from_ymd_opt(2026, 3, 31).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
                NaiveDate::from_ymd_opt(2026, 9, 30).unwrap(),
            ]
        );
    }

    #[test]
    fn forecast_row_maps_event_fields_and_available_at() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("600896.SH"));
        item.insert("ann_date".to_string(), json!("20240501"));
        item.insert("end_date".to_string(), json!("20231231"));
        item.insert("type".to_string(), json!("续亏"));
        item.insert("p_change_min".to_string(), json!(4.6273));
        item.insert("p_change_max".to_string(), json!(4.6273));
        item.insert("net_profit_min".to_string(), json!(-18900.0));
        item.insert("net_profit_max".to_string(), json!(-18900.0));
        item.insert("first_ann_date".to_string(), json!("20240430"));
        item.insert("summary".to_string(), json!("sample summary"));
        item.insert("change_reason".to_string(), json!("sample reason"));

        let row = forecast_row_from_map(&item).expect("forecast row");

        assert_eq!(row.symbol, "600896.SH");
        assert_eq!(row.ann_date, NaiveDate::from_ymd_opt(2024, 5, 1).unwrap());
        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2023, 12, 31).unwrap());
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2024, 4, 30).unwrap()
        );
        assert_eq!(row.forecast_type, "续亏");
        assert_eq!(row.p_change_min, Decimal::from_f64_retain(4.6273));
        assert_eq!(row.net_profit_min, Decimal::from_f64_retain(-18900.0));
        assert_eq!(row.raw_payload["ts_code"], json!("600896.SH"));
    }

    #[test]
    fn express_row_maps_event_fields() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("ann_date".to_string(), json!("20230117"));
        item.insert("end_date".to_string(), json!("20221231"));
        item.insert("revenue".to_string(), json!(179895000000.0));
        item.insert("n_income".to_string(), json!(45516000000.0));
        item.insert("yoy_sales".to_string(), json!(6.2));
        item.insert("yoy_dedu_np".to_string(), json!(25.3));
        item.insert("diluted_eps".to_string(), json!(2.35));
        item.insert("diluted_roe".to_string(), json!(12.8));
        item.insert("is_audit".to_string(), json!(0));

        let row = express_row_from_map(&item).expect("express row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2023, 1, 17).unwrap()
        );
        assert_eq!(row.revenue, Decimal::from_f64_retain(179895000000.0));
        assert_eq!(row.n_income, Decimal::from_f64_retain(45516000000.0));
        assert_eq!(row.yoy_sales, Decimal::from_f64_retain(6.2));
        assert_eq!(row.is_audit, Some(0));
    }

    #[test]
    fn disclosure_date_row_maps_available_at_from_actual_date() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("ann_date".to_string(), json!("20240131"));
        item.insert("end_date".to_string(), json!("20231231"));
        item.insert("pre_date".to_string(), json!("20240315"));
        item.insert("actual_date".to_string(), json!("20240314"));
        item.insert("modify_date".to_string(), json!("20240220"));

        let row = disclosure_date_row_from_map(&item).expect("disclosure date row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2023, 12, 31).unwrap());
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2024, 3, 14).unwrap()
        );
        assert_eq!(
            row.pre_date,
            Some(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())
        );
    }
}
