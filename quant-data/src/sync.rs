//! 数据同步服务 — Tushare → 标准化 → PostgreSQL

use chrono::{Datelike, Duration, NaiveDate};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::time::Duration as StdDuration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::model::entities::{
    MarketAdjustmentFactor, MarketFuturesDaily, MarketFuturesHoldingRank,
    MarketFuturesWarehouseReceipt, MarketIndexDailyBar, MarketStock, MarketStockCashflow,
    MarketStockDailyBar, MarketStockDailyBasic, MarketStockDisclosureDate, MarketStockDividend,
    MarketStockExpress, MarketStockForecast, MarketStockIndustryMembershipPit,
    MarketStockMainBusiness, MarketStockMarginDetail, MarketStockMoneyflow, MarketStockRepurchase,
    MarketStockShareFloat, MarketTradeCalendar,
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

fn value_key_part(item: &Map<String, Value>, key: &str) -> String {
    match item.get(key) {
        Some(Value::String(value)) => value.trim().to_string(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Null) | None => String::new(),
        Some(value) => value.to_string(),
    }
}

fn stable_source_row_hash(parts: &[String]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{:016x}", hash)
}

fn non_empty_opt_str(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn main_business_attempt_source(symbols: &[String]) -> &'static str {
    if symbols.is_empty() {
        "main_business"
    } else {
        "main_business_sample"
    }
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
    const DEFAULT_TIMEOUT_SECS: u64 = 30;
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
    let chunk_count = if total == 0 { 0 } else { (total + 499) / 500 };
    let total_work = months.len().saturating_mul(chunk_count).max(1);
    let mut processed_work = 0usize;
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
                                        let returned_symbols: std::collections::HashSet<String> =
                                            bars.iter().map(|bar| bar.symbol.clone()).collect();
                                        total_rows += bars.len();
                                        repository::upsert_daily_bars_batch(
                                            pool, &bars, dv_id, "tushare",
                                        )
                                        .await?;
                                        for symbol in returned_symbols {
                                            synced.insert(symbol);
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
            processed_work += 1;
            let progress = ((processed_work * 100) / total_work).min(99) as i32;
            repository::heartbeat_sync_task(
                pool,
                &task_id,
                total as i32,
                synced.len() as i32,
                failed.len() as i32,
                progress,
            )
            .await?;
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

/// Generate (start, end) tuples for each calendar quarter in a date range.
fn quarters_in_range(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    let mut result = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let year = cursor.year();
        let quarter_end_month = ((cursor.month() - 1) / 3 + 1) * 3;
        let quarter_end = NaiveDate::from_ymd_opt(
            year,
            quarter_end_month,
            days_in_month(year, quarter_end_month),
        )
        .unwrap_or(cursor);
        let actual_end = quarter_end.min(end);
        result.push((cursor, actual_end));
        cursor = actual_end + chrono::Duration::days(1);
    }
    result
}

fn calendar_dates_in_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        dates.push(cursor);
        cursor += Duration::days(1);
    }
    dates
}

fn moneyflow_full_market_trade_dates(
    start: NaiveDate,
    end: NaiveDate,
    open_dates: Vec<NaiveDate>,
) -> Vec<NaiveDate> {
    if open_dates.is_empty() {
        return calendar_dates_in_range(start, end);
    }

    open_dates
        .into_iter()
        .filter(|date| *date >= start && *date <= end)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn load_moneyflow_full_market_trade_dates(
    pool: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<NaiveDate>, sqlx::Error> {
    let open_dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT trade_date
         FROM market_trade_calendar
         WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;
    Ok(moneyflow_full_market_trade_dates(start, end, open_dates))
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
        cursor = NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap_or(end + chrono::Duration::days(1));
    }
    result
}

fn date_chunks_by_days(
    start: NaiveDate,
    end: NaiveDate,
    chunk_days: i64,
) -> Vec<(NaiveDate, NaiveDate)> {
    let chunk_days = chunk_days.max(1);
    let mut result = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let chunk_end = (cursor + chrono::Duration::days(chunk_days - 1)).min(end);
        result.push((cursor, chunk_end));
        cursor = chunk_end + chrono::Duration::days(1);
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
        total_share: to_opt_decimal(get_f64(item, "total_share")),
        float_share: to_opt_decimal(get_f64(item, "float_share")),
        free_share: to_opt_decimal(get_f64(item, "free_share")),
        total_mv: to_opt_decimal(get_f64(item, "total_mv")),
        circ_mv: to_opt_decimal(get_f64(item, "circ_mv")),
    })
}

fn is_tushare_hourly_limit_error(message: &str) -> bool {
    message.contains("40203") || message.contains("每小时最多访问") || message.contains("4000次")
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
                        let err_msg = error.to_string();
                        warn!("daily_basic {}..{} failed: {}", sd, ed, err_msg);
                        if is_tushare_hourly_limit_error(&err_msg) {
                            failed += 1;
                            repository::update_sync_task(
                                pool,
                                &task_id,
                                "partial",
                                months.len() as i32,
                                ok as i32,
                                failed as i32,
                            )
                            .await?;
                            return Err(format!(
                                "daily_basic rate limited by Tushare before fallback: {}",
                                err_msg
                            )
                            .into());
                        }
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
                                        let day_err_msg = day_error.to_string();
                                        warn!("daily_basic {} failed: {}", trade_date, day_err_msg);
                                        failed += 1;
                                        fallback_failed = true;
                                        if is_tushare_hourly_limit_error(&day_err_msg) {
                                            repository::update_sync_task(
                                                pool,
                                                &task_id,
                                                "partial",
                                                months.len() as i32,
                                                ok as i32,
                                                failed as i32,
                                            )
                                            .await?;
                                            return Err(format!(
                                                "daily_basic rate limited by Tushare during daily fallback: {}",
                                                day_err_msg
                                            )
                                            .into());
                                        }
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
                        let err_msg = error.to_string();
                        warn!("{} daily_basic failed: {}", symbol, err_msg);
                        failed += 1;
                        symbol_failed = true;
                        if is_tushare_hourly_limit_error(&err_msg) {
                            repository::update_sync_task(
                                pool,
                                &task_id,
                                "partial",
                                symbols.len() as i32,
                                ok as i32,
                                failed as i32,
                            )
                            .await?;
                            return Err(format!(
                                "daily_basic rate limited by Tushare at symbol {}: {}",
                                symbol, err_msg
                            )
                            .into());
                        }
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
pub async fn sync_fund_basic(pool: &PgPool, client: &TushareClient) -> Result<usize, String> {
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

            if ts_code.is_empty() || status == "D" {
                continue;
            } // skip delisted

            // exchange 按 ts_code 后缀判定(SH→SSE, SZ→SZSE),修正原硬编码 SSE
            let exchange = if ts_code.ends_with(".SH") {
                "SSE"
            } else if ts_code.ends_with(".SZ") {
                "SZSE"
            } else {
                "SSE"
            };
            // list_date/delist_date:tushare 返回 "YYYYMMDD" 字符串,转 NaiveDate;空串→NULL
            let list_date = item["list_date"]
                .as_str()
                .filter(|s| !s.is_empty())
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());
            let delist_date = item["delist_date"]
                .as_str()
                .filter(|s| !s.is_empty())
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());

            sqlx::query(
                "INSERT INTO market_stock (symbol, name, exchange, list_status, is_st, list_date, delist_date)
                 VALUES ($1, $2, $3, 'L', false, $4, $5)
                 ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name, exchange = EXCLUDED.exchange,
                   list_date = COALESCE(EXCLUDED.list_date, market_stock.list_date),
                   delist_date = COALESCE(EXCLUDED.delist_date, market_stock.delist_date)",
            )
            .bind(ts_code)
            .bind(format!("{} ({})", name, fund_type))
            .bind(exchange)
            .bind(list_date)
            .bind(delist_date)
            .execute(pool)
            .await
            .map_err(|e| format!("insert failed: {}", e))?;
            total += 1;
        }
    }

    Ok(total)
}

// ─── sync_fund_daily (ETF/LOF 基金日线) ──────────────────────────

fn fund_daily_bars_from_maps(maps: &[Map<String, Value>]) -> Vec<MarketStockDailyBar> {
    maps.iter()
        .filter_map(|item| {
            let ts_code = get_str(item, "ts_code");
            Some(MarketStockDailyBar {
                symbol: ts_code.to_string(),
                trade_date: to_date(&get_str(item, "trade_date"))?,
                open: to_decimal(get_f64(item, "open")),
                high: to_decimal(get_f64(item, "high")),
                low: to_decimal(get_f64(item, "low")),
                close: to_decimal(get_f64(item, "close")),
                pre_close: get_f64(item, "pre_close").and_then(|v| Decimal::from_f64_retain(v)),
                change_pct: get_f64(item, "pct_chg")
                    .and_then(|v| Decimal::from_f64_retain(v / 100.0)),
                volume: to_decimal(get_f64(item, "vol")),
                amount: to_decimal(get_f64(item, "amount")),
            })
        })
        .collect()
}

async fn record_fund_daily_completed_attempt(
    pool: &PgPool,
    symbol: &str,
    start: NaiveDate,
    end: NaiveDate,
    task_id: &str,
    bars: &[MarketStockDailyBar],
    record_exact_zero_days: bool,
) -> Result<(), sqlx::Error> {
    repository::upsert_sync_attempt(
        pool,
        "fund_daily",
        symbol,
        start,
        end,
        task_id,
        "completed",
        bars.len() as i64,
        None,
    )
    .await?;

    if !record_exact_zero_days {
        return Ok(());
    }

    let present_dates = bars
        .iter()
        .map(|bar| bar.trade_date)
        .collect::<std::collections::HashSet<_>>();
    let open_dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT DISTINCT trade_date
         FROM market_trade_calendar
         WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;

    for date in open_dates {
        if present_dates.contains(&date) {
            continue;
        }
        repository::upsert_sync_attempt(
            pool,
            "fund_daily",
            symbol,
            date,
            date,
            task_id,
            "completed",
            0,
            Some("upstream fund_daily returned no row for this open date"),
        )
        .await?;
    }

    Ok(())
}

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
        pool,
        &task_id,
        "fund_daily",
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
        "fund daily bars sync",
        "tushare",
        &["market_stock_daily_bar"],
        s,
        e,
    )
    .await?;

    let mut total_rows = 0usize;
    // Use yearly chunks instead of monthly — reduces API calls ~12× (240→20 for 2006-2026)
    let years = years_in_range(s, e);
    info!(
        "Syncing {} fund symbols across {} yearly chunks ({} to {})",
        symbols.len(),
        years.len(),
        start,
        end
    );

    // Rate-limit tracking: max 4000 calls/hr, target ~3000 calls/hr = 50 calls/min
    let mut calls_this_minute = 0u32;
    let max_calls_per_minute = 45u32; // conservative: 45 × 60 = 2700/hr, well under 4000 limit
    let record_exact_zero_days = symbols.len() <= 32;

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
                    let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                    let bars = fund_daily_bars_from_maps(&maps);
                    if !bars.is_empty() {
                        total_rows += bars.len();
                        repository::upsert_daily_bars_batch(pool, &bars, dv_id, "tushare").await?;
                    }
                    record_fund_daily_completed_attempt(
                        pool,
                        symbol,
                        *y_start,
                        *y_end,
                        &task_id,
                        &bars,
                        record_exact_zero_days,
                    )
                    .await?;
                }
                Err(e) => {
                    let err_str = e.to_string();
                    // Rate-limit hit: wait and retry once
                    if err_str.contains("40203") || err_str.contains("4000") {
                        warn!(
                            "fund_daily rate-limit, waiting 5s for {}: {}",
                            symbol, err_str
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        calls_this_minute = 0;
                        // Retry once
                        match client
                            .fund_daily(Some(symbol), None, Some(&sd), Some(&ed))
                            .await
                        {
                            Ok(resp) => {
                                calls_this_minute += 1;
                                let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                                let bars = fund_daily_bars_from_maps(&maps);
                                if !bars.is_empty() {
                                    total_rows += bars.len();
                                    repository::upsert_daily_bars_batch(
                                        pool, &bars, dv_id, "tushare",
                                    )
                                    .await?;
                                }
                                record_fund_daily_completed_attempt(
                                    pool,
                                    symbol,
                                    *y_start,
                                    *y_end,
                                    &task_id,
                                    &bars,
                                    record_exact_zero_days,
                                )
                                .await?;
                            }
                            Err(e2) => {
                                warn!(
                                    "fund_daily retry also failed for {} in {}-{}: {}",
                                    symbol, sd, ed, e2
                                );
                                let error_message = e2.to_string();
                                repository::upsert_sync_attempt(
                                    pool,
                                    "fund_daily",
                                    symbol,
                                    *y_start,
                                    *y_end,
                                    &task_id,
                                    "failed",
                                    0,
                                    Some(&error_message),
                                )
                                .await?;
                            }
                        }
                    } else {
                        warn!("fund_daily failed for {} in {}-{}: {}", symbol, sd, ed, e);
                        repository::upsert_sync_attempt(
                            pool,
                            "fund_daily",
                            symbol,
                            *y_start,
                            *y_end,
                            &task_id,
                            "failed",
                            0,
                            Some(&err_str),
                        )
                        .await?;
                    }
                }
            }
        }
    }

    repository::update_sync_task(
        pool,
        &task_id,
        "completed",
        total_rows as i32,
        total_rows as i32,
        0,
    )
    .await?;
    info!(
        "fund_daily 同步完成: {} symbols, {} rows, {} yearly chunks",
        symbols.len(),
        total_rows,
        years.len()
    );
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
        let trade_dates = load_moneyflow_full_market_trade_dates(pool, s, e).await?;
        let total = trade_dates.len() as i32;
        for trade_day in &trade_dates {
            let trade_date = trade_day.format("%Y%m%d").to_string();
            let mut offset = 0usize;
            let mut day_failed = false;

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
                        warn!("moneyflow {} failed: {}", trade_date, error);
                        failed += 1;
                        day_failed = true;
                        break;
                    }
                }
            }
            if !day_failed {
                ok += 1;
            }
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                total,
                ok as i32,
                failed as i32,
            )
            .await?;
            if total_rows % 500_000 < page_limit {
                info!(
                    "moneyflow daily full-market sync: {} rows through {}",
                    total_rows, trade_date
                );
            }
        }

        repository::update_sync_task(
            pool,
            &task_id,
            if failed > 0 { "partial" } else { "completed" },
            total,
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

// ─── sync_margin_detail (个股融资融券明细) ─────────────────────

fn margin_detail_available_at_from_open_dates(
    trade_date: NaiveDate,
    open_dates: &[NaiveDate],
) -> NaiveDate {
    open_dates
        .iter()
        .copied()
        .find(|date| *date > trade_date)
        .unwrap_or_else(|| trade_date + Duration::days(1))
}

fn margin_detail_source_published_at(
    available_at: NaiveDate,
) -> Option<chrono::DateTime<chrono::Utc>> {
    available_at
        .and_hms_opt(0, 30, 0)
        .map(|published_at| published_at.and_utc())
}

fn margin_detail_row_from_map(
    item: &Map<String, Value>,
    open_dates: &[NaiveDate],
) -> Option<MarketStockMarginDetail> {
    let symbol = get_str(item, "ts_code");
    if symbol.trim().is_empty() {
        return None;
    }
    let trade_date = to_date(&get_str(item, "trade_date"))?;
    let available_at = margin_detail_available_at_from_open_dates(trade_date, open_dates);
    Some(MarketStockMarginDetail {
        symbol,
        trade_date,
        name: non_empty_opt_str(get_str(item, "name")),
        rzye: to_opt_decimal(get_f64(item, "rzye")),
        rqye: to_opt_decimal(get_f64(item, "rqye")),
        rzmre: to_opt_decimal(get_f64(item, "rzmre")),
        rqyl: to_opt_decimal(get_f64(item, "rqyl")),
        rzche: to_opt_decimal(get_f64(item, "rzche")),
        rqchl: to_opt_decimal(get_f64(item, "rqchl")),
        rqmcl: to_opt_decimal(get_f64(item, "rqmcl")),
        rzrqye: to_opt_decimal(get_f64(item, "rzrqye")),
        available_at,
        source_published_at: margin_detail_source_published_at(available_at),
        raw_payload: raw_payload(item),
    })
}

async fn load_margin_detail_available_open_dates(
    pool: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<NaiveDate>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT DISTINCT trade_date
         FROM market_trade_calendar
         WHERE is_open = true
           AND trade_date > $1
           AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(start)
    .bind(end + Duration::days(14))
    .fetch_all(pool)
    .await
}

pub async fn sync_margin_detail(
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
    if s > e {
        return Err("margin_detail start_date cannot be after end_date".into());
    }

    repository::create_sync_task_with_context(
        pool,
        &task_id,
        "margin_detail",
        "tushare:margin_detail",
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
        "security-level margin detail raw PIT sync",
        "tushare:margin_detail",
        &["market_stock_margin_detail"],
        s,
        e,
    )
    .await?;

    let page_limit = 6_000usize;
    let open_dates = load_margin_detail_available_open_dates(pool, s, e).await?;
    let mut total_rows = 0usize;
    let mut ok = 0usize;
    let mut failed = 0usize;

    if symbols.is_empty() {
        let trade_dates = load_moneyflow_full_market_trade_dates(pool, s, e).await?;
        let total = trade_dates.len() as i32;
        repository::heartbeat_sync_task(pool, &task_id, total, 0, 0, 0).await?;
        for trade_day in &trade_dates {
            let trade_date = trade_day.format("%Y%m%d").to_string();
            let mut offset = 0usize;
            let mut day_failed = false;
            loop {
                match client
                    .margin_detail(
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
                        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                        let row_count = maps.len();
                        let rows: Vec<MarketStockMarginDetail> = maps
                            .iter()
                            .filter_map(|item| margin_detail_row_from_map(item, &open_dates))
                            .collect();
                        if !rows.is_empty() {
                            total_rows += rows.len();
                            repository::upsert_margin_detail_batch(
                                pool,
                                &rows,
                                dv_id,
                                "tushare:margin_detail",
                            )
                            .await?;
                        }
                        if row_count < page_limit {
                            break;
                        }
                        offset += page_limit;
                    }
                    Err(error) => {
                        warn!("margin_detail {} failed: {}", trade_date, error);
                        failed += 1;
                        day_failed = true;
                        break;
                    }
                }
            }
            if !day_failed {
                ok += 1;
            }
            repository::update_sync_task(
                pool,
                &task_id,
                "running",
                total,
                ok as i32,
                failed as i32,
            )
            .await?;
        }
        repository::update_sync_task(
            pool,
            &task_id,
            if failed > 0 { "partial" } else { "completed" },
            total,
            ok as i32,
            failed as i32,
        )
        .await?;
    } else {
        repository::heartbeat_sync_task(pool, &task_id, symbols.len() as i32, 0, 0, 0).await?;
        for symbol in symbols {
            let mut offset = 0usize;
            let mut symbol_failed = false;
            loop {
                match client
                    .margin_detail(
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
                        let rows: Vec<MarketStockMarginDetail> = maps
                            .iter()
                            .filter_map(|item| margin_detail_row_from_map(item, &open_dates))
                            .collect();
                        if !rows.is_empty() {
                            total_rows += rows.len();
                            repository::upsert_margin_detail_batch(
                                pool,
                                &rows,
                                dv_id,
                                "tushare:margin_detail",
                            )
                            .await?;
                        }
                        if row_count < page_limit {
                            break;
                        }
                        offset += page_limit;
                    }
                    Err(error) => {
                        warn!("{} margin_detail failed: {}", symbol, error);
                        failed += 1;
                        symbol_failed = true;
                        break;
                    }
                }
            }
            if !symbol_failed {
                ok += 1;
            }
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
        "margin_detail 同步完成: rows={}, ok={}, failed={}",
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

        let resp = client
            .moneyflow_hsgt(None, Some(&start_str), Some(&end_str))
            .await?;
        if let Some(data) = resp.data {
            for item in data.items {
                let date = item.first().and_then(|v| v.as_str()).unwrap_or("");
                if date.is_empty() {
                    continue;
                }
                let nf = item
                    .get(1)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let sf = item
                    .get(2)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let nb = item
                    .get(3)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let sb = item
                    .get(4)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
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
        info!(
            ?batch_start,
            ?batch_end,
            batch_rows = total,
            "HSGT 同步进度"
        );
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

        let resp = client
            .margin(None, Some(&start_str), Some(&end_str))
            .await?;
        if let Some(data) = resp.data {
            for item in data.items {
                let date = item.first().and_then(|v| v.as_str()).unwrap_or("");
                if date.is_empty() {
                    continue;
                }
                let exchange = item.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let rzye = item
                    .get(2)
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                let rqye = item
                    .get(3)
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                let rzrqye = item
                    .get(4)
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
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
        info!(
            ?batch_start,
            ?batch_end,
            batch_rows = total,
            "Margin 同步进度"
        );
        batch_start = batch_end + chrono::Duration::days(1);
    }
    Ok(total)
}

// ─── sync_block_trade (大宗交易) ─────────────────────────────────

pub async fn sync_block_trade(
    pool: &PgPool,
    client: &TushareClient,
    task_id: &str,
    start: &str,
    end: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    let mut total = 0usize;
    let mut trade_dates: Vec<NaiveDate> = sqlx::query_scalar(
        r#"
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open
          AND trade_date BETWEEN $1 AND $2
        ORDER BY trade_date
        "#,
    )
    .bind(s)
    .bind(e)
    .fetch_all(pool)
    .await?;

    if trade_dates.is_empty() {
        let mut current = s;
        while current <= e {
            trade_dates.push(current);
            current += chrono::Duration::days(1);
        }
    }

    let total_days = trade_dates.len();
    let progress_interval = event_sync_progress_interval(total_days, 100);
    repository::heartbeat_sync_task(pool, task_id, total_days as i32, 0, 0, 0).await?;

    for (day_idx, trade_date) in trade_dates.into_iter().enumerate() {
        let trade_date_str = trade_date.format("%Y%m%d").to_string();
        let resp = client
            .block_trade(None, Some(&trade_date_str), None, None)
            .await?;
        if let Some(data) = resp.data {
            for (idx, item) in data.items.into_iter().enumerate() {
                let source_row_no = (idx + 1) as i32;
                let code = item.first().and_then(|v| v.as_str()).unwrap_or("");
                let date = item.get(1).and_then(|v| v.as_str()).unwrap_or("");
                if code.is_empty() || date.is_empty() {
                    continue;
                }
                let numeric = |idx: usize| {
                    item.get(idx)
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0)
                };
                let price = numeric(2);
                let vol = numeric(3);
                let amount = numeric(4);
                let buyer = item.get(5).and_then(|v| v.as_str()).unwrap_or("");
                let seller = item.get(6).and_then(|v| v.as_str()).unwrap_or("");
                let row_trade_date = NaiveDate::parse_from_str(date, "%Y%m%d")?;
                let available_at = row_trade_date + chrono::Duration::days(1);

                sqlx::query(
                    r#"
                    INSERT INTO market_stock_block_trade (
                        source_row_no, ts_code, trade_date, price, vol, amount, buyer, seller, available_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (trade_date, source_row_no)
                    DO UPDATE SET ts_code = EXCLUDED.ts_code,
                                  price = EXCLUDED.price,
                                  vol = EXCLUDED.vol,
                                  amount = EXCLUDED.amount,
                                  buyer = EXCLUDED.buyer,
                                  seller = EXCLUDED.seller,
                                  available_at = EXCLUDED.available_at,
                                  updated_at = now()
                    "#,
                )
                .bind(source_row_no)
                .bind(code)
                .bind(row_trade_date)
                .bind(price)
                .bind(vol)
                .bind(amount)
                .bind(buyer)
                .bind(seller)
                .bind(available_at)
                .execute(pool)
                .await?;
                total += 1;
            }
        }
        info!(?trade_date, batch_rows = total, "Block trade 同步进度");
        let completed_days = day_idx + 1;
        if completed_days % progress_interval == 0 || completed_days == total_days {
            let progress = if total_days > 0 {
                ((completed_days * 100) / total_days).min(99) as i32
            } else {
                0
            };
            repository::heartbeat_sync_task(
                pool,
                task_id,
                total_days as i32,
                completed_days as i32,
                0,
                progress,
            )
            .await?;
        }
    }

    Ok(total)
}

#[derive(Debug, Clone)]
struct EquityPledgeStatRow {
    symbol: String,
    end_date: NaiveDate,
    pledge_count: Option<i32>,
    unrest_pledge: Decimal,
    rest_pledge: Decimal,
    total_share: Decimal,
    pledge_ratio: Decimal,
    available_at: NaiveDate,
    raw_payload: Value,
}

#[derive(Debug, Clone)]
struct EquityPledgeDetailRow {
    symbol: String,
    ann_date: NaiveDate,
    holder_name: String,
    pledge_amount: Decimal,
    pledge_start_date: Option<NaiveDate>,
    pledge_end_date: Option<NaiveDate>,
    is_release: Option<String>,
    release_date: Option<NaiveDate>,
    pledgor: String,
    holding_amount: Option<Decimal>,
    pledged_amount: Option<Decimal>,
    p_total_ratio: Option<Decimal>,
    h_total_ratio: Option<Decimal>,
    is_buyback: Option<String>,
    available_at: NaiveDate,
    raw_payload: Value,
    source_row_hash: String,
}

fn equity_pledge_stat_row_from_map(item: &Map<String, Value>) -> Option<EquityPledgeStatRow> {
    let symbol = get_str(item, "ts_code");
    let end_date = to_date(&get_str(item, "end_date"))?;
    if symbol.is_empty() {
        return None;
    }

    Some(EquityPledgeStatRow {
        symbol,
        end_date,
        pledge_count: get_i64(item, "pledge_count").map(|value| value as i32),
        unrest_pledge: to_decimal(get_f64(item, "unrest_pledge")),
        rest_pledge: to_decimal(get_f64(item, "rest_pledge")),
        total_share: to_decimal(get_f64(item, "total_share")),
        pledge_ratio: to_decimal(get_f64(item, "pledge_ratio")),
        available_at: end_date + Duration::days(1),
        raw_payload: raw_payload(item),
    })
}

fn equity_pledge_detail_row_from_map(item: &Map<String, Value>) -> Option<EquityPledgeDetailRow> {
    let symbol = get_str(item, "ts_code");
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    if symbol.is_empty() {
        return None;
    }

    let holder_name = get_str(item, "holder_name");
    let pledge_amount = to_decimal(get_f64(item, "pledge_amount"));
    let pledge_start_date = to_date(&get_str(item, "start_date"));
    let pledge_end_date = to_date(&get_str(item, "end_date"));
    let pledgor = get_str(item, "pledgor");
    let release_date = to_date(&get_str(item, "release_date"));
    let source_row_hash = stable_source_row_hash(&[
        symbol.clone(),
        ann_date.format("%Y%m%d").to_string(),
        holder_name.clone(),
        value_key_part(item, "pledge_amount"),
        value_key_part(item, "start_date"),
        value_key_part(item, "end_date"),
        value_key_part(item, "is_release"),
        value_key_part(item, "release_date"),
        pledgor.clone(),
    ]);

    Some(EquityPledgeDetailRow {
        symbol,
        ann_date,
        holder_name,
        pledge_amount,
        pledge_start_date,
        pledge_end_date,
        is_release: non_empty_opt_str(get_str(item, "is_release")),
        release_date,
        pledgor,
        holding_amount: to_opt_decimal(get_f64(item, "holding_amount")),
        pledged_amount: to_opt_decimal(get_f64(item, "pledged_amount")),
        p_total_ratio: to_opt_decimal(get_f64(item, "p_total_ratio")),
        h_total_ratio: to_opt_decimal(get_f64(item, "h_total_ratio")),
        is_buyback: non_empty_opt_str(get_str(item, "is_buyback")),
        available_at: ann_date,
        raw_payload: raw_payload(item),
        source_row_hash,
    })
}

async fn upsert_equity_pledge_stat_rows(
    pool: &PgPool,
    rows: &[EquityPledgeStatRow],
    task_id: &str,
    source: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut saved = 0usize;
    for row in rows {
        sqlx::query(
            r#"
            INSERT INTO market_stock_pledge_stat (
                symbol, end_date, pledge_count, unrest_pledge, rest_pledge,
                total_share, pledge_ratio, available_at, raw_payload, source, data_version_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (symbol, end_date)
            DO UPDATE SET pledge_count = EXCLUDED.pledge_count,
                          unrest_pledge = EXCLUDED.unrest_pledge,
                          rest_pledge = EXCLUDED.rest_pledge,
                          total_share = EXCLUDED.total_share,
                          pledge_ratio = EXCLUDED.pledge_ratio,
                          available_at = EXCLUDED.available_at,
                          raw_payload = EXCLUDED.raw_payload,
                          source = EXCLUDED.source,
                          data_version_id = EXCLUDED.data_version_id,
                          updated_at = now()
            "#,
        )
        .bind(&row.symbol)
        .bind(row.end_date)
        .bind(row.pledge_count)
        .bind(row.unrest_pledge)
        .bind(row.rest_pledge)
        .bind(row.total_share)
        .bind(row.pledge_ratio)
        .bind(row.available_at)
        .bind(&row.raw_payload)
        .bind(source)
        .bind(task_id)
        .execute(pool)
        .await?;
        saved += 1;
    }
    Ok(saved)
}

async fn upsert_equity_pledge_detail_rows(
    pool: &PgPool,
    rows: &[EquityPledgeDetailRow],
    task_id: &str,
    source: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut saved = 0usize;
    for row in rows {
        sqlx::query(
            r#"
            INSERT INTO market_stock_pledge_detail (
                symbol, ann_date, holder_name, pledge_amount, pledge_start_date,
                pledge_end_date, is_release, release_date, pledgor, holding_amount,
                pledged_amount, p_total_ratio, h_total_ratio, is_buyback,
                available_at, raw_payload, source, data_version_id, source_row_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (symbol, ann_date, source_row_hash)
            DO UPDATE SET pledge_end_date = EXCLUDED.pledge_end_date,
                          is_release = EXCLUDED.is_release,
                          release_date = EXCLUDED.release_date,
                          holding_amount = EXCLUDED.holding_amount,
                          pledged_amount = EXCLUDED.pledged_amount,
                          p_total_ratio = EXCLUDED.p_total_ratio,
                          h_total_ratio = EXCLUDED.h_total_ratio,
                          is_buyback = EXCLUDED.is_buyback,
                          available_at = EXCLUDED.available_at,
                          raw_payload = EXCLUDED.raw_payload,
                          source = EXCLUDED.source,
                          data_version_id = EXCLUDED.data_version_id,
                          updated_at = now()
            "#,
        )
        .bind(&row.symbol)
        .bind(row.ann_date)
        .bind(&row.holder_name)
        .bind(row.pledge_amount)
        .bind(row.pledge_start_date)
        .bind(row.pledge_end_date)
        .bind(&row.is_release)
        .bind(row.release_date)
        .bind(&row.pledgor)
        .bind(row.holding_amount)
        .bind(row.pledged_amount)
        .bind(row.p_total_ratio)
        .bind(row.h_total_ratio)
        .bind(&row.is_buyback)
        .bind(row.available_at)
        .bind(&row.raw_payload)
        .bind(source)
        .bind(task_id)
        .bind(&row.source_row_hash)
        .execute(pool)
        .await?;
        saved += 1;
    }
    Ok(saved)
}

async fn equity_pledge_stat_dates(
    pool: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<NaiveDate>, Box<dyn std::error::Error>> {
    let dates: Vec<NaiveDate> = sqlx::query_scalar(
        r#"
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open
          AND trade_date BETWEEN $1 AND $2
        ORDER BY trade_date
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;

    if !dates.is_empty() {
        return Ok(dates);
    }

    let mut fallback = Vec::new();
    let mut current = start;
    while current <= end {
        fallback.push(current);
        current += Duration::days(1);
    }
    Ok(fallback)
}

async fn fetch_equity_pledge_detail_rows(
    client: &TushareClient,
    symbol: Option<&str>,
    target_label: &str,
    window_start: NaiveDate,
    window_end: NaiveDate,
    call_timeout: StdDuration,
    page_limit: usize,
) -> Result<Vec<EquityPledgeDetailRow>, String> {
    let start_date = window_start.format("%Y%m%d").to_string();
    let end_date = window_end.format("%Y%m%d").to_string();
    let mut offset = 0usize;
    let mut rows = Vec::new();
    loop {
        let resp = bounded_tushare_symbol_call(
            "equity_pledge_pressure/pledge_detail",
            target_label,
            call_timeout,
            client.pledge_detail(
                symbol,
                None,
                Some(&start_date),
                Some(&end_date),
                Some(page_limit),
                Some(offset),
            ),
        )
        .await?;
        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
        let row_count = maps.len();
        rows.extend(
            maps.iter()
                .filter_map(equity_pledge_detail_row_from_map)
                .filter(|row| date_in_range(row.ann_date, window_start, window_end)),
        );
        if row_count < page_limit {
            break;
        }
        offset += page_limit;
    }
    Ok(rows)
}

pub async fn sync_equity_pledge_pressure(
    pool: &PgPool,
    client: &TushareClient,
    task_id: &str,
    symbols: &[String],
    start: &str,
    end: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    if s > e {
        return Err("equity_pledge_pressure start_date cannot be after end_date".into());
    }

    repository::create_sync_task_with_context(
        pool,
        task_id,
        "equity_pledge_pressure",
        "tushare:equity_pledge_pressure",
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
        task_id,
        "equity pledge pressure raw PIT sync",
        "tushare:equity_pledge_pressure",
        &["market_stock_pledge_stat", "market_stock_pledge_detail"],
        s,
        e,
    )
    .await?;

    const PAGE_LIMIT: usize = 5_000;
    let call_timeout = tushare_symbol_call_timeout();
    let stat_dates = if symbols.is_empty() {
        equity_pledge_stat_dates(pool, s, e).await?
    } else {
        Vec::new()
    };
    let detail_windows = quarters_in_range(s, e);
    let total_units = if symbols.is_empty() {
        stat_dates.len() + detail_windows.len()
    } else {
        symbols.len() + symbols.len() * detail_windows.len()
    };
    let progress_interval = event_sync_progress_interval(total_units, 20);
    repository::heartbeat_sync_task(pool, task_id, total_units as i32, 0, 0, 0).await?;

    let mut total_rows = 0usize;
    let mut completed_units = 0usize;
    let mut failed_units = 0usize;

    if symbols.is_empty() {
        for end_date in stat_dates {
            let end_date_str = end_date.format("%Y%m%d").to_string();
            let mut offset = 0usize;
            let mut rows = Vec::new();
            loop {
                let resp = bounded_tushare_symbol_call(
                    "equity_pledge_pressure/pledge_stat",
                    &end_date_str,
                    call_timeout,
                    client.pledge_stat(None, Some(&end_date_str), Some(PAGE_LIMIT), Some(offset)),
                )
                .await;
                match resp {
                    Ok(resp) => {
                        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                        let row_count = maps.len();
                        rows.extend(
                            maps.iter()
                                .filter_map(equity_pledge_stat_row_from_map)
                                .filter(|row| date_in_range(row.end_date, s, e)),
                        );
                        if row_count < PAGE_LIMIT {
                            break;
                        }
                        offset += PAGE_LIMIT;
                    }
                    Err(message) => {
                        failed_units += 1;
                        repository::upsert_sync_attempt(
                            pool,
                            "equity_pledge_stat_end_date",
                            &end_date_str,
                            end_date,
                            end_date,
                            task_id,
                            "failed",
                            rows.len() as i64,
                            Some(message.as_str()),
                        )
                        .await?;
                        repository::update_sync_task_with_error(
                            pool,
                            task_id,
                            "partial",
                            total_units as i32,
                            completed_units as i32,
                            failed_units as i32,
                            &message,
                        )
                        .await?;
                        return Err(format!(
                            "equity_pledge_pressure pledge_stat {} failed: {}",
                            end_date_str, message
                        )
                        .into());
                    }
                }
            }
            let saved =
                upsert_equity_pledge_stat_rows(pool, &rows, task_id, "tushare:pledge_stat").await?;
            total_rows += saved;
            completed_units += 1;
            repository::upsert_sync_attempt(
                pool,
                "equity_pledge_stat_end_date",
                &end_date_str,
                end_date,
                end_date,
                task_id,
                "completed",
                saved as i64,
                None,
            )
            .await?;
            if completed_units % progress_interval == 0 || completed_units == total_units {
                let progress = ((completed_units * 100) / total_units.max(1)).min(99) as i32;
                repository::heartbeat_sync_task(
                    pool,
                    task_id,
                    total_units as i32,
                    completed_units as i32,
                    failed_units as i32,
                    progress,
                )
                .await?;
            }
        }
    } else {
        for symbol in symbols {
            let mut offset = 0usize;
            let mut rows = Vec::new();
            loop {
                let resp = bounded_tushare_symbol_call(
                    "equity_pledge_pressure/pledge_stat",
                    symbol,
                    call_timeout,
                    client.pledge_stat(Some(symbol), None, Some(PAGE_LIMIT), Some(offset)),
                )
                .await;
                match resp {
                    Ok(resp) => {
                        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                        let row_count = maps.len();
                        rows.extend(
                            maps.iter()
                                .filter_map(equity_pledge_stat_row_from_map)
                                .filter(|row| date_in_range(row.end_date, s, e)),
                        );
                        if row_count < PAGE_LIMIT {
                            break;
                        }
                        offset += PAGE_LIMIT;
                    }
                    Err(message) => {
                        failed_units += 1;
                        repository::upsert_sync_attempt(
                            pool,
                            "equity_pledge_stat_symbol",
                            symbol,
                            s,
                            e,
                            task_id,
                            "failed",
                            rows.len() as i64,
                            Some(message.as_str()),
                        )
                        .await?;
                        repository::update_sync_task_with_error(
                            pool,
                            task_id,
                            "partial",
                            total_units as i32,
                            completed_units as i32,
                            failed_units as i32,
                            &message,
                        )
                        .await?;
                        return Err(format!(
                            "equity_pledge_pressure pledge_stat {} failed: {}",
                            symbol, message
                        )
                        .into());
                    }
                }
            }
            let saved =
                upsert_equity_pledge_stat_rows(pool, &rows, task_id, "tushare:pledge_stat").await?;
            total_rows += saved;
            completed_units += 1;
            repository::upsert_sync_attempt(
                pool,
                "equity_pledge_stat_symbol",
                symbol,
                s,
                e,
                task_id,
                "completed",
                saved as i64,
                None,
            )
            .await?;
        }
    }

    let detail_scopes: Vec<Option<&str>> = if symbols.is_empty() {
        vec![None]
    } else {
        symbols.iter().map(|symbol| Some(symbol.as_str())).collect()
    };
    for symbol in detail_scopes {
        let attempt_key = symbol.unwrap_or("all");
        for (window_start, window_end) in &detail_windows {
            let start_date = window_start.format("%Y%m%d").to_string();
            let end_date = window_end.format("%Y%m%d").to_string();
            let target_label = format!("{}:{}-{}", attempt_key, start_date, end_date);
            let rows = match fetch_equity_pledge_detail_rows(
                client,
                symbol,
                &target_label,
                *window_start,
                *window_end,
                call_timeout,
                PAGE_LIMIT,
            )
            .await
            {
                Ok(rows) => rows,
                Err(message) if message.contains("timed out") && window_start < window_end => {
                    let mut fallback_rows = Vec::new();
                    for (month_start, month_end) in months_in_range(*window_start, *window_end) {
                        let month_label = format!(
                            "{}:{}-{}",
                            attempt_key,
                            month_start.format("%Y%m%d"),
                            month_end.format("%Y%m%d")
                        );
                        let month_result = fetch_equity_pledge_detail_rows(
                            client,
                            symbol,
                            &month_label,
                            month_start,
                            month_end,
                            call_timeout,
                            PAGE_LIMIT,
                        )
                        .await;
                        match month_result {
                            Ok(mut month_rows) => fallback_rows.append(&mut month_rows),
                            Err(month_message) => {
                                let combined_message = format!(
                                    "{}; monthly fallback {} failed: {}",
                                    message, month_label, month_message
                                );
                                failed_units += 1;
                                repository::upsert_sync_attempt(
                                    pool,
                                    "equity_pledge_detail_ann_date",
                                    attempt_key,
                                    *window_start,
                                    *window_end,
                                    task_id,
                                    "failed",
                                    fallback_rows.len() as i64,
                                    Some(combined_message.as_str()),
                                )
                                .await?;
                                repository::update_sync_task_with_error(
                                    pool,
                                    task_id,
                                    "partial",
                                    total_units as i32,
                                    completed_units as i32,
                                    failed_units as i32,
                                    &combined_message,
                                )
                                .await?;
                                return Err(format!(
                                    "equity_pledge_pressure pledge_detail {} failed: {}",
                                    target_label, combined_message
                                )
                                .into());
                            }
                        }
                    }
                    fallback_rows
                }
                Err(message) => {
                    failed_units += 1;
                    repository::upsert_sync_attempt(
                        pool,
                        "equity_pledge_detail_ann_date",
                        attempt_key,
                        *window_start,
                        *window_end,
                        task_id,
                        "failed",
                        0,
                        Some(message.as_str()),
                    )
                    .await?;
                    repository::update_sync_task_with_error(
                        pool,
                        task_id,
                        "partial",
                        total_units as i32,
                        completed_units as i32,
                        failed_units as i32,
                        &message,
                    )
                    .await?;
                    return Err(format!(
                        "equity_pledge_pressure pledge_detail {} failed: {}",
                        target_label, message
                    )
                    .into());
                }
            };
            let saved =
                upsert_equity_pledge_detail_rows(pool, &rows, task_id, "tushare:pledge_detail")
                    .await?;
            total_rows += saved;
            completed_units += 1;
            repository::upsert_sync_attempt(
                pool,
                "equity_pledge_detail_ann_date",
                attempt_key,
                *window_start,
                *window_end,
                task_id,
                "completed",
                saved as i64,
                None,
            )
            .await?;
            let progress = ((completed_units * 100) / total_units.max(1)).min(99) as i32;
            repository::heartbeat_sync_task(
                pool,
                task_id,
                total_units as i32,
                completed_units as i32,
                failed_units as i32,
                progress,
            )
            .await?;
        }
    }

    repository::update_sync_task(
        pool,
        task_id,
        if failed_units > 0 {
            "partial"
        } else {
            "completed"
        },
        total_units as i32,
        completed_units as i32,
        failed_units as i32,
    )
    .await?;
    info!(
        total_rows,
        completed_units, failed_units, "equity_pledge_pressure 同步完成"
    );
    Ok(total_rows)
}

// ─── sync_shareholder_structure ─────────────────────────────────

#[derive(Debug, Clone)]
struct ShareholderHolderNumberRow {
    symbol: String,
    ann_date: NaiveDate,
    end_date: NaiveDate,
    holder_num: Option<i64>,
    available_at: NaiveDate,
    raw_payload: Value,
    source_row_hash: String,
}

#[derive(Debug, Clone)]
struct ShareholderTop10HolderRow {
    symbol: String,
    ann_date: NaiveDate,
    end_date: NaiveDate,
    holder_name: String,
    hold_amount: Option<Decimal>,
    hold_ratio: Option<Decimal>,
    hold_float_ratio: Option<Decimal>,
    hold_change: Option<Decimal>,
    holder_type: Option<String>,
    available_at: NaiveDate,
    raw_payload: Value,
    source_row_hash: String,
}

#[derive(Debug, Clone)]
struct ShareholderHolderTradeRow {
    symbol: String,
    ann_date: NaiveDate,
    holder_name: String,
    holder_type: Option<String>,
    in_de: Option<String>,
    change_vol: Option<Decimal>,
    change_ratio: Option<Decimal>,
    after_share: Option<Decimal>,
    after_ratio: Option<Decimal>,
    avg_price: Option<Decimal>,
    total_share: Option<Decimal>,
    begin_date: Option<NaiveDate>,
    close_date: Option<NaiveDate>,
    available_at: NaiveDate,
    raw_payload: Value,
    source_row_hash: String,
}

#[derive(Debug, Clone)]
struct ShareholderStructureSourceFilterFlags {
    holder_number: bool,
    holder_trade: bool,
    top10_holders: bool,
    top10_float_holders: bool,
}

fn shareholder_structure_source_filter_flags(
    source_filters: &[String],
) -> Result<ShareholderStructureSourceFilterFlags, String> {
    if source_filters.is_empty() {
        return Ok(ShareholderStructureSourceFilterFlags {
            holder_number: true,
            holder_trade: true,
            top10_holders: true,
            top10_float_holders: true,
        });
    }

    let mut flags = ShareholderStructureSourceFilterFlags {
        holder_number: false,
        holder_trade: false,
        top10_holders: false,
        top10_float_holders: false,
    };
    for source in source_filters {
        match source.trim().to_ascii_lowercase().as_str() {
            "holder_number" | "stk_holdernumber" | "shareholder_holder_number_ann_date" => {
                flags.holder_number = true;
            }
            "holder_trade" | "stk_holdertrade" | "shareholder_holder_trade_ann_date" => {
                flags.holder_trade = true;
            }
            "top10_holders" | "shareholder_top10_holders_ann_date" => {
                flags.top10_holders = true;
            }
            "top10_float_holders"
            | "top10_floatholders"
            | "shareholder_top10_float_holders_ann_date" => {
                flags.top10_float_holders = true;
            }
            other => {
                return Err(format!(
                    "unsupported shareholder_structure source_filter: {other}"
                ));
            }
        }
    }
    Ok(flags)
}

fn shareholder_holder_number_row_from_map(
    item: &Map<String, Value>,
) -> Option<ShareholderHolderNumberRow> {
    let symbol = required_text(item, "ts_code")?;
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let end_date = to_date(&get_str(item, "end_date"))?;
    let source_row_hash = stable_source_row_hash(&[
        symbol.clone(),
        ann_date.format("%Y%m%d").to_string(),
        end_date.format("%Y%m%d").to_string(),
        value_key_part(item, "holder_num"),
    ]);

    Some(ShareholderHolderNumberRow {
        symbol,
        ann_date,
        end_date,
        holder_num: get_i64(item, "holder_num"),
        available_at: ann_date,
        raw_payload: raw_payload(item),
        source_row_hash,
    })
}

fn shareholder_top10_holder_row_from_map(
    item: &Map<String, Value>,
    _float_holder: bool,
) -> Option<ShareholderTop10HolderRow> {
    let symbol = required_text(item, "ts_code")?;
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let end_date = to_date(&get_str(item, "end_date"))?;
    let holder_name = get_str(item, "holder_name");
    let source_row_hash = stable_source_row_hash(&[
        symbol.clone(),
        ann_date.format("%Y%m%d").to_string(),
        end_date.format("%Y%m%d").to_string(),
        holder_name.clone(),
        value_key_part(item, "hold_amount"),
        value_key_part(item, "hold_ratio"),
        value_key_part(item, "hold_float_ratio"),
        value_key_part(item, "hold_change"),
        value_key_part(item, "holder_type"),
    ]);

    Some(ShareholderTop10HolderRow {
        symbol,
        ann_date,
        end_date,
        holder_name,
        hold_amount: to_opt_decimal(get_f64(item, "hold_amount")),
        hold_ratio: to_opt_decimal(get_f64(item, "hold_ratio")),
        hold_float_ratio: to_opt_decimal(get_f64(item, "hold_float_ratio")),
        hold_change: to_opt_decimal(get_f64(item, "hold_change")),
        holder_type: non_empty_opt_str(get_str(item, "holder_type")),
        available_at: ann_date,
        raw_payload: raw_payload(item),
        source_row_hash,
    })
}

fn shareholder_holder_trade_row_from_map(
    item: &Map<String, Value>,
) -> Option<ShareholderHolderTradeRow> {
    let symbol = required_text(item, "ts_code")?;
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let holder_name = get_str(item, "holder_name");
    let source_row_hash = stable_source_row_hash(&[
        symbol.clone(),
        ann_date.format("%Y%m%d").to_string(),
        holder_name.clone(),
        value_key_part(item, "holder_type"),
        value_key_part(item, "in_de"),
        value_key_part(item, "change_vol"),
        value_key_part(item, "change_ratio"),
        value_key_part(item, "after_share"),
        value_key_part(item, "after_ratio"),
        value_key_part(item, "avg_price"),
        value_key_part(item, "total_share"),
        value_key_part(item, "begin_date"),
        value_key_part(item, "close_date"),
    ]);

    Some(ShareholderHolderTradeRow {
        symbol,
        ann_date,
        holder_name,
        holder_type: non_empty_opt_str(get_str(item, "holder_type")),
        in_de: non_empty_opt_str(get_str(item, "in_de")),
        change_vol: to_opt_decimal(get_f64(item, "change_vol")),
        change_ratio: to_opt_decimal(get_f64(item, "change_ratio")),
        after_share: to_opt_decimal(get_f64(item, "after_share")),
        after_ratio: to_opt_decimal(get_f64(item, "after_ratio")),
        avg_price: to_opt_decimal(get_f64(item, "avg_price")),
        total_share: to_opt_decimal(get_f64(item, "total_share")),
        begin_date: to_date(&get_str(item, "begin_date")),
        close_date: to_date(&get_str(item, "close_date")),
        available_at: ann_date,
        raw_payload: raw_payload(item),
        source_row_hash,
    })
}

async fn upsert_shareholder_holder_number_rows(
    pool: &PgPool,
    rows: &[ShareholderHolderNumberRow],
    task_id: &str,
    source: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut saved = 0usize;
    for row in rows {
        sqlx::query(
            r#"
            INSERT INTO market_stock_holder_number (
                symbol, ann_date, end_date, holder_num, available_at,
                raw_payload, source, data_version_id, source_row_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (symbol, ann_date, end_date)
            DO UPDATE SET holder_num = EXCLUDED.holder_num,
                          available_at = EXCLUDED.available_at,
                          raw_payload = EXCLUDED.raw_payload,
                          source = EXCLUDED.source,
                          data_version_id = EXCLUDED.data_version_id,
                          source_row_hash = EXCLUDED.source_row_hash,
                          updated_at = now()
            "#,
        )
        .bind(&row.symbol)
        .bind(row.ann_date)
        .bind(row.end_date)
        .bind(row.holder_num)
        .bind(row.available_at)
        .bind(&row.raw_payload)
        .bind(source)
        .bind(task_id)
        .bind(&row.source_row_hash)
        .execute(pool)
        .await?;
        saved += 1;
    }
    Ok(saved)
}

async fn upsert_shareholder_top10_holder_rows(
    pool: &PgPool,
    table: &str,
    rows: &[ShareholderTop10HolderRow],
    task_id: &str,
    source: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let table_name = match table {
        "market_stock_top10_holders" => "market_stock_top10_holders",
        "market_stock_top10_float_holders" => "market_stock_top10_float_holders",
        _ => return Err(format!("unsupported shareholder top10 table: {table}").into()),
    };
    let sql = format!(
        r#"
        INSERT INTO {table_name} (
            symbol, ann_date, end_date, holder_name, hold_amount, hold_ratio,
            hold_float_ratio, hold_change, holder_type, available_at,
            raw_payload, source, data_version_id, source_row_hash
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (symbol, ann_date, end_date, source_row_hash)
        DO UPDATE SET holder_name = EXCLUDED.holder_name,
                      hold_amount = EXCLUDED.hold_amount,
                      hold_ratio = EXCLUDED.hold_ratio,
                      hold_float_ratio = EXCLUDED.hold_float_ratio,
                      hold_change = EXCLUDED.hold_change,
                      holder_type = EXCLUDED.holder_type,
                      available_at = EXCLUDED.available_at,
                      raw_payload = EXCLUDED.raw_payload,
                      source = EXCLUDED.source,
                      data_version_id = EXCLUDED.data_version_id,
                      updated_at = now()
        "#
    );

    let mut saved = 0usize;
    for row in rows {
        sqlx::query(&sql)
            .bind(&row.symbol)
            .bind(row.ann_date)
            .bind(row.end_date)
            .bind(&row.holder_name)
            .bind(row.hold_amount)
            .bind(row.hold_ratio)
            .bind(row.hold_float_ratio)
            .bind(row.hold_change)
            .bind(&row.holder_type)
            .bind(row.available_at)
            .bind(&row.raw_payload)
            .bind(source)
            .bind(task_id)
            .bind(&row.source_row_hash)
            .execute(pool)
            .await?;
        saved += 1;
    }
    Ok(saved)
}

async fn upsert_shareholder_holder_trade_rows(
    pool: &PgPool,
    rows: &[ShareholderHolderTradeRow],
    task_id: &str,
    source: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut saved = 0usize;
    for row in rows {
        sqlx::query(
            r#"
            INSERT INTO market_stock_holder_trade (
                symbol, ann_date, holder_name, holder_type, in_de,
                change_vol, change_ratio, after_share, after_ratio, avg_price,
                total_share, begin_date, close_date, available_at,
                raw_payload, source, data_version_id, source_row_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (symbol, ann_date, source_row_hash)
            DO UPDATE SET holder_name = EXCLUDED.holder_name,
                          holder_type = EXCLUDED.holder_type,
                          in_de = EXCLUDED.in_de,
                          change_vol = EXCLUDED.change_vol,
                          change_ratio = EXCLUDED.change_ratio,
                          after_share = EXCLUDED.after_share,
                          after_ratio = EXCLUDED.after_ratio,
                          avg_price = EXCLUDED.avg_price,
                          total_share = EXCLUDED.total_share,
                          begin_date = EXCLUDED.begin_date,
                          close_date = EXCLUDED.close_date,
                          available_at = EXCLUDED.available_at,
                          raw_payload = EXCLUDED.raw_payload,
                          source = EXCLUDED.source,
                          data_version_id = EXCLUDED.data_version_id,
                          updated_at = now()
            "#,
        )
        .bind(&row.symbol)
        .bind(row.ann_date)
        .bind(&row.holder_name)
        .bind(&row.holder_type)
        .bind(&row.in_de)
        .bind(row.change_vol)
        .bind(row.change_ratio)
        .bind(row.after_share)
        .bind(row.after_ratio)
        .bind(row.avg_price)
        .bind(row.total_share)
        .bind(row.begin_date)
        .bind(row.close_date)
        .bind(row.available_at)
        .bind(&row.raw_payload)
        .bind(source)
        .bind(task_id)
        .bind(&row.source_row_hash)
        .execute(pool)
        .await?;
        saved += 1;
    }
    Ok(saved)
}

async fn load_shareholder_structure_symbols(
    pool: &PgPool,
    symbols: &[String],
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if !symbols.is_empty() {
        return Ok(symbols.to_vec());
    }

    let rows = sqlx::query_as::<_, (String,)>(
        r#"
        SELECT symbol
        FROM market_stock
        WHERE symbol ~ '^[036][0-9]{5}\.(SH|SZ)$'
          AND list_date IS NOT NULL
          AND list_date <= $1
          AND (delist_date IS NULL OR delist_date >= $2)
        ORDER BY symbol
        "#,
    )
    .bind(end)
    .bind(start)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(symbol,)| symbol).collect())
}

async fn fetch_shareholder_holder_number_rows(
    client: &TushareClient,
    window_start: NaiveDate,
    window_end: NaiveDate,
    call_timeout: StdDuration,
    page_limit: usize,
) -> Result<Vec<ShareholderHolderNumberRow>, String> {
    let start_date = window_start.format("%Y%m%d").to_string();
    let end_date = window_end.format("%Y%m%d").to_string();
    let label = format!("{}-{}", start_date, end_date);
    let mut offset = 0usize;
    let mut rows = Vec::new();
    loop {
        let resp = bounded_tushare_symbol_call(
            "shareholder_structure/stk_holdernumber",
            &label,
            call_timeout,
            client.stk_holdernumber(
                None,
                None,
                Some(&start_date),
                Some(&end_date),
                Some(page_limit),
                Some(offset),
            ),
        )
        .await?;
        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
        let row_count = maps.len();
        rows.extend(
            maps.iter()
                .filter_map(shareholder_holder_number_row_from_map)
                .filter(|row| date_in_range(row.ann_date, window_start, window_end)),
        );
        if row_count < page_limit {
            break;
        }
        offset += page_limit;
    }
    Ok(rows)
}

async fn fetch_shareholder_top10_holder_rows(
    client: &TushareClient,
    symbol: &str,
    window_start: NaiveDate,
    window_end: NaiveDate,
    float_holder: bool,
    call_timeout: StdDuration,
    page_limit: usize,
) -> Result<Vec<ShareholderTop10HolderRow>, String> {
    let start_date = window_start.format("%Y%m%d").to_string();
    let end_date = window_end.format("%Y%m%d").to_string();
    let source = if float_holder {
        "shareholder_structure/top10_floatholders"
    } else {
        "shareholder_structure/top10_holders"
    };
    let label = format!("{}:{}-{}", symbol, start_date, end_date);
    let mut offset = 0usize;
    let mut rows = Vec::new();
    loop {
        let resp = if float_holder {
            bounded_tushare_symbol_call(
                source,
                &label,
                call_timeout,
                client.top10_floatholders(
                    Some(symbol),
                    None,
                    Some(&start_date),
                    Some(&end_date),
                    Some(page_limit),
                    Some(offset),
                ),
            )
            .await?
        } else {
            bounded_tushare_symbol_call(
                source,
                &label,
                call_timeout,
                client.top10_holders(
                    Some(symbol),
                    None,
                    Some(&start_date),
                    Some(&end_date),
                    Some(page_limit),
                    Some(offset),
                ),
            )
            .await?
        };
        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
        let row_count = maps.len();
        rows.extend(
            maps.iter()
                .filter_map(|item| shareholder_top10_holder_row_from_map(item, float_holder))
                .filter(|row| date_in_range(row.ann_date, window_start, window_end)),
        );
        if row_count < page_limit {
            break;
        }
        offset += page_limit;
    }
    Ok(rows)
}

async fn fetch_shareholder_holder_trade_rows(
    client: &TushareClient,
    window_start: NaiveDate,
    window_end: NaiveDate,
    call_timeout: StdDuration,
    page_limit: usize,
) -> Result<Vec<ShareholderHolderTradeRow>, String> {
    let start_date = window_start.format("%Y%m%d").to_string();
    let end_date = window_end.format("%Y%m%d").to_string();
    let label = format!("{}-{}", start_date, end_date);
    let mut offset = 0usize;
    let mut rows = Vec::new();
    loop {
        let resp = bounded_tushare_symbol_call(
            "shareholder_structure/stk_holdertrade",
            &label,
            call_timeout,
            client.stk_holdertrade(
                None,
                None,
                Some(&start_date),
                Some(&end_date),
                Some(page_limit),
                Some(offset),
            ),
        )
        .await?;
        let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
        let row_count = maps.len();
        rows.extend(
            maps.iter()
                .filter_map(shareholder_holder_trade_row_from_map)
                .filter(|row| date_in_range(row.ann_date, window_start, window_end)),
        );
        if row_count < page_limit {
            break;
        }
        offset += page_limit;
    }
    Ok(rows)
}

pub async fn sync_shareholder_structure(
    pool: &PgPool,
    client: &TushareClient,
    task_id: &str,
    symbols: &[String],
    source_filters: &[String],
    start: &str,
    end: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    if s > e {
        return Err("shareholder_structure start_date cannot be after end_date".into());
    }

    let source_flags = shareholder_structure_source_filter_flags(source_filters)?;
    let needs_symbol_sources = source_flags.top10_holders || source_flags.top10_float_holders;
    let sync_symbols = if needs_symbol_sources {
        load_shareholder_structure_symbols(pool, symbols, s, e).await?
    } else {
        Vec::new()
    };
    let windows = quarters_in_range(s, e);
    repository::create_sync_task_with_context(
        pool,
        task_id,
        "shareholder_structure",
        "tushare:shareholder_structure",
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
        task_id,
        "shareholder structure raw PIT sync",
        "tushare:shareholder_structure",
        &[
            "market_stock_holder_number",
            "market_stock_top10_holders",
            "market_stock_top10_float_holders",
            "market_stock_holder_trade",
        ],
        s,
        e,
    )
    .await?;

    const PAGE_LIMIT: usize = 5_000;
    let call_timeout = tushare_symbol_call_timeout();
    let global_units_per_window =
        usize::from(source_flags.holder_number) + usize::from(source_flags.holder_trade);
    let symbol_units_per_window =
        usize::from(source_flags.top10_holders) + usize::from(source_flags.top10_float_holders);
    let total_units = windows.len() * global_units_per_window
        + sync_symbols.len() * windows.len() * symbol_units_per_window;
    if total_units == 0 {
        return Err("shareholder_structure source_filters selected no sync sources".into());
    }
    let progress_interval = event_sync_progress_interval(total_units, 20);
    repository::heartbeat_sync_task(pool, task_id, total_units as i32, 0, 0, 0).await?;

    let mut total_rows = 0usize;
    let mut completed_units = 0usize;
    let mut failed_units = 0usize;

    for (window_start, window_end) in &windows {
        if source_flags.holder_number {
            let rows = match fetch_shareholder_holder_number_rows(
                client,
                *window_start,
                *window_end,
                call_timeout,
                PAGE_LIMIT,
            )
            .await
            {
                Ok(rows) => rows,
                Err(message) => {
                    failed_units += 1;
                    repository::upsert_sync_attempt(
                        pool,
                        "shareholder_holder_number_ann_date",
                        "__ALL__",
                        *window_start,
                        *window_end,
                        task_id,
                        "failed",
                        0,
                        Some(message.as_str()),
                    )
                    .await?;
                    repository::update_sync_task_with_error(
                        pool,
                        task_id,
                        "partial",
                        total_units as i32,
                        completed_units as i32,
                        failed_units as i32,
                        &message,
                    )
                    .await?;
                    return Err(
                        format!("shareholder_structure holder_number failed: {message}").into(),
                    );
                }
            };
            let saved = upsert_shareholder_holder_number_rows(
                pool,
                &rows,
                task_id,
                "tushare:stk_holdernumber",
            )
            .await?;
            total_rows += saved;
            completed_units += 1;
            repository::upsert_sync_attempt(
                pool,
                "shareholder_holder_number_ann_date",
                "__ALL__",
                *window_start,
                *window_end,
                task_id,
                "completed",
                saved as i64,
                None,
            )
            .await?;
        }

        if source_flags.holder_trade {
            let rows = match fetch_shareholder_holder_trade_rows(
                client,
                *window_start,
                *window_end,
                call_timeout,
                PAGE_LIMIT,
            )
            .await
            {
                Ok(rows) => rows,
                Err(message) => {
                    failed_units += 1;
                    repository::upsert_sync_attempt(
                        pool,
                        "shareholder_holder_trade_ann_date",
                        "__ALL__",
                        *window_start,
                        *window_end,
                        task_id,
                        "failed",
                        0,
                        Some(message.as_str()),
                    )
                    .await?;
                    repository::update_sync_task_with_error(
                        pool,
                        task_id,
                        "partial",
                        total_units as i32,
                        completed_units as i32,
                        failed_units as i32,
                        &message,
                    )
                    .await?;
                    return Err(
                        format!("shareholder_structure holder_trade failed: {message}").into(),
                    );
                }
            };
            let saved = upsert_shareholder_holder_trade_rows(
                pool,
                &rows,
                task_id,
                "tushare:stk_holdertrade",
            )
            .await?;
            total_rows += saved;
            completed_units += 1;
            repository::upsert_sync_attempt(
                pool,
                "shareholder_holder_trade_ann_date",
                "__ALL__",
                *window_start,
                *window_end,
                task_id,
                "completed",
                saved as i64,
                None,
            )
            .await?;
        }

        if completed_units % progress_interval == 0 || completed_units == total_units {
            let progress = ((completed_units * 100) / total_units.max(1)).min(99) as i32;
            repository::heartbeat_sync_task(
                pool,
                task_id,
                total_units as i32,
                completed_units as i32,
                failed_units as i32,
                progress,
            )
            .await?;
        }
    }

    for symbol in &sync_symbols {
        for (window_start, window_end) in &windows {
            for (float_holder, table, attempt_source, source_name) in [
                (
                    false,
                    "market_stock_top10_holders",
                    "shareholder_top10_holders_ann_date",
                    "tushare:top10_holders",
                ),
                (
                    true,
                    "market_stock_top10_float_holders",
                    "shareholder_top10_float_holders_ann_date",
                    "tushare:top10_floatholders",
                ),
            ] {
                if (!float_holder && !source_flags.top10_holders)
                    || (float_holder && !source_flags.top10_float_holders)
                {
                    continue;
                }
                let rows = match fetch_shareholder_top10_holder_rows(
                    client,
                    symbol,
                    *window_start,
                    *window_end,
                    float_holder,
                    call_timeout,
                    PAGE_LIMIT,
                )
                .await
                {
                    Ok(rows) => rows,
                    Err(message) => {
                        failed_units += 1;
                        repository::upsert_sync_attempt(
                            pool,
                            attempt_source,
                            symbol,
                            *window_start,
                            *window_end,
                            task_id,
                            "failed",
                            0,
                            Some(message.as_str()),
                        )
                        .await?;
                        repository::update_sync_task_with_error(
                            pool,
                            task_id,
                            "partial",
                            total_units as i32,
                            completed_units as i32,
                            failed_units as i32,
                            &message,
                        )
                        .await?;
                        return Err(format!(
                            "shareholder_structure {} {} failed: {}",
                            attempt_source, symbol, message
                        )
                        .into());
                    }
                };
                let saved =
                    upsert_shareholder_top10_holder_rows(pool, table, &rows, task_id, source_name)
                        .await?;
                total_rows += saved;
                completed_units += 1;
                repository::upsert_sync_attempt(
                    pool,
                    attempt_source,
                    symbol,
                    *window_start,
                    *window_end,
                    task_id,
                    "completed",
                    saved as i64,
                    None,
                )
                .await?;

                if completed_units % progress_interval == 0 || completed_units == total_units {
                    let progress = ((completed_units * 100) / total_units.max(1)).min(99) as i32;
                    repository::heartbeat_sync_task(
                        pool,
                        task_id,
                        total_units as i32,
                        completed_units as i32,
                        failed_units as i32,
                        progress,
                    )
                    .await?;
                }
            }
        }
    }

    repository::update_sync_task(
        pool,
        task_id,
        if failed_units > 0 {
            "partial"
        } else {
            "completed"
        },
        total_units as i32,
        completed_units as i32,
        failed_units as i32,
    )
    .await?;
    info!(
        total_rows,
        completed_units, failed_units, "shareholder_structure 同步完成"
    );
    Ok(total_rows)
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
        pool,
        &task_id,
        "fund_adj",
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
        "fund adj sync",
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
        pool,
        &task_id,
        if fail > 0 { "partial" } else { "completed" },
        total as i32,
        ok as i32,
        fail as i32,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventSymbolSyncAttempt {
    source: String,
    symbol: String,
    start_date: NaiveDate,
    end_date: NaiveDate,
    task_id: String,
    status: &'static str,
    row_count: i64,
    error_message: Option<String>,
}

fn event_symbol_sync_attempt(
    source: &str,
    symbol: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    task_id: &str,
    row_count: usize,
    error_message: Option<String>,
) -> EventSymbolSyncAttempt {
    EventSymbolSyncAttempt {
        source: source.to_string(),
        symbol: symbol.to_string(),
        start_date,
        end_date,
        task_id: task_id.to_string(),
        status: if error_message.is_some() {
            "failed"
        } else {
            "completed"
        },
        row_count: row_count as i64,
        error_message,
    }
}

fn event_sync_progress_interval(total: usize, max_interval: usize) -> usize {
    (total / 10).clamp(1, max_interval.max(1))
}

async fn record_event_symbol_sync_attempt(
    pool: &PgPool,
    attempt: EventSymbolSyncAttempt,
) -> Result<(), sqlx::Error> {
    repository::upsert_sync_attempt(
        pool,
        &attempt.source,
        &attempt.symbol,
        attempt.start_date,
        attempt.end_date,
        &attempt.task_id,
        attempt.status,
        attempt.row_count,
        attempt.error_message.as_deref(),
    )
    .await
}

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
        available_at: ann_date,
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
    let progress_interval = event_sync_progress_interval(total, 100);
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 2000usize;
    repository::update_sync_task(pool, &task_id, "running", total as i32, 0, 0).await?;

    for symbol in symbols {
        let mut offset = 0usize;
        let mut symbol_failed = false;
        let mut symbol_rows = 0usize;
        let mut symbol_error = None;
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
                        let saved =
                            repository::upsert_forecast_batch(pool, &rows, dv_id, "tushare")
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
                    let error_message = error.to_string();
                    warn!("{} forecast failed: {}", symbol, error_message);
                    failed += 1;
                    symbol_failed = true;
                    symbol_error = Some(error_message);
                    break;
                }
            }
        }
        record_event_symbol_sync_attempt(
            pool,
            event_symbol_sync_attempt(
                "forecast",
                symbol,
                s,
                e,
                &task_id,
                symbol_rows,
                symbol_error,
            ),
        )
        .await?;
        if !symbol_failed {
            ok += 1;
        }
        let completed = ok + failed;
        if completed % progress_interval == 0 || completed == total {
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
    let progress_interval = event_sync_progress_interval(total, 100);
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 2000usize;
    repository::update_sync_task(pool, &task_id, "running", total as i32, 0, 0).await?;

    for symbol in symbols {
        let mut offset = 0usize;
        let mut symbol_failed = false;
        let mut symbol_rows = 0usize;
        let mut symbol_error = None;
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
                        let saved =
                            repository::upsert_express_batch(pool, &rows, dv_id, "tushare").await?;
                        symbol_rows += saved;
                        total_rows += saved;
                    }
                    if row_count < page_limit {
                        break;
                    }
                    offset += page_limit;
                }
                Err(error) => {
                    let error_message = error.to_string();
                    warn!("{} express failed: {}", symbol, error_message);
                    failed += 1;
                    symbol_failed = true;
                    symbol_error = Some(error_message);
                    break;
                }
            }
        }
        record_event_symbol_sync_attempt(
            pool,
            event_symbol_sync_attempt("express", symbol, s, e, &task_id, symbol_rows, symbol_error),
        )
        .await?;
        if !symbol_failed {
            ok += 1;
        }
        let completed = ok + failed;
        if completed % progress_interval == 0 || completed == total {
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
    let modify_date = to_date(&get_str(item, "modify_date"));
    let available_at = [Some(ann_date), actual_date, modify_date]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(ann_date);
    Some(MarketStockDisclosureDate {
        symbol: get_str(item, "ts_code"),
        end_date,
        ann_date,
        pre_date: to_date(&get_str(item, "pre_date")),
        actual_date,
        modify_date,
        available_at,
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
    let mut symbol_row_counts = symbols
        .iter()
        .map(|symbol| (symbol.clone(), 0usize))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut sync_error = None;
    let periods = quarter_end_dates_in_range(s, e);
    let total = periods.len();
    let progress_interval = event_sync_progress_interval(total, 8);
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut total_rows = 0usize;
    let page_limit = 3000usize;
    repository::update_sync_task(pool, &task_id, "running", total as i32, 0, 0).await?;

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
                    for row in &rows {
                        if let Some(count) = symbol_row_counts.get_mut(&row.symbol) {
                            *count += 1;
                        }
                    }
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
                    sync_error = Some(append_symbol_error(
                        sync_error,
                        format!("period {} failed: {}", period.format("%Y%m%d"), error),
                    ));
                    failed += 1;
                    period_failed = true;
                    break;
                }
            }
        }
        if !period_failed {
            ok += 1;
        }
        let completed = ok + failed;
        if completed % progress_interval == 0 || completed == total {
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

    for (symbol, symbol_rows) in symbol_row_counts {
        record_event_symbol_sync_attempt(
            pool,
            event_symbol_sync_attempt(
                "disclosure_date",
                &symbol,
                s,
                e,
                &task_id,
                symbol_rows,
                sync_error.clone(),
            ),
        )
        .await?;
    }
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

// ─── sync_share_float ───────────────────────────────────────────

fn share_float_row_from_map(item: &Map<String, Value>) -> Option<MarketStockShareFloat> {
    let ann_date = to_date(&get_str(item, "ann_date"))?;
    let float_date = to_date(&get_str(item, "float_date"))?;
    Some(MarketStockShareFloat {
        symbol: get_str(item, "ts_code"),
        ann_date,
        float_date,
        available_at: ann_date,
        float_share: to_opt_decimal(get_f64(item, "float_share")),
        float_ratio: to_opt_decimal(get_f64(item, "float_ratio")),
        holder_name: get_opt_str(item, "holder_name").unwrap_or_default(),
        share_type: get_opt_str(item, "share_type").unwrap_or_default(),
        raw_payload: raw_payload(item),
    })
}

fn main_business_row_from_map(
    item: &Map<String, Value>,
    available_at: NaiveDate,
) -> Option<MarketStockMainBusiness> {
    let symbol = get_str(item, "ts_code");
    let end_date = to_date(&get_str(item, "end_date"))?;
    let business_type = get_opt_str(item, "bz_code").unwrap_or_else(|| "P".to_string());
    let bz_item = get_opt_str(item, "bz_item").unwrap_or_default();
    let bz_code = get_opt_str(item, "bz_code").unwrap_or_default();
    let curr_type = get_opt_str(item, "curr_type").unwrap_or_default();
    let update_flag = get_opt_str(item, "update_flag").unwrap_or_default();
    if symbol.is_empty() || available_at < end_date {
        return None;
    }
    let source_row_hash = stable_source_row_hash(&[
        symbol.clone(),
        end_date.to_string(),
        business_type.clone(),
        bz_item.clone(),
        bz_code.clone(),
        value_key_part(item, "bz_sales"),
        value_key_part(item, "bz_profit"),
        value_key_part(item, "bz_cost"),
        curr_type.clone(),
        update_flag.clone(),
    ]);

    Some(MarketStockMainBusiness {
        symbol,
        end_date,
        available_at,
        business_type,
        bz_item,
        bz_code,
        bz_sales: to_opt_decimal(get_f64(item, "bz_sales")),
        bz_profit: to_opt_decimal(get_f64(item, "bz_profit")),
        bz_cost: to_opt_decimal(get_f64(item, "bz_cost")),
        curr_type,
        update_flag,
        source_row_hash,
        raw_payload: raw_payload(item),
    })
}

// ─── sync_futures_price_chain ───────────────────────────────────

fn futures_price_chain_available_at(trade_date: NaiveDate) -> NaiveDate {
    trade_date + Duration::days(1)
}

fn required_text(item: &Map<String, Value>, key: &str) -> Option<String> {
    let value = get_str(item, key).trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn futures_daily_row_from_map(item: &Map<String, Value>) -> Option<MarketFuturesDaily> {
    let ts_code = required_text(item, "ts_code")?;
    let trade_date = to_date(&get_str(item, "trade_date"))?;
    Some(MarketFuturesDaily {
        ts_code,
        trade_date,
        pre_close: to_opt_decimal(get_f64(item, "pre_close")),
        pre_settle: to_opt_decimal(get_f64(item, "pre_settle")),
        open: to_opt_decimal(get_f64(item, "open")),
        high: to_opt_decimal(get_f64(item, "high")),
        low: to_opt_decimal(get_f64(item, "low")),
        close: to_opt_decimal(get_f64(item, "close")),
        settle: to_opt_decimal(get_f64(item, "settle")),
        change1: to_opt_decimal(get_f64(item, "change1")),
        change2: to_opt_decimal(get_f64(item, "change2")),
        vol: to_opt_decimal(get_f64(item, "vol")),
        amount: to_opt_decimal(get_f64(item, "amount")),
        oi: to_opt_decimal(get_f64(item, "oi")),
        oi_chg: to_opt_decimal(get_f64(item, "oi_chg")),
        delv_settle: to_opt_decimal(get_f64(item, "delv_settle")),
        available_at: futures_price_chain_available_at(trade_date),
        source_published_at: None,
        raw_payload: raw_payload(item),
    })
}

fn futures_warehouse_receipt_row_from_map(
    item: &Map<String, Value>,
) -> Option<MarketFuturesWarehouseReceipt> {
    let trade_date = to_date(&get_str(item, "trade_date"))?;
    let symbol = required_text(item, "symbol")?;
    let exchange = required_text(item, "exchange")?;
    let warehouse = required_text(item, "warehouse")?;
    Some(MarketFuturesWarehouseReceipt {
        trade_date,
        symbol,
        exchange,
        fut_name: get_opt_str(item, "fut_name"),
        warehouse,
        wh_id: get_opt_str(item, "wh_id"),
        pre_vol: to_opt_decimal(get_f64(item, "pre_vol")),
        vol: to_opt_decimal(get_f64(item, "vol")),
        vol_chg: to_opt_decimal(get_f64(item, "vol_chg")),
        area: get_opt_str(item, "area"),
        year: get_opt_str(item, "year"),
        grade: get_opt_str(item, "grade"),
        brand: get_opt_str(item, "brand"),
        place: get_opt_str(item, "place"),
        pd: to_opt_decimal(get_f64(item, "pd")),
        is_ct: get_opt_str(item, "is_ct"),
        unit: get_opt_str(item, "unit"),
        available_at: futures_price_chain_available_at(trade_date),
        source_published_at: None,
        raw_payload: raw_payload(item),
    })
}

fn futures_holding_rank_row_from_map(
    item: &Map<String, Value>,
) -> Option<MarketFuturesHoldingRank> {
    let trade_date = to_date(&get_str(item, "trade_date"))?;
    let symbol = required_text(item, "symbol")?;
    let exchange = required_text(item, "exchange")?;
    let broker = required_text(item, "broker")?;
    Some(MarketFuturesHoldingRank {
        trade_date,
        symbol,
        exchange,
        broker,
        vol: to_opt_decimal(get_f64(item, "vol")),
        vol_chg: to_opt_decimal(get_f64(item, "vol_chg")),
        long_hld: to_opt_decimal(get_f64(item, "long_hld")),
        long_chg: to_opt_decimal(get_f64(item, "long_chg")),
        short_hld: to_opt_decimal(get_f64(item, "short_hld")),
        short_chg: to_opt_decimal(get_f64(item, "short_chg")),
        available_at: futures_price_chain_available_at(trade_date),
        source_published_at: None,
        raw_payload: raw_payload(item),
    })
}

fn futures_price_chain_trade_dates(
    start: NaiveDate,
    end: NaiveDate,
    open_dates: Vec<NaiveDate>,
) -> Result<Vec<NaiveDate>, String> {
    if start > end {
        return Ok(Vec::new());
    }

    let dates: Vec<_> = open_dates
        .into_iter()
        .filter(|date| *date >= start && *date <= end)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if dates.is_empty() {
        return Err(format!(
            "futures_price_chain requires market_trade_calendar open dates between {} and {}; sync trade_calendar before futures_price_chain raw sync",
            start.format("%Y%m%d"),
            end.format("%Y%m%d")
        ));
    }

    Ok(dates)
}

fn futures_price_chain_trade_dates_sql() -> &'static str {
    "WITH futures_calendar AS (
         SELECT DISTINCT trade_date
         FROM market_trade_calendar
         WHERE is_open = true
           AND exchange IN ('SHFE', 'DCE', 'CZCE', 'CFFEX', 'INE')
           AND trade_date >= $1 AND trade_date <= $2
     ),
     fallback_calendar AS (
         SELECT DISTINCT trade_date
         FROM market_trade_calendar
         WHERE is_open = true
           AND trade_date >= $1 AND trade_date <= $2
     )
     SELECT trade_date
     FROM futures_calendar
     UNION
     SELECT trade_date
     FROM fallback_calendar
     WHERE NOT EXISTS (SELECT 1 FROM futures_calendar)
     ORDER BY trade_date"
}

async fn load_futures_price_chain_trade_dates(
    pool: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<NaiveDate>, Box<dyn std::error::Error>> {
    let open_dates: Vec<NaiveDate> = sqlx::query_scalar(futures_price_chain_trade_dates_sql())
        .bind(start)
        .bind(end)
        .fetch_all(pool)
        .await?;

    futures_price_chain_trade_dates(start, end, open_dates)
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidData, message).into())
}

fn futures_price_chain_attempt_key(
    _trade_date: NaiveDate,
    symbol: Option<&str>,
    exchange: Option<&str>,
) -> String {
    let symbol = symbol
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let exchange = exchange
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let key = match (symbol, exchange) {
        (None, None) => "__ALL__".to_string(),
        (Some(symbol), None) => symbol,
        (None, Some(exchange)) => format!("EX:{exchange}"),
        (Some(symbol), Some(exchange)) => format!("{symbol}@{exchange}"),
    };
    if key.len() <= 20 {
        key
    } else {
        format!("fpc-{}", stable_source_row_hash(&[key]))
    }
}

pub async fn sync_futures_price_chain(
    pool: &PgPool,
    client: &TushareClient,
    task_id: &str,
    symbols: &[String],
    exchanges: &[String],
    start: &str,
    end: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    if s > e {
        return Err("futures_price_chain start_date cannot be after end_date".into());
    }
    let dates = load_futures_price_chain_trade_dates(pool, s, e).await?;
    repository::create_sync_task_with_context(
        pool,
        task_id,
        "futures_price_chain",
        "tushare:futures_price_chain",
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
        task_id,
        "futures price-chain raw PIT sync",
        "tushare:futures_price_chain",
        &[
            "market_futures_daily",
            "market_futures_warehouse_receipt",
            "market_futures_holding_rank",
        ],
        s,
        e,
    )
    .await?;

    let symbol_filters: Vec<Option<&str>> = if symbols.is_empty() {
        vec![None]
    } else {
        symbols.iter().map(|value| Some(value.as_str())).collect()
    };
    let exchange_filters: Vec<Option<&str>> = if exchanges.is_empty() {
        vec![None]
    } else {
        exchanges.iter().map(|value| Some(value.as_str())).collect()
    };
    let total_units = dates.len() * symbol_filters.len() * exchange_filters.len() * 3;
    let progress_interval = event_sync_progress_interval(total_units, 20);
    repository::heartbeat_sync_task(pool, task_id, total_units as i32, 0, 0, 0).await?;

    const PAGE_LIMIT: usize = 5_000;
    let call_timeout = tushare_symbol_call_timeout();
    let mut completed_units = 0usize;
    let mut failed_units = 0usize;
    let mut total_rows = 0usize;

    for trade_date in dates {
        let trade_date_str = trade_date.format("%Y%m%d").to_string();
        for symbol_filter in &symbol_filters {
            for exchange_filter in &exchange_filters {
                let attempt_key =
                    futures_price_chain_attempt_key(trade_date, *symbol_filter, *exchange_filter);

                let mut daily_rows = Vec::new();
                let mut daily_offset = 0usize;
                loop {
                    let resp = bounded_tushare_symbol_call(
                        "futures_price_chain/fut_daily",
                        &attempt_key,
                        call_timeout,
                        client.fut_daily(
                            *symbol_filter,
                            Some(&trade_date_str),
                            *exchange_filter,
                            None,
                            None,
                            Some(PAGE_LIMIT),
                            Some(daily_offset),
                        ),
                    )
                    .await;
                    match resp {
                        Ok(resp) => {
                            let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                            let row_count = maps.len();
                            daily_rows.extend(maps.iter().filter_map(futures_daily_row_from_map));
                            if row_count < PAGE_LIMIT {
                                break;
                            }
                            daily_offset += PAGE_LIMIT;
                        }
                        Err(message) => {
                            failed_units += 1;
                            repository::upsert_sync_attempt(
                                pool,
                                "futures_price_chain_daily",
                                &attempt_key,
                                trade_date,
                                trade_date,
                                task_id,
                                "failed",
                                daily_rows.len() as i64,
                                Some(message.as_str()),
                            )
                            .await?;
                            repository::update_sync_task_with_error(
                                pool,
                                task_id,
                                "partial",
                                total_units as i32,
                                completed_units as i32,
                                failed_units as i32,
                                &message,
                            )
                            .await?;
                            return Err(format!(
                                "futures_price_chain fut_daily {} failed: {}",
                                attempt_key, message
                            )
                            .into());
                        }
                    }
                }
                let saved = repository::upsert_futures_daily_batch(
                    pool,
                    &daily_rows,
                    task_id,
                    "tushare:fut_daily",
                )
                .await?;
                total_rows += saved;
                completed_units += 1;
                repository::upsert_sync_attempt(
                    pool,
                    "futures_price_chain_daily",
                    &attempt_key,
                    trade_date,
                    trade_date,
                    task_id,
                    "completed",
                    saved as i64,
                    None,
                )
                .await?;

                let mut wsr_rows = Vec::new();
                let mut wsr_offset = 0usize;
                loop {
                    let resp = bounded_tushare_symbol_call(
                        "futures_price_chain/fut_wsr",
                        &attempt_key,
                        call_timeout,
                        client.fut_wsr(
                            Some(&trade_date_str),
                            *symbol_filter,
                            None,
                            None,
                            *exchange_filter,
                            Some(PAGE_LIMIT),
                            Some(wsr_offset),
                        ),
                    )
                    .await;
                    match resp {
                        Ok(resp) => {
                            let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                            let row_count = maps.len();
                            wsr_rows.extend(
                                maps.iter()
                                    .filter_map(futures_warehouse_receipt_row_from_map),
                            );
                            if row_count < PAGE_LIMIT {
                                break;
                            }
                            wsr_offset += PAGE_LIMIT;
                        }
                        Err(message) => {
                            failed_units += 1;
                            repository::upsert_sync_attempt(
                                pool,
                                "futures_price_chain_wsr",
                                &attempt_key,
                                trade_date,
                                trade_date,
                                task_id,
                                "failed",
                                wsr_rows.len() as i64,
                                Some(message.as_str()),
                            )
                            .await?;
                            repository::update_sync_task_with_error(
                                pool,
                                task_id,
                                "partial",
                                total_units as i32,
                                completed_units as i32,
                                failed_units as i32,
                                &message,
                            )
                            .await?;
                            return Err(format!(
                                "futures_price_chain fut_wsr {} failed: {}",
                                attempt_key, message
                            )
                            .into());
                        }
                    }
                }
                let saved = repository::upsert_futures_warehouse_receipt_batch(
                    pool,
                    &wsr_rows,
                    task_id,
                    "tushare:fut_wsr",
                )
                .await?;
                total_rows += saved;
                completed_units += 1;
                repository::upsert_sync_attempt(
                    pool,
                    "futures_price_chain_wsr",
                    &attempt_key,
                    trade_date,
                    trade_date,
                    task_id,
                    "completed",
                    saved as i64,
                    None,
                )
                .await?;

                let mut holding_rows = Vec::new();
                let mut holding_offset = 0usize;
                loop {
                    let resp = bounded_tushare_symbol_call(
                        "futures_price_chain/fut_holding",
                        &attempt_key,
                        call_timeout,
                        client.fut_holding(
                            Some(&trade_date_str),
                            *symbol_filter,
                            None,
                            None,
                            *exchange_filter,
                            Some(PAGE_LIMIT),
                            Some(holding_offset),
                        ),
                    )
                    .await;
                    match resp {
                        Ok(resp) => {
                            let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
                            let row_count = maps.len();
                            holding_rows
                                .extend(maps.iter().filter_map(futures_holding_rank_row_from_map));
                            if row_count < PAGE_LIMIT {
                                break;
                            }
                            holding_offset += PAGE_LIMIT;
                        }
                        Err(message) => {
                            failed_units += 1;
                            repository::upsert_sync_attempt(
                                pool,
                                "futures_price_chain_holding",
                                &attempt_key,
                                trade_date,
                                trade_date,
                                task_id,
                                "failed",
                                holding_rows.len() as i64,
                                Some(message.as_str()),
                            )
                            .await?;
                            repository::update_sync_task_with_error(
                                pool,
                                task_id,
                                "partial",
                                total_units as i32,
                                completed_units as i32,
                                failed_units as i32,
                                &message,
                            )
                            .await?;
                            return Err(format!(
                                "futures_price_chain fut_holding {} failed: {}",
                                attempt_key, message
                            )
                            .into());
                        }
                    }
                }
                let saved = repository::upsert_futures_holding_rank_batch(
                    pool,
                    &holding_rows,
                    task_id,
                    "tushare:fut_holding",
                )
                .await?;
                total_rows += saved;
                completed_units += 1;
                repository::upsert_sync_attempt(
                    pool,
                    "futures_price_chain_holding",
                    &attempt_key,
                    trade_date,
                    trade_date,
                    task_id,
                    "completed",
                    saved as i64,
                    None,
                )
                .await?;

                if completed_units % progress_interval == 0 || completed_units == total_units {
                    let progress = if total_units > 0 {
                        ((completed_units * 100) / total_units).min(99) as i32
                    } else {
                        0
                    };
                    repository::heartbeat_sync_task(
                        pool,
                        task_id,
                        total_units as i32,
                        completed_units as i32,
                        failed_units as i32,
                        progress,
                    )
                    .await?;
                }
            }
        }
    }

    repository::update_sync_task(
        pool,
        task_id,
        if failed_units > 0 {
            "partial"
        } else {
            "completed"
        },
        total_units as i32,
        completed_units as i32,
        failed_units as i32,
    )
    .await?;
    info!(
        total_rows,
        completed_units, failed_units, "futures_price_chain 同步完成"
    );
    Ok(total_rows)
}

pub async fn sync_share_float(
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
        "share_float",
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
        "share float unlock-date sync",
        "tushare",
        &["market_stock_share_float"],
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
    let mut rows_by_symbol = std::collections::HashMap::<String, usize>::new();
    let mut sync_error: Option<String> = None;
    let page_limit = 2000usize;
    let mut offset = 0usize;

    loop {
        match client
            .share_float(
                None,
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
                let rows: Vec<MarketStockShareFloat> = maps
                    .iter()
                    .filter_map(share_float_row_from_map)
                    .filter(|row| {
                        symbol_filter
                            .as_ref()
                            .map(|filter| filter.contains(&row.symbol))
                            .unwrap_or(true)
                    })
                    .collect();
                if !rows.is_empty() {
                    for row in &rows {
                        *rows_by_symbol.entry(row.symbol.clone()).or_insert(0) += 1;
                    }
                    total_rows +=
                        repository::upsert_share_float_batch(pool, &rows, dv_id, "tushare").await?;
                }
                if row_count < page_limit {
                    break;
                }
                offset += page_limit;
            }
            Err(error) => {
                let message = error.to_string();
                warn!("share_float {}..{} failed: {}", start, end, message);
                sync_error = Some(message);
                failed += 1;
                repository::update_sync_task_with_error(
                    pool,
                    &task_id,
                    "partial",
                    1,
                    0,
                    failed as i32,
                    sync_error.as_deref().unwrap_or("share_float sync failed"),
                )
                .await?;
                break;
            }
        }
    }

    if !symbols.is_empty() {
        for symbol in symbols {
            record_event_symbol_sync_attempt(
                pool,
                event_symbol_sync_attempt(
                    "share_float",
                    symbol,
                    s,
                    e,
                    &task_id,
                    rows_by_symbol.get(symbol).copied().unwrap_or_default(),
                    sync_error.clone(),
                ),
            )
            .await?;
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
        "share_float 同步完成: rows={}, failed={}",
        total_rows, failed
    );
    if let Some(message) = sync_error {
        return Err(format!(
            "share_float {}..{} partial: rows_saved={}, error={}",
            start, end, total_rows, message
        )
        .into());
    }
    Ok(total_rows)
}

async fn load_main_business_available_at_for_period(
    pool: &PgPool,
    period: NaiveDate,
    symbols: &[String],
) -> Result<std::collections::BTreeMap<String, NaiveDate>, sqlx::Error> {
    let mut unique_symbols: Vec<String> = symbols
        .iter()
        .filter(|symbol| !symbol.is_empty())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if unique_symbols.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }
    unique_symbols.sort();

    let mut mapped = std::collections::BTreeMap::new();
    let financial_rows = sqlx::query_as::<_, (String, NaiveDate)>(
        r#"
        WITH input AS (
            SELECT DISTINCT symbol
            FROM UNNEST($1::text[]) AS input(symbol)
        )
        SELECT input.symbol, MIN(fs.ann_date) AS available_at
        FROM input
        JOIN market_financial_statement fs
          ON fs.ts_code = input.symbol
         AND fs.end_date = $2
         AND fs.ann_date >= $2
        GROUP BY input.symbol
        "#,
    )
    .bind(&unique_symbols)
    .bind(period)
    .fetch_all(pool)
    .await?;
    for (symbol, available_at) in financial_rows {
        mapped.insert(symbol, available_at);
    }

    let missing_symbols: Vec<String> = unique_symbols
        .into_iter()
        .filter(|symbol| !mapped.contains_key(symbol))
        .collect();
    if missing_symbols.is_empty() {
        return Ok(mapped);
    }

    let disclosure_rows = sqlx::query_as::<_, (String, NaiveDate)>(
        r#"
        WITH input AS (
            SELECT DISTINCT symbol
            FROM UNNEST($1::text[]) AS input(symbol)
        )
        SELECT input.symbol, MIN(disclosure.available_at) AS available_at
        FROM input
        JOIN market_stock_disclosure_date disclosure
          ON disclosure.symbol = input.symbol
         AND disclosure.end_date = $2
         AND disclosure.available_at >= $2
        GROUP BY input.symbol
        "#,
    )
    .bind(&missing_symbols)
    .bind(period)
    .fetch_all(pool)
    .await?;
    for (symbol, available_at) in disclosure_rows {
        mapped.insert(symbol, available_at);
    }
    Ok(mapped)
}

async fn load_main_business_market_universe_for_period(
    pool: &PgPool,
    period: NaiveDate,
) -> Result<HashSet<String>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String,)>(
        r#"
        SELECT symbol
        FROM market_stock
        WHERE symbol IS NOT NULL
          AND (list_date IS NULL OR list_date <= $1)
          AND (delist_date IS NULL OR delist_date >= $1)
        "#,
    )
    .bind(period)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(symbol,)| symbol).collect())
}

pub async fn sync_main_business(
    pool: &PgPool,
    client: &TushareClient,
    task_id: &str,
    symbols: &[String],
    start: &str,
    end: &str,
    business_type: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    let periods = quarter_end_dates_in_range(s, e);
    repository::create_sync_task_with_context(
        pool,
        task_id,
        "main_business",
        "tushare:fina_mainbz_vip",
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
        task_id,
        "main business composition PIT sync",
        "tushare:fina_mainbz_vip",
        &["market_stock_main_business"],
        s,
        e,
    )
    .await?;

    let requested_symbol_filter = if symbols.is_empty() {
        None
    } else {
        Some(symbols.iter().cloned().collect::<HashSet<_>>())
    };
    let total_periods = periods.len();
    let progress_interval = event_sync_progress_interval(total_periods, 20);
    let attempt_source = main_business_attempt_source(symbols);
    repository::heartbeat_sync_task(pool, task_id, total_periods as i32, 0, 0, 0).await?;

    let mut total_rows = 0usize;
    let mut failed = 0usize;
    let mut total_missing_available_at = 0usize;
    const PAGE_LIMIT: usize = 5_000;

    for (period_idx, period) in periods.into_iter().enumerate() {
        let period_str = period.format("%Y%m%d").to_string();
        let period_attempt_key = format!("period:{}", period_str);
        let period_symbol_filter = match &requested_symbol_filter {
            Some(filter) => filter.clone(),
            None => load_main_business_market_universe_for_period(pool, period).await?,
        };
        let mut offset = 0usize;
        let mut period_saved = 0usize;
        let mut period_raw_rows = 0usize;
        let mut period_out_of_universe_rows = 0usize;
        let mut period_missing_available_at = 0usize;

        loop {
            let resp = match client
                .fina_mainbz_vip(
                    &period_str,
                    Some(business_type),
                    Some(PAGE_LIMIT),
                    Some(offset),
                )
                .await
            {
                Ok(resp) => resp,
                Err(error) => {
                    failed += 1;
                    let message = error.to_string();
                    repository::upsert_sync_attempt(
                        pool,
                        attempt_source,
                        &period_attempt_key,
                        period,
                        period,
                        task_id,
                        "failed",
                        period_saved as i64,
                        Some(message.as_str()),
                    )
                    .await?;
                    repository::update_sync_task_with_error(
                        pool,
                        task_id,
                        "partial",
                        total_periods as i32,
                        period_idx as i32,
                        failed as i32,
                        &message,
                    )
                    .await?;
                    return Err(format!("main_business {} failed: {}", period_str, message).into());
                }
            };

            let maps = resp.data.map(|data| data.to_maps()).unwrap_or_default();
            let row_count = maps.len();
            period_raw_rows += row_count;
            let page_symbols: Vec<String> = maps
                .iter()
                .map(|item| get_str(item, "ts_code"))
                .filter(|symbol| period_symbol_filter.contains(symbol))
                .collect();
            let available_by_symbol =
                load_main_business_available_at_for_period(pool, period, &page_symbols).await?;
            let mut rows = Vec::new();
            for item in &maps {
                let symbol = get_str(item, "ts_code");
                if !period_symbol_filter.contains(&symbol) {
                    period_out_of_universe_rows += 1;
                    continue;
                }
                let Some(available_at) = available_by_symbol.get(&symbol).copied() else {
                    period_missing_available_at += 1;
                    continue;
                };
                if let Some(row) = main_business_row_from_map(item, available_at) {
                    if row.end_date == period {
                        rows.push(row);
                    }
                }
            }
            if !rows.is_empty() {
                period_saved +=
                    repository::upsert_main_business_batch(pool, &rows, task_id, "tushare").await?;
            }
            if row_count < PAGE_LIMIT {
                break;
            }
            offset += PAGE_LIMIT;
        }

        total_rows += period_saved;
        total_missing_available_at += period_missing_available_at;
        let attempt_note = if period_missing_available_at > 0 || period_out_of_universe_rows > 0 {
            let mut parts = Vec::new();
            if period_missing_available_at > 0 {
                parts.push(format!(
                    "missing_available_at_rows={}",
                    period_missing_available_at
                ));
            }
            if period_out_of_universe_rows > 0 {
                parts.push(format!(
                    "out_of_universe_rows={}",
                    period_out_of_universe_rows
                ));
            }
            parts.push(format!("raw_rows={}", period_raw_rows));
            Some(parts.join(", "))
        } else {
            None
        };
        repository::upsert_sync_attempt(
            pool,
            attempt_source,
            &period_attempt_key,
            period,
            period,
            task_id,
            "completed",
            period_saved as i64,
            attempt_note.as_deref(),
        )
        .await?;

        let completed_periods = period_idx + 1;
        if completed_periods % progress_interval == 0 || completed_periods == total_periods {
            let progress = if total_periods > 0 {
                ((completed_periods * 100) / total_periods).min(99) as i32
            } else {
                0
            };
            repository::heartbeat_sync_task(
                pool,
                task_id,
                total_periods as i32,
                completed_periods as i32,
                failed as i32,
                progress,
            )
            .await?;
        }
        info!(
            %period_str,
            period_saved,
            period_raw_rows,
            period_out_of_universe_rows,
            period_missing_available_at,
            total_rows,
            "main_business period sync complete"
        );
    }

    repository::update_sync_task(
        pool,
        task_id,
        if failed > 0 { "partial" } else { "completed" },
        total_periods as i32,
        total_periods.saturating_sub(failed) as i32,
        failed as i32,
    )
    .await?;
    info!(
        total_rows,
        total_missing_available_at, "main_business 同步完成"
    );
    Ok(total_rows)
}

// ─── sync_industry_membership (PIT 行业成员历史) ────────────────

#[derive(Debug, Clone)]
struct IndustryClassifyMeta {
    classification_source: String,
    industry_level: String,
    index_code: String,
    industry_code: String,
    industry_name: String,
    parent_code: String,
}

fn industry_classify_meta_from_map(
    item: &Map<String, Value>,
    fallback_source: &str,
) -> Option<IndustryClassifyMeta> {
    let index_code = get_str(item, "index_code");
    if index_code.is_empty() {
        return None;
    }
    Some(IndustryClassifyMeta {
        classification_source: get_opt_str(item, "src").unwrap_or_else(|| fallback_source.into()),
        industry_level: get_str(item, "level"),
        index_code,
        industry_code: get_str(item, "industry_code"),
        industry_name: get_str(item, "industry_name"),
        parent_code: get_opt_str(item, "parent_code").unwrap_or_default(),
    })
}

fn industry_membership_raw_payload(
    classify: &IndustryClassifyMeta,
    item: &Map<String, Value>,
) -> Value {
    let mut payload = item.clone();
    payload.insert(
        "_classification_source".to_string(),
        Value::String(classify.classification_source.clone()),
    );
    payload.insert(
        "_industry_level".to_string(),
        Value::String(classify.industry_level.clone()),
    );
    payload.insert(
        "_industry_code".to_string(),
        Value::String(classify.industry_code.clone()),
    );
    payload.insert(
        "_industry_name".to_string(),
        Value::String(classify.industry_name.clone()),
    );
    payload.insert(
        "_parent_code".to_string(),
        Value::String(classify.parent_code.clone()),
    );
    Value::Object(payload)
}

fn industry_membership_row_from_map(
    classify: &IndustryClassifyMeta,
    item: &Map<String, Value>,
    start: NaiveDate,
    end: NaiveDate,
) -> Option<MarketStockIndustryMembershipPit> {
    let symbol = get_str(item, "con_code");
    let in_date = to_date(&get_str(item, "in_date"))?;
    let out_date = to_date(&get_str(item, "out_date"));
    if symbol.is_empty() || in_date > end || out_date.map(|date| date < start).unwrap_or(false) {
        return None;
    }
    let index_code = get_opt_str(item, "index_code").unwrap_or_else(|| classify.index_code.clone());
    let index_name = get_opt_str(item, "index_name").unwrap_or_default();
    Some(MarketStockIndustryMembershipPit {
        classification_source: classify.classification_source.clone(),
        industry_level: classify.industry_level.clone(),
        index_code,
        index_name,
        industry_code: classify.industry_code.clone(),
        industry_name: classify.industry_name.clone(),
        parent_code: classify.parent_code.clone(),
        symbol,
        symbol_name: get_opt_str(item, "con_name").unwrap_or_default(),
        in_date,
        out_date,
        available_at: in_date,
        exit_available_at: out_date,
        is_new: get_opt_str(item, "is_new").unwrap_or_default(),
        raw_payload: industry_membership_raw_payload(classify, item),
    })
}

fn industry_membership_classification_sources() -> &'static [&'static str] {
    &["SW2014", "SW2021"]
}

pub async fn sync_industry_membership(
    pool: &PgPool,
    client: &TushareClient,
    task_id: &str,
    index_codes: &[String],
    start: &str,
    end: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    let industry_level = "L1";

    repository::create_data_version(
        pool,
        task_id,
        "industry membership PIT sync",
        "tushare",
        &["market_stock_industry_membership_pit"],
        s,
        e,
    )
    .await?;

    let requested_codes = if index_codes.is_empty() {
        None
    } else {
        Some(
            index_codes
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
        )
    };
    let mut metas = Vec::new();
    for classification_source in industry_membership_classification_sources() {
        let classify_resp = client
            .index_classify(
                None,
                Some(industry_level),
                None,
                Some(classification_source),
            )
            .await?;
        let classify_maps = classify_resp
            .data
            .map(|data| data.to_maps())
            .unwrap_or_default();
        metas.extend(
            classify_maps
                .iter()
                .filter_map(|item| industry_classify_meta_from_map(item, classification_source))
                .filter(|meta| {
                    requested_codes
                        .as_ref()
                        .map(|codes| codes.contains(&meta.index_code))
                        .unwrap_or(true)
                }),
        );
    }

    let total_indices = metas.len().max(1);
    repository::heartbeat_sync_task(pool, task_id, total_indices as i32, 0, 0, 0).await?;

    let mut total_rows = 0usize;
    let mut failed = 0usize;
    let page_limit = 5000usize;
    for (idx, meta) in metas.iter().enumerate() {
        let mut offset = 0usize;
        loop {
            match client
                .index_member(
                    Some(&meta.index_code),
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
                    let rows: Vec<MarketStockIndustryMembershipPit> = maps
                        .iter()
                        .filter_map(|item| industry_membership_row_from_map(meta, item, s, e))
                        .collect();
                    if !rows.is_empty() {
                        total_rows += repository::upsert_industry_membership_batch(
                            pool, &rows, task_id, "tushare",
                        )
                        .await?;
                    }
                    if row_count < page_limit {
                        break;
                    }
                    offset += page_limit;
                }
                Err(error) => {
                    failed += 1;
                    warn!(
                        index_code = %meta.index_code,
                        error = %error,
                        "industry membership sync failed for index"
                    );
                    break;
                }
            }
        }

        let completed = idx + 1;
        let progress = ((completed * 100) / total_indices).min(99) as i32;
        repository::heartbeat_sync_task(
            pool,
            task_id,
            total_indices as i32,
            completed as i32,
            failed as i32,
            progress,
        )
        .await?;
    }

    if failed > 0 {
        repository::update_sync_task_with_error(
            pool,
            task_id,
            "partial",
            total_indices as i32,
            (total_indices.saturating_sub(failed)) as i32,
            failed as i32,
            "industry membership sync had failed index probes",
        )
        .await?;
        return Err(format!(
            "industry membership sync partial: rows_saved={}, failed_indices={}",
            total_rows, failed
        )
        .into());
    }

    info!(
        "industry membership PIT 同步完成: rows={}, failed_indices={}",
        total_rows, failed
    );
    Ok(total_rows)
}

// ─── sync_namechange (ST 历史 PIT 合规) ──────────────────────────

/// 同步股票名称变更历史，构建 PIT 合规的 ST 判断数据。
/// 从 Tushare namechange API 获取所有名称变更记录，提取 ST 期间。
pub async fn sync_namechange(pool: &PgPool, client: &TushareClient) -> Result<usize, String> {
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

    info!(
        updated = updated.rows_affected(),
        "已更新 market_stock.is_st 当前状态"
    );

    Ok(total)
}

// ─── sync_suspension (停牌数据同步) ──────────────────────────

async fn record_event_sync_completion(
    pool: &PgPool,
    task_type: &str,
    source: &str,
    trade_date: NaiveDate,
    row_count: usize,
) -> Result<(), String> {
    let task_id = format!("{}-{}", task_type, trade_date.format("%Y%m%d"));
    sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status,
            total_count, success_count, failed_count, progress,
            last_heartbeat_at, started_at, completed_at)
         VALUES ($1, $2, $3, $4, $4, 'completed', $5, $5, 0, 100, now(), now(), now())
         ON CONFLICT (task_id) DO UPDATE SET
            status='completed',
            source=EXCLUDED.source,
            start_date=EXCLUDED.start_date,
            end_date=EXCLUDED.end_date,
            total_count=EXCLUDED.total_count,
            success_count=EXCLUDED.success_count,
            failed_count=0,
            progress=100,
            last_heartbeat_at=now(),
            completed_at=now()",
    )
    .bind(task_id)
    .bind(task_type)
    .bind(source)
    .bind(trade_date)
    .bind(row_count as i32)
    .execute(pool)
    .await
    .map_err(|e| format!("记录{}完成标记失败: {}", task_type, e))?;
    Ok(())
}

async fn backfill_event_sync_completion_markers(
    pool: &PgPool,
    task_type: &str,
    source: &str,
    event_table: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<u64, String> {
    let sql = format!(
        "WITH calendar AS (
           SELECT DISTINCT trade_date
           FROM market_trade_calendar
           WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
         ),
         counts AS (
           SELECT trade_date, COUNT(*)::int4 AS row_count
           FROM {event_table}
           WHERE trade_date >= $1 AND trade_date <= $2
           GROUP BY trade_date
         )
         INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status,
            total_count, success_count, failed_count, progress,
            last_heartbeat_at, started_at, completed_at)
         SELECT $3 || '-' || to_char(c.trade_date, 'YYYYMMDD'),
                $3, $4, c.trade_date, c.trade_date, 'completed',
                COALESCE(counts.row_count, 0), COALESCE(counts.row_count, 0), 0, 100,
                now(), now(), now()
         FROM calendar c
         LEFT JOIN counts USING (trade_date)
         ON CONFLICT (task_id) DO UPDATE SET
            status='completed',
            source=CASE
                WHEN data_sync_task.source LIKE 'tushare:%' THEN data_sync_task.source
                ELSE EXCLUDED.source
            END,
            start_date=EXCLUDED.start_date,
            end_date=EXCLUDED.end_date,
            total_count=EXCLUDED.total_count,
            success_count=EXCLUDED.success_count,
            failed_count=0,
            progress=100,
            last_heartbeat_at=now(),
            completed_at=now()"
    );
    sqlx::query(&sql)
        .bind(start)
        .bind(end)
        .bind(task_type)
        .bind(source)
        .execute(pool)
        .await
        .map(|result| result.rows_affected())
        .map_err(|e| format!("补齐{}完成标记失败: {}", task_type, e))
}

pub async fn backfill_suspension_completion_markers(
    pool: &PgPool,
    start_date: &str,
    end_date: &str,
) -> Result<u64, String> {
    let start = NaiveDate::parse_from_str(start_date, "%Y%m%d")
        .map_err(|e| format!("start_date解析: {}", e))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y%m%d")
        .map_err(|e| format!("end_date解析: {}", e))?;
    if start > end {
        return Err("start_date 不能晚于 end_date".to_string());
    }
    backfill_event_sync_completion_markers(
        pool,
        "suspension_daily",
        "derived:susp_existing",
        "market_stock_suspension",
        start,
        end,
    )
    .await
}

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
    let d =
        NaiveDate::parse_from_str(trade_date, "%Y%m%d").map_err(|e| format!("日期解析: {}", e))?;
    sqlx::query("DELETE FROM market_stock_suspension WHERE trade_date = $1")
        .bind(d)
        .execute(pool)
        .await
        .map_err(|e| format!("清除旧数据: {}", e))?;

    for item in &maps {
        let ts_code = item["ts_code"].as_str().unwrap_or("");
        let s_type = item["suspend_type"].as_str().unwrap_or("");

        if ts_code.is_empty() {
            continue;
        }

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

    info!(
        total,
        updated = updated.rows_affected(),
        date = trade_date,
        "停牌数据同步完成"
    );
    record_event_sync_completion(pool, "suspension_daily", "tushare:suspend_d", d, total).await?;
    Ok(total)
}

/// 同步停牌/复牌信息（日期范围），并为零停牌交易日写入官方完成标记。
///
/// suspend_d 的 range 参数在当前代理路径上会长时间无响应；这里显式按
/// 交易日调用单日官方接口，优先保证可审计完整性。
pub async fn sync_suspension_range(
    pool: &PgPool,
    client: &TushareClient,
    start_date: &str,
    end_date: &str,
) -> Result<usize, String> {
    let start = NaiveDate::parse_from_str(start_date, "%Y%m%d")
        .map_err(|e| format!("start_date解析: {}", e))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y%m%d")
        .map_err(|e| format!("end_date解析: {}", e))?;
    if start > end {
        return Err("start_date 不能晚于 end_date".to_string());
    }

    let trade_dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT DISTINCT trade_date
         FROM market_trade_calendar
         WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("查询交易日历失败: {}", e))?;

    let mut total = 0usize;
    for trade_date in trade_dates {
        let trade_date_s = trade_date.format("%Y%m%d").to_string();
        let day_count = tokio::time::timeout(std::time::Duration::from_secs(45), async {
            sync_suspension(pool, client, &trade_date_s).await
        })
        .await
        .map_err(|_| format!("suspend_d API 超时: {}", trade_date_s))??;
        total += day_count;
    }

    info!(
        total,
        start = start_date,
        end = end_date,
        "停牌范围数据同步完成"
    );
    Ok(total)
}

/// 从已同步的官方日线缺失中派生历史停牌。
///
/// Tushare `suspend_d` 在部分早期历史日期可能返回 0 行，但同日全量 daily
/// 同步不会返回停牌股票行情。对已上市、未退市且缺少同日 daily bar 的 A 股，
/// 记录为派生停牌事实；不补价格，不跨日推断。
pub async fn derive_suspension_from_daily_absence(
    pool: &PgPool,
    start_date: &str,
    end_date: &str,
) -> Result<u64, String> {
    let start = NaiveDate::parse_from_str(start_date, "%Y%m%d")
        .map_err(|e| format!("start_date解析: {}", e))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y%m%d")
        .map_err(|e| format!("end_date解析: {}", e))?;
    if start > end {
        return Err("start_date 不能晚于 end_date".to_string());
    }

    let inserted = sqlx::query(
        "WITH calendar AS (
           SELECT DISTINCT trade_date
           FROM market_trade_calendar
           WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
         ),
         expected AS (
           SELECT DISTINCT s.symbol, c.trade_date
           FROM calendar c
           JOIN market_stock s
             ON s.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
            AND s.list_date IS NOT NULL
            AND s.list_date <= c.trade_date
            AND (s.delist_date IS NULL OR s.delist_date >= c.trade_date)
         ),
         missing AS (
           SELECT e.symbol, e.trade_date
           FROM expected e
           LEFT JOIN market_stock_daily_bar_adj bar
             ON bar.symbol = e.symbol AND bar.trade_date = e.trade_date
           LEFT JOIN market_stock_suspension susp
             ON susp.symbol = e.symbol AND susp.trade_date = e.trade_date
           WHERE bar.symbol IS NULL
             AND susp.symbol IS NULL
         )
         INSERT INTO market_stock_suspension (symbol, trade_date, suspend_type)
         SELECT symbol, trade_date, 'S'
         FROM missing
         ON CONFLICT (symbol, trade_date) DO NOTHING",
    )
    .bind(start)
    .bind(end)
    .execute(pool)
    .await
    .map_err(|e| format!("派生停牌缺失失败: {}", e))?
    .rows_affected();

    sqlx::query(
        "WITH calendar AS (
           SELECT DISTINCT trade_date
           FROM market_trade_calendar
           WHERE is_open = true AND trade_date >= $1 AND trade_date <= $2
         ),
         counts AS (
           SELECT trade_date, COUNT(*)::int4 AS row_count
           FROM market_stock_suspension
           WHERE trade_date >= $1 AND trade_date <= $2
           GROUP BY trade_date
         )
         INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status,
            total_count, success_count, failed_count, progress,
            last_heartbeat_at, started_at, completed_at)
         SELECT 'suspension_daily-' || to_char(c.trade_date, 'YYYYMMDD'),
                'suspension_daily',
                CASE
                  WHEN COALESCE(counts.row_count, 0) > 0
                  THEN 'derived:daily_absence_suspension'
                  ELSE 'tushare:suspend_d'
                END,
                c.trade_date, c.trade_date, 'completed',
                COALESCE(counts.row_count, 0), COALESCE(counts.row_count, 0), 0, 100,
                now(), now(), now()
         FROM calendar c
         LEFT JOIN counts USING (trade_date)
         ON CONFLICT (task_id) DO UPDATE SET
            status='completed',
            source=CASE
              WHEN COALESCE(EXCLUDED.total_count, 0) > 0
               AND NOT (data_sync_task.source LIKE 'tushare:%' AND data_sync_task.total_count > 0)
              THEN 'derived:daily_absence_suspension'
              ELSE data_sync_task.source
            END,
            total_count=EXCLUDED.total_count,
            success_count=EXCLUDED.success_count,
            failed_count=0,
            progress=100,
            last_heartbeat_at=now(),
            completed_at=now()",
    )
    .bind(start)
    .bind(end)
    .execute(pool)
    .await
    .map_err(|e| format!("记录派生停牌完成标记失败: {}", e))?;

    info!(
        inserted,
        start = start_date,
        end = end_date,
        "从日线缺失派生历史停牌完成"
    );
    Ok(inserted)
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

    let d =
        NaiveDate::parse_from_str(trade_date, "%Y%m%d").map_err(|e| format!("日期解析: {}", e))?;

    // 清除当日旧数据
    sqlx::query("DELETE FROM market_stock_limit WHERE trade_date = $1")
        .bind(d)
        .execute(pool)
        .await
        .map_err(|e| format!("清除: {}", e))?;

    for item in &maps {
        let ts_code = item["ts_code"].as_str().unwrap_or("");
        if ts_code.is_empty() {
            continue;
        }

        sqlx::query(
            "INSERT INTO market_stock_limit (symbol, trade_date) VALUES ($1, $2) ON CONFLICT (symbol, trade_date) DO NOTHING",
        )
        .bind(ts_code).bind(d).execute(pool).await.map_err(|e| format!("insert: {}", e))?;
        total += 1;
    }

    info!(total, date = trade_date, "涨跌停数据同步完成");
    record_event_sync_completion(pool, "limit_daily", "tushare:limit_list_d", d, total).await?;
    Ok(total)
}

/// 同步涨跌停数据（日期范围，一次API调用）
pub async fn sync_limit_list_range(
    pool: &PgPool,
    client: &TushareClient,
    start_date: &str,
    end_date: &str,
) -> Result<usize, String> {
    let mut total = 0usize;
    let start = NaiveDate::parse_from_str(start_date, "%Y%m%d")
        .map_err(|e| format!("start_date解析: {}", e))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y%m%d")
        .map_err(|e| format!("end_date解析: {}", e))?;
    if start > end {
        return Err("start_date 不能晚于 end_date".to_string());
    }

    for (chunk_start, chunk_end) in date_chunks_by_days(start, end, 3) {
        let chunk_start_s = chunk_start.format("%Y%m%d").to_string();
        let chunk_end_s = chunk_end.format("%Y%m%d").to_string();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(45),
            client.limit_list_d(None, None, Some(&chunk_start_s), Some(&chunk_end_s)),
        )
        .await
        .map_err(|_| {
            format!(
                "limit_list_d range API 超时: {}~{}",
                chunk_start_s, chunk_end_s
            )
        })?
        .map_err(|e| {
            format!(
                "limit_list_d range API {}~{}: {}",
                chunk_start_s, chunk_end_s, e
            )
        })?;

        sqlx::query("DELETE FROM market_stock_limit WHERE trade_date >= $1 AND trade_date <= $2")
            .bind(chunk_start)
            .bind(chunk_end)
            .execute(pool)
            .await
            .map_err(|e| format!("清除涨跌停范围数据 {}~{}: {}", chunk_start, chunk_end, e))?;

        let maps = resp.data.map(|d| d.to_maps()).unwrap_or_default();
        for item in &maps {
            let ts_code = item["ts_code"].as_str().unwrap_or("");
            let trade_date_str = item["trade_date"].as_str().unwrap_or("");
            if ts_code.is_empty() || trade_date_str.is_empty() {
                continue;
            }

            let d = NaiveDate::parse_from_str(trade_date_str, "%Y%m%d")
                .map_err(|_| "日期解析".to_string())?;

            sqlx::query(
                "INSERT INTO market_stock_limit (symbol, trade_date) VALUES ($1, $2) ON CONFLICT (symbol, trade_date) DO NOTHING",
            )
            .bind(ts_code)
            .bind(d)
            .execute(pool)
            .await
            .map_err(|e| format!("insert: {}", e))?;
            total += 1;
        }
    }

    info!(
        total,
        start = start_date,
        end = end_date,
        "涨跌停范围同步完成"
    );
    Ok(total)
}

pub async fn derive_limit_list_from_daily_bars(
    pool: &PgPool,
    start_date: &str,
    end_date: &str,
) -> Result<u64, String> {
    let start = NaiveDate::parse_from_str(start_date, "%Y%m%d")
        .map_err(|e| format!("start_date解析: {}", e))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y%m%d")
        .map_err(|e| format!("end_date解析: {}", e))?;
    if start > end {
        return Err("start_date 不能晚于 end_date".to_string());
    }

    let mut inserted = 0u64;
    for (chunk_start, chunk_end) in years_in_range(start, end) {
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("启动派生涨跌停事务失败: {}", e))?;

        sqlx::query("DELETE FROM market_stock_limit WHERE trade_date >= $1 AND trade_date <= $2")
            .bind(chunk_start)
            .bind(chunk_end)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                format!(
                    "清除派生涨跌停旧数据({}~{})失败: {}",
                    chunk_start, chunk_end, e
                )
            })?;

        let chunk_inserted = sqlx::query(
            "WITH open_calendar AS MATERIALIZED (
               SELECT trade_date, row_number() OVER (ORDER BY trade_date) AS seq
               FROM (
                 SELECT DISTINCT trade_date
                 FROM market_trade_calendar
                 WHERE is_open = true
               ) c
             ),
             stock_profile AS MATERIALIZED (
               SELECT ms.symbol, ms.exchange, ms.market, ms.list_date, ms.delist_date,
                      CASE
                        WHEN ms.list_date IS NULL OR ms.list_date < ($1::date - 45) THEN 0::bigint
                        ELSE (
                          SELECT MIN(oc.seq)
                          FROM open_calendar oc
                          WHERE oc.trade_date >= ms.list_date
                        )
                      END AS list_seq
               FROM market_stock ms
               WHERE ms.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
             ),
             st_ranges AS MATERIALIZED (
               SELECT symbol, start_date, COALESCE(end_date, DATE '9999-12-31') AS end_date
               FROM market_stock_name_history
               WHERE is_st = true
                 AND start_date <= $2
                 AND COALESCE(end_date, DATE '9999-12-31') >= $1
             ),
             bars AS (
               SELECT b.symbol, b.trade_date, b.close, b.pre_close,
                      sp.exchange, sp.market, oc.seq AS trade_seq, sp.list_seq,
                      BOOL_OR(sr.symbol IS NOT NULL) AS is_st_at_trade
               FROM market_stock_daily_bar b
               JOIN stock_profile sp ON sp.symbol = b.symbol
               JOIN open_calendar oc ON oc.trade_date = b.trade_date
               LEFT JOIN st_ranges sr
                 ON sr.symbol = b.symbol
                AND sr.start_date <= b.trade_date
                AND sr.end_date >= b.trade_date
               WHERE b.trade_date >= $1
                 AND b.trade_date <= $2
                 AND b.close IS NOT NULL
                 AND b.pre_close IS NOT NULL
                 AND b.pre_close > 0
                 AND COALESCE(sp.list_date, DATE '1900-01-01') <= b.trade_date
                 AND (sp.delist_date IS NULL OR sp.delist_date >= b.trade_date)
               GROUP BY b.symbol, b.trade_date, b.close, b.pre_close,
                        sp.exchange, sp.market, oc.seq, sp.list_seq
             ),
             classified AS (
               SELECT symbol, trade_date,
                      CASE
                        WHEN is_st_at_trade THEN 0.05::numeric
                        WHEN symbol LIKE '688%.SH' THEN 0.20::numeric
                        WHEN (symbol LIKE '300%.SZ' OR symbol LIKE '301%.SZ')
                             AND trade_date >= DATE '2020-08-24' THEN 0.20::numeric
                        ELSE 0.10::numeric
                      END AS limit_rate,
                      close,
                      pre_close
               FROM bars
               WHERE list_seq = 0 OR list_seq IS NULL OR trade_seq - list_seq + 1 > 5
             ),
             limit_rows AS (
               SELECT symbol,
                      trade_date,
                      CASE
                        WHEN close >= ROUND(pre_close * (1 + limit_rate), 2) - 0.001::numeric
                          THEN 'U'
                        WHEN close <= ROUND(pre_close * (1 - limit_rate), 2) + 0.001::numeric
                          THEN 'D'
                        ELSE NULL
                      END AS limit_type
               FROM classified
             )
             INSERT INTO market_stock_limit (symbol, trade_date, limit_type)
             SELECT symbol, trade_date, limit_type
             FROM limit_rows
             WHERE limit_type IS NOT NULL
             ON CONFLICT (symbol, trade_date) DO UPDATE SET limit_type = EXCLUDED.limit_type",
        )
        .bind(chunk_start)
        .bind(chunk_end)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("派生涨跌停历史({}~{})失败: {}", chunk_start, chunk_end, e))?
        .rows_affected();

        tx.commit()
            .await
            .map_err(|e| format!("提交派生涨跌停事务失败: {}", e))?;
        inserted += chunk_inserted;
        info!(
            inserted = chunk_inserted,
            start = %chunk_start,
            end = %chunk_end,
            "涨跌停历史分片已由日线派生"
        );
    }

    let _ = backfill_event_sync_completion_markers(
        pool,
        "limit_daily",
        "derived:daily_limit",
        "market_stock_limit",
        start,
        end,
    )
    .await?;

    info!(
        inserted,
        start = start_date,
        end = end_date,
        "涨跌停历史已由日线派生"
    );
    Ok(inserted)
}

pub async fn backfill_limit_completion_markers(
    pool: &PgPool,
    start_date: &str,
    end_date: &str,
    source: &str,
) -> Result<u64, String> {
    let start = NaiveDate::parse_from_str(start_date, "%Y%m%d")
        .map_err(|e| format!("start_date解析: {}", e))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y%m%d")
        .map_err(|e| format!("end_date解析: {}", e))?;
    if start > end {
        return Err("start_date 不能晚于 end_date".to_string());
    }
    backfill_event_sync_completion_markers(
        pool,
        "limit_daily",
        source,
        "market_stock_limit",
        start,
        end,
    )
    .await
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
    fn quarters_in_range_splits_by_calendar_quarter() {
        let start = NaiveDate::from_ymd_opt(2026, 2, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 7, 2).unwrap();

        let quarters = quarters_in_range(start, end);

        assert_eq!(
            quarters,
            vec![
                (
                    NaiveDate::from_ymd_opt(2026, 2, 15).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 3, 31).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 4, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 6, 30).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 7, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 7, 2).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn moneyflow_full_market_trade_dates_use_open_calendar_days() {
        let start = NaiveDate::from_ymd_opt(2026, 6, 13).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 17).unwrap();
        let open_dates = vec![
            NaiveDate::from_ymd_opt(2026, 6, 15).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 16).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 17).unwrap(),
        ];

        let dates = moneyflow_full_market_trade_dates(start, end, open_dates);

        assert_eq!(
            dates,
            vec![
                NaiveDate::from_ymd_opt(2026, 6, 15).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 16).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 17).unwrap(),
            ]
        );
    }

    #[test]
    fn moneyflow_full_market_trade_dates_fall_back_to_calendar_days() {
        let start = NaiveDate::from_ymd_opt(2026, 6, 13).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();

        let dates = moneyflow_full_market_trade_dates(start, end, Vec::new());

        assert_eq!(
            dates,
            vec![
                NaiveDate::from_ymd_opt(2026, 6, 13).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 14).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 15).unwrap(),
            ]
        );
    }

    #[test]
    fn date_chunks_by_days_splits_without_crossing_end_date() {
        let start = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 8).unwrap();

        let chunks = date_chunks_by_days(start, end, 3);

        assert_eq!(
            chunks,
            vec![
                (
                    NaiveDate::from_ymd_opt(2026, 6, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 6, 3).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 6, 4).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 6, 6).unwrap()
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 6, 7).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 6, 8).unwrap()
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
        item.insert("total_share".to_string(), json!(1940591.8198));
        item.insert("float_share".to_string(), json!(1940554.4675));
        item.insert("free_share".to_string(), json!(1800000.25));
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
        assert_eq!(row.total_share, Decimal::from_f64_retain(1940591.8198));
        assert_eq!(row.float_share, Decimal::from_f64_retain(1940554.4675));
        assert_eq!(row.free_share, Decimal::from_f64_retain(1800000.25));
        assert_eq!(row.total_mv, Decimal::from_f64_retain(123456.7));
        assert_eq!(row.circ_mv, Decimal::from_f64_retain(98765.4));
    }

    #[test]
    fn daily_basic_detects_tushare_hourly_limit_errors() {
        assert!(is_tushare_hourly_limit_error(
            "API error (code=40203): 抱歉，您每小时最多访问该接口4000次"
        ));
        assert!(!is_tushare_hourly_limit_error("network timeout"));
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
    fn margin_detail_row_uses_next_open_day_available_at() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("trade_date".to_string(), json!("20260619"));
        item.insert("name".to_string(), json!("平安银行"));
        item.insert("rzye".to_string(), json!(5231404083.0));
        item.insert("rqye".to_string(), json!(18014220.0));
        item.insert("rzmre".to_string(), json!(81747812.0));
        item.insert("rqyl".to_string(), json!(1682000.0));
        let open_dates = vec![
            NaiveDate::from_ymd_opt(2026, 6, 19).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 22).unwrap(),
        ];

        let row = margin_detail_row_from_map(&item, &open_dates).expect("margin detail row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(
            row.trade_date,
            NaiveDate::from_ymd_opt(2026, 6, 19).unwrap()
        );
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 6, 22).unwrap()
        );
        assert_eq!(
            row.source_published_at
                .expect("source published at")
                .to_rfc3339(),
            "2026-06-22T00:30:00+00:00"
        );
        assert_eq!(row.rzye, Decimal::from_f64_retain(5231404083.0));
        assert_eq!(row.rqye, Decimal::from_f64_retain(18014220.0));
        assert_eq!(row.rzmre, Decimal::from_f64_retain(81747812.0));
        assert_eq!(row.rqyl, Decimal::from_f64_retain(1682000.0));
    }

    #[test]
    fn margin_detail_available_at_falls_back_to_next_calendar_day_without_calendar() {
        let trade_date = NaiveDate::from_ymd_opt(2026, 6, 19).unwrap();

        assert_eq!(
            margin_detail_available_at_from_open_dates(trade_date, &[]),
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap()
        );
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
    fn share_float_row_maps_ann_date_as_available_at_and_keeps_unlock_date() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("ann_date".to_string(), json!("20260115"));
        item.insert("float_date".to_string(), json!("20260301"));
        item.insert("float_share".to_string(), json!(12500.0));
        item.insert("float_ratio".to_string(), json!(2.5));
        item.insert("holder_name".to_string(), json!("Sample Holder"));
        item.insert("share_type".to_string(), json!("首发原股东限售股份"));

        let row = share_float_row_from_map(&item).expect("share float row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(row.ann_date, NaiveDate::from_ymd_opt(2026, 1, 15).unwrap());
        assert_eq!(row.float_date, NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 1, 15).unwrap()
        );
        assert_eq!(row.float_share, Decimal::from_f64_retain(12500.0));
        assert_eq!(row.float_ratio, Decimal::from_f64_retain(2.5));
        assert_eq!(row.holder_name, "Sample Holder");
        assert_eq!(row.share_type, "首发原股东限售股份");
    }

    #[test]
    fn main_business_row_uses_joined_available_at_and_stable_row_hash() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("end_date".to_string(), json!("20231231"));
        item.insert("bz_item".to_string(), json!("零售金融业务"));
        item.insert("bz_code".to_string(), json!("P"));
        item.insert("bz_sales".to_string(), json!(123456.78));
        item.insert("bz_profit".to_string(), json!(34567.89));
        item.insert("bz_cost".to_string(), json!(88888.89));
        item.insert("curr_type".to_string(), json!("CNY"));
        item.insert("update_flag".to_string(), json!("0"));
        let available_at = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();

        let row = main_business_row_from_map(&item, available_at).expect("main business row");
        let same_row =
            main_business_row_from_map(&item, available_at).expect("same main business row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2023, 12, 31).unwrap());
        assert_eq!(row.available_at, available_at);
        assert_eq!(row.business_type, "P");
        assert_eq!(row.bz_item, "零售金融业务");
        assert_eq!(row.bz_sales, Decimal::from_f64_retain(123456.78));
        assert_eq!(row.bz_profit, Decimal::from_f64_retain(34567.89));
        assert_eq!(row.bz_cost, Decimal::from_f64_retain(88888.89));
        assert_eq!(row.source_row_hash.len(), 16);
        assert_eq!(row.source_row_hash, same_row.source_row_hash);
    }

    #[test]
    fn main_business_sample_sync_uses_separate_attempt_source() {
        assert_eq!(main_business_attempt_source(&[]), "main_business");
        assert_eq!(
            main_business_attempt_source(&["000001.SZ".to_string()]),
            "main_business_sample"
        );
    }

    #[test]
    fn equity_pledge_stat_uses_conservative_next_day_available_at() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("end_date".to_string(), json!("20260618"));
        item.insert("pledge_count".to_string(), json!(9));
        item.insert("unrest_pledge".to_string(), json!(2460.5));
        item.insert("rest_pledge".to_string(), json!(0.0));
        item.insert("total_share".to_string(), json!(1940591.82));
        item.insert("pledge_ratio".to_string(), json!(0.13));

        let row = equity_pledge_stat_row_from_map(&item).expect("pledge stat row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2026, 6, 18).unwrap());
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 6, 19).unwrap()
        );
        assert_eq!(row.pledge_count, Some(9));
        assert_eq!(row.pledge_ratio, Decimal::from_f64_retain(0.13).unwrap());
    }

    #[test]
    fn equity_pledge_detail_uses_ann_date_as_available_at_and_hashes_source_row() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000002.SZ"));
        item.insert("ann_date".to_string(), json!("20170323"));
        item.insert("holder_name".to_string(), json!("深圳市钜盛华股份有限公司"));
        item.insert("pledge_amount".to_string(), json!(9100.0));
        item.insert("start_date".to_string(), json!("20170321"));
        item.insert("end_date".to_string(), Value::Null);
        item.insert("is_release".to_string(), json!("0"));
        item.insert("release_date".to_string(), Value::Null);
        item.insert("pledgor".to_string(), json!("平安证券股份有限公司"));
        item.insert("holding_amount".to_string(), json!(92607.0472));
        item.insert("pledged_amount".to_string(), json!(92607.0462));
        item.insert("p_total_ratio".to_string(), Value::Null);
        item.insert("h_total_ratio".to_string(), json!(8.39));
        item.insert("is_buyback".to_string(), json!("1"));

        let row = equity_pledge_detail_row_from_map(&item).expect("pledge detail row");
        let same_row = equity_pledge_detail_row_from_map(&item).expect("same pledge detail row");

        assert_eq!(row.symbol, "000002.SZ");
        assert_eq!(row.ann_date, NaiveDate::from_ymd_opt(2017, 3, 23).unwrap());
        assert_eq!(row.available_at, row.ann_date);
        assert_eq!(
            row.pledge_start_date,
            Some(NaiveDate::from_ymd_opt(2017, 3, 21).unwrap())
        );
        assert_eq!(row.holder_name, "深圳市钜盛华股份有限公司");
        assert_eq!(row.pledgor, "平安证券股份有限公司");
        assert_eq!(row.source_row_hash.len(), 16);
        assert_eq!(row.source_row_hash, same_row.source_row_hash);
    }

    #[test]
    fn equity_pledge_detail_allows_missing_source_start_date() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000040.SZ"));
        item.insert("ann_date".to_string(), json!("20140107"));
        item.insert("holder_name".to_string(), json!("中国宝安集团控股有限公司"));
        item.insert("pledge_amount".to_string(), json!(4500.0));
        item.insert("start_date".to_string(), Value::Null);
        item.insert("end_date".to_string(), Value::Null);
        item.insert("pledgor".to_string(), json!(""));

        let row = equity_pledge_detail_row_from_map(&item).expect("pledge detail row");

        assert_eq!(row.symbol, "000040.SZ");
        assert_eq!(row.ann_date, NaiveDate::from_ymd_opt(2014, 1, 7).unwrap());
        assert_eq!(row.available_at, row.ann_date);
        assert_eq!(row.pledge_start_date, None);
        assert_eq!(row.source_row_hash.len(), 16);
    }

    #[test]
    fn shareholder_structure_rows_use_ann_date_as_available_at_and_hash_source_rows() {
        let mut holder_number = Map::new();
        holder_number.insert("ts_code".to_string(), json!("000001.SZ"));
        holder_number.insert("ann_date".to_string(), json!("20260425"));
        holder_number.insert("end_date".to_string(), json!("20260331"));
        holder_number.insert("holder_num".to_string(), json!(543210));

        let number_row =
            shareholder_holder_number_row_from_map(&holder_number).expect("holder number row");
        assert_eq!(number_row.symbol, "000001.SZ");
        assert_eq!(
            number_row.ann_date,
            NaiveDate::from_ymd_opt(2026, 4, 25).unwrap()
        );
        assert_eq!(number_row.available_at, number_row.ann_date);
        assert_eq!(
            number_row.end_date,
            NaiveDate::from_ymd_opt(2026, 3, 31).unwrap()
        );
        assert_eq!(number_row.holder_num, Some(543210));

        let mut top10 = Map::new();
        top10.insert("ts_code".to_string(), json!("000001.SZ"));
        top10.insert("ann_date".to_string(), json!("20260425"));
        top10.insert("end_date".to_string(), json!("20260331"));
        top10.insert(
            "holder_name".to_string(),
            json!("中央汇金资产管理有限责任公司"),
        );
        top10.insert("hold_amount".to_string(), json!(12345.67));
        top10.insert("hold_ratio".to_string(), json!(1.23));
        top10.insert("hold_change".to_string(), json!(-456.0));
        top10.insert("holder_type".to_string(), json!("机构"));

        let top10_row = shareholder_top10_holder_row_from_map(&top10, false).expect("top10 row");
        let same_top10 =
            shareholder_top10_holder_row_from_map(&top10, false).expect("same top10 row");
        assert_eq!(top10_row.available_at, top10_row.ann_date);
        assert_eq!(top10_row.source_row_hash.len(), 16);
        assert_eq!(top10_row.source_row_hash, same_top10.source_row_hash);

        let mut trade = Map::new();
        trade.insert("ts_code".to_string(), json!("000002.SZ"));
        trade.insert("ann_date".to_string(), json!("20250115"));
        trade.insert("holder_name".to_string(), json!("董事张三"));
        trade.insert("in_de".to_string(), json!("增持"));
        trade.insert("change_vol".to_string(), json!(120.0));
        trade.insert("change_ratio".to_string(), json!(0.02));
        trade.insert("avg_price".to_string(), json!(11.8));

        let trade_row = shareholder_holder_trade_row_from_map(&trade).expect("holder trade row");
        assert_eq!(trade_row.symbol, "000002.SZ");
        assert_eq!(trade_row.available_at, trade_row.ann_date);
        assert_eq!(trade_row.in_de.as_deref(), Some("增持"));
        assert_eq!(trade_row.source_row_hash.len(), 16);
    }

    #[test]
    fn shareholder_structure_source_filters_allow_staged_low_fanout_sync() {
        let low_fanout = shareholder_structure_source_filter_flags(&[
            "holder_number".to_string(),
            "holder_trade".to_string(),
        ])
        .expect("low fanout source filters");
        assert!(low_fanout.holder_number);
        assert!(low_fanout.holder_trade);
        assert!(!low_fanout.top10_holders);
        assert!(!low_fanout.top10_float_holders);

        let all = shareholder_structure_source_filter_flags(&[]).expect("empty means all");
        assert!(all.holder_number);
        assert!(all.holder_trade);
        assert!(all.top10_holders);
        assert!(all.top10_float_holders);

        let error = shareholder_structure_source_filter_flags(&["mystery".to_string()])
            .expect_err("unknown source filter should fail");
        assert!(error.contains("unsupported shareholder_structure source_filter"));
    }

    #[test]
    fn futures_price_chain_rows_use_next_day_available_at_for_pit_safety() {
        let mut daily = Map::new();
        daily.insert("ts_code".to_string(), json!("CU1811.SHF"));
        daily.insert("trade_date".to_string(), json!("20181113"));
        daily.insert("close".to_string(), json!(48900.0));
        daily.insert("settle".to_string(), json!(48820.0));
        daily.insert("vol".to_string(), json!(153210.0));
        daily.insert("oi".to_string(), json!(232410.0));

        let row = futures_daily_row_from_map(&daily).expect("futures daily row");

        assert_eq!(row.ts_code, "CU1811.SHF");
        assert_eq!(
            row.trade_date,
            NaiveDate::from_ymd_opt(2018, 11, 13).unwrap()
        );
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2018, 11, 14).unwrap()
        );
        assert_eq!(row.close, Decimal::from_f64_retain(48900.0));
        assert_eq!(row.raw_payload["ts_code"], json!("CU1811.SHF"));
    }

    #[test]
    fn futures_price_chain_rows_require_native_keys_before_syncing() {
        let mut wsr = Map::new();
        wsr.insert("trade_date".to_string(), json!("20181113"));
        wsr.insert("symbol".to_string(), json!("CU"));
        wsr.insert("exchange".to_string(), json!("SHFE"));
        wsr.insert("warehouse".to_string(), json!("上港物流"));
        wsr.insert("vol_chg".to_string(), json!(-500.0));

        let row = futures_warehouse_receipt_row_from_map(&wsr).expect("warehouse row");
        assert_eq!(row.symbol, "CU");
        assert_eq!(row.exchange, "SHFE");
        assert_eq!(row.warehouse, "上港物流");
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2018, 11, 14).unwrap()
        );

        let mut missing_broker = Map::new();
        missing_broker.insert("trade_date".to_string(), json!("20181113"));
        missing_broker.insert("symbol".to_string(), json!("CU"));
        missing_broker.insert("exchange".to_string(), json!("SHFE"));
        assert!(futures_holding_rank_row_from_map(&missing_broker).is_none());
    }

    #[test]
    fn futures_price_chain_attempt_key_fits_sync_attempt_symbol_column() {
        let trade_date = NaiveDate::from_ymd_opt(2018, 11, 13).unwrap();

        assert!(futures_price_chain_attempt_key(trade_date, None, None).len() <= 20);
        assert!(
            futures_price_chain_attempt_key(
                trade_date,
                Some("VERY-LONG-FUTURES-CONTRACT-CODE"),
                Some("SHFE")
            )
            .len()
                <= 20
        );
    }

    #[test]
    fn futures_price_chain_trade_dates_use_open_calendar_days_without_weekends() {
        let start = NaiveDate::from_ymd_opt(2026, 6, 13).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 17).unwrap();
        let open_dates = vec![
            NaiveDate::from_ymd_opt(2026, 6, 15).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 16).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 17).unwrap(),
        ];

        let dates = futures_price_chain_trade_dates(start, end, open_dates)
            .expect("calendar-backed futures dates");

        assert_eq!(
            dates,
            vec![
                NaiveDate::from_ymd_opt(2026, 6, 15).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 16).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 17).unwrap(),
            ]
        );
    }

    #[test]
    fn futures_price_chain_trade_dates_error_when_calendar_is_missing() {
        let start = NaiveDate::from_ymd_opt(2026, 6, 13).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 17).unwrap();

        let error = futures_price_chain_trade_dates(start, end, Vec::new())
            .expect_err("missing calendar must block raw sync");

        assert!(error.contains("market_trade_calendar"));
        assert!(error.contains("sync trade_calendar"));
    }

    #[test]
    fn futures_price_chain_trade_dates_sql_prefers_futures_exchange_calendars() {
        let sql = futures_price_chain_trade_dates_sql();

        assert!(sql.contains("futures_calendar"));
        assert!(sql.contains("'SHFE'"));
        assert!(sql.contains("'DCE'"));
        assert!(sql.contains("'CZCE'"));
        assert!(sql.contains("'CFFEX'"));
        assert!(sql.contains("'INE'"));
        assert!(sql.contains("NOT EXISTS (SELECT 1 FROM futures_calendar)"));
    }

    #[test]
    fn industry_membership_row_uses_effective_dates_as_pit_boundaries() {
        let classify = IndustryClassifyMeta {
            classification_source: "SW2021".to_string(),
            industry_level: "L1".to_string(),
            index_code: "801010.SI".to_string(),
            industry_code: "110000".to_string(),
            industry_name: "农林牧渔".to_string(),
            parent_code: "0".to_string(),
        };
        let mut item = Map::new();
        item.insert("index_code".to_string(), json!("801010.SI"));
        item.insert("index_name".to_string(), json!("农林牧渔(申万)"));
        item.insert("con_code".to_string(), json!("000034.SZ"));
        item.insert("con_name".to_string(), json!("神州数码"));
        item.insert("in_date".to_string(), json!("20070703"));
        item.insert("out_date".to_string(), json!("20090601"));
        item.insert("is_new".to_string(), json!("N"));

        let row = industry_membership_row_from_map(
            &classify,
            &item,
            NaiveDate::from_ymd_opt(2006, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
        )
        .expect("industry membership row");

        assert_eq!(row.symbol, "000034.SZ");
        assert_eq!(row.in_date, NaiveDate::from_ymd_opt(2007, 7, 3).unwrap());
        assert_eq!(row.available_at, row.in_date);
        assert_eq!(
            row.out_date,
            Some(NaiveDate::from_ymd_opt(2009, 6, 1).unwrap())
        );
        assert_eq!(row.exit_available_at, row.out_date);
        assert_eq!(row.is_new, "N");
        assert_eq!(row.industry_code, "110000");
    }

    #[test]
    fn industry_membership_sync_defaults_to_sw2014_and_sw2021_sources() {
        assert_eq!(
            industry_membership_classification_sources(),
            &["SW2014", "SW2021"]
        );
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
    fn event_sync_progress_interval_updates_bounded_tasks_before_completion() {
        assert_eq!(event_sync_progress_interval(0, 100), 1);
        assert_eq!(event_sync_progress_interval(20, 100), 2);
        assert_eq!(event_sync_progress_interval(100, 100), 10);
        assert_eq!(event_sync_progress_interval(2_000, 100), 100);
        assert_eq!(event_sync_progress_interval(37, 8), 3);
    }

    #[test]
    fn event_sync_attempt_status_tracks_symbol_success_and_row_count() {
        let attempt = event_symbol_sync_attempt(
            "forecast",
            "000001.SZ",
            NaiveDate::from_ymd_opt(2017, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
            "task-1",
            3,
            None,
        );

        assert_eq!(attempt.source, "forecast");
        assert_eq!(attempt.symbol, "000001.SZ");
        assert_eq!(attempt.status, "completed");
        assert_eq!(attempt.row_count, 3);
        assert_eq!(attempt.error_message, None);
    }

    #[test]
    fn event_sync_attempt_preserves_failed_symbol_error() {
        let attempt = event_symbol_sync_attempt(
            "express",
            "600000.SH",
            NaiveDate::from_ymd_opt(2017, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
            "task-2",
            0,
            Some("permission denied".to_string()),
        );

        assert_eq!(attempt.source, "express");
        assert_eq!(attempt.symbol, "600000.SH");
        assert_eq!(attempt.status, "failed");
        assert_eq!(attempt.row_count, 0);
        assert_eq!(attempt.error_message.as_deref(), Some("permission denied"));
    }

    #[test]
    fn forecast_row_uses_revision_ann_date_as_available_at() {
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
            NaiveDate::from_ymd_opt(2024, 5, 1).unwrap()
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
    fn disclosure_date_row_uses_latest_required_available_date() {
        let mut item = Map::new();
        item.insert("ts_code".to_string(), json!("000001.SZ"));
        item.insert("ann_date".to_string(), json!("20240401"));
        item.insert("end_date".to_string(), json!("20231231"));
        item.insert("pre_date".to_string(), json!("20240315"));
        item.insert("actual_date".to_string(), json!("20240314"));
        item.insert("modify_date".to_string(), json!("20240220"));

        let row = disclosure_date_row_from_map(&item).expect("disclosure date row");

        assert_eq!(row.symbol, "000001.SZ");
        assert_eq!(row.end_date, NaiveDate::from_ymd_opt(2023, 12, 31).unwrap());
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2024, 4, 1).unwrap()
        );
        assert_eq!(
            row.pre_date,
            Some(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_sync_fund_basic_writes_list_date() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let pool = sqlx::PgPool::connect(&url).await.expect("pool");
        let client = crate::tushare::client::TushareClient::from_env().expect("tushare");
        let n = sync_fund_basic(&pool, &client).await.expect("sync");
        assert!(n > 0, "应同步到 ETF");
        // 验证 518880 list_date 非 NULL
        let ld: Option<chrono::NaiveDate> = sqlx::query_scalar(
            "SELECT list_date FROM market_stock WHERE symbol = '518880.SH'",
        )
        .fetch_one(&pool)
        .await
        .expect("query");
        assert!(ld.is_some(), "518880.SH list_date 应被回填");
    }
}
