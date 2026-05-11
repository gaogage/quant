//! 数据同步服务 — Tushare → 标准化 → PostgreSQL

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::model::entities::{MarketStock, MarketStockDailyBar, MarketTradeCalendar, MarketAdjustmentFactor};
use crate::repository;
use crate::tushare::client::TushareClient;

// ─── JSON helpers ────────────────────────────────────────────────
use serde_json::Map;

fn get_str(m: &Map<String, Value>, key: &str) -> String {
    m.get(key).and_then(|v| v.as_str()).unwrap_or("").to_string()
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
    v.and_then(|v| Decimal::from_f64_retain(v)).unwrap_or_default()
}
fn to_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y%m%d").ok()
}

// ─── sync_stock_basic ────────────────────────────────────────────

pub async fn sync_stock_basic(
    pool: &PgPool,
    client: &TushareClient,
    _data_version_id: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = Uuid::new_v4().to_string();
    repository::create_sync_task(pool, &task_id, "stock_basic", "running").await?;
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
                                if s.is_empty() { "L".to_string() } else { s }
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
                repository::update_sync_task(pool, &task_id, "failed", all.len() as i32, all.len() as i32, 1).await?;
                return Err(e.into());
            }
        }
    }

    let total = all.len();
    info!("共拉取 {} 只股票", total);
    let count = repository::upsert_stocks_batch(pool, &all).await?;
    repository::update_sync_task(pool, &task_id, "completed", total as i32, count as i32, 0).await?;
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
    let task_id = Uuid::new_v4().to_string();
    repository::create_sync_task(pool, &task_id, "daily", "running").await?;

    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_data_version(
        pool, dv_id, "daily bars sync", "tushare",
        &["market_stock_daily_bar"], s, e,
    ).await?;

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
                    match client.daily_batch(&chunk_vec, Some(&sd), Some(&ed), Some(page_limit), Some(offset)).await {
                        Ok(resp) => {
                            if let Some(data) = resp.data {
                                let maps = data.to_maps();
                                row_count = maps.len();

                                if row_count > 0 {
                                    let bars: Vec<MarketStockDailyBar> = maps.iter().filter_map(|item| {
                                        let ts_code = get_str(item, "ts_code");
                                        let symbol = ts_code.to_string();
                                        Some(MarketStockDailyBar {
                                            symbol,
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
                                    }).collect();

                                    if !bars.is_empty() {
                                        total_rows += bars.len();
                                        repository::upsert_daily_bars_batch(pool, &bars, dv_id, "tushare").await?;
                                        for s in chunk { synced.insert(s.clone()); }
                                    }
                                }
                            }
                            break; // page succeeded
                        }
                        Err(e) => {
                            retries += 1;
                            if retries > 3 {
                                warn!("Batch daily failed after 3 retries for chunk offset {}: {}", offset, e);
                                for s in chunk { failed.insert(s.clone()); }
                            } else {
                                let delay = std::time::Duration::from_millis(2000 * 2u64.pow(retries as u32 - 1));
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
        if total_rows % 50000 == 0 { info!("Daily sync: {} rows", total_rows); }
    }

    // Remove failed symbols from synced set
    for s in &failed { synced.remove(s); }
    let ok = synced.len() as i32;
    let fail = failed.len() as i32;

    repository::update_sync_task(pool, &task_id, if fail>0{"partial"}else{"completed"}, total as i32, ok, fail).await?;
    info!("Daily sync done: {} rows, ok={}/{}, fail={}", total_rows, ok, total, fail);
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
        let month_end = NaiveDate::from_ymd_opt(
            year, month,
            days_in_month(year, month)
        ).unwrap_or(cursor);
        let actual_end = month_end.min(end);
        result.push((cursor, actual_end));
        // Move to first day of next month
        cursor = NaiveDate::from_ymd_opt(
            if month == 12 { year + 1 } else { year },
            if month == 12 { 1 } else { month + 1 },
            1
        ).unwrap_or(end + chrono::Duration::days(1));
    }
    result
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1|3|5|7|8|10|12 => 31,
        4|6|9|11 => 30,
        2 => if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 29 } else { 28 },
        _ => 30,
    }
}

// ─── sync_trade_calendar ─────────────────────────────────────────

pub async fn sync_trade_calendar(
    pool: &PgPool,
    client: &TushareClient,
    exchange: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let task_id = Uuid::new_v4().to_string();
    repository::create_sync_task(pool, &task_id, "trade_cal", "running").await?;

    let resp = client.trade_cal(exchange, None, None).await?;
    let mut cals = Vec::new();

    if let Some(data) = resp.data {
        let maps = data.to_maps();
        for item in &maps {
            cals.push(MarketTradeCalendar {
                exchange: exchange.to_string(),
                trade_date: to_date(&get_str(item, "cal_date"))
                    .unwrap_or(NaiveDate::from_ymd_opt(2000,1,1).unwrap()),
                is_open: get_i64(item, "is_open").map(|v| v == 1).unwrap_or(false),
                pre_trade_date: {
                    let s = get_str(item, "pretrade_date");
                    if s.is_empty() { None } else { to_date(&s) }
                },
            });
        }
    }

    let count = repository::bulk_upsert_calendars(pool, &cals).await?;
    repository::update_sync_task(pool, &task_id, "completed", count as i32, count as i32, 0).await?;
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
    let task_id = Uuid::new_v4().to_string();
    repository::create_sync_task(pool, &task_id, "adj_factor", "running").await?;

    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_data_version(
        pool, dv_id, "adj factor sync", "tushare",
        &["market_adjustment_factor"], s, e,
    ).await?;

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
                        repository::upsert_adj_factors_batch(
                            pool, &factors, dv_id, "tushare",
                        ).await?;
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
        pool, &task_id,
        if fail > 0 { "partial" } else { "completed" },
        total as i32, ok as i32, fail as i32,
    ).await?;
    info!("复权因子同步完成: ok={}/{}, fail={}", ok, total, fail);
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
    let task_id = Uuid::new_v4().to_string();
    repository::create_sync_task(pool, &task_id, "index_daily", "running").await?;

    let s = NaiveDate::parse_from_str(start, "%Y%m%d")?;
    let e = NaiveDate::parse_from_str(end, "%Y%m%d")?;
    repository::create_data_version(
        pool, dv_id, "index daily sync", "tushare",
        &["market_index_daily_bar"], s, e,
    ).await?;

    let total = index_codes.len();
    let mut ok = 0usize;
    let mut fail = 0usize;

    for idx in index_codes {
        match client.index_daily(idx, Some(start), Some(end)).await {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    let bars: Vec<MarketStockDailyBar> = maps
                        .iter()
                        .filter_map(|item| {
                            Some(MarketStockDailyBar {
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
                        repository::upsert_daily_bars_batch(
                            pool, &bars, dv_id, "tushare",
                        ).await?;
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
        pool, &task_id,
        if fail > 0 { "partial" } else { "completed" },
        total as i32, ok as i32, fail as i32,
    ).await?;
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
    repository::create_sync_task(pool, &task_id, "financial", "running").await?;

    let mut stmt_count = 0usize;
    let mut ind_count = 0usize;
    let total = symbols.len();

    for (i, sym) in symbols.iter().enumerate() {
        if i % 100 == 0 {
            info!("财务数据同步: {}/{} ({} {} {})", i, total, stmt_count, ind_count, sym);
        }

        // 利润表
        if let Ok(resp) = client.income(sym, None, None).await {
            if let Some(data) = resp.data {
                let maps = data.to_maps();
                for item in &maps {
                    let end_date = to_date(&get_str(item, "end_date"));
                    let ann_date = to_date(&get_str(item, "ann_date"));
                    if end_date.is_none() { continue; }
                    let ed = end_date.unwrap();
                    let ad = ann_date.unwrap_or(ed);

                    for (field, val) in item.iter() {
                        if field == "ts_code" || field == "end_date" || field == "ann_date"
                            || field == "f_ann_date" || field == "report_type" || field == "comp_type"
                        { continue; }
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

        // 资产负债表 — 只取关键字段减少数据量
        if let Ok(resp) = client.balancesheet(sym, None, None).await {
            if let Some(data) = resp.data {
                let maps = data.to_maps();
                const BS_FIELDS: &[&str] = &[
                    "total_assets", "total_liab", "total_hldr_eqy_inc_min_int",
                    "total_cur_assets", "total_cur_liab", "money_cap",
                    "inventories", "accounts_receiv", "fix_assets",
                ];
                for item in &maps {
                    let end_date = to_date(&get_str(item, "end_date"));
                    let ann_date = to_date(&get_str(item, "ann_date"));
                    if end_date.is_none() { continue; }
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

        // 财务指标
        if let Ok(resp) = client.fina_indicator(sym, None, None).await {
            if let Some(data) = resp.data {
                let maps = data.to_maps();
                for item in &maps {
                    let end_date = to_date(&get_str(item, "end_date"));
                    let ann_date = to_date(&get_str(item, "ann_date"));
                    if end_date.is_none() { continue; }
                    let ed = end_date.unwrap();
                    let ad = ann_date.unwrap_or(ed);

                    sqlx::query(
                        "INSERT INTO market_financial_indicator (ts_code, ann_date, end_date,
                         eps, roe, roa, gross_margin, netprofit_margin, debt_to_assets, current_ratio)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                         ON CONFLICT (ts_code, end_date) DO UPDATE SET
                         eps=$4, roe=$5, roa=$6, gross_margin=$7, netprofit_margin=$8, debt_to_assets=$9, current_ratio=$10",
                    )
                    .bind(sym).bind(ad).bind(ed)
                    .bind(item.get("eps").and_then(|v| v.as_f64()))
                    .bind(item.get("roe").and_then(|v| v.as_f64()))
                    .bind(item.get("roa").and_then(|v| v.as_f64()))
                    .bind(item.get("gross_margin").and_then(|v| v.as_f64()))
                    .bind(item.get("netprofit_margin").and_then(|v| v.as_f64()))
                    .bind(item.get("debt_to_assets").and_then(|v| v.as_f64()))
                    .bind(item.get("current_ratio").and_then(|v| v.as_f64()))
                    .execute(pool).await?;
                    ind_count += 1;
                }
            }
        }

        // 速率控制 — 每只股票 ~0.3s，避免被限流
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    repository::update_sync_task(pool, &task_id, "completed", (stmt_count + ind_count) as i32, (stmt_count + ind_count) as i32, 0).await?;
    info!("财务数据同步完成: {} statements + {} indicators", stmt_count, ind_count);
    Ok((stmt_count, ind_count))
}
