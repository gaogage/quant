//! 数据同步服务 — Tushare → 标准化 → PostgreSQL

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::model::entities::{MarketStock, MarketStockDailyBar, MarketTradeCalendar};
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
    let total = symbols.len();
    let mut ok = 0usize;
    let mut fail = 0usize;

    for sym in symbols {
        match client.daily(sym, Some(start), Some(end)).await {
            Ok(resp) => {
                if let Some(data) = resp.data {
                    let maps = data.to_maps();
                    let bars: Vec<MarketStockDailyBar> = maps.iter().filter_map(|item| {
                        Some(MarketStockDailyBar {
                            symbol: sym.clone(),
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
                        repository::upsert_daily_bars_batch(pool, &bars, dv_id, "tushare").await?;
                        ok += 1;
                    }
                }
                if ok % 200 == 0 { info!("日线进度: {}/{}", ok, total); }
            }
            Err(e) => { warn!("{} 日线失败: {}", sym, e); fail += 1; }
        }
    }

    repository::update_sync_task(pool, &task_id, if fail>0{"partial"}else{"completed"}, total as i32, ok as i32, fail as i32).await?;
    info!("日线同步完成: ok={}/{}, fail={}", ok, total, fail);
    Ok(ok)
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
