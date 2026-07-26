//! 盘中 ETF 实时价格模块（DDD Step 6b 从 scheduler.rs 迁出）。
//!
//! 包含：
//! - [`fetch_intraday_etf_prices`]：盘中调仓时获取 ETF 当日实时价格（Tushare fund_daily API）
//!
//! 原位置：scheduler.rs:157-246。

use sqlx::PgPool;
use quant_data::tushare::client::TushareClient;
use tracing::{info, warn};

/// 盘中调仓时获取 ETF 当日实时价格（通过 Tushare fund_daily API）
/// 若 Tushare 尚未有当日数据（T+1限制），回退到昨日收盘价。
pub(crate) async fn fetch_intraday_etf_prices(
    tushare: &TushareClient,
    etf_symbols: &[String],
    today: chrono::NaiveDate,
    db: &PgPool,
) -> std::collections::HashMap<String, f64> {
    use std::collections::HashMap;
    let mut prices = HashMap::new();

    // 1. 先尝试从 DB 获取当日数据（可能已被其他同步流程更新）
    for sym in etf_symbols {
        let row: Option<(Option<rust_decimal::Decimal>,)> = sqlx::query_as(
            "SELECT close FROM market_stock_daily_bar_adj WHERE symbol = $1 AND trade_date = $2",
        )
        .bind(sym)
        .bind(today)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        if let Some((Some(p),)) = row {
            prices.insert(sym.clone(), p.to_string().parse::<f64>().unwrap_or(0.0));
        }
    }

    // 2. 对于 DB 中没有当日数据的 ETF，通过 Tushare realtime_quote 获取盘中实时价格
    let missing: Vec<&String> = etf_symbols
        .iter()
        .filter(|s| !prices.contains_key(*s))
        .collect();
    if !missing.is_empty() {
        for sym in missing {
            // 盘中实时行情（PRO 用户可用）
            match tushare.realtime_quote(Some(sym)).await {
                Ok(resp) => {
                    if let Some(data) = resp.data {
                        let maps = data.to_maps();
                        if let Some(row) = maps.first() {
                            if let Some(price) = row.get("price").and_then(|v| v.as_f64()) {
                                if price > 0.0 {
                                    prices.insert(sym.clone(), price);
                                    info!("[intraday] {} Tushare实时价 {:.4}", sym, price);
                                    continue;
                                }
                            }
                        }
                    }
                }
                Err(ref e) => warn!("[intraday] {} realtime_quote失败: {}", sym, e),
            }
            // realtime_quote 失败时，尝试 fund_daily (T+1 数据)
            let today_str = today.format("%Y%m%d").to_string();
            match tushare
                .fund_daily(Some(sym), None, Some(&today_str), Some(&today_str))
                .await
            {
                Ok(resp) => {
                    if let Some(data) = resp.data {
                        let maps = data.to_maps();
                        if let Some(row) = maps.first() {
                            if let Some(close) = row.get("close").and_then(|v| v.as_f64()) {
                                if close > 0.0 {
                                    prices.insert(sym.clone(), close);
                                    info!("[intraday] {} fund_daily价 {:.4}", sym, close);
                                }
                            }
                        }
                    }
                }
                Err(ref e) => warn!("[intraday] {} fund_daily失败: {}", sym, e),
            }
        }
    }

    // 3. 仍未获取到的，回退到昨日收盘价
    for sym in etf_symbols {
        if !prices.contains_key(sym) {
            let row: Option<(Option<rust_decimal::Decimal>,)> = sqlx::query_as(
                "SELECT close FROM market_stock_daily_bar_adj WHERE symbol = $1 ORDER BY trade_date DESC LIMIT 1"
            ).bind(sym).fetch_optional(db).await.ok().flatten();
            if let Some((Some(p),)) = row {
                let price = p.to_string().parse::<f64>().unwrap_or(0.0);
                prices.insert(sym.clone(), price);
                info!("[intraday] {} 无当日数据，回退昨日收盘价 {:.4}", sym, price);
            }
        }
    }

    prices
}
