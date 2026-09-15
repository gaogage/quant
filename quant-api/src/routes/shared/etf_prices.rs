//! 盘中 ETF 实时价格模块（DDD Step 6b 从 scheduler.rs 迁出）。
//!
//! 包含：
//! - [`fetch_intraday_etf_prices`]：盘中调仓时获取 ETF 当日实时价格（Tushare fund_daily API）
//!
//! 原位置：scheduler.rs:157-246。

use quant_data::tushare::client::TushareClient;
use sqlx::PgPool;
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
            "SELECT close FROM market_stock_daily_bar WHERE symbol = $1 AND trade_date = $2",
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

    // 2. 对于 DB 中没有当日数据的 ETF, 尝试 Tushare fund_daily。
    // (历史: 此处曾先调 realtime_quote 盘中实时价, 但 Tushare 官方 HTTP API 无此接口
    //  —— 主备源均 40101"接口不存在", 该调用从未成功过, 2026-09-15 移除。
    //  实时价需 rt_k 接口(realtimeapi 网关, 另行申请开通)再接入。)
    let missing: Vec<&String> = etf_symbols
        .iter()
        .filter(|s| !prices.contains_key(*s))
        .collect();
    if !missing.is_empty() {
        for sym in missing {
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
                "SELECT close FROM market_stock_daily_bar WHERE symbol = $1 ORDER BY trade_date DESC LIMIT 1"
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
