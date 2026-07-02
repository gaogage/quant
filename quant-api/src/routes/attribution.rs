//! 持仓层面回撤归因:按行业/市值/风格分解收益贡献。
//! 蓝图 §86 组合归因闭环:补行业/风格暴露归因。

use std::collections::HashMap;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use axum::{extract::State, response::IntoResponse, Json};
use std::sync::Arc;

use crate::AppState;

/// 持仓 + 行业标签(归因基础数据)。
#[derive(Debug, Clone)]
pub struct PositionWithIndustry {
    pub symbol: String,
    pub quantity: Decimal,
    pub market_value: Decimal,
    pub weight: f64,
    pub industry: Option<String>,
}

/// 取某日持仓 + 行业标签(JOIN market_stock.industry)。
pub async fn fetch_positions_with_industry(
    db: &sqlx::PgPool,
    task_id: &str,
    date: NaiveDate,
) -> Result<Vec<PositionWithIndustry>, String> {
    let rows: Vec<(String, Decimal, Decimal, f64, Option<String>)> = sqlx::query_as(
        "SELECT bp.symbol, bp.quantity, bp.market_value, bp.weight::double precision, ms.industry
         FROM backtest_position bp
         LEFT JOIN market_stock ms ON bp.symbol = ms.symbol
         WHERE bp.task_id = $1 AND bp.position_date = $2 AND bp.quantity > 0",
    )
    .bind(task_id)
    .bind(date)
    .fetch_all(db)
    .await
    .map_err(|e| format!("fetch positions: {}", e))?;
    Ok(rows
        .into_iter()
        .map(|(symbol, quantity, market_value, weight, industry)| PositionWithIndustry {
            symbol, quantity, market_value, weight, industry,
        })
        .collect())
}

/// 取一组 symbol 在 date 当日(或之前最近)的总市值(亿元)。
pub async fn fetch_market_cap(
    db: &sqlx::PgPool,
    symbols: &[String],
    date: NaiveDate,
) -> Result<HashMap<String, f64>, String> {
    let rows: Vec<(String, f64)> = sqlx::query_as(
        "SELECT DISTINCT ON (symbol) symbol, total_mv::double precision / 1e8 AS mv_yi
         FROM market_stock_daily_basic
         WHERE symbol = ANY($1) AND trade_date <= $2 AND total_mv > 0
         ORDER BY symbol, trade_date DESC",
    )
    .bind(symbols)
    .bind(date)
    .fetch_all(db)
    .await
    .map_err(|e| format!("fetch market_cap: {}", e))?;
    Ok(rows.into_iter().collect())
}

/// 市值分桶:大盘(>500亿) / 中盘(>100亿) / 小盘(<=100亿)。
pub fn bucket_market_cap(mv_yi: f64) -> &'static str {
    if mv_yi > 500.0 { "large" } else if mv_yi > 100.0 { "mid" } else { "small" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_market_cap() {
        assert_eq!(bucket_market_cap(600.0), "large");
        assert_eq!(bucket_market_cap(500.0), "mid"); // 边界:500 不含
        assert_eq!(bucket_market_cap(300.0), "mid");
        assert_eq!(bucket_market_cap(100.0), "small"); // 边界:100 不含
        assert_eq!(bucket_market_cap(50.0), "small");
    }
}
