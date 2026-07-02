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
        "SELECT DISTINCT ON (symbol) symbol, total_mv::double precision / 1e4 AS mv_yi
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

/// 取 symbol 在 [start, end] 的区间收益(end 收盘 / start 收盘 - 1)。
/// 用 market_stock_daily_bar_adj.close,PIT 取 [start, end] 区间内已知收盘价。
async fn fetch_stock_period_return(
    db: &sqlx::PgPool,
    symbol: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<f64, String> {
    let prices: Vec<(NaiveDate, f64)> = sqlx::query_as(
        "SELECT trade_date, close::double precision FROM market_stock_daily_bar_adj
         WHERE symbol = $1 AND trade_date BETWEEN $2 AND $3 AND close > 0
         ORDER BY trade_date",
    )
    .bind(symbol)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .map_err(|e| format!("fetch return {}: {}", symbol, e))?;
    if prices.len() < 2 {
        return Ok(0.0);
    }
    let first = prices.first().unwrap().1;
    let last = prices.last().unwrap().1;
    if first > 0.0 { Ok(last / first - 1.0) } else { Ok(0.0) }
}

/// 单持仓的收益贡献归因。
#[derive(Debug, Clone)]
pub struct Contribution {
    pub symbol: String,
    pub industry: Option<String>,
    pub cap_bucket: String,
    pub weight_avg: f64,
    pub stock_return: f64,
    pub contribution: f64, // weight_avg * stock_return
}

/// 算各持仓的区间收益贡献:contribution = avg_weight × 个股区间收益。
/// weight 取 start 日持仓权重(简化:用首日权重代表区间,精确做法需逐日但成本高)。
pub async fn compute_return_contribution(
    db: &sqlx::PgPool,
    positions: &[PositionWithIndustry],
    market_caps: &HashMap<String, f64>,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<Contribution>, String> {
    let mut out = Vec::with_capacity(positions.len());
    for p in positions {
        let stock_return = fetch_stock_period_return(db, &p.symbol, start, end).await?;
        let mv = market_caps.get(&p.symbol).copied().unwrap_or(0.0);
        let cap_bucket = if mv > 0.0 { bucket_market_cap(mv).to_string() } else { "unknown".into() };
        out.push(Contribution {
            symbol: p.symbol.clone(),
            industry: p.industry.clone(),
            cap_bucket,
            weight_avg: p.weight,
            stock_return,
            contribution: p.weight * stock_return,
        });
    }
    Ok(out)
}

/// 维度归因聚合结果。
#[derive(Debug, Clone, serde::Serialize)]
pub struct DimensionAttribution {
    pub dimension: String,
    pub bucket: String,
    pub weight_avg: f64,
    pub contribution: f64,
    pub contribution_pct: f64,
    pub n_positions: usize,
}

/// 按维度(industry/cap_bucket)聚合贡献。
pub fn aggregate_by_dimension(contributions: &[Contribution], dimension: &str) -> Vec<DimensionAttribution> {
    let mut buckets: HashMap<String, (f64, f64, usize)> = HashMap::new();
    let mut total_contrib = 0.0_f64;
    for c in contributions {
        let key = match dimension {
            "industry" => c.industry.clone().unwrap_or_else(|| "unknown".into()),
            "market_cap" => c.cap_bucket.clone(),
            _ => continue,
        };
        let entry = buckets.entry(key).or_insert((0.0, 0.0, 0));
        entry.0 += c.weight_avg; // 权重累加
        entry.1 += c.contribution; // 贡献累加
        entry.2 += 1;
        total_contrib += c.contribution;
    }
    let mut out: Vec<DimensionAttribution> = buckets
        .into_iter()
        .map(|(bucket, (weight_sum, contrib, n))| DimensionAttribution {
            dimension: dimension.into(),
            bucket,
            weight_avg: if n > 0 { weight_sum / n as f64 } else { 0.0 },
            contribution: contrib,
            contribution_pct: if total_contrib.abs() > 1e-12 { contrib / total_contrib } else { 0.0 },
            n_positions: n,
        })
        .collect();
    out.sort_by(|a, b| a.contribution.partial_cmp(&b.contribution).unwrap_or(std::cmp::Ordering::Equal));
    out
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

    #[test]
    fn test_aggregate_by_industry() {
        let contribs = vec![
            Contribution { symbol: "A".into(), industry: Some("银行".into()), cap_bucket: "large".into(), weight_avg: 0.1, stock_return: 0.2, contribution: 0.02 },
            Contribution { symbol: "B".into(), industry: Some("银行".into()), cap_bucket: "large".into(), weight_avg: 0.1, stock_return: -0.1, contribution: -0.01 },
            Contribution { symbol: "C".into(), industry: Some("地产".into()), cap_bucket: "mid".into(), weight_avg: 0.05, stock_return: 0.4, contribution: 0.02 },
        ];
        let agg = aggregate_by_dimension(&contribs, "industry");
        // 银行: 0.02 + (-0.01) = 0.01;地产: 0.02;总 0.03
        let bank = agg.iter().find(|a| a.bucket == "银行").unwrap();
        assert!((bank.contribution - 0.01).abs() < 1e-9);
        assert_eq!(bank.n_positions, 2);
        let realestate = agg.iter().find(|a| a.bucket == "地产").unwrap();
        assert!((realestate.contribution - 0.02).abs() < 1e-9);
    }

    #[test]
    fn test_aggregate_by_market_cap() {
        let contribs = vec![
            Contribution { symbol: "A".into(), industry: None, cap_bucket: "large".into(), weight_avg: 0.2, stock_return: 0.1, contribution: 0.02 },
            Contribution { symbol: "B".into(), industry: None, cap_bucket: "small".into(), weight_avg: 0.1, stock_return: 0.3, contribution: 0.03 },
        ];
        let agg = aggregate_by_dimension(&contribs, "market_cap");
        assert_eq!(agg.len(), 2);
        let small = agg.iter().find(|a| a.bucket == "small").unwrap();
        assert!((small.contribution - 0.03).abs() < 1e-9);
    }
}
