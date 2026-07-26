//! 交易阻断模块（DDD Step 6b 从 scheduler.rs 迁出）。
//!
//! 包含：
//! - [`TradeBlock`]：交易阻断状态（含涨跌停方向）
//! - [`preload_trade_block_map`]：批量预加载当日所有 A 股停牌/涨跌停状态
//! - 附属私有：[`is_a_share_symbol`]
//!
//! 原位置：scheduler.rs:418-426 / 2440-2513。

use chrono::NaiveDate;
use sqlx::PgPool;

/// 交易阻断状态(含涨跌停方向)。
///
/// A股规则:涨停封板无对手盘→禁买(可卖);跌停封板无对手盘→禁卖(可买);停牌→买卖都禁。
/// 调用方按 side 区分:sell 时遇 limit_type='D' 跳过,buy 时遇 'U' 跳过,停牌都跳过。
#[derive(Debug, Clone)]
pub struct TradeBlock {
    /// 阻断原因("停牌"/"涨跌停")
    pub reason: String,
    /// 涨跌停方向:'U'涨停/'D'跌停/None(停牌或 limit_type 未回填)。
    /// 停牌时为 None;涨跌停但 limit_type NULL 时也为 None(按"无方向"处理,买卖都阻断保守)。
    pub limit_type: Option<char>,
}

impl TradeBlock {
    /// 该 symbol 在指定 side(buy/sell)下是否应被阻断。
    /// - 停牌(reason="停牌"):买卖都阻断
    /// - 涨停('U'):只阻断 buy(可卖)
    /// - 跌停('D'):只阻断 sell(可买)
    /// - 涨跌停但方向 NULL:买卖都阻断(保守,因 limit_type 未回填无法判断方向)
    pub fn blocks_side(&self, side: &str) -> bool {
        if self.reason == "停牌" {
            return true;
        }
        match self.limit_type {
            Some('U') => side == "buy",
            Some('D') => side == "sell",
            _ => true, // 方向未知,保守阻断买卖
        }
    }
}

fn is_a_share_symbol(symbol: &str) -> bool {
    let Some((code, suffix)) = symbol.split_once('.') else {
        return false;
    };
    if !matches!(suffix, "SH" | "SZ") || code.len() != 6 {
        return false;
    }
    matches!(code.as_bytes().first(), Some(b'0' | b'3' | b'6'))
}

/// 批量预加载当日所有 A 股的停牌/涨跌停阻断状态(mvo_simulate P2-A 性能优化)。
///
/// 替代逐股调 a_share_trade_block_reason(每股 2 次 DB)。一次性 UNION ALL 查停牌 + 涨跌停,
/// 返回 `HashMap<symbol, TradeBlock>`(含涨跌停方向 limit_type,供调用方按 buy/sell 区分)。
/// 未在 map 中的 symbol 视为可交易。
///
/// 停牌优先于涨跌停(UNION ALL 顺序 + entry().or_insert_with 保证停牌先入,涨跌停不覆盖)。
pub async fn preload_trade_block_map(
    db: &PgPool,
    trade_date: NaiveDate,
    symbols: &[String],
) -> std::collections::HashMap<String, TradeBlock> {
    if symbols.is_empty() {
        return std::collections::HashMap::new();
    }
    // 只查 A 股(非 A 股 symbol 如 ETF 不在此表,直接跳过)
    let a_shares: Vec<&str> = symbols
        .iter()
        .filter(|s| is_a_share_symbol(s))
        .map(|s| s.as_str())
        .collect();
    if a_shares.is_empty() {
        return std::collections::HashMap::new();
    }
    // reason + limit_type:停牌 NULL,涨跌停带 U/D(可能 NULL,未回填方向)
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT s.symbol, '停牌' AS reason, NULL::text AS limit_type FROM market_stock_suspension s
          WHERE s.trade_date = $1 AND s.symbol = ANY($2) AND COALESCE(s.suspend_type, 'S') = 'S'
         UNION ALL
         SELECT l.symbol, '涨跌停', l.limit_type::text FROM market_stock_limit l
          WHERE l.trade_date = $1 AND l.symbol = ANY($2)",
    )
    .bind(trade_date)
    .bind(&a_shares)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut map: std::collections::HashMap<String, TradeBlock> =
        std::collections::HashMap::new();
    for (sym, reason, lt) in rows {
        // 停牌在 UNION ALL 前置,先 insert;涨跌停后置,用 or_insert 不覆盖(停牌优先)
        map.entry(sym).or_insert(TradeBlock {
            reason,
            limit_type: lt.and_then(|s| s.chars().next()),
        });
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_share_event_gate_only_targets_main_a_share_symbols() {
        assert!(is_a_share_symbol("000001.SZ"));
        assert!(is_a_share_symbol("600000.SH"));
        assert!(is_a_share_symbol("300750.SZ"));
        assert!(!is_a_share_symbol("518880.SH"));
        assert!(!is_a_share_symbol("513500.SH"));
        assert!(!is_a_share_symbol("AAPL.US"));
    }
}
