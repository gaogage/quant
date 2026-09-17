//! ETF 溢价门禁·方向感知版(2026-09-17 二期调整, 513100 溢价停牌事故)。
//!
//! 背景: QDII ETF(纳指/标普/原油)因外汇额度紧张出现高溢价(实测 13%+), 交易所
//! 允许基金公司开市起停牌至 10:30——与原执行窗口 09:35-10:30 结构性重叠, 且这种
//! "溢价临时停牌"不进 Tushare 停牌表(suspend_d 不覆盖), 常规停牌检查全部失效。
//! 实测 10:30 复牌后即有成交 → 执行窗口后移 10:35-11:30 避开停牌段, 门禁专注
//! 溢价本身的价格保护。
//!
//! 方向语义(2026-09-17 用户定版, 取代一期"整体 blocked 维持现状"):
//! - 溢价 > +threshold → **禁买可卖**: 正向高溢价买入会承受溢价回归损失;
//!   卖出反而占便宜(以高溢价价成交), 放行。
//! - 溢价 < -threshold → **禁卖可买**: 负向高折价卖出吃亏; 买入折价占便宜。
//! - |溢价| ≤ threshold → 正常双向调仓。
//! - 阈值为策略层配置(strategy_config.etf_premium_gate, DB 可调), 默认 0.10。
//!
//! 时序: T 日 23:30 信号生成时最近可得净值通常为 T-1(QDII T+1 上午公布),
//! 溢价含隔夜美股误差 ±2-3%, 默认阈值 10% 余量充足。
//!
//! 降级取向(刻意): 净值/收盘价缺失 → 放行不拦截(宁少拦不错拦)——数据管道故障
//! 不能放大为全部 ETF 调仓停摆。fresh_days 内的净值才参与计算, 陈旧净值同样放行。

use chrono::NaiveDate;
use sqlx::PgPool;
use std::collections::HashMap;

/// 默认溢价门禁阈值(|溢价| 超过此值触发单边阻断)。
/// 策略层配置: strategy_config.etf_premium_gate 列(每策略可独立调整)。
pub const DEFAULT_ETF_PREMIUM_GATE: f64 = 0.10;
/// 净值新鲜度上限(天): nav_date 距截面日超过此值视为数据陈旧, 门禁降级放行。
/// QDII 净值 T+1 公布, 正常滞后 1-3 天; 7 天覆盖长假场景。
pub const ETF_NAV_FRESH_DAYS: i64 = 7;

/// ETF 溢价快照(单标的, 方向感知)。
#[derive(Debug, Clone)]
pub struct EtfPremium {
    /// 溢价率(close/unit_nav - 1), 如 0.132 = 13.2%。None = 数据缺失(放行)。
    pub premium_pct: Option<f64>,
    /// 正向高溢价(> +gate): 禁买可卖。
    pub block_buy: bool,
    /// 负向高折价(< -gate): 禁卖可买。
    pub block_sell: bool,
    /// 参与计算的单位净值(None = 缺失)
    pub unit_nav: Option<f64>,
    /// 净值日期(审计用)
    pub nav_date: Option<NaiveDate>,
}

impl EtfPremium {
    /// 数据完备且超阈值才单边拦截; 数据缺失(premium_pct=None)恒放行。
    pub fn from_parts(premium_pct: Option<f64>, threshold: f64) -> Self {
        Self {
            block_buy: premium_pct.is_some_and(|v| v > threshold),
            block_sell: premium_pct.is_some_and(|v| v < -threshold),
            premium_pct,
            unit_nav: None,
            nav_date: None,
        }
    }

    /// 该 side(buy/sell)是否被阻断。
    pub fn blocks_side(&self, side: &str) -> bool {
        if side == "buy" {
            self.block_buy
        } else {
            self.block_sell
        }
    }

    /// 是否完全无阻断(数据缺失或区间内)。
    pub fn is_free(&self) -> bool {
        !self.block_buy && !self.block_sell
    }
}

/// 批量加载 ETF 溢价并按阈值判定单边门禁。
///
/// 溢价 = 截面日收盘价 / 最近可得单位净值 - 1。
/// 缺收盘价、缺净值、净值陈旧(> fresh_days) → 该标的放行(premium_pct=None)。
/// 返回 map 仅含数据完备的 ETF symbol; 未在 map 的标的视为放行。
pub async fn load_etf_premium_map(
    db: &PgPool,
    date: NaiveDate,
    etf_symbols: &[String],
    threshold: f64,
) -> HashMap<String, EtfPremium> {
    let mut map: HashMap<String, EtfPremium> = HashMap::new();
    if etf_symbols.is_empty() {
        return map;
    }
    // 最近净值(每标的一条, DISTINCT ON) + 截面日收盘价, 单次联查
    let rows: Vec<(String, Option<chrono::NaiveDate>, Option<f64>, Option<f64>)> =
        sqlx::query_as(
            r#"SELECT b.symbol,
                      n.nav_date,
                      n.unit_nav::float8,
                      b.close::float8
               FROM (SELECT symbol, close FROM market_stock_daily_bar
                     WHERE trade_date = $1 AND symbol = ANY($2)) b
               LEFT JOIN LATERAL (
                   -- PIT: 取 nav_date <= 截面日 的最近一条。条件不可省——否则回放
                   -- 历史日期时取到全局最新净值, 既穿越又不满足 7 天新鲜度被 WHERE
                   -- 过滤 → 门禁静默放行(2026-09-17 实测回放零触发, 实盘恰好正常)。
                   SELECT nav_date, unit_nav FROM market_fund_nav
                   WHERE symbol = b.symbol AND nav_date <= $1
                   ORDER BY nav_date DESC LIMIT 1
               ) n ON true
               WHERE n.unit_nav IS NOT NULL
                 AND n.unit_nav > 0
                 AND n.nav_date >= $1 - $3::int"#,
        )
        .bind(date)
        .bind(etf_symbols)
        .bind(ETF_NAV_FRESH_DAYS as i32)
        .fetch_all(db)
        .await
        .unwrap_or_default();

    for (sym, nav_date, unit_nav, close) in rows {
        let (Some(nav), Some(px)) = (unit_nav, close) else {
            continue;
        };
        if px <= 0.0 || nav <= 0.0 {
            continue;
        }
        let premium = px / nav - 1.0;
        let mut e = EtfPremium::from_parts(Some(premium), threshold);
        e.unit_nav = Some(nav);
        e.nav_date = nav_date;
        map.insert(sym, e);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_is_directional_only_with_complete_data() {
        // 正向高溢价: 禁买可卖
        let hi = EtfPremium::from_parts(Some(0.136), 0.10);
        assert!(hi.block_buy && !hi.block_sell);
        assert!(hi.blocks_side("buy") && !hi.blocks_side("sell"));
        // 负向高折价: 禁卖可买
        let lo = EtfPremium::from_parts(Some(-0.12), 0.10);
        assert!(lo.block_sell && !lo.block_buy);
        assert!(!lo.blocks_side("buy") && lo.blocks_side("sell"));
        // 合理区间: 双向放行(阈值边界不拦, 严格大于/小于)
        let mid = EtfPremium::from_parts(Some(0.10), 0.10);
        assert!(mid.is_free());
        let mid2 = EtfPremium::from_parts(Some(-0.10), 0.10);
        assert!(mid2.is_free());
        // 数据缺失恒放行(降级取向)
        let none = EtfPremium::from_parts(None, 0.10);
        assert!(none.is_free());
    }
}
