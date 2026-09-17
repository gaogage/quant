//! ETF 溢价门禁(2026-09-17 一期, 513100 溢价停牌事故)。
//!
//! 背景: QDII ETF(纳指/标普/原油)因外汇额度紧张出现高溢价(实测 13%+), 交易所
//! 允许基金公司开市起停牌至 10:30——与执行窗口 09:35-10:30 结构性重叠, 且这种
//! "溢价临时停牌"不进 Tushare 停牌表(suspend_d 不覆盖), 常规停牌检查全部失效。
//!
//! 主判据用溢价率而非停牌状态:
//! 1. 溢价是停牌的原因与同步指标(公告/停牌数据滞后, 溢价每日可得);
//! 2. 溢价本身直接危害买入(13% 溢价买入 = 溢价回归时亏损), 无论次日停不停牌,
//!    超阈值都不该按市价买。
//!
//! 时序: T 日 23:30 信号生成时最近可得净值通常为 T-1(QDII T+1 上午公布),
//! 溢价含隔夜美股误差 ±2-3%, 默认阈值 5% 已含余量; 精确保护由执行端
//! 快照防御与二期限价挂单承担。
//!
//! 降级取向(刻意): 净值/收盘价缺失 → 放行不拦截(宁少拦不错拦)——数据管道故障
//! 不能放大为全部 ETF 调仓停摆。fresh_days 内的净值才参与计算, 陈旧净值同样放行。

use chrono::NaiveDate;
use sqlx::PgPool;
use std::collections::HashMap;

/// 默认溢价门禁阈值(信号侧可被 scheduled_task_config params.premium_gate_pct 覆盖)。
pub const DEFAULT_ETF_PREMIUM_GATE: f64 = 0.05;
/// 净值新鲜度上限(天): nav_date 距截面日超过此值视为数据陈旧, 门禁降级放行。
/// QDII 净值 T+1 公布, 正常滞后 1-3 天; 7 天覆盖长假场景。
pub const ETF_NAV_FRESH_DAYS: i64 = 7;

/// ETF 溢价快照(单标的)。
#[derive(Debug, Clone)]
pub struct EtfPremium {
    /// 溢价率(close/unit_nav - 1), 如 0.132 = 13.2%。None = 数据缺失(放行)。
    pub premium_pct: Option<f64>,
    /// 门禁判定结果: true = 维持现状(跳过调仓/信号标记 blocked)。
    pub blocked: bool,
    /// 参与计算的单位净值(None = 缺失)
    pub unit_nav: Option<f64>,
    /// 净值日期(审计用)
    pub nav_date: Option<NaiveDate>,
}

impl EtfPremium {
    /// 数据完备且超阈值才拦截; 数据缺失(premium_pct=None)恒放行。
    pub fn from_parts(premium_pct: Option<f64>, threshold: f64) -> Self {
        Self {
            blocked: premium_pct.map_or(false, |p| p > threshold),
            premium_pct,
            unit_nav: None,
            nav_date: None,
        }
    }
}

/// 批量加载 ETF 溢价并按阈值判定门禁。
///
/// 溢价 = 截面日收盘价 / 最近可得单位净值 - 1。
/// 缺收盘价、缺净值、净值陈旧(> fresh_days) → 该标的放行(premium_pct=None)。
/// 返回 map 仅含 ETF symbol; 未在 map 的标的视为放行。
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
                   SELECT nav_date, unit_nav FROM market_fund_nav
                   WHERE symbol = b.symbol
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
    fn gate_blocks_only_high_premium_with_complete_data() {
        // 高溢价拦截
        assert!(EtfPremium::from_parts(Some(0.132), 0.05).blocked);
        // 正常溢价放行
        assert!(!EtfPremium::from_parts(Some(0.008), 0.05).blocked);
        // 阈值边界: 等于阈值不拦(>)
        assert!(!EtfPremium::from_parts(Some(0.05), 0.05).blocked);
        // 数据缺失恒放行(降级取向)
        assert!(!EtfPremium::from_parts(None, 0.05).blocked);
    }
}
