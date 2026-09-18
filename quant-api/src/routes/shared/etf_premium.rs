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

/// (2026-09-18: 删除未引用的 DEFAULT_ETF_PREMIUM_GATE——阈值已收敛到
/// strategy_config.etf_premium_gate 列, 信号导出/调仓均读策略配置, 常量无消费方;
/// 新策略未配置该列时的 DB 默认值即 0.10。)
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
    let rows = sqlx::query_as::<_, (String, Option<chrono::NaiveDate>, Option<f64>, Option<f64>)>(
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
        .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            // 勿静默吞错(2026-09-17 实测疑点: 执行错误被 unwrap_or_default 吞掉时
            // map 恒空 → 门禁形同虚设; 降级放行指"数据缺失", 不包括"查询故障")
            tracing::error!("[etf_premium] 溢价查询失败(门禁降级放行): {}", e);
            return map;
        }
    };

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

/// ETF 溢价存量退出 overlay(2026-09-18 回测定版: 6.8 年回测全指标最优档)。
///
/// 规则(滞回两档, 状态由持仓本身承载——无需额外存储, 信号端/模拟盘天然一致):
/// - 持有 + 溢价 > gate      → 目标置 0(清仓: 高溢价持有负期望, 501018 溢价>10%
///   的 183 个持有日次日期望 -0.35%/中位 -0.32%; 全周期 Sharpe 1.266→1.370,
///   MaxDD -15.4%→-13.6%, 代价=常态溢价年约 -6pp 保费)
/// - 未持有 + 溢价 >= gate/2 → 保持 0(滞回中间态不买回, 防止 5~10% 区间反复进出)
/// - 未持有 + 溢价 < gate/2  → 恢复正常目标
/// - 折价方向豁免: 深折价持有为正期望(等净值回归, 全历史折价<-5% 仅 17 天且次日
///   期望 +0.4%~+4.0%), 禁卖门禁(blocked_side=sell)已保护, 不做退出。
/// - 溢价数据缺失(premium=None/不在 map): 放行(降级取向, 与门禁一致)。
///
/// 返回调整后的 allocations; 触发退出的标的以 warn 日志留痕(回放审计)。
pub fn apply_premium_exit_overlay(
    allocations: &[(String, f64)],
    premium_map: &HashMap<String, EtfPremium>,
    holding: &std::collections::HashSet<String>,
    gate: f64,
) -> Vec<(String, f64)> {
    allocations
        .iter()
        .map(|(sym, w)| {
            let adjusted = premium_map
                .get(sym)
                .and_then(|p| p.premium_pct)
                .and_then(|prem| {
                    if holding.contains(sym) {
                        // 持有: 仅高溢价退出(折价与中间态不动)
                        (prem > gate).then_some(0.0)
                    } else if prem >= gate / 2.0 {
                        // 滞回: 中间态/高溢价不买回
                        Some(0.0)
                    } else {
                        None // 溢价已回落: 恢复
                    }
                });
            match adjusted {
                Some(0.0) => {
                    let prem = premium_map[sym].premium_pct.unwrap_or(0.0);
                    tracing::warn!(
                        "[溢价退出] {} 目标置 0(溢价 {:+.1}%, gate {:.0}%, {})",
                        sym,
                        prem * 100.0,
                        gate * 100.0,
                        if holding.contains(sym) { "持有清仓" } else { "滞回不买回" }
                    );
                    (sym.clone(), 0.0)
                }
                _ => (sym.clone(), *w),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prem_map(entries: &[(&str, f64)]) -> HashMap<String, EtfPremium> {
        entries
            .iter()
            .map(|(s, p)| (s.to_string(), EtfPremium::from_parts(Some(*p), 0.10)))
            .collect()
    }

    fn holding(syms: &[&str]) -> std::collections::HashSet<String> {
        syms.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn premium_exit_overlay_full_ruleset() {
        let allocs = vec![
            ("513100.SH".to_string(), 0.12),
            ("501018.SH".to_string(), 0.12),
            ("518880.SH".to_string(), 0.12),
            ("511010.SH".to_string(), 0.12),
        ];
        let pm = prem_map(&[
            ("513100.SH", 0.26),  // 高溢价
            ("501018.SH", 0.07),  // 滞回中间态(gate/2 ~ gate)
            ("518880.SH", -0.12), // 深折价
        ]);
        // 511010 不在 map = 数据缺失
        // 持有态: 513100 清仓; 501018 中间态持有不动; 518880 折价豁免; 511010 放行
        let hold = holding(&["513100.SH", "501018.SH", "518880.SH", "511010.SH"]);
        let out = apply_premium_exit_overlay(&allocs, &pm, &hold, 0.10);
        assert_eq!(out[0].1, 0.0, "持有+高溢价26%→清仓");
        assert_eq!(out[1].1, 0.12, "持有+中间态7%→不动(滞回只管买回)");
        assert_eq!(out[2].1, 0.12, "持有+深折价→豁免(正期望)");
        assert_eq!(out[3].1, 0.12, "数据缺失→放行");

        // 未持有态: 513100/501018 都不买回; 518880 折价正常配置(禁卖门禁另有保护)
        let empty = holding(&[]);
        let out2 = apply_premium_exit_overlay(&allocs, &pm, &empty, 0.10);
        assert_eq!(out2[0].1, 0.0, "未持有+高溢价→不买回");
        assert_eq!(out2[1].1, 0.0, "未持有+中间态→滞回不买回");

        // 恢复: 溢价回落 < gate/2(5%) 后未持有标的恢复配置
        let pm2 = prem_map(&[("513100.SH", 0.04)]);
        let out3 = apply_premium_exit_overlay(&allocs, &pm2, &empty, 0.10);
        assert_eq!(out3[0].1, 0.12, "溢价回落 4% < 5% → 恢复");
    }

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
