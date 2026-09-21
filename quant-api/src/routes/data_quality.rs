//! 数据质量校验模块（DDD R10b/Step 6c-4：从 scheduler 上帝模块迁出）。
//!
//! 原属 scheduler.rs 的 `run_data_quality_check`，职责是盘后扫描全量数据缺口
//! （A股/ETF/复权因子/ML预测/v24 因子覆盖率/策略依赖/任务依赖顺序）并告警。
//! 与调度编排无关，迁出后 scheduler 只管任务生命周期与 CRON。
//!
//! 依赖：shared::send_quality_alert + scheduler::{V24_FACTOR_CODES, check_task_dependency_order}
//! （单向引用，无循环依赖）。

use chrono::NaiveDate;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::routes::scheduler::{check_task_dependency_order, V24_FACTOR_CODES};
use crate::routes::shared::send_quality_alert;

/// 事件驱动因子 → 上游稀疏公告源表映射(P2-2b 健康指标用)。
/// 此类因子的覆盖率随披露季节脉冲波动, 不适用覆盖率骤降检测, 改查源表新鲜度。
/// (factor_code 前缀, 源表, 日期列, 允许最大滞后自然日)
const EVENT_DRIVEN_FACTOR_SOURCES: &[(&str, &str, &str, i64)] = &[
    ("forecast_", "market_stock_forecast", "ann_date", 60),
    ("repurchase_", "market_stock_repurchase", "ann_date", 90),
    ("block_trade_", "market_stock_block_trade", "trade_date", 14),
];

/// v24 因子滞缓判定的期望覆盖基准(任务71, 2026-09-21)。
/// 因子 T 日值由 22:10 夜间链回填: 检查跑在回填前还是后, 期望不同——
/// 22:01 EOD 尾(回填前)因子本就只应到 T-1, 用 T 基准恒定误报"7/14 滞缓";
/// 夜间链收尾(回填后)才应到 T。调用方按时点显式选择, 不做时刻魔法推断。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactorFreshnessBaseline {
    /// 因子应覆盖到最近已收盘交易日(夜间回填完成后口径)
    LatestTradeDate,
    /// 因子应覆盖到上一交易日(回填前时点口径, 如 EOD 尾 22:01)
    PreviousTradeDate,
}

impl FactorFreshnessBaseline {
    /// 解析期望覆盖日。日历残缺时退化为最近交易日(宁可漏报不误报)。
    async fn resolve(self, db: &PgPool, latest_td: NaiveDate) -> NaiveDate {
        match self {
            Self::LatestTradeDate => latest_td,
            Self::PreviousTradeDate => sqlx::query_as::<_, (NaiveDate,)>(
                "SELECT trade_date FROM market_trade_calendar
                     WHERE is_open = true AND trade_date < $1
                     ORDER BY trade_date DESC LIMIT 1",
            )
            .bind(latest_td)
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .map(|(d,)| d)
            .unwrap_or(latest_td),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::LatestTradeDate => "T日",
            Self::PreviousTradeDate => "T-1",
        }
    }
}

/// 盘后数据质量校验：扫描全量数据缺口并告警。
///
/// 检查项：
/// - A 股 / ETF 日线交易日 gap
/// - 复权因子 / ML 预测日历缺口
/// - v24 14 活跃因子滞后与覆盖率骤降（`factor_baseline` 按调用时点选基准）
/// - 活跃策略所需数据是否有自动同步任务
/// - 定时任务依赖顺序
pub async fn run_data_quality_check(db: &PgPool, factor_baseline: FactorFreshnessBaseline) {
    let today = chrono::Utc::now().date_naive();

    // 计算 A 股日线的交易日 gap
    let mut gaps: Vec<String> = Vec::new();
    let stock_max: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE symbol LIKE '6%'",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    if let Some((max_dt,)) = stock_max {
        let trading_days_behind: (i64,) = sqlx::query_as(
            "SELECT COUNT(DISTINCT trade_date) FROM market_trade_calendar WHERE exchange = 'SSE' AND is_open = true AND trade_date > $1 AND trade_date < $2"
        ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((0,));
        if trading_days_behind.0 > 1 {
            gaps.push(format!(
                "A股日线: 最新={}, 落后{}个交易日",
                max_dt, trading_days_behind.0
            ));
        }
    }

    // 检查 ETF 日线（按策略配置的 ETF 列表逐个查）
    let etf_symbols: Vec<String> = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT etf_symbols FROM strategy_config WHERE status = 'active' ORDER BY updated_at DESC LIMIT 1"
    ).fetch_optional(db).await.ok().flatten()
        .and_then(|(v,)| serde_json::from_value::<Vec<String>>(v).ok())
        .unwrap_or_default();

    if !etf_symbols.is_empty() {
        for symbol in &etf_symbols {
            let max_row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
                "SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE symbol = $1",
            )
            .bind(symbol)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
            if let Some((max_dt,)) = max_row {
                let trading_gap: (i64,) = sqlx::query_as(
                    "SELECT COUNT(DISTINCT trade_date) FROM market_trade_calendar WHERE exchange = 'SSE' AND is_open = true AND trade_date > $1 AND trade_date < $2"
                ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((0,));
                if trading_gap.0 > 1 {
                    // ETF T+1，允许落后1个交易日
                    gaps.push(format!(
                        "ETF {}: 最新={}, 落后{}个交易日",
                        symbol, max_dt, trading_gap.0
                    ));
                }
            } else {
                gaps.push(format!("ETF {}: 无数据", symbol));
            }
        }
    }

    // 其他检查项
    let other_checks: Vec<(&str, &str, i64)> = vec![
        ("复权因子", "market_adjustment_factor", 30),
        ("ML预测", "model_prediction", 5),
    ];
    for (name, table, max_calendar_gap) in &other_checks {
        let max_row: Option<(String,)> =
            sqlx::query_as(&format!("SELECT MAX(trade_date)::text FROM {}", table))
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
        if let Some((max_d,)) = max_row {
            if let (Ok(max_dt), Ok(today_dt)) = (
                NaiveDate::parse_from_str(&max_d, "%Y-%m-%d"),
                NaiveDate::parse_from_str(&today.format("%Y-%m-%d").to_string(), "%Y-%m-%d"),
            ) {
                let gap = (today_dt - max_dt).num_days();
                if gap > *max_calendar_gap {
                    gaps.push(format!("{}: 最新={}, 缺口={}天", name, max_d, gap));
                }
            }
        }
    }

    // ── v24 因子数据健康检查：14 活跃因子每日覆盖率 ──
    // 任何因子滞后(最新数据日落后 >1 交易日)都会导致 combo 打分静默降级。
    // 检查每个因子最新 trade_date 是否覆盖到最近交易日。
    {
        let latest_trade: Option<(chrono::NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE is_open = true AND trade_date <= $1
             ORDER BY trade_date DESC LIMIT 1",
        )
        .bind(today)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
        if let Some((latest_td,)) = latest_trade {
            let baseline_td = factor_baseline.resolve(db, latest_td).await;
            let mut stale_factors: Vec<String> = Vec::new();
            for code in V24_FACTOR_CODES {
                let max_row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
                    "SELECT MAX(trade_date) FROM factor_value
                     WHERE factor_code = $1 AND factor_version = '1.0.0'",
                )
                .bind(code)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
                match max_row {
                    Some((factor_max,)) if factor_max >= baseline_td => {}
                    Some((factor_max,)) => {
                        let gap = (baseline_td - factor_max).num_days();
                        stale_factors.push(format!("{}(最新{}天前)", code, gap));
                    }
                    None => {
                        stale_factors.push(format!("{}(无数据)", code));
                    }
                }
            }
            if !stale_factors.is_empty() {
                gaps.push(format!(
                    "v24因子滞缓({}/14, 基准{}): {}",
                    stale_factors.len(),
                    factor_baseline.label(),
                    stale_factors.join(", ")
                ));
            }

            // ── P2-2: v24 因子覆盖率骤降检测 ──
            // 部分因子(回购/大宗交易/研报评级等)天然只覆盖部分标的,不能拿"全市场"做分母，
            // 否则天天误报。改用"该因子自身近 30 日单日最大覆盖数"做基准——覆盖率 =
            // 最新日覆盖数 / 近期最大覆盖数，<80% 说明相对自身正常水平出现骤降(真实数据缺口)，
            // 而非因子设计上的稀疏性。
            //
            // 事件驱动因子豁免(2026-09-18): forecast/repurchase 族的数据来自稀疏公告源表,
            // 覆盖率随披露季节脉冲波动(业绩预告集中 1/4/7/10 月, 9 月真空期 + 120d 窗口
            // 出窗衰减属正常机制), "最新日/近30日最高"口径在季节转换期必然误报
            // (2026-09-17 实锤: forecast_type_upgrade_120d_std 8/21=38% 连续两周误报,
            // 而 Tushare 上游对账一致)。此类因子改用上游源表最新公告日期滞后天数做健康
            // 指标——公告断更才是真故障, 覆盖率季节波动是常态。
            let coverage_codes: Vec<&str> = V24_FACTOR_CODES
                .iter()
                .filter(|c| {
                    !EVENT_DRIVEN_FACTOR_SOURCES
                        .iter()
                        .any(|(prefix, _, _, _)| c.starts_with(prefix))
                })
                .copied()
                .collect();
            let window_start = latest_td - chrono::Duration::days(30);
            let coverage_rows: Vec<(String, chrono::NaiveDate, i64, i64)> = sqlx::query_as(
                "WITH per_day AS (
                    SELECT factor_code, trade_date, COUNT(DISTINCT symbol) AS day_cnt
                    FROM factor_value
                    WHERE factor_version = '1.0.0'
                      AND factor_code = ANY($1)
                      AND trade_date >= $2
                    GROUP BY factor_code, trade_date
                 ),
                 latest AS (
                    SELECT DISTINCT ON (factor_code) factor_code, trade_date, day_cnt
                    FROM per_day ORDER BY factor_code, trade_date DESC
                 )
                 SELECT l.factor_code, l.trade_date, l.day_cnt, MAX(p.day_cnt) AS recent_max
                 FROM latest l JOIN per_day p USING (factor_code)
                 GROUP BY l.factor_code, l.trade_date, l.day_cnt",
            )
            .bind(&coverage_codes)
            .bind(window_start)
            .fetch_all(db)
            .await
            .unwrap_or_default();

            let mut low_coverage: Vec<String> = Vec::new();
            for (code, factor_latest_dt, latest_cnt, recent_max) in &coverage_rows {
                if *recent_max <= 0 {
                    continue;
                }
                let ratio = *latest_cnt as f64 / *recent_max as f64;
                if ratio < 0.8 {
                    low_coverage.push(format!(
                        "{}({}覆盖{}/{}={:.0}%)",
                        code,
                        factor_latest_dt,
                        latest_cnt,
                        recent_max,
                        ratio * 100.0
                    ));
                }
            }
            if !low_coverage.is_empty() {
                gaps.push(format!(
                    "v24因子覆盖率骤降({}/14, 较近30日最高<80%): {}",
                    low_coverage.len(),
                    low_coverage.join(", ")
                ));
            }

            // ── P2-2b: 事件驱动因子上游源表新鲜度检测 ──
            // 对 forecast_/repurchase_/block_trade_ 族: 覆盖率季节波动是常态, 真正的
            // 健康指标是上游源表最新日期的滞后天数(公告断更=因子在吃旧快照)。
            // 阈值为自然日: forecast 60(最长真空 9月至10月中旬约 6 周),
            // repurchase 90(公告驱动无固定节奏), block_trade 14(日频同步)。
            let mut event_stale: Vec<String> = Vec::new();
            for (prefix, table, date_col, max_lag) in EVENT_DRIVEN_FACTOR_SOURCES {
                let matched: Vec<&str> = V24_FACTOR_CODES
                    .iter()
                    .filter(|c| c.starts_with(prefix))
                    .copied()
                    .collect();
                if matched.is_empty() {
                    continue;
                }
                let max_row: Option<(chrono::NaiveDate,)> =
                    sqlx::query_as(&format!("SELECT MAX({}) FROM {}", date_col, table))
                        .fetch_optional(db)
                        .await
                        .ok()
                        .flatten();
                match max_row {
                    Some((max_d,)) => {
                        let lag = (today - max_d).num_days();
                        if lag > *max_lag {
                            event_stale.push(format!(
                                "{}族[{}: 最新={}, 滞后{}天>{}]",
                                prefix.trim_end_matches('_'),
                                table,
                                max_d,
                                lag,
                                max_lag
                            ));
                        }
                    }
                    None => event_stale.push(format!(
                        "{}族[{}: 无数据]",
                        prefix.trim_end_matches('_'),
                        table
                    )),
                }
            }
            if !event_stale.is_empty() {
                gaps.push(format!(
                    "事件因子上游停更({}): {}",
                    event_stale.len(),
                    event_stale.join(", ")
                ));
            }
        }
    }

    // ── 策略依赖覆盖检查：验证所有活跃策略所需数据都有自动同步 ──
    {
        let signal_sources: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT signal_source, paper_account_id FROM paper_account WHERE status = 'active'"
        ).fetch_all(db).await.unwrap_or_default();
        for (signal_source, _account_id) in &signal_sources {
            let required: Vec<&str> = match signal_source.as_str() {
                "factor" => vec!["A股日线", "ETF日线", "因子(pv)", "CSI300"],
                "prediction" | "prediction_blend" => {
                    vec!["A股日线", "ETF日线", "因子(pv)", "CSI300", "ML预测"]
                }
                _ => vec!["A股日线", "ETF日线", "因子(pv)", "CSI300"],
            };
            for item in &required {
                let covered = match *item {
                    "A股日线" | "ETF日线" | "CSI300" | "停牌" | "涨跌停" | "复权因子" => {
                        true
                    } // scheduler 9:00/16:00 内置
                    "因子(pv)" => {
                        // 查 scheduled_task_config 确认 factor_backfill_daily 已配置且启用
                        // (T+1 9:00 run_tick 内联也会回填,但需有可查/可触发的任务保障)
                        let n: i64 = sqlx::query_scalar(
                            "SELECT COUNT(*) FROM scheduled_task_config
                             WHERE task_name='factor_backfill_daily' AND enabled=true",
                        )
                        .fetch_one(db)
                        .await
                        .unwrap_or(0);
                        n > 0
                    }
                    "ML预测" => true,   // scheduler 16:00 EOD (60天检查)
                    "权益曲线" => true, // equity_curve_monthly 任务
                    _ => false,
                };
                if !covered {
                    gaps.push(format!(
                        "策略依赖缺失: signal={} 需要 {} 但无自动同步任务",
                        signal_source, item
                    ));
                }
            }
        }
    }

    // ── 定时任务依赖顺序检查 ──
    {
        let deps = check_task_dependency_order(db).await;
        for d in &deps {
            gaps.push(format!("任务依赖顺序异常: {}", d));
        }
    }

    if !gaps.is_empty() {
        let msg = format!(
            "[数据质量] 发现 {} 个缺口(因子基准={}):\n{}",
            gaps.len(),
            factor_baseline.label(),
            gaps.join("\n")
        );
        warn!("{}", msg);
        send_quality_alert(db, &gaps).await;
    } else {
        info!(
            "[数据质量] 全部数据完整, 检查日期={}(因子基准={})",
            today,
            factor_baseline.label()
        );
    }

    let _ = sqlx::query(
        "INSERT INTO data_quality_config (config_key, config_value, description) VALUES ('last_quality_check', $1, '最后质量检查日期') ON CONFLICT (config_key) DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = NOW()"
    ).bind(today.format("%Y-%m-%d").to_string()).execute(db).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 基准枚举语义锁定(任务71): 两个变体分别对应"回填后/回填前"时点期望,
    /// label 用于告警文案自我说明。防止未来重构时含义漂移。
    #[test]
    fn factor_freshness_baseline_semantics() {
        assert_eq!(FactorFreshnessBaseline::LatestTradeDate.label(), "T日");
        assert_eq!(FactorFreshnessBaseline::PreviousTradeDate.label(), "T-1");
        // EOD 尾调用点必须传 T-1(回填前时点), 夜间链收尾必须传 T(回填后)——
        // 这是消除"7/14 滞缓"恒定误报的关键口径, 改动需评审两处调用方。
        assert_ne!(
            FactorFreshnessBaseline::LatestTradeDate,
            FactorFreshnessBaseline::PreviousTradeDate
        );
    }
}
