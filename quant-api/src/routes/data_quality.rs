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

use crate::routes::shared::send_quality_alert;
use crate::routes::scheduler::{V24_FACTOR_CODES, check_task_dependency_order};

/// 盘后数据质量校验：扫描全量数据缺口并告警。
///
/// 检查项：
/// - A 股 / ETF 日线交易日 gap
/// - 复权因子 / ML 预测日历缺口
/// - v24 14 活跃因子滞后与覆盖率骤降
/// - 活跃策略所需数据是否有自动同步任务
/// - 定时任务依赖顺序
pub async fn run_data_quality_check(db: &PgPool) {
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
            "SELECT COUNT(*) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
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
                    "SELECT COUNT(*) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
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
                    Some((factor_max,)) if factor_max >= latest_td => {}
                    Some((factor_max,)) => {
                        let gap = (latest_td - factor_max).num_days();
                        stale_factors.push(format!("{}(最新{}天前)", code, gap));
                    }
                    None => {
                        stale_factors.push(format!("{}(无数据)", code));
                    }
                }
            }
            if !stale_factors.is_empty() {
                gaps.push(format!(
                    "v24因子滞缓({}/14): {}",
                    stale_factors.len(),
                    stale_factors.join(", ")
                ));
            }

            // ── P2-2: v24 因子覆盖率骤降检测 ──
            // 部分因子(回购/大宗交易/研报评级等)天然只覆盖部分标的,不能拿"全市场"做分母，
            // 否则天天误报。改用"该因子自身近 30 日单日最大覆盖数"做基准——覆盖率 =
            // 最新日覆盖数 / 近期最大覆盖数，<80% 说明相对自身正常水平出现骤降(真实数据缺口)，
            // 而非因子设计上的稀疏性。
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
            .bind(V24_FACTOR_CODES)
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
            "[数据质量] 发现 {} 个缺口:\n{}",
            gaps.len(),
            gaps.join("\n")
        );
        warn!("{}", msg);
        send_quality_alert(db, &gaps).await;
    } else {
        info!("[数据质量] 全部数据完整, 检查日期={}", today);
    }

    let _ = sqlx::query(
        "INSERT INTO data_quality_config (config_key, config_value, description) VALUES ('last_quality_check', $1, '最后质量检查日期') ON CONFLICT (config_key) DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = NOW()"
    ).bind(today.format("%Y-%m-%d").to_string()).execute(db).await;
}
