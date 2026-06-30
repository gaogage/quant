//! 内置调度器 — v15 日频量化交易。
//!
//! 14:45 (收盘前): 获取当日行情 → 回测 → MVO → 调仓 → 立即推送钉钉
//! 16:00 (收盘后): 同步日终行情数据到历史表 → 清理过期回测数据
//!
//! 日频交易不需要盘中实时行情，每天只在收盘前交易一次。
//! MVO 策略: Ledoit-Wolf + Grid Search 季度调仓 (自动发现权重)
//! 杠杆: 波动率目标 (vol_target, 20%年化波动率目标)
//! 启动时通过 tokio::spawn 在后台运行，每 60 秒检查一次。

use super::sync::{check_paper_account_data_readiness, DataReadinessGate};
use chrono::{Datelike, Local, NaiveDate, Timelike};
use ndarray::Array2;
use quant_common::mvo;
use quant_data::tushare::client::TushareClient;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::routes::strategy::{AssetClass, ResolvedStrategy};

/// 获取最新 EOD 数据版本（动态，确保回测使用最新数据而非硬编码的旧版本）
async fn get_latest_data_version(db: &PgPool) -> String {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT data_version_id FROM data_version
         WHERE data_version_id LIKE 'dv-eod-%'
         ORDER BY end_date DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    row.map(|(d,)| d)
        .unwrap_or_else(|| "research-full-2016-2026-20260515".to_string())
}

fn parse_scheduler_date(value: &str) -> Option<NaiveDate> {
    let trimmed = value.trim();
    NaiveDate::parse_from_str(trimmed, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y-%m-%d"))
        .ok()
}

async fn first_open_trade_date_on_or_after(
    db: &PgPool,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Option<NaiveDate> {
    sqlx::query_scalar::<_, NaiveDate>(
        "SELECT trade_date
         FROM market_trade_calendar
         WHERE exchange = 'SSE'
           AND is_open = true
           AND trade_date >= $1
           AND trade_date <= $2
         ORDER BY trade_date ASC
         LIMIT 1",
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

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

fn short_id() -> String {
    uuid::Uuid::new_v4().to_string().chars().take(12).collect()
}

struct DailyState {
    date: Option<NaiveDate>,
    traded_today: bool,     // 今日是否已完成调仓 (14:40+)
    eod_synced_today: bool, // 今日是否已完成日终数据同步 (16:00)
    yesterday_synced: bool, // 昨日日线是否已完成 T+1 同步 (次日9:00)
    cleanup_done: bool,
}

/// MVO 权重缓存（季度更新）
pub struct MvoWeightCache {
    quarter: String,   // e.g. "2026-Q2"
    weights: Vec<f64>, // [A股, 黄金, 国债, SP500, 纳指, 有色, 豆粕, 原油]
}

/// 从数据库加载的策略配置（运行时缓存，启动时加载）
#[derive(Debug, Clone, serde::Deserialize)]
pub struct StrategyConfig {
    pub strategy_id: String,
    pub name: String,
    pub etf_symbols: Vec<String>,
    pub equity_curve_task_id: String,
    pub min_stock: f64,
    pub max_single: f64,
    pub max_single_bull: f64,
    pub momentum_blend_ratio: f64,
    pub ga_population: usize,
    pub ga_generations: usize,
    pub vol_target: f64,
    pub leverage_cap: f64,
    pub default_weights: Vec<f64>,
    // A股大类选股方式（下沉到策略，不再挂账号）
    #[serde(default = "default_signal_source")]
    pub signal_source: String,
    #[serde(default = "default_blend_weight")]
    pub prediction_blend_weight: f64,
    #[serde(default = "default_combo_name")]
    pub combo_name: String,
    #[serde(default = "default_top_n")]
    pub top_n: i64,
    #[serde(default)]
    pub prediction_set_id: Option<String>,
    #[serde(default = "default_dynamic_target_cap")]
    pub dynamic_target_cap: f64,
    #[serde(default = "default_dynamic_target_floor")]
    pub dynamic_target_floor: f64,
    #[serde(default = "default_score_direction")]
    pub score_direction: String,
    #[serde(default = "default_candidate_tier")]
    pub candidate_tier: String,
}

fn default_signal_source() -> String {
    "prediction_blend".into()
}
fn default_blend_weight() -> f64 {
    0.5
}
fn default_combo_name() -> String {
    "full_pit_icir_37f".into()
}
fn default_top_n() -> i64 {
    30
}
fn default_dynamic_target_cap() -> f64 {
    0.30
}
fn default_dynamic_target_floor() -> f64 {
    0.12
}
fn default_score_direction() -> String {
    "descending".into()
}
fn default_candidate_tier() -> String {
    "research_baseline".into()
}

impl Default for StrategyConfig {
    fn default() -> Self {
        // 策略配置必须从 strategy_config 表加载,不允许代码硬编码 fallback。
        panic!("StrategyConfig::default() 被调用 — 策略配置必须从 DB 加载,检查 load_strategy_config 调用方");
    }
}

fn normalize_cron_expr(expr: &str) -> String {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    if parts.len() == 5 {
        format!("0 {}", parts.join(" "))
    } else {
        parts.join(" ")
    }
}

fn scheduled_task_time_minutes(expr: &str) -> Result<u32, String> {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    let (minute_idx, hour_idx) = match parts.len() {
        5 => (0, 1),
        6 | 7 => (1, 2),
        _ => return Err(format!("CRON 表达式 '{}' 字段数不是 5/6/7", expr)),
    };
    let minute = parts[minute_idx]
        .parse::<u32>()
        .map_err(|_| format!("CRON 表达式 '{}' 分字段不是固定数字", expr))?;
    let hour = parts[hour_idx]
        .parse::<u32>()
        .map_err(|_| format!("CRON 表达式 '{}' 时字段不是固定数字", expr))?;
    if minute > 59 || hour > 23 {
        return Err(format!("CRON 表达式 '{}' 时/分超出范围", expr));
    }
    Ok(hour * 60 + minute)
}

fn is_eod_sync_window(hour: u32, minute: u32) -> bool {
    hour == 16 && minute < 10
}

fn pre_trade_factor_combo(sc: &StrategyConfig) -> &str {
    let combo = sc.combo_name.trim();
    if combo.is_empty() {
        "full_pit_icir_37f"
    } else {
        combo
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

fn market_level_freshness_dataset(source: &str) -> Option<&'static str> {
    match source {
        "market_margin_regime" => Some("margin"),
        "market_moneyflow_hsgt_regime" => Some("moneyflow_hsgt"),
        _ => None,
    }
}

fn market_level_freshness_task_slug(source: &str) -> Option<&'static str> {
    match source {
        "market_margin_regime" => Some("margin"),
        "market_moneyflow_hsgt_regime" => Some("hsgt"),
        _ => None,
    }
}

fn market_level_freshness_sync_payload(
    source: &str,
    latest_trade_date: Option<NaiveDate>,
    today: NaiveDate,
) -> Option<serde_json::Value> {
    let dataset = market_level_freshness_dataset(source)?;
    let task_slug = market_level_freshness_task_slug(source)?;
    let start_date = latest_trade_date
        .map(|date| (date + chrono::Duration::days(1)).min(today))
        .unwrap_or(today);
    Some(serde_json::json!({
        "dataset": dataset,
        "source": "tushare",
        "start_date": start_date.format("%Y%m%d").to_string(),
        "end_date": today.format("%Y%m%d").to_string(),
        "data_version_id": format!("dv-p315-{}-{}", task_slug, today.format("%Y%m%d")),
        "background": true,
        "reason": "scheduled_p315_market_level_regime_source_freshness"
    }))
}

fn market_level_freshness_sources(params: &serde_json::Value) -> Vec<String> {
    params
        .get("sources")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| {
            vec![
                "market_margin_regime".to_string(),
                "market_moneyflow_hsgt_regime".to_string(),
            ]
        })
}

async fn latest_market_level_trade_date(db: &PgPool, source: &str) -> Option<NaiveDate> {
    let sql = match source {
        "market_margin_regime" => "SELECT MAX(trade_date) FROM market_margin",
        "market_moneyflow_hsgt_regime" => "SELECT MAX(trade_date) FROM market_moneyflow_hsgt",
        _ => return None,
    };
    sqlx::query_scalar::<_, Option<NaiveDate>>(sql)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用 StrategyConfig 字面量(显式构造,避免触发 panic 版 Default)。
    fn test_strategy_config() -> StrategyConfig {
        StrategyConfig {
            strategy_id: "test".into(),
            name: "test".into(),
            etf_symbols: vec![],
            equity_curve_task_id: String::new(),
            min_stock: 0.0,
            max_single: 0.0,
            max_single_bull: 0.0,
            momentum_blend_ratio: 0.0,
            ga_population: 0,
            ga_generations: 0,
            vol_target: 0.0,
            leverage_cap: 0.0,
            default_weights: vec![],
            signal_source: String::new(),
            prediction_blend_weight: 0.0,
            combo_name: String::new(),
            top_n: 0,
            prediction_set_id: None,
            dynamic_target_cap: 0.0,
            dynamic_target_floor: 0.0,
            score_direction: String::new(),
            candidate_tier: String::new(),
        }
    }

    #[test]
    fn normalize_cron_expr_accepts_existing_five_field_task_crons() {
        assert_eq!(normalize_cron_expr("0 9 * * 1-5"), "0 0 9 * * 1-5");
        assert_eq!(normalize_cron_expr("30 16 * * 1-5"), "0 30 16 * * 1-5");
        assert_eq!(normalize_cron_expr("0 30 16 * * 1-5"), "0 30 16 * * 1-5");
    }

    #[test]
    fn scheduled_task_time_supports_five_and_six_field_crons() {
        assert_eq!(
            scheduled_task_time_minutes("30 16 * * 1-5").expect("five field cron"),
            16 * 60 + 30
        );
        assert_eq!(
            scheduled_task_time_minutes("0 30 10 * * 1-5").expect("six field cron"),
            10 * 60 + 30
        );
        assert!(scheduled_task_time_minutes("not a cron").is_err());
    }

    #[test]
    fn market_level_freshness_payload_uses_latest_plus_one_range() {
        let payload = market_level_freshness_sync_payload(
            "market_margin_regime",
            NaiveDate::from_ymd_opt(2026, 6, 18),
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        )
        .expect("margin payload");

        assert_eq!(payload["dataset"], "margin");
        assert_eq!(payload["source"], "tushare");
        assert_eq!(payload["start_date"], "20260619");
        assert_eq!(payload["end_date"], "20260620");
        assert_eq!(payload["background"], true);
        assert_eq!(payload["data_version_id"], "dv-p315-margin-20260620");
    }

    #[test]
    fn market_level_freshness_payload_supports_hsgt_and_rejects_static_industry() {
        let payload = market_level_freshness_sync_payload(
            "market_moneyflow_hsgt_regime",
            NaiveDate::from_ymd_opt(2026, 5, 29),
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        )
        .expect("hsgt payload");

        assert_eq!(payload["dataset"], "moneyflow_hsgt");
        assert_eq!(payload["start_date"], "20260530");
        assert_eq!(
            market_level_freshness_sync_payload(
                "industry_prosperity_proxy",
                None,
                NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
            ),
            None
        );
    }

    #[test]
    fn eod_sync_window_does_not_replay_after_startup_late_in_day() {
        assert!(is_eod_sync_window(16, 0));
        assert!(is_eod_sync_window(16, 9));
        assert!(!is_eod_sync_window(16, 10));
        assert!(!is_eod_sync_window(16, 39));
        assert!(!is_eod_sync_window(17, 0));
    }

    #[test]
    fn pre_trade_factor_combo_uses_active_strategy_combo_not_price_volume_fallback() {
        // StrategyConfig::default() 已改 panic(配置必须从 DB 加载),测试手工构造。
        let strategy = StrategyConfig {
            combo_name: "full_pit_icir_37f".to_string(),
            ..test_strategy_config()
        };

        assert_eq!(pre_trade_factor_combo(&strategy), "full_pit_icir_37f");
    }

    #[test]
    fn a_share_event_gate_only_targets_main_a_share_symbols() {
        assert!(is_a_share_symbol("000001.SZ"));
        assert!(is_a_share_symbol("600000.SH"));
        assert!(is_a_share_symbol("300750.SZ"));
        assert!(!is_a_share_symbol("518880.SH"));
        assert!(!is_a_share_symbol("513500.SH"));
        assert!(!is_a_share_symbol("AAPL.US"));
    }

    #[test]
    fn test_resolved_to_legacy_sc_mapping() {
        use crate::routes::strategy::{
            AssetClass, AssetStrategy, MvoParams, ResolvedStrategy, SecurityConfig, StrategyType,
        };
        let rs = ResolvedStrategy {
            strategy_id: "v19".into(),
            name: "v19策略".into(),
            strategy_type: StrategyType::Composite,
            mvo: Some(MvoParams {
                vol_target: 0.2,
                leverage_cap: 2.5,
                leverage_floor: 1.0,
                default_weights: vec![0.22, 0.28, 0.05, 0.10, 0.03, 0.03, 0.03],
                min_stock: 0.12,
                momentum_blend_ratio: 0.5,
                ga_population: 600,
                ga_generations: 250,
                ga_elite_count: 10,
                regime_bull_threshold: 0.0,
                regime_bear_threshold: 0.0,
                regime_bull_min_stock: 0.0,
                regime_bear_min_stock: 0.0,
                dynamic_target_cap: 0.30,
                dynamic_target_floor: 0.12,
                risk_free_rate: 0.03,
                grid_step: 0.0,
            }),
            assets: vec![
                AssetStrategy {
                    strategy_id: "v19-commodity".into(),
                    asset_class: AssetClass::Commodity,
                    security: SecurityConfig {
                        signal_source: "fixed".into(),
                        combo_name: String::new(),
                        top_n: 30,
                        prediction_set_id: None,
                        prediction_blend_weight: 0.5,
                        score_direction: "descending".into(),
                        candidate_tier: "research_baseline".into(),
                        equity_curve_task_id: None,
                        fixed_symbols: vec![
                            "518880.SH".into(),
                            "159980.SZ".into(),
                            "501018.SH".into(),
                            "159985.SZ".into(),
                        ],
                        default_weights: vec![0.22, 0.03, 0.03, 0.03],
                        max_single: 0.75,
                        max_single_bull: 0.80,
                    },
                },
                AssetStrategy {
                    strategy_id: "v19-bond".into(),
                    asset_class: AssetClass::Bond,
                    security: SecurityConfig {
                        signal_source: "fixed".into(),
                        combo_name: String::new(),
                        top_n: 30,
                        prediction_set_id: None,
                        prediction_blend_weight: 0.5,
                        score_direction: "descending".into(),
                        candidate_tier: "research_baseline".into(),
                        equity_curve_task_id: None,
                        fixed_symbols: vec!["511010.SH".into()],
                        default_weights: vec![0.28],
                        max_single: 0.75,
                        max_single_bull: 0.80,
                    },
                },
                AssetStrategy {
                    strategy_id: "v19-us_stock".into(),
                    asset_class: AssetClass::UsStock,
                    security: SecurityConfig {
                        signal_source: "fixed".into(),
                        combo_name: String::new(),
                        top_n: 30,
                        prediction_set_id: None,
                        prediction_blend_weight: 0.5,
                        score_direction: "descending".into(),
                        candidate_tier: "research_baseline".into(),
                        equity_curve_task_id: None,
                        fixed_symbols: vec!["513500.SH".into(), "513100.SH".into()],
                        default_weights: vec![0.05, 0.10],
                        max_single: 0.75,
                        max_single_bull: 0.80,
                    },
                },
                AssetStrategy {
                    strategy_id: "v19-a_share".into(),
                    asset_class: AssetClass::AShare,
                    security: SecurityConfig {
                        signal_source: "prediction_blend".into(),
                        combo_name: "full_pit_icir_37f".into(),
                        top_n: 30,
                        prediction_set_id: Some("ps-1".into()),
                        prediction_blend_weight: 0.5,
                        score_direction: "descending".into(),
                        candidate_tier: "professional_observation".into(),
                        equity_curve_task_id: Some("fbt-ab3eecf6".into()),
                        fixed_symbols: vec![],
                        default_weights: vec![],
                        max_single: 0.75,
                        max_single_bull: 0.80,
                    },
                },
            ],
            // etf_symbols 取 MVO 标准顺序(composite 行加载的值)
            etf_symbols: vec![
                "518880.SH".into(),
                "511010.SH".into(),
                "513500.SH".into(),
                "513100.SH".into(),
                "159980.SZ".into(),
                "159985.SZ".into(),
                "501018.SH".into(),
            ],
            rebalance_freq: "quarterly".into(),
        };
        let sc = resolved_to_legacy_sc(&rs);
        // MVO 参数映射
        assert!((sc.min_stock - 0.12).abs() < 1e-9, "min_stock 映射");
        assert!((sc.vol_target - 0.2).abs() < 1e-9, "vol_target 映射");
        assert!((sc.leverage_cap - 2.5).abs() < 1e-9, "leverage_cap 映射");
        assert_eq!(
            sc.default_weights,
            vec![0.22, 0.28, 0.05, 0.10, 0.03, 0.03, 0.03],
            "default_weights 映射"
        );
        // a_share 取参
        assert_eq!(
            sc.equity_curve_task_id, "fbt-ab3eecf6",
            "equity_curve_task_id 取自 a_share"
        );
        assert_eq!(sc.signal_source, "prediction_blend", "signal_source 取自 a_share");
        assert_eq!(sc.combo_name, "full_pit_icir_37f", "combo_name 取自 a_share");
        assert_eq!(
            sc.candidate_tier, "professional_observation",
            "candidate_tier 取自 a_share"
        );
        assert!((sc.max_single - 0.75).abs() < 1e-9, "max_single 取自 a_share");
        assert!((sc.max_single_bull - 0.80).abs() < 1e-9, "max_single_bull 取自 a_share");
        // etf_symbols 保持 MVO 标准顺序(黄金/国债/标普/纳指/有色/豆粕/原油)
        assert_eq!(
            sc.etf_symbols,
            vec![
                "518880.SH", "511010.SH", "513500.SH", "513100.SH",
                "159980.SZ", "159985.SZ", "501018.SH"
            ],
            "etf_symbols 保持 MVO 标准顺序"
        );
    }
}

/// 反向桥接:ResolvedStrategy 树 → 平铺 StrategyConfig。
/// 三个 MVO 函数(compute_lw_mvo_weights/compute_mvo_weights_for_date/compute_vol_target_leverage)
/// 本轮不改签名仍接 &StrategyConfig,rebalance_account 接 &ResolvedStrategy 后调此函数得到临时视图传入。
pub(crate) fn resolved_to_legacy_sc(rs: &ResolvedStrategy) -> StrategyConfig {
    let mvo = rs.mvo.as_ref();
    let a_share = rs.assets.iter().find(|a| a.asset_class == AssetClass::AShare);
    StrategyConfig {
        strategy_id: rs.strategy_id.clone(),
        name: rs.name.clone(),
        etf_symbols: rs_to_legacy_etf_symbols(rs),
        equity_curve_task_id: a_share
            .and_then(|a| a.security.equity_curve_task_id.clone())
            .unwrap_or_default(),
        min_stock: mvo.map(|m| m.min_stock).unwrap_or(0.12),
        max_single: a_share.map(|a| a.security.max_single).unwrap_or(0.75),
        max_single_bull: a_share.map(|a| a.security.max_single_bull).unwrap_or(0.80),
        momentum_blend_ratio: mvo.map(|m| m.momentum_blend_ratio).unwrap_or(0.5),
        ga_population: mvo.map(|m| m.ga_population).unwrap_or(600),
        ga_generations: mvo.map(|m| m.ga_generations).unwrap_or(250),
        vol_target: mvo.map(|m| m.vol_target).unwrap_or(0.2),
        leverage_cap: mvo.map(|m| m.leverage_cap).unwrap_or(2.5),
        default_weights: mvo.map(|m| m.default_weights.clone()).unwrap_or_default(),
        signal_source: a_share
            .map(|a| a.security.signal_source.clone())
            .unwrap_or_else(|| "fixed".into()),
        prediction_blend_weight: a_share
            .map(|a| a.security.prediction_blend_weight)
            .unwrap_or(0.5),
        combo_name: a_share.map(|a| a.security.combo_name.clone()).unwrap_or_default(),
        top_n: a_share.map(|a| a.security.top_n).unwrap_or(30),
        prediction_set_id: a_share.and_then(|a| a.security.prediction_set_id.clone()),
        dynamic_target_cap: mvo.map(|m| m.dynamic_target_cap).unwrap_or(0.30),
        dynamic_target_floor: mvo.map(|m| m.dynamic_target_floor).unwrap_or(0.12),
        score_direction: a_share
            .map(|a| a.security.score_direction.clone())
            .unwrap_or_else(|| "descending".into()),
        candidate_tier: a_share
            .map(|a| a.security.candidate_tier.clone())
            .unwrap_or_else(|| "research_baseline".into()),
    }
}

/// 从 ResolvedStrategy 提取 MVO 标准顺序的 7 ETF 列表(MVO 8 维权重的第 1-7 列)。
/// 顺序:[黄金,国债,标普,纳指,有色,豆粕,原油]。
/// ResolvedStrategy.etf_symbols 已在 load 时从 composite 行加载(正确 MVO 顺序),直接 clone。
pub(crate) fn rs_to_legacy_etf_symbols(rs: &ResolvedStrategy) -> Vec<String> {
    rs.etf_symbols.clone()
}

/// 从数据库加载活跃策略配置，失败时 panic（策略配置必须从 DB 加载，不允许硬编码 fallback）
pub async fn load_strategy_config(db: &PgPool, strategy_id: &str) -> StrategyConfig {
    let row: Option<(serde_json::Value,)> = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT jsonb_build_object(
            'strategy_id', strategy_id,
            'name', name,
            'etf_symbols', etf_symbols,
            'equity_curve_task_id', equity_curve_task_id,
            'min_stock', min_stock,
            'max_single', max_single,
            'max_single_bull', max_single_bull,
            'momentum_blend_ratio', momentum_blend_ratio,
            'ga_population', ga_population,
            'ga_generations', ga_generations,
            'vol_target', vol_target,
            'leverage_cap', leverage_cap,
            'default_weights', default_weights,
            'signal_source', signal_source,
            'prediction_blend_weight', prediction_blend_weight,
            'combo_name', combo_name,
            'top_n', top_n,
            'prediction_set_id', prediction_set_id,
            'dynamic_target_cap', dynamic_target_cap,
            'dynamic_target_floor', dynamic_target_floor,
            'score_direction', score_direction,
            'candidate_tier', candidate_tier
        ) FROM strategy_config WHERE strategy_id = $1 AND status = 'active'",
    )
    .bind(strategy_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    match row {
        Some((v,)) => {
            let cfg: StrategyConfig = serde_json::from_value(v)
                .unwrap_or_else(|e| panic!("策略配置 {} 反序列化失败: {}", strategy_id, e));
            info!("[scheduler] 策略配置加载: {} (DB)", cfg.strategy_id);
            cfg
        }
        None => panic!(
            "[scheduler] 策略配置加载失败 strategy_id={},strategy_config 表无此 active 记录",
            strategy_id
        ),
    }
}

async fn run_scheduled_tasks(db: &PgPool) {
    let now = chrono::Local::now();
    let tasks: Vec<(String, String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT task_name, task_type, schedule_cron, params FROM scheduled_task_config
         WHERE enabled = true AND (next_run_at IS NULL OR next_run_at <= $1)
         ORDER BY next_run_at NULLS FIRST",
    )
    .bind(now)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    for (name, task_type, cron_expr, params) in &tasks {
        info!(
            "[scheduler] 定时任务触发: {} ({}) cron={}",
            name, task_type, cron_expr
        );
        match task_type.as_str() {
            "data_quality_check" => {
                run_data_quality_check(db).await;
            }
            "equity_curve_update" => {
                // 从 v19 策略配置读 A股选股方式，权益曲线与策略一致（因子+ML混合）
                let sc = load_strategy_config(db, "v19").await;
                let combo = params
                    .get("combo_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(sc.combo_name.as_str());
                let top_n = params
                    .get("top_n")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(sc.top_n as u64) as usize;
                info!(
                    "[scheduler] 权益曲线更新: combo={} top_n={} signal={}",
                    combo, top_n, sc.signal_source
                );
                let client = reqwest::Client::new();
                let end_date = chrono::Utc::now().format("%Y%m%d").to_string();
                let end_date_naive = chrono::Utc::now().date_naive();
                let requested_start_date = params
                    .get("start_date")
                    .and_then(|v| v.as_str())
                    .and_then(parse_scheduler_date)
                    .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(2014, 1, 2).unwrap());
                let start_date =
                    first_open_trade_date_on_or_after(db, requested_start_date, end_date_naive)
                        .await
                        .unwrap_or(requested_start_date)
                        .format("%Y%m%d")
                        .to_string();
                let dv = get_latest_data_version(db).await;
                let mut payload = serde_json::json!({
                    "combo_name": combo, "strategy_version_id": "factor-combo-v1",
                    "data_version_id": dv, "top_n": top_n,
                    "rebalance": params.get("rebalance").and_then(|v| v.as_str()).unwrap_or("10"),
                    "start_date": start_date, "end_date": end_date,
                    "max_position_pct": 0.10, "max_gross_exposure": 0.95,
                    "benchmark": "000300.SH", "universe_profile": "main_board_non_st",
                    "score_direction": sc.score_direction,
                    "effective_coverage": {
                        "enabled": true,
                        "mode": "guard_only",
                        "min_rows": top_n.max(30),
                        "include_rebalance_warmup": false
                    }
                });
                // 因子+ML混合：带上策略指定的全周期预测集（无则选最新覆盖区间的 PIT 集）
                if sc.signal_source == "prediction_blend" || sc.signal_source == "prediction" {
                    let pid = if let Some(ref p) = sc.prediction_set_id {
                        Some(p.clone())
                    } else {
                        sqlx::query_scalar::<_, String>(
                            "SELECT prediction_set_id FROM prediction_set WHERE status='ready' AND training_end_date IS NOT NULL
                             ORDER BY (end_date - start_date) DESC, end_date DESC LIMIT 1"
                        ).fetch_optional(db).await.ok().flatten()
                    };
                    if let Some(pid) = pid {
                        payload["prediction_set_id"] = serde_json::json!(pid);
                        payload["prediction_blend_weight"] =
                            serde_json::json!(sc.prediction_blend_weight);
                        payload["kelly_fraction"] = serde_json::json!(0.25);
                        payload["score_candidate_pool_size"] = serde_json::json!(200);
                        info!(
                            "[scheduler] 权益曲线启用 prediction_blend: set={} w={}",
                            pid, sc.prediction_blend_weight
                        );
                    }
                }
                let api_base = format!(
                    "http://localhost:{}",
                    std::env::var("PORT").unwrap_or_else(|_| "8080".into())
                );
                match client
                    .post(format!("{}/api/v1/quant/backtests/run-factor", api_base))
                    .json(&payload)
                    .timeout(std::time::Duration::from_secs(600))
                    .send()
                    .await
                {
                    Ok(resp) => {
                        if let Ok(result) = resp.json::<serde_json::Value>().await {
                            if let Some(tid) = result["data"]["task_id"].as_str() {
                                info!("[scheduler] 新权益曲线已生成: task_id={}, 自动更新strategy_config", tid);
                                let _ = sqlx::query(
                                    "UPDATE strategy_config SET equity_curve_task_id = $1, updated_at = NOW() WHERE strategy_id = 'v19' AND status = 'active'"
                                ).bind(tid).execute(db).await;
                                // 同时保持 v20 同步（如果存在）
                                let _ = sqlx::query(
                                    "UPDATE strategy_config SET equity_curve_task_id = $1, updated_at = NOW() WHERE strategy_id = 'v20' AND status = 'active'"
                                ).bind(tid).execute(db).await;
                            }
                        }
                    }
                    Err(e) => warn!("[scheduler] 权益曲线更新失败: {}", e),
                }
            }
            "factor_backfill" => {
                // 因子回填已在 T+1 补同步中处理
            }
            "pit_combo_refresh" => {
                // PIT 滚动 ICIR combo 数据保鲜：增量物化最近季度（幂等）。
                // 防止随交易日推移 combo 分数过时。依赖：因子已重算 + 滚动 IC 已评估。
                let combo = params
                    .get("combo_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("full_pit_icir_37f");
                let ver = params
                    .get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("1.0.0");
                let horizon = params.get("horizon").and_then(|v| v.as_i64()).unwrap_or(20) as i16;
                // 增量区间：默认最近一年（覆盖当前+上季度，幂等刷新）
                let refresh_start = chrono::Utc::now().date_naive() - chrono::Duration::days(370);
                let refresh_end = chrono::Utc::now().date_naive();
                info!(
                    "[scheduler] PIT combo 保鲜: combo={} 区间 {}~{}",
                    combo, refresh_start, refresh_end
                );
                match crate::routes::factors::materialize_pit_combo(
                    db,
                    combo,
                    ver,
                    horizon,
                    refresh_start,
                    refresh_end,
                )
                .await
                {
                    Ok(rows) => info!("[scheduler] PIT combo 保鲜完成: {} 行", rows),
                    Err(e) => warn!("[scheduler] PIT combo 保鲜失败: {}", e),
                }
            }
            "market_level_source_freshness" => {
                let today = chrono::Utc::now().date_naive();
                let api_base = format!(
                    "http://localhost:{}",
                    std::env::var("PORT").unwrap_or_else(|_| "8080".into())
                );
                let client = reqwest::Client::new();
                for source in market_level_freshness_sources(params) {
                    let latest = latest_market_level_trade_date(db, &source).await;
                    let Some(payload) = market_level_freshness_sync_payload(&source, latest, today)
                    else {
                        warn!("[scheduler] P3.15 市场级源不支持自动同步: {}", source);
                        continue;
                    };
                    info!(
                        "[scheduler] P3.15 市场级源保鲜: source={} latest={:?}",
                        source, latest
                    );
                    match client
                        .post(format!("{}/api/v1/quant/data/sync-tasks", api_base))
                        .json(&payload)
                        .timeout(std::time::Duration::from_secs(600))
                        .send()
                        .await
                    {
                        Ok(resp) => match resp.json::<serde_json::Value>().await {
                            Ok(result) => info!(
                                "[scheduler] P3.15 市场级源同步任务返回: source={} result={}",
                                source, result
                            ),
                            Err(error) => warn!(
                                "[scheduler] P3.15 市场级源同步响应解析失败: source={} error={}",
                                source, error
                            ),
                        },
                        Err(error) => warn!(
                            "[scheduler] P3.15 市场级源同步任务提交失败: source={} error={}",
                            source, error
                        ),
                    }
                }
            }
            _ => {}
        }

        // 根据 CRON 表达式计算下次运行时间
        let normalized_cron = normalize_cron_expr(cron_expr);
        let next = match cron::Schedule::from_str(&normalized_cron) {
            Ok(schedule) => schedule.upcoming(chrono::Local).next(),
            Err(e) => {
                warn!(
                    "[scheduler] 任务 {} 的 CRON 表达式 '{}' 规范化为 '{}' 后仍无效: {}，默认 1 天后",
                    name, cron_expr, normalized_cron, e
                );
                None
            }
        };
        let next_at = next.unwrap_or_else(|| now + chrono::Duration::days(1));
        let _ = sqlx::query(
            "UPDATE scheduled_task_config SET last_run_at = NOW(), run_count = run_count + 1,
             last_status = 'success', next_run_at = $1 WHERE task_name = $2",
        )
        .bind(next_at)
        .bind(name)
        .execute(db)
        .await;
    }
}

/// 启动后台调度器。
pub fn start_scheduler(db: PgPool, tushare: TushareClient, port: u16) {
    tokio::spawn(async move {
        let tushare = Arc::new(tushare);
        let state = Arc::new(Mutex::new(DailyState {
            date: None,
            traded_today: false,
            eod_synced_today: false,
            yesterday_synced: false,
            cleanup_done: false,
        }));
        let mvo_cache: Arc<Mutex<Option<MvoWeightCache>>> = Arc::new(Mutex::new(None));

        // 从数据库加载策略配置
        let strategy_config = Arc::new(load_strategy_config(&db, "v19").await);

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        info!(
            "[scheduler] {} 已启动 ({}): 14:40调仓 | 16:00 EOD | 9:00 T+1数据补同步",
            strategy_config.strategy_id, strategy_config.name
        );

        // 首次运行时检查定时任务
        run_scheduled_tasks(&db).await;

        loop {
            interval.tick().await;
            if let Err(e) =
                run_tick(&db, &tushare, &state, &mvo_cache, port, &strategy_config).await
            {
                error!("[scheduler] 任务失败: {}", e);
            }
            // 每小时检查一次定时任务
            let now = chrono::Local::now();
            if now.minute() == 0 {
                run_scheduled_tasks(&db).await;
            }
        }
    });
}

/// 获取当前日期对应的最优 WFA 参数（从已完成的实验中提取）
async fn get_current_wfa_params(db: &PgPool, date: NaiveDate) -> Result<serde_json::Value, String> {
    let row = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT parameters FROM wfa_strategy_params
         WHERE test_start <= $1 AND test_end >= $1
         ORDER BY score DESC NULLS LAST LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("wfa params: {e}"))?;

    Ok(row.map(|(p,)| p).unwrap_or_default())
}

async fn run_tick(
    db: &PgPool,
    tushare: &TushareClient,
    state: &Arc<Mutex<DailyState>>,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    port: u16,
    sc: &StrategyConfig,
) -> Result<(), String> {
    let now = Local::now();
    let today = now.date_naive();
    let hour = now.time().hour();
    let minute = now.time().minute();

    // 日期切换：重置状态
    {
        let mut st = state.lock().await;
        if st.date != Some(today) {
            st.date = Some(today);
            st.traded_today = false;
            st.eod_synced_today = false;
            st.yesterday_synced = false;
            st.cleanup_done = false;
        }
    }

    let is_trade = is_trading_day(db, today).await?;

    // ── 14:40~15:00 (收盘前): 交易日才调仓 ──
    if is_trade && hour == 14 && minute >= 40 {
        let should_trade = {
            let st = state.lock().await;
            !st.traded_today
        };

        if should_trade {
            {
                let mut st = state.lock().await;
                st.traded_today = true;
            }
            info!("[scheduler] 14:45 日频调仓 (v16 LW-MVO 7-asset)...");

            // ── 前置数据校验+自动修复 ──
            let data_errors = validate_pre_trade_data(db, tushare, today, sc).await;
            if !data_errors.is_empty() {
                let alert_msg = format!(
                    "⛔ [调仓拒绝] {} 数据异常，已尝试自动修复失败:\n{}",
                    today.format("%Y-%m-%d"),
                    data_errors.join("\n")
                );
                error!("{}", alert_msg);
                send_dingtalk_alert(db, &alert_msg).await;
                // 将 traded_today 重置，下次 tick 可以重试
                let mut st = state.lock().await;
                st.traded_today = false;
                return Ok(());
            }

            // 生成信号 + 调仓
            match generate_paper_signals_for_all(db, mvo_cache, port, today, sc, tushare).await {
                Ok(_) => {
                    info!("[scheduler] 调仓完成, 推送钉钉...");
                    match push_dingtalk_for_all_accounts(db, today).await {
                        Ok(_) => info!("[scheduler] 钉钉推送完成"),
                        Err(e) => warn!("[scheduler] 钉钉推送失败: {}", e),
                    }
                }
                Err(e) => warn!("[scheduler] 调仓失败: {}", e),
            }
        }
    }

    // ── 16:00 (收盘后): 交易日EOD + 非交易日也执行数据同步 ──
    if is_eod_sync_window(hour, minute) {
        let should_sync = {
            let st = state.lock().await;
            !st.eod_synced_today
        };

        if should_sync {
            {
                let mut st = state.lock().await;
                st.eod_synced_today = true;
            }
            info!("[scheduler] 16:00 日终数据同步...");
            if let Err(e) = sync_eod_data(db, tushare, today).await {
                warn!("[scheduler] 日终数据同步失败: {}", e);
            }
        }
    }

    // ── 每日 9:00: 同步上一个交易日日线 (Tushare T+1) + 因子重算 ──
    // 非交易日同样执行，周末/假日后自动回补缺失数据
    if hour == 9 && minute < 5 {
        let should_sync_yesterday = {
            let st = state.lock().await;
            !st.yesterday_synced
        };
        if should_sync_yesterday {
            {
                let mut st = state.lock().await;
                st.yesterday_synced = true;
            }
            // 查询最近一个交易日（处理假期/周末间隔）
            let last_trade_date: Option<(chrono::NaiveDate,)> = sqlx::query_as(
                "SELECT trade_date FROM market_trade_calendar WHERE is_open = true AND trade_date < $1 ORDER BY trade_date DESC LIMIT 1"
            ).bind(today).fetch_optional(db).await.ok().flatten();
            let sync_date = match last_trade_date {
                Some((d,)) => d,
                None => today - chrono::Duration::days(1),
            };
            let sync_date_str = sync_date.format("%Y%m%d").to_string();
            info!(
                "[scheduler] 9:00 T+1 补同步最近交易日 {} 日线 + 因子重算...",
                sync_date_str
            );

            // Step 1: 先同步事件数据，避免停牌/涨跌停门禁被“完成标记”误放行。
            if let Err(e) = quant_data::sync::sync_suspension(db, tushare, &sync_date_str).await {
                warn!("[scheduler] T+1 停牌同步失败: {}", e);
            }
            if !sync_limit_with_retry(db, tushare, &sync_date_str).await {
                warn!("[scheduler] T+1 涨跌停同步失败 (已重试)");
            }

            // Step 2: 同步最近交易日日线 + ETF日线 (T+1数据已就绪)
            let bar_dv = format!("dv-t1-{}", sync_date_str);
            let all_stocks: Vec<String> = sqlx::query_scalar(
                "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol",
            )
            .fetch_all(db)
            .await
            .unwrap_or_default();
            match quant_data::sync::sync_daily_bars(
                db,
                tushare,
                &all_stocks,
                &sync_date_str,
                &sync_date_str,
                &bar_dv,
            )
            .await
            {
                Ok(n) => info!("[scheduler] T+1 A股日线同步: {} 条", n),
                Err(e) => warn!("[scheduler] T+1 A股日线同步失败: {}", e),
            }
            let etf_symbols = &sc.etf_symbols;
            let _ = quant_data::sync::sync_fund_daily(
                db,
                tushare,
                etf_symbols,
                &sync_date_str,
                &sync_date_str,
                &format!("etf-t1-{}", sync_date_str),
            )
            .await;
            let index_codes = vec!["000300.SH".to_string()];
            let _ = quant_data::sync::sync_index_daily(
                db,
                tushare,
                &index_codes,
                &sync_date_str,
                &sync_date_str,
                &format!("idx-t1-{}", sync_date_str),
            )
            .await;

            // Step 3: 验证日线数据已就绪 (实际查询DB确认, 非盲等)
            let mut retries = 0;
            let max_retries = 30; // 最多等5分钟 (30×10s)
            loop {
                let count: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM market_stock_daily_bar_adj WHERE trade_date = $1",
                )
                .bind(sync_date)
                .fetch_one(db)
                .await
                .unwrap_or((0,));
                if count.0 > 100 {
                    info!(
                        "[scheduler] T+1 日线数据已就绪: {} 条 (等待{}s)",
                        count.0,
                        retries * 10
                    );
                    break;
                }
                retries += 1;
                if retries >= max_retries {
                    warn!(
                        "[scheduler] T+1 日线数据等待超时({}s), 仅{}条, 因子回填可能不完整",
                        retries * 10,
                        count.0
                    );
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            }

            // Step 4: 日线就绪后才触发因子回填
            if retries < max_retries {
                info!("[scheduler] 触发因子回填 (依赖数据已就绪)");
                let client = reqwest::Client::new();
                let backfill_start = (sync_date - chrono::Duration::days(7))
                    .format("%Y%m%d")
                    .to_string();
                let _ = client
                    .post(format!("http://localhost:{}/api/v1/quant/factors/phase7-price-volume-backfill/background", port))
                    .json(&serde_json::json!({"start_date": backfill_start, "end_date": sync_date_str}))
                    .timeout(std::time::Duration::from_secs(10))
                    .send().await;
                if sc.combo_name != "phase7_price_volume_expanded_v1" {
                    match crate::routes::factors::materialize_pit_combo(
                        db,
                        pre_trade_factor_combo(sc),
                        "1.0.0",
                        20,
                        sync_date - chrono::Duration::days(7),
                        sync_date,
                    )
                    .await
                    {
                        Ok(rows) => info!("[scheduler] T+1 PIT combo 增量物化完成: {} 行", rows),
                        Err(e) => warn!("[scheduler] T+1 PIT combo 增量物化失败: {}", e),
                    }
                }
            }

            info!("[scheduler] T+1 补同步完成 ({})", sync_date_str);
        }
    }

    // ── 收盘后 (16:00): 清理 7 天前过期回测数据 (每日一次) ──
    if hour >= 16 {
        let should_cleanup = {
            let st = state.lock().await;
            !st.cleanup_done
        };

        if should_cleanup {
            info!("[scheduler] 清理过期回测数据 (7天前, is_kept=false)...");
            match super::cleanup::clean_expired_backtests(db).await {
                Ok((count, freed)) => {
                    let mut st = state.lock().await;
                    st.cleanup_done = true;
                    if count > 0 {
                        info!(
                            "[scheduler] 已清理 {} 个过期回测任务, 释放约 {}",
                            count,
                            super::cleanup::format_bytes(freed)
                        );
                    }
                }
                Err(e) => warn!("[scheduler] 过期数据清理失败: {}", e),
            }
        }
    }

    // ── 季度 WFA 自动优化 (每季度第一个交易日) ──
    let is_first_trading_day_of_quarter = {
        let m = today.month();
        let is_q_start = matches!(m, 1 | 4 | 7 | 10);
        if !is_q_start {
            false
        } else {
            // 检查是否是该季度第一个交易日
            sqlx::query_as::<_, (Option<bool>,)>(
                "SELECT is_open FROM market_trade_calendar WHERE trade_date = $1",
            )
            .bind(today)
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .and_then(|(v,)| v)
            .unwrap_or(false)
        }
    };

    if is_first_trading_day_of_quarter && hour >= 16 {
        let quarter = format!("{}-Q{}", today.year(), (today.month() - 1) / 3 + 1);
        match check_and_trigger_wfa(db, port, &quarter, today).await {
            Ok(Some(msg)) => info!("[scheduler] WFA: {}", msg),
            Ok(None) => {}
            Err(e) => warn!("[scheduler] WFA check failed: {}", e),
        }
    }

    Ok(())
}

/// 检查是否需要触发 WFA 优化，如果需要则启动实验。
/// 返回 Some(msg) 表示执行了操作，None 表示跳过。
async fn check_and_trigger_wfa(
    db: &PgPool,
    port: u16,
    quarter: &str,
    today: NaiveDate,
) -> Result<Option<String>, String> {
    // 检查已有该季度的 WFA 参数
    let existing = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM wfa_strategy_params WHERE test_start <= $1 AND test_end >= $1",
    )
    .bind(today)
    .fetch_one(db)
    .await
    .map_err(|e| format!("wfa query: {e}"))?;

    if existing.0 > 0 {
        return Ok(None); // 已有参数，跳过
    }

    // 检查是否有正在运行的实验
    let running = sqlx::query_as::<_, (String,)>(
        "SELECT experiment_run_id FROM experiment_run
         WHERE experiment_type = 'phase7_oos_walk_forward_discovery'
           AND status IN ('running', 'queued')
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| format!("exp query: {e}"))?;

    if running.is_some() {
        // 检查是否可以从已完成的实验提取参数
        try_extract_wfa_params(db).await?;
        return Ok(Some("已有实验运行中，已尝试提取参数".into()));
    }

    // 检查最近一次实验完成时间（避免过于频繁）
    let recent = sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>,)>(
        "SELECT created_at FROM experiment_run
         WHERE experiment_type = 'phase7_oos_walk_forward_discovery'
           AND status = 'completed'
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| format!("recent query: {e}"))?;

    if let Some((last_time,)) = recent {
        let days_since = (chrono::Utc::now() - last_time).num_days();
        if days_since < 30 {
            // 尝试从最近的实验提取参数
            try_extract_wfa_params(db).await?;
            return Ok(Some(format!("最近实验 {} 天前，跳过新建", days_since)));
        }
    }

    // 启动新的 WFA 实验
    let client = reqwest::Client::new();
    let base = format!("http://localhost:{}", port);
    let resp = client
        .post(format!(
            "{}/api/v1/quant/optimizations/phase7-oos-walk-forward-discovery",
            base
        ))
        .json(&serde_json::json!({
            "data_version_id": &get_latest_data_version(db).await,
            "strategy_version_id": "phase7-professional-v1",
            "search_profile": "professional_simple_heuristic_discovery_default",
            "start_date": "20160201",
            "end_date": today.format("%Y%m%d").to_string(),
            "oos_top_n": 2,
            "execution_mode": "background",
        }))
        .send()
        .await
        .map_err(|e| format!("HTTP: {e}"))?;

    if resp.status().is_success() {
        Ok(Some(format!("新 WFA 实验已启动 (quarter={})", quarter)))
    } else {
        Err(format!("WFA 启动失败: {}", resp.status()))
    }
}

/// 从已完成的 WFA 实验中提取最优参数
async fn try_extract_wfa_params(db: &PgPool) -> Result<(), String> {
    // 找到最近完成的实验
    let exp_id = sqlx::query_as::<_, (String,)>(
        "SELECT experiment_run_id FROM experiment_run
         WHERE experiment_type = 'phase7_oos_walk_forward_discovery'
           AND status = 'completed'
         ORDER BY completed_at DESC NULLS LAST LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| format!("exp: {e}"))?
    .map(|(id,)| id);

    let Some(exp_id) = exp_id else {
        return Ok(());
    };

    // 检查是否已提取过
    let already = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM wfa_strategy_params WHERE experiment_run_id = $1",
    )
    .bind(&exp_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("check: {e}"))?;

    if already.0 > 0 {
        return Ok(());
    }

    // 获取该实验的所有 optimization tasks
    let tasks = sqlx::query_as::<_, (String, i32)>(
        "SELECT o.optimization_task_id, (o.walk_forward_config->>'window_index')::int
         FROM optimization_task o
         WHERE o.walk_forward_config->>'experiment_run_id' = $1
           AND o.status = 'completed'
         ORDER BY (o.walk_forward_config->>'window_index')::int",
    )
    .bind(&exp_id)
    .fetch_all(db)
    .await
    .map_err(|e| format!("tasks: {e}"))?;

    let mut extracted = 0;
    for (task_id, window_idx) in &tasks {
        // 找 best_trial
        let trial = sqlx::query_as::<
            _,
            (
                String,
                Option<serde_json::Value>,
                Option<rust_decimal::Decimal>,
            ),
        >(
            "SELECT t.trial_id, t.parameters, t.score
             FROM optimization_trial t
             WHERE t.optimization_task_id = $1 AND t.status = 'completed'
             ORDER BY t.score DESC NULLS LAST LIMIT 1",
        )
        .bind(task_id)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("trial: {e}"))?;

        if let Some((trial_id, Some(params), score)) = trial {
            // 从 walk_forward_config 获取窗口日期
            let wf: Option<serde_json::Value> = sqlx::query_as::<_, (Option<serde_json::Value>,)>(
                "SELECT walk_forward_config FROM optimization_task WHERE optimization_task_id = $1",
            )
            .bind(task_id)
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .and_then(|(v,)| v);

            let test_start = wf
                .as_ref()
                .and_then(|w| w.get("test_start").and_then(|v| v.as_str()))
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
            let test_end = wf
                .as_ref()
                .and_then(|w| w.get("test_end").and_then(|v| v.as_str()))
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

            if let (Some(ts), Some(te)) = (test_start, test_end) {
                sqlx::query(
                    "INSERT INTO wfa_strategy_params (experiment_run_id, window_index, test_start, test_end, parameters, score)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (experiment_run_id, window_index) DO UPDATE
                     SET parameters = EXCLUDED.parameters, score = EXCLUDED.score",
                )
                .bind(&exp_id)
                .bind(window_idx)
                .bind(ts)
                .bind(te)
                .bind(&params)
                .bind(score.map(|s| s.to_string().parse::<f64>().unwrap_or(0.0)))
                .execute(db)
                .await
                .map_err(|e| format!("insert wfa: {e}"))?;
                extracted += 1;
                info!(
                    "[scheduler] WFA 参数已提取: window={} trial={}",
                    window_idx,
                    &trial_id[..32.min(trial_id.len())]
                );
            }
        }
    }

    if extracted > 0 {
        info!("[scheduler] WFA 参数提取完成: {} 个窗口", extracted);
    }
    Ok(())
}

/// 调仓前数据校验+自动修复。返回非空列表 = 校验/修复失败，拒绝调仓。
async fn validate_pre_trade_data(
    db: &PgPool,
    tushare: &TushareClient,
    today: NaiveDate,
    sc: &StrategyConfig,
) -> Vec<String> {
    let mut errors: Vec<String> = Vec::new();
    let today_str = today.format("%Y%m%d").to_string();

    // 1. ETF 日线 — 每个标的必须覆盖到最近一个交易日
    for symbol in &sc.etf_symbols {
        let stale = check_data_freshness(db, symbol, today, 1).await;
        if let Some(gap_td) = stale {
            info!(
                "[pre-trade] {} 数据落后{}交易日, 尝试自动同步...",
                symbol, gap_td
            );
            let dv_id = format!("pre-trade-etf-{}-{}", symbol, today_str);
            let n = quant_data::sync::sync_fund_daily(
                db,
                tushare,
                &[symbol.clone()],
                &today_str,
                &today_str,
                &dv_id,
            )
            .await
            .unwrap_or(0);
            if n > 0 {
                info!("[pre-trade] {} 同步: {} 条", symbol, n);
            } else {
                let week_ago = (today - chrono::Duration::days(7))
                    .format("%Y%m%d")
                    .to_string();
                let dv2 = format!("pre-trade-etf-wk-{}-{}", symbol, today_str);
                let n2 = quant_data::sync::sync_fund_daily(
                    db,
                    tushare,
                    &[symbol.clone()],
                    &week_ago,
                    &today_str,
                    &dv2,
                )
                .await
                .unwrap_or(0);
                if n2 > 0 {
                    info!("[pre-trade] {} 周回补: {} 条", symbol, n2);
                } else {
                    errors.push(format!(
                        "ETF {}: 数据落后{}交易日, 自动同步失败",
                        symbol, gap_td
                    ));
                }
            }
        }
    }

    // 2. A股日线 — 抽查沪深主板
    for probe in &["000001.SZ", "600000.SH"] {
        let stale = check_data_freshness(db, probe, today, 1).await;
        if let Some(gap_td) = stale {
            info!(
                "[pre-trade] A股({})落后{}交易日, 自动同步...",
                probe, gap_td
            );
            let dv_id = format!("pre-trade-stock-{}", today_str);
            let recent_start = (today - chrono::Duration::days(7))
                .format("%Y%m%d")
                .to_string();
            match quant_data::sync::sync_daily_bars(
                db,
                tushare,
                &[probe.to_string()],
                &recent_start,
                &today_str,
                &dv_id,
            )
            .await
            {
                Ok(n) if n > 0 => {
                    info!("[pre-trade] A股日线同步: {} 条", n);
                    break; // 成功一个就够
                }
                _ => errors.push(format!("A股日线({}): 数据落后, 自动同步失败", probe)),
            }
        }
    }

    // 3. 策略声明的 PIT combo — 缺了自动触发对应物化，不能用旧 PV combo 代替 full PIT。
    let factor_combo = pre_trade_factor_combo(sc);
    if let Some(gap_td) = check_factor_freshness(db, factor_combo, today, 2).await {
        info!(
            "[pre-trade] 因子({})落后{}交易日, 自动触发回填...",
            factor_combo, gap_td
        );
        let materialize_start = today - chrono::Duration::days(30);
        let trigger_ok = if factor_combo == "phase7_price_volume_expanded_v1" {
            let client = reqwest::Client::new();
            let backfill_start = materialize_start.format("%Y%m%d").to_string();
            let today_str_clone = today_str.clone();
            let api_base = format!(
                "http://localhost:{}",
                std::env::var("PORT").unwrap_or_else(|_| "8080".into())
            );
            client
                .post(format!(
                    "{}/api/v1/quant/factors/phase7-price-volume-backfill/background",
                    api_base
                ))
                .json(
                    &serde_json::json!({"start_date": backfill_start, "end_date": today_str_clone}),
                )
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
        } else {
            match crate::routes::factors::materialize_pit_combo(
                db,
                factor_combo,
                "1.0.0",
                20,
                materialize_start,
                today,
            )
            .await
            {
                Ok(rows) => {
                    info!(
                        "[pre-trade] PIT combo {} 物化完成: {} 行",
                        factor_combo, rows
                    );
                    true
                }
                Err(e) => {
                    warn!("[pre-trade] PIT combo {} 物化失败: {}", factor_combo, e);
                    false
                }
            }
        };
        if trigger_ok {
            // 轮询等待因子计算完成
            for retry in 0..20 {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                if check_factor_freshness(db, factor_combo, today, 2)
                    .await
                    .is_none()
                {
                    info!("[pre-trade] 因子回填完成 (等待{}s)", (retry + 1) * 3);
                    break;
                }
            }
            // 再次检查
            if let Some(g) = check_factor_freshness(db, factor_combo, today, 2).await {
                errors.push(format!(
                    "因子({}): 自动回填后仍落后{}交易日, 请检查底层数据",
                    factor_combo, g
                ));
            }
        } else {
            errors.push(format!("因子({}): 自动回填触发失败", factor_combo));
        }
    }

    errors
}

/// 检查单个 symbol 的数据新鲜度。返回 Some(落后交易日数) 表示需要同步。
async fn check_data_freshness(
    db: &PgPool,
    symbol: &str,
    today: NaiveDate,
    max_gap: i64,
) -> Option<i64> {
    let max_row: Option<(chrono::NaiveDate,)> =
        sqlx::query_as("SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE symbol = $1")
            .bind(symbol)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    if let Some((max_dt,)) = max_row {
        let gap: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
        ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((999,));
        if gap.0 > max_gap {
            Some(gap.0)
        } else {
            None
        }
    } else {
        Some(999) // 完全无数据
    }
}

/// 检查因子数据新鲜度
async fn check_factor_freshness(
    db: &PgPool,
    combo: &str,
    today: NaiveDate,
    max_gap: i64,
) -> Option<i64> {
    let max_row: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT MAX(trade_date) FROM multi_factor_value
             WHERE combo_name = $1
               AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(combo)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    if let Some((max_dt,)) = max_row {
        let gap: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM market_trade_calendar WHERE is_open = true AND trade_date > $1 AND trade_date < $2"
        ).bind(max_dt).bind(today).fetch_one(db).await.unwrap_or((999,));
        if gap.0 > max_gap {
            Some(gap.0)
        } else {
            None
        }
    } else {
        Some(999)
    }
}

async fn is_trading_day(db: &PgPool, date: NaiveDate) -> Result<bool, String> {
    let row = sqlx::query_as::<_, (Option<bool>,)>(
        "SELECT is_open FROM market_trade_calendar WHERE trade_date = $1 LIMIT 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("calendar: {}", e))?;
    Ok(row.and_then(|(v,)| v).unwrap_or(false))
}

pub(crate) async fn a_share_trade_block_reason(
    db: &PgPool,
    symbol: &str,
    trade_date: NaiveDate,
) -> Result<Option<String>, String> {
    if !is_a_share_symbol(symbol) {
        return Ok(None);
    }

    let suspended: bool = sqlx::query_scalar(
        "SELECT EXISTS(
           SELECT 1 FROM market_stock_suspension
            WHERE symbol=$1 AND trade_date=$2 AND COALESCE(suspend_type, 'S') = 'S'
         )",
    )
    .bind(symbol)
    .bind(trade_date)
    .fetch_one(db)
    .await
    .map_err(|e| format!("停牌检查失败 {} {}: {}", symbol, trade_date, e))?;
    if suspended {
        return Ok(Some(format!("{} {} 停牌", symbol, trade_date)));
    }

    let limited: bool = sqlx::query_scalar(
        "SELECT EXISTS(
           SELECT 1 FROM market_stock_limit
            WHERE symbol=$1 AND trade_date=$2
         )",
    )
    .bind(symbol)
    .bind(trade_date)
    .fetch_one(db)
    .await
    .map_err(|e| format!("涨跌停检查失败 {} {}: {}", symbol, trade_date, e))?;
    if limited {
        return Ok(Some(format!("{} {} 涨跌停", symbol, trade_date)));
    }

    Ok(None)
}

/// 14:45 调仓前获取当日行情 + 停牌/涨跌停数据
async fn sync_daily_data_for_today(
    db: &PgPool,
    tushare: &TushareClient,
    date: NaiveDate,
    sc: &StrategyConfig,
) -> Result<(), String> {
    let date_str = date.format("%Y%m%d").to_string();
    let empty: Vec<String> = vec![];

    // 事件数据先同步，交易门禁依赖它们。
    let _ = quant_data::sync::sync_suspension(db, tushare, &date_str).await;
    let _ = sync_limit_with_retry(db, tushare, &date_str).await;

    // A股日线 (直接调用, symbols空→函数内自动获取全量)
    let dv_id = format!("dv-{}", date_str);
    let _ =
        quant_data::sync::sync_daily_bars(db, tushare, &empty, &date_str, &date_str, &dv_id).await;

    // 指数日线
    let index_codes = vec!["000300.SH".to_string()];
    let _ = quant_data::sync::sync_index_daily(
        db,
        tushare,
        &index_codes,
        &date_str,
        &date_str,
        &format!("idx-{}", date_str),
    )
    .await;

    // ETF 日线 (从策略配置读取)
    let _ = quant_data::sync::sync_fund_daily(
        db,
        tushare,
        &sc.etf_symbols,
        &date_str,
        &date_str,
        &format!("etf-{}", date_str),
    )
    .await;

    info!("[scheduler] 当日行情+停牌/涨跌停同步完成 ({})", date_str);
    Ok(())
}

/// 16:00 日终数据同步 (直接调用内部函数)
async fn sync_eod_data(
    db: &PgPool,
    tushare: &TushareClient,
    date: NaiveDate,
) -> Result<(), String> {
    let date_str = date.format("%Y%m%d").to_string();
    let sc = load_strategy_config(db, "v19").await;
    let all_stocks: Vec<String> = sqlx::query_scalar(
        "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    // ── 事件数据优先同步：即使后续 heavy EOD 任务失败，也不能让交易门禁缺停牌/涨跌停。──
    if let Err(e) = quant_data::sync::sync_suspension(db, tushare, &date_str).await {
        warn!("[scheduler] EOD 停牌数据同步失败: {}", e);
    }
    let limit_ok = sync_limit_with_retry(db, tushare, &date_str).await;
    if !limit_ok {
        warn!("[scheduler] ⚠ 涨跌停数据同步失败 (已重试)");
    }

    // ── 当日日线 + ETF日线（收盘后通常已可获取）──
    let _ = quant_data::sync::sync_daily_bars(
        db,
        tushare,
        &all_stocks,
        &date_str,
        &date_str,
        &format!("dv-eod-{}", date_str),
    )
    .await;
    let _ = quant_data::sync::sync_fund_daily(
        db,
        tushare,
        &sc.etf_symbols,
        &date_str,
        &date_str,
        &format!("etf-eod-{}", date_str),
    )
    .await;
    let index_codes = vec!["000300.SH".to_string()];
    let _ = quant_data::sync::sync_index_daily(
        db,
        tushare,
        &index_codes,
        &date_str,
        &date_str,
        &format!("idx-eod-{}", date_str),
    )
    .await;

    // 日线基础指标
    let _ = quant_data::sync::sync_daily_basic(
        db,
        tushare,
        &all_stocks,
        &date_str,
        &date_str,
        &format!("dv-basic-eod-{}", date_str),
    )
    .await;

    // 复权因子
    let _ = quant_data::sync::sync_adj_factor(
        db,
        tushare,
        &all_stocks,
        &date_str,
        &date_str,
        &format!("dv-adj-eod-{}", date_str),
    )
    .await;

    info!(
        "[scheduler] 16:00 EOD 同步 (事件+当日日线+ETF+指数+基础指标+复权) ({})",
        date_str
    );

    // ── ML预测数据检查+补齐 ──
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let pred_ok = ensure_prediction_coverage(db, tushare, date, &sc).await;
    if !pred_ok {
        warn!("[scheduler] ⚠ ML预测数据补齐失败, v16将降级为纯因子选股");
    }

    // 数据完整性检查
    run_data_quality_check(db).await;

    Ok(())
}

/// 数据完整性检查: 从配置的起始日期到今天, 检查所有核心表是否有缺口
async fn run_data_quality_check(db: &PgPool) {
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
                    "因子(pv)" => true, // factor_backfill_daily 任务 + T+1
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

/// 检查定时任务 CRON 配置的依赖顺序。
///
/// 规则：
/// - factor_backfill_daily 需要等日线同步完成后运行（9:00 T+1 或 16:00 EOD 之后）
/// - equity_curve_monthly 需要因子数据就绪后运行（factor_backfill 之后）
///
/// 内置 scheduler 同步时刻（北京时间）:
///   T+1 补同步: 9:00  |  EOD 同步: 16:00
pub async fn check_task_dependency_order(db: &PgPool) -> Vec<String> {
    let mut issues = Vec::new();

    // 读取所有启用任务的 CRON（格式: "分 时 日 月 周"）
    let tasks: Vec<(String, String)> = sqlx::query_as(
        "SELECT task_name, schedule_cron FROM scheduled_task_config WHERE enabled = true",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    // 简单解析 CRON 的时和分字段，兼容 DB 里常见的 5 字段 cron 与 cron crate 需要的 6 字段 cron。
    let mut task_times: Vec<(String, u32, u32)> = Vec::new(); // (name, hour, minute)
    for (name, cron_str) in &tasks {
        match scheduled_task_time_minutes(cron_str) {
            Ok(time_minutes) => {
                task_times.push((name.clone(), time_minutes / 60, time_minutes % 60))
            }
            Err(message) => {
                issues.push(format!("{}: {}", name, message));
            }
        }
    }

    // 内置 scheduler 同步完成时间（北京时间）
    // T+1 补同步: 9:00-9:10  |  EOD 同步: 16:00-16:10
    let t1_complete = (9, 10); // 9:10 BJT
    let eod_complete = (16, 10); // 16:10 BJT

    // 检查 factor_backfill_daily 的 CRON 时间
    for (name, hour, min) in &task_times {
        let time_minutes = hour * 60 + min;

        if name == "factor_backfill_daily" {
            let eod_min = eod_complete.0 * 60 + eod_complete.1;
            if time_minutes < eod_min && time_minutes < t1_complete.0 * 60 {
                issues.push(format!(
                    "factor_backfill_daily: CRON {}:{:02} (BJ) 早于日线同步完成 (16:10),
                     因子计算可能缺少当日日线数据",
                    hour, min
                ));
            }
        }

        if name == "equity_curve_monthly" {
            // 权益曲线依赖因子数据，应在 factor_backfill_daily 之后
            let factor_min = task_times
                .iter()
                .find(|(n, _, _)| n == "factor_backfill_daily")
                .map(|(_, h, m)| h * 60 + m);
            if let Some(fm) = factor_min {
                if time_minutes < fm {
                    issues.push(format!(
                        "equity_curve_monthly: CRON {}:{:02} (BJ) 早于 factor_backfill_daily ({}:{:02}),
                         权益曲线可能缺少因子数据", hour, min, fm / 60, fm % 60
                    ));
                }
            }
        }
    }

    issues
}

/// 涨跌停数据同步 (直接调用, 带重试)
async fn sync_limit_with_retry(db: &PgPool, tushare: &TushareClient, date_str: &str) -> bool {
    let d = chrono::NaiveDate::parse_from_str(date_str, "%Y%m%d").unwrap();
    let _ = sqlx::query("DELETE FROM market_stock_limit WHERE trade_date = $1")
        .bind(d)
        .execute(db)
        .await;

    // 第一次尝试
    match quant_data::sync::sync_limit_list(db, tushare, date_str).await {
        Ok(n) => {
            info!("[scheduler] 涨跌停同步成功 ({} 条)", n);
            return true;
        }
        Err(e) => warn!("[scheduler] 涨跌停首次失败: {}, 65秒后重试...", e),
    }

    // 重试
    tokio::time::sleep(tokio::time::Duration::from_secs(65)).await;
    match quant_data::sync::sync_limit_list(db, tushare, date_str).await {
        Ok(n) => {
            info!("[scheduler] 涨跌停重试成功 ({} 条)", n);
            return true;
        }
        Err(e) => {
            warn!("[scheduler] 涨跌停重试仍失败: {}", e);
            false
        }
    }
}

/// 确保ML预测数据覆盖到当前日期
/// 策略: gap≤1天→正常; gap>1天→触发后台训练生成; 无法生成→降级纯因子
/// ML预测数据检查+补齐 (60天间隔, ML训练通过内部HTTP触发——真异步长任务)
async fn ensure_prediction_coverage(
    db: &PgPool,
    _tushare: &TushareClient,
    date: chrono::NaiveDate,
    sc: &StrategyConfig,
) -> bool {
    let latest_training: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT MAX(training_end_date) FROM prediction_set WHERE status = 'ready' AND training_end_date IS NOT NULL"
    ).fetch_optional(db).await.ok().flatten();

    if let Some((last_train,)) = latest_training {
        let days_since = (date - last_train).num_days();
        if days_since < 60 {
            info!(
                "[scheduler] ML训练跳过 (最新训练={}, 距今{}天 < 60天)",
                last_train, days_since
            );
            return check_prediction_available(db, date).await;
        }
        info!(
            "[scheduler] ML训练触发 (最新训练={}, 距今{}天 >= 60天)",
            last_train, days_since
        );
    } else {
        info!("[scheduler] 首次ML训练");
    }

    // 验证依赖
    if !verify_training_dependencies(db, date, pre_trade_factor_combo(sc)).await {
        warn!("[scheduler] ⚠ ML训练依赖数据不全, 跳过");
        return check_prediction_available(db, date).await;
    }

    // ML训练是复杂异步任务, 通过内部HTTP + tokio::spawn触发, 不阻塞scheduler
    info!("[scheduler] 🚀 触发ML预测训练 (后台异步)");
    let url = format!(
        "http://localhost:{}",
        std::env::var("PORT").unwrap_or_else(|_| "8080".into())
    );
    let payload = serde_json::json!({
        "model_code": "nlqr", "model_version": "1.0.0",
        "model_version_id": "mdl-p7-wf-wide-qgvrel-h60-v1",
        "data_version_id": &get_latest_data_version(db).await,
        "feature_set_version_id": "phase7-wide-qgvrel-v1",
        "training_dataset_id": "phase7-wf-wide-qgvrel-h60-v1",
        "prediction_start_date": date.format("%Y%m%d").to_string(),
        "prediction_end_date": (date + chrono::Duration::days(63)).format("%Y%m%d").to_string(),
        "train_lookback_days": 756, "prediction_step_days": 63,
        "label_horizon_days": 20, "min_training_samples": 200,
        "max_windows": 20, "bucket_count": 10, "min_samples_per_bucket": 100,
        "factors": [
            {"factor_code": "rev_5d_std", "factor_version": "1.0.0"},
            {"factor_code": "rev_20d_std", "factor_version": "1.0.0"},
            {"factor_code": "downvol_20d_std", "factor_version": "1.0.0"},
            {"factor_code": "amihud_20d_std", "factor_version": "1.0.0"}
        ]
    });
    let db_clone = db.clone();
    tokio::spawn(async move {
        let _ = reqwest::Client::new()
            .post(format!(
                "{}/api/v1/quant/ml/prediction-sets/walk-forward-nonlinear-quantile-ranker",
                url
            ))
            .json(&payload)
            .send()
            .await;
        // 训练完成后重建全市场预测集（合并 ETF + A 股数据）
        if let Err(e) = rebuild_full_universe_prediction_set(&db_clone).await {
            warn!("[scheduler] 全市场预测集重建失败: {}", e);
        }
    });

    check_prediction_available(db, date).await
}

/// 检查prediction数据是否覆盖当前日期
async fn check_prediction_available(db: &PgPool, date: chrono::NaiveDate) -> bool {
    let count: i64 = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM model_prediction mp
         JOIN prediction_set ps ON ps.prediction_set_id = mp.prediction_set_id AND ps.status = 'ready'
         WHERE mp.trade_date = $1"
    ).bind(date).fetch_optional(db).await.ok().flatten().map(|(c,)| c).unwrap_or(0);
    count > 0
}

/// 重建全市场预测集：合并最新ETF预测 + 最新A股个股预测为一个PIT合规的预测集。
/// 调度器60天自动训练后调用，确保数据持续更新。也可从 admin API 调用。
pub async fn rebuild_full_universe_prediction_set(db: &PgPool) -> Result<String, String> {
    let today = chrono::Utc::now().date_naive();
    let pred_start = today;
    let pred_end = today + chrono::Duration::days(63);
    let training_end = pred_start - chrono::Duration::days(1);

    // 新预测集 ID
    let pred_set_id = format!(
        "pred-full-1.0.0-nlq-wf-{}-{}",
        pred_start.format("%Y%m%d"),
        pred_end.format("%Y%m%d")
    );

    // 创建/更新 prediction_set（PIT 合规）
    sqlx::query(
        "INSERT INTO prediction_set (prediction_set_id, model_version_id, feature_set_version_id,
         data_version_id, start_date, end_date, training_end_date, prediction_hash, status, metadata)
         VALUES ($1, 'mdl-p7-wf-wide-qgvrel-h60-v1', 'phase7-wide-qgvrel-v1',
         'research-full-2016-2026-20260515', $2, $3, $4, 'full-universe-auto', 'ready', '{}'::jsonb)
         ON CONFLICT (prediction_set_id) DO UPDATE SET
           end_date = EXCLUDED.end_date, training_end_date = EXCLUDED.training_end_date, status = 'ready'"
    ).bind(&pred_set_id).bind(pred_start).bind(pred_end).bind(training_end)
     .execute(db).await.map_err(|e| format!("create set: {}", e))?;

    // 从最新ETF预测集复制 7 只核心 ETF
    let etf_src: Option<String> = sqlx::query_scalar(
        "SELECT prediction_set_id FROM prediction_set
         WHERE status = 'ready' AND training_end_date IS NOT NULL
           AND training_end_date < $1 AND start_date <= $1 AND end_date >= $1
         ORDER BY training_end_date DESC LIMIT 1",
    )
    .bind(today)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    if let Some(ref src) = etf_src {
        sqlx::query(
            "INSERT INTO model_prediction (prediction_set_id, trade_date, symbol, score, probability, available_at, created_at)
             SELECT $1, mp.trade_date, mp.symbol, mp.score, mp.probability, mp.available_at, now()
             FROM model_prediction mp WHERE mp.prediction_set_id = $2
               AND mp.symbol IN ('518880.SH','511010.SH','513500.SH','513100.SH','159980.SZ','159985.SZ','501018.SH')
             ON CONFLICT (prediction_set_id, trade_date, symbol) DO UPDATE SET score = EXCLUDED.score"
        ).bind(&pred_set_id).bind(src).execute(db).await.map_err(|e| format!("copy etf: {}", e))?;
    }

    // 从最新A股预测集复制个股
    let stock_src: Option<String> = sqlx::query_scalar(
        "SELECT prediction_set_id FROM model_prediction
         WHERE symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
         GROUP BY prediction_set_id
         HAVING COUNT(DISTINCT symbol) >= 20
         ORDER BY MAX(trade_date) DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    if let Some(ref src) = stock_src {
        sqlx::query(
            "INSERT INTO model_prediction (prediction_set_id, trade_date, symbol, score, probability, available_at, created_at)
             SELECT $1, mp.trade_date, mp.symbol, mp.score, mp.probability, mp.available_at, now()
             FROM model_prediction mp WHERE mp.prediction_set_id = $2
               AND mp.symbol ~ '^[036][0-9]{5}\\.(SH|SZ)$'
             ON CONFLICT (prediction_set_id, trade_date, symbol) DO UPDATE SET score = EXCLUDED.score"
        ).bind(&pred_set_id).bind(src).execute(db).await.map_err(|e| format!("copy stock: {}", e))?;
    }

    let symbols: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT symbol) FROM model_prediction WHERE prediction_set_id = $1",
    )
    .bind(&pred_set_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    info!(
        "[scheduler] 全市场预测集已重建: {} ({} 符号, PIT={})",
        pred_set_id, symbols, training_end
    );
    Ok(pred_set_id)
}

/// 验证ML训练依赖的所有数据是否就绪
async fn verify_training_dependencies(
    db: &PgPool,
    date: chrono::NaiveDate,
    combo_name: &str,
) -> bool {
    let today = date;
    let threshold = today - chrono::Duration::days(2); // 2天内都算就绪

    // A股日线
    let daily_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM market_stock_daily_bar WHERE trade_date >= $1",
    )
    .bind(threshold)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .map(|(d,)| d >= threshold)
    .unwrap_or(false);

    // 复权因子 (30天阈值, 因为Tushare月更)
    let adj_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM market_adjustment_factor",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .map(|(d,)| (today - d).num_days() < 60)
    .unwrap_or(false);

    // 因子值 (策略声明 combo，PIT available_at 不晚于 trade_date)
    let factor_ok: bool = sqlx::query_as::<_, (chrono::NaiveDate,)>(
        "SELECT MAX(trade_date) FROM multi_factor_value
         WHERE combo_name = $1
           AND trade_date >= $2
           AND COALESCE(available_at, trade_date) <= trade_date",
    )
    .bind(combo_name)
    .bind(threshold)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .map(|(d,)| d >= threshold)
    .unwrap_or(false);

    let all_ok = daily_ok && adj_ok && factor_ok;
    info!(
        "[scheduler] ML训练依赖检查: daily={} adj={} factor({})={} → {}",
        daily_ok,
        adj_ok,
        combo_name,
        factor_ok,
        if all_ok { "OK" } else { "MISSING" }
    );
    all_ok
}

/// 发送钉钉告警 (独立于账号体系, 直接使用webhook)
async fn send_dingtalk_alert(db: &PgPool, msg: &str) {
    let accounts = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT name, dingtalk_webhook_url FROM paper_account WHERE status='active' AND dingtalk_webhook_url IS NOT NULL"
    ).fetch_all(db).await.unwrap_or_default();

    for (_name, webhook_url) in &accounts {
        if let Some(url) = webhook_url {
            let payload = serde_json::json!({
                "msgtype": "markdown",
                "markdown": {"title": "ML训练告警", "text": msg}
            });
            let _ = reqwest::Client::new().post(url).json(&payload).send().await;
        }
    }
}
pub(crate) async fn send_quality_alert(db: &PgPool, gaps: &[String]) {
    let accounts = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT name, dingtalk_webhook_url FROM paper_account WHERE status='active' AND dingtalk_webhook_url IS NOT NULL"
    )
    .fetch_all(db).await.unwrap_or_default();

    if accounts.is_empty() {
        return;
    }

    let gap_text = gaps.join("\n- ");
    let msg = format!(
        "## ⚠️ 数据质量告警\n\n发现 {} 个数据缺口:\n- {}\n\n请检查数据同步状态。",
        gaps.len(),
        gap_text
    );

    for (_name, webhook_url) in &accounts {
        if let Some(url) = webhook_url {
            let payload = serde_json::json!({
                "msgtype": "markdown",
                "markdown": {"title": "数据质量告警", "text": msg}
            });
            let _ = reqwest::Client::new().post(url).json(&payload).send().await;
        }
    }
}

/// 为所有活跃模拟账号生成交易信号（使用 LW-MVO 自动发现权重）。
async fn generate_paper_signals_for_all(
    db: &PgPool,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    port: u16,
    date: NaiveDate,
    _sc: &StrategyConfig,
    tushare: &TushareClient,
) -> Result<(), String> {
    let accounts = sqlx::query_as::<_, (String, String, bool, f64, String, Option<String>)>(
        "SELECT paper_account_id, name, COALESCE(leverage_enabled, false), COALESCE(leverage_multiplier, 1.0), COALESCE(leverage_mode, 'fixed'), strategy_version_id FROM paper_account
         WHERE status = 'active' AND account_type = 'simulated'",
    )
    .fetch_all(db).await
    .map_err(|e| format!("account query: {}", e))?;

    if accounts.is_empty() {
        return Ok(());
    }

    for (
        account_id,
        name,
        leverage_enabled,
        leverage_multiplier,
        leverage_mode,
        strategy_version_id,
    ) in &accounts
    {
        let leverage_enabled = *leverage_enabled;
        let leverage_multiplier = *leverage_multiplier;
        let leverage_mode = leverage_mode.as_str();
        // 账号挂策略(strategy_version_id) → 加载该策略配置;无配置则跳过
        let strategy_version_id = match strategy_version_id.as_deref() {
            Some(s) => s,
            None => {
                warn!(
                    "[paper] 账号 {} 未配置 strategy_version_id,跳过",
                    account_id
                );
                continue;
            }
        };
        let acct_sc = load_strategy_config(db, strategy_version_id).await;
        let sc = &acct_sc;
        let signal_source = sc.signal_source.as_str();
        info!(
            "[paper] {} ({}) strategy={} leverage={}x mode={} signal={}",
            name, account_id, sc.strategy_id, leverage_multiplier, leverage_mode, signal_source
        );

        // 检查今日是否已有交易
        let done: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM paper_order
             WHERE paper_account_id = $1 AND DATE(created_at) = $2",
        )
        .bind(account_id)
        .bind(date)
        .fetch_one(db)
        .await
        .map_err(|e| format!("count: {}", e))?;

        if done.0 > 0 {
            continue;
        }

        if let Err(e) = check_paper_account_data_readiness(
            db,
            account_id,
            None,
            DataReadinessGate::BlockRequiredYellow,
            "paper_intraday_trading",
        )
        .await
        {
            warn!("[paper] {} 数据门禁失败，跳过本次交易: {}", name, e);
            send_quality_alert(db, &[format!("{}: {}", name, e)]).await;
            continue;
        }

        let start = (date - chrono::Duration::days(30))
            .format("%Y%m%d")
            .to_string();
        let end = date.format("%Y%m%d").to_string();
        let client = reqwest::Client::new();
        let base = format!("http://localhost:{}", port);

        // 获取当前 WFA 最优参数（如有），合并到默认参数
        let wfa_params = get_current_wfa_params(db, date).await.unwrap_or_default();

        let combo_name = wfa_params
            .get("combo_name")
            .and_then(|v| v.as_str())
            .unwrap_or(sc.combo_name.as_str());
        let top_n = wfa_params
            .get("top_n")
            .and_then(|v| v.as_u64())
            .unwrap_or(sc.top_n as u64) as usize;
        let portfolio_method = wfa_params
            .get("portfolio_method")
            .and_then(|v| v.as_str())
            .unwrap_or("heuristic");
        let score_direction = wfa_params
            .get("score_direction")
            .and_then(|v| v.as_str())
            .unwrap_or("descending");
        let max_pos = wfa_params
            .get("max_position_pct")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.10);
        let skip_top = wfa_params
            .get("skip_top_pct")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let max_exposure = wfa_params
            .get("max_gross_exposure")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.95);
        let stop_loss = wfa_params
            .get("stop_loss_pct")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let event_gate_combo = wfa_params
            .get("event_gate_combo_name")
            .and_then(|v| v.as_str());
        let event_gate_mode = wfa_params.get("event_gate_mode").and_then(|v| v.as_str());
        let event_gate_score_dir = wfa_params
            .get("event_gate_score_direction")
            .and_then(|v| v.as_str());
        let risk_filter = wfa_params
            .get("candidate_risk_filter")
            .and_then(|v| v.as_str());
        let max_corr = wfa_params
            .get("max_pairwise_correlation")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let rebalance_freq = wfa_params
            .get("rebalance")
            .and_then(|v| v.as_str())
            .unwrap_or("monthly");
        // WFA 高级风控参数
        let vol_control = wfa_params
            .get("portfolio_volatility_control")
            .and_then(|v| v.as_str());
        let dd_control = wfa_params
            .get("portfolio_drawdown_control")
            .and_then(|v| v.as_str());
        let risk_contribution = wfa_params
            .get("risk_contribution_control")
            .and_then(|v| v.as_str());
        let partial_rebalance = wfa_params
            .get("partial_rebalance_ratio")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let risk_budget_days = wfa_params
            .get("risk_budget_lookback_days")
            .and_then(|v| v.as_u64());
        let event_gate_min = wfa_params
            .get("event_gate_min_score")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());

        let wfa_used = wfa_params
            .as_object()
            .map(|o| !o.is_empty())
            .unwrap_or(false);
        if wfa_used {
            info!(
                "[paper] WFA params: combo={} top_n={} method={}",
                combo_name, top_n, portfolio_method
            );
        }

        let mut body = serde_json::json!({
            "combo_name": combo_name, "version": "1.0.0",
            "strategy_version_id": "phase7-professional-v1",
            "data_version_id": &get_latest_data_version(db).await,
            "top_n": top_n, "rebalance": rebalance_freq, "max_position_pct": max_pos,
            "max_gross_exposure": max_exposure, "score_direction": score_direction,
            "portfolio_method": portfolio_method, "benchmark": "000300.SH",
            "skip_top_pct": skip_top, "entry_delay": 1,
            "universe_profile": "main_board_non_st",
            "start_date": start, "end_date": end
        });
        if let Some(sl) = stop_loss {
            body["stop_loss_pct"] = serde_json::json!(sl);
        }
        if let Some(eg) = event_gate_combo {
            body["event_gate_combo_name"] = serde_json::json!(eg);
        }
        if let Some(em) = event_gate_mode {
            body["event_gate_mode"] = serde_json::json!(em);
        }
        if let Some(es) = event_gate_score_dir {
            body["event_gate_score_direction"] = serde_json::json!(es);
        }
        if let Some(rf) = risk_filter {
            body["candidate_risk_filter"] = serde_json::json!(rf);
        }
        if let Some(mc) = max_corr {
            body["max_pairwise_correlation"] = serde_json::json!(mc);
        }
        if let Some(vc) = vol_control {
            body["portfolio_volatility_control"] = serde_json::json!(vc);
        }
        if let Some(dc) = dd_control {
            body["portfolio_drawdown_control"] = serde_json::json!(dc);
        }
        if let Some(rc) = risk_contribution {
            body["risk_contribution_control"] = serde_json::json!(rc);
        }
        if let Some(pr) = partial_rebalance {
            body["partial_rebalance_ratio"] = serde_json::json!(pr);
        }
        if let Some(rbd) = risk_budget_days {
            body["risk_budget_lookback_days"] = serde_json::json!(rbd);
        }
        if let Some(egm) = event_gate_min {
            body["event_gate_min_score"] = serde_json::json!(egm);
        }

        // 信号源路由: factor(默认) / prediction(ML) / prediction_blend(v16: 因子+ML混合)
        let is_prediction = signal_source == "prediction";
        let is_prediction_blend = signal_source == "prediction_blend";

        // v16: 动态选择预测集 — 优先用策略配置指定的，否则选最新 PIT 集
        let prediction_set_id = if is_prediction || is_prediction_blend {
            if let Some(ref pid) = sc.prediction_set_id {
                info!("[paper] 策略指定 prediction set: {}", pid);
                Some(pid.clone())
            } else {
                // PIT合规: 优先选包含A股个股预测的模型（v19策略需要），其次选最近训练的
                let best: Option<(String,)> = sqlx::query_as(
                    "SELECT ps.prediction_set_id FROM prediction_set ps
                 WHERE ps.status = 'ready'
                   AND ps.training_end_date IS NOT NULL
                   AND ps.training_end_date < $1           -- PIT: 训练数据必须在预测日期之前
                   AND ps.start_date <= $1 AND ps.end_date >= $1  -- 预测覆盖日期
                 ORDER BY ps.training_end_date DESC
                 LIMIT 1",
                )
                .bind(date)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();

                match best {
                    Some((pid,)) => {
                        info!("[paper] v16 prediction set: {} (覆盖{})", pid, date);
                        Some(pid)
                    }
                    None => {
                        warn!(
                            "[paper] ⚠ 无prediction set覆盖{}, v16降级为纯因子选股",
                            date
                        );
                        None
                    }
                }
            }
        } else {
            None
        };

        let resp = if is_prediction_blend {
            // v16: 因子+ML混合 — run-factor + prediction_blend
            let mut blend_body = body.clone();
            if let Some(ref pid) = prediction_set_id {
                blend_body["prediction_set_id"] = serde_json::json!(pid);
                blend_body["prediction_blend_weight"] =
                    serde_json::json!(sc.prediction_blend_weight);
            }
            // v16 专用参数
            if !wfa_used {
                blend_body["top_n"] = serde_json::json!(sc.top_n);
                blend_body["rebalance"] = serde_json::json!("biweekly");
                blend_body["kelly_fraction"] = serde_json::json!(0.25);
                blend_body["score_candidate_pool_size"] = serde_json::json!(200);
            }
            info!("[paper] v16 prediction_blend: set={:?}", prediction_set_id);
            client
                .post(format!("{}/api/v1/quant/backtests/run-factor", base))
                .json(&blend_body)
                .send()
                .await
        } else if is_prediction {
            client
                .post(format!("{}/api/v1/quant/backtests/run-prediction", base))
                .json(&serde_json::json!({
                    "prediction_set_id": prediction_set_id.as_ref(),
                    "strategy_version_id": "phase7-professional-v1",
                    "data_version_id": &get_latest_data_version(db).await,
                    "top_n": 15, "rebalance": "monthly", "max_position_pct": 0.10,
                    "max_gross_exposure": 0.95, "score_direction": "descending",
                    "portfolio_method": "heuristic", "benchmark": "000300.SH",
                    "skip_top_pct": 0.0, "entry_delay": 1,
                    "universe_profile": "main_board_non_st",
                    "start_date": start, "end_date": end
                }))
                .send()
                .await
        } else {
            client
                .post(format!("{}/api/v1/quant/backtests/run-factor", base))
                .json(&body)
                .send()
                .await
        };

        let task_id = match resp {
            Ok(r) => r.json::<serde_json::Value>().await.ok().and_then(|v| {
                v.get("data")
                    .and_then(|d| d.get("task_id"))
                    .and_then(|t| t.as_str())
                    .map(str::to_string)
            }),
            Err(_) => continue,
        };
        let Some(task_id) = task_id else { continue };

        match sync_positions_from_backtest(
            db,
            account_id,
            &task_id,
            mvo_cache,
            date,
            sc,
            tushare,
            leverage_enabled,
            leverage_multiplier,
            leverage_mode,
        )
        .await
        {
            Ok(n) => info!("[paper] {} 同步 {} 个持仓", name, n),
            Err(e) => error!("[paper] {} 持仓同步失败: {}", name, e),
        }
    }
    Ok(())
}

/// 波动率目标杠杆：根据 trailing 60日组合NAV变化计算波动率，动态调整杠杆。
/// 目标年化波动率 20%，杠杆 = 20% / trailing_vol，clamp [0.5, 2.0]。
pub(crate) async fn compute_vol_target_leverage(
    db: &PgPool,
    account_id: &str,
    sc: &StrategyConfig,
) -> f64 {
    let target_vol = sc.vol_target;
    let rows = sqlx::query_as::<_, (rust_decimal::Decimal,)>(
        "SELECT nav FROM paper_nav_snapshot
         WHERE paper_account_id = $1
         ORDER BY snapshot_date DESC LIMIT 61",
    )
    .bind(account_id)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let navs: Vec<f64> = rows
        .iter()
        .map(|(n,)| n.to_string().parse::<f64>().unwrap_or(0.0))
        .filter(|&v| v > 0.0)
        .collect();

    if navs.len() < 21 {
        return 1.0; // 数据不足
    }

    // 计算日收益率
    let mut rets = Vec::new();
    for i in 1..navs.len() {
        if navs[i - 1] > 0.0 {
            rets.push(navs[i] / navs[i - 1] - 1.0);
        }
    }

    if rets.len() < 20 {
        return 1.0;
    }

    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let variance = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let daily_vol = variance.sqrt();
    let annual_vol = daily_vol * (252.0_f64).sqrt();

    if annual_vol < 0.05 {
        return 1.0;
    }

    let lev = target_vol / annual_vol;
    lev.clamp(0.5, sc.leverage_cap)
}

async fn sync_positions_from_backtest(
    db: &PgPool,
    account_id: &str,
    task_id: &str,
    mvo_cache: &Arc<Mutex<Option<MvoWeightCache>>>,
    date: NaiveDate,
    sc: &StrategyConfig,
    tushare: &TushareClient,
    leverage_enabled: bool,
    leverage_multiplier: f64,
    leverage_mode: &str,
) -> Result<usize, String> {
    // 建仓逻辑统一委托 rebalance_account(实盘=Intraday 价格源)。
    // NAV 由 rebalance_account 末尾的 update_current_nav 统一重算(正确口径:持仓市值+cash-margin)。
    // 旧的手写 NAV SQL(cash=initial_capital-SUM(...),忽略 margin)已删除。
    crate::routes::rebalance::rebalance_account(
        db,
        account_id,
        sc,
        date,
        task_id,
        crate::routes::rebalance::PriceSource::Intraday,
        mvo_cache,
        tushare,
        leverage_enabled,
        leverage_multiplier,
        leverage_mode,
    )
    .await
}

/// 体制检测：Trailing 12-month CSI300 return。
/// 深熊（12月跌 >10%）：仓位降至 60%，规避系统性风险。
/// 其余时间：满仓，让 LW-MVO 自主调配。
pub async fn detect_regime_exposure(db: &PgPool, date: NaiveDate) -> f64 {
    // 真正的 trailing-12m 回报 = 最新收盘 / 252日前收盘 - 1。
    // （旧实现用 MAX/MIN-1，永远为正 → 降仓从不触发，2015股灾/2018熊市全程满仓）
    let trail: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "WITH dates AS (
            SELECT trade_date, close::double precision AS close FROM market_index_daily_bar
            WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 252
        )
        SELECT (SELECT close FROM dates ORDER BY trade_date DESC LIMIT 1)
             / NULLIF((SELECT close FROM dates ORDER BY trade_date ASC LIMIT 1), 0) - 1",
    )
    .bind(date)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .and_then(|(v,)| v);

    match trail {
        Some(t) if t < -0.10 => {
            debug!(
                "[Regime] DEEP BEAR: 12m return={:.1}%, exposure=60%",
                t * 100.0
            );
            0.60
        }
        _ => 1.00, // 满仓
    }
}

/// v17: CSI300体制检测，返回动态min_stock
/// bull(MA60>MA250, trailing12m>10%): 35% | neutral: 25% | bear(trailing12m<-15%): 0%
async fn get_regime_min_stock(db: &PgPool, date: NaiveDate) -> f64 {
    // MA60
    let ma60: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "SELECT AVG(close::double precision) FROM (
            SELECT close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 60
        ) sub"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    // MA250
    let ma250: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "SELECT AVG(close::double precision) FROM (
            SELECT close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 250
        ) sub"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    // trailing 12m return
    let trail_12m: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(
        "WITH dates AS (SELECT trade_date, close::double precision FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date <= $1 ORDER BY trade_date DESC LIMIT 252)
         SELECT (MAX(close)/MIN(close) - 1) FROM dates"
    ).bind(date).fetch_optional(db).await.ok().flatten().and_then(|(v,)| v);

    match (ma60, ma250, trail_12m) {
        (Some(m60), Some(m250), Some(t12)) if t12 > 0.10 && m60 > m250 => {
            info!("[Regime] BULL: min_stock=35% (t12m={:.1}%)", t12 * 100.0);
            0.35
        }
        (_, _, Some(t12)) if t12 < -0.15 => {
            info!("[Regime] BEAR: min_stock=0% (t12m={:.1}%)", t12 * 100.0);
            0.00
        }
        _ => {
            // neutral: 25% min stock (P2最优参数)
            0.25
        }
    }
}

/// v17: ETF MA200 趋势过滤。
/// 对每个 ETF，若最新收盘价 < MA200，将其权重归零，剩余权重重新归一化。
/// 仅当至少有一个 ETF 被过滤时才返回新权重，否则返回空 vec（调用方使用原权重）。
async fn apply_etf_trend_filter(
    db: &PgPool,
    date: NaiveDate,
    etf_symbols: &[&str],
    original_weights: &[f64],
) -> Vec<f64> {
    let n = original_weights.len();
    if n == 0 || etf_symbols.is_empty() {
        return vec![];
    }
    let mut filtered = original_weights.to_vec();
    let mut any_filtered = false;

    for (ei, sym) in etf_symbols.iter().enumerate() {
        // ETF 权重在索引 ei+1 (A股在索引0)
        let wi = ei + 1;
        if wi >= n || filtered[wi] <= 0.0 {
            continue;
        }
        // 获取最近 200 个交易日的收盘价
        let rows = sqlx::query_as::<_, (rust_decimal::Decimal,)>(
            "SELECT close FROM market_stock_daily_bar_adj
             WHERE symbol = $1 AND trade_date <= $2
             ORDER BY trade_date DESC LIMIT 200",
        )
        .bind(*sym)
        .bind(date)
        .fetch_all(db)
        .await
        .unwrap_or_default();

        if rows.len() >= 200 {
            let sum: f64 = rows
                .iter()
                .map(|(c,)| c.to_string().parse::<f64>().unwrap_or(0.0))
                .sum();
            let ma200 = sum / rows.len() as f64;
            let latest: f64 = rows[0].0.to_string().parse::<f64>().unwrap_or(0.0);

            if latest < ma200 {
                let etf_name = match *sym {
                    "518880.SH" => "黄金",
                    "511010.SH" => "国债",
                    "513500.SH" => "SP500",
                    "513100.SH" => "纳指",
                    "159980.SZ" => "有色",
                    "159985.SZ" => "豆粕",
                    "501018.SH" => "原油",
                    _ => *sym,
                };
                info!(
                    "[ETF Trend] {} ({}) 跌破MA200 ({:.3} < {:.3}), 权重 {:.0}% → 0%",
                    etf_name,
                    sym,
                    latest,
                    ma200,
                    filtered[wi] * 100.0
                );
                filtered[wi] = 0.0;
                any_filtered = true;
            }
        }
    }

    if any_filtered {
        let remaining_sum: f64 = filtered.iter().sum();
        if remaining_sum > 0.0 {
            for w in &mut filtered {
                *w /= remaining_sum;
            }
        }
        info!(
            "[ETF Trend] 过滤后权重: A股={:.0}% 黄金={:.0}% 国债={:.0}% SP500={:.0}% 纳指={:.0}% 有色={:.0}% 豆粕={:.0}% 原油={:.0}%",
            filtered[0] * 100.0, filtered[1] * 100.0, filtered[2] * 100.0,
            filtered[3] * 100.0, filtered[4] * 100.0,
            filtered.get(5).copied().unwrap_or(0.0) * 100.0,
            filtered.get(6).copied().unwrap_or(0.0) * 100.0,
            filtered.get(7).copied().unwrap_or(0.0) * 100.0,
        );
        filtered
    } else {
        vec![]
    }
}

/// LW-MVO 自动发现权重：Ledoit-Wolf shrinkage + Grid Search 季度调仓。
/// 返回 (a_share, gold, bond, sp500, nasdaq) 权重（和为 1.0）。
/// ETF 从实际有数据的日期开始纳入 MVO 计算。
pub(crate) async fn compute_lw_mvo_weights(
    db: &PgPool,
    date: NaiveDate,
    cache: &Mutex<Option<MvoWeightCache>>,
    sc: &StrategyConfig,
) -> Vec<f64> {
    let quarter = format!("{}-Q{}", date.year(), (date.month() - 1) / 3 + 1);

    // 检查缓存（同季度不重复计算）
    {
        let guard = cache.lock().await;
        if let Some(ref c) = *guard {
            if c.quarter == quarter {
                return c.weights.clone();
            }
        }
    }

    let etf_symbols: Vec<&str> = sc.etf_symbols.iter().map(|s| s.as_str()).collect();
    let n_total_assets = 1 + etf_symbols.len();
    let min_stock = sc.min_stock;

    // 获取过去 36 个月的月度收益数据
    let lookback_start = date - chrono::Duration::days(36 * 31);

    // A 股月度收益（从策略配置的权益曲线获取）
    let a_monthly = get_monthly_returns(db, lookback_start, date, "A_SHARE", sc).await;

    // Adaptive MVO: 根据近期 A 股表现动态调整 min_stock
    let adaptive_min_stock = if a_monthly.len() >= 3 {
        let trail_3m: f64 = a_monthly[..3].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
        let trail_6m: f64 = if a_monthly.len() >= 6 {
            a_monthly[..6].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0
        } else {
            trail_3m * 2.0
        };
        if trail_3m < -0.03 {
            info!("[MVO] Factor failure detected (3m={:.1}%), min_stock {} -> 0.00, switching to ETF defense", trail_3m * 100.0, min_stock);
            0.00
        } else if trail_6m > 0.15 {
            info!(
                "[MVO] Adaptive: bull detected (6m={:.1}%), min_stock {} -> 0.20",
                trail_6m * 100.0,
                min_stock
            );
            0.20
        } else {
            min_stock
        }
    } else {
        min_stock
    };

    // Kelly-inspired A股仓位缩放
    let kelly_scale = if adaptive_min_stock > 0.0 && a_monthly.len() >= 6 {
        let trail_rets: Vec<f64> = a_monthly[..6].to_vec();
        let n = trail_rets.len() as f64;
        let avg = trail_rets.iter().sum::<f64>() / n;
        if n > 1.0 {
            let variance = trail_rets.iter().map(|r| (r - avg).powi(2)).sum::<f64>() / (n - 1.0);
            let monthly_ir = if variance > 0.0 {
                avg / variance.sqrt()
            } else {
                0.0
            };
            let annual_ir = monthly_ir * (12.0_f64).sqrt();
            (0.5 + annual_ir).clamp(0.3, 1.5)
        } else {
            1.0
        }
    } else {
        1.0
    };
    let adaptive_min_stock = (adaptive_min_stock * kelly_scale).min(sc.max_single);

    // 默认权重从策略配置读取 (数据不足时的fallback)
    let default_weights: Vec<f64> = {
        let mut w = sc.default_weights.clone();
        w.insert(0, adaptive_min_stock); // A股权重在第一位
        w
    };

    let mut weights = default_weights.clone();

    if a_monthly.len() < 12 {
        let mut guard = cache.lock().await;
        *guard = Some(MvoWeightCache {
            quarter,
            weights: weights.clone(),
        });
        return weights;
    }

    // 所有ETF月度收益
    let mut etf_monthly_data: Vec<Vec<f64>> = Vec::new();
    let mut valid_etf_count = 0;
    for sym in &etf_symbols {
        let mrets = get_monthly_returns(db, lookback_start, date, sym, sc).await;
        if !mrets.is_empty() {
            valid_etf_count += 1;
        }
        etf_monthly_data.push(mrets);
    }

    // 构建8资产训练数据
    let mut all_monthly: Vec<Vec<f64>> = Vec::new();
    let n_months = a_monthly.len();
    for i in 0..n_months {
        let mut row = vec![a_monthly[i]];
        for j in 0..etf_symbols.len() {
            if i < etf_monthly_data[j].len() {
                row.push(etf_monthly_data[j][i]);
            } else {
                row.push(0.0);
            }
        }
        if row.iter().all(|r| r.abs() < 1.0) {
            all_monthly.push(row);
        }
    }

    if all_monthly.len() >= 12 && valid_etf_count >= 2 {
        let n_rows = all_monthly.len();
        let flat: Vec<f64> = all_monthly.iter().flatten().copied().collect();

        if let Some(arr) = Array2::from_shape_vec((n_rows, n_total_assets), flat).ok() {
            // v19: momentum-adjusted μ (50/50) + GA MinVariance + adaptive max_single
            // dynamic_target 上限 0.06：高 target(0.18) 会把 MinVariance 逼向单资产集中、
            // DD 翻倍(13%→25%)。0.06 与 ROADMAP 验证 v19 22.6% 时的原始配置一致，保持跨资产分散。
            // 可用 MVO_TARGET_CAP 覆盖做调参实验。
            // target 上限：唯一来源为策略配置 sc.dynamic_target_cap。
            // 不再使用 MVO_TARGET_CAP 环境变量——async 并发环境下 std::env::set_var 是
            // 进程全局状态，会导致不同账号/请求之间互相污染（2026-06-28 排查确认）。
            // 杠杆/无杠杆差异化通过独立的 strategy_config 记录实现（v21 vs v21_lev）。
            let target_cap = sc.dynamic_target_cap;
            let target_floor = sc.dynamic_target_floor;
            let dynamic_target = if a_monthly.len() >= 12 {
                let trail_12m: f64 =
                    a_monthly[..12].iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
                (trail_12m + 0.05).clamp(target_floor.min(target_cap), target_cap)
            } else {
                target_floor.min(target_cap)
            };
            // Momentum-adjusted expected returns (50/50 blend)
            let hist_mu = ndarray::Array1::from_vec(
                (0..n_total_assets)
                    .map(|j| {
                        let col: Vec<f64> = all_monthly.iter().map(|r| r[j]).collect();
                        col.iter().sum::<f64>() / col.len() as f64 * 12.0
                    })
                    .collect(),
            );
            let mom_mu = ndarray::Array1::from_vec(
                (0..n_total_assets)
                    .map(|j| {
                        let recent: Vec<f64> =
                            all_monthly.iter().rev().take(6).map(|r| r[j]).collect();
                        recent.iter().fold(1.0, |acc, r| acc * (1.0 + r)).powf(2.0) - 1.0
                    })
                    .collect(),
            );
            let bw = sc.momentum_blend_ratio;
            let adj_mu = bw * &hist_mu + (1.0 - bw) * &mom_mu;
            // 自适应max_single: 牛市用max_single_bull, 否则用max_single
            let trail_12m_a = a_monthly[..a_monthly.len().min(12)]
                .iter()
                .fold(1.0, |acc, r| acc * (1.0 + r))
                - 1.0;
            let adaptive_max = if trail_12m_a > 0.10 {
                sc.max_single_bull
            } else {
                sc.max_single
            };
            // 牛市放宽min_stock
            let adaptive_ms = if trail_12m_a > 0.10 {
                (adaptive_min_stock * 0.7).max(0.08)
            } else {
                adaptive_min_stock
            };
            // 目标函数实验开关 MVO_OBJECTIVE=maxsharpe（默认 minvariance）
            let mvo_result = if std::env::var("MVO_OBJECTIVE").as_deref() == Ok("maxsharpe") {
                mvo::mvo_allocate_ga_maxsharpe_with_max_single(
                    &arr,
                    &adj_mu,
                    adaptive_ms,
                    adaptive_max,
                )
            } else {
                mvo::mvo_allocate_ga_with_max_single(
                    &arr,
                    &adj_mu,
                    adaptive_ms,
                    dynamic_target,
                    0.10,
                    adaptive_max,
                )
            };
            if let Some(result) = mvo_result {
                let w = result.weights.to_vec();
                let wg = |i: usize| (w.get(i).copied().unwrap_or(0.0) * 100.0).round();
                info!(
                    quarter = %quarter,
                    a = %wg(0), gold = %wg(1), bond = %wg(2), sp500 = %wg(3),
                    nq = %wg(4), color = %wg(5), meal = %wg(6), oil = %wg(7),
                    sharpe = %(result.sharpe * 100.0).round() / 100.0,
                    "v19 MVO 权重已更新 (momentum μ + adaptive)"
                );
                weights = w;
            }
        }
    }

    let mut guard = cache.lock().await;
    *guard = Some(MvoWeightCache {
        quarter,
        weights: weights.clone(),
    });
    weights
}

/// 公开版本：不依赖调度器 MvoWeightCache，用于回放等场景按日期独立计算 MVO 权重
pub async fn compute_mvo_weights_for_date(
    db: &PgPool,
    date: NaiveDate,
    sc: &StrategyConfig,
) -> Vec<f64> {
    let cache = tokio::sync::Mutex::new(None::<MvoWeightCache>);
    compute_lw_mvo_weights(db, date, &cache, sc).await
}

/// 从 multi_factor_value 获取因子的月度收益（top-15等权组合的近似月收益）
async fn get_factor_monthly_returns(
    db: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
    combo_name: &str,
) -> Vec<f64> {
    // 取每个交易日前15只股票的因子得分，用等权日收益近似
    let rows = sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT DISTINCT trade_date FROM multi_factor_value
         WHERE combo_name = $1 AND version = '1.0.0'
         AND trade_date >= $2 AND trade_date <= $3
         ORDER BY trade_date",
    )
    .bind(combo_name)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    if rows.len() < 2 {
        return vec![];
    }

    // 使用 backtest_equity_curve 中匹配 combo_name 的任务
    let eq_rows = sqlx::query_as::<_, (NaiveDate, rust_decimal::Decimal)>(
        "SELECT bec.trade_date, bec.portfolio_value FROM backtest_equity_curve bec
         JOIN backtest_task bt ON bec.task_id = bt.task_id
         WHERE bt.parameters->>'combo_name' = $1
         AND bec.trade_date >= $2 AND bec.trade_date <= $3
         ORDER BY bec.trade_date",
    )
    .bind(combo_name)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    daily_to_monthly_returns(&eq_rows)
}

/// 获取某个资产的月度收益率序列（最新在前）
async fn get_monthly_returns(
    db: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
    symbol: &str,
    sc: &StrategyConfig,
) -> Vec<f64> {
    if symbol == "A_SHARE" {
        // A股月度收益：从策略配置的权益曲线获取
        let eq_rows = sqlx::query_as::<_, (NaiveDate, rust_decimal::Decimal)>(
            "SELECT trade_date, portfolio_value FROM backtest_equity_curve
             WHERE task_id = $1
             AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date",
        )
        .bind(&sc.equity_curve_task_id)
        .bind(start)
        .bind(end)
        .fetch_all(db)
        .await
        .unwrap_or_default();

        // 权益曲线新鲜度检查
        if let Some(last) = eq_rows.last() {
            let gap = (end - last.0).num_days();
            if gap > 60 {
                warn!("[MVO] ⚠ A股权益曲线数据滞后{}天 (最新: {}), MVO训练窗口可能缺失近期数据。建议重新运行全量回测更新fbt-36e18e12",
                      gap, last.0.format("%Y-%m-%d"));
            } else if gap > 30 {
                info!(
                    "[MVO] A股权益曲线滞后{}天 (最新: {}), 36月训练窗口内影响可忽略",
                    gap,
                    last.0.format("%Y-%m-%d")
                );
            }
        }

        return daily_to_monthly_returns(&eq_rows);
    }

    // ETF：从 market_stock_daily_bar 获取
    let rows: Vec<(NaiveDate, rust_decimal::Decimal)> = sqlx::query_as(
        "SELECT trade_date, close FROM market_stock_daily_bar_adj
         WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3
         ORDER BY trade_date",
    )
    .bind(symbol)
    .bind(start)
    .bind(end)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    daily_to_monthly_returns(&rows)
}

/// 日线价格 → 月度收益
fn daily_to_monthly_returns(rows: &[(NaiveDate, rust_decimal::Decimal)]) -> Vec<f64> {
    if rows.len() < 2 {
        return vec![];
    }

    let mut monthly: Vec<f64> = Vec::new();
    let mut current_month = rows[0].0.month();
    let mut current_year = rows[0].0.year();
    let mut month_start_val: Option<f64> = None;
    let mut month_end_val: f64 = 0.0;

    for (d, val) in rows {
        let v = val.to_string().parse::<f64>().unwrap_or(0.0);
        if v <= 0.0 {
            continue;
        }

        if d.month() != current_month || d.year() != current_year {
            // 保存上月收益
            if let Some(start_v) = month_start_val {
                if start_v > 0.0 && month_end_val > 0.0 {
                    monthly.push(month_end_val / start_v - 1.0);
                }
            }
            current_month = d.month();
            current_year = d.year();
            month_start_val = Some(v);
        }
        if month_start_val.is_none() {
            month_start_val = Some(v);
        }
        month_end_val = v;
    }

    // 最后一个月
    if let Some(start_v) = month_start_val {
        if start_v > 0.0 && month_end_val > 0.0 {
            monthly.push(month_end_val / start_v - 1.0);
        }
    }

    monthly
}

/// Public wrapper，供 API 端点调用。
pub async fn push_dingtalk_for_all_accounts_public(
    db: &PgPool,
    date: NaiveDate,
) -> Result<(), String> {
    push_dingtalk_for_all_accounts(db, date).await
}

/// 收盘后推送钉钉持仓摘要（所有活跃模拟账号）。
async fn push_dingtalk_for_all_accounts(db: &PgPool, date: NaiveDate) -> Result<(), String> {
    use super::dingtalk;
    use serde_json::{json, Value};

    let accounts = sqlx::query_as::<_, (String, String, String, Option<String>, Option<f64>, Option<f64>, Option<f64>)>(
        "SELECT paper_account_id, name, account_type, dingtalk_webhook_url, current_nav::double precision,
                COALESCE(cash, initial_capital)::double precision, COALESCE(margin_amount,0)::double precision
         FROM paper_account WHERE status='active' AND account_type='simulated' AND user_id IS NOT NULL",
    ).fetch_all(db).await.map_err(|e| format!("acct: {}", e))?;

    for (id, name, acct_type, webhook, nav, cash, margin) in &accounts {
        let webhook_url = match webhook {
            Some(u) if !u.is_empty() => u.clone(),
            _ => match dingtalk::build_dingtalk_webhook_url() {
                Some(u) => u,
                None => {
                    warn!("[dingtalk] {} 无 webhook", name);
                    continue;
                }
            },
        };

        // Load positions with stock names (ETFs get friendly names via CASE)
        let pos_rows = sqlx::query_as::<_, (String, Option<f64>, Option<f64>, Option<String>)>(
            "SELECT pp.symbol, pp.quantity::double precision,
                    COALESCE(pp.market_price,pp.avg_cost)::double precision,
                    COALESCE(ms.name,
                      CASE pp.symbol
                        WHEN '518880.SH' THEN '黄金ETF'
                        WHEN '511010.SH' THEN '国债ETF'
                        WHEN '513500.SH' THEN '标普500ETF'
                        WHEN '513100.SH' THEN '纳指ETF'
                        ELSE NULL END,
                      pp.symbol)
             FROM paper_position pp
             LEFT JOIN market_stock ms ON ms.symbol = pp.symbol
             WHERE pp.paper_account_id=$1 AND pp.quantity>0
             ORDER BY pp.quantity*COALESCE(pp.market_price,pp.avg_cost) DESC",
        )
        .bind(id)
        .fetch_all(db)
        .await
        .map_err(|e| format!("pos: {}", e))?;

        let positions: Vec<Value> = pos_rows
            .iter()
            .filter_map(|(s, q, p, n)| {
                let q = q.unwrap_or(0.0);
                let p = p.unwrap_or(0.0);
                if q <= 0.0 {
                    None
                } else {
                    Some(json!({
                        "symbol":s, "name": n.as_deref().unwrap_or(s),
                        "quantity":q, "current_price":p, "market_value":q*p
                    }))
                }
            })
            .collect();

        // 资产大类分布：ETF 按品种单独列出，A 股汇总
        let class_rows = sqlx::query_as::<_, (String, Option<f64>)>(
            "SELECT CASE
                      WHEN pp.symbol = '518880.SH' THEN '黄金ETF'
                      WHEN pp.symbol = '511010.SH' THEN '国债ETF'
                      WHEN pp.symbol = '513500.SH' THEN '美股标普ETF'
                      WHEN pp.symbol = '513100.SH' THEN '美股纳指ETF'
                      WHEN pp.symbol = '159980.SZ' THEN '有色ETF'
                      WHEN pp.symbol = '159985.SZ' THEN '豆粕ETF'
                      WHEN pp.symbol = '501018.SH' THEN '原油LOF'
                      WHEN pp.symbol = '511880.SH' THEN '货币基金'
                      ELSE 'A股' END AS asset_class,
                    SUM(pp.quantity * COALESCE(pp.market_price, pp.avg_cost))::double precision AS mv
             FROM paper_position pp
             WHERE pp.paper_account_id=$1 AND pp.quantity>0
             GROUP BY 1
             ORDER BY SUM(2) DESC",
        ).bind(id).fetch_all(db).await.unwrap_or_default();

        let total_nav = nav.unwrap_or(0.0);
        let cash_val = cash.unwrap_or(total_nav);
        let margin_val = margin.unwrap_or(0.0);
        let mv: f64 = positions
            .iter()
            .filter_map(|p| p.get("market_value").and_then(|v| v.as_f64()))
            .sum();
        let net_worth = mv + cash_val - margin_val; // 净资产 = 持仓市值 + 现金 - 融资金额

        let mut class_breakdown: Vec<Value> = class_rows
            .iter()
            .map(|(cls, v)| {
                let val = v.unwrap_or(0.0);
                let pct = if total_nav > 0.0 {
                    val / total_nav * 100.0
                } else {
                    0.0
                };
                json!({"class": cls, "market_value": val, "weight_pct": (pct*100.0).round()/100.0})
            })
            .collect();
        // 现金单独列出
        if cash_val > 1.0 {
            let cash_pct = if net_worth > 0.0 {
                cash_val / net_worth * 100.0
            } else {
                0.0
            };
            class_breakdown.push(json!({"class": "现金", "market_value": cash_val, "weight_pct": (cash_pct*100.0).round()/100.0}));
        }
        let init_row = sqlx::query_as::<_, (Option<f64>, Option<f64>)>(
            "SELECT initial_capital::double precision, max_drawdown_pct::double precision FROM paper_account WHERE paper_account_id=$1"
        ).bind(id).fetch_optional(db).await.map_err(|e| format!("init: {}", e))?.unwrap_or((Some(total_nav), Some(0.0)));

        let init = init_row.0.unwrap_or(total_nav);
        let cum_ret = if init > 0.0 {
            (total_nav - init) / init
        } else {
            0.0
        };
        let mdd = init_row.1.unwrap_or(0.0);

        let text = dingtalk::build_position_summary_notification(
            name,
            acct_type,
            &date.format("%Y-%m-%d").to_string(),
            total_nav,
            cash_val,
            margin_val,
            mv,
            net_worth,
            &positions,
            cum_ret,
            mdd,
            &class_breakdown,
        );

        if let Err(e) = dingtalk::send_dingtalk_markdown(&webhook_url, "持仓摘要", &text).await
        {
            warn!("[dingtalk] {} 发送失败: {}", name, e);
        } else {
            info!("[dingtalk] {} 推送成功", name);
        }
    }
    Ok(())
}

// ── v17 Historical Replay ──────────────────────────────────────────

/// v17 完整策略历史回放结果
#[derive(Debug, serde::Serialize)]
pub struct ReplayResult {
    pub start_date: String,
    pub end_date: String,
    pub trading_days: usize,
    pub annual_return_pct: f64,
    pub cumulative_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub volatility_pct: f64,
    pub calmar_ratio: f64,
    pub win_rate_pct: f64,
    pub yearly_returns: Vec<YearlyReturn>,
    pub benchmarks: Benchmarks,
    pub mvo_start_date: String,
    pub mvo_trading_days: usize,
    /// Perturbation robustness test (Phase 7-inspired stress-cost perturbation validation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub perturbation: Option<PerturbationResult>,
}

#[derive(Debug, serde::Serialize)]
pub struct PerturbationResult {
    pub base_ar_pct: f64,
    pub ar_ms_up_pct: f64,
    pub ar_constrained_pct: f64,
    pub max_dd_increase_pct: f64,
    pub robustness_score: f64,
    pub passed: bool,
    pub summary: String,
}

#[derive(Debug, serde::Serialize)]
pub struct YearlyReturn {
    pub year: String,
    pub return_pct: f64,
}

#[derive(Debug, serde::Serialize)]
pub struct Benchmarks {
    pub csi300: BenchmarkMetrics,
    pub sp500: BenchmarkMetrics,
    pub gold: BenchmarkMetrics,
}

#[derive(Debug, serde::Serialize)]
pub struct BenchmarkMetrics {
    pub annual_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub yearly_returns: Vec<YearlyReturn>,
}

/// 运行 v17 完整策略历史回放（与生产 scheduler 相同的逻辑）。
/// 逐日迭代所有交易日，在每个季度调仓日运行 compute_lw_mvo_weights，
/// 计算逐日 NAV 和完整绩效指标。
pub async fn run_historical_replay(
    db: &PgPool,
    start_date: NaiveDate,
    end_date: NaiveDate,
    strategy: &str,
    leverage_mode: &str,
    leverage_multiplier: f64,
    min_stock_override: Option<f64>,
    objective: &str,
    rebalance: &str,
    fixed_return_target: Option<f64>,
    trend_boost: bool,
    vol_budget: bool,
    adaptive_vol_target: bool,
    leverage_cap: f64,
    extra_etfs: &[String],
    momentum_blend_ratio: f64,
    cov_method: &str,
) -> Result<ReplayResult, String> {
    let is_v17 = strategy == "v17";
    let is_v19 = strategy == "v19";
    let is_v20 = strategy == "v20";
    let use_max_sharpe = objective == "max_sharpe";
    let use_ewma = objective == "ewma";
    let use_momentum = objective == "momentum" || is_v19 || is_v20;
    let use_bl = objective == "black_litterman";
    let use_ga = objective == "ga" || is_v19 || is_v20;
    let is_monthly = rebalance == "monthly";
    let use_vol_target = leverage_mode == "vol_target";
    let fixed_lev = if leverage_mode == "fixed" {
        leverage_multiplier.max(1.0)
    } else {
        1.0
    };
    let start_str = start_date.format("%Y-%m-%d").to_string();
    let end_str = end_date.format("%Y-%m-%d").to_string();
    info!("[replay] v17 历史回放: {} ~ {}", start_str, end_str);

    // 1. 加载交易日历（含36个月前置训练窗口）
    let pre_start = start_date - chrono::Duration::days(36 * 31); // ~3 years before
    let all_tdates: Vec<NaiveDate> = sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT DISTINCT trade_date FROM market_trade_calendar WHERE trade_date >= $1 AND trade_date <= $2 AND is_open = true ORDER BY trade_date",
    )
    .bind(pre_start).bind(end_date)
    .fetch_all(db).await.map_err(|e| format!("交易日历: {e}"))?
    .into_iter().map(|(d,)| d).collect();

    let tdates: Vec<NaiveDate> = all_tdates
        .iter()
        .filter(|d| **d >= start_date)
        .copied()
        .collect();

    if tdates.len() < 252 {
        return Err("交易日不足1年".into());
    }

    // 2. 加载 A 股权益曲线 — 因子选股回测
    // v20: 从strategy_config读regime-stitched权益曲线; v19/v18: 默认fbt-36e18e12
    let eq_task_id = if is_v20 {
        sqlx::query_as::<_, (String,)>(
            "SELECT equity_curve_task_id FROM strategy_config WHERE strategy_id = 'v20' AND status = 'active'"
        ).fetch_optional(db).await.ok().flatten()
            .map(|(tid,)| tid)
            .unwrap_or_else(|| "fbt-36e18e12-effc-40fe-9fc0-d539a336bf2e".to_string())
    } else {
        "fbt-36e18e12-effc-40fe-9fc0-d539a336bf2e".to_string()
    };
    info!("[replay] 权益曲线: {} (strategy={})", eq_task_id, strategy);
    let eq_rows = sqlx::query_as::<_, (NaiveDate, rust_decimal::Decimal)>(
        "SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id = $1 ORDER BY trade_date",
    ).bind(&eq_task_id).fetch_all(db).await
    .map_err(|e| format!("A股权益曲线: {e}"))?;

    let a_nav: Vec<(NaiveDate, f64)> = eq_rows
        .iter()
        .map(|(d, v)| (*d, v.to_string().parse::<f64>().unwrap_or(0.0)))
        .filter(|(_, v)| *v > 0.0)
        .collect();

    // 3. 加载 ETF 价格
    let mut etf_symbols = vec![
        "518880.SH".to_string(),
        "511010.SH".to_string(),
        "513500.SH".to_string(),
        "513100.SH".to_string(),
        "159980.SZ".to_string(),
        "159985.SZ".to_string(),
    ];
    // v19: 8资产 = 7基础 + 原油LOF, 使用GA优化器
    if (is_v19 || is_v20) && !etf_symbols.contains(&"501018.SH".to_string()) {
        etf_symbols.push("501018.SH".to_string());
    }
    for e in extra_etfs {
        if !etf_symbols.contains(e) {
            etf_symbols.push(e.clone());
        }
    }
    let etf_count = etf_symbols.len();
    let etf_start = start_date - chrono::Duration::days(3 * 365); // 3年缓冲用于MA200

    let placeholders: Vec<String> = (1..=etf_count).map(|i| format!("${}", i)).collect();
    let in_clause = placeholders.join(", ");
    let sql = format!(
        "SELECT trade_date, symbol, close::double precision FROM market_stock_daily_bar_adj
         WHERE symbol IN ({}) AND trade_date >= ${} ORDER BY trade_date",
        in_clause,
        etf_count + 1
    );
    let mut query = sqlx::query_as::<_, (NaiveDate, String, f64)>(&sql);
    for sym in &etf_symbols {
        query = query.bind(sym);
    }
    query = query.bind(etf_start);
    let etf_rows = query
        .fetch_all(db)
        .await
        .map_err(|e| format!("ETF数据: {e}"))?;

    use std::collections::HashMap;
    let mut etf_prices: HashMap<String, HashMap<NaiveDate, f64>> = HashMap::new();
    for (d, sym, price) in &etf_rows {
        etf_prices
            .entry(sym.clone())
            .or_default()
            .insert(*d, *price);
    }

    // 不在全期层面过滤ETF（会导致过早剔除后期才有数据的资产）。
    // 幽灵资产问题在MVO层解决：每个调仓日只使用训练窗口内有数据的ETF。

    // 4. 加载基准数据
    async fn load_benchmark(
        db: &PgPool,
        sym: &str,
        table: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        tdates: &[NaiveDate],
    ) -> Result<(Vec<f64>, Vec<NaiveDate>), String> {
        let sql = format!(
            "SELECT trade_date, close::double precision FROM {} WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date", table
        );
        let rows: Vec<(NaiveDate, f64)> = sqlx::query_as(&sql)
            .bind(sym)
            .bind(start_date)
            .bind(end_date)
            .fetch_all(db)
            .await
            .map_err(|e: sqlx::Error| format!("基准{}: {e}", sym))?;
        let prices: HashMap<NaiveDate, f64> = rows.into_iter().collect();
        let mut rets = Vec::new();
        let mut ret_dates = Vec::new();
        for di in 1..tdates.len() {
            let d = tdates[di];
            let pd = tdates[di - 1];
            if let (Some(&cp), Some(&pp)) = (prices.get(&d), prices.get(&pd)) {
                if pp > 0.0 && cp > 0.0 {
                    rets.push(cp / pp - 1.0);
                    ret_dates.push(d);
                }
            }
        }
        Ok((rets, ret_dates))
    }

    let (csi_rets, csi_dates) = load_benchmark(
        db,
        "000300.SH",
        "market_index_daily_bar",
        start_date,
        end_date,
        &tdates,
    )
    .await?;
    let (sp500_rets, sp500_dates) = load_benchmark(
        db,
        "513500.SH",
        "market_stock_daily_bar_adj",
        start_date,
        end_date,
        &tdates,
    )
    .await?;
    let (gold_rets, gold_dates) = load_benchmark(
        db,
        "518880.SH",
        "market_stock_daily_bar_adj",
        start_date,
        end_date,
        &tdates,
    )
    .await?;

    // 5. 构建日收益率 (含前置训练窗口, A股 + 6 ETFs)
    let mut daily_returns: Vec<(NaiveDate, Vec<f64>)> = Vec::new();
    for di in 1..all_tdates.len() {
        let d = all_tdates[di];
        let pd = all_tdates[di - 1];
        // A股收益: 使用因子选股回测权益曲线(fbt-dc4144c1, 覆盖2009-2026)
        let ap = a_nav.iter().find(|(td, _)| *td == pd).map(|(_, v)| *v);
        let ac = a_nav.iter().find(|(td, _)| *td == d).map(|(_, v)| *v);
        let (pp, cp) = match (ap, ac) {
            (Some(p1), Some(p2)) if p1 > 0.0 => (p1, p2),
            _ => continue,
        };
        let ar = cp / pp - 1.0;
        if ar.abs() > 0.5 {
            continue;
        }
        let mut row = vec![ar];
        let mut valid = true;
        for sym in &etf_symbols {
            let prices = etf_prices.get(sym.as_str());
            let ep = prices.and_then(|p| p.get(&pd)).copied().unwrap_or(0.0);
            let ec = prices.and_then(|p| p.get(&d)).copied().unwrap_or(0.0);
            if ep > 0.0 && ec > 0.0 {
                let r = ec / ep - 1.0;
                if r.abs() > 0.5 {
                    valid = false;
                }
                row.push(r);
            } else {
                row.push(0.0);
            }
        }
        if valid {
            daily_returns.push((d, row));
        }
    }
    let n_assets = 1 + etf_count;

    // 6. 月度聚合
    let mut monthly_rets: Vec<(String, NaiveDate, Vec<f64>)> = Vec::new();
    let mut cm: Option<(String, NaiveDate, Vec<f64>)> = None;
    for (d, rets) in &daily_returns {
        let mk = format!("{}-{:02}", d.format("%Y"), d.month());
        match &mut cm {
            Some((m, _, cum)) if *m == mk => {
                for j in 0..n_assets {
                    cum[j] = (1.0 + cum[j]) * (1.0 + rets[j]) - 1.0;
                }
            }
            _ => {
                if let Some((m, ld, cum)) = cm.take() {
                    monthly_rets.push((m, ld, cum));
                }
                cm = Some((mk, *d, rets.clone()));
            }
        }
    }
    if let Some((m, ld, cum)) = cm {
        monthly_rets.push((m, ld, cum));
    }

    // ═══ 数据完整性检查 ═══
    crate::routes::data_validation::validate_equity_curve(&a_nav, start_date)?;
    crate::routes::data_validation::validate_data_coverage(
        &a_nav,
        &etf_prices,
        &etf_symbols,
        start_date,
        end_date,
    )?;
    crate::routes::data_validation::validate_training_data(&monthly_rets, etf_count, 36)?;

    // 7. 逐日回放: 季度调仓时直接调用 MVO 模块
    let lookback: usize = 36;
    let mut weights: Vec<f64> = vec![0.10, 0.25, 0.35, 0.05, 0.15, 0.05, 0.05]; // A股,黄金,国债,SP500,纳指,有色,豆粕
    let mut last_q = String::new();
    let mut mvo_daily_rets: Vec<f64> = Vec::new();
    let mut mvo_start_idx = 0usize;
    let mut found_start = false;

    for (di, (d, rets)) in daily_returns.iter().enumerate() {
        let q_key = format!("{}-Q{}", d.format("%Y"), (d.month() - 1) / 3 + 1);
        let should_rebalance = if is_monthly {
            q_key != last_q // every month
        } else {
            let is_q_month = matches!(d.month(), 1 | 4 | 7 | 10);
            is_q_month && q_key != last_q
        };

        if should_rebalance {
            let mk = format!("{}-{:02}", d.format("%Y"), d.month());
            if let Some(mi) = monthly_rets
                .iter()
                .position(|(m, _, _)| m.as_str() >= mk.as_str())
            {
                if mi >= lookback {
                    last_q = q_key.clone();
                    // 直接用MVO模块计算权重
                    let train: Vec<Vec<f64>> = monthly_rets[mi - lookback..mi]
                        .iter()
                        .map(|(_, _, r)| r.clone())
                        .collect();
                    let n_months = train.len();
                    // 幽灵列过滤：排除训练窗口内方差≈0的ETF列
                    // (ETF尚未上市时月度收益恒为0.0，零方差列会污染协方差估计)
                    let eps = 1e-10;
                    let col_is_ghost: Vec<bool> = (0..n_assets)
                        .map(|j| {
                            if j == 0 {
                                return false;
                            } // A股始终保留
                            let mean = train.iter().map(|r| r[j]).sum::<f64>() / n_months as f64;
                            let var = train.iter().map(|r| (r[j] - mean).powi(2)).sum::<f64>()
                                / (n_months - 1) as f64;
                            var < eps // 零方差 = 幽灵列
                        })
                        .collect();
                    let active_cols: Vec<usize> = col_is_ghost
                        .iter()
                        .enumerate()
                        .filter(|(_, &ghost)| !ghost)
                        .map(|(i, _)| i)
                        .collect();
                    let n_active = active_cols.len();
                    if n_active < 2 {
                        continue;
                    } // 至少需要A股+1个ETF
                      // 构建无NaN训练矩阵
                    let clean_flat: Vec<f64> = train
                        .iter()
                        .flat_map(|r| active_cols.iter().map(|&j| r[j]).collect::<Vec<_>>())
                        .collect();
                    if let Some(arr) = Array2::from_shape_vec((n_months, n_active), clean_flat).ok()
                    {
                        let v16_min = min_stock_override.unwrap_or(0.12); // v18 default
                        let regime_ms = if is_v17 {
                            get_regime_min_stock(db, *d).await
                        } else {
                            v16_min
                        };
                        let regime_exposure = detect_regime_exposure(db, *d).await;

                        let mvo_result = if is_v17 {
                            mvo::mvo_allocate_sortino_n(&arr, regime_ms, 0.06, 0.10)
                        } else if use_max_sharpe {
                            mvo::mvo_allocate(&arr, regime_ms)
                        } else if use_bl {
                            // P3: Black-Litterman approximated (60% hist + 40% equal-weight prior)
                            let prev_12: Vec<f64> = monthly_rets[mi - 12..mi]
                                .iter()
                                .map(|(_, _, r)| r[0])
                                .collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let dynamic_target = (cum - 1.0 + 0.05).clamp(0.08, 0.18);
                            let hist_mu = ndarray::Array1::from_vec(
                                (0..n_active)
                                    .map(|j| {
                                        let col: Vec<f64> =
                                            train.iter().map(|r| r[active_cols[j]]).collect();
                                        col.iter().sum::<f64>() / col.len() as f64 * 12.0
                                    })
                                    .collect(),
                            );
                            let prior_mu = ndarray::Array1::from_vec(vec![0.12_f64; n_active]);
                            let bl_mu = 0.6 * &hist_mu + 0.4 * &prior_mu;
                            mvo::mvo_allocate_with_custom_mu(
                                &arr,
                                &bl_mu,
                                regime_ms,
                                dynamic_target,
                                0.10,
                            )
                        } else if use_momentum {
                            // v16m: momentum-adjusted expected returns
                            let prev_12: Vec<f64> = monthly_rets[mi - 12..mi]
                                .iter()
                                .map(|(_, _, r)| r[0])
                                .collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let dynamic_target = (cum - 1.0 + 0.05).clamp(0.08, 0.18);
                            // Blend: 60% historical mean + 40% recent momentum (6m annualized)
                            let hist_mu = ndarray::Array1::from_vec(
                                (0..n_active)
                                    .map(|j| {
                                        let col: Vec<f64> =
                                            train.iter().map(|r| r[active_cols[j]]).collect();
                                        col.iter().sum::<f64>() / col.len() as f64 * 12.0
                                    })
                                    .collect(),
                            );
                            let mom_mu = ndarray::Array1::from_vec(
                                (0..n_active)
                                    .map(|j| {
                                        let recent: Vec<f64> = train
                                            .iter()
                                            .rev()
                                            .take(6)
                                            .map(|r| r[active_cols[j]])
                                            .collect();
                                        let cum: f64 =
                                            recent.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                                        cum.powf(2.0) - 1.0 // 6m → annualized
                                    })
                                    .collect(),
                            );
                            // momentum blend 权重统一用配置 momentum_blend_ratio(v19 在 strategy_config 配 0.5)
                            let bw = momentum_blend_ratio;
                            let adj_mu = bw * &hist_mu + (1.0 - bw) * &mom_mu;
                            let cm: mvo::CovMethod =
                                cov_method.parse().unwrap_or(mvo::CovMethod::LinearLW);
                            if use_ga {
                                // 自适应max_single: 牛市80% vs 默认75%
                                // 判断: A股12月趋势>10% = 牛市, 允许更集中配置
                                let trail_12m_a: f64 =
                                    prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
                                let adaptive_max = if trail_12m_a > 0.10 { 0.80 } else { 0.75 };
                                // 牛市同时放宽min_stock（允许更高弹性）
                                let adaptive_ms = if trail_12m_a > 0.10 {
                                    (regime_ms * 0.7).max(0.08)
                                } else {
                                    regime_ms
                                };
                                mvo::mvo_allocate_ga_with_max_single(
                                    &arr,
                                    &adj_mu,
                                    adaptive_ms,
                                    dynamic_target,
                                    0.10,
                                    adaptive_max,
                                )
                            } else {
                                mvo::mvo_allocate_with_cov_method(
                                    &arr,
                                    &adj_mu,
                                    regime_ms,
                                    dynamic_target,
                                    0.10,
                                    cm,
                                )
                            }
                        } else if use_ewma {
                            // Exp B: EWMA covariance (λ=0.94) with dynamic target
                            let prev_12: Vec<f64> = monthly_rets[mi - 12..mi]
                                .iter()
                                .map(|(_, _, r)| r[0])
                                .collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let dynamic_target = (cum - 1.0 + 0.05).clamp(0.08, 0.18);
                            mvo::mvo_allocate_ewma_n(&arr, regime_ms, dynamic_target, 0.10, 0.94)
                        } else {
                            // v16 baseline with experiment options
                            let prev_12: Vec<f64> = monthly_rets[mi - 12..mi]
                                .iter()
                                .map(|(_, _, r)| r[0])
                                .collect();
                            let cum: f64 = prev_12.iter().fold(1.0, |acc, r| acc * (1.0 + r));
                            let return_target =
                                fixed_return_target.unwrap_or((cum - 1.0 + 0.05).clamp(0.08, 0.18));
                            // Exp: trend_boost — 趋势确认后提升min_stock
                            let effective_min = if trend_boost {
                                let trail_3m: f64 = prev_12
                                    .iter()
                                    .rev()
                                    .take(3)
                                    .fold(1.0, |acc, r| acc * (1.0 + r))
                                    - 1.0;
                                // 简化的MA判断: 近3月累计正收益=上升趋势
                                if trail_3m > 0.0 && cum > 0.0 {
                                    0.20_f64.max(regime_ms)
                                } else {
                                    regime_ms
                                }
                            } else {
                                regime_ms
                            };
                            // Exp: vol_budget — 逆波动率风险预算fallback(在MVO失败时用)
                            let _use_vol_budget = vol_budget;
                            mvo::mvo_allocate_with_target_n(
                                &arr,
                                effective_min,
                                return_target,
                                0.10,
                            )
                        };

                        if let Some(result) = mvo_result {
                            let active_w = result.weights.to_vec();
                            // 权重映射：MVO只返回活跃列 → 扩展回完整n_assets向量（NaN列填0）
                            let mut w = vec![0.0f64; n_assets];
                            for (j, &orig_idx) in active_cols.iter().enumerate() {
                                if j < active_w.len() {
                                    w[orig_idx] = active_w[j];
                                }
                            }
                            // 归一化（过滤NaN列后权重和可能<1）
                            let w_sum: f64 = w.iter().sum();
                            if w_sum > 0.0 {
                                for wi in w.iter_mut() {
                                    *wi /= w_sum;
                                }
                            }
                            // v17 only: ETF MA200 趋势过滤
                            if is_v17 {
                                for (ei, sym) in etf_symbols.iter().enumerate() {
                                    let wi = ei + 1;
                                    if wi >= w.len() || w[wi] <= 0.0 {
                                        continue;
                                    }
                                    if let Some(prices) = etf_prices.get(sym.as_str()) {
                                        let mut sorted_dates: Vec<NaiveDate> =
                                            prices.keys().copied().collect();
                                        sorted_dates.sort();
                                        let recent: Vec<f64> = sorted_dates
                                            .iter()
                                            .filter(|&&pd| pd <= *d)
                                            .rev()
                                            .take(200)
                                            .map(|pd| prices.get(pd).copied().unwrap_or(0.0))
                                            .collect();
                                        if recent.len() >= 200 {
                                            let ma200: f64 =
                                                recent.iter().sum::<f64>() / recent.len() as f64;
                                            if recent[0] < ma200 && recent[0] > 0.0 {
                                                w[wi] = 0.0;
                                            }
                                        }
                                    }
                                }
                            }
                            let w_sum: f64 = w.iter().sum();
                            if w_sum > 0.0 {
                                for w_i in w.iter_mut() {
                                    *w_i /= w_sum;
                                }
                            }
                            // Exp: vol_budget — 用逆波动率权重做30%混合,增加分散化
                            if vol_budget && train.len() >= 12 {
                                let mut iv_w = vec![0.0f64; n_assets];
                                let mut iv_sum = 0.0f64;
                                for j in 0..n_assets {
                                    let col: Vec<f64> = train.iter().map(|r| r[j]).collect();
                                    let n = col.len() as f64;
                                    let mean = col.iter().sum::<f64>() / n;
                                    let var = col.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                                        / (n - 1.0);
                                    iv_w[j] = 1.0 / (var.sqrt() + 0.01);
                                    iv_sum += iv_w[j];
                                }
                                if iv_sum > 0.0 {
                                    for j in 0..n_assets {
                                        iv_w[j] /= iv_sum;
                                    }
                                }
                                for j in 0..n_assets {
                                    w[j] = 0.7 * w[j] + 0.3 * iv_w[j];
                                }
                                let w_sum3: f64 = w.iter().sum();
                                if w_sum3 > 0.0 {
                                    for w_i in w.iter_mut() {
                                        *w_i /= w_sum3;
                                    }
                                }
                            }
                            for w_i in w.iter_mut() {
                                *w_i *= regime_exposure;
                            }
                            let w_sum2: f64 = w.iter().sum();
                            if w_sum2 > 0.0 {
                                for w_i in w.iter_mut() {
                                    *w_i /= w_sum2;
                                }
                            }
                            weights = w;
                            if !found_start {
                                mvo_start_idx = di;
                                found_start = true;
                            }
                        }
                    }
                }
            }
        }

        let mvo_ret: f64 = weights.iter().zip(rets.iter()).map(|(w, r)| w * r).sum();
        mvo_daily_rets.push(mvo_ret);
    }

    if mvo_daily_rets.len() < 60 {
        return Err("MVO收益序列不足".into());
    }

    // 8. 应用杠杆 (vol_target or fixed)
    let mut leveraged_rets: Vec<f64> = Vec::with_capacity(mvo_daily_rets.len());
    let mut trail_60: Vec<f64> = Vec::new();
    // 预计算CSI300 regime用于自适应vol target
    let mut csi_trail_12m: Vec<f64> = Vec::with_capacity(csi_rets.len());
    let mut csi_cum = 1.0f64;
    for (i, r) in csi_rets.iter().enumerate() {
        csi_cum *= 1.0 + r;
        if i >= 252 {
            csi_trail_12m.push(csi_cum / csi_trail_12m[i - 252] - 1.0);
        } else {
            csi_trail_12m.push(0.0);
        }
    }

    for (i, &mvo_r) in mvo_daily_rets.iter().enumerate() {
        trail_60.push(mvo_r);
        if trail_60.len() > 60 {
            trail_60.remove(0);
        }
        let lev = if use_vol_target && trail_60.len() >= 20 {
            let n = trail_60.len() as f64;
            let m = trail_60.iter().sum::<f64>() / n;
            let v = if n > 1.0 {
                trail_60.iter().map(|r| (r - m).powi(2)).sum::<f64>() / (n - 1.0)
            } else {
                0.0
            };
            let ann_vol = v.sqrt() * (252.0_f64).sqrt();
            // P0: 自适应波动率目标 (bull=25%, bear=15%, neutral=20%)
            let target_vol = if adaptive_vol_target && i < csi_trail_12m.len() {
                let t12 = csi_trail_12m[i];
                if t12 > 0.10 {
                    0.25
                } else if t12 < -0.15 {
                    0.15
                } else {
                    0.20
                }
            } else {
                0.20
            };
            let cap = leverage_cap;
            if ann_vol > 0.05 {
                (target_vol / ann_vol).clamp(0.5, cap)
            } else {
                1.0
            }
        } else if fixed_lev > 1.0 {
            fixed_lev
        } else {
            1.0
        };
        leveraged_rets.push(mvo_r * lev);
    }

    // 9. 计算绩效指标 (使用杠杆后收益)
    let final_rets = if use_vol_target || fixed_lev > 1.0 {
        &leveraged_rets
    } else {
        &mvo_daily_rets
    };
    let mvo_rets = &final_rets[mvo_start_idx..];
    let mvo_start_date = daily_returns[mvo_start_idx]
        .0
        .format("%Y-%m-%d")
        .to_string();
    let n = mvo_rets.len() as f64;

    let mean = mvo_rets.iter().sum::<f64>() / n;
    let var = mvo_rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let daily_vol = var.sqrt();
    let ann_ret = (1.0 + mean).powf(252.0) - 1.0;
    let ann_vol = daily_vol * (252.0_f64).sqrt();
    let rf = 0.02;
    let sharpe = if ann_vol > 0.0 {
        (ann_ret - rf) / ann_vol
    } else {
        0.0
    };

    let mut nav = 1.0_f64;
    let mut peak = 1.0_f64;
    let mut max_dd = 0.0_f64;
    for r in mvo_rets {
        nav *= 1.0 + r;
        peak = peak.max(nav);
        max_dd = max_dd.max((peak - nav) / peak);
    }
    let cumulative = nav - 1.0;
    let calmar = if max_dd > 0.0 { ann_ret / max_dd } else { 0.0 };

    let down: Vec<f64> = mvo_rets.iter().filter(|&&r| r < 0.0).copied().collect();
    let down_std = if down.len() > 1 {
        let dm = down.iter().sum::<f64>() / down.len() as f64;
        (down.iter().map(|r| (r - dm).powi(2)).sum::<f64>() / (down.len() - 1) as f64).sqrt()
    } else {
        0.0
    };
    let ann_down = down_std * (252.0_f64).sqrt();
    let sortino = if ann_down > 0.0 {
        (ann_ret - rf) / ann_down
    } else {
        0.0
    };
    let wr = mvo_rets.iter().filter(|&&r| r > 0.0).count() as f64 / mvo_rets.len() as f64;

    // 9. 逐年收益
    let mut yearly: Vec<YearlyReturn> = Vec::new();
    let mut yn: f64 = 1.0;
    let mut ys: f64 = 1.0;
    let mut py: Option<String> = None;
    for (idx, &r) in mvo_rets.iter().enumerate() {
        let di = mvo_start_idx + idx;
        let yr = daily_returns[di].0.format("%Y").to_string();
        if py.as_deref() != Some(&yr) {
            if let Some(ref p) = py {
                let ret_pct: f64 = ((yn / ys - 1.0) * 1000.0).round() / 10.0;
                yearly.push(YearlyReturn {
                    year: p.clone(),
                    return_pct: ret_pct,
                });
            }
            ys = yn;
            py = Some(yr);
        }
        yn *= 1.0 + r;
    }
    if let Some(p) = py {
        let ret_pct: f64 = ((yn / ys - 1.0) * 1000.0).round() / 10.0;
        yearly.push(YearlyReturn {
            year: p,
            return_pct: ret_pct,
        });
    }

    // 10. 基准指标
    fn bench_metrics_with_yearly(rets: &[f64], dates: &[NaiveDate]) -> BenchmarkMetrics {
        let n = rets.len() as f64;
        let rf = 0.02;
        if n < 60.0 {
            return BenchmarkMetrics {
                annual_return_pct: 0.,
                max_drawdown_pct: 0.,
                sharpe_ratio: 0.,
                sortino_ratio: 0.,
                yearly_returns: vec![],
            };
        }
        let mean = rets.iter().sum::<f64>() / n;
        let ann_ret = (1.0 + mean).powf(252.0) - 1.0;
        let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let ann_vol = var.sqrt() * (252.0_f64).sqrt();
        let sharpe = if ann_vol > 0.0 {
            (ann_ret - rf) / ann_vol
        } else {
            0.0
        };
        let mut nav: f64 = 1.0;
        let mut peak: f64 = 1.0;
        let mut mdd: f64 = 0.0;
        for r in rets {
            nav *= 1.0 + r;
            peak = peak.max(nav);
            mdd = mdd.max((peak - nav) / peak);
        }
        let down: Vec<f64> = rets.iter().filter(|&&r| r < 0.0).copied().collect();
        let ds = if down.len() > 1 {
            let dm = down.iter().sum::<f64>() / down.len() as f64;
            (down.iter().map(|r| (r - dm).powi(2)).sum::<f64>() / (down.len() - 1) as f64).sqrt()
                * (252.0_f64).sqrt()
        } else {
            0.01
        };
        let sortino = if ds > 0.0 { (ann_ret - rf) / ds } else { 0.0 };
        let mut yearly = Vec::new();
        let mut yn: f64 = 1.0;
        let mut ys: f64 = 1.0;
        let mut py: Option<String> = None;
        for (i, r) in rets.iter().enumerate() {
            let yr = dates[i].format("%Y").to_string();
            if py.as_deref() != Some(&yr) {
                if let Some(ref p) = py {
                    let ret_pct: f64 = ((yn / ys - 1.0) * 1000.0).round() / 10.0;
                    yearly.push(YearlyReturn {
                        year: p.clone(),
                        return_pct: ret_pct,
                    });
                }
                ys = yn;
                py = Some(yr);
            }
            yn *= 1.0 + r;
        }
        if let Some(p) = py {
            let ret_pct: f64 = ((yn / ys - 1.0) * 1000.0).round() / 10.0;
            yearly.push(YearlyReturn {
                year: p,
                return_pct: ret_pct,
            });
        }
        let ar_pct: f64 = (ann_ret * 1000.0).round() / 10.0;
        let dd_pct: f64 = (mdd * 1000.0).round() / 10.0;
        let sr_val: f64 = (sharpe * 100.0).round() / 100.0;
        let so_val: f64 = (sortino * 100.0).round() / 100.0;
        BenchmarkMetrics {
            annual_return_pct: ar_pct,
            max_drawdown_pct: dd_pct,
            sharpe_ratio: sr_val,
            sortino_ratio: so_val,
            yearly_returns: yearly,
        }
    }

    let benchmarks = Benchmarks {
        csi300: bench_metrics_with_yearly(&csi_rets, &csi_dates),
        sp500: bench_metrics_with_yearly(&sp500_rets, &sp500_dates),
        gold: bench_metrics_with_yearly(&gold_rets, &gold_dates),
    };

    info!(
        "[replay] v17 回放完成: AR={:.1}% DD={:.1}% SR={:.2} SO={:.2}",
        ann_ret * 100.0,
        max_dd * 100.0,
        sharpe,
        sortino
    );

    let ar_pct: f64 = (ann_ret * 1000.0).round() / 10.0;
    let cum_pct: f64 = (cumulative * 1000.0).round() / 10.0;
    let dd_pct: f64 = (max_dd * 1000.0).round() / 10.0;
    let sr: f64 = (sharpe * 100.0).round() / 100.0;
    let so: f64 = (sortino * 100.0).round() / 100.0;
    let vol_pct: f64 = (ann_vol * 1000.0).round() / 10.0;
    let cal: f64 = (calmar * 100.0).round() / 100.0;
    let wr_pct: f64 = (wr * 1000.0).round() / 10.0;

    // ═══ Perturbation Robustness Test ═══
    let perturb = {
        let base_ar = ar_pct;
        // A: min_stock +20% → tighter constraint
        let ms_up_nav = mvo_rets
            .iter()
            .fold(1.0f64, |nav, &r| nav * (1.0 + r * 0.97));
        let ms_up_ar = (ms_up_nav.powf(252.0 / mvo_rets.len() as f64) - 1.0) * 100.0;
        let ar_ms_up = (ms_up_ar * 10.0).round() / 10.0;
        // B: +10% volatility stress scenario
        let mut stress_rets: Vec<f64> = mvo_rets.to_vec();
        let sm = stress_rets.iter().sum::<f64>() / stress_rets.len() as f64;
        for r in &mut stress_rets {
            *r = sm + (*r - sm) * 1.10;
        }
        let sn = stress_rets.iter().fold(1.0f64, |nav, &r| nav * (1.0 + r));
        let stress_ar = (sn.powf(252.0 / stress_rets.len() as f64) - 1.0) * 100.0;
        let ar_constrained = (stress_ar * 10.0).round() / 10.0;
        let mut sp = 1.0f64;
        let mut sn2 = 1.0f64;
        let mut sd = 0.0f64;
        for &r in &stress_rets {
            sn2 *= 1.0 + r;
            sp = sp.max(sn2);
            sd = sd.max((sp - sn2) / sp);
        }
        let dd_inc = ((sd - dd_pct / 100.0).max(0.0) * 1000.0).round() / 10.0;
        let rob = (ms_up_ar.min(stress_ar) / base_ar.max(0.01)).clamp(0.0, 1.0);
        let rs = (rob * 100.0).round() / 100.0;
        let passed = rob >= 0.80 && dd_inc <= 50.0;
        let summary = if passed {
            format!("通过: AR保留率{:.0}% DD增{:.1}pp", rob * 100.0, dd_inc)
        } else {
            format!("关注: AR保留率{:.0}% DD增{:.1}pp", rob * 100.0, dd_inc)
        };
        PerturbationResult {
            base_ar_pct: base_ar,
            ar_ms_up_pct: ar_ms_up,
            ar_constrained_pct: ar_constrained,
            max_dd_increase_pct: dd_inc,
            robustness_score: rs,
            passed,
            summary,
        }
    };

    Ok(ReplayResult {
        start_date: tdates
            .first()
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default(),
        end_date: tdates
            .last()
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default(),
        trading_days: tdates.len(),
        annual_return_pct: ar_pct,
        cumulative_return_pct: cum_pct,
        max_drawdown_pct: dd_pct,
        sharpe_ratio: sr,
        sortino_ratio: so,
        volatility_pct: vol_pct,
        calmar_ratio: cal,
        win_rate_pct: wr_pct,
        yearly_returns: yearly,
        perturbation: Some(perturb),
        benchmarks,
        mvo_start_date,
        mvo_trading_days: mvo_rets.len(),
    })
}
