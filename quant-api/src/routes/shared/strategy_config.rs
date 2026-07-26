//! 策略配置桥接模块（DDD Step 6b 从 scheduler.rs 迁出）。
//!
//! 包含：
//! - [`StrategyConfig`]：从 DB 加载的策略配置（运行时缓存）+ 11 个 serde default 回调 + panic 版 Default
//! - [`resolved_to_legacy_sc`] / [`rs_to_legacy_etf_symbols`]：ResolvedStrategy → StrategyConfig 桥接
//!
//! 原位置：scheduler.rs:258-374 / 1057-1109。

use crate::routes::strategy::{AssetClass, ResolvedStrategy};

/// 从数据库加载的策略配置（运行时缓存，启动时加载）
#[allow(dead_code)]
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
    // regime 自适应 A 股最低占比(compute_lw_mvo_weights 牛市/熊市分支使用)。
    // DB strategy_config 表有列,load_strategy_config 读出;resolved_to_legacy_sc 从 MvoParams 映射。
    #[serde(default)]
    pub regime_bull_min_stock: f64,
    #[serde(default)]
    pub regime_bear_min_stock: f64,
    // detect_regime_exposure 深熊降仓阈值/暴露(P1-2 配置化,原硬编码 -0.10/0.60)。
    // trailing-12m 收益 < deep_bear_threshold → 仓位降为 deep_bear_exposure。
    #[serde(default = "default_deep_bear_threshold")]
    pub deep_bear_threshold: f64,
    #[serde(default = "default_deep_bear_exposure")]
    pub deep_bear_exposure: f64,
    // v16 ML blend 策略参数(P1-4 配置化,原硬编码 0.25/200)。
    #[serde(default = "default_kelly_fraction")]
    pub kelly_fraction: f64,
    #[serde(default = "default_score_candidate_pool_size")]
    pub score_candidate_pool_size: i64,
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
    #[serde(default = "default_leverage_regime_threshold")]
    pub leverage_regime_threshold: f64,
    #[serde(default = "default_slippage_pct")]
    pub slippage_pct: f64,
    #[serde(default = "default_mvo_objective")]
    pub mvo_objective: String,
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
fn default_deep_bear_threshold() -> f64 {
    -0.10
}
fn default_deep_bear_exposure() -> f64 {
    0.60
}
fn default_kelly_fraction() -> f64 {
    0.25
}
fn default_score_candidate_pool_size() -> i64 {
    200
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
fn default_leverage_regime_threshold() -> f64 {
    0.9
}
fn default_slippage_pct() -> f64 {
    0.002
}
fn default_mvo_objective() -> String {
    "minvariance".into()
}

impl Default for StrategyConfig {
    fn default() -> Self {
        // 策略配置必须从 strategy_config 表加载,不允许代码硬编码 fallback。
        panic!("StrategyConfig::default() 被调用 — 策略配置必须从 DB 加载,检查 load_strategy_config 调用方");
    }
}

/// 反向桥接:ResolvedStrategy 树 → 平铺 StrategyConfig。
/// 三个 MVO 函数(compute_lw_mvo_weights/compute_mvo_weights_for_date/compute_vol_target_leverage)
/// 本轮不改签名仍接 &StrategyConfig,rebalance_account 接 &ResolvedStrategy 后调此函数得到临时视图传入。
pub(crate) fn resolved_to_legacy_sc(rs: &ResolvedStrategy) -> Result<StrategyConfig, String> {
    let mvo = rs
        .mvo
        .as_ref()
        .ok_or_else(|| format!("策略 {} 缺 mvo 配置", rs.strategy_id))?;
    let a_share = rs
        .assets
        .iter()
        .find(|a| a.asset_class == AssetClass::AShare)
        .ok_or_else(|| format!("策略 {} 缺 a_share asset", rs.strategy_id))?;
    Ok(StrategyConfig {
        strategy_id: rs.strategy_id.clone(),
        name: rs.name.clone(),
        etf_symbols: rs_to_legacy_etf_symbols(rs),
        equity_curve_task_id: a_share.security.equity_curve_task_id.clone().unwrap_or_default(),
        min_stock: mvo.min_stock,
        max_single: a_share.security.max_single,
        max_single_bull: a_share.security.max_single_bull,
        momentum_blend_ratio: mvo.momentum_blend_ratio,
        ga_population: mvo.ga_population,
        ga_generations: mvo.ga_generations,
        vol_target: mvo.vol_target,
        leverage_cap: mvo.leverage_cap,
        default_weights: mvo.default_weights.clone(),
        regime_bull_min_stock: mvo.regime_bull_min_stock,
        regime_bear_min_stock: mvo.regime_bear_min_stock,
        deep_bear_threshold: mvo.deep_bear_threshold,
        deep_bear_exposure: mvo.deep_bear_exposure,
        kelly_fraction: mvo.kelly_fraction,
        score_candidate_pool_size: mvo.score_candidate_pool_size,
        signal_source: a_share.security.signal_source.clone(),
        prediction_blend_weight: a_share.security.prediction_blend_weight,
        combo_name: a_share.security.combo_name.clone(),
        top_n: a_share.security.top_n,
        prediction_set_id: a_share.security.prediction_set_id.clone(),
        dynamic_target_cap: mvo.dynamic_target_cap,
        dynamic_target_floor: mvo.dynamic_target_floor,
        score_direction: a_share.security.score_direction.clone(),
        candidate_tier: a_share.security.candidate_tier.clone(),
        // rebalance 级参数:从 MvoParams 读(P1-3 统一加载机制,消除硬编码默认)。
        // load_resolved_strategy 已从 DB strategy_config 读 leverage_regime_threshold/slippage_pct/mvo_objective。
        leverage_regime_threshold: mvo.leverage_regime_threshold,
        slippage_pct: mvo.slippage_pct,
        mvo_objective: mvo.mvo_objective.clone(),
    })
}

/// 从 ResolvedStrategy 提取 MVO 标准顺序的 7 ETF 列表(MVO 8 维权重的第 1-7 列)。
/// 顺序:[黄金,国债,标普,纳指,有色,豆粕,原油]。
/// ResolvedStrategy.etf_symbols 已在 load 时从 composite 行加载(正确 MVO 顺序),直接 clone。
pub(crate) fn rs_to_legacy_etf_symbols(rs: &ResolvedStrategy) -> Vec<String> {
    rs.etf_symbols.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::strategy::{
        AssetClass, AssetStrategy, MvoParams, ResolvedStrategy, SecurityConfig, StrategyType,
    };

    #[test]
    fn test_resolved_to_legacy_sc_mapping() {
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
                deep_bear_threshold: -0.10,
                deep_bear_exposure: 0.60,
                dynamic_target_cap: 0.30,
                dynamic_target_floor: 0.12,
                risk_free_rate: 0.03,
                grid_step: 0.0,
                leverage_regime_threshold: 0.9,
                slippage_pct: 0.002,
                mvo_objective: "minvariance".into(),
                kelly_fraction: 0.25,
                score_candidate_pool_size: 200,
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
        let sc = resolved_to_legacy_sc(&rs).unwrap();
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

    #[test]
    fn test_resolved_to_legacy_sc_missing_mvo_errors() {
        let rs = ResolvedStrategy {
            strategy_id: "v19".into(),
            name: "v19策略".into(),
            strategy_type: StrategyType::Composite,
            mvo: None,
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
                        fixed_symbols: vec![],
                        default_weights: vec![],
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
            etf_symbols: vec![],
            rebalance_freq: "quarterly".into(),
        };
        let result = resolved_to_legacy_sc(&rs);
        assert!(result.is_err(), "mvo=None 必须报错");
        let err = result.unwrap_err();
        assert!(
            err.contains("缺 mvo 配置"),
            "错误消息应包含 '缺 mvo 配置',实际: {}",
            err
        );
    }

    #[test]
    fn test_resolved_to_legacy_sc_missing_a_share_errors() {
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
                deep_bear_threshold: -0.10,
                deep_bear_exposure: 0.60,
                dynamic_target_cap: 0.30,
                dynamic_target_floor: 0.12,
                risk_free_rate: 0.03,
                grid_step: 0.0,
                leverage_regime_threshold: 0.9,
                slippage_pct: 0.002,
                mvo_objective: "minvariance".into(),
                kelly_fraction: 0.25,
                score_candidate_pool_size: 200,
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
                        fixed_symbols: vec![],
                        default_weights: vec![],
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
                        fixed_symbols: vec![],
                        default_weights: vec![],
                        max_single: 0.75,
                        max_single_bull: 0.80,
                    },
                },
            ],
            // 注意:无 AssetClass::AShare 的 asset
            etf_symbols: vec![],
            rebalance_freq: "quarterly".into(),
        };
        let result = resolved_to_legacy_sc(&rs);
        assert!(result.is_err(), "缺 a_share asset 必须报错");
        let err = result.unwrap_err();
        assert!(
            err.contains("缺 a_share asset"),
            "错误消息应包含 '缺 a_share asset',实际: {}",
            err
        );
    }
}
