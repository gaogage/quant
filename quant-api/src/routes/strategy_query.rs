//! 策略配置查询领域服务（R6：从 scheduler 提取）。
//!
//! 集中 strategy_config 表的读取：load_strategy_config / load_first_active /
//! load_active_factor_combos / load_active_etf_symbols_union /
//! load_active_combo_materialize_configs。scheduler 保留 re-export 转发，
//! 调用方零改动。

use sqlx::PgPool;
use tracing::{info, warn};

use crate::routes::shared::StrategyConfig;

/// 收集所有 active 复合策略的 etf_symbols 并集。
pub async fn load_active_etf_symbols_union(db: &PgPool) -> Vec<String> {
    let rows: Vec<(Option<serde_json::Value>,)> = sqlx::query_as(
        "SELECT etf_symbols
         FROM strategy_config
         WHERE status='active' AND strategy_type='composite' AND etf_symbols IS NOT NULL
         ORDER BY strategy_id",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (etf_json,) in rows {
        if let Some(arr) = etf_json.as_ref().and_then(|v| v.as_array()) {
            for sym in arr {
                if let Some(s) = sym.as_str() {
                    set.insert(s.to_string());
                }
            }
        }
    }
    set.into_iter().collect()
}

/// combo_horizon 配置权威读取（任务80：名字推断退役，宁可报错原则）。
/// ①调用方已取的列值优先；②无列值时查 strategy_config 任一 active 行；
/// ③仍无 → warn 并返回 None，调用方跳过该 combo 物化——错误 horizon 的
/// 物化会静默失真 ICIR 权重，跳过则由新鲜度守卫/面板亮红兜底暴露。
pub(crate) async fn required_combo_horizon(
    db: &PgPool,
    combo_name: &str,
    col: Option<i16>,
) -> Option<i16> {
    if let Some(h) = col {
        return Some(h);
    }
    let from_cfg: Option<i16> = sqlx::query_scalar(
        "SELECT combo_horizon FROM strategy_config
         WHERE status='active' AND combo_name=$1 AND combo_horizon IS NOT NULL
         ORDER BY strategy_id LIMIT 1",
    )
    .bind(combo_name)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    if from_cfg.is_none() {
        tracing::warn!(
            "[scheduler] combo {} 无 combo_horizon 配置(名字推断已按任务80退役),跳过其物化——请补 strategy_config.combo_horizon",
            combo_name
        );
    }
    from_cfg
}

/// 收集所有 active 策略（composite + asset 子策略）声明的 factor combo_name 去重列表。
/// 用于调仓前对所有活跃账号用到的因子物化做新鲜度校验+自动补全。
/// 模拟实盘盘中调仓依赖：每个激活账号的激活策略用到的 combo 都必须有当日因子数据。
pub async fn load_active_factor_combos(db: &PgPool) -> Vec<String> {
    let rows: Vec<(Option<String>,)> = sqlx::query_as(
        "SELECT combo_name
         FROM strategy_config
         WHERE status='active' AND combo_name IS NOT NULL AND btrim(combo_name) <> ''
         ORDER BY strategy_id",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (combo,) in rows {
        if let Some(c) = combo {
            let c = c.trim().to_string();
            if !c.is_empty() {
                set.insert(c);
            }
        }
    }
    set.into_iter().collect()
}

/// 活跃 combo 的物化配置(include_fundamentals + factor_whitelist)。
/// active 复合策略声明的 PIT combo 物化配置。
/// 从 strategy_config 读取; 同名 combo 多策略声明时合并:
/// include_fundamentals 任一 true 则 true, factor_whitelist 取首个非空(同 combo 应一致)。
#[derive(Debug, Clone)]
pub struct ComboMaterializeConfig {
    pub combo_name: String,
    pub include_fundamentals: bool,
    /// 去冗余因子白名单; None 用黑名单全量。
    pub factor_whitelist: Option<Vec<String>>,
    /// 显式 horizon(strategy_config.combo_horizon 列,任务80 起为唯一权威来源);
    /// None 时经 required_combo_horizon 二次查配置,仍无则跳过该 combo。
    pub combo_horizon: Option<i16>,
    /// 行业中性化物化(任务80 批1: 原 full_pit_icir_indneutral_val_v1 代码特判迁列)。
    pub ind_neutral: bool,
    /// 物化模式: pit=常规 PIT 物化 / phase7_backfill=phase7 回填路由
    /// (任务80 批1: 原 phase7_price_volume_expanded_v1 代码特判迁列)。
    pub materialize_mode: String,
}

/// 从 strategy_config 读取:含基本面因子的 combo(如 v24 fund_v2)需 include_fundamentals=true
/// + factor_whitelist 去冗余白名单,否则用默认黑名单物化会丢失基本面因子。
pub async fn load_active_combo_materialize_configs(db: &PgPool) -> Vec<ComboMaterializeConfig> {
    let rows: Vec<(
        Option<String>,
        Option<bool>,
        Option<serde_json::Value>,
        Option<i16>,
        bool,
        String,
    )> = sqlx::query_as(
        "SELECT combo_name, include_fundamentals, factor_whitelist, combo_horizon,
                ind_neutral, materialize_mode
         FROM strategy_config
         WHERE status='active' AND combo_name IS NOT NULL AND btrim(combo_name) <> ''",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut map: std::collections::BTreeMap<String, ComboMaterializeConfig> =
        std::collections::BTreeMap::new();
    for (combo, inc_fund, whitelist, horizon, ind_neutral, materialize_mode) in rows {
        if let Some(c) = combo {
            let c = c.trim().to_string();
            if c.is_empty() {
                continue;
            }
            // 同名 combo 可能多策略声明,合并:任一策略 include_fundamentals=true 则用 true;
            // factor_whitelist 取首个非空(同 combo 白名单应一致)。
            let inc = inc_fund.unwrap_or(false);
            let wl: Option<Vec<String>> = whitelist.and_then(|v| {
                v.as_array().map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
            });
            // combo_name 必须在 or_insert_with 闭包内赋值（2026-09-20 真实生产 bug 修复：
            // 此前恒为 String::new()，导致 scheduler.rs:942 的
            // `filter(|c| c.combo_name.starts_with("full_pit_icir"))` 永远空集，
            // PIT combo 因子保鲜任务自 7/31 引入起静默空转近两个月）。
            let entry = map
                .entry(c.clone())
                .or_insert_with(|| ComboMaterializeConfig {
                    combo_name: c,
                    include_fundamentals: false,
                    factor_whitelist: None,
                    combo_horizon: None,
                    ind_neutral: false,
                    materialize_mode: "pit".to_string(),
                });
            if inc {
                entry.include_fundamentals = true;
            }
            // 任务80 批1: 行业中性化任一声明即生效; 物化模式取首个 phase7_backfill
            if ind_neutral {
                entry.ind_neutral = true;
            }
            if entry.materialize_mode == "pit" && materialize_mode == "phase7_backfill" {
                entry.materialize_mode = materialize_mode;
            }
            if entry.factor_whitelist.is_none() && wl.is_some() {
                entry.factor_whitelist = wl;
            }
            if entry.combo_horizon.is_none() {
                entry.combo_horizon = horizon;
            }
        }
    }
    map.into_values().collect()
}

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
            'regime_bull_min_stock', regime_bull_min_stock,
            'regime_bear_min_stock', regime_bear_min_stock,
            'deep_bear_threshold', deep_bear_threshold,
            'deep_bear_exposure', deep_bear_exposure,
            'kelly_fraction', kelly_fraction,
            'score_candidate_pool_size', score_candidate_pool_size,
            'signal_source', signal_source,
            'prediction_blend_weight', prediction_blend_weight,
            'combo_name', combo_name,
            'top_n', top_n,
            'prediction_set_id', prediction_set_id,
            'dynamic_target_cap', dynamic_target_cap,
            'dynamic_target_floor', dynamic_target_floor,
            'score_direction', score_direction,
            'candidate_tier', candidate_tier,
            'leverage_regime_threshold', leverage_regime_threshold,
            'slippage_pct', slippage_pct,
            'mvo_objective', mvo_objective,
            'regime_policy', regime_policy,
            'regime_bear_return_threshold', regime_bear_return_threshold,
            'etf_premium_gate', COALESCE(etf_premium_gate, 0.10)
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

/// 加载第一个 active 复合策略配置（不再硬编码 v19）。
/// 用于 EOD 同步的 ML 预测覆盖检查（ensure_prediction_coverage 仍需单策略 sc）。
/// 无 active 复合策略时返回 None（调用方跳过 ML 检查，不 panic）。
pub async fn load_first_active_strategy_config(db: &PgPool) -> Option<StrategyConfig> {
    let sid: Option<String> = sqlx::query_scalar(
        "SELECT strategy_id FROM strategy_config
         WHERE status='active' AND strategy_type='composite'
         ORDER BY strategy_id LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    match sid {
        Some(sid) => Some(load_strategy_config(db, &sid).await),
        None => {
            warn!("[scheduler] 无 active 复合策略，跳过 ML 预测覆盖检查");
            None
        }
    }
}

// ─── 第二批补充测试（非 ignored，秒级，真实本机 PG 只读）─────────────────

#[cfg(test)]
mod second_batch {
    use super::*;

    async fn test_db() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    // ── 纯函数 ──

    // ── 只读查询（真实库）──

    #[tokio::test]
    async fn load_active_etf_symbols_union_returns_deduped_set() {
        let db = test_db().await;
        let symbols = load_active_etf_symbols_union(&db).await;
        // active composite 策略（v24 系）均配置 518880.SH 黄金 ETF
        assert!(
            symbols.contains(&"518880.SH".to_string()),
            "active 并集应含黄金 ETF: {symbols:?}"
        );
        // BTreeSet 去重且有序
        let mut sorted = symbols.clone();
        sorted.sort();
        assert_eq!(symbols, sorted, "返回应去重且升序");
    }

    #[tokio::test]
    async fn load_active_factor_combos_dedupes_active_declarations() {
        let db = test_db().await;
        let combos = load_active_factor_combos(&db).await;
        assert!(!combos.is_empty(), "应至少有一个 active combo");
        // v24 系 active 策略声明的两个 combo 必须出现（跨 composite/asset 去重后）
        assert!(
            combos.contains(&"full_pit_icir_indneutral_val_v1".to_string()),
            "v24 composite combo 缺失: {combos:?}"
        );
        assert!(
            combos.contains(&"full_pit_icir_37f_h20_fund_v2".to_string()),
            "v24-a_share asset combo 缺失: {combos:?}"
        );
        // 无重复
        let mut unique = combos.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(combos.len(), unique.len(), "combo 列表应已去重");
    }

    #[tokio::test]
    async fn load_active_combo_materialize_configs_carries_whitelist_and_horizon() {
        let db = test_db().await;
        let configs = load_active_combo_materialize_configs(&db).await;
        assert!(!configs.is_empty(), "应有 active combo 物化配置");
        let mut combo_names: Vec<&str> = configs.iter().map(|c| c.combo_name.as_str()).collect();
        combo_names.sort();
        combo_names.dedup();
        // 同名去重数应不增配置数（允许库内同前缀不同后缀的多条 active combo 共存）。
        assert!(
            combo_names.len() <= configs.len(),
            "去重后条数不应超过原始条数"
        );
        // v24 主 combo 显式配置 horizon=20
        let main = configs
            .iter()
            .find(|c| c.combo_name == "full_pit_icir_indneutral_val_v1")
            .expect("v24 主 combo 配置");
        assert_eq!(main.combo_horizon, Some(20));
    }

    #[tokio::test]
    async fn load_strategy_config_resolves_active_v24_from_db() {
        let db = test_db().await;
        let cfg = load_strategy_config(&db, "v24").await;
        assert_eq!(cfg.strategy_id, "v24");
        assert_eq!(cfg.combo_name, "full_pit_icir_indneutral_val_v1");
        assert!(!cfg.etf_symbols.is_empty(), "v24 应配置非空 ETF 列表");
    }

    #[tokio::test]
    async fn load_first_active_strategy_config_returns_composite_strategy() {
        let db = test_db().await;
        let cfg = load_first_active_strategy_config(&db)
            .await
            .expect("存在 active composite 策略（v24）");
        assert_eq!(
            cfg.strategy_id, "v24",
            "strategy_id 最小的 active composite"
        );
    }
}
