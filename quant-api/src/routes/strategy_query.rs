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

/// 从 combo_name 推断 PIT horizon：`full_pit_icir_37f_h20` → 20，无 `_hN` 后缀 → 1。
/// 用于 pit_combo_refresh 遍历所有 active combo 时为每个 combo 取正确 horizon。
pub(crate) fn combo_horizon_from_name(combo: &str) -> i16 {
    if let Some(idx) = combo.rfind("_h") {
        if let Ok(h) = combo[idx + 2..].parse::<i16>() {
            if h > 0 {
                return h;
            }
        }
    }
    1
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
/// 从 strategy_config 读取:含基本面因子的 combo(如 v24 fund_v2)需 include_fundamentals=true
/// + factor_whitelist 去冗余白名单,否则用默认黑名单物化会丢失基本面因子。
/// 返回 (combo_name, include_fundamentals, factor_whitelist) 列表。
pub async fn load_active_combo_materialize_configs(
    db: &PgPool,
) -> Vec<(String, bool, Option<Vec<String>>)> {
    let rows: Vec<(Option<String>, Option<bool>, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT combo_name, include_fundamentals, factor_whitelist
         FROM strategy_config
         WHERE status='active' AND combo_name IS NOT NULL AND btrim(combo_name) <> ''",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    let mut map: std::collections::BTreeMap<String, (bool, Option<Vec<String>>)> =
        std::collections::BTreeMap::new();
    for (combo, inc_fund, whitelist) in rows {
        if let Some(c) = combo {
            let c = c.trim().to_string();
            if c.is_empty() {
                continue;
            }
            // 同名 combo 可能多策略声明,合并:任一策略 include_fundamentals=true 则用 true;
            // factor_whitelist 取首个非空(同 combo 白名单应一致)。
            let inc = inc_fund.unwrap_or(false);
            let wl: Option<Vec<String>> = whitelist.and_then(|v| {
                v.as_array()
                    .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            });
            let entry = map.entry(c).or_insert((false, None));
            if inc {
                entry.0 = true;
            }
            if entry.1.is_none() && wl.is_some() {
                entry.1 = wl;
            }
        }
    }
    map.into_iter()
        .map(|(combo, (inc, wl))| (combo, inc, wl))
        .collect()
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
            'regime_bear_return_threshold', regime_bear_return_threshold
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
