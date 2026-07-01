//! 权益曲线多策略同步模块。
//!
//! 消除 scheduler 硬编码 v19:自动同步覆盖所有活跃账号关联策略,
//! 检测 combo 共用去重(v21/v21_lev 共用曲线只跑一次 run-factor)。
//!
//! 架构要点:
//! - strategy_config 单表:composite 行(无 parent)+ asset 子行(parent_strategy_id 指向 composite)。
//! - equity_curve_task_id 同时冗余存储在 composite 行和 a_share 子行(同值)。
//! - detect_combo_sharing 通过 a_share 子行的 equity_curve_task_id 反查所有共用同 combo 的 composite。
//! - UPDATE 时需同时写 composite 行和 a_share 子行,保持两处一致(否则 load_strategy_config
//!   读 composite 行、load_resolved_strategy 读 a_share 子行会看到不同值)。

use axum::{extract::Path, Json};
use chrono::NaiveDate;
use serde::Deserialize;
use sqlx::PgPool;
use tracing::{info, warn};

// ===== ETF 发行日查询(双保险) =====

/// 判断 ETF 在 date 当日是否已发行。
/// 双保险:优先读 market_stock.list_date(Task 0 回填的权威发行日);
/// list_date 为 NULL 时 fallback 到 market_stock_daily_bar_adj 的 MIN(trade_date) 推断首发日。
pub async fn is_etf_listed_on(db: &PgPool, symbol: &str, date: NaiveDate) -> bool {
    // 优先:market_stock.list_date(Task 0 修复 tushare 后回填)
    let list_date: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT list_date FROM market_stock WHERE symbol = $1",
    )
    .bind(symbol)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    if let Some(ld) = list_date {
        return ld <= date;
    }
    // fallback:daily_bar_adj 的 MIN(trade_date)(list_date 未回填时用首发行情日推断)
    let first: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT MIN(trade_date) FROM market_stock_daily_bar_adj WHERE symbol = $1",
    )
    .bind(symbol)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    first.map(|f| f <= date).unwrap_or(false)
}

// ===== 活跃策略收集 + combo 共用检测 =====

/// 收集所有活跃账号关联的策略(去重)。
pub async fn collect_active_strategies(db: &PgPool) -> Vec<String> {
    let rows: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT DISTINCT strategy_version_id FROM paper_account \
         WHERE status = 'active' AND strategy_version_id IS NOT NULL",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    rows.into_iter().flatten().collect()
}

/// 查与 strategy_id 共用同 combo 的 active 策略列表(含自身)。
/// 共用键:a_share 子行的 equity_curve_task_id 指向同一 task(即同 combo 产出)。
pub async fn detect_combo_sharing(db: &PgPool, strategy_id: &str) -> Vec<String> {
    // 取该策略 a_share 子行的 equity_curve_task_id
    let task_id: Option<String> = sqlx::query_scalar(
        "SELECT equity_curve_task_id FROM strategy_config \
         WHERE parent_strategy_id = $1 AND asset_class = 'a_share' AND status = 'active' \
         LIMIT 1",
    )
    .bind(strategy_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    let Some(tid) = task_id else {
        return vec![strategy_id.to_string()];
    };
    // 查所有 a_share 子行指向同一 task 的 composite 策略
    let sharing: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT parent_strategy_id FROM strategy_config \
         WHERE asset_class = 'a_share' AND status = 'active' AND equity_curve_task_id = $1",
    )
    .bind(&tid)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    if sharing.is_empty() {
        vec![strategy_id.to_string()]
    } else {
        sharing
    }
}

// ===== SyncResult + 单策略同步 =====

#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncResult {
    pub strategy_id: String,
    pub task_id: Option<String>,
    pub updated_strategy_ids: Vec<String>,
    pub status: String, // success | failed | partial
    pub error: Option<String>,
}

/// 单策略权益曲线同步:用策略配置跑 run-factor,UPDATE 共用该 combo 的所有策略。
///
/// UPDATE 范围:同时写 composite 行(strategy_id = $sid)和 a_share 子行
/// (parent_strategy_id = $sid AND asset_class = 'a_share'),保持两处 equity_curve_task_id 一致。
pub async fn sync_strategy_equity_curve(
    db: &PgPool,
    strategy_id: &str,
    start: NaiveDate,
    end: NaiveDate,
    background: bool,
) -> Result<SyncResult, String> {
    let sc = crate::routes::scheduler::load_strategy_config(db, strategy_id).await;
    let sharing = detect_combo_sharing(db, strategy_id).await;
    let combo = sc.combo_name.as_str();
    let top_n = sc.top_n as usize;

    let client = reqwest::Client::new();
    let api_base = format!(
        "http://localhost:{}",
        std::env::var("PORT").unwrap_or_else(|_| "8080".into())
    );
    let dv = crate::routes::scheduler::get_latest_data_version(db).await;
    let start_str = start.format("%Y%m%d").to_string();
    let end_str = end.format("%Y%m%d").to_string();
    let mut payload = serde_json::json!({
        "combo_name": combo,
        "strategy_version_id": "factor-combo-v1",
        "data_version_id": dv,
        "top_n": top_n,
        "rebalance": "10",
        "start_date": start_str,
        "end_date": end_str,
        "max_position_pct": 0.10,
        "max_gross_exposure": 0.95,
        "benchmark": "000300.SH",
        "universe_profile": "main_board_non_st",
        "score_direction": sc.score_direction,
        "effective_coverage": {
            "enabled": true,
            "mode": "guard_only",
            "min_rows": top_n.max(30),
            "include_rebalance_warmup": false
        }
    });
    // 因子+ML混合:带上策略指定的全周期预测集
    if sc.signal_source == "prediction_blend" || sc.signal_source == "prediction" {
        let pid = if let Some(ref p) = sc.prediction_set_id {
            Some(p.clone())
        } else {
            sqlx::query_scalar::<_, String>(
                "SELECT prediction_set_id FROM prediction_set WHERE status='ready' AND training_end_date IS NOT NULL \
                 ORDER BY (end_date - start_date) DESC, end_date DESC LIMIT 1",
            )
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
        };
        if let Some(pid) = pid {
            payload["prediction_set_id"] = serde_json::json!(pid);
            payload["prediction_blend_weight"] = serde_json::json!(sc.prediction_blend_weight);
            payload["kelly_fraction"] = serde_json::json!(0.25);
            payload["score_candidate_pool_size"] = serde_json::json!(200);
            info!(
                "[equity-sync] {} prediction_blend: set={} w={}",
                strategy_id, pid, sc.prediction_blend_weight
            );
        }
    }

    let timeout = if background {
        std::time::Duration::from_secs(60)
    } else {
        std::time::Duration::from_secs(600)
    };
    match client
        .post(format!(
            "{}/api/v1/quant/backtests/run-factor",
            api_base
        ))
        .json(&payload)
        .timeout(timeout)
        .send()
        .await
    {
        Ok(resp) => {
            let result: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("parse resp: {}", e))?;
            if let Some(tid) = result["data"]["task_id"].as_str() {
                // UPDATE 共用该 combo 的所有 active 策略(composite 行 + a_share 子行)
                for sid in &sharing {
                    // composite 行
                    let _ = sqlx::query(
                        "UPDATE strategy_config SET equity_curve_task_id = $1, updated_at = NOW() \
                         WHERE strategy_id = $2 AND status = 'active'",
                    )
                    .bind(tid)
                    .bind(sid)
                    .execute(db)
                    .await;
                    // a_share 子行(保持与 composite 行一致)
                    let _ = sqlx::query(
                        "UPDATE strategy_config SET equity_curve_task_id = $1, updated_at = NOW() \
                         WHERE parent_strategy_id = $2 AND asset_class = 'a_share' AND status = 'active'",
                    )
                    .bind(tid)
                    .bind(sid)
                    .execute(db)
                    .await;
                }
                info!(
                    "[equity-sync] {} 新曲线 task_id={}, 更新策略 {:?}",
                    strategy_id, tid, sharing
                );
                Ok(SyncResult {
                    strategy_id: strategy_id.to_string(),
                    task_id: Some(tid.to_string()),
                    updated_strategy_ids: sharing,
                    status: "success".into(),
                    error: None,
                })
            } else {
                let msg = format!("run-factor 未返回 task_id: {:?}", result);
                warn!("[equity-sync] {} {}", strategy_id, msg);
                Ok(SyncResult {
                    strategy_id: strategy_id.to_string(),
                    task_id: None,
                    updated_strategy_ids: vec![],
                    status: "failed".into(),
                    error: Some(msg),
                })
            }
        }
        Err(e) => {
            let msg = format!("run-factor 请求失败: {}", e);
            warn!("[equity-sync] {} {}", strategy_id, msg);
            Ok(SyncResult {
                strategy_id: strategy_id.to_string(),
                task_id: None,
                updated_strategy_ids: vec![],
                status: "failed".into(),
                error: Some(msg),
            })
        }
    }
}

// ===== 活跃策略遍历同步 =====

/// 遍历所有活跃账号关联策略,按 combo 去重,逐个同步。
/// 单策略失败不阻断其他。
pub async fn sync_active_strategies_equity_curves(db: &PgPool) -> Vec<SyncResult> {
    let strategies = collect_active_strategies(db).await;
    // 按 combo 去重:同 combo 的策略只同步一次(取首个代表)
    let mut seen_combos: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut to_sync: Vec<String> = Vec::new();
    for sid in &strategies {
        let sharing = detect_combo_sharing(db, sid).await;
        // 用排序后的 sharing 列表作 combo 指纹
        let mut fp = sharing.clone();
        fp.sort();
        let key = fp.join(",");
        if seen_combos.insert(key) {
            to_sync.push(sid.clone());
        }
    }
    info!(
        "[equity-sync] 活跃策略 {:?} 去 combo 后 {:?}",
        strategies, to_sync
    );
    let end = chrono::Utc::now().date_naive();
    let start = chrono::NaiveDate::from_ymd_opt(2014, 1, 2).unwrap();
    let mut results = Vec::new();
    for sid in &to_sync {
        let r = sync_strategy_equity_curve(db, sid, start, end, false).await;
        match r {
            Ok(r) => results.push(r),
            Err(e) => {
                warn!("[equity-sync] {} 异常: {}", sid, e);
                results.push(SyncResult {
                    strategy_id: sid.clone(),
                    task_id: None,
                    updated_strategy_ids: vec![],
                    status: "failed".into(),
                    error: Some(e),
                });
            }
        }
    }
    results
}

// ===== ReadinessReport + 审计 =====

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadinessReport {
    pub strategy_id: String,
    pub equity_curve_task_id: Option<String>,
    pub equity_curve_coverage: EquityCurveCoverage,
    pub etf_price_coverage: EtfPriceCoverage,
    pub missing_items: Vec<String>,
    pub ready: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EquityCurveCoverage {
    pub trade_day_count: i64,
    pub first_date: Option<NaiveDate>,
    pub last_date: Option<NaiveDate>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EtfPriceCoverage {
    pub total_etfs: usize,
    pub listed_etfs: usize,
    pub not_yet_listed: Vec<String>,
}

/// 审计策略权益曲线 + ETF 价格覆盖(区分未发行 vs 缺数据)。
pub async fn audit_equity_curve_readiness(
    db: &PgPool,
    strategy_id: &str,
) -> Result<ReadinessReport, String> {
    let rs = crate::routes::strategy::load_resolved_strategy(db, strategy_id)
        .await
        .map_err(|e| format!("load strategy: {}", e))?;
    let a_task_id = rs
        .assets
        .iter()
        .find(|a| a.asset_class == crate::routes::strategy::AssetClass::AShare)
        .and_then(|a| a.security.equity_curve_task_id.as_deref())
        .ok_or_else(|| format!("策略 {} 无 a_share equity_curve_task_id", strategy_id))?;

    let ec: EquityCurveCoverage =
        sqlx::query_as::<_, (i64, Option<NaiveDate>, Option<NaiveDate>)>(
            "SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM backtest_equity_curve WHERE task_id = $1",
        )
        .bind(a_task_id)
        .fetch_one(db)
        .await
        .map(|(c, f, l)| EquityCurveCoverage {
            trade_day_count: c,
            first_date: f,
            last_date: l,
        })
        .map_err(|e| format!("ec query: {}", e))?;

    let mut not_yet_listed = Vec::new();
    let today = chrono::Utc::now().date_naive();
    for sym in &rs.etf_symbols {
        if !is_etf_listed_on(db, sym, today).await {
            not_yet_listed.push(sym.clone());
        }
    }
    let listed = rs.etf_symbols.len() - not_yet_listed.len();
    let etf_price_coverage = EtfPriceCoverage {
        total_etfs: rs.etf_symbols.len(),
        listed_etfs: listed,
        not_yet_listed,
    };

    let mut missing = Vec::new();
    if ec.trade_day_count == 0 {
        missing.push(format!(
            "equity_curve_task_id={} 无 backtest_equity_curve 数据",
            a_task_id
        ));
    }
    let ready = missing.is_empty();
    Ok(ReadinessReport {
        strategy_id: strategy_id.to_string(),
        equity_curve_task_id: Some(a_task_id.to_string()),
        equity_curve_coverage: ec,
        etf_price_coverage,
        missing_items: missing,
        ready,
    })
}

// ===== HTTP handler =====

#[derive(Debug, Deserialize)]
pub struct EquityCurveSyncRequest {
    pub start_date: Option<String>,  // YYYYMMDD
    pub end_date: Option<String>,
    pub background: Option<bool>,
}

/// POST /api/v1/strategies/{strategy_id}/equity-curve/sync
pub async fn handle_equity_curve_sync(
    Path(strategy_id): Path<String>,
    Json(req): Json<EquityCurveSyncRequest>,
) -> Json<serde_json::Value> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
    let db = match sqlx::PgPool::connect(&url).await {
        Ok(db) => db,
        Err(e) => return Json(serde_json::json!({"code": 1, "message": format!("db: {}", e)})),
    };
    let parse_date = |s: &Option<String>, default: chrono::NaiveDate| -> Result<chrono::NaiveDate, String> {
        match s {
            Some(d) => chrono::NaiveDate::parse_from_str(d, "%Y%m%d")
                .or_else(|_| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d"))
                .map_err(|e| format!("日期格式错误 {}: {}", d, e)),
            None => Ok(default),
        }
    };
    let start = match parse_date(&req.start_date, chrono::NaiveDate::from_ymd_opt(2014, 1, 2).unwrap()) {
        Ok(d) => d, Err(e) => return Json(serde_json::json!({"code": 1, "message": e})),
    };
    let end = match parse_date(&req.end_date, chrono::Utc::now().date_naive()) {
        Ok(d) => d, Err(e) => return Json(serde_json::json!({"code": 1, "message": e})),
    };
    if start > end {
        return Json(serde_json::json!({"code": 1, "message": "start_date > end_date"}));
    }
    let background = req.background.unwrap_or(false);
    match sync_strategy_equity_curve(&db, &strategy_id, start, end, background).await {
        Ok(r) => Json(serde_json::json!({"code": 0, "data": r})),
        Err(e) => Json(serde_json::json!({"code": 1, "message": e})),
    }
}

/// GET /api/v1/strategies/{strategy_id}/equity-curve/readiness-audit
pub async fn handle_equity_curve_readiness_audit(
    Path(strategy_id): Path<String>,
) -> Json<serde_json::Value> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
    let db = match sqlx::PgPool::connect(&url).await {
        Ok(db) => db,
        Err(e) => return Json(serde_json::json!({"code": 1, "message": format!("db: {}", e)})),
    };
    match audit_equity_curve_readiness(&db, &strategy_id).await {
        Ok(r) => Json(serde_json::json!({"code": 0, "data": r})),
        Err(e) => Json(serde_json::json!({"code": 1, "message": e})),
    }
}

// ===== 集成测试(需 DB,默认 ignore) =====

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_collect_active_strategies_dedup() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        let strategies = collect_active_strategies(&db).await;
        // 当前活跃账号挂 v21/v21_lev(v19 已无 active 账号),去重后应含这两个
        assert!(strategies.contains(&"v21".to_string()), "含 v21");
        assert!(strategies.contains(&"v21_lev".to_string()), "含 v21_lev");
        assert_eq!(
            strategies.len(),
            strategies
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            "无重复"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_detect_combo_sharing_v21_v21_lev() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        // v21 和 v21_lev 共用 fbt-ab3eecf6 → detect_combo_sharing(v21) 应含 v21_lev
        let sharing = detect_combo_sharing(&db, "v21").await;
        assert!(sharing.contains(&"v21".to_string()), "含自身");
        assert!(sharing.contains(&"v21_lev".to_string()), "v21_lev 共用 v21 曲线");
    }

    #[tokio::test]
    #[ignore]
    async fn test_is_etf_listed_on() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("db");
        // 518880 首发日 2013-07-29
        assert!(
            is_etf_listed_on(
                &db,
                "518880.SH",
                chrono::NaiveDate::from_ymd_opt(2014, 1, 2).unwrap()
            )
            .await,
            "518880 在 2014 已发行"
        );
        // 159980 首发日 2019-12-24
        assert!(
            !is_etf_listed_on(
                &db,
                "159980.SZ",
                chrono::NaiveDate::from_ymd_opt(2014, 1, 2).unwrap()
            )
            .await,
            "159980 在 2014 未发行"
        );
        assert!(
            is_etf_listed_on(
                &db,
                "159980.SZ",
                chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
            )
            .await,
            "159980 在 2020 已发行"
        );
    }
}
