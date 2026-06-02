//! 数据清理 API — 清理回测缓存、历史回测记录、模型预测等一次性数据
//!
//! 不会清理的数据：
//!   - market_* 行情基础数据
//!   - factor_value 因子值（TimescaleDB 超表）
//!   - factor_definition / factor_evaluation 因子定义
//!   - paper_* 模拟交易数据
//!   - data_version / data_sync_task 数据版本
//!
//! 可清理的类别：
//!   1. market_feature_cache_* — 全部缓存表（~295 GB）
//!   2. backtest_* + portfolio_* — 按 task_id 清理回测记录（~34 GB）
//!   3. model_prediction — 按 prediction_set_id 清理预测数据（~47 GB）
//!   4. multi_factor_value — 按 combo_name 清理因子组合值（~78 GB）

use axum::{extract::State, response::IntoResponse, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;
use tracing::{info, warn};

use crate::AppState;

// ── 请求体 ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CleanupRequest {
    /// 清理所有 market_feature_cache_* 缓存表
    #[serde(default)]
    pub clean_all_cache: bool,
    /// 按 cache_key 清理指定缓存
    #[serde(default)]
    pub cache_keys: Vec<String>,
    /// 按回测任务 ID 列表清理回测相关数据（如果 is_kept=true 且未设 force=true 则跳过）
    #[serde(default)]
    pub backtest_task_ids: Vec<String>,
    /// 按 prediction_set_id 列表清理模型预测
    #[serde(default)]
    pub prediction_set_ids: Vec<String>,
    /// 按 combo_name 列表清理多因子组合值
    #[serde(default)]
    pub combo_names: Vec<String>,
    /// 仅预览，不实际执行删除
    #[serde(default)]
    pub dry_run: bool,
    /// 强制删除（包括 is_kept=true 的回测记录）
    #[serde(default)]
    pub force: bool,
    /// 自动清理：删除 7 天前且 is_kept=false 的回测记录
    #[serde(default)]
    pub auto_cleanup_expired: bool,
}

/// 标记回测任务保留状态
#[derive(Debug, Deserialize)]
pub struct MarkKeepRequest {
    pub is_kept: bool,
}

// ── 统计响应 ────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
struct CategoryStat {
    category: String,
    description: String,
    table_count: usize,
    total_size_bytes: i64,
    total_size_pretty: String,
    row_count: i64,
    cleanable: bool,
}

#[derive(Debug, serde::Serialize)]
struct TableSize {
    table_name: String,
    size_bytes: i64,
    size_pretty: String,
    row_count: i64,
}

// ── 数据统计 ────────────────────────────────────────────

/// GET /api/v1/quant/data/cleanup/stats
pub async fn cleanup_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match get_cleanup_stats(&state.db).await {
        Ok(stats) => Json(json!({"code": 0, "data": stats})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

async fn get_cleanup_stats(db: &sqlx::PgPool) -> Result<Value, String> {
    // 所有 public schema 表的大小（pg_class + pg_stat_user_tables 联合查询）
    let query_rows = sqlx::query(
        r#"
        SELECT
            c.relname AS table_name,
            pg_total_relation_size(c.oid) AS size_bytes,
            pg_size_pretty(pg_total_relation_size(c.oid)) AS size_pretty,
            COALESCE(s.n_live_tup, 0) AS row_count
        FROM pg_class c
        LEFT JOIN pg_stat_user_tables s ON s.relname = c.relname AND s.schemaname = 'public'
        WHERE c.relnamespace = 'public'::regnamespace
          AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
        "#,
    )
    .fetch_all(db)
    .await
    .map_err(|e| format!("查询表大小失败: {e}"))?;

    let rows: Vec<TableSize> = query_rows
        .iter()
        .map(|r| TableSize {
            table_name: r.get::<String, _>(0),
            size_bytes: r.get::<i64, _>(1),
            size_pretty: r.get::<String, _>(2),
            row_count: r.get::<i64, _>(3),
        })
        .collect();

    let total_bytes: i64 = rows.iter().map(|r| r.size_bytes).sum();

    // 按类别汇总
    let categories = build_categories(&rows);

    // 明细
    let details: Vec<Value> = rows
        .iter()
        .map(|r| {
            let cat = classify_table(&r.table_name);
            json!({
                "table": r.table_name,
                "size": r.size_pretty,
                "size_bytes": r.size_bytes,
                "rows": r.row_count,
                "category": cat.0,
                "cleanable": cat.1,
            })
        })
        .collect();

    Ok(json!({
        "total_size_bytes": total_bytes,
        "total_size_pretty": format_bytes(total_bytes),
        "categories": categories,
        "tables": details,
    }))
}

fn classify_table(name: &str) -> (&'static str, bool) {
    if name.starts_with("market_feature_cache_") {
        ("cache", true)
    } else if name.starts_with("backtest_") || name.starts_with("portfolio_") {
        ("backtest", true)
    } else if name == "model_prediction" {
        ("prediction", true)
    } else if name == "multi_factor_value" || name == "multi_factor_weight" {
        ("factor_combo", true)
    } else if name.starts_with("market_") {
        ("market_data", false)
    } else if name.starts_with("factor_") && name != "factor_value" {
        ("factor_meta", false)
    } else if name == "factor_value" {
        ("factor_value", false) // 因子值不清理
    } else if name.starts_with("paper_") {
        ("paper", false)
    } else if name == "prediction_set"
        || name == "training_dataset"
        || name == "experiment_run"
        || name == "optimization_trial"
        || name == "optimization_task"
        || name == "robustness_gate_result"
    {
        ("experiment", false) // 实验/优化记录有参考价值
    } else {
        ("other", false)
    }
}

fn build_categories(rows: &[TableSize]) -> Vec<CategoryStat> {
    let mut cats: Vec<CategoryStat> = Vec::new();
    let category_defs = vec![
        ("cache", "回测缓存 (market_feature_cache_*)", true),
        ("backtest", "回测记录 (backtest_* / portfolio_*)", true),
        ("prediction", "模型预测 (model_prediction)", true),
        ("factor_combo", "因子组合值 (multi_factor_value)", true),
        ("market_data", "行情基础数据 (market_*)", false),
        ("factor_value", "因子值 (factor_value 超表)", false),
        ("factor_meta", "因子元数据", false),
        ("experiment", "实验/优化记录", false),
        ("paper", "模拟交易", false),
        ("other", "其他", false),
    ];

    for (cat, desc, cleanable) in &category_defs {
        let subset: Vec<&TableSize> = rows
            .iter()
            .filter(|r| classify_table(&r.table_name).0 == *cat)
            .collect();
        let total_bytes: i64 = subset.iter().map(|r| r.size_bytes).sum();
        let total_rows: i64 = subset.iter().map(|r| r.row_count).sum();
        cats.push(CategoryStat {
            category: cat.to_string(),
            description: desc.to_string(),
            table_count: subset.len(),
            total_size_bytes: total_bytes,
            total_size_pretty: format_bytes(total_bytes),
            row_count: total_rows,
            cleanable: *cleanable,
        });
    }
    cats
}

pub fn format_bytes(bytes: i64) -> String {
    if bytes == 0 {
        return "0 B".into();
    }
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut idx = 0;
    while size >= 1024.0 && idx < units.len() - 1 {
        size /= 1024.0;
        idx += 1;
    }
    format!("{:.2} {}", size, units[idx])
}

// ── 清理预览 ────────────────────────────────────────────

/// POST /api/v1/quant/data/cleanup/preview
pub async fn cleanup_preview(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupRequest>,
) -> impl IntoResponse {
    match preview_cleanup(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

async fn preview_cleanup(db: &sqlx::PgPool, req: &CleanupRequest) -> Result<Value, String> {
    let mut previews: Vec<Value> = vec![];
    let mut total_estimated_bytes: i64 = 0;

    // 1. 缓存表
    if req.clean_all_cache {
        let size = get_cache_total_size(db).await?;
        total_estimated_bytes += size;
        previews.push(json!({
            "action": "TRUNCATE_ALL_CACHE",
            "tables": ["market_feature_cache_value", "market_feature_cache_return_risk_matrix_row",
                       "market_feature_cache_return_risk_pairwise_row", "market_feature_cache_symbol",
                       "market_feature_cache_return_risk_stats_row", "market_feature_cache_manifest"],
            "estimated_size": format_bytes(size),
            "estimated_size_bytes": size,
        }));
    }
    if !req.cache_keys.is_empty() {
        for ck in &req.cache_keys {
            let size = get_cache_key_size(db, ck).await?;
            total_estimated_bytes += size;
            previews.push(json!({
                "action": "DELETE_CACHE_KEY",
                "cache_key": ck,
                "estimated_size": format_bytes(size),
                "estimated_size_bytes": size,
            }));
        }
    }

    // 2. 自动清理过期回测
    if req.auto_cleanup_expired {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*)::bigint FROM backtest_task
            WHERE is_kept = false AND status = 'completed'
              AND created_at < NOW() - INTERVAL '7 days'
            "#,
        )
        .fetch_one(db)
        .await
        .map_err(|e| format!("查询过期任务失败: {e}"))?;
        let expired_count: i64 = row.get(0);
        total_estimated_bytes += expired_count * 500_000; // 粗略估计每个 task 约 500KB
        previews.push(json!({
            "action": "AUTO_CLEANUP_EXPIRED",
            "description": "清理 7 天前且 is_kept=false 的已完成回测",
            "expired_task_count": expired_count,
            "estimated_size": format_bytes(expired_count * 500_000),
            "estimated_size_bytes": expired_count * 500_000,
        }));
    }

    // 3. 回测任务
    if !req.backtest_task_ids.is_empty() {
        for tid in &req.backtest_task_ids {
            let rows = count_backtest_rows(db, tid).await?;
            let size = estimate_backtest_size(db, tid).await?;
            total_estimated_bytes += size;
            previews.push(json!({
                "action": "DELETE_BACKTEST_TASK",
                "task_id": tid,
                "affected_tables": rows,
                "estimated_size": format_bytes(size),
                "estimated_size_bytes": size,
            }));
        }
    }

    // 4. 模型预测
    if !req.prediction_set_ids.is_empty() {
        for psid in &req.prediction_set_ids {
            let count = count_prediction_rows(db, psid).await?;
            let est_size = count * 200; // ~200 bytes per row
            total_estimated_bytes += est_size;
            previews.push(json!({
                "action": "DELETE_MODEL_PREDICTIONS",
                "prediction_set_id": psid,
                "row_count": count,
                "estimated_size": format_bytes(est_size),
                "estimated_size_bytes": est_size,
            }));
        }
    }

    // 5. 因子组合
    if !req.combo_names.is_empty() {
        for cn in &req.combo_names {
            let count = count_combo_rows(db, cn).await?;
            let est_size = count * 40; // ~40 bytes per row
            total_estimated_bytes += est_size;
            previews.push(json!({
                "action": "DELETE_COMBO",
                "combo_name": cn,
                "row_count": count,
                "estimated_size": format_bytes(est_size),
                "estimated_size_bytes": est_size,
            }));
        }
    }

    Ok(json!({
        "total_actions": previews.len(),
        "total_estimated_size_bytes": total_estimated_bytes,
        "total_estimated_size_pretty": format_bytes(total_estimated_bytes),
        "actions": previews,
    }))
}

// ── 执行清理 ────────────────────────────────────────────

/// POST /api/v1/quant/data/cleanup
pub async fn execute_cleanup(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupRequest>,
) -> impl IntoResponse {
    match run_cleanup(&state.db, &req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

async fn run_cleanup(db: &sqlx::PgPool, req: &CleanupRequest) -> Result<Value, String> {
    if req.dry_run {
        return preview_cleanup(db, req).await;
    }

    let has_any_action = req.clean_all_cache
        || !req.cache_keys.is_empty()
        || !req.backtest_task_ids.is_empty()
        || !req.prediction_set_ids.is_empty()
        || !req.combo_names.is_empty()
        || req.auto_cleanup_expired;

    if !has_any_action {
        return Err("至少需要指定一种清理操作".into());
    }

    info!(
        clean_all_cache = req.clean_all_cache,
        cache_keys = req.cache_keys.len(),
        backtest_task_ids = req.backtest_task_ids.len(),
        prediction_set_ids = req.prediction_set_ids.len(),
        combo_names = req.combo_names.len(),
        "开始数据清理"
    );

    let mut results: Vec<Value> = vec![];
    let mut total_freed_bytes: i64 = 0;

    // ── 1. 清理全部缓存表 ──
    if req.clean_all_cache {
        match clean_all_cache_tables(db).await {
            Ok(freed) => {
                total_freed_bytes += freed;
                results.push(json!({
                    "action": "TRUNCATE_ALL_CACHE",
                    "status": "success",
                    "freed_size": format_bytes(freed),
                    "freed_bytes": freed,
                }));
                info!(freed = format_bytes(freed), "已清理全部回测缓存");
            }
            Err(e) => {
                warn!("清理缓存失败: {e}");
                results.push(json!({"action": "TRUNCATE_ALL_CACHE", "status": "error", "error": e}));
            }
        }
    }

    // ── 2. 按 cache_key 清理 ──
    for ck in &req.cache_keys {
        match clean_cache_by_key(db, ck).await {
            Ok(freed) => {
                total_freed_bytes += freed;
                results.push(json!({
                    "action": "DELETE_CACHE_KEY",
                    "cache_key": ck,
                    "status": "success",
                    "freed_bytes": freed,
                }));
            }
            Err(e) => {
                results.push(json!({
                    "action": "DELETE_CACHE_KEY", "cache_key": ck,
                    "status": "error", "error": e,
                }));
            }
        }
    }

    // ── 3. 自动清理过期回测（7天前 + is_kept=false） ──
    if req.auto_cleanup_expired {
        match clean_expired_backtests(db).await {
            Ok((count, freed)) => {
                total_freed_bytes += freed;
                results.push(json!({
                    "action": "AUTO_CLEANUP_EXPIRED",
                    "status": "success",
                    "deleted_tasks": count,
                    "freed_bytes": freed,
                    "freed_size": format_bytes(freed),
                }));
                info!(deleted_tasks = count, freed = format_bytes(freed), "已自动清理过期回测");
            }
            Err(e) => {
                results.push(json!({
                    "action": "AUTO_CLEANUP_EXPIRED",
                    "status": "error",
                    "error": e,
                }));
            }
        }
    }

    // ── 4. 清理指定回测任务 ──
    for tid in &req.backtest_task_ids {
        match clean_backtest_task(db, tid, req.force).await {
            Ok(freed) => {
                total_freed_bytes += freed;
                results.push(json!({
                    "action": "DELETE_BACKTEST_TASK",
                    "task_id": tid,
                    "status": "success",
                    "freed_bytes": freed,
                }));
                info!(task_id = %tid, "已清理回测任务");
            }
            Err(e) => {
                results.push(json!({
                    "action": "DELETE_BACKTEST_TASK", "task_id": tid,
                    "status": "error", "error": e,
                }));
            }
        }
    }

    // ── 5. 清理模型预测 ──
    for psid in &req.prediction_set_ids {
        match clean_model_predictions(db, psid).await {
            Ok(count) => {
                let freed = count * 200;
                total_freed_bytes += freed;
                results.push(json!({
                    "action": "DELETE_MODEL_PREDICTIONS",
                    "prediction_set_id": psid,
                    "status": "success",
                    "deleted_rows": count,
                    "freed_bytes": freed,
                }));
                info!(prediction_set_id = %psid, rows = count, "已清理模型预测");
            }
            Err(e) => {
                results.push(json!({
                    "action": "DELETE_MODEL_PREDICTIONS", "prediction_set_id": psid,
                    "status": "error", "error": e,
                }));
            }
        }
    }

    // ── 6. 清理因子组合 ──
    for cn in &req.combo_names {
        match clean_combo(db, cn).await {
            Ok(count) => {
                let freed = count * 40;
                total_freed_bytes += freed;
                results.push(json!({
                    "action": "DELETE_COMBO",
                    "combo_name": cn,
                    "status": "success",
                    "deleted_rows": count,
                    "freed_bytes": freed,
                }));
                info!(combo_name = %cn, rows = count, "已清理因子组合");
            }
            Err(e) => {
                results.push(json!({
                    "action": "DELETE_COMBO", "combo_name": cn,
                    "status": "error", "error": e,
                }));
            }
        }
    }

    info!(
        total_freed = format_bytes(total_freed_bytes),
        actions = results.len(),
        "数据清理完成"
    );

    Ok(json!({
        "total_freed_bytes": total_freed_bytes,
        "total_freed_pretty": format_bytes(total_freed_bytes),
        "actions": results,
    }))
}

// ── 缓存表操作 ──────────────────────────────────────────

async fn get_cache_total_size(db: &sqlx::PgPool) -> Result<i64, String> {
    let row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint AS size_bytes
        FROM pg_class c
        WHERE c.relnamespace = 'public'::regnamespace
          AND c.relname LIKE 'market_feature_cache_%'
        "#,
    )
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询缓存总大小失败: {e}"))?;
    Ok(row.get::<i64, _>(0))
}

async fn get_cache_key_size(db: &sqlx::PgPool, cache_key: &str) -> Result<i64, String> {
    let row = sqlx::query(
        "SELECT COUNT(*)::bigint AS cnt FROM market_feature_cache_value WHERE cache_key = $1",
    )
    .bind(cache_key)
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询缓存key行数失败: {e}"))?;
    let cnt: i64 = row.get(0);
    Ok(cnt * 450) // 每行约 450 bytes (value + index overhead)
}

async fn clean_all_cache_tables(db: &sqlx::PgPool) -> Result<i64, String> {
    let size_before = get_cache_total_size(db).await?;

    // 使用 TRUNCATE 快速清空（不需要 VACUUM）
    let tables = [
        "market_feature_cache_value",
        "market_feature_cache_return_risk_matrix_row",
        "market_feature_cache_return_risk_pairwise_row",
        "market_feature_cache_symbol",
        "market_feature_cache_return_risk_stats_row",
        "market_feature_cache_manifest",
    ];

    for tbl in &tables {
        let sql = format!("TRUNCATE TABLE {} CASCADE", tbl);
        sqlx::query(&sql)
            .execute(db)
            .await
            .map_err(|e| format!("清理表 {} 失败: {}", tbl, e))?;
    }

    Ok(size_before)
}

async fn clean_cache_by_key(db: &sqlx::PgPool, cache_key: &str) -> Result<i64, String> {
    let size_before = get_cache_key_size(db, cache_key).await?;

    // 先删子表，再删 manifest
    let child_tables = [
        "market_feature_cache_value",
        "market_feature_cache_return_risk_matrix_row",
        "market_feature_cache_return_risk_pairwise_row",
        "market_feature_cache_symbol",
        "market_feature_cache_return_risk_stats_row",
    ];

    for tbl in &child_tables {
        let sql = format!("DELETE FROM {} WHERE cache_key = $1", tbl);
        sqlx::query(&sql)
            .bind(cache_key)
            .execute(db)
            .await
            .map_err(|e| format!("删除 {} cache_key={}: {}", tbl, cache_key, e))?;
    }

    sqlx::query("DELETE FROM market_feature_cache_manifest WHERE cache_key = $1")
        .bind(cache_key)
        .execute(db)
        .await
        .map_err(|e| format!("删除 manifest cache_key={}: {}", cache_key, e))?;

    Ok(size_before)
}

// ── 回测任务操作 ────────────────────────────────────────

async fn count_backtest_rows(db: &sqlx::PgPool, task_id: &str) -> Result<Value, String> {
    let tables = [
        "backtest_position",
        "backtest_trade",
        "backtest_equity_curve",
        "backtest_result",
        "portfolio_exposure",
        "portfolio_attribution",
        "portfolio_target",
        "portfolio_constraint_violation",
    ];

    let mut counts = serde_json::Map::new();
    for tbl in &tables {
        let sql = format!(
            "SELECT COUNT(*)::bigint FROM {} WHERE task_id = $1",
            tbl
        );
        let row = sqlx::query_scalar::<_, Option<i64>>(&sql)
            .bind(task_id)
            .fetch_one(db)
            .await
            .map_err(|e| format!("查询 {} 行数失败: {}", tbl, e))?;
        counts.insert(tbl.to_string(), json!(row.unwrap_or(0)));
    }
    Ok(Value::Object(counts))
}

async fn estimate_backtest_size(db: &sqlx::PgPool, task_id: &str) -> Result<i64, String> {
    let row = sqlx::query(
        "SELECT COUNT(*)::bigint AS cnt FROM backtest_position WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询回测行数失败: {e}"))?;
    let cnt: i64 = row.get(0);
    Ok(cnt * 500) // position 每行约 300 bytes, 加上其他表约 200 bytes overhead
}

async fn clean_backtest_task(db: &sqlx::PgPool, task_id: &str, force: bool) -> Result<i64, String> {
    // 检查是否标记为保留
    if !force {
        let row = sqlx::query("SELECT is_kept FROM backtest_task WHERE task_id = $1")
            .bind(task_id)
            .fetch_optional(db)
            .await
            .map_err(|e| format!("查询回测任务失败: {e}"))?;
        match row {
            Some(r) if r.get::<bool, _>(0) => {
                return Err(format!(
                    "回测任务 {} 已标记为保留（is_kept=true），使用 force=true 强制删除",
                    task_id
                ));
            }
            None => return Err(format!("回测任务 {} 不存在", task_id)),
            _ => {}
        }
    }

    let freed = estimate_backtest_size(db, task_id).await?;

    // 删除顺序：先删引用 backtest_task 的子表
    let child_tables = [
        "backtest_position",
        "backtest_trade",
        "backtest_equity_curve",
        "backtest_result",
        "portfolio_exposure",
        "portfolio_attribution",
        "portfolio_target",
        "portfolio_constraint_violation",
    ];

    for tbl in &child_tables {
        let sql = format!("DELETE FROM {} WHERE task_id = $1", tbl);
        sqlx::query(&sql)
            .bind(task_id)
            .execute(db)
            .await
            .map_err(|e| format!("删除 {} task_id={}: {}", tbl, task_id, e))?;
    }

    // 最后删回测任务记录
    sqlx::query("DELETE FROM backtest_task WHERE task_id = $1")
        .bind(task_id)
        .execute(db)
        .await
        .map_err(|e| format!("删除 backtest_task task_id={}: {}", task_id, e))?;

    Ok(freed)
}

// ── 模型预测操作 ────────────────────────────────────────

async fn count_prediction_rows(db: &sqlx::PgPool, prediction_set_id: &str) -> Result<i64, String> {
    let row = sqlx::query(
        "SELECT COUNT(*)::bigint AS cnt FROM model_prediction WHERE prediction_set_id = $1",
    )
    .bind(prediction_set_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询预测行数失败: {e}"))?;
    Ok(row.get::<i64, _>(0))
}

async fn clean_model_predictions(
    db: &sqlx::PgPool,
    prediction_set_id: &str,
) -> Result<i64, String> {
    let count = count_prediction_rows(db, prediction_set_id).await?;
    if count == 0 {
        return Ok(0);
    }

    // model_prediction 是 TimescaleDB 超表，使用 DELETE
    sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id = $1")
        .bind(prediction_set_id)
        .execute(db)
        .await
        .map_err(|e| format!("删除 model_prediction prediction_set_id={}: {}", prediction_set_id, e))?;

    Ok(count)
}

// ── 因子组合操作 ────────────────────────────────────────

async fn count_combo_rows(db: &sqlx::PgPool, combo_name: &str) -> Result<i64, String> {
    let row = sqlx::query(
        "SELECT COUNT(*)::bigint AS cnt FROM multi_factor_value WHERE combo_name = $1",
    )
    .bind(combo_name)
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询组合行数失败: {e}"))?;
    Ok(row.get::<i64, _>(0))
}

async fn clean_combo(db: &sqlx::PgPool, combo_name: &str) -> Result<i64, String> {
    let count = count_combo_rows(db, combo_name).await?;
    if count == 0 {
        return Ok(0);
    }

    sqlx::query("DELETE FROM multi_factor_value WHERE combo_name = $1")
        .bind(combo_name)
        .execute(db)
        .await
        .map_err(|e| format!("删除 multi_factor_value combo_name={}: {}", combo_name, e))?;

    Ok(count)
}

// ── 过期回测自动清理（供 scheduler 调用） ──────────────

/// 清理 7 天前且 is_kept=false 的回测记录及其关联数据。
/// 返回 (删除的 task 数量, 释放的字节数)
pub async fn clean_expired_backtests(db: &sqlx::PgPool) -> Result<(i64, i64), String> {
    // 查找 7 天前、未标记保留的 completed 回测任务
    let rows = sqlx::query(
        r#"
        SELECT task_id FROM backtest_task
        WHERE is_kept = false
          AND status = 'completed'
          AND created_at < NOW() - INTERVAL '7 days'
        "#,
    )
    .fetch_all(db)
    .await
    .map_err(|e| format!("查询过期回测任务失败: {e}"))?;

    let task_ids: Vec<String> = rows.iter().map(|r| r.get::<String, _>(0)).collect();
    let count = task_ids.len() as i64;

    if count == 0 {
        info!("没有过期的回测任务需要清理");
        return Ok((0, 0));
    }

    info!(count, "发现过期回测任务，开始批量清理");

    let backend_tables = [
        "backtest_position",
        "backtest_trade",
        "backtest_equity_curve",
        "backtest_result",
        "portfolio_exposure",
        "portfolio_attribution",
        "portfolio_target",
        "portfolio_constraint_violation",
    ];

    // 批量删除：每个表一条 SQL，用 ANY($1) 替代逐条 DELETE
    for tbl in &backend_tables {
        let sql = format!("DELETE FROM {} WHERE task_id = ANY($1)", tbl);
        match sqlx::query(&sql).bind(&task_ids).execute(db).await {
            Ok(r) => info!(table = tbl, deleted = r.rows_affected(), "批量清理"),
            Err(e) => warn!(table = tbl, error = %e, "批量清理失败"),
        }
    }

    // 删除回测任务记录
    match sqlx::query("DELETE FROM backtest_task WHERE task_id = ANY($1)")
        .bind(&task_ids)
        .execute(db)
        .await
    {
        Ok(r) => info!(deleted = r.rows_affected(), "清理回测任务记录"),
        Err(e) => warn!(error = %e, "清理回测任务记录失败"),
    }

    // 估算释放空间（粗略）
    let est_freed = count * 500_000;

    info!(count, freed = format_bytes(est_freed), "过期回测批量清理完成");

    Ok((count, est_freed))
}

// ── 标记回测任务保留状态 ────────────────────────────────

/// POST /api/v1/quant/data/cleanup/backtest-tasks/{task_id}/keep
pub async fn mark_backtest_kept(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
    Json(req): Json<MarkKeepRequest>,
) -> impl IntoResponse {
    match sqlx::query("UPDATE backtest_task SET is_kept = $1 WHERE task_id = $2")
        .bind(req.is_kept)
        .bind(&task_id)
        .execute(&state.db)
        .await
    {
        Ok(result) if result.rows_affected() > 0 => {
            let action = if req.is_kept { "标记保留" } else { "取消保留" };
            info!(task_id = %task_id, is_kept = req.is_kept, "{}", action);
            Json(json!({
                "code": 0,
                "data": {
                    "task_id": task_id,
                    "is_kept": req.is_kept,
                    "message": format!("已{}", action),
                }
            }))
        }
        Ok(_) => Json(json!({"code": 1, "message": format!("回测任务 {} 不存在", task_id)})),
        Err(e) => Json(json!({"code": 1, "message": format!("更新失败: {}", e)})),
    }
}

/// GET /api/v1/quant/data/cleanup/expired-stats
/// 查看有多少过期回测任务可被清理
pub async fn expired_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match get_expired_stats(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(e) => Json(json!({"code": 1, "message": e})),
    }
}

async fn get_expired_stats(db: &sqlx::PgPool) -> Result<Value, String> {
    // 过期（7天前 + is_kept=false + completed）
    let row = sqlx::query(
        r#"
        SELECT COUNT(*)::bigint AS cnt
        FROM backtest_task
        WHERE is_kept = false
          AND status = 'completed'
          AND created_at < NOW() - INTERVAL '7 days'
        "#,
    )
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询过期任务失败: {e}"))?;
    let expired_count: i64 = row.get(0);

    // 保留的
    let row2 = sqlx::query(
        "SELECT COUNT(*)::bigint AS cnt FROM backtest_task WHERE is_kept = true",
    )
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询保留任务失败: {e}"))?;
    let kept_count: i64 = row2.get(0);

    // 未过期
    let row3 = sqlx::query(
        r#"
        SELECT COUNT(*)::bigint AS cnt FROM backtest_task
        WHERE is_kept = false
          AND (status != 'completed' OR created_at >= NOW() - INTERVAL '7 days')
        "#,
    )
    .fetch_one(db)
    .await
    .map_err(|e| format!("查询活跃任务失败: {e}"))?;
    let active_count: i64 = row3.get(0);

    Ok(json!({
        "expired_cleanable": expired_count,
        "kept": kept_count,
        "active_not_expired": active_count,
        "policy": "is_kept=false + status=completed + 超过7天 → 自动清理",
    }))
}
