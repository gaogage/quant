//! 仪表盘"生产链路健康"聚合 API（2026-09-24 立项）。
//!
//! 驾驶舱一瞥三要素：调度任务态势 / 今日调仓 / 信号导出，外加数据新鲜度锚点。
//! 数据源全部现成（scheduled_task_config / paper_order / 信号文件系统 / 数据表
//! MAX(trade_date)），本模块只做只读聚合——不引入新状态、不重复数据健康页的
//! 全量口径（那页管治理细节，这里管"现在生产是否正常"）。

use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;

use crate::AppState;

/// GET /api/v1/quant/dashboard/pipeline-health
pub async fn pipeline_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = chrono::Utc::now();

    // ── 1. 调度任务态势（启用任务：最近执行 + 下次排期）──
    let tasks: Vec<(
        String,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT task_name, task_type, last_run_at, last_status, next_run_at
             FROM scheduled_task_config WHERE enabled = true
             ORDER BY next_run_at NULLS FIRST",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    let tasks_json: Vec<serde_json::Value> = tasks
        .iter()
        .map(|(name, ttype, last_run, last_status, next_run)| {
            // 迟到判定：排期已过 10 分钟仍未执行（catch-up 语义下正常偏差 <1min）
            let overdue = match next_run {
                Some(nr) => now > *nr + chrono::Duration::minutes(10),
                None => false,
            };
            json!({
                "task_name": name,
                "task_type": ttype,
                "last_run_at": last_run.map(|t| t.to_rfc3339()),
                "last_status": last_status,
                "next_run_at": next_run.map(|t| t.to_rfc3339()),
                "overdue": overdue,
            })
        })
        .collect();

    // ── 2. 今日调仓（当日成交：笔数 + 首末时刻，按账户分组）──
    let today = chrono::Local::now().date_naive();
    let fills: Vec<(
        String,
        i64,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT paper_account_id, COUNT(*)::bigint, MIN(created_at), MAX(created_at)
             FROM paper_order WHERE created_at::date = $1
             GROUP BY paper_account_id ORDER BY paper_account_id",
    )
    .bind(today)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    let fills_json: Vec<serde_json::Value> = fills
        .iter()
        .map(|(aid, cnt, first, last)| {
            json!({
                "account_id": aid,
                "orders": cnt,
                "first_at": first.map(|t| t.to_rfc3339()),
                "last_at": last.map(|t| t.to_rfc3339()),
            })
        })
        .collect();

    // ── 3. 信号导出（最新信号文件 + 导出时刻；文件名含目标交易日）──
    // 信号目录约定：/tmp/quant_signals/<account_id>/signal_YYYYMMDD_001.json
    let mut latest_signal: Option<(String, String, std::time::SystemTime)> = None;
    if let Ok(entries) = std::fs::read_dir("/tmp/quant_signals") {
        for acct_dir in entries.flatten() {
            let acct = acct_dir.file_name().to_string_lossy().to_string();
            if let Ok(files) = std::fs::read_dir(acct_dir.path()) {
                for f in files.flatten() {
                    let fname = f.file_name().to_string_lossy().to_string();
                    if !fname.starts_with("signal_") || !fname.ends_with(".json") {
                        continue;
                    }
                    if let Ok(meta) = f.metadata() {
                        if let Ok(mtime) = meta.modified() {
                            let better = match &latest_signal {
                                Some((_, _, t)) => mtime > *t,
                                None => true,
                            };
                            if better {
                                latest_signal = Some((acct.clone(), fname, mtime));
                            }
                        }
                    }
                }
            }
        }
    }
    let signal_json = latest_signal
        .map(|(acct, fname, mtime)| {
            // 文件名 signal_YYYYMMDD_001.json → 目标交易日
            let target_day = fname
                .strip_prefix("signal_")
                .and_then(|r| r.get(0..8))
                .map(|s| s.to_string());
            let exported_at = mtime
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp(d.as_secs() as i64, 0)
                        .map(|t| t.to_rfc3339())
                });
            json!({
                "account_id": acct,
                "file": fname,
                "target_trade_date": target_day,
                "exported_at": exported_at,
            })
        })
        .unwrap_or(serde_json::Value::Null);

    // ── 4. 数据新鲜度锚点（三查：日线/因子截面/combo 保鲜）──
    let bar_latest: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(trade_date) FROM market_stock_daily_bar")
            .fetch_one(&state.db)
            .await
            .ok()
            .flatten();
    let mfv_latest: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(trade_date) FROM multi_factor_value")
            .fetch_one(&state.db)
            .await
            .ok()
            .flatten();
    let nav_latest: Option<chrono::NaiveDate> =
        sqlx::query_scalar("SELECT MAX(snapshot_date) FROM paper_nav_snapshot")
            .fetch_one(&state.db)
            .await
            .ok()
            .flatten();

    let any_overdue = tasks_json
        .iter()
        .any(|t| t["overdue"].as_bool().unwrap_or(false));
    let any_failed = tasks_json.iter().any(|t| {
        t["last_status"].as_str() == Some("failed")
            || t["last_status"].as_str() == Some("unhandled")
    });

    Json(json!({
        "code": 0,
        "data": {
            "generated_at": now.to_rfc3339(),
            "overall": if any_failed { "degraded" } else if any_overdue { "warning" } else { "healthy" },
            "tasks": tasks_json,
            "today_rebalance": {
                "date": today.format("%Y-%m-%d").to_string(),
                "accounts": fills_json,
            },
            "signal_export": signal_json,
            "freshness": {
                "bar_latest": bar_latest.map(|d| d.format("%Y-%m-%d").to_string()),
                "factor_latest": mfv_latest.map(|d| d.format("%Y-%m-%d").to_string()),
                "nav_latest": nav_latest.map(|d| d.format("%Y-%m-%d").to_string()),
            },
        }
    }))
}
