/// 数据同步路由
use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::json;
use std::{
    sync::Arc,
};
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;

use crate::AppState;

use super::*;

#[derive(Debug, Clone, Deserialize)]
pub struct DataSyncTaskReq {
    pub dataset: String,
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub source_filters: Vec<String>,
    #[serde(default)]
    pub index_codes: Vec<String>,
    #[serde(default)]
    pub exchanges: Vec<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    #[serde(default)]
    pub data_version_id: Option<String>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub quality_check: bool,
    #[serde(default)]
    pub create_data_version: bool,
    #[serde(default)]
    pub retry_of_task_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]


pub struct CleanupStaleSyncTasksReq {
    #[serde(default)]
    pub dry_run: bool,
    pub default_timeout_seconds: Option<i64>,
    pub limit: Option<i64>,
}



pub async fn create_sync_task(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DataSyncTaskReq>,
) -> impl IntoResponse {
    let task_id = req
        .data_version_id
        .clone()
        .unwrap_or_else(|| format!("data-sync-{}", Uuid::new_v4()));

    if let Err(message) = register_sync_task(&state, &task_id, &req, "running").await {
        return Json(json!({"code": 1, "message": message}));
    }

    if req.background {
        let state_for_task = state.clone();
        let task_id_for_task = task_id.clone();
        let req_for_task = req.clone();
        crate::sync_task_registry::spawn_sync_task(
            state.sync_tasks.clone(),
            task_id.clone(),
            async move {
                if let Err(message) = execute_sync_task(
                    state_for_task.clone(),
                    task_id_for_task.clone(),
                    req_for_task,
                )
                .await
                {
                    let _ = quant_data::repository::fail_sync_task(
                        &state_for_task.db,
                        &task_id_for_task,
                        &message,
                    )
                    .await;
                    tracing::error!(task_id = %task_id_for_task, error = %message, "统一同步任务失败");
                }
            },
        )
        .await;

        return Json(json!({"code": 0, "data": {
            "task_id": task_id,
            "status": "running",
            "dataset": req.dataset,
            "mode": req.mode,
            "quality_check": req.quality_check,
            "create_data_version": req.create_data_version,
            "reason": req.reason
        }}));
    }

    match execute_sync_task(state.clone(), task_id.clone(), req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => {
            let _ = quant_data::repository::fail_sync_task(&state.db, &task_id, &message).await;
            Json(json!({"code": 1, "message": message, "task_id": task_id}))
        }
    }
}

/// POST /api/v1/quant/data/sync/stock-basic


pub async fn sync_task_status(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let row: Option<(
        String,
        String,
        Option<Vec<String>>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT task_type, status, symbols, start_date, end_date,
                    total_count, success_count, failed_count, progress,
                    last_heartbeat_at, heartbeat_timeout_seconds,
                    retry_of_task_id, error_message
             FROM data_sync_task WHERE task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    match row {
        Some((
            task_type,
            status,
            symbols,
            start_date,
            end_date,
            total,
            success,
            failed,
            progress,
            last_heartbeat_at,
            heartbeat_timeout_seconds,
            retry_of_task_id,
            error_message,
        )) => {
            let stale = match (last_heartbeat_at, heartbeat_timeout_seconds) {
                (Some(last), Some(timeout)) if status == "running" => {
                    chrono::Utc::now().signed_duration_since(last).num_seconds() > timeout as i64
                }
                _ => false,
            };
            Json(json!({"code": 0, "data": {
                "task_id": task_id,
                "task_type": task_type,
                "status": status,
                "symbols": symbols.unwrap_or_default(),
                "start_date": start_date.map(|date| date.to_string()),
                "end_date": end_date.map(|date| date.to_string()),
                "total": total,
                "success": success,
                "failed": failed,
                "progress": progress,
                "last_heartbeat_at": fmt_rfc3339_local(last_heartbeat_at),
                "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
                "stale": stale,
                "retry_of_task_id": retry_of_task_id,
                "error_message": error_message,
            }}))
        }
        None => Json(json!({"code": 1, "message": "task not found"})),
    }
}

/// POST /api/v1/quant/data/sync-tasks/cleanup-stale
///
/// 将 heartbeat 超时的 running 同步任务标记为 failed，将超时的 cancel_requested
/// 任务收敛为 cancelled。默认 dry-run=false；可用 dry_run=true 先查看候选任务，
/// 避免误伤仍在正常推进的后台任务。


pub async fn cleanup_stale_sync_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CleanupStaleSyncTasksReq>,
) -> impl IntoResponse {
    let default_timeout_seconds =
        stale_cleanup_default_timeout_seconds(req.default_timeout_seconds);
    let limit = stale_cleanup_limit(req.limit);

    let rows: Vec<(
        String,
        String,
        String,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT task_id, task_type, status, progress, last_heartbeat_at,
                heartbeat_timeout_seconds, started_at, created_at
         FROM data_sync_task
         WHERE status IN ('running', 'cancel_requested')
           AND COALESCE(last_heartbeat_at, started_at, created_at)
               < now() - (COALESCE(heartbeat_timeout_seconds, $1)::text || ' seconds')::interval
         ORDER BY COALESCE(last_heartbeat_at, started_at, created_at) ASC
         LIMIT $2",
    )
    .bind(default_timeout_seconds as i32)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let candidates = rows
        .iter()
        .map(
            |(
                task_id,
                task_type,
                status,
                progress,
                last_heartbeat_at,
                heartbeat_timeout_seconds,
                started_at,
                created_at,
            )| {
                let observed_at = last_heartbeat_at.or(*started_at).or(*created_at);
                let timeout_seconds = heartbeat_timeout_seconds
                    .map(i64::from)
                    .unwrap_or(default_timeout_seconds);
                json!({
                    "task_id": task_id,
                    "task_type": task_type,
                    "status": status,
                    "progress": progress,
                    "last_heartbeat_at": fmt_rfc3339_local(*last_heartbeat_at),
                    "started_at": fmt_rfc3339_local(*started_at),
                    "created_at": fmt_rfc3339_local(*created_at),
                    "observed_at": fmt_rfc3339_local(observed_at),
                    "heartbeat_timeout_seconds": timeout_seconds,
                    "cleanup_action": stale_sync_task_cleanup_action(status),
                    "terminal_status": stale_sync_task_cleanup_terminal_status(status)
                })
            },
        )
        .collect::<Vec<_>>();

    if req.dry_run || rows.is_empty() {
        return Json(json!({"code": 0, "data": {
            "dry_run": true,
            "candidate_count": candidates.len(),
            "updated_count": 0,
            "candidates": candidates
        }}));
    }

    let task_ids = rows
        .iter()
        .map(|(task_id, ..)| task_id.clone())
        .collect::<Vec<_>>();
    let result = sqlx::query(
        "UPDATE data_sync_task
         SET status = CASE
                 WHEN status = 'cancel_requested' THEN 'cancelled'
                 ELSE 'failed'
             END,
             failed_count = CASE
                 WHEN status = 'running' THEN GREATEST(COALESCE(failed_count, 0), 1)
                 ELSE COALESCE(failed_count, 0)
             END,
             completed_at = now(),
             last_heartbeat_at = now(),
             error_message = CONCAT(
                 COALESCE(NULLIF(error_message, '') || '; ', ''),
                 CASE
                     WHEN status = 'cancel_requested' THEN
                         'stale cancel_requested task finalized by cleanup-stale: no worker acknowledgement within configured timeout'
                     ELSE
                         'stale running task timed out by cleanup-stale: no heartbeat within configured timeout'
                 END
             )
         WHERE task_id = ANY($1) AND status IN ('running', 'cancel_requested')",
    )
    .bind(&task_ids)
    .execute(&state.db)
    .await;

    match result {
        Ok(result) => Json(json!({"code": 0, "data": {
            "dry_run": false,
            "candidate_count": candidates.len(),
            "updated_count": result.rows_affected(),
            "candidates": candidates
        }})),
        Err(error) => Json(
            json!({"code": 1, "message": format!("cleanup stale sync tasks failed: {}", error)}),
        ),
    }
}



pub async fn cancel_sync_task(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status
         FROM data_sync_task
         WHERE task_id = $1",
    )
    .bind(&task_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let Some(status) = status else {
        return Json(json!({"code": 1, "message": "task not found"}));
    };

    let Some(next_status) = sync_task_cancel_transition(&status) else {
        return Json(json!({
            "code": 1,
            "message": format!("task cannot be cancelled from status {}", status),
            "data": {
                "task_id": task_id,
                "status": status,
            }
        }));
    };

    let result = sqlx::query(
        "UPDATE data_sync_task
         SET status = $2,
             error_message = COALESCE(error_message, 'cancel requested by user'),
             last_heartbeat_at = now(),
             completed_at = CASE WHEN $2 = 'cancelled' THEN now() ELSE completed_at END
         WHERE task_id = $1 AND status = $3",
    )
    .bind(&task_id)
    .bind(next_status)
    .bind(&status)
    .execute(&state.db)
    .await;

    match result {
        Ok(result) if result.rows_affected() == 1 => {
            // DB status 已改为 cancel_requested/cancelled,
            // 再主动 abort tokio task(双保险:循环内 cancel 检查 + tokio abort)。
            // running → cancel_requested 时 task 仍在跑,abort 立即终止;
            // pending → cancelled 时 task 可能未启动,abort 无副作用。
            let aborted = state.sync_tasks.abort(&task_id).await;
            Json(json!({
                "code": 0,
                "data": {
                    "task_id": task_id,
                    "previous_status": status,
                    "status": next_status,
                    "tokio_aborted": aborted,
                }
            }))
        }
        Ok(_) => Json(json!({
            "code": 1,
            "message": "task status changed before cancel request was applied",
            "data": {
                "task_id": task_id,
                "previous_status": status,
            }
        })),
        Err(error) => Json(json!({
            "code": 1,
            "message": format!("Failed to cancel sync task: {}", error)
        })),
    }
}


