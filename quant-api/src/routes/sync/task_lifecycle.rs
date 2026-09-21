/// 数据同步路由
use axum::{extract::State, response::IntoResponse, Json};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
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

// ─── 第三批补充测试（非 ignored，秒级，真实本机 PG）─────────────────────
//
// 安全边界：不触发真实数据同步（create_sync_task 只测参数校验早退分支）；
// 写库仅限 data_sync_task 的 zzz_test_ 前缀独占任务行（前置+结尾精确键清理）。

#[cfg(test)]
mod third_batch {
    use super::*;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use std::sync::Arc;

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    async fn test_state() -> Arc<crate::AppState> {
        let _ = dotenv::from_filename("../.env");
        let _ = dotenv::dotenv();
        Arc::new(crate::AppState {
            start_time: chrono::Utc::now(),
            db: test_db().await,
            tushare: quant_data::tushare::client::TushareClient::from_env()
                .expect("Tushare client init (需 TUSHARE_TOKEN: source ../.env)"),
            sync_tasks: crate::sync_task_registry::new_registry(),
        })
    }

    async fn resp_json(resp: impl IntoResponse) -> serde_json::Value {
        let body = resp.into_response().into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("json response body")
    }

    /// 精确键清理测试任务行（禁 LIKE 宽前缀，防并行竞态）
    async fn cleanup_task(db: &sqlx::PgPool, task_id: &str) {
        let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
            .bind(task_id)
            .execute(db)
            .await;
    }

    // ── 请求反序列化默认值 ──

    #[test]
    fn data_sync_task_req_defaults_source_to_tushare() {
        // dataset 必填，其余字段全部有安全默认值
        let req: DataSyncTaskReq =
            serde_json::from_str(r#"{"dataset":"zzz_test"}"#).expect("minimal req");
        assert_eq!(req.dataset, "zzz_test");
        assert_eq!(req.source, "tushare");
        assert_eq!(req.mode, None);
        assert!(req.symbols.is_empty());
        assert!(!req.background);
        assert!(!req.quality_check);
        assert!(!req.create_data_version);
        assert_eq!(req.retry_of_task_id, None);

        // 缺 dataset → 反序列化失败（axum 层 400）
        assert!(serde_json::from_str::<DataSyncTaskReq>(r#"{}"#).is_err());
    }

    // ── create_sync_task 参数校验早退（零写入）──

    #[tokio::test]
    async fn create_sync_task_rejects_non_yyyymmdd_dates_before_db_write() {
        let state = test_state().await;
        let req = DataSyncTaskReq {
            dataset: "zzz_test".to_string(),
            source: "tushare".to_string(),
            mode: None,
            symbols: vec![],
            source_filters: vec![],
            index_codes: vec![],
            exchanges: vec![],
            // register_sync_task 仅接受 YYYYMMDD；连字符格式在写库前被拒
            start_date: Some("2026-01-01".to_string()),
            end_date: None,
            data_version_id: None,
            background: false,
            quality_check: false,
            create_data_version: false,
            retry_of_task_id: None,
            reason: None,
        };
        let resp = create_sync_task(State(state), Json(req)).await;
        let body = resp_json(resp).await;
        assert_eq!(body["code"], 1, "非法日期应拒绝: {body}");
        assert_eq!(
            body["message"], "date must use YYYYMMDD format: 2026-01-01",
            "错误信息应指明需要的日期格式"
        );
    }

    // ── sync_task_status 查询 ──

    #[tokio::test]
    async fn sync_task_status_reports_missing_task() {
        let state = test_state().await;
        let resp = sync_task_status(
            State(state),
            axum::extract::Path("zzz_test_third_batch_no_such_task".to_string()),
        )
        .await;
        let body = resp_json(resp).await;
        assert_eq!(body["code"], 1);
        assert_eq!(body["message"], "task not found");
    }

    #[tokio::test]
    async fn sync_task_status_returns_terminal_task_without_stale_flag() {
        let db = test_db().await;
        let task_id = "zzz_test_third_batch_status_completed";
        cleanup_task(&db, task_id).await;

        quant_data::repository::create_sync_task_with_context(
            &db,
            task_id,
            "zzz_test_dataset",
            "tushare",
            None,
            None,
            None,
            "completed",
            None,
        )
        .await
        .expect("seed completed task");

        let state = test_state().await;
        let resp = sync_task_status(State(state), axum::extract::Path(task_id.to_string())).await;
        let body = resp_json(resp).await;
        // 终态任务无心跳超时判定：stale 仅对 running 状态计算
        assert_eq!(body["code"], 0, "{body}");
        assert_eq!(body["data"]["task_id"], task_id);
        assert_eq!(body["data"]["task_type"], "zzz_test_dataset");
        assert_eq!(body["data"]["status"], "completed");
        assert_eq!(body["data"]["stale"], false);

        cleanup_task(&db, task_id).await;
    }

    // ── cancel_sync_task 状态机 ──

    #[tokio::test]
    async fn cancel_sync_task_reports_missing_task() {
        let state = test_state().await;
        let resp = cancel_sync_task(
            State(state),
            axum::extract::Path("zzz_test_third_batch_no_such_task".to_string()),
        )
        .await;
        let body = resp_json(resp).await;
        assert_eq!(body["code"], 1);
        assert_eq!(body["message"], "task not found");
    }

    #[tokio::test]
    async fn cancel_sync_task_refuses_terminal_completed_status() {
        let db = test_db().await;
        let task_id = "zzz_test_third_batch_cancel_completed";
        cleanup_task(&db, task_id).await;

        quant_data::repository::create_sync_task_with_context(
            &db,
            task_id,
            "zzz_test_dataset",
            "tushare",
            None,
            None,
            None,
            "completed",
            None,
        )
        .await
        .expect("seed completed task");

        let state = test_state().await;
        let resp = cancel_sync_task(State(state), axum::extract::Path(task_id.to_string())).await;
        let body = resp_json(resp).await;
        // completed 无合法取消迁移 → 拒绝并回显当前状态
        assert_eq!(body["code"], 1, "{body}");
        assert_eq!(
            body["message"],
            "task cannot be cancelled from status completed"
        );
        assert_eq!(body["data"]["status"], "completed");

        // 任务行未被改动
        let status: String =
            sqlx::query_scalar("SELECT status FROM data_sync_task WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&db)
                .await
                .expect("task row");
        assert_eq!(status, "completed");

        cleanup_task(&db, task_id).await;
    }

    #[tokio::test]
    async fn cancel_sync_task_moves_running_to_cancel_requested() {
        let db = test_db().await;
        let task_id = "zzz_test_third_batch_cancel_running";
        cleanup_task(&db, task_id).await;

        // running 任务注册时带 started_at/last_heartbeat_at（repository 语义）
        quant_data::repository::create_sync_task_with_context(
            &db,
            task_id,
            "zzz_test_dataset",
            "tushare",
            None,
            None,
            None,
            "running",
            None,
        )
        .await
        .expect("seed running task");

        let state = test_state().await;
        let resp = cancel_sync_task(State(state), axum::extract::Path(task_id.to_string())).await;
        let body = resp_json(resp).await;
        assert_eq!(body["code"], 0, "{body}");
        assert_eq!(body["data"]["task_id"], task_id);
        assert_eq!(body["data"]["previous_status"], "running");
        assert_eq!(body["data"]["status"], "cancel_requested");
        // 任务不在本测试进程的注册表中 → tokio abort 无目标，返回 false
        assert_eq!(body["data"]["tokio_aborted"], false);

        // DB 状态确实迁移，且 error_message 兜底写入取消原因
        let (status, error): (String, Option<String>) =
            sqlx::query_as("SELECT status, error_message FROM data_sync_task WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&db)
                .await
                .expect("task row");
        assert_eq!(status, "cancel_requested");
        assert_eq!(error.as_deref(), Some("cancel requested by user"));

        cleanup_task(&db, task_id).await;
    }

    // ── cleanup_stale_sync_tasks dry-run（只读）──

    #[tokio::test]
    async fn cleanup_stale_sync_tasks_dry_run_never_updates() {
        let state = test_state().await;
        let resp = cleanup_stale_sync_tasks(
            State(state),
            Json(CleanupStaleSyncTasksReq {
                dry_run: true,
                default_timeout_seconds: Some(1),
                limit: Some(5),
            }),
        )
        .await;
        let body = resp_json(resp).await;
        // dry-run 固定返回 dry_run=true / updated_count=0；候选数与明细一致
        assert_eq!(body["code"], 0, "{body}");
        assert_eq!(body["data"]["dry_run"], true);
        assert_eq!(body["data"]["updated_count"], 0);
        let candidates = body["data"]["candidates"].as_array().expect("candidates");
        assert_eq!(body["data"]["candidate_count"], candidates.len());
        // 每个候选都带终态收敛动作语义
        for candidate in candidates {
            assert!(candidate["cleanup_action"].is_string());
            assert!(candidate["terminal_status"].is_string());
        }
    }
}
