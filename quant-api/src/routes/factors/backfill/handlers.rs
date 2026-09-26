//! phase7 因子回填 axum handlers：40 个 backfill_*_background 路由处理
//! （同构模式：注册 data_sync_task → 后台调度 run_*）。
use super::*;
use crate::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;

pub async fn backfill_phase7_price_volume_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7PriceVolumeBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_price_volume_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_price_volume_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 price-volume backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 price-volume backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 price-volume backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/p42b-large-cap-momentum-reversal-backfill/background
///
/// Set-based daily backfill for the P4.2b large-cap momentum-reversal
/// interaction factor (`large_cap_mom_rev_daily_std`): within the large-cap
/// pool (total_mv > threshold), CUME_DIST(reversal) x CUME_DIST(momentum)
/// interaction, cross-sectional percent_rank normalization.
pub async fn backfill_p42b_large_cap_momentum_reversal_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<P42bLargeCapMomentumReversalBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create p42b large-cap momentum-reversal backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_p42b_large_cap_momentum_reversal_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = p42b_large_cap_momentum_reversal_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist P4.2b large-cap momentum-reversal backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "P4.2b large-cap momentum-reversal backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "P4.2b large-cap momentum-reversal backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

#[derive(Debug, serde::Deserialize)]

pub struct P42bLargeCapMomentumReversalBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

impl P42bLargeCapMomentumReversalBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<SetBasedFactorBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "p42b_large_cap_mom_rev_daily_std",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(SetBasedFactorBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "p42b_large_cap_momentum_reversal_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "p42b_large_cap_mom_rev_daily_std",
            category: "price_volume",
            phase: "P4.2b",
            dependencies: &["market_stock_daily_bar_adj", "market_stock_daily_basic"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

/// POST /api/v1/quant/factors/p42b-defensive-low-vol-quality-backfill/background
///
/// Set-based daily backfill for the P4.2b defensive low-volatility quality
/// interaction factor (`defensive_lowvol_quality_daily_std`): within the
/// defensive industry pool, CUME_DIST(-volatility) x CUME_DIST(fin_roe)
/// interaction, cross-sectional percent_rank normalization.
pub async fn backfill_p42b_defensive_low_vol_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<P42bDefensiveLowVolQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create p42b defensive low-vol quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_p42b_defensive_low_vol_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = p42b_defensive_low_vol_quality_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist P4.2b defensive low-vol quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "P4.2b defensive low-vol quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "P4.2b defensive low-vol quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

#[derive(Debug, serde::Deserialize)]

pub struct P42bDefensiveLowVolQualityBackfillRequest {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub version: Option<String>,
    pub combo_name: Option<String>,
    pub statement_timeout_ms: Option<u64>,
}

impl P42bDefensiveLowVolQualityBackfillRequest {
    pub(crate) fn into_plan(self) -> Result<SetBasedFactorBackfillPlan, String> {
        let start_date = parse_phase7_backfill_date(
            self.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).expect("static date"),
            "start_date",
        )?;
        let end_date =
            parse_phase7_backfill_date(self.end_date, chrono::Utc::now().date_naive(), "end_date")?;

        if start_date > end_date {
            return Err("start_date must be <= end_date".to_string());
        }

        let version = trim_or_default(self.version, "1.0.0", "version")?;
        let combo_name = trim_or_default(
            self.combo_name,
            "defensive_lowvol_quality_daily_std",
            "combo_name",
        )?;

        if version.len() > 32 {
            return Err("version must be <= 32 chars".to_string());
        }
        if combo_name.len() > 128 {
            return Err("combo_name must be <= 128 chars".to_string());
        }

        Ok(SetBasedFactorBackfillPlan {
            start_date,
            end_date,
            version,
            combo_name,
            statement_timeout_ms: self.statement_timeout_ms.unwrap_or(0),
            task_type: "p42b_defensive_low_vol_quality_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "defensive_lowvol_quality_daily_std",
            category: "price_volume",
            phase: "P4.2b",
            dependencies: &["market_stock_daily_bar_adj", "market_stock", "factor_value"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: Vec::new(),
        })
    }
}

/// POST /api/v1/quant/factors/phase7-financial-quality-backfill/background
///
/// Set-based daily PIT carry-forward backfill for the Phase 7 financial
/// quality alpha bundle and its equal-weight combo score.
pub async fn backfill_phase7_financial_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7FinancialQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 financial quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_financial_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_financial_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 financial quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 financial quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 financial quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-financial-quality-change-backfill/background
///
/// Set-based daily PIT backfill for financial quality YoY acceleration sources.
pub async fn backfill_phase7_financial_quality_change_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7FinancialQualityChangeBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 financial quality change backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_financial_quality_change_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_financial_quality_change_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 financial quality change backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 financial quality change backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(
                    task_id = %tid,
                    error = %error,
                    "Phase 7 financial quality change backfill failed"
                );
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-earnings-recovery-persistence-backfill/background
///
/// Set-based daily PIT backfill for multi-period earnings recovery persistence sources.
pub async fn backfill_phase7_earnings_recovery_persistence_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EarningsRecoveryPersistenceBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 earnings recovery persistence backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_earnings_recovery_persistence_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_earnings_recovery_persistence_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 earnings recovery persistence backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 earnings recovery persistence backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(
                    task_id = %tid,
                    error = %error,
                    "Phase 7 earnings recovery persistence backfill failed"
                );
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-industry-residual-quality-backfill/background
///
/// Set-based daily PIT financial quality backfill that removes same-industry
/// mean exposure before ranking, so discovery can test quality alpha beyond
/// broad industry tilts.
pub async fn backfill_phase7_industry_residual_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7IndustryResidualQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 industry-residual quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_industry_residual_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_industry_residual_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 industry-residual quality profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 industry-residual quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 industry-residual quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-relative-strength-backfill/background
///
/// Set-based market/industry-relative momentum backfill for expanding Phase 7
/// alpha sources beyond absolute price-volume and financial quality signals.
pub async fn backfill_phase7_relative_strength_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7RelativeStrengthBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 relative strength backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_relative_strength_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_relative_strength_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 relative strength backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 relative strength backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 relative strength backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-quality-relative-strength-backfill/background
///
/// Combo-only backfill for the Phase 7 composite alpha candidate that blends
/// daily PIT financial quality with market/industry relative strength.
pub async fn backfill_phase7_quality_relative_strength_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7QualityRelativeStrengthBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 quality-relative-strength backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_quality_relative_strength_combo_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_quality_relative_strength_combo_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 quality-relative-strength profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    combo_rows = report.combo_rows,
                    "Phase 7 quality-relative-strength combo backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 quality-relative-strength backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-growth-recovery-backfill/background
///
/// Set-based daily PIT backfill for non-momentum fundamental growth and
/// earnings-recovery alpha sources.
pub async fn backfill_phase7_growth_recovery_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7GrowthRecoveryBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 growth recovery backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_growth_recovery_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = report.total_rows();
                let total_rows = usize_to_i32(total_rows);
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_growth_recovery_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 growth-recovery backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 growth-recovery backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 growth-recovery backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-valuation-backfill/background
///
/// Set-based daily valuation alpha backfill from `market_stock_daily_basic`.
pub async fn backfill_phase7_valuation_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ValuationBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 valuation backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_valuation_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_valuation_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 valuation backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 valuation backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 valuation backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-moneyflow-backfill/background
///
/// Set-based rolling moneyflow alpha backfill from `market_stock_moneyflow`.
pub async fn backfill_phase7_moneyflow_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MoneyflowBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 moneyflow backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_moneyflow_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_moneyflow_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 moneyflow backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 moneyflow backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 moneyflow backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-moneyflow-congestion-backfill/background
///
/// Set-based rolling moneyflow alpha backfill adjusted by capacity crowding.
pub async fn backfill_phase7_moneyflow_congestion_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MoneyflowCongestionBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 moneyflow congestion backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_moneyflow_congestion_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_moneyflow_congestion_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 moneyflow congestion backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 moneyflow congestion backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(
                    task_id = %tid,
                    error = %error,
                    "Phase 7 moneyflow congestion backfill failed"
                );
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-cashflow-quality-backfill/background
///
/// Set-based PIT cashflow quality alpha backfill from `market_stock_cashflow`.
pub async fn backfill_phase7_cashflow_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7CashflowQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 cashflow quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_cashflow_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_cashflow_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 cashflow quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 cashflow quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 cashflow quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-dividend-quality-backfill/background
///
/// Set-based PIT dividend quality alpha backfill from `market_stock_dividend`.
pub async fn backfill_phase7_dividend_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7DividendQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 dividend quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_dividend_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_dividend_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 dividend quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 dividend quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 dividend quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-event-alpha-backfill/background
///
/// Set-based PIT event alpha backfill from forecast, express, and disclosure
/// date event tables.
pub async fn backfill_phase7_event_alpha_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EventAlphaBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 event alpha backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_event_alpha_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_event_alpha_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 event alpha backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 event alpha backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 event alpha backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-event-surprise-backfill/background
///
/// Set-based PIT event surprise backfill. This keeps the ordinary-permission
/// forecast, express, and disclosure data path, but turns available fields into
/// bucketed/nonlinear event surprise factors.
pub async fn backfill_phase7_event_surprise_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EventSurpriseBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 event surprise backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_event_surprise_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_event_surprise_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 event surprise backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 event surprise backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 event surprise backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-forecast-revision-surprise-backfill/background
///
/// Set-based PIT forecast revision surprise backfill. This source uses only
/// same-symbol/same-period forecast revisions that were both visible by the
/// signal date.
pub async fn backfill_phase7_forecast_revision_surprise_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ForecastRevisionSurpriseBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 forecast revision surprise backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_forecast_revision_surprise_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_forecast_revision_surprise_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 forecast revision surprise backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 forecast revision surprise backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 forecast revision surprise backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-repurchase-supply-shock-backfill/background
///
/// Set-based PIT supply-demand shock alpha backfill from repurchase announcements.
pub async fn backfill_phase7_repurchase_supply_shock_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7RepurchaseSupplyShockBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 repurchase supply shock backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_repurchase_supply_shock_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_repurchase_supply_shock_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 repurchase supply shock backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 repurchase supply shock backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 repurchase supply shock backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-block-trade-supply-demand-backfill/background
///
/// Set-based PIT supply-demand alpha backfill from block-trade disclosures.
pub async fn backfill_phase7_block_trade_supply_demand_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7BlockTradeSupplyDemandBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 block-trade supply-demand backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result =
            run_phase7_block_trade_supply_demand_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_block_trade_supply_demand_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 block-trade supply-demand backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 block-trade supply-demand backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 block-trade supply-demand backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-unlock-supply-pressure-backfill/background
///
/// Set-based PIT unlock pressure alpha backfill from share_float announcements.
/// POST /api/v1/quant/factors/phase7-limit-pressure-backfill/background
///
/// Set-based PIT limit net pressure alpha backfill from market_stock_limit.
/// P3 第二轮（2026-09-26）：涨跌停净压力 inverse 因子（ICIR -0.803 验证投产）。
pub async fn backfill_phase7_limit_pressure_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7LimitPressureBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 limit-pressure backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_limit_pressure_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_limit_pressure_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 limit-pressure backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 limit-pressure backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 limit-pressure backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

pub async fn backfill_phase7_unlock_supply_pressure_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7UnlockSupplyPressureBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 unlock supply pressure backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_unlock_supply_pressure_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_unlock_supply_pressure_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 unlock supply pressure backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 unlock supply pressure backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 unlock supply pressure backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-supply-float-shock-backfill/background
///
/// Set-based PIT broad supply proxy from daily float/total market value and
/// unadjusted close. This approximates share-base changes without using static
/// industry membership or future corporate-action knowledge.
pub async fn backfill_phase7_supply_float_shock_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7SupplyFloatShockBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 supply float shock backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_supply_float_shock_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_supply_float_shock_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 supply float shock backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 supply float shock backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 supply float shock backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-liquidity-quality-backfill/background
///
/// Set-based PIT broad-base liquidity-quality alpha from daily price, traded
/// amount, and same-day float market value snapshots.
pub async fn backfill_phase7_liquidity_quality_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7LiquidityQualityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 liquidity quality backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_liquidity_quality_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_liquidity_quality_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 liquidity quality backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 liquidity quality backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 liquidity quality backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-market-residual-risk-backfill/background
///
/// Set-based PIT broad-base market beta and residual-risk alpha from stock
/// daily returns and same-day CSI 300 index returns.
pub async fn backfill_phase7_market_residual_risk_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MarketResidualRiskBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 market residual risk backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_market_residual_risk_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_market_residual_risk_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 market residual risk backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 market residual risk backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 market residual risk backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-industry-prosperity-backfill/background
///
/// Dedicated PIT market-scope industry prosperity proxy. It only runs with the
/// alpha admission gate and uses PIT industry membership to restrict evaluation
/// to the main-board + ChiNext scope approved by coverage audit.
pub async fn backfill_phase7_industry_prosperity_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7IndustryProsperityBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 industry prosperity backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_industry_prosperity_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_industry_prosperity_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 industry prosperity backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 industry prosperity backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 industry prosperity backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-futures-price-chain-backfill/background
///
/// Dedicated PIT futures price-chain factor builder. It requires the coverage
/// admission gate and only writes research-source factor/multi-factor rows for
/// P3.10 diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_futures_price_chain_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7FuturesPriceChainBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 futures price-chain backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_futures_price_chain_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_futures_price_chain_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 futures price-chain backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 futures price-chain backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 futures price-chain backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-equity-pledge-pressure-backfill/background
///
/// Dedicated PIT equity pledge pressure factor builder. It requires the raw
/// coverage admission gate and only writes a research-source combo for P3.10
/// diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_equity_pledge_pressure_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EquityPledgePressureBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 equity pledge pressure backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_equity_pledge_pressure_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_equity_pledge_pressure_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 equity pledge pressure backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 equity pledge pressure backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 equity pledge pressure backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-event-window-alpha-backfill/background
///
/// Set-based PIT event-window alpha backfill from forecast, express, and
/// disclosure-date event tables. Unlike latest-event carry-forward, this keeps
/// each event signal alive only for a decayed post-event window.
pub async fn backfill_phase7_event_window_alpha_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7EventWindowAlphaBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 event-window alpha backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_event_window_alpha_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_event_window_alpha_backfill_specs_for_plan(&task_plan);
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 event-window alpha backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 event-window alpha backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 event-window alpha backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-shareholder-structure-backfill/background
///
/// Dedicated PIT shareholder-structure low-fanout factor builder. It requires
/// the strict low-fanout admission gate and writes only a research-source combo
/// for P3.10 diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_shareholder_structure_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7ShareholderStructureBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 shareholder structure backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_shareholder_structure_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_shareholder_structure_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 shareholder structure backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 shareholder structure backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 shareholder structure backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-margin-detail-backfill/background
///
/// Dedicated PIT margin-detail leverage-crowding factor builder. It requires
/// the margin-detail coverage gate and writes only a research-source combo for
/// P3.10 diagnostics; WFA and v19 train selection remain separate gates.
pub async fn backfill_phase7_margin_detail_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7MarginDetailBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 margin detail backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_margin_detail_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_margin_detail_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 margin detail backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 margin detail backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 margin detail backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-analyst-revision-backfill/background
///
/// Dedicated PIT AkShare/multi-vendor analyst-revision factor builder. It
/// requires the full-history coverage/PIT/correlation admission gate and writes
/// only a research-source combo for P3.10 diagnostics; WFA and v19 train
/// selection remain separate gates.
pub async fn backfill_phase7_analyst_revision_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7AnalystRevisionBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 analyst revision backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_analyst_revision_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                let specs = phase7_analyst_revision_backfill_specs();
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &specs,
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 analyst revision backfill profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    factor_rows = report.factor_rows,
                    combo_rows = report.combo_rows,
                    "Phase 7 analyst revision backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 analyst revision backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-alpha-blend-backfill/background
///
/// Combo-only backfill that blends existing multi-factor alpha scores by
/// explicit source weights.
pub async fn backfill_phase7_alpha_blend_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7AlphaBlendBackfillRequest>,
) -> impl IntoResponse {
    let plan = match req.into_plan() {
        Ok(plan) => plan,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', 0, 0, 0, 0, now(), $6, now())",
    )
    .bind(&task_id)
    .bind(plan.task_type)
    .bind(plan.source)
    .bind(plan.start_date)
    .bind(plan.end_date)
    .bind(plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 alpha blend backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plan = plan.clone();

    tokio::spawn(async move {
        let result = run_phase7_alpha_blend_backfill(&state.db, &tid, &task_plan).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                if let Err(error) = persist_factor_backfill_experiment_run(
                    &state.db,
                    &tid,
                    &task_plan,
                    &[],
                    &completion,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %tid,
                        error = %error,
                        "Failed to persist Phase 7 alpha blend profile"
                    );
                }
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    combo_rows = report.combo_rows,
                    "Phase 7 alpha blend backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 alpha blend backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": plan.task_type,
            "combo_name": plan.combo_name,
            "version": plan.version,
            "start_date": plan.start_date,
            "end_date": plan.end_date,
            "sources": plan.source_combos,
        }
    }))
}

/// POST /api/v1/quant/factors/phase7-alpha-blend-profiles-backfill/background
///
/// Batch backfill the canonical Phase 7 alpha blend weight profiles so the
/// optimizer can search across valuation/quality/growth/recovery/relative
/// strength weight mixes as normal factor combos.
pub async fn backfill_phase7_alpha_blend_profiles_background(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Phase7AlphaBlendProfilesBackfillRequest>,
) -> impl IntoResponse {
    let plans = match req.into_plans() {
        Ok(plans) => plans,
        Err(error) => {
            return Json(json!({"code": 1, "message": error}));
        }
    };
    let task_id = background_factor_task_id();
    let first_plan = plans.first().expect("plans checked non-empty");
    let last_plan = plans.last().expect("plans checked non-empty");
    let total_steps = alpha_blend_profile_backfill_total_steps(&plans);

    let insert_result = sqlx::query(
        "INSERT INTO data_sync_task
           (task_id, task_type, source, start_date, end_date, status, total_count,
            success_count, failed_count, progress, last_heartbeat_at,
            heartbeat_timeout_seconds, started_at)
         VALUES ($1, $2, $3, $4, $5, 'running', $6, 0, 0, 0, now(), $7, now())",
    )
    .bind(&task_id)
    .bind(first_plan.task_type)
    .bind(first_plan.source)
    .bind(first_plan.start_date)
    .bind(first_plan.end_date)
    .bind(usize_to_i32(total_steps))
    .bind(first_plan.heartbeat_timeout_seconds)
    .execute(&state.db)
    .await;

    if let Err(error) = insert_result {
        return Json(json!({
            "code": 1,
            "message": format!("Failed to create phase7 alpha blend profiles backfill task: {}", error)
        }));
    }

    let state = state.clone();
    let tid = task_id.clone();
    let task_plans = plans.clone();

    tokio::spawn(async move {
        let result = run_phase7_alpha_blend_profiles_backfill(&state.db, &tid, &task_plans).await;
        match result {
            Ok(completion) => {
                let report = completion.report();
                let total_rows = usize_to_i32(report.total_rows());
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status=$2,
                         total_count=$3,
                         success_count=$3,
                         failed_count=0,
                         progress=CASE WHEN $2 = 'completed' THEN 100 ELSE progress END,
                         error_message=CASE
                             WHEN $2 = 'cancelled' THEN COALESCE(error_message, 'cancelled by user request')
                             ELSE NULL
                         END,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(completion.task_status())
                .bind(total_rows)
                .execute(&state.db)
                .await;
                info!(
                    task_id = %tid,
                    status = completion.task_status(),
                    combo_rows = report.combo_rows,
                    "Phase 7 alpha blend profiles backfill completed"
                );
            }
            Err(error) => {
                tracing::error!(task_id = %tid, error = %error, "Phase 7 alpha blend profiles backfill failed");
                let _ = sqlx::query(
                    "UPDATE data_sync_task
                     SET status='failed',
                         failed_count=1,
                         error_message=$2,
                         last_heartbeat_at=now(),
                         completed_at=now()
                     WHERE task_id=$1",
                )
                .bind(&tid)
                .bind(&error)
                .execute(&state.db)
                .await;
            }
        }
    });

    Json(json!({
        "code": 0,
        "data": {
            "task_id": task_id,
            "status": "running",
            "task_type": first_plan.task_type,
            "version": first_plan.version,
            "start_date": first_plan.start_date,
            "end_date": last_plan.end_date,
            "profiles": plans.iter().map(|plan| json!({
                "combo_name": plan.combo_name,
                "version": plan.version,
                "sources": plan.source_combos,
            })).collect::<Vec<_>>(),
        }
    }))
}

// ─── 第九批测试（B 线）：P42b into_plan 全分支 + handler 拒绝分支参数形态 ───
//
// 安全边界（与第二批一致）：backfill_*_background handler 只直调**拒绝分支**——
// into_plan 校验失败在写 data_sync_task / spawn 重回填之前短路返回，不触库不启动后台任务。
// P42b 两个 into_plan 是本文件内的纯函数，成功分支直测构造 SetBasedFactorBackfillPlan，
// 不需要 AppState、不写任何表。33 个 handler 的日期倒置拒绝分支已由第二批覆盖
// （tests.rs::second_batch），本批补不同参数形态（blank version / 非法日期格式 / 超长 combo_name）。

#[cfg(test)]
mod ninth_batch {
    use super::*;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::Json;
    use std::sync::Arc;

    async fn test_db() -> sqlx::PgPool {
        let _ = dotenv::from_filename("../.env");
        let _ = dotenv::dotenv();
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 构造直调用 AppState（拒绝分支不触 Tushare/库，客户端仅初始化不发请求）。
    async fn test_state() -> Arc<crate::AppState> {
        let db = test_db().await;
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("Tushare client init (需 TUSHARE_TOKEN: source ../.env)");
        Arc::new(crate::AppState {
            start_time: chrono::Utc::now(),
            db,
            tushare,
            sync_tasks: crate::sync_task_registry::new_registry(),
        })
    }

    /// handler 返回的 Json 响应体解析为 serde_json::Value。
    async fn resp_json(resp: impl IntoResponse) -> serde_json::Value {
        let body = resp.into_response().into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("json response body")
    }

    fn assert_rejected(value: &serde_json::Value, expect_fragment: &str, handler: &str) {
        assert_eq!(
            value["code"], 1,
            "[{handler}] 拒绝分支应返回 code 1: {value}"
        );
        let message = value["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expect_fragment),
            "[{handler}] message 应含 {expect_fragment:?}: {message}"
        );
    }

    // ── P42b into_plan 成功分支（纯函数直测，不触库不 spawn）──

    #[test]
    fn p42b_momentum_reversal_into_plan_fills_defaults_and_overrides() {
        // 全默认：start 固定 2016-02-01，end 默认当天
        let plan = P42bLargeCapMomentumReversalBackfillRequest {
            start_date: None,
            end_date: None,
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("default plan");
        assert_eq!(
            plan.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).unwrap()
        );
        // end_date 默认取调用当天，容忍跨午夜竞态（±1 天内）
        let today = chrono::Utc::now().date_naive();
        assert!(
            plan.end_date == today || plan.end_date == today.pred_opt().unwrap(),
            "默认 end_date 应为当天: {} vs {today}",
            plan.end_date
        );
        assert_eq!(plan.version, "1.0.0");
        assert_eq!(plan.combo_name, "p42b_large_cap_mom_rev_daily_std");
        assert_eq!(plan.task_type, "p42b_large_cap_momentum_reversal_backfill");
        assert_eq!(plan.source, "factor");
        assert_eq!(plan.heartbeat_timeout_seconds, 3600);
        assert_eq!(plan.bundle_name, "p42b_large_cap_mom_rev_daily_std");
        assert_eq!(plan.category, "price_volume");
        assert_eq!(plan.phase, "P4.2b");
        assert_eq!(
            plan.dependencies,
            &["market_stock_daily_bar_adj", "market_stock_daily_basic"]
        );
        assert_eq!(plan.combo_method, "equal_weight");
        assert_eq!(plan.experiment_type, "phase7_factor_backfill_profile");
        assert_eq!(plan.statement_timeout_ms, 0);
        assert!(plan.source_combos.is_empty());

        // 显式覆盖：紧凑日期格式 + version/combo_name/超时
        let plan = P42bLargeCapMomentumReversalBackfillRequest {
            start_date: Some("20240506".into()),
            end_date: Some(" 2024-06-05 ".into()),
            version: Some(" 9.9.9 ".into()),
            combo_name: Some("zzz_test_api_bf_p42b".into()),
            statement_timeout_ms: Some(12345),
        }
        .into_plan()
        .expect("override plan");
        assert_eq!(
            plan.start_date,
            NaiveDate::from_ymd_opt(2024, 5, 6).unwrap()
        );
        assert_eq!(plan.end_date, NaiveDate::from_ymd_opt(2024, 6, 5).unwrap());
        assert_eq!(plan.version, "9.9.9");
        assert_eq!(plan.combo_name, "zzz_test_api_bf_p42b");
        assert_eq!(plan.statement_timeout_ms, 12345);
    }

    #[test]
    fn p42b_defensive_low_vol_quality_into_plan_fills_defaults_and_overrides() {
        let plan = P42bDefensiveLowVolQualityBackfillRequest {
            start_date: None,
            end_date: None,
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect("default plan");
        assert_eq!(
            plan.start_date,
            NaiveDate::from_ymd_opt(2016, 2, 1).unwrap()
        );
        assert_eq!(plan.combo_name, "defensive_lowvol_quality_daily_std");
        assert_eq!(plan.task_type, "p42b_defensive_low_vol_quality_backfill");
        assert_eq!(plan.bundle_name, "defensive_lowvol_quality_daily_std");
        assert_eq!(plan.category, "price_volume");
        assert_eq!(plan.phase, "P4.2b");
        assert_eq!(
            plan.dependencies,
            &["market_stock_daily_bar_adj", "market_stock", "factor_value"]
        );
        assert_eq!(plan.combo_method, "equal_weight");
        assert_eq!(plan.experiment_type, "phase7_factor_backfill_profile");

        let plan = P42bDefensiveLowVolQualityBackfillRequest {
            start_date: Some("2024-05-06".into()),
            end_date: Some("20240605".into()),
            version: Some("2.0.0".into()),
            combo_name: Some("zzz_test_api_bf_defensive".into()),
            statement_timeout_ms: Some(999),
        }
        .into_plan()
        .expect("override plan");
        assert_eq!(plan.version, "2.0.0");
        assert_eq!(plan.combo_name, "zzz_test_api_bf_defensive");
        assert_eq!(plan.statement_timeout_ms, 999);
    }

    #[test]
    fn p42b_into_plan_rejects_invalid_payload_shapes() {
        let base = || P42bLargeCapMomentumReversalBackfillRequest {
            start_date: Some("2024-05-06".into()),
            end_date: Some("2024-06-05".into()),
            version: None,
            combo_name: None,
            statement_timeout_ms: None,
        };

        // 日期倒置
        let err = P42bLargeCapMomentumReversalBackfillRequest {
            start_date: Some("2024-06-10".into()),
            end_date: Some("2024-06-01".into()),
            ..base()
        }
        .into_plan()
        .expect_err("inverted dates");
        assert_eq!(err, "start_date must be <= end_date");

        // 非法日期格式
        let err = P42bLargeCapMomentumReversalBackfillRequest {
            start_date: Some("2024/05/06".into()),
            ..base()
        }
        .into_plan()
        .expect_err("bad date format");
        assert_eq!(err, "start_date must use YYYY-MM-DD or YYYYMMDD");

        // 日期空白
        let err = P42bLargeCapMomentumReversalBackfillRequest {
            end_date: Some("   ".into()),
            ..base()
        }
        .into_plan()
        .expect_err("blank date");
        assert_eq!(err, "end_date must not be blank");

        // version 空白 / 超长
        let err = P42bLargeCapMomentumReversalBackfillRequest {
            version: Some("   ".into()),
            ..base()
        }
        .into_plan()
        .expect_err("blank version");
        assert_eq!(err, "version must not be blank");
        let err = P42bLargeCapMomentumReversalBackfillRequest {
            version: Some("v".repeat(33)),
            ..base()
        }
        .into_plan()
        .expect_err("long version");
        assert_eq!(err, "version must be <= 32 chars");

        // combo_name 空白 / 超长（defensive 家族各验一例超长）
        let err = P42bLargeCapMomentumReversalBackfillRequest {
            combo_name: Some("   ".into()),
            ..base()
        }
        .into_plan()
        .expect_err("blank combo");
        assert_eq!(err, "combo_name must not be blank");
        let err = P42bDefensiveLowVolQualityBackfillRequest {
            start_date: Some("2024-05-06".into()),
            end_date: Some("2024-06-05".into()),
            version: None,
            combo_name: Some("c".repeat(129)),
            statement_timeout_ms: None,
        }
        .into_plan()
        .expect_err("long combo");
        assert_eq!(err, "combo_name must be <= 128 chars");
    }

    // ── handler 拒绝分支：不同参数形态各测一例（直调，成功路径不触）──

    #[tokio::test]
    async fn handlers_reject_blank_version_payloads_without_scheduling() {
        let state = test_state().await;

        let v = resp_json(
            backfill_phase7_price_volume_background(
                State(state.clone()),
                Json(Phase7PriceVolumeBackfillRequest {
                    start_date: None,
                    end_date: None,
                    version: Some("   ".into()),
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "version must not be blank", "price_volume");

        let v = resp_json(
            backfill_phase7_dividend_quality_background(
                State(state),
                Json(Phase7DividendQualityBackfillRequest {
                    start_date: None,
                    end_date: None,
                    version: Some("".into()),
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "version must not be blank", "dividend_quality");
    }

    #[tokio::test]
    async fn handlers_reject_non_iso_date_payloads_without_scheduling() {
        let state = test_state().await;

        let v = resp_json(
            backfill_phase7_valuation_background(
                State(state.clone()),
                Json(Phase7ValuationBackfillRequest {
                    start_date: Some("2026/01/01".into()),
                    end_date: None,
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(
            &v,
            "start_date must use YYYY-MM-DD or YYYYMMDD",
            "valuation",
        );

        let v = resp_json(
            backfill_phase7_moneyflow_background(
                State(state),
                Json(Phase7MoneyflowBackfillRequest {
                    start_date: None,
                    end_date: Some("not-a-date".into()),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "end_date must use YYYY-MM-DD or YYYYMMDD", "moneyflow");
    }

    #[tokio::test]
    async fn handlers_reject_oversized_combo_name_payloads_without_scheduling() {
        let state = test_state().await;

        let v = resp_json(
            backfill_phase7_market_residual_risk_background(
                State(state.clone()),
                Json(Phase7MarketResidualRiskBackfillRequest {
                    start_date: None,
                    end_date: None,
                    version: None,
                    combo_name: Some("x".repeat(129)),
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(
            &v,
            "combo_name must be <= 128 chars",
            "market_residual_risk",
        );

        let v = resp_json(
            backfill_phase7_supply_float_shock_background(
                State(state),
                Json(Phase7SupplyFloatShockBackfillRequest {
                    start_date: None,
                    end_date: None,
                    version: None,
                    combo_name: Some("y".repeat(200)),
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "combo_name must be <= 128 chars", "supply_float_shock");
    }
}
