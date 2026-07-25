//! Blueprint progress API.
//!
//! This endpoint is intentionally read-only. It summarizes current v19 progress
//! toward the professional/elite gates, plus storage pressure from regenerable
//! research/cache tables.

use axum::{extract::State, response::IntoResponse, Json};
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_rfc3339_local;

use crate::auth::middleware::UserContext;
use crate::AppState;

const PROFESSIONAL_ANNUAL_RETURN: f64 = 15.0;
const PROFESSIONAL_SHARPE: f64 = 1.0;
const PROFESSIONAL_SORTINO: f64 = 1.5;
const PROFESSIONAL_MAX_DRAWDOWN: f64 = 35.0;

const ELITE_ANNUAL_RETURN: f64 = 20.0;
const ELITE_SHARPE: f64 = 1.5;
const ELITE_SORTINO: f64 = 1.8;
const ELITE_CALMAR: f64 = 2.0;
const ELITE_PROFIT_FACTOR: f64 = 1.5;
const ELITE_TRADES: f64 = 200.0;

/// GET /api/v1/quant/blueprint/progress
pub async fn blueprint_progress(
    State(state): State<Arc<AppState>>,
    _user: UserContext,
) -> impl IntoResponse {
    match build_blueprint_progress(&state.db).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(error) => Json(json!({"code": 1, "message": error})),
    }
}

async fn build_blueprint_progress(db: &sqlx::PgPool) -> Result<Value, String> {
    let strategies = load_active_strategies(db).await?;
    let accounts = load_active_simulated_accounts(db).await?;
    let selected = accounts
        .iter()
        .filter(|account| account["annual_return_pct"].is_number())
        .max_by(|left, right| {
            value_f64(left, "annual_return_pct")
                .partial_cmp(&value_f64(right, "annual_return_pct"))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();

    let professional = build_professional_gate(selected.as_ref());
    let elite = build_elite_gate(selected.as_ref());
    let storage = load_storage_summary(db).await?;
    let task_state = load_task_state_summary(db).await?;

    let phase_progress = roadmap_phase_progress();
    let system_build_progress_pct = weighted_phase_progress(&phase_progress);
    let professional_progress_pct = professional["available_metric_progress_pct"]
        .as_f64()
        .unwrap_or(0.0);
    let elite_progress_pct = elite["available_metric_progress_pct"]
        .as_f64()
        .unwrap_or(0.0);

    let overall_progress_pct = round2(
        0.35 * system_build_progress_pct
            + 0.45 * professional_progress_pct
            + 0.20 * elite_progress_pct,
    );

    let mut blockers = Vec::new();
    push_blockers(&mut blockers, &professional, "professional");
    push_blockers(&mut blockers, &elite, "elite");
    if storage["pressure_level"] == "red" || storage["pressure_level"] == "yellow" {
        blockers.push(json!({
            "scope": "storage",
            "reason": "regenerable_cache_or_research_tables_are_large",
            "detail": storage["cleanup_posture"],
        }));
    }

    Ok(json!({
        "as_of": fmt_rfc3339_local(Some(chrono::Utc::now())).unwrap_or_default(),
        "canonical": {
            "strategies": strategies,
            "scope": "active simulated accounts and full PIT canonical lineage",
            "promotion_status": "defensive_candidate"
        },
        "selected_account": selected,
        "accounts": accounts,
        "targets": {
            "professional_observation": {
                "annual_return_pct": PROFESSIONAL_ANNUAL_RETURN,
                "excess_return_pct": "> 0; current paper_replay does not persist this metric",
                "sharpe_ratio": PROFESSIONAL_SHARPE,
                "sortino_ratio": PROFESSIONAL_SORTINO,
                "max_drawdown_pct": PROFESSIONAL_MAX_DRAWDOWN
            },
            "professional_elite": {
                "annual_return_pct": ELITE_ANNUAL_RETURN,
                "sharpe_ratio": ELITE_SHARPE,
                "sortino_ratio": ELITE_SORTINO,
                "calmar_ratio": ELITE_CALMAR,
                "profit_factor": ELITE_PROFIT_FACTOR,
                "independent_trades": ELITE_TRADES
            }
        },
        "progress": {
            "overall_progress_pct": overall_progress_pct,
            "system_build_progress_pct": system_build_progress_pct,
            "professional_metric_progress_pct": professional_progress_pct,
            "elite_metric_progress_pct": elite_progress_pct,
            "professional_hard_gate_passed": professional["hard_gate_passed"],
            "elite_hard_gate_passed": elite["hard_gate_passed"],
            "current_phase": "P3.19 new broad-base PIT alpha source discovery",
            "interpretation": "progress is distance visualization only; promotion still requires every hard gate and robustness gate to pass"
        },
        "professional": professional,
        "elite": elite,
        "roadmap_phases": phase_progress,
        "storage": storage,
        "task_state": task_state,
        "blockers": blockers,
        "next_actions": [
            "pre-register P3.19 low-correlation broad-base PIT sources before building factors",
            "run permission/schema/available_at audit before any full backfill",
            "run P3.10A-D diagnostics before bounded WFA",
            "use cleanup preview for cache and stopped combo cleanup; do not delete protected objective history or active canonical combos",
            "add excess_return/profit_factor persistence to paper_replay before formal promotion review"
        ]
    }))
}

async fn load_active_strategies(db: &sqlx::PgPool) -> Result<Vec<Value>, String> {
    let rows = sqlx::query(
        r#"
        SELECT strategy_id, combo_name, prediction_set_id, equity_curve_task_id,
               signal_source, score_direction, candidate_tier, status
        FROM strategy_config
        WHERE status = 'active' AND strategy_type = 'composite'
        ORDER BY strategy_id
        "#,
    )
    .fetch_all(db)
    .await
    .map_err(|error| format!("load active strategies failed: {error}"))?;

    Ok(rows
        .into_iter()
        .map(|row| {
            json!({
                "strategy_id": row.get::<String, _>("strategy_id"),
                "combo_name": row.try_get::<String, _>("combo_name").unwrap_or_default(),
                "prediction_set_id": row.try_get::<Option<String>, _>("prediction_set_id").ok().flatten(),
                "equity_curve_task_id": row.try_get::<Option<String>, _>("equity_curve_task_id").ok().flatten(),
                "signal_source": row.try_get::<String, _>("signal_source").unwrap_or_default(),
                "score_direction": row.try_get::<String, _>("score_direction").unwrap_or_default(),
                "candidate_tier": row.try_get::<String, _>("candidate_tier").unwrap_or_else(|_| "research_baseline".into()),
                "status": row.get::<String, _>("status"),
            })
        })
        .collect())
}

async fn load_active_simulated_accounts(db: &sqlx::PgPool) -> Result<Vec<Value>, String> {
    let rows = sqlx::query(
        r#"
        SELECT pa.paper_account_id, pa.name, pa.status, pa.account_type, pa.strategy_version_id,
               pa.leverage_enabled, pa.leverage_multiplier,
               pa.current_nav::double precision AS current_nav,
               pa.cash::double precision AS cash,
               pa.total_trades,
               pr.start_date, pr.end_date, pr.annual_return_pct, pr.cumulative_return_pct,
               pr.sharpe_ratio, pr.sortino_ratio, pr.calmar_ratio, pr.max_drawdown_pct,
               pr.volatility_pct, pr.win_rate_pct, pr.trading_days
        FROM paper_account pa
        LEFT JOIN LATERAL (
            SELECT *
            FROM paper_replay
            WHERE paper_account_id = pa.paper_account_id
            ORDER BY created_at DESC
            LIMIT 1
        ) pr ON TRUE
        WHERE pa.status = 'active'
          AND pa.account_type = 'simulated'
          AND pa.strategy_version_id IS NOT NULL
        ORDER BY pa.paper_account_id
        "#,
    )
    .fetch_all(db)
    .await
    .map_err(|error| format!("load active simulated accounts failed: {error}"))?;

    Ok(rows
        .into_iter()
        .map(|row| {
            json!({
                "paper_account_id": row.get::<String, _>("paper_account_id"),
                "name": row.get::<String, _>("name"),
                "status": row.get::<String, _>("status"),
                "account_type": row.get::<String, _>("account_type"),
                "strategy_version_id": row.get::<String, _>("strategy_version_id"),
                "leverage_enabled": row.get::<bool, _>("leverage_enabled"),
                "leverage_multiplier": row.get::<f64, _>("leverage_multiplier"),
                "current_nav": row.try_get::<Option<f64>, _>("current_nav").ok().flatten(),
                "cash": row.try_get::<Option<f64>, _>("cash").ok().flatten(),
                "total_trades": row.try_get::<Option<i32>, _>("total_trades").ok().flatten(),
                "replay_start_date": row.try_get::<Option<chrono::NaiveDate>, _>("start_date").ok().flatten().map(|d| d.to_string()),
                "replay_end_date": row.try_get::<Option<chrono::NaiveDate>, _>("end_date").ok().flatten().map(|d| d.to_string()),
                "annual_return_pct": row.try_get::<Option<f64>, _>("annual_return_pct").ok().flatten(),
                "cumulative_return_pct": row.try_get::<Option<f64>, _>("cumulative_return_pct").ok().flatten(),
                "sharpe_ratio": row.try_get::<Option<f64>, _>("sharpe_ratio").ok().flatten(),
                "sortino_ratio": row.try_get::<Option<f64>, _>("sortino_ratio").ok().flatten(),
                "calmar_ratio": row.try_get::<Option<f64>, _>("calmar_ratio").ok().flatten(),
                "max_drawdown_pct": row.try_get::<Option<f64>, _>("max_drawdown_pct").ok().flatten(),
                "volatility_pct": row.try_get::<Option<f64>, _>("volatility_pct").ok().flatten(),
                "win_rate_pct": row.try_get::<Option<f64>, _>("win_rate_pct").ok().flatten(),
                "trading_days": row.try_get::<Option<i32>, _>("trading_days").ok().flatten(),
            })
        })
        .collect())
}

async fn load_storage_summary(db: &sqlx::PgPool) -> Result<Value, String> {
    let rows = sqlx::query(
        r#"
        SELECT c.relname AS table_name,
               pg_total_relation_size(c.oid) AS size_bytes,
               pg_size_pretty(pg_total_relation_size(c.oid)) AS size_pretty,
               COALESCE(s.n_live_tup, 0) AS row_count
        FROM pg_class c
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE c.relnamespace = 'public'::regnamespace
          AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 40
        "#,
    )
    .fetch_all(db)
    .await
    .map_err(|error| format!("load storage summary failed: {error}"))?;

    let mut total_top_bytes = 0_i64;
    let mut cleanable_top_bytes = 0_i64;
    let mut protected_top_bytes = 0_i64;
    let mut top_tables = Vec::new();

    for row in rows {
        let table_name = row.get::<String, _>("table_name");
        let size_bytes = row.get::<i64, _>("size_bytes");
        let row_count = row.get::<i64, _>("row_count");
        let (category, cleanable, posture) = storage_table_policy(&table_name);
        total_top_bytes += size_bytes;
        if cleanable {
            cleanable_top_bytes += size_bytes;
        } else {
            protected_top_bytes += size_bytes;
        }
        top_tables.push(json!({
            "table": table_name,
            "category": category,
            "cleanup_posture": posture,
            "cleanable_by_policy": cleanable,
            "size_bytes": size_bytes,
            "size_pretty": row.get::<String, _>("size_pretty"),
            "row_count_estimate": row_count,
        }));
    }

    let pressure_level = if cleanable_top_bytes >= 500_i64 * 1024 * 1024 * 1024 {
        "red"
    } else if cleanable_top_bytes >= 200_i64 * 1024 * 1024 * 1024 {
        "yellow"
    } else {
        "green"
    };

    Ok(json!({
        "pressure_level": pressure_level,
        "top_tables_total_size_bytes": total_top_bytes,
        "top_tables_total_size_pretty": format_bytes(total_top_bytes),
        "top_cleanable_size_bytes": cleanable_top_bytes,
        "top_cleanable_size_pretty": format_bytes(cleanable_top_bytes),
        "top_protected_size_bytes": protected_top_bytes,
        "top_protected_size_pretty": format_bytes(protected_top_bytes),
        "cleanup_posture": "preview first; keep immutable objective history; clean regenerable caches and stopped/inactive research combos only after dependency check",
        "protected_examples": [
            "market_* objective history",
            "active strategy_config combo full_pit_icir_37f",
            "active prediction set pred-fullperiod-nlqr-20140101-20260630",
            "paper_* live/replay account state",
            "data_sync_attempt audit ledger"
        ],
        "top_tables": top_tables,
    }))
}

async fn load_task_state_summary(db: &sqlx::PgPool) -> Result<Value, String> {
    let sync = status_counts(db, "data_sync_task").await?;
    let optimization = status_counts(db, "optimization_task").await?;
    let experiment = status_counts(db, "experiment_run").await?;

    Ok(json!({
        "data_sync_task": sync,
        "optimization_task": optimization,
        "experiment_run": experiment,
        "stale_cleanup_endpoints": [
            "/api/v1/quant/data/sync-tasks/cleanup-stale",
            "/api/v1/quant/optimizations/cleanup-stale",
            "/api/v1/quant/experiments/cleanup-stale"
        ],
        "cleanup_preview_endpoint": "/api/v1/quant/data/cleanup/preview"
    }))
}

async fn status_counts(db: &sqlx::PgPool, table: &str) -> Result<Value, String> {
    let sql = format!(
        "SELECT status, COUNT(*)::bigint AS cnt FROM {table} GROUP BY status ORDER BY status"
    );
    let rows = sqlx::query(&sql)
        .fetch_all(db)
        .await
        .map_err(|error| format!("load status counts for {table} failed: {error}"))?;

    let mut counts = serde_json::Map::new();
    for row in rows {
        counts.insert(
            row.get::<String, _>("status"),
            json!(row.get::<i64, _>("cnt")),
        );
    }
    Ok(Value::Object(counts))
}

fn build_professional_gate(account: Option<&Value>) -> Value {
    let metrics = vec![
        metric_min(
            "annual_return_pct",
            "年化收益",
            account.and_then(|a| a["annual_return_pct"].as_f64()),
            PROFESSIONAL_ANNUAL_RETURN,
            "%",
            true,
        ),
        metric_min("excess_return_pct", "超额收益", None, 0.0, "%", true),
        metric_min(
            "sharpe_ratio",
            "Sharpe",
            account.and_then(|a| a["sharpe_ratio"].as_f64()),
            PROFESSIONAL_SHARPE,
            "",
            false,
        ),
        metric_min(
            "sortino_ratio",
            "Sortino",
            account.and_then(|a| a["sortino_ratio"].as_f64()),
            PROFESSIONAL_SORTINO,
            "",
            true,
        ),
        metric_max(
            "max_drawdown_pct",
            "最大回撤",
            account.and_then(|a| a["max_drawdown_pct"].as_f64()),
            PROFESSIONAL_MAX_DRAWDOWN,
            "%",
            false,
        ),
    ];
    gate_report("professional_observation", metrics)
}

fn build_elite_gate(account: Option<&Value>) -> Value {
    let metrics = vec![
        metric_min(
            "annual_return_pct",
            "年化收益",
            account.and_then(|a| a["annual_return_pct"].as_f64()),
            ELITE_ANNUAL_RETURN,
            "%",
            true,
        ),
        metric_min(
            "sharpe_ratio",
            "Sharpe",
            account.and_then(|a| a["sharpe_ratio"].as_f64()),
            ELITE_SHARPE,
            "",
            false,
        ),
        metric_min(
            "sortino_ratio",
            "Sortino",
            account.and_then(|a| a["sortino_ratio"].as_f64()),
            ELITE_SORTINO,
            "",
            false,
        ),
        metric_min(
            "calmar_ratio",
            "Calmar",
            account.and_then(|a| a["calmar_ratio"].as_f64()),
            ELITE_CALMAR,
            "",
            false,
        ),
        metric_min(
            "profit_factor",
            "Profit Factor",
            None,
            ELITE_PROFIT_FACTOR,
            "",
            false,
        ),
        metric_min(
            "independent_trades",
            "独立交易数",
            account
                .and_then(|a| a["total_trades"].as_i64())
                .map(|v| v as f64),
            ELITE_TRADES,
            "笔",
            true,
        ),
    ];
    gate_report("professional_elite", metrics)
}

fn gate_report(gate: &str, metrics: Vec<Value>) -> Value {
    let available: Vec<&Value> = metrics
        .iter()
        .filter(|metric| !metric["missing"].as_bool().unwrap_or(true))
        .collect();
    let passed = metrics
        .iter()
        .all(|metric| metric["passed"].as_bool().unwrap_or(false));
    let evidence_completeness_pct = if metrics.is_empty() {
        0.0
    } else {
        round2(100.0 * available.len() as f64 / metrics.len() as f64)
    };
    let available_metric_progress_pct = if available.is_empty() {
        0.0
    } else {
        round2(
            available
                .iter()
                .map(|metric| metric["progress_pct"].as_f64().unwrap_or(0.0))
                .sum::<f64>()
                / available.len() as f64,
        )
    };
    let blocking_metrics: Vec<Value> = metrics
        .iter()
        .filter(|metric| !metric["passed"].as_bool().unwrap_or(false))
        .map(|metric| {
            json!({
                "key": metric["key"],
                "name": metric["name"],
                "reason": if metric["missing"].as_bool().unwrap_or(false) { "missing_metric" } else { "below_gate" },
                "current": metric["current"],
                "target": metric["target"]
            })
        })
        .collect();

    json!({
        "gate": gate,
        "hard_gate_passed": passed,
        "available_metric_progress_pct": available_metric_progress_pct,
        "evidence_completeness_pct": evidence_completeness_pct,
        "metrics": metrics,
        "blocking_metrics": blocking_metrics,
    })
}

fn metric_min(
    key: &str,
    name: &str,
    current: Option<f64>,
    target: f64,
    unit: &str,
    inclusive: bool,
) -> Value {
    let passed = current
        .map(|value| {
            if inclusive {
                value >= target
            } else {
                value > target
            }
        })
        .unwrap_or(false);
    let progress = current
        .map(|value| {
            if target.abs() < f64::EPSILON {
                if value > 0.0 {
                    100.0
                } else {
                    0.0
                }
            } else {
                clamp_progress(value / target * 100.0)
            }
        })
        .unwrap_or(0.0);

    metric_json(key, name, current, target, unit, "min", passed, progress)
}

fn metric_max(
    key: &str,
    name: &str,
    current: Option<f64>,
    target: f64,
    unit: &str,
    inclusive: bool,
) -> Value {
    let passed = current
        .map(|value| {
            if inclusive {
                value <= target
            } else {
                value < target
            }
        })
        .unwrap_or(false);
    let progress = current
        .map(|value| {
            if value <= target {
                100.0
            } else if value.abs() < f64::EPSILON {
                0.0
            } else {
                clamp_progress(target / value * 100.0)
            }
        })
        .unwrap_or(0.0);

    metric_json(key, name, current, target, unit, "max", passed, progress)
}

fn metric_json(
    key: &str,
    name: &str,
    current: Option<f64>,
    target: f64,
    unit: &str,
    direction: &str,
    passed: bool,
    progress_pct: f64,
) -> Value {
    json!({
        "key": key,
        "name": name,
        "current": current.map(round4),
        "target": target,
        "unit": unit,
        "direction": direction,
        "passed": passed,
        "missing": current.is_none(),
        "progress_pct": round2(progress_pct),
    })
}

fn roadmap_phase_progress() -> Vec<Value> {
    vec![
        phase(
            "P0",
            "可复现与数据门禁",
            100.0,
            "done",
            "账号回放、盘中模拟交易、数据健康检查已统一门禁",
        ),
        phase(
            "P1",
            "canonical v19 基线",
            100.0,
            "done",
            "full_pit_icir_37f + prediction_blend 已成为 active canonical",
        ),
        phase(
            "P2",
            "strict OOS/WFA 基线",
            85.0,
            "done_with_gap",
            "pipeline 可用，但当前 stitched OOS 未达专业门禁",
        ),
        phase(
            "P3",
            "低相关 alpha source 建设",
            45.0,
            "in_progress",
            "多源已证伪，P3.19 转向真实经营/产业链/订单价格链等 PIT 源",
        ),
        phase(
            "P4",
            "bounded WFA 与鲁棒性准入",
            35.0,
            "blocked_waiting_alpha",
            "门禁体系存在，但暂无训练窗成本容量扰动后合格的新源",
        ),
        phase(
            "P5",
            "专业/精英晋级与生产观察",
            10.0,
            "not_ready",
            "active v19 仍为 defensive_candidate",
        ),
        phase(
            "P6",
            "存储治理与可持续运营",
            70.0,
            "in_progress",
            "cleanup/stale-cleanup API 已有，仍需正式 retention 白名单和例行 dry-run",
        ),
    ]
}

fn phase(id: &str, name: &str, progress_pct: f64, status: &str, evidence: &str) -> Value {
    json!({
        "id": id,
        "name": name,
        "progress_pct": progress_pct,
        "status": status,
        "evidence": evidence,
    })
}

fn weighted_phase_progress(phases: &[Value]) -> f64 {
    if phases.is_empty() {
        return 0.0;
    }
    round2(
        phases
            .iter()
            .map(|phase| phase["progress_pct"].as_f64().unwrap_or(0.0))
            .sum::<f64>()
            / phases.len() as f64,
    )
}

fn push_blockers(blockers: &mut Vec<Value>, gate: &Value, scope: &str) {
    if gate["hard_gate_passed"].as_bool().unwrap_or(false) {
        return;
    }
    for metric in gate["blocking_metrics"].as_array().into_iter().flatten() {
        blockers.push(json!({
            "scope": scope,
            "metric": metric["key"],
            "name": metric["name"],
            "reason": metric["reason"],
            "current": metric["current"],
            "target": metric["target"]
        }));
    }
}

fn value_f64(value: &Value, key: &str) -> f64 {
    value[key].as_f64().unwrap_or(f64::NEG_INFINITY)
}

fn storage_table_policy(name: &str) -> (&'static str, bool, &'static str) {
    if name.starts_with("market_feature_cache_") {
        ("cache", true, "regenerable_cache_preview_then_truncate")
    } else if name == "multi_factor_value" || name == "multi_factor_weight" {
        (
            "factor_combo",
            true,
            "clean_stopped_or_inactive_combos_after_dependency_check",
        )
    } else if name.starts_with("backtest_") || name.starts_with("portfolio_") {
        ("backtest", true, "clean_unkept_expired_research_runs")
    } else if name == "model_prediction" {
        ("prediction", true, "clean_superseded_prediction_sets_only")
    } else if name.starts_with("market_") {
        (
            "objective_history",
            false,
            "protected_immutable_market_or_pit_history",
        )
    } else if name.starts_with("paper_") {
        (
            "paper_accounting",
            false,
            "protected_live_and_replay_account_state",
        )
    } else if name == "data_sync_attempt" || name == "data_sync_task" {
        ("audit_ledger", false, "protected_or_stale_cleanup_only")
    } else if name == "experiment_run"
        || name == "optimization_task"
        || name == "optimization_trial"
    {
        (
            "experiment_metadata",
            false,
            "retain_metadata_clean_stale_status_only",
        )
    } else {
        ("other", false, "manual_review")
    }
}

fn clamp_progress(value: f64) -> f64 {
    if value.is_nan() || value.is_sign_negative() {
        0.0
    } else if value > 100.0 {
        100.0
    } else {
        value
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

fn format_bytes(bytes: i64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn professional_gate_marks_current_v19_blockers() {
        let account = json!({
            "annual_return_pct": 14.68046266840608,
            "sharpe_ratio": 0.9244780248619116,
            "sortino_ratio": 1.0755189570240777,
            "max_drawdown_pct": 17.92069406054321,
            "total_trades": 44
        });

        let report = build_professional_gate(Some(&account));

        assert_eq!(report["hard_gate_passed"], json!(false));
        assert_eq!(report["evidence_completeness_pct"], json!(80.0));
        assert_eq!(report["blocking_metrics"].as_array().unwrap().len(), 4);
        assert!(report["available_metric_progress_pct"].as_f64().unwrap() > 88.0);
    }

    #[test]
    fn elite_gate_requires_profit_factor_and_trade_count() {
        let account = json!({
            "annual_return_pct": 14.68,
            "sharpe_ratio": 0.92,
            "sortino_ratio": 1.08,
            "calmar_ratio": 0.82,
            "total_trades": 44
        });

        let report = build_elite_gate(Some(&account));
        let blockers = report["blocking_metrics"].as_array().unwrap();

        assert_eq!(report["hard_gate_passed"], json!(false));
        assert!(blockers.iter().any(|b| b["key"] == "profit_factor"));
        assert!(blockers.iter().any(|b| b["key"] == "independent_trades"));
    }

    #[test]
    fn storage_policy_protects_objective_history_and_flags_cache() {
        assert_eq!(
            storage_table_policy("market_stock_daily_bar"),
            (
                "objective_history",
                false,
                "protected_immutable_market_or_pit_history"
            )
        );
        assert_eq!(
            storage_table_policy("market_feature_cache_value"),
            ("cache", true, "regenerable_cache_preview_then_truncate")
        );
        assert_eq!(
            storage_table_policy("multi_factor_value"),
            (
                "factor_combo",
                true,
                "clean_stopped_or_inactive_combos_after_dependency_check"
            )
        );
    }

    #[test]
    fn max_metric_progress_is_inverted() {
        let good = metric_max("max_drawdown_pct", "最大回撤", Some(17.9), 35.0, "%", false);
        let bad = metric_max("max_drawdown_pct", "最大回撤", Some(70.0), 35.0, "%", false);

        assert_eq!(good["passed"], json!(true));
        assert_eq!(good["progress_pct"], json!(100.0));
        assert_eq!(bad["passed"], json!(false));
        assert_eq!(bad["progress_pct"], json!(50.0));
    }
}
