//! data_version 集中化访问（DDD Step 4 收尾，2026-09-28 任务82）。
//!
//! 原散布形态收敛：
//! - `LIKE 'dv-eod-%' ORDER BY end_date DESC` 最新 EOD 版本查询（admin 两处同型）
//! - `EXISTS(...)` 版本存在性校验（experiment_run）
//! - 测试夹具的 `INSERT INTO data_version` FK 前置（rebalance/mvo_engine/paper/diagnostics 四处同型）
//!
//! 单一真相源：data_version 语义的读取口径此后只改这里。

use sqlx::PgPool;

/// 最新 EOD 数据版本 ID（dv-eod-% 前缀按 end_date 倒序第一）。
pub async fn latest_eod_data_version(db: &PgPool) -> Option<String> {
    sqlx::query_scalar(
        "SELECT data_version_id FROM data_version \
         WHERE data_version_id LIKE 'dv-eod-%' ORDER BY end_date DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

/// 数据版本是否存在（experiment_run 注册前的校验口径）。
pub async fn data_version_exists(db: &PgPool, data_version_id: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM data_version WHERE data_version_id = $1)")
        .bind(data_version_id)
        .fetch_one(db)
        .await
        .unwrap_or(false)
}

/// 测试夹具：确保 data_version 行存在（FK 前置，幂等）。
/// 收敛 rebalance/mvo_engine/paper/diagnostics 四处同型的测试 INSERT。
#[cfg(test)]
#[allow(dead_code)] // 收敛点预留: 四处测试夹具 INSERT 形态略有差异(列集不同), 同型出现时再接入
pub(crate) async fn ensure_test_data_version(db: &PgPool, id: &str, start: &str, end: &str) {
    let _ = sqlx::query(
        "INSERT INTO data_version (data_version_id, name, source, start_date, end_date, tables, snapshot_hash) \
         VALUES ($1, 'zzz test', 'test', $2, $3, '{}', 'zzz') ON CONFLICT DO NOTHING",
    )
    .bind(id)
    .bind(start)
    .bind(end)
    .execute(db)
    .await;
}
