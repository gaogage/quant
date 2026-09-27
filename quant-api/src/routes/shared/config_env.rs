//! 任务80 C类可配项 helper：factor_value.factor_version 单源取值。
//!
//! 改造前全仓散布 10+ 处 `"1.0.0"` 字面量（SQL 查询口径/物化参数/请求负载），
//! 版本升级需逐处翻找。统一收敛到本函数：env `FACTOR_VERSION` 可配，
//! 未配置回退 "1.0.0"（原写死值，行为零变化）。
//!
//! 注意：quant-factor 写入口的因子元数据版本不在本单源范围内（见任务80报告）。

/// factor_value.factor_version 统一取值口径。
pub(crate) fn factor_version() -> String {
    // 任务80: C类特许 → env 化（默认=原写死值）
    std::env::var("FACTOR_VERSION").unwrap_or_else(|_| "1.0.0".to_string())
}

// ── 第二轮审计 C5/C6/C7 窗口/运维类 env 化（2026-09-27 用户全批，默认=原写死值） ──

/// C5: WFA 参数读取回看窗天数（原 signal_export.rs 写死 180）。
pub(crate) fn wfa_params_lookback_days() -> i64 {
    std::env::var("WFA_PARAMS_LOOKBACK_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(180)
}

/// C6: EOD 曲线同步起点回看天数（原 scheduler.rs 写死 190）。
pub(crate) fn curve_sync_start_lookback_days() -> i64 {
    std::env::var("CURVE_SYNC_START_LOOKBACK_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(190)
}

/// C6: 夜链物化增量窗口天数（原 scheduler.rs 写死 120，两处）。
pub(crate) fn pit_materialize_window_days() -> i64 {
    std::env::var("PIT_MATERIALIZE_WINDOW_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120)
}

/// C6: v24 曲线增量同步窗口天数（原 scheduler.rs 写死 10）。
pub(crate) fn v24_curve_sync_window_days() -> i64 {
    std::env::var("V24_CURVE_SYNC_WINDOW_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

/// C6: 早间曲线重建回看天数（原 scheduler.rs 写死 370）。
pub(crate) fn morning_curve_rebuild_lookback_days() -> i64 {
    std::env::var("MORNING_CURVE_REBUILD_LOOKBACK_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(370)
}

/// C7: 回填对账窗（原 scheduler.rs const BACKFILL_WINDOW_DAYS=14 天）。
pub(crate) fn backfill_window_days() -> chrono::Duration {
    chrono::Duration::days(
        std::env::var("BACKFILL_WINDOW_DAYS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(14),
    )
}
