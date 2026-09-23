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
