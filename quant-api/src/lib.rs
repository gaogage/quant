//! Quant API library crate - 暴露给 examples / tests / 集成测试复用的模块。
//!
//! binary（`main.rs`）内的 `routes` / `auth` / `sync_task_registry` 等模块不在此暴露，
//! 它们属于 binary crate 内部。此处只放需要跨 crate / 跨 example 复用的领域模块。

pub mod discovery;
pub mod query_perf_baseline;
