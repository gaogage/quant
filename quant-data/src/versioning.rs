//! 数据版本注册中心（DDD 重构 Step 2 引入）。
//!
//! 背景：`data_version_id` 散落全项目 18+ 文件，7 种 ID 格式，PIT（point-in-time）
//! 语义靠口头约定。本模块定义领域 trait，为 Step 4 集中化 data_version 管理铺轨道。
//!
//! 设计原则（本步只引入 trait 骨架，不改现有实现）：
//! - trait 定义"注册/查询/解析状态"的统一契约，现有 repository 代码后续 Step 4 实现。
//! - `DataVersionState` 用类型状态区分"已注册/未注册/已废弃"，Step 5 升级为编译期门禁。

use chrono::NaiveDate;
use quant_common::identifiers::DataVersionId;
use serde::{Deserialize, Serialize};

/// 数据版本生命周期状态。
///
/// PIT 语义：只有 `Active` 状态的版本可被回测/调仓引用；
/// `Deprecated` 版本保留可追溯但不可新引用；`Draft` 尚未落库。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataVersionState {
    /// 草稿：尚未落库，仅存在于内存构建过程。
    Draft,
    /// 活跃：已注册落库，可被回测/调仓安全引用。
    Active,
    /// 废弃：保留可追溯，不可被新任务引用。
    Deprecated,
}

/// 数据版本注册中心契约。
///
/// 集中 data_version 的注册/查询/状态解析，消除散落的 INSERT/SELECT 样板
/// （当前散在 quant-data/repository.rs、quant-api/routes/sync.rs 等 8+ 处）。
///
/// 本 trait 为 Step 2 纯新增，现有代码尚未实现它；Step 4 集中化时由
/// `QuantDataRepository` 实现并替换散落样板。
///
/// 注：Rust 1.75+ 原生支持 trait 内 `async fn`，无需 async_trait 宏。
/// 当前未标 `#[async_trait]`，故本 trait 暂非 dyn-compatible；Step 4 若需 trait object
/// 再按需加 `#[async_trait]` 或显式返回 `Pin<Box<dyn Future>>`。
pub trait DataVersionRegistry {
    /// 注册一个新数据版本，返回其 ID 与状态。
    ///
    /// 幂等：若同 (data_date, source) 已存在 Active 版本，返回既有 ID 而非新建。
    fn register_version(
        &self,
        data_date: NaiveDate,
        source: &str,
        description: Option<&str>,
    ) -> impl std::future::Future<Output = Result<DataVersionId, DataVersionRegistryError>> + Send;

    /// 查询某版本当前状态（用于调仓前 PIT 校验）。
    fn resolve_state(
        &self,
        version_id: &DataVersionId,
    ) -> impl std::future::Future<Output = Result<DataVersionState, DataVersionRegistryError>> + Send;

    /// 将版本标记为 Deprecated（数据发现问题后阻断新引用）。
    fn deprecate(
        &self,
        version_id: &DataVersionId,
    ) -> impl std::future::Future<Output = Result<(), DataVersionRegistryError>> + Send;
}

impl DataVersionState {
    /// 是否可被回测/调仓安全引用。
    pub fn is_referenceable(&self) -> bool {
        matches!(self, Self::Active)
    }
}

impl std::fmt::Display for DataVersionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_string(self).unwrap_or_default();
        f.write_str(s.trim_matches('"'))
    }
}
#[derive(Debug, thiserror::Error)]
pub enum DataVersionRegistryError {
    #[error("data version not found: {0}")]
    NotFound(String),

    #[error("version is {state}, cannot be referenced")]
    InvalidState { state: DataVersionState },

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}
