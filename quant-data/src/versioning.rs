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

// ============================================================
// PG 实现（R11：激活 Step 2 零 impl 的 trait 骨架）
// ============================================================

use sqlx::PgPool;

/// PostgreSQL 实现的数据版本注册中心。
///
/// 集中 data_version 的注册/查询/状态解析，替代散落在 repository.rs /
/// sync.rs / scheduler.rs 的 INSERT/SELECT 样板。
pub struct PgDataVersionRegistry<'a> {
    pool: &'a PgPool,
}

impl<'a> PgDataVersionRegistry<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// 查询最新的 EOD 数据版本 ID（原 scheduler::get_latest_data_version 集中化）。
    ///
    /// `dv-eod-%` 前缀的版本按 end_date 降序取首条；无则回退默认基线版本。
    /// 返回裸 String 以兼容现有调用方（批次 2 可升级为 DataVersionId）。
    pub async fn latest_eod_version_id(&self) -> String {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT data_version_id FROM data_version
             WHERE data_version_id LIKE 'dv-eod-%'
             ORDER BY end_date DESC LIMIT 1",
        )
        .fetch_optional(self.pool)
        .await
        .ok()
        .flatten();
        row.map(|(d,)| d)
            .unwrap_or_else(|| "research-full-2016-2026-20260515".to_string())
    }

    /// 注册完整数据版本（原 repository::create_data_version 集中化）。
    ///
    /// 幂等：同 data_version_id 已存在则 ON CONFLICT DO NOTHING。
    /// 供 sync.rs 的 19 个 sync_* 函数与路由层调用方收敛为单一出口。
    pub async fn create_version(
        &self,
        dv_id: &str,
        name: &str,
        source: &str,
        tables: &[&str],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<(), DataVersionRegistryError> {
        sqlx::query(
            r#"INSERT INTO data_version (data_version_id, name, source, start_date, end_date, tables, snapshot_hash)
               VALUES ($1, $2, $3, $4, $5, $6, '')
               ON CONFLICT (data_version_id) DO NOTHING"#,
        )
        .bind(dv_id)
        .bind(name)
        .bind(source)
        .bind(start_date)
        .bind(end_date)
        .bind(tables)
        .execute(self.pool)
        .await?;
        Ok(())
    }
}

/// 生成时间戳格式的 data_version_id（原 sync/mod.rs::generated_data_version_id 集中化）。
///
/// 格式 `dv-{YYYYMMDD}-{HHMMSS}{ms}`，供 market_data 等同步路由生成唯一版本 ID。
/// 返回 DataVersionId 新类型，调用方需 .to_string() 兼容现有 String 接口。
pub fn generate_version_id() -> DataVersionId {
    DataVersionId::new(chrono::Utc::now().format("dv-%Y%m%d-%H%M%S%3f").to_string())
}

impl<'a> DataVersionRegistry for PgDataVersionRegistry<'a> {
    /// 注册一个新数据版本（简化契约：由实现生成 dv_id）。
    ///
    /// 当前等价 create_version 的简化入口：dv_id 格式 `dv-eod-{date}`，
    /// name=description，tables 留空。完整字段版本用 create_version。
    async fn register_version(
        &self,
        data_date: NaiveDate,
        source: &str,
        description: Option<&str>,
    ) -> Result<DataVersionId, DataVersionRegistryError> {
        let dv_id = format!("dv-eod-{}", data_date.format("%Y%m%d"));
        let name = description.unwrap_or(source);
        self.create_version(&dv_id, name, source, &[], data_date, data_date)
            .await?;
        Ok(DataVersionId::new(dv_id))
    }

    /// 查询版本状态（调仓前 PIT 校验）。
    ///
    /// state 列已落地（2026-09-19 P3 DDL）：active/deprecated 两态，
    /// 存量行迁移时全量默认 active（与旧行为"存在即 Active"零差异）。
    async fn resolve_state(
        &self,
        version_id: &DataVersionId,
    ) -> Result<DataVersionState, DataVersionRegistryError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT state FROM data_version WHERE data_version_id = $1")
                .bind(version_id.as_str())
                .fetch_optional(self.pool)
                .await?;
        match row {
            Some((state,)) if state == "deprecated" => Ok(DataVersionState::Deprecated),
            Some(_) => Ok(DataVersionState::Active),
            None => Err(DataVersionRegistryError::NotFound(
                version_id.as_str().to_string(),
            )),
        }
    }

    /// 标记版本废弃（数据发现问题后阻断新引用）。
    ///
    /// 幂等：重复 deprecate 同一版本不报错；未注册的 dv_id 报 NotFound。
    async fn deprecate(&self, version_id: &DataVersionId) -> Result<(), DataVersionRegistryError> {
        let result = sqlx::query(
            "UPDATE data_version SET state = 'deprecated'
             WHERE data_version_id = $1 AND state = 'active'",
        )
        .bind(version_id.as_str())
        .execute(self.pool)
        .await?;
        if result.rows_affected() == 0 {
            // 幂等分支存在性探测（2026-09-21 修复：PG 字面量 1 是 INT4，解 (i64,)
            // 必报 ColumnDecode——已废弃版本重复废弃曾误报 Database 错而非幂等 Ok，
            // 由覆盖率第四批测试暴露）。显式 ::int8 对齐 i64。
            let exists: Option<(i64,)> =
                sqlx::query_as("SELECT 1::int8 FROM data_version WHERE data_version_id = $1")
                    .bind(version_id.as_str())
                    .fetch_optional(self.pool)
                    .await?;
            if exists.is_none() {
                return Err(DataVersionRegistryError::NotFound(
                    version_id.as_str().to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl PgDataVersionRegistry<'_> {
    /// 批量校验 dv_id 注册状态（Step 4a：VerifiedBar 构造前置校验的单次查询）。
    ///
    /// 返回**可引用**（active）的 dv_id 集合；deprecated 或未注册的 dv_id
    /// 不在集合中。批量语义对齐 quant-backtest types.rs 注释
    /// （`WHERE data_version_id = ANY($1)`，避免逐条 EXISTS 的 N+1）。
    pub async fn verify_registered(
        &self,
        dv_ids: &[String],
    ) -> Result<std::collections::HashSet<String>, DataVersionRegistryError> {
        if dv_ids.is_empty() {
            return Ok(std::collections::HashSet::new());
        }
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT data_version_id FROM data_version
             WHERE data_version_id = ANY($1) AND state = 'active'",
        )
        .bind(dv_ids)
        .fetch_all(self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}

#[cfg(test)]
mod fourth_batch_tests {
    use super::*;
    use crate::tushare::test_support::local_pool;

    // ─── 纯函数：状态语义 / Display / 错误格式 / ID 生成 ───────────

    #[test]
    fn data_version_state_referenceability_follows_active_only() {
        assert!(!DataVersionState::Draft.is_referenceable());
        assert!(DataVersionState::Active.is_referenceable());
        assert!(!DataVersionState::Deprecated.is_referenceable());
    }

    #[test]
    fn data_version_state_display_renders_snake_case() {
        assert_eq!(DataVersionState::Draft.to_string(), "draft");
        assert_eq!(DataVersionState::Active.to_string(), "active");
        assert_eq!(DataVersionState::Deprecated.to_string(), "deprecated");
    }

    #[test]
    fn registry_error_variants_render_messages() {
        let err = DataVersionRegistryError::NotFound("dv-x".to_string());
        assert_eq!(err.to_string(), "data version not found: dv-x");

        let err = DataVersionRegistryError::InvalidState {
            state: DataVersionState::Deprecated,
        };
        assert_eq!(
            err.to_string(),
            "version is deprecated, cannot be referenced"
        );

        // sqlx::Error 经 #[from] 自动转入 Database 变体
        let err: DataVersionRegistryError = sqlx::Error::RowNotFound.into();
        assert!(err.to_string().starts_with("database error:"));
    }

    #[test]
    fn generate_version_id_uses_timestamp_shape() {
        let id = generate_version_id();
        let s = id.as_str();
        // 格式 dv-{YYYYMMDD}-{HHMMSS}{3位毫秒}：前缀 + 8 位日期 + 分隔 + 9 位数字
        let rest = s.strip_prefix("dv-").expect("dv- 前缀");
        let (date_part, time_part) = rest.split_once('-').expect("日期/时间以 - 分隔");
        assert_eq!(date_part.len(), 8, "8 位日期，实际 {}", date_part);
        assert_eq!(time_part.len(), 9, "HHMMSS+3 位毫秒，实际 {}", time_part);
        assert!(date_part.bytes().all(|b| b.is_ascii_digit()));
        assert!(time_part.bytes().all(|b| b.is_ascii_digit()));
        // 唯一性（毫秒位区分）：并行下两次调用可能落在同一毫秒，单对 assert_ne
        // 偶发脆弱——改为快速连生成多次，出现至少 2 个不同值即证毫秒位参与区分
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..2000 {
            distinct.insert(generate_version_id().as_str().to_string());
        }
        assert!(distinct.len() >= 2, "2000 次生成应跨毫秒产生不同 id");
    }

    // ─── 连库：注册中心生命周期（真实本机 PG，zzz 语义键）──────────

    /// 独占键：2099-12-31 远离真实 dv-eod-{交易日} 序列，精确清理
    const ZZZ_EOD_DV: &str = "dv-eod-20991231";

    #[tokio::test]
    async fn pg_registry_register_resolve_deprecate_lifecycle() {
        let pool = local_pool().await;
        let zzz_date = chrono::NaiveDate::from_ymd_opt(2099, 12, 31).unwrap();
        let _ = sqlx::query("DELETE FROM data_version WHERE data_version_id = $1")
            .bind(ZZZ_EOD_DV)
            .execute(&pool)
            .await;

        let reg = PgDataVersionRegistry::new(&pool);

        // 注册 → Active；重复注册幂等（同 ID ON CONFLICT DO NOTHING）
        let id = reg
            .register_version(zzz_date, "tushare", Some("第四批测试版本"))
            .await
            .expect("注册应成功");
        assert_eq!(id.as_str(), ZZZ_EOD_DV);
        reg.register_version(zzz_date, "tushare", None)
            .await
            .expect("重复注册应幂等");

        assert_eq!(
            reg.resolve_state(&id).await.expect("解析状态"),
            DataVersionState::Active
        );
        assert!(reg.resolve_state(&id).await.unwrap().is_referenceable());

        // 废弃 → Deprecated；重复废弃幂等
        reg.deprecate(&id).await.expect("废弃应成功");
        assert_eq!(
            reg.resolve_state(&id).await.expect("解析状态 2"),
            DataVersionState::Deprecated
        );
        reg.deprecate(&id).await.expect("重复废弃应幂等");

        // 未注册 dv_id → NotFound
        let missing = DataVersionId::new("dv-eod-20991231-not-exist");
        match reg.resolve_state(&missing).await {
            Err(DataVersionRegistryError::NotFound(dv)) => assert_eq!(dv, missing.as_str()),
            other => panic!("应报 NotFound，实际 {:?}", other.map(|_| ())),
        }
        // 废弃未注册 dv_id → NotFound（而非静默成功）
        match reg.deprecate(&missing).await {
            Err(DataVersionRegistryError::NotFound(_)) => {}
            other => panic!("deprecate 未注册版本应报 NotFound，实际 {:?}", other),
        }

        let _ = sqlx::query("DELETE FROM data_version WHERE data_version_id = $1")
            .bind(ZZZ_EOD_DV)
            .execute(&pool)
            .await;
    }

    #[tokio::test]
    async fn pg_registry_verify_registered_and_latest_eod() {
        let pool = local_pool().await;
        const ZZZ_V1: &str = "zzz-test-dv-vr1";
        const ZZZ_V2: &str = "zzz-test-dv-vr2";
        for dv in [ZZZ_V1, ZZZ_V2] {
            let _ = sqlx::query("DELETE FROM data_version WHERE data_version_id = $1")
                .bind(dv)
                .execute(&pool)
                .await;
        }

        let reg = PgDataVersionRegistry::new(&pool);

        // 空入参短路：不查库直接返回空集
        assert!(reg.verify_registered(&[]).await.unwrap().is_empty());

        let span = (
            chrono::NaiveDate::from_ymd_opt(2099, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2099, 12, 31).unwrap(),
        );
        // create_version 幂等：同 dv_id 连续两次均 Ok
        reg.create_version(
            ZZZ_V1,
            "活跃版本",
            "tushare",
            &["market_stock"],
            span.0,
            span.1,
        )
        .await
        .expect("创建 v1");
        reg.create_version(
            ZZZ_V1,
            "活跃版本",
            "tushare",
            &["market_stock"],
            span.0,
            span.1,
        )
        .await
        .expect("重复创建 v1 幂等");
        reg.create_version(
            ZZZ_V2,
            "待废弃版本",
            "tushare",
            &["market_stock"],
            span.0,
            span.1,
        )
        .await
        .expect("创建 v2");
        reg.deprecate(&DataVersionId::new(ZZZ_V2))
            .await
            .expect("废弃 v2");

        // 批量校验：只返回 active 子集，deprecated 与未注册均被过滤
        let ids = vec![
            ZZZ_V1.to_string(),
            ZZZ_V2.to_string(),
            "zzz-test-dv-none".to_string(),
        ];
        let verified = reg.verify_registered(&ids).await.expect("批量校验");
        assert_eq!(verified.len(), 1);
        assert!(verified.contains(ZZZ_V1));

        // latest_eod_version_id：有 dv-eod-* 取最新，无则回退研究基线
        let latest = reg.latest_eod_version_id().await;
        assert!(
            latest == "research-full-2016-2026-20260515" || latest.starts_with("dv-eod-"),
            "应返回基线或 dv-eod 前缀，实际 {}",
            latest
        );

        for dv in [ZZZ_V1, ZZZ_V2] {
            let _ = sqlx::query("DELETE FROM data_version WHERE data_version_id = $1")
                .bind(dv)
                .execute(&pool)
                .await;
        }
    }
}
