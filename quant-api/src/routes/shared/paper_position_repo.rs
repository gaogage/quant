//! Paper 持仓仓储（DDD 重构 R5b 续批：收敛 paper_position 散落 SQL）。
//!
//! 与 paper_account_repo.rs 同款设计：trait 定义统一契约，PG 实现同文件，
//! 原生 `impl Future`（非 async_trait），错误复用 `PaperRepositoryError`。
//!
//! 收敛纪律（R5b 教训）：只收敛逐字相同的 SQL；变体驱动组（字段集/过滤/
//! JOIN 不同）保留原处不强行抽象。写路径（upsert/减仓/盯市）属资金核心，
//! 保留原处（见 paper_account_repo.rs 模块文档）。
//!
//! 分批推进：本文件首批只收敛报表侧完全相同的一对聚合查询
//! （report.rs:206/563，日报 vs 调仓后快照）。

use rust_decimal::Decimal;
use sqlx::PgPool;

use crate::routes::shared::paper_account_repo::PaperRepositoryError;

/// 持仓汇总行（COUNT + 市值 + 账号现金）。
///
/// 注：cash 来自 paper_account 的标量子查询，聚合以 position 为主体，
/// 归本仓储（跨表子查询不单独立方法）。
#[derive(Debug, Clone, Copy)]
pub struct PositionSummary {
    /// 持仓笔数（quantity > 0）。
    pub position_count: i64,
    /// 持仓市值 `SUM(quantity * COALESCE(market_price, avg_cost))`。
    pub market_value: Decimal,
    /// 账号现金（子查询读 paper_account.cash）。
    pub cash: Decimal,
}

/// Paper 持仓仓储契约。
///
/// 集中 paper_position 的只读查询，消除散落 SQL 样板。
/// 注：用原生 `impl Future`（Rust 1.75+），暂非 dyn-compatible（与 account 仓储一致）。
pub trait PaperPositionRepository {
    /// 查持仓汇总（重复组：report.rs 2 处 SQL 完全相同）。
    ///
    /// `SELECT COUNT(*)::bigint,
    ///         COALESCE(SUM(quantity * COALESCE(market_price, avg_cost)), 0),
    ///         COALESCE((SELECT cash FROM paper_account WHERE paper_account_id = $1), 0)
    ///  FROM paper_position WHERE paper_account_id = $1 AND quantity > 0`
    ///
    /// 无持仓行时 COUNT=0/SUM=0 仍返回一行（聚合查询特性）；账号不存在时
    /// cash 子查询为 NULL→COALESCE 0，同样返回一行（与原行为一致）。
    fn find_summary(
        &self,
        account_id: &str,
    ) -> impl std::future::Future<Output = Result<PositionSummary, PaperRepositoryError>> + Send;
}

/// PostgreSQL 实现的 paper 持仓仓储。
pub struct PgPaperPositionRepo<'a> {
    pool: &'a PgPool,
}

impl<'a> PgPaperPositionRepo<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

// R5b 有意的"原生 impl Future" trait 形态（与 paper_account_repo 同款），
// AFIT 迁移待 trait 定义与全部调用方一并现代化；显式豁免 manual_async_fn。
#[allow(clippy::manual_async_fn)]
impl<'a> PaperPositionRepository for PgPaperPositionRepo<'a> {
    /// 持仓汇总聚合（COUNT + 市值 + 现金三件套）。
    ///
    /// 2 处原样收敛：report.rs:206（钉钉日报）+ report.rs:563（调仓后快照推送）。
    fn find_summary(
        &self,
        account_id: &str,
    ) -> impl std::future::Future<Output = Result<PositionSummary, PaperRepositoryError>> + Send
    {
        async move {
            let row: (i64, Decimal, Decimal) = sqlx::query_as(
                "SELECT COUNT(*)::bigint,
                        COALESCE(SUM(quantity * COALESCE(market_price, avg_cost)), 0),
                        COALESCE((SELECT cash FROM paper_account WHERE paper_account_id = $1), 0)
                 FROM paper_position WHERE paper_account_id = $1 AND quantity > 0",
            )
            .bind(account_id)
            .fetch_one(self.pool)
            .await?;
            Ok(PositionSummary {
                position_count: row.0,
                market_value: row.1,
                cash: row.2,
            })
        }
    }
}
