//! Paper 持仓仓储（DDD 重构 R5b 续批：收敛 paper_position 散落 SQL）。
//!
//! 与 paper_account_repo.rs 同款设计：trait 定义统一契约，PG 实现同文件，
//! 原生 `impl Future`（非 async_trait），错误复用 `PaperRepositoryError`。
//!
//! 收敛纪律（R5b 教训）：读侧重叠组只收敛逐字相同的 SQL，变体保留原处；
//! 写侧（批次 3，2026-09-19）按「单点语义方法」收敛——每处写 SQL 语义
//! 独立（信号占位/成交累积/减仓/清零/镜像重建），**不是合并去重，而是
//! 把资金核心 SQL 收进统一持久化入口**，调用方只依赖 trait 契约。
//!
//! 两种 upsert 严禁合并（R5b 关键设计决定）：
//! - [`PaperPositionRepository::upsert_signal_position`]：信号层写入，quantity=
//!   权重占位、avg_cost=0，只更新 target_weight——**不碰 avg_cost/quantity 语义**；
//! - [`PaperPositionRepository::upsert_on_fill`]：成交层写入，移动加权 avg_cost +
//!   quantity 累加——资金语义。强行合并会引入 bug。
//!
//! 留批次 4（资金路径最高风险，需 paper 盘验证）：mark_to_market 事务大 CTE、
//! apply_fill 的 cash/margin UPDATE 族。

use chrono::NaiveDate;
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

    /// 信号层 upsert：只写 target_weight 占位（paper.rs 策略信号路径，单点）。
    ///
    /// quantity = weight（占位语义）、avg_cost = 0，冲突时只更新
    /// target_weight/last_trade_date/updated_at——**严禁与成交 upsert 合并**。
    fn upsert_signal_position(
        &self,
        position_id: &str,
        account_id: &str,
        symbol: &str,
        weight: f64,
        trade_date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;

    /// 成交层 upsert：移动加权 avg_cost + quantity 累加（rebalance 资金核心，单点）。
    ///
    /// 买入成交写持仓：新仓直插（market_value=qty×price），存续仓 avg_cost 按移动
    /// 加权重算、quantity 累加、market_value 重估。资金语义，I 组严禁合并教训载体。
    fn upsert_on_fill(
        &self,
        position_id: &str,
        account_id: &str,
        symbol: &str,
        qty: Decimal,
        fill_price: Decimal,
        date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;

    /// 卖出减仓：quantity -= qty（守卫 quantity >= qty）+ market_value 重估（单点）。
    ///
    /// 资金核心。行不存在或数量不足时静默无操作（rows_affected=0），
    /// 调用方按原逻辑以 SQL 是否报错判定成败。
    fn reduce_quantity(
        &self,
        account_id: &str,
        symbol: &str,
        qty: Decimal,
        date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;

    /// 清零行删除：quantity <= 0 的行整账号清理（保持持仓表干净，单点）。
    fn delete_zero_quantity(
        &self,
        account_id: &str,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;

    /// T+1 卖出门控：symbol 当日买入（last_trade_date = date 且 quantity > 0）不可卖。
    ///
    /// last_trade_date IS NULL（历史数据未标记）兜底放行，避免误拦正常持仓。
    fn exists_t1_blocked(
        &self,
        account_id: &str,
        symbol: &str,
        date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<bool, PaperRepositoryError>> + Send;

    /// 镜像全清：删除账号全部持仓（ptrade_report 镜像重建前置，高风险单点）。
    fn delete_all_positions(
        &self,
        account_id: &str,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;

    /// 镜像重建 INSERT：PTrade 回报持仓直插（avg_cost 用现价近似，单点）。
    ///
    /// 无冲突处理（DELETE 先行清场），绩效口径以 NAV 为准。
    fn insert_mirror_position(
        &self,
        position_id: &str,
        account_id: &str,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
        market_value: Decimal,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send;
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

    /// 信号层 upsert（原 paper.rs:277，SQL 逐字搬移）。
    fn upsert_signal_position(
        &self,
        position_id: &str,
        account_id: &str,
        symbol: &str,
        weight: f64,
        trade_date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            sqlx::query(
                "INSERT INTO paper_position
                   (paper_position_id, paper_account_id, symbol, quantity, avg_cost,
                    target_weight, last_trade_date, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, 0, $5, $6, now(), now())
                 ON CONFLICT (paper_account_id, symbol)
                 DO UPDATE SET target_weight = EXCLUDED.target_weight,
                               last_trade_date = EXCLUDED.last_trade_date,
                               updated_at = now()",
            )
            .bind(position_id)
            .bind(account_id)
            .bind(symbol)
            .bind(weight) // quantity = weight（占位语义，原样保留）
            .bind(weight)
            .bind(trade_date)
            .execute(self.pool)
            .await?;
            Ok(())
        }
    }

    /// 成交层 upsert：移动加权（原 rebalance.rs apply_fill_common_buy，SQL 逐字搬移）。
    fn upsert_on_fill(
        &self,
        position_id: &str,
        account_id: &str,
        symbol: &str,
        qty: Decimal,
        fill_price: Decimal,
        date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            sqlx::query(
                "INSERT INTO paper_position (paper_position_id, paper_account_id, symbol, quantity, avg_cost, market_price, market_value, last_trade_date)
                 VALUES ($1, $2, $3, $4, $5, $5, $4*$5, $6)
                 ON CONFLICT (paper_account_id, symbol) DO UPDATE SET
                     avg_cost = (paper_position.avg_cost * paper_position.quantity + EXCLUDED.avg_cost * EXCLUDED.quantity)
                                / (paper_position.quantity + EXCLUDED.quantity),
                     quantity = paper_position.quantity + EXCLUDED.quantity,
                     market_price = EXCLUDED.market_price,
                     market_value = (paper_position.quantity + EXCLUDED.quantity) * EXCLUDED.market_price,
                     last_trade_date = $6",
            )
            .bind(position_id)
            .bind(account_id)
            .bind(symbol)
            .bind(qty)
            .bind(fill_price)
            .bind(date)
            .execute(self.pool)
            .await?;
            Ok(())
        }
    }

    /// 卖出减仓（原 rebalance.rs apply_fill_common_sell，SQL 逐字搬移）。
    fn reduce_quantity(
        &self,
        account_id: &str,
        symbol: &str,
        qty: Decimal,
        date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            sqlx::query(
                "UPDATE paper_position SET quantity = quantity - $3,
                     market_value = (quantity - $3) * market_price,
                     last_trade_date = $4
                 WHERE paper_account_id = $1 AND symbol = $2 AND quantity >= $3",
            )
            .bind(account_id)
            .bind(symbol)
            .bind(qty)
            .bind(date)
            .execute(self.pool)
            .await?;
            Ok(())
        }
    }

    /// 清零行删除（原 rebalance.rs apply_fill_common_sell 尾部，SQL 逐字搬移）。
    fn delete_zero_quantity(
        &self,
        account_id: &str,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            sqlx::query("DELETE FROM paper_position WHERE paper_account_id = $1 AND quantity <= 0")
                .bind(account_id)
                .execute(self.pool)
                .await?;
            Ok(())
        }
    }

    /// T+1 卖出门控（原 rebalance.rs apply_fill_common_sell 前置，SQL 逐字搬移）。
    fn exists_t1_blocked(
        &self,
        account_id: &str,
        symbol: &str,
        date: NaiveDate,
    ) -> impl std::future::Future<Output = Result<bool, PaperRepositoryError>> + Send {
        async move {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM paper_position
                  WHERE paper_account_id=$1 AND symbol=$2 AND quantity>0
                    AND last_trade_date IS NOT NULL AND last_trade_date = $3)",
            )
            .bind(account_id)
            .bind(symbol)
            .bind(date)
            .fetch_one(self.pool)
            .await?;
            Ok(blocked)
        }
    }

    /// 镜像全清（原 ptrade_report.rs，SQL 逐字搬移）。
    fn delete_all_positions(
        &self,
        account_id: &str,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            sqlx::query("DELETE FROM paper_position WHERE paper_account_id=$1")
                .bind(account_id)
                .execute(self.pool)
                .await?;
            Ok(())
        }
    }

    /// 镜像重建 INSERT（原 ptrade_report.rs，SQL 逐字搬移）。
    fn insert_mirror_position(
        &self,
        position_id: &str,
        account_id: &str,
        symbol: &str,
        quantity: Decimal,
        price: Decimal,
        market_value: Decimal,
    ) -> impl std::future::Future<Output = Result<(), PaperRepositoryError>> + Send {
        async move {
            sqlx::query(
                "INSERT INTO paper_position (paper_position_id, paper_account_id, symbol,
                   quantity, avg_cost, market_price, market_value, created_at, updated_at)
                 VALUES ($1,$2,$3,$4,$5,$5,$6,now(),now())",
            )
            .bind(position_id)
            .bind(account_id)
            .bind(symbol)
            .bind(quantity)
            .bind(price)
            .bind(market_value)
            .execute(self.pool)
            .await?;
            Ok(())
        }
    }
}
