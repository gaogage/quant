//! NAV 快照仓储（DDD R5：消除 5 处重复 INSERT，统一写入出口）。
//!
//! 原 5 处重复：
//! - paper.rs:299（初始化快照）
//! - paper.rs:844（完整快照含 benchmark/excess_return）
//! - mvo_engine.rs:275（回测快照）
//! - scheduler.rs:3185（EOD 日报快照）
//! - scheduler.rs:3351（调仓后即时快照）
//!
//! 统一为 [`upsert_nav_snapshot`]，调用方构造 [`NavSnapshot`] 传参，
//! ON CONFLICT (paper_account_id, snapshot_date) DO UPDATE 覆盖。

use chrono::NaiveDate;
use sqlx::PgPool;

/// NAV 快照（实盘账号每日净值快照）。
///
/// 必填字段：account_id / date / nav / cash / market_value / position_count。
/// 其余为 Option，调用方按场景填充，None 字段写 NULL。
#[derive(Debug, Clone)]
pub struct NavSnapshot {
    pub account_id: String,
    pub date: NaiveDate,
    pub nav: f64,
    pub cash: f64,
    pub market_value: f64,
    pub position_count: i32,
    /// 当日收益率
    pub daily_return: Option<f64>,
    /// 累计收益率
    pub cumulative_return: Option<f64>,
    /// 基准收益率
    pub benchmark_return: Option<f64>,
    /// 超额收益率
    pub excess_return: Option<f64>,
    /// 最大回撤
    pub max_drawdown: Option<f64>,
    /// 运行 Sharpe
    pub running_sharpe: Option<f64>,
    /// 策略版本 ID
    pub strategy_version_id: Option<String>,
    /// 预测集 ID
    pub prediction_set_id: Option<String>,
    /// 信号数
    pub signal_count: Option<i32>,
    /// 成交笔数
    pub trade_count: Option<i32>,
}

impl NavSnapshot {
    /// 构造最小快照（必填字段，其余 None）
    pub fn new(account_id: impl Into<String>, date: NaiveDate, nav: f64) -> Self {
        Self {
            account_id: account_id.into(),
            date,
            nav,
            cash: 0.0,
            market_value: 0.0,
            position_count: 0,
            daily_return: None,
            cumulative_return: None,
            benchmark_return: None,
            excess_return: None,
            max_drawdown: None,
            running_sharpe: None,
            strategy_version_id: None,
            prediction_set_id: None,
            signal_count: None,
            trade_count: None,
        }
    }
}

/// 写入或更新 NAV 快照（ON CONFLICT 覆盖）。
///
/// 幂等：同 (account_id, date) 重复写入会 UPDATE 而非报错。
/// snap_id 自动生成（调用方无需传）。
pub async fn upsert_nav_snapshot(db: &PgPool, snap: &NavSnapshot) -> Result<(), String> {
    let snap_id = format!("ns-{}", uuid::Uuid::new_v4());
    sqlx::query(
        "INSERT INTO paper_nav_snapshot
            (nav_snapshot_id, paper_account_id, snapshot_date, nav, cash, market_value,
             position_count, daily_return, cumulative_return, benchmark_return, excess_return,
             max_drawdown, running_sharpe, strategy_version_id, prediction_set_id,
             signal_count, trade_count, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now())
         ON CONFLICT (paper_account_id, snapshot_date) DO UPDATE SET
            nav = EXCLUDED.nav, cash = EXCLUDED.cash, market_value = EXCLUDED.market_value,
            position_count = EXCLUDED.position_count, daily_return = EXCLUDED.daily_return,
            cumulative_return = EXCLUDED.cumulative_return, benchmark_return = EXCLUDED.benchmark_return,
            excess_return = EXCLUDED.excess_return, max_drawdown = EXCLUDED.max_drawdown,
            running_sharpe = EXCLUDED.running_sharpe, strategy_version_id = EXCLUDED.strategy_version_id,
            prediction_set_id = EXCLUDED.prediction_set_id, signal_count = EXCLUDED.signal_count,
            trade_count = EXCLUDED.trade_count",
    )
    .bind(&snap_id)
    .bind(&snap.account_id)
    .bind(snap.date)
    .bind(snap.nav)
    .bind(snap.cash)
    .bind(snap.market_value)
    .bind(snap.position_count)
    .bind(snap.daily_return)
    .bind(snap.cumulative_return)
    .bind(snap.benchmark_return)
    .bind(snap.excess_return)
    .bind(snap.max_drawdown)
    .bind(snap.running_sharpe)
    .bind(&snap.strategy_version_id)
    .bind(&snap.prediction_set_id)
    .bind(snap.signal_count)
    .bind(snap.trade_count)
    .execute(db)
    .await
    .map_err(|e| format!("upsert_nav_snapshot {}: {}", snap.account_id, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nav_snapshot_new_sets_required_fields() {
        let s = NavSnapshot::new("acc1", NaiveDate::from_ymd_opt(2026, 7, 30).unwrap(), 1_000_000.0);
        assert_eq!(s.account_id, "acc1");
        assert_eq!(s.nav, 1_000_000.0);
        assert_eq!(s.cash, 0.0);
        assert_eq!(s.position_count, 0);
        assert!(s.daily_return.is_none());
        assert!(s.trade_count.is_none());
    }
}
