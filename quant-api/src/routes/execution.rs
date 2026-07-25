//! 执行网关契约（BC5 执行订单，DDD 重构 Step 2 引入）。
//!
//! 背景：当前订单执行散在 routes/trading.rs、routes/rebalance.rs、routes/scheduler.rs，
//! 实盘/模拟盘/回测三种执行路径靠 if-else 分支区分，无统一抽象。Step 6 将统一
//! ExecutionGateway，解 scheduler<->rebalance 循环依赖。
//!
//! 设计原则（本步只引入 trait 骨架，不改现有实现）：
//! - trait 定义"提交/查询/取消订单"统一契约，三种执行路径各一个实现。
//! - `ExecutionMode` 区分回测（无滑点无延迟）/模拟盘（实时但无资金）/实盘（真实交易所）。
//!
//! 本模块尚未被路由挂载（Step 6 统一 ExecutionGateway 时接入），
//! 允许 dead_code 直到届时启用。

#![allow(dead_code)]

use chrono::NaiveDateTime;
use quant_common::identifiers::Symbol;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 订单方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

/// 订单状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// 已提交，待成交。
    Pending,
    /// 部分成交。
    PartiallyFilled,
    /// 完全成交。
    Filled,
    /// 已取消。
    Cancelled,
    /// 被拒绝。
    Rejected,
}

impl OrderStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

/// 执行模式（类型状态标记，Step 5 升级为编译期门禁）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// 回测：无滑点、无延迟、无真实交易所。
    Backtest,
    /// 模拟盘：实时行情但无真实资金。
    Paper,
    /// 实盘：真实交易所、真实资金。
    Live,
}

/// 订单请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: Symbol,
    pub side: OrderSide,
    /// 目标数量（股/手，由调用方约定单位）。
    pub quantity: Decimal,
    /// 限价单价格（None 表示市价单）。
    pub limit_price: Option<Decimal>,
    pub mode: ExecutionMode,
}

/// 订单执行结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResult {
    pub broker_order_id: String,
    pub status: OrderStatus,
    /// 已成交数量。
    pub filled_quantity: Decimal,
    /// 成交均价。
    pub avg_fill_price: Option<Decimal>,
    pub submitted_at: NaiveDateTime,
}

/// 执行网关契约。
///
/// 统一回测/模拟盘/实盘三种执行路径，Step 6 将用此 trait 解
/// scheduler<->rebalance 循环依赖（当前 rebalance 直接调 trading 函数，
/// scheduler 又调 rebalance，形成环）。
///
/// 本 trait 为 Step 2 纯新增，现有代码尚未实现它。
pub trait ExecutionGateway {
    /// 提交订单。
    fn submit_order(
        &self,
        request: OrderRequest,
    ) -> impl std::future::Future<Output = Result<OrderResult, ExecutionError>> + Send;

    /// 查询订单状态。
    fn query_order(
        &self,
        broker_order_id: &str,
    ) -> impl std::future::Future<Output = Result<OrderResult, ExecutionError>> + Send;

    /// 取消订单（仅 Pending/PartiallyFilled 可取消）。
    fn cancel_order(
        &self,
        broker_order_id: &str,
    ) -> impl std::future::Future<Output = Result<(), ExecutionError>> + Send;
}

/// 执行错误。
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("order not found: {0}")]
    OrderNotFound(String),

    #[error("order is {status:?}, cannot cancel")]
    NotCancellable { status: OrderStatus },

    #[error("broker rejected order: {0}")]
    Rejected(String),

    #[error("execution timeout")]
    Timeout,

    #[error("network error: {0}")]
    Network(String),
}
