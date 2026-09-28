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
//! Step 6 挂载（2026-09-28 任务82）：PaperGateway 实现落位 + 路由接入
//! （POST /execution/orders 提交、GET /execution/orders/{id} 查询）。
//! 渐进式策略：现有执行链（rebalance→trading）不动，Gateway 作为旁路契约端点
//! 供新调用方使用，验证契约可用后再逐步收编存量路径。

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
    pub submitted_at: chrono::DateTime<chrono::Utc>,
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
    #[allow(dead_code)] // 契约完备性预留: 网关超时语义, 接入方超时控制时启用
    Timeout,

    #[error("network error: {0}")]
    Network(String),
}

// ── Step 6 挂载：模拟盘 Gateway 实现（2026-09-28 任务82）──

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;

use crate::AppState;

/// 模拟盘执行网关：包装 paper_order 落库 + 模拟成交语义（与
/// trading::execute_simulated_trade 同源口径，走统一契约）。
pub struct PaperGateway {
    pub db: sqlx::PgPool,
    pub account_id: String,
    pub strategy_version_id: String,
}

impl ExecutionGateway for PaperGateway {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResult, ExecutionError> {
        let order_id = format!("eg-{}", uuid::Uuid::new_v4());
        let now = chrono::Utc::now();
        let symbol = request.symbol.as_str();
        // 模拟成交：限价直接按 limit_price 全额成交（模拟盘无排队语义）
        let fill_price = request
            .limit_price
            .ok_or_else(|| ExecutionError::Rejected("市价单暂不支持, 请传 limit_price".into()))?;
        if request.quantity <= Decimal::ZERO {
            return Err(ExecutionError::Rejected("quantity 必须为正".into()));
        }
        let insert = sqlx::query(
            "INSERT INTO paper_order (order_id, paper_account_id, strategy_version_id, \
             symbol, side, order_type, quantity, limit_price, target_price, status) \
             VALUES ($1, $2, $3, $4, $5, 'limit', $6, $7, $7, 'filled')",
        )
        .bind(&order_id)
        .bind(&self.account_id)
        .bind(&self.strategy_version_id)
        .bind(symbol)
        .bind(match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        })
        .bind(request.quantity)
        .bind(fill_price)
        .execute(&self.db)
        .await
        .map_err(|e| ExecutionError::Network(format!("paper_order insert: {e}")))?;
        if insert.rows_affected() == 0 {
            return Err(ExecutionError::Rejected("订单未落库".into()));
        }
        Ok(OrderResult {
            broker_order_id: order_id,
            status: OrderStatus::Filled,
            filled_quantity: request.quantity,
            avg_fill_price: Some(fill_price),
            submitted_at: now,
        })
    }

    async fn query_order(&self, broker_order_id: &str) -> Result<OrderResult, ExecutionError> {
        type Row = (
            String,
            String,
            Option<Decimal>,
            Option<chrono::DateTime<chrono::Utc>>,
        );
        let row: Option<Row> = sqlx::query_as(
            "SELECT side, status, quantity, created_at FROM paper_order WHERE order_id = $1",
        )
        .bind(broker_order_id)
        .fetch_optional(&self.db)
        .await
        .map_err(|e| ExecutionError::Network(format!("paper_order query: {e}")))?;
        let (_side, status, qty, created) =
            row.ok_or_else(|| ExecutionError::OrderNotFound(broker_order_id.into()))?;
        Ok(OrderResult {
            broker_order_id: broker_order_id.to_string(),
            status: match status.as_str() {
                "filled" => OrderStatus::Filled,
                "cancelled" => OrderStatus::Cancelled,
                "rejected" => OrderStatus::Rejected,
                "partially_filled" => OrderStatus::PartiallyFilled,
                _ => OrderStatus::Pending,
            },
            filled_quantity: if status == "filled" {
                qty.unwrap_or_default()
            } else {
                Decimal::ZERO
            },
            avg_fill_price: None,
            submitted_at: created.unwrap_or_default(),
        })
    }

    async fn cancel_order(&self, broker_order_id: &str) -> Result<(), ExecutionError> {
        let current = self.query_order(broker_order_id).await?;
        if current.status.is_terminal() {
            return Err(ExecutionError::NotCancellable {
                status: current.status,
            });
        }
        sqlx::query("UPDATE paper_order SET status = 'cancelled' WHERE order_id = $1")
            .bind(broker_order_id)
            .execute(&self.db)
            .await
            .map_err(|e| ExecutionError::Network(format!("cancel: {e}")))?;
        Ok(())
    }
}

/// POST /api/v1/quant/execution/orders —— 经 ExecutionGateway 契约提交订单（模拟盘）。
#[derive(Debug, Deserialize)]
pub struct SubmitOrderRequest {
    pub paper_account_id: String,
    pub strategy_version_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub limit_price: Option<Decimal>,
}

pub async fn submit_order_via_gateway(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SubmitOrderRequest>,
) -> impl IntoResponse {
    let gateway = PaperGateway {
        db: state.db.clone(),
        account_id: req.paper_account_id.clone(),
        strategy_version_id: req.strategy_version_id.clone(),
    };
    let order_req = OrderRequest {
        symbol: Symbol::new(req.symbol),
        side: req.side,
        quantity: req.quantity,
        limit_price: req.limit_price,
        mode: ExecutionMode::Paper,
    };
    match gateway.submit_order(order_req).await {
        Ok(result) => Json(json!({"code": 0, "data": result})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}

/// GET /api/v1/quant/execution/orders/{order_id} —— 查询订单（契约口径）。
pub async fn query_order_via_gateway(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(order_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let gateway = PaperGateway {
        db: state.db.clone(),
        account_id: String::new(),
        strategy_version_id: String::new(),
    };
    match gateway.query_order(&order_id).await {
        Ok(result) => (StatusCode::OK, Json(json!({"code": 0, "data": result}))),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({"code": 1, "message": e.to_string()})),
        ),
    }
}

/// POST /api/v1/quant/execution/orders/{order_id}/cancel —— 经契约取消订单。
pub async fn cancel_order_via_gateway(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(order_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let gateway = PaperGateway {
        db: state.db.clone(),
        account_id: String::new(),
        strategy_version_id: String::new(),
    };
    match gateway.cancel_order(&order_id).await {
        Ok(()) => Json(json!({"code": 0, "data": {"cancelled": true}})),
        Err(e) => Json(json!({"code": 1, "message": e.to_string()})),
    }
}
