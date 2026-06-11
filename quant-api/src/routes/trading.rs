//! 交易系统模块 — 计划交易、实际交易、融资交易
//!
//! 核心公式：净资产(current_nav) = 持仓市值 + cash - margin_amount
//!
//! 交易类型：
//! - buy/sell: 计划买卖资产
//! - borrow: 融资借款（借钱买入），cash↑ margin_amount↑
//! - repay:  融资归还（还钱），cash↓ margin_amount↓

use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

fn short_id() -> String { Uuid::new_v4().to_string().split('-').next().unwrap().to_string() }

// ── 计划交易 ──────────────────────────────────────────

pub struct PlannedTrade {
    pub account_id: String,
    pub symbol: String,
    pub side: String,            // buy / sell
    pub target_quantity: Decimal,
    pub target_price: Decimal,
    pub price_upper_limit: Option<Decimal>,
    pub price_lower_limit: Option<Decimal>,
    pub slippage_pct: f64,
    pub target_value: Decimal,
    pub reason: Option<String>,
    pub strategy_version_id: Option<String>,
}

// ── 实际交易 ──────────────────────────────────────────

pub struct ActualTrade {
    pub planned_order_id: String,
    pub fill_price: Decimal,
    pub fill_quantity: Decimal,
    pub fill_amount: Decimal,
    pub commission: Decimal,
    pub tax: Decimal,
    pub slippage: Decimal,
    pub fill_status: String, // filled / partial
}

// ── 创建计划交易 ──────────────────────────────────────

pub async fn create_planned_trade(
    db: &PgPool,
    trade: &PlannedTrade,
) -> Result<String, String> {
    let order_id = format!("po-{}", short_id());
    sqlx::query(
        "INSERT INTO paper_order (order_id, paper_account_id, strategy_version_id,
         symbol, side, order_type, quantity, limit_price, status, reason,
         target_price, price_upper_limit, price_lower_limit, slippage_pct, target_value)
         VALUES ($1,$2,$3,$4,$5,'market',$6,$7,'pending',$8,$9,$10,$11,$12,$13)",
    )
    .bind(&order_id)
    .bind(&trade.account_id)
    .bind(trade.strategy_version_id.as_deref())
    .bind(&trade.symbol)
    .bind(&trade.side)
    .bind(trade.target_quantity)
    .bind(trade.target_price)
    .bind(trade.reason.as_deref())
    .bind(trade.target_price)
    .bind(trade.price_upper_limit)
    .bind(trade.price_lower_limit)
    .bind(Decimal::from_f64_retain(trade.slippage_pct).unwrap_or(Decimal::ZERO))
    .bind(trade.target_value)
    .execute(db)
    .await
    .map_err(|e| format!("create_planned_trade: {}", e))?;

    Ok(order_id)
}

// ── 执行实际交易 ──────────────────────────────────────

pub async fn execute_actual_trade(
    db: &PgPool,
    account_id: &str,
    order_id: &str,
    fill: &ActualTrade,
) -> Result<String, String> {
    let fill_id = format!("pf-{}", short_id());
    sqlx::query(
        "INSERT INTO paper_fill (fill_id, order_id, paper_account_id, symbol,
         fill_time, side, quantity, price, amount, commission, tax, slippage,
         planned_order_id, fill_status)
         SELECT $1, $2, $3, symbol, now(), side, $4, $5, $6, $7, $8, $9, $10, $11
         FROM paper_order WHERE order_id = $2",
    )
    .bind(&fill_id)
    .bind(order_id)
    .bind(account_id)
    .bind(fill.fill_quantity)
    .bind(fill.fill_price)
    .bind(fill.fill_amount)
    .bind(fill.commission)
    .bind(fill.tax)
    .bind(fill.slippage)
    .bind(order_id) // planned_order_id = order_id
    .bind(&fill.fill_status)
    .execute(db)
    .await
    .map_err(|e| format!("execute_actual_trade: {}", e))?;

    // 更新计划交易状态
    sqlx::query("UPDATE paper_order SET status = $1 WHERE order_id = $2")
        .bind(&fill.fill_status)
        .bind(order_id)
        .execute(db)
        .await
        .map_err(|e| format!("update order status: {}", e))?;

    Ok(fill_id)
}

// ── 模拟执行（计划=实际） ─────────────────────────────

pub async fn execute_simulated_trade(
    db: &PgPool,
    trade: &PlannedTrade,
) -> Result<(String, String), String> {
    let order_id = create_planned_trade(db, trade).await?;
    let fill = ActualTrade {
        planned_order_id: order_id.clone(),
        fill_price: trade.target_price,
        fill_quantity: trade.target_quantity,
        fill_amount: trade.target_value,
        commission: Decimal::ZERO,
        tax: Decimal::ZERO,
        slippage: Decimal::ZERO,
        fill_status: "filled".to_string(),
    };
    let fill_id = execute_actual_trade(db, &trade.account_id, &order_id, &fill).await?;
    Ok((order_id, fill_id))
}

// ── 融资借款 ──────────────────────────────────────────

pub async fn execute_margin_borrow(
    db: &PgPool,
    account_id: &str,
    amount: Decimal,
    reason: &str,
) -> Result<String, String> {
    if amount <= Decimal::ZERO {
        return Err("融资借款金额必须大于0".into());
    }
    let trade_id = format!("mt-{}", short_id());
    sqlx::query(
        "INSERT INTO paper_margin_trade (margin_trade_id, paper_account_id, side, amount, reason)
         VALUES ($1, $2, 'borrow', $3, $4)",
    )
    .bind(&trade_id).bind(account_id).bind(amount).bind(reason)
    .execute(db).await.map_err(|e| format!("margin_borrow: {}", e))?;

    // 更新账户：现金增加，融资金额增加
    sqlx::query(
        "UPDATE paper_account SET cash = cash + $1, margin_amount = COALESCE(margin_amount,0) + $1 WHERE paper_account_id = $2"
    )
    .bind(amount).bind(account_id)
    .execute(db).await.map_err(|e| format!("margin_borrow update: {}", e))?;

    Ok(trade_id)
}

// ── 融资归还 ──────────────────────────────────────────

pub async fn execute_margin_repay(
    db: &PgPool,
    account_id: &str,
    amount: Decimal,
    reason: &str,
) -> Result<String, String> {
    if amount <= Decimal::ZERO {
        return Err("融资归还金额必须大于0".into());
    }
    // 检查当前 margin_amount 是否足够
    let row: Option<(Option<rust_decimal::Decimal>,)> = sqlx::query_as(
        "SELECT COALESCE(margin_amount,0) FROM paper_account WHERE paper_account_id = $1"
    ).bind(account_id).fetch_optional(db).await.map_err(|e| format!("query: {}", e))?;

    let current_margin = row.and_then(|(m,)| m).unwrap_or(Decimal::ZERO);
    if amount > current_margin {
        return Err(format!("融资归还金额({})超过当前融资金额({})", amount, current_margin));
    }

    let trade_id = format!("mt-{}", short_id());
    sqlx::query(
        "INSERT INTO paper_margin_trade (margin_trade_id, paper_account_id, side, amount, reason)
         VALUES ($1, $2, 'repay', $3, $4)",
    )
    .bind(&trade_id).bind(account_id).bind(amount).bind(reason)
    .execute(db).await.map_err(|e| format!("margin_repay: {}", e))?;

    // 更新账户：现金减少，融资金额减少
    sqlx::query(
        "UPDATE paper_account SET cash = cash - $1, margin_amount = margin_amount - $1 WHERE paper_account_id = $2"
    )
    .bind(amount).bind(account_id)
    .execute(db).await.map_err(|e| format!("margin_repay update: {}", e))?;

    Ok(trade_id)
}

// ── 自动归还融资 ──────────────────────────────────────

/// 如果 cash > reserve_amount，归还超出部分（不超过 margin_amount）
pub async fn try_auto_repay(db: &PgPool, account_id: &str) -> Result<Option<String>, String> {
    let row: Option<(Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>)> = sqlx::query_as(
        "SELECT COALESCE(cash,0), COALESCE(margin_amount,0), COALESCE(reserve_amount,0)
         FROM paper_account WHERE paper_account_id = $1"
    ).bind(account_id).fetch_optional(db).await.ok().flatten();

    let (cash, margin, reserve) = match row {
        Some((c, m, r)) => (
            c.unwrap_or(Decimal::ZERO),
            m.unwrap_or(Decimal::ZERO),
            r.unwrap_or(Decimal::ZERO),
        ),
        None => return Ok(None),
    };

    let excess = cash - reserve;
    if excess <= Decimal::ZERO || margin <= Decimal::ZERO {
        return Ok(None);
    }

    let repay_amount = if excess < margin { excess } else { margin };
    execute_margin_repay(db, account_id, repay_amount, "自动归还多余融资").await.map(Some)
}

// ── 更新净资产 ────────────────────────────────────────

pub async fn update_current_nav(db: &PgPool, account_id: &str) -> Result<(), String> {
    sqlx::query(
        "UPDATE paper_account SET
         current_nav = (SELECT COALESCE(SUM(market_value),0) FROM paper_position WHERE paper_account_id = $1)
                     + COALESCE(cash,0) - COALESCE(margin_amount,0)
         WHERE paper_account_id = $1"
    )
    .bind(account_id)
    .execute(db)
    .await
    .map_err(|e| format!("update_nav: {}", e))?;
    Ok(())
}
