use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::{FromPrimitive, Zero};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct CreatePaperAccountRequest {
    pub name: String,
    pub initial_capital: f64,
    pub base_currency: Option<String>,
    pub operator: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitPaperOrderRequest {
    pub paper_account_id: String,
    pub strategy_version_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub order_type: Option<String>,
    pub quantity: f64,
    pub limit_price: Option<f64>,
    pub estimated_price: Option<f64>,
    pub operator: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FillPaperOrderRequest {
    pub price: f64,
    pub quantity: Option<f64>,
    pub commission: Option<f64>,
    pub tax: Option<f64>,
    pub slippage: Option<f64>,
    pub operator: Option<String>,
    pub trace_id: Option<String>,
}

struct NormalizedAccountRequest {
    name: String,
    initial_capital: Decimal,
    base_currency: String,
    operator: Option<String>,
}

struct NormalizedOrderRequest {
    paper_account_id: String,
    strategy_version_id: Option<String>,
    symbol: String,
    side: String,
    order_type: String,
    quantity: Decimal,
    limit_price: Option<Decimal>,
    estimated_price: Option<Decimal>,
    operator: Option<String>,
    trace_id: String,
}

struct NormalizedFillRequest {
    price: Decimal,
    quantity: Option<Decimal>,
    commission: Decimal,
    tax: Decimal,
    slippage: Decimal,
    operator: Option<String>,
    trace_id: String,
}

pub async fn create_paper_account(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreatePaperAccountRequest>,
) -> impl IntoResponse {
    match create_paper_account_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn submit_paper_order(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SubmitPaperOrderRequest>,
) -> impl IntoResponse {
    match submit_paper_order_inner(&state.db, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn fill_paper_order(
    State(state): State<Arc<AppState>>,
    Path(order_id): Path<String>,
    Json(req): Json<FillPaperOrderRequest>,
) -> impl IntoResponse {
    match fill_paper_order_inner(&state.db, &order_id, req).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn paper_account_summary(
    State(state): State<Arc<AppState>>,
    Path(account_id): Path<String>,
) -> impl IntoResponse {
    match paper_account_summary_inner(&state.db, &account_id).await {
        Ok(data) => Json(json!({"code": 0, "data": data})),
        Err(message) => Json(json!({"code": 1, "message": message})),
    }
}

pub async fn paper_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let metrics = paper_metrics_inner(&state.db)
        .await
        .unwrap_or_else(|error| {
            json!({
                "error": error
            })
        });
    Json(json!({
        "code": 0,
        "data": {
            "service": "paper-trading",
            "status": if metrics.get("error").is_some() { "degraded" } else { "ok" },
            "metrics": metrics
        }
    }))
}

async fn create_paper_account_inner(
    db: &sqlx::PgPool,
    req: CreatePaperAccountRequest,
) -> Result<Value, String> {
    let req = normalize_account_request(req)?;
    let account_id = format!("pa-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO paper_account
           (paper_account_id, name, base_currency, initial_capital, cash, status)
         VALUES ($1, $2, $3, $4, $4, 'active')",
    )
    .bind(&account_id)
    .bind(&req.name)
    .bind(&req.base_currency)
    .bind(req.initial_capital)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to create paper_account: {}", error))?;

    write_audit_event(
        db,
        "paper_account.create",
        "paper_account",
        &account_id,
        req.operator.as_deref(),
        "Created paper account",
        json!({
            "initial_capital": req.initial_capital,
            "base_currency": req.base_currency
        }),
    )
    .await?;

    Ok(json!({
        "paper_account_id": account_id,
        "status": "active",
        "cash": req.initial_capital,
        "initial_capital": req.initial_capital
    }))
}

async fn submit_paper_order_inner(
    db: &sqlx::PgPool,
    req: SubmitPaperOrderRequest,
) -> Result<Value, String> {
    let req = normalize_order_request(req)?;
    let account = sqlx::query_as::<_, (String, Decimal)>(
        "SELECT status, cash FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(&req.paper_account_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load paper_account: {}", error))?
    .ok_or_else(|| "paper_account not found".to_string())?;

    let risk = evaluate_order_risk(&req, &account.0, account.1);
    let order_id = format!("po-{}", Uuid::new_v4());
    let status = if risk.passed { "submitted" } else { "rejected" };

    sqlx::query(
        "INSERT INTO paper_order
           (order_id, paper_account_id, strategy_version_id, symbol, side, order_type,
            quantity, limit_price, status, reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(&order_id)
    .bind(&req.paper_account_id)
    .bind(req.strategy_version_id.as_deref())
    .bind(&req.symbol)
    .bind(&req.side)
    .bind(&req.order_type)
    .bind(req.quantity)
    .bind(req.limit_price)
    .bind(status)
    .bind(risk.reason.as_deref())
    .execute(db)
    .await
    .map_err(|error| format!("Failed to insert paper_order: {}", error))?;

    write_audit_event(
        db,
        if risk.passed {
            "paper_order.submit"
        } else {
            "paper_order.risk_reject"
        },
        "paper_order",
        &order_id,
        req.operator.as_deref(),
        if risk.passed {
            "Submitted paper order"
        } else {
            "Rejected paper order"
        },
        json!({
            "paper_account_id": req.paper_account_id,
            "strategy_version_id": req.strategy_version_id,
            "symbol": req.symbol,
            "side": req.side,
            "quantity": req.quantity,
            "estimated_price": req.estimated_price,
            "risk_reason": risk.reason,
            "trace_id": req.trace_id
        }),
    )
    .await?;

    Ok(json!({
        "order_id": order_id,
        "status": status,
        "risk": {
            "passed": risk.passed,
            "reason": risk.reason
        },
        "trace_id": req.trace_id
    }))
}

async fn fill_paper_order_inner(
    db: &sqlx::PgPool,
    order_id: &str,
    req: FillPaperOrderRequest,
) -> Result<Value, String> {
    let req = normalize_fill_request(req)?;
    let order = sqlx::query_as::<_, (String, String, String, Decimal, String)>(
        "SELECT paper_account_id, symbol, side, quantity, status
         FROM paper_order
         WHERE order_id = $1",
    )
    .bind(order_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load paper_order: {}", error))?
    .ok_or_else(|| "paper_order not found".to_string())?;
    if order.4 != "submitted" && order.4 != "partially_filled" {
        return Err(format!(
            "paper_order cannot be filled from status {}",
            order.4
        ));
    }

    let fill_quantity = req.quantity.unwrap_or(order.3);
    if fill_quantity <= Decimal::ZERO || fill_quantity > order.3 {
        return Err("fill quantity must be positive and no greater than order quantity".into());
    }
    let amount = fill_quantity * req.price;
    let total_cost = amount + req.commission + req.tax + req.slippage;
    let cash_delta = if order.2 == "buy" {
        -total_cost
    } else {
        amount - req.commission - req.tax - req.slippage
    };
    let fill_id = format!("pf-{}", Uuid::new_v4());
    let fill_time: DateTime<Utc> = Utc::now();

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("Failed to begin paper fill transaction: {}", error))?;
    sqlx::query(
        "INSERT INTO paper_fill
           (fill_id, order_id, paper_account_id, symbol, fill_time, side,
            quantity, price, amount, commission, tax, slippage)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(&fill_id)
    .bind(order_id)
    .bind(&order.0)
    .bind(&order.1)
    .bind(fill_time)
    .bind(&order.2)
    .bind(fill_quantity)
    .bind(req.price)
    .bind(amount)
    .bind(req.commission)
    .bind(req.tax)
    .bind(req.slippage)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("Failed to insert paper_fill: {}", error))?;
    sqlx::query("UPDATE paper_account SET cash = cash + $2 WHERE paper_account_id = $1")
        .bind(&order.0)
        .bind(cash_delta)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to update paper_account cash: {}", error))?;
    sqlx::query("UPDATE paper_order SET status = 'filled' WHERE order_id = $1")
        .bind(order_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("Failed to update paper_order status: {}", error))?;
    tx.commit()
        .await
        .map_err(|error| format!("Failed to commit paper fill transaction: {}", error))?;

    write_audit_event(
        db,
        "paper_order.fill",
        "paper_order",
        order_id,
        req.operator.as_deref(),
        "Filled paper order",
        json!({
            "fill_id": fill_id,
            "paper_account_id": order.0,
            "symbol": order.1,
            "side": order.2,
            "quantity": fill_quantity,
            "price": req.price,
            "amount": amount,
            "cash_delta": cash_delta,
            "trace_id": req.trace_id
        }),
    )
    .await?;

    Ok(json!({
        "fill_id": fill_id,
        "order_id": order_id,
        "status": "filled",
        "quantity": fill_quantity,
        "price": req.price,
        "amount": amount,
        "cash_delta": cash_delta,
        "trace_id": req.trace_id
    }))
}

async fn paper_account_summary_inner(db: &sqlx::PgPool, account_id: &str) -> Result<Value, String> {
    let account = sqlx::query_as::<_, (String, String, Decimal, Decimal, String)>(
        "SELECT paper_account_id, name, initial_capital, cash, status
         FROM paper_account
         WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|error| format!("Failed to load paper_account: {}", error))?
    .ok_or_else(|| "paper_account not found".to_string())?;
    let counts = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE status = 'filled')::bigint,
                COUNT(*) FILTER (WHERE status = 'rejected')::bigint
         FROM paper_order
         WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize paper_order: {}", error))?;

    Ok(json!({
        "paper_account_id": account.0,
        "name": account.1,
        "initial_capital": account.2,
        "cash": account.3,
        "status": account.4,
        "nav": account.3,
        "order_count": counts.0,
        "filled_order_count": counts.1,
        "rejected_order_count": counts.2
    }))
}

async fn paper_metrics_inner(db: &sqlx::PgPool) -> Result<Value, String> {
    let order_counts = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        "SELECT COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE status = 'submitted')::bigint,
                COUNT(*) FILTER (WHERE status = 'filled')::bigint,
                COUNT(*) FILTER (WHERE status = 'rejected')::bigint
         FROM paper_order",
    )
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize paper_order metrics: {}", error))?;
    let fill_counts = sqlx::query_as::<_, (i64, Option<Decimal>)>(
        "SELECT COUNT(*)::bigint, SUM(amount) FROM paper_fill",
    )
    .fetch_one(db)
    .await
    .map_err(|error| format!("Failed to summarize paper_fill metrics: {}", error))?;

    Ok(json!({
        "order_count": order_counts.0,
        "submitted_order_count": order_counts.1,
        "filled_order_count": order_counts.2,
        "rejected_order_count": order_counts.3,
        "fill_count": fill_counts.0,
        "filled_amount": fill_counts.1.unwrap_or_else(Decimal::zero)
    }))
}

struct RiskResult {
    passed: bool,
    reason: Option<String>,
}

fn evaluate_order_risk(
    req: &NormalizedOrderRequest,
    account_status: &str,
    cash: Decimal,
) -> RiskResult {
    if account_status != "active" {
        return RiskResult {
            passed: false,
            reason: Some("account_not_active".into()),
        };
    }
    if req.side != "buy" && req.side != "sell" {
        return RiskResult {
            passed: false,
            reason: Some("unsupported_side".into()),
        };
    }
    if req.quantity <= Decimal::ZERO {
        return RiskResult {
            passed: false,
            reason: Some("non_positive_quantity".into()),
        };
    }
    if req.side == "buy" {
        let price = req.estimated_price.or(req.limit_price);
        let Some(price) = price else {
            return RiskResult {
                passed: false,
                reason: Some("buy_order_requires_estimated_or_limit_price".into()),
            };
        };
        if req.quantity * price > cash {
            return RiskResult {
                passed: false,
                reason: Some("insufficient_cash".into()),
            };
        }
    }
    RiskResult {
        passed: true,
        reason: None,
    }
}

fn normalize_account_request(
    req: CreatePaperAccountRequest,
) -> Result<NormalizedAccountRequest, String> {
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Err("name must not be empty".into());
    }
    let initial_capital = decimal_from_f64(req.initial_capital, "initial_capital")?;
    if initial_capital <= Decimal::ZERO {
        return Err("initial_capital must be positive".into());
    }
    Ok(NormalizedAccountRequest {
        name,
        initial_capital,
        base_currency: req
            .base_currency
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("CNY")
            .to_string(),
        operator: normalize_optional_string(req.operator),
    })
}

fn normalize_order_request(req: SubmitPaperOrderRequest) -> Result<NormalizedOrderRequest, String> {
    let paper_account_id = required_trimmed(req.paper_account_id, "paper_account_id")?;
    let symbol = required_trimmed(req.symbol, "symbol")?;
    let side = required_trimmed(req.side, "side")?.to_lowercase();
    let quantity = decimal_from_f64(req.quantity, "quantity")?;
    let limit_price = optional_decimal(req.limit_price, "limit_price")?;
    let estimated_price = optional_decimal(req.estimated_price, "estimated_price")?;
    Ok(NormalizedOrderRequest {
        paper_account_id,
        strategy_version_id: normalize_optional_string(req.strategy_version_id),
        symbol,
        side,
        order_type: req
            .order_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("market")
            .to_string(),
        quantity,
        limit_price,
        estimated_price,
        operator: normalize_optional_string(req.operator),
        trace_id: normalize_optional_string(req.trace_id)
            .unwrap_or_else(|| format!("trace-{}", Uuid::new_v4())),
    })
}

fn normalize_fill_request(req: FillPaperOrderRequest) -> Result<NormalizedFillRequest, String> {
    let price = decimal_from_f64(req.price, "price")?;
    if price <= Decimal::ZERO {
        return Err("price must be positive".into());
    }
    Ok(NormalizedFillRequest {
        price,
        quantity: optional_decimal(req.quantity, "quantity")?,
        commission: optional_decimal(req.commission, "commission")?.unwrap_or_else(Decimal::zero),
        tax: optional_decimal(req.tax, "tax")?.unwrap_or_else(Decimal::zero),
        slippage: optional_decimal(req.slippage, "slippage")?.unwrap_or_else(Decimal::zero),
        operator: normalize_optional_string(req.operator),
        trace_id: normalize_optional_string(req.trace_id)
            .unwrap_or_else(|| format!("trace-{}", Uuid::new_v4())),
    })
}

fn required_trimmed(value: String, field: &str) -> Result<String, String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        Err(format!("{} must not be empty", field))
    } else {
        Ok(value)
    }
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_decimal(value: Option<f64>, field: &str) -> Result<Option<Decimal>, String> {
    value
        .map(|value| decimal_from_f64(value, field))
        .transpose()
}

fn decimal_from_f64(value: f64, field: &str) -> Result<Decimal, String> {
    Decimal::from_f64(value).ok_or_else(|| format!("{} must be a finite number", field))
}

async fn write_audit_event(
    db: &sqlx::PgPool,
    event_type: &str,
    entity_type: &str,
    entity_id: &str,
    actor: Option<&str>,
    summary: &str,
    details: Value,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO audit_event
           (audit_event_id, event_type, entity_type, entity_id, actor, summary, details)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(format!("audit-{}", Uuid::new_v4()))
    .bind(event_type)
    .bind(entity_type)
    .bind(entity_id)
    .bind(actor)
    .bind(summary)
    .bind(details)
    .execute(db)
    .await
    .map_err(|error| format!("Failed to insert audit_event: {}", error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_order_risk_rejects_insufficient_cash() {
        let req = normalize_order_request(SubmitPaperOrderRequest {
            paper_account_id: "pa-1".into(),
            strategy_version_id: None,
            symbol: "000001.SZ".into(),
            side: "buy".into(),
            order_type: None,
            quantity: 200.0,
            limit_price: Some(10.0),
            estimated_price: None,
            operator: None,
            trace_id: None,
        })
        .expect("order request");

        let risk = evaluate_order_risk(&req, "active", Decimal::from_i32(1000).unwrap());

        assert!(!risk.passed);
        assert_eq!(risk.reason.as_deref(), Some("insufficient_cash"));
    }

    #[test]
    fn buy_order_risk_passes_when_cash_covers_estimate() {
        let req = normalize_order_request(SubmitPaperOrderRequest {
            paper_account_id: "pa-1".into(),
            strategy_version_id: Some("factor-combo-v1".into()),
            symbol: "000001.SZ".into(),
            side: "buy".into(),
            order_type: Some("limit".into()),
            quantity: 100.0,
            limit_price: Some(10.0),
            estimated_price: None,
            operator: Some("tester".into()),
            trace_id: Some("trace-1".into()),
        })
        .expect("order request");

        let risk = evaluate_order_risk(&req, "active", Decimal::from_i32(1001).unwrap());

        assert!(risk.passed);
        assert_eq!(req.trace_id, "trace-1");
        assert_eq!(req.order_type, "limit");
    }

    #[test]
    fn fill_request_defaults_costs_and_trace() {
        let req = normalize_fill_request(FillPaperOrderRequest {
            price: 10.0,
            quantity: None,
            commission: None,
            tax: None,
            slippage: None,
            operator: None,
            trace_id: None,
        })
        .expect("fill request");

        assert_eq!(req.price, Decimal::from_i32(10).unwrap());
        assert_eq!(req.commission, Decimal::ZERO);
        assert!(req.trace_id.starts_with("trace-"));
    }
}
