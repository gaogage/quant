//! 投资账号管理 API — 用户可创建/管理自己的模拟或实盘账号。
//!
//! GET    /api/v1/accounts        — 自己的账号列表
//! POST   /api/v1/accounts        — 创建新账号
//! PUT    /api/v1/accounts/{id}   — 修改账号配置
//! DELETE /api/v1/accounts/{id}   — 删除账号

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::UserContext;
use crate::AppState;

/// 账号列表行（含 strategy_config 杠杆上限 + paper_replay 绩效，LEFT JOIN）。
/// 用 FromRow struct 突破 sqlx 16 元组列限制。
#[derive(sqlx::FromRow)]
struct AccountListRow {
    paper_account_id: String,
    account_type: String,
    name: String,
    initial_capital: f64,
    leverage_enabled: bool,
    leverage_mode: String,
    leverage_multiplier: f64,
    status: String,
    user_id: Option<String>,
    current_nav: Option<f64>,
    cash: f64,
    max_drawdown_pct: Option<f64>,
    margin_amount: f64,
    reserve_amount: f64,
    strategy_version_id: Option<String>,
    leverage_cap: Option<f64>,
    annual_return_pct: Option<f64>,
    cumulative_return_pct: Option<f64>,
    sharpe_ratio: Option<f64>,
    sortino_ratio: Option<f64>,
    calmar_ratio: Option<f64>,
}

// ── 请求体 ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateAccountRequest {
    pub strategy_id: String,
    pub account_type: Option<String>,
    pub name: String,
    pub initial_capital: Option<f64>,
    pub leverage_enabled: Option<bool>,
    pub leverage_mode: Option<String>,
    pub leverage_multiplier: Option<f64>,
    pub signal_source: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAccountRequest {
    pub name: Option<String>,
    pub initial_capital: Option<f64>,
    pub leverage_enabled: Option<bool>,
    pub leverage_mode: Option<String>,
    pub leverage_multiplier: Option<f64>,
    pub signal_source: Option<String>,
    pub status: Option<String>,
    pub dingtalk_webhook_url: Option<String>,
    pub margin_amount: Option<f64>,
    pub cash: Option<f64>,
    pub reserve_amount: Option<f64>,
    pub strategy_version_id: Option<String>,
}

// ── 列表 ────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
pub struct AccountFilter {
    pub name: Option<String>,          // 账号名模糊搜索
    pub leverage: Option<String>,      // "enabled" | "disabled" | absent(all)
    pub signal_source: Option<String>, // "factor" | "prediction" | "prediction_blend" | absent(all)
    pub lev_mult_min: Option<f64>,     // 杠杆倍率下限（含）
    pub lev_mult_max: Option<f64>,     // 杠杆倍率上限（含）
    pub status: Option<String>,        // "active" | "inactive" | absent(all)
}

/// GET /api/v1/accounts — 账号列表（自己的，admin 可看全部），支持条件过滤
pub async fn list_accounts(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    axum::extract::Query(filter): axum::extract::Query<AccountFilter>,
) -> impl IntoResponse {
    let is_admin = user.role == "admin";
    let mut where_clauses: Vec<String> = Vec::new();

    // 权限过滤
    if !is_admin {
        where_clauses.push(format!("(pa.user_id = '{}' OR (pa.user_id IS NULL AND pa.status = 'active'))", user.user_id));
    }

    // 名称模糊搜索
    if let Some(ref name) = filter.name {
        if !name.is_empty() {
            where_clauses.push(format!("pa.name ILIKE '%{}%'", name.replace('\'', "''")));
        }
    }

    // 杠杆开关
    if let Some(ref lev) = filter.leverage {
        match lev.as_str() {
            "enabled" => where_clauses.push("pa.leverage_enabled = true".into()),
            "disabled" => where_clauses.push("pa.leverage_enabled = false".into()),
            _ => {}
        }
    }

    // 信号源
    if let Some(ref ss) = filter.signal_source {
        if !ss.is_empty() && ss != "all" {
            where_clauses.push(format!("pa.signal_source = '{}'", ss.replace('\'', "''")));
        }
    }

    // 杠杆倍率范围
    if let Some(min) = filter.lev_mult_min {
        where_clauses.push(format!("pa.leverage_multiplier >= {}", min));
    }
    if let Some(max) = filter.lev_mult_max {
        where_clauses.push(format!("pa.leverage_multiplier <= {}", max));
    }

    // 状态
    if let Some(ref st) = filter.status {
        if !st.is_empty() && st != "all" {
            where_clauses.push(format!("pa.status = '{}'", st.replace('\'', "''")));
        }
    }

    let where_sql = if where_clauses.is_empty() {
        String::from("TRUE")
    } else {
        where_clauses.join(" AND ")
    };

    let sql = format!(
        "SELECT pa.paper_account_id, pa.account_type, pa.name, pa.initial_capital::double precision AS initial_capital,
                pa.leverage_enabled, pa.leverage_mode, pa.leverage_multiplier, pa.status,
                pa.user_id, pa.current_nav::double precision AS current_nav,
                COALESCE(pa.cash, pa.initial_capital)::double precision AS cash,
                pa.max_drawdown_pct::double precision AS max_drawdown_pct,
                COALESCE(pa.margin_amount,0)::double precision AS margin_amount,
                COALESCE(pa.reserve_amount,0)::double precision AS reserve_amount,
                pa.strategy_version_id,
                sc.leverage_cap::double precision AS leverage_cap,
                rp.annual_return_pct::double precision AS annual_return_pct,
                rp.cumulative_return_pct::double precision AS cumulative_return_pct,
                rp.sharpe_ratio::double precision AS sharpe_ratio,
                rp.sortino_ratio::double precision AS sortino_ratio,
                rp.calmar_ratio::double precision AS calmar_ratio
         FROM paper_account pa
         LEFT JOIN strategy_config sc ON sc.strategy_id = pa.strategy_version_id AND sc.status = 'active'
         LEFT JOIN LATERAL (
             SELECT annual_return_pct, cumulative_return_pct, sharpe_ratio, sortino_ratio, calmar_ratio
             FROM paper_replay WHERE paper_account_id = pa.paper_account_id LIMIT 1
         ) rp ON true
         WHERE {}
         ORDER BY pa.status ASC, pa.created_at DESC",
        where_sql
    );

    let rows: Vec<AccountListRow> = sqlx::query_as(&sql).fetch_all(&state.db).await.unwrap_or_default();

    let list: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "account_id": r.paper_account_id, "account_type": r.account_type,
                "name": r.name, "initial_capital": r.initial_capital,
                "leverage_enabled": r.leverage_enabled, "leverage_mode": r.leverage_mode,
                "leverage_multiplier": r.leverage_multiplier, "status": r.status,
                "owner": r.user_id.unwrap_or_default(),
                "current_nav": r.current_nav, "cash": r.cash, "max_drawdown": r.max_drawdown_pct,
                "leverage_cap": r.leverage_cap,
                "margin_amount": r.margin_amount, "reserve_amount": r.reserve_amount,
                "strategy_version_id": r.strategy_version_id.unwrap_or_default(),
                "annual_return_pct": r.annual_return_pct,
                "cumulative_return_pct": r.cumulative_return_pct,
                "sharpe_ratio": r.sharpe_ratio,
                "sortino_ratio": r.sortino_ratio,
                "calmar_ratio": r.calmar_ratio,
            })
        })
        .collect();

    Json(serde_json::json!({"code": 0, "data": list}))
}

// ── 详情 ────────────────────────────────────────────────

/// GET /api/v1/accounts/{id} — 账号详情（绩效+持仓+交易记录）
pub async fn account_detail(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
) -> impl IntoResponse {
    // 权限校验
    let is_admin = user.role == "admin";
    if !is_admin {
        let owner = sqlx::query_as::<_, (Option<String>,)>(
            "SELECT user_id FROM paper_account WHERE paper_account_id = $1"
        ).bind(&account_id).fetch_optional(&state.db).await;
        match owner {
            Ok(Some((Some(oid),))) if oid == user.user_id => {}
            Ok(Some((None,))) => {}
            _ => return Json(serde_json::json!({"code": 403, "message": "无权访问"})).into_response(),
        }
    }

    // 基本信息
    let acc: Option<(
        String, String, String, f64, f64, f64, f64, bool, String, f64, String, String, Option<String>, Option<f64>, Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT paper_account_id, account_type, name, initial_capital::double precision,
                COALESCE(current_nav, initial_capital)::double precision as nav,
                COALESCE(cash, initial_capital)::double precision as cash,
                COALESCE(margin_amount, 0)::double precision as margin_amount,
                leverage_enabled, leverage_mode, leverage_multiplier, signal_source, status,
                user_id, max_drawdown_pct::double precision, created_at
         FROM paper_account WHERE paper_account_id = $1"
    ).bind(&account_id).fetch_optional(&state.db).await.ok().flatten();

    let Some((aid, at, name, cap, nav, cash, margin, le, lm, lmp, ss, st, uid, mdd, created)) = acc else {
        return Json(serde_json::json!({"code": 404, "message": "账号不存在"})).into_response();
    };

    // 绩效指标：从 paper_nav_snapshot 计算
    let metrics = sqlx::query_as::<_, (Option<f64>,)>(
        "SELECT (nav / LAG(nav) OVER (ORDER BY snapshot_date) - 1.0)::double precision as daily_ret
         FROM paper_nav_snapshot WHERE paper_account_id = $1 ORDER BY snapshot_date"
    ).bind(&account_id).fetch_all(&state.db).await.unwrap_or_default();

    let returns: Vec<f64> = metrics.iter().filter_map(|(r,)| *r).collect();
    let n = returns.len() as f64;
    let annual_return = if n > 20.0 {
        let mean_daily = returns.iter().sum::<f64>() / n;
        let ann = (1.0 + mean_daily).powi(252) - 1.0;
        (ann * 10000.0).round() / 100.0
    } else { 0.0 };
    let sharpe = if n > 1.0 {
        let mean = returns.iter().sum::<f64>() / n;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let daily_vol = var.sqrt();
        let annual_vol = daily_vol * (252.0_f64).sqrt();
        if annual_vol > 0.0 { (annual_return / 100.0) / annual_vol } else { 0.0 }
    } else { 0.0 };
    let sortino = if n > 1.0 {
        let down_var = returns.iter().filter(|&&r| r < 0.0).map(|r| r.powi(2)).sum::<f64>() / n;
        let down_vol = down_var.sqrt() * (252.0_f64).sqrt();
        if down_vol > 0.0 { (annual_return / 100.0) / down_vol } else { 0.0 }
    } else { 0.0 };
    let cum_ret = if cap > 0.0 { (nav - cap) / cap * 100.0 } else { 0.0 };
    let calmar = if let Some(m) = mdd { if m > 0.01 { annual_return / m } else { 0.0 } } else { 0.0 };

    // 如果快照无有效收益数据（回放产生的是聚合指标），从 paper_replay 读取
    let mut yearly_returns_json: Option<serde_json::Value> = None;
    let mut benchmarks_json: Option<serde_json::Value> = None;
    let (annual_return, sharpe, sortino, cum_ret, calmar, mdd, replay_days) =
        if annual_return == 0.0 && n > 1.0 {
            // 检查是否有回放记录
            let replay: Option<(
                Option<f64>, Option<f64>, Option<f64>, Option<f64>,
                Option<f64>, Option<f64>, Option<f64>, Option<i32>,
                Option<serde_json::Value>, Option<serde_json::Value>,
            )> = sqlx::query_as(
                "SELECT annual_return_pct, cumulative_return_pct, sharpe_ratio, sortino_ratio,
                 calmar_ratio, max_drawdown_pct, volatility_pct, trading_days,
                 yearly_returns, benchmarks
                 FROM paper_replay WHERE paper_account_id = $1 LIMIT 1"
            ).bind(&account_id).fetch_optional(&state.db).await.ok().flatten();
            if let Some((ar, cr, sh, so, ca, md, _vol, td, yr, bm)) = replay {
                yearly_returns_json = yr;
                benchmarks_json = bm;
                (ar.unwrap_or(0.0), sh.unwrap_or(0.0), so.unwrap_or(0.0),
                 cr.unwrap_or(0.0), ca.unwrap_or(0.0), md, td.unwrap_or(0) as i64)
            } else {
                (annual_return, sharpe, sortino, cum_ret, calmar, mdd, n as i64)
            }
        } else {
            (annual_return, sharpe, sortino, cum_ret, calmar, mdd, n as i64)
        };

    // 当前持仓
    let positions: Vec<serde_json::Value> = sqlx::query_as::<_, (String, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, quantity, avg_cost, market_price, market_value FROM paper_position
         WHERE paper_account_id = $1 AND quantity > 0 ORDER BY market_value DESC NULLS LAST LIMIT 100"
    ).bind(&account_id).fetch_all(&state.db).await.unwrap_or_default()
    .into_iter().map(|(sym, qty, cost, price, mv)| {
        serde_json::json!({
            "symbol": sym,
            "quantity": qty.map(|v| v.to_string()).unwrap_or_default(),
            "avg_cost": cost.map(|v| v.to_string()).unwrap_or_default(),
            "market_price": price.map(|v| v.to_string()).unwrap_or_default(),
            "market_value": mv.map(|v| v.to_string()).unwrap_or_default(),
        })
    }).collect();

    // 最近交易记录（实际交易 + 计划交易）
    let trades: Vec<serde_json::Value> = sqlx::query_as::<_, (String, String, String, String, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<chrono::DateTime<chrono::Utc>>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>)>(
        "SELECT o.order_id, o.symbol, o.side, o.status, o.quantity, o.limit_price,
                f.price as fill_price, f.fill_time,
                o.target_price, o.price_upper_limit, o.price_lower_limit,
                o.slippage_pct, f.quantity as fill_quantity, f.amount as fill_amount
         FROM paper_order o LEFT JOIN paper_fill f ON o.order_id = f.planned_order_id
         WHERE o.paper_account_id = $1
         ORDER BY COALESCE(f.fill_time, o.created_at) DESC LIMIT 50"
    ).bind(&account_id).fetch_all(&state.db).await.unwrap_or_default()
    .into_iter().map(|(oid, sym, side, sts, qty, lim, fill_price, fill_time, target_price, upper, lower, slip, fill_qty, fill_amt)| {
        serde_json::json!({
            "order_id": oid, "symbol": sym, "side": side, "status": sts,
            "quantity": qty.map(|v| v.to_string()).unwrap_or_default(),
            "limit_price": lim.map(|v| v.to_string()).unwrap_or_default(),
            "fill_price": fill_price.map(|v| v.to_string()).unwrap_or_default(),
            "fill_time": fill_time.map(|t| t.to_string()).unwrap_or_default(),
            "planned": {
                "target_price": target_price.map(|v| v.to_string()),
                "price_upper": upper.map(|v| v.to_string()),
                "price_lower": lower.map(|v| v.to_string()),
                "slippage_pct": slip.map(|v| v.to_string()),
            },
            "fill_quantity": fill_qty.map(|v| v.to_string()),
            "fill_amount": fill_amt.map(|v| v.to_string()),
        })
    }).collect();

    Json(serde_json::json!({
        "code": 0, "data": {
            "account_id": aid, "account_type": at, "name": name,
            "initial_capital": cap, "current_nav": nav,
            "cash": cash, "margin_amount": margin,
            "leverage_enabled": le, "leverage_mode": lm,
            "leverage_multiplier": lmp, "signal_source": ss,
            "status": st, "owner": uid.unwrap_or_default(),
            "created_at": created.map(|t| t.to_string()),
            "metrics": {
                "annual_return_pct": annual_return,
                "cumulative_return_pct": (cum_ret * 100.0).round() / 100.0,
                "sharpe_ratio": (sharpe * 100.0).round() / 100.0,
                "sortino_ratio": (sortino * 100.0).round() / 100.0,
                "max_drawdown_pct": mdd.unwrap_or(0.0),
                "calmar_ratio": (calmar * 100.0).round() / 100.0,
                "nav_history_days": replay_days,
            },
            "yearly_returns": yearly_returns_json,
            "benchmarks": benchmarks_json,
            "asset_allocation": asset_allocation(&positions),
            "positions": positions,
            "trades": trades,
        }
    })).into_response()
}

// ── 创建 ────────────────────────────────────────────────

/// POST /api/v1/accounts — 创建新账号
pub async fn create_account(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Json(req): Json<CreateAccountRequest>,
) -> impl IntoResponse {
    let aid = format!("pa-{}", Uuid::new_v4().simple()); // paper_account 前缀
    let at = req.account_type.as_deref().unwrap_or("simulated");
    let cap = req.initial_capital.unwrap_or(1_000_000.0);
    let le = req.leverage_enabled.unwrap_or(false);
    let lm = req.leverage_mode.as_deref().unwrap_or("fixed");
    let lmp = req.leverage_multiplier.unwrap_or(1.0);
    let ss = req.signal_source.as_deref().unwrap_or("factor");

    match sqlx::query(
        "INSERT INTO paper_account (paper_account_id, name, account_type, initial_capital, cash,
         leverage_enabled, leverage_mode, leverage_multiplier, signal_source, status, user_id)
         VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, 'active', $9)"
    )
    .bind(&aid).bind(&req.name).bind(at).bind(cap)
    .bind(le).bind(lm).bind(lmp).bind(ss).bind(&user.user_id)
    .execute(&state.db).await
    {
        Ok(_) => Json(serde_json::json!({"code": 0, "data": {"account_id": aid}})),
        Err(e) => Json(serde_json::json!({"code": 1, "message": format!("创建失败: {}", e)})),
    }
}

// ── 修改 ────────────────────────────────────────────────

/// PUT /api/v1/accounts/{id} — 修改自己的账号
pub async fn update_account(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
    Json(req): Json<UpdateAccountRequest>,
) -> impl IntoResponse {
    // 校验所有权（admin 可改任意账号）
    let is_admin = user.role == "admin";
    if !is_admin {
        let owner = sqlx::query_as::<_, (Option<String>,)>(
            "SELECT user_id FROM paper_account WHERE paper_account_id = $1"
        ).bind(&account_id).fetch_optional(&state.db).await;
        match owner {
            Ok(Some((Some(oid),))) if oid == user.user_id => {}
            Ok(Some((None,))) => {} // 无主账号允许修改
            _ => return Json(serde_json::json!({"code": 403, "message": "只能修改自己的账号"})),
        }
    }

    let _ = sqlx::query(
        "UPDATE paper_account SET
         name = COALESCE($1, name),
         leverage_enabled = COALESCE($2, leverage_enabled),
         leverage_mode = COALESCE($3, leverage_mode),
         leverage_multiplier = COALESCE($4, leverage_multiplier),
         signal_source = COALESCE($5, signal_source),
         status = COALESCE($6, status),
         dingtalk_webhook_url = COALESCE($7, dingtalk_webhook_url),
         margin_amount = COALESCE($8, margin_amount),
         cash = COALESCE($9, cash),
         reserve_amount = COALESCE($10, reserve_amount),
         strategy_version_id = COALESCE($12, strategy_version_id),
         updated_at = NOW()
         WHERE paper_account_id = $11"
    )
    .bind(&req.name).bind(req.leverage_enabled)
    .bind(&req.leverage_mode).bind(req.leverage_multiplier)
    .bind(&req.signal_source).bind(&req.status)
    .bind(&req.dingtalk_webhook_url).bind(req.margin_amount)
    .bind(req.cash).bind(req.reserve_amount)
    .bind(&account_id)
    .bind(&req.strategy_version_id)
    .execute(&state.db).await;

    Json(serde_json::json!({"code": 0}))
}

// ── 删除 ────────────────────────────────────────────────

/// DELETE /api/v1/accounts/{id} — 删除自己的账号
pub async fn delete_account(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
) -> impl IntoResponse {
    let is_admin = user.role == "admin";
    if !is_admin {
        let owner = sqlx::query_as::<_, (Option<String>,)>(
            "SELECT user_id FROM paper_account WHERE paper_account_id = $1"
        ).bind(&account_id).fetch_optional(&state.db).await;
        match owner {
            Ok(Some((Some(oid),))) if oid == user.user_id => {}
            Ok(Some((None,))) => {}
            _ => return Json(serde_json::json!({"code": 403, "message": "只能删除自己的账号"})),
        }
    }

    // 软删除：改为 inactive
    let _ = sqlx::query("UPDATE paper_account SET status = 'inactive', updated_at = NOW() WHERE paper_account_id = $1")
        .bind(&account_id)
        .execute(&state.db).await;

    Json(serde_json::json!({"code": 0, "message": "已停用"}))
}

// ── 重置 ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ResetAccountRequest {
    pub initial_capital: f64,
    /// 账号起始日期，格式 YYYY-MM-DD
    pub start_date: String,
}

/// POST /api/v1/accounts/{id}/reset — 重置账号：清空交易记录并重新初始化
pub async fn reset_account(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
    Json(req): Json<ResetAccountRequest>,
) -> impl IntoResponse {
    let is_admin = user.role == "admin";
    if !is_admin {
        let owner = sqlx::query_as::<_, (Option<String>,)>(
            "SELECT user_id FROM paper_account WHERE paper_account_id = $1"
        ).bind(&account_id).fetch_optional(&state.db).await;
        match owner {
            Ok(Some((Some(oid),))) if oid == user.user_id => {}
            Ok(Some((None,))) => {}
            _ => return Json(serde_json::json!({"code": 403, "message": "只能重置自己的账号"})).into_response(),
        }
    }

    let start_date = match chrono::NaiveDate::parse_from_str(&req.start_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => return Json(serde_json::json!({"code": 1, "message": "日期格式错误，需要 YYYY-MM-DD"})).into_response(),
    };

    // 清空关联数据
    for table in &["paper_order", "paper_fill", "paper_position", "paper_nav_snapshot", "paper_replay", "paper_margin_trade"] {
        let sql = format!("DELETE FROM {} WHERE paper_account_id = $1", table);
        let _ = sqlx::query(&sql).bind(&account_id).execute(&state.db).await;
    }

    // 重置账号状态
    let cap = req.initial_capital;
    let _ = sqlx::query(
        "UPDATE paper_account SET
         initial_capital = $1, cash = $1, current_nav = $1, peak_nav = $1,
         margin_amount = 0, max_drawdown_pct = 0, total_trades = 0,
         created_at = $2, updated_at = NOW()
         WHERE paper_account_id = $3"
    )
    .bind(cap).bind(start_date).bind(&account_id)
    .execute(&state.db).await;

    Json(serde_json::json!({"code": 0, "message": format!("账号已重置，起始资金¥{}，起始日期{}", cap as i64, req.start_date)})).into_response()
}


/// 根据持仓列表计算资产大类占比
fn asset_allocation(positions: &[serde_json::Value]) -> Vec<serde_json::Value> {
    use std::collections::HashMap;
    let mut categories: HashMap<String, f64> = HashMap::new();
    for p in positions {
        let sym = p.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
        let mv_str = p.get("market_value").and_then(|v| v.as_str()).unwrap_or("0");
        let mv: f64 = mv_str.parse().unwrap_or(0.0);
        let cat = classify_asset(sym);
        *categories.entry(cat).or_default() += mv;
    }
    let total: f64 = categories.values().sum();
    let mut result: Vec<serde_json::Value> = categories.into_iter()
        .filter(|(_, v)| *v > 0.0)
        .map(|(name, value)| serde_json::json!({
            "name": name, "market_value": (value * 100.0).round() / 100.0,
            "pct": if total > 0.0 { (value / total * 10000.0).round() / 100.0 } else { 0.0 }
        }))
        .collect();
    result.sort_by(|a, b| b["pct"].as_f64().unwrap_or(0.0).partial_cmp(&a["pct"].as_f64().unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal));
    result
}

fn classify_asset(symbol: &str) -> String {
    match symbol {
        "511010.SH" | "511260.SH" => "国债ETF".into(),
        "518880.SH" => "黄金ETF".into(),
        "513100.SH" => "纳指ETF".into(),
        "513500.SH" => "标普ETF".into(),
        "501018.SH" => "原油LOF".into(),
        "159980.SZ" => "商品ETF".into(),
        "159985.SZ" => "商品ETF".into(),
        s if s.ends_with(".SH") || s.ends_with(".SZ") => "A股".into(),
        _ => "其他".into(),
    }
}

// ── 钉钉推送（单账号） ──────────────────────────────────

/// POST /api/v1/accounts/{id}/push-dingtalk — 推送当日持仓摘要到钉钉
pub async fn push_account_dingtalk(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
) -> impl IntoResponse {
    use super::dingtalk;

    // 权限校验
    let is_admin = user.role == "admin";
    if !is_admin {
        let owner = sqlx::query_as::<_, (Option<String>,)>(
            "SELECT user_id FROM paper_account WHERE paper_account_id = $1"
        ).bind(&account_id).fetch_optional(&state.db).await;
        match owner {
            Ok(Some((Some(oid),))) if oid == user.user_id => {}
            Ok(Some((None,))) => {}
            _ => return Json(serde_json::json!({"code": 403, "message": "无权操作"})).into_response(),
        }
    }

    // 查询账号信息
    let acc: Option<(String, String, Option<String>, f64, f64, f64)> = sqlx::query_as(
        "SELECT name, account_type, dingtalk_webhook_url,
                COALESCE(current_nav, initial_capital)::double precision,
                COALESCE(cash, initial_capital)::double precision,
                COALESCE(margin_amount, 0)::double precision
         FROM paper_account WHERE paper_account_id = $1"
    ).bind(&account_id).fetch_optional(&state.db).await.ok().flatten();

    let (name, acc_type, webhook_url, nav, cash, margin) = match acc {
        Some(a) => a,
        None => return Json(serde_json::json!({"code": 404, "message": "账号不存在"})).into_response(),
    };

    // 解析 webhook URL：优先账号级，fallback 环境变量
    let webhook = match webhook_url.filter(|u| !u.is_empty()) {
        Some(u) => u,
        None => match dingtalk::build_dingtalk_webhook_url() {
            Some(u) => u,
            None => return Json(serde_json::json!({
                "code": 1, "message": "未配置钉钉 Webhook：请在账号编辑中配置 dingtalk_webhook_url 或设置 DINGTALK_CLIENT_ID 环境变量"
            })).into_response(),
        },
    };

    // 最新快照日期
    let snap: Option<(chrono::NaiveDate,)> = sqlx::query_as(
        "SELECT snapshot_date FROM paper_nav_snapshot WHERE paper_account_id = $1 ORDER BY snapshot_date DESC LIMIT 1"
    ).bind(&account_id).fetch_optional(&state.db).await.ok().flatten();

    let trade_date = snap.map(|(d,)| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| chrono::Local::now().format("%Y-%m-%d").to_string());

    // 累计收益 & 最大回撤
    let perf: Option<(Option<f64>,)> = sqlx::query_as(
        "SELECT max_drawdown_pct::double precision FROM paper_account WHERE paper_account_id = $1"
    ).bind(&account_id).fetch_optional(&state.db).await.ok().flatten();

    let mdd = perf.and_then(|(m,)| m).unwrap_or(0.0) / 100.0; // pct → decimal
    let cum_ret = if nav > 0.0 {
        // 从 paper_account 获取 initial_capital 计算
        let cap: Option<(f64,)> = sqlx::query_as(
            "SELECT initial_capital::double precision FROM paper_account WHERE paper_account_id = $1"
        ).bind(&account_id).fetch_optional(&state.db).await.ok().flatten();
        let init = cap.map(|(c,)| c).unwrap_or(nav);
        if init > 0.0 { nav / init - 1.0 } else { 0.0 }
    } else { 0.0 };

    // 当前持仓
    let positions: Vec<serde_json::Value> = sqlx::query_as::<_, (String, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>)>(
        "SELECT symbol, quantity, avg_cost, market_price
         FROM paper_position WHERE paper_account_id = $1 AND ABS(quantity) > 0"
    ).bind(&account_id).fetch_all(&state.db).await.unwrap_or_default()
        .into_iter()
        .map(|(sym, qty, _cost, price)| {
            let q: f64 = qty.as_ref().and_then(|v| v.to_string().parse().ok()).unwrap_or(0.0);
            let p: f64 = price.as_ref().and_then(|v| v.to_string().parse().ok()).unwrap_or(0.0);
            let mv = (q * p).to_string();
            serde_json::json!({
                "symbol": sym,
                "name": "",
                "quantity": qty.map(|v| v.to_string()).unwrap_or_default(),
                "current_price": price.map(|v| v.to_string()).unwrap_or_default(),
                "market_value": mv,
            })
        })
        .collect();

    // 资产大类分布
    let class_breakdown = asset_allocation(&positions);

    let mv: f64 = positions.iter()
        .filter_map(|p| p.get("market_value").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()))
        .sum();
    let net_worth = mv + cash - margin;
    let total_assets = mv + cash; // 总资产=持仓市值+现金(含融资买入部分)；净资产=总资产-融资额

    let text = dingtalk::build_position_summary_notification(
        &name, &acc_type, &trade_date,
        total_assets, cash, margin, mv, net_worth, &positions, cum_ret, mdd, &class_breakdown,
    );

    match dingtalk::send_dingtalk_markdown(&webhook, "持仓摘要", &text).await {
        Ok(()) => Json(serde_json::json!({"code": 0, "message": "推送成功"})).into_response(),
        Err(e) => Json(serde_json::json!({"code": 1, "message": format!("推送失败: {}", e)})).into_response(),
    }
}
