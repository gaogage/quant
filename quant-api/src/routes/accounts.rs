//! 投资账号管理 API — 用户可创建/管理自己的模拟或实盘账号。
//!
//! GET    /api/v1/accounts        — 自己的账号列表
//! POST   /api/v1/accounts        — 创建新账号
//! PUT    /api/v1/accounts/{id}   — 修改账号配置
//! DELETE /api/v1/accounts/{id}   — 删除账号

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

// 时间序列化统一走本地时区（Asia/Shanghai），避免 UI 出现 "... UTC" 后缀
use quant_common::time_utils::fmt_datetime;

use chrono::{Datelike, NaiveDate};

use crate::auth::middleware::UserContext;
use crate::routes::mvo_engine::compute_metrics;
use crate::routes::shared::{PaperAccountRepository, PgPaperAccountRepo};
use crate::AppState;

/// 绩效唯一数据源 = 每日 NAV 累计（paper_nav_snapshot）。
/// 模拟回放 / 每日盘中调仓更新 NAV 后，绩效自然随之变动。
/// 统一用 compute_metrics 算（与回写 paper_replay 同公式），保证标题栏、指标区、回放三处一致。
struct NavPerf {
    annual_return_pct: Option<f64>,
    cumulative_return_pct: Option<f64>,
    sharpe_ratio: Option<f64>,
    sortino_ratio: Option<f64>,
    calmar_ratio: Option<f64>,
    max_drawdown_pct: Option<f64>,
    trading_days: i64,
    yearly_returns: Option<serde_json::Value>,
}

impl NavPerf {
    fn empty() -> Self {
        NavPerf {
            annual_return_pct: None,
            cumulative_return_pct: None,
            sharpe_ratio: None,
            sortino_ratio: None,
            calmar_ratio: None,
            max_drawdown_pct: None,
            trading_days: 0,
            yearly_returns: None,
        }
    }
}

/// 从 paper_nav_snapshot 实时计算单个账号的绩效。
/// 返回 None 表示无快照数据（新账号尚未回放/调仓）。
async fn compute_perf_from_nav(
    db: &sqlx::PgPool,
    account_id: &str,
    initial_capital: f64,
) -> Option<NavPerf> {
    // 逐日 NAV + 日期，按 snapshot_date 排序
    let rows: Vec<(chrono::NaiveDate, f64)> = sqlx::query_as(
        "SELECT snapshot_date, nav::double precision
         FROM paper_nav_snapshot WHERE paper_account_id = $1 ORDER BY snapshot_date",
    )
    .bind(account_id)
    .fetch_all(db)
    .await
    .ok()?;

    if rows.len() < 2 {
        return Some(NavPerf::empty());
    }

    // 逐日收益率
    let returns: Vec<f64> = rows.windows(2).map(|w| w[1].1 / w[0].1 - 1.0).collect();
    if returns.is_empty() {
        return Some(NavPerf::empty());
    }

    // TODO: compute_perf_from_nav 无 ResolvedStrategy 上下文，暂用默认无风险利率；后续应从账号关联策略读 risk_free_rate
    let m = compute_metrics(&returns, crate::routes::mvo_engine::DEFAULT_RISK_FREE_RATE);
    let final_nav = rows.last().unwrap().1;
    let cum_pct = if initial_capital > 0.0 {
        (final_nav / initial_capital - 1.0) * 100.0
    } else {
        0.0
    };

    // 逐年收益（按日收益复利聚合），与 historical_replay::compute_yearly 同口径
    let mut yearly: Vec<serde_json::Value> = Vec::new();
    let mut cur_year = 0i32;
    let mut yr_nav = 1.0f64;
    // rows[0] 为首日（无前值收益），用于锚定起始年份；rows[1..] 对应 returns[i-1]
    for (i, (d, _)) in rows.iter().enumerate() {
        if i == 0 {
            cur_year = d.year();
            yr_nav = 1.0;
            continue;
        }
        let y = d.year();
        if y != cur_year {
            if cur_year != 0 {
                yearly.push(serde_json::json!({
                    "year": cur_year.to_string(),
                    "return_pct": ((yr_nav - 1.0) * 1000.0).round() / 10.0
                }));
            }
            cur_year = y;
            yr_nav = 1.0;
        }
        yr_nav *= 1.0 + returns[i - 1];
    }
    if cur_year != 0 {
        yearly.push(serde_json::json!({
            "year": cur_year.to_string(),
            "return_pct": ((yr_nav - 1.0) * 1000.0).round() / 10.0
        }));
    }

    Some(NavPerf {
        annual_return_pct: Some((m.annual_return * 10000.0).round() / 100.0),
        cumulative_return_pct: Some((cum_pct * 100.0).round() / 100.0),
        sharpe_ratio: Some((m.sharpe * 100.0).round() / 100.0),
        sortino_ratio: Some((m.sortino * 100.0).round() / 100.0),
        calmar_ratio: Some((m.calmar * 100.0).round() / 100.0),
        max_drawdown_pct: Some((m.max_drawdown * 1000.0).round() / 10.0),
        trading_days: m.trading_days as i64,
        yearly_returns: Some(serde_json::Value::Array(yearly)),
    })
}

/// 账号列表行（含 strategy_config 杠杆上限 + 从 paper_nav_snapshot 实时算的绩效）。
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
}

// ── 请求体 ──────────────────────────────────────────────

#[allow(dead_code)]
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

#[allow(dead_code)]
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
        where_clauses.push(format!(
            "(pa.user_id = '{}' OR (pa.user_id IS NULL AND pa.status = 'active'))",
            user.user_id
        ));
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
                sc.leverage_cap::double precision AS leverage_cap
         FROM paper_account pa
         LEFT JOIN strategy_config sc ON sc.strategy_id = pa.strategy_version_id AND sc.status = 'active'
         WHERE {}
         ORDER BY pa.status ASC, pa.created_at DESC",
        where_sql
    );

    let rows: Vec<AccountListRow> = sqlx::query_as(&sql)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    // 绩效统一从 paper_nav_snapshot 实时算（唯一数据源 = 每日 NAV 累计）
    let mut list: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
    for r in rows {
        let perf = compute_perf_from_nav(&state.db, &r.paper_account_id, r.initial_capital)
            .await
            .unwrap_or_else(NavPerf::empty);
        list.push(serde_json::json!({
            "account_id": r.paper_account_id, "account_type": r.account_type,
            "name": r.name, "initial_capital": r.initial_capital,
            "leverage_enabled": r.leverage_enabled, "leverage_mode": r.leverage_mode,
            "leverage_multiplier": r.leverage_multiplier, "status": r.status,
            "owner": r.user_id.unwrap_or_default(),
            "current_nav": r.current_nav, "cash": r.cash,
            "max_drawdown": perf.max_drawdown_pct
                .or_else(|| r.max_drawdown_pct.map(|v| v * 100.0))
                .unwrap_or(0.0),
            "leverage_cap": r.leverage_cap,
            "margin_amount": r.margin_amount, "reserve_amount": r.reserve_amount,
            "strategy_version_id": r.strategy_version_id.unwrap_or_default(),
            "annual_return_pct": perf.annual_return_pct,
            "cumulative_return_pct": perf.cumulative_return_pct,
            "sharpe_ratio": perf.sharpe_ratio,
            "sortino_ratio": perf.sortino_ratio,
            "calmar_ratio": perf.calmar_ratio,
        }));
    }

    Json(serde_json::json!({"code": 0, "data": list}))
}

// ── 详情 ────────────────────────────────────────────────

/// 账号访问权限校验：admin 可访问任意账号；非 admin 只能访问自己拥有的账号(或无主账号)。
/// account_detail / nav_history / rebalance_history 三个端点共用同一套校验规则。
async fn check_account_access(
    db: &sqlx::PgPool,
    user: &UserContext,
    account_id: &str,
) -> Result<(), axum::response::Response> {
    if user.role == "admin" {
        return Ok(());
    }
    let owner = PgPaperAccountRepo::new(db).find_user_id(account_id).await;
    match owner {
        Ok(Some(oid)) if oid == user.user_id => Ok(()),
        Ok(None) => Ok(()),
        _ => Err(Json(serde_json::json!({"code": 403, "message": "无权访问"})).into_response()),
    }
}

/// GET /api/v1/accounts/{id} — 账号详情（绩效+持仓+交易记录）
pub async fn account_detail(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = check_account_access(&state.db, &user, &account_id).await {
        return resp;
    }

    // 基本信息
    let acc: Option<(
        String,
        String,
        String,
        f64,
        f64,
        f64,
        f64,
        bool,
        String,
        f64,
        String,
        String,
        Option<String>,
        Option<f64>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        "SELECT paper_account_id, account_type, name, initial_capital::double precision,
                COALESCE(current_nav, initial_capital)::double precision as nav,
                COALESCE(cash, initial_capital)::double precision as cash,
                COALESCE(margin_amount, 0)::double precision as margin_amount,
                leverage_enabled, leverage_mode, leverage_multiplier, signal_source, status,
                user_id, max_drawdown_pct::double precision, created_at
         FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let Some((aid, at, name, cap, nav, cash, margin, le, lm, lmp, ss, st, uid, mdd, created)) = acc
    else {
        return Json(serde_json::json!({"code": 404, "message": "账号不存在"})).into_response();
    };

    // 绩效指标：统一从 paper_nav_snapshot 实时算（唯一数据源 = 每日 NAV 累计）。
    // 模拟回放/每日调仓更新 NAV 后绩效自然变动，不再依赖 paper_replay 存死的聚合值。
    let perf = compute_perf_from_nav(&state.db, &account_id, cap)
        .await
        .unwrap_or_else(NavPerf::empty);
    let annual_return = perf.annual_return_pct.unwrap_or(0.0);
    let sharpe = perf.sharpe_ratio.unwrap_or(0.0);
    let sortino = perf.sortino_ratio.unwrap_or(0.0);
    let cum_ret = perf.cumulative_return_pct.unwrap_or(0.0);
    let calmar = perf.calmar_ratio.unwrap_or(0.0);
    // 最大回撤：优先用从 NAV 序列实时算的值；无快照时回退账号字段
    let mdd = perf.max_drawdown_pct.or(mdd);
    let replay_days = perf.trading_days;
    let yearly_returns_json = perf.yearly_returns;
    // benchmarks 为另一套回放(paper.rs)的产物，v19/v21 回放本就未写入，保持 None
    let benchmarks_json: Option<serde_json::Value> = None;

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
    let trades: Vec<serde_json::Value> = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
        ),
    >(
        "SELECT o.order_id, o.symbol, o.side, o.status, o.quantity, o.limit_price,
                f.price as fill_price, f.fill_time,
                o.target_price, o.price_upper_limit, o.price_lower_limit,
                o.slippage_pct, f.quantity as fill_quantity, f.amount as fill_amount
         FROM paper_order o LEFT JOIN paper_fill f ON o.order_id = f.planned_order_id
         WHERE o.paper_account_id = $1
         ORDER BY COALESCE(f.fill_time, o.created_at) DESC LIMIT 50",
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(
        |(
            oid,
            sym,
            side,
            sts,
            qty,
            lim,
            fill_price,
            fill_time,
            target_price,
            upper,
            lower,
            slip,
            fill_qty,
            fill_amt,
        )| {
            serde_json::json!({
                "order_id": oid, "symbol": sym, "side": side, "status": sts,
                "quantity": qty.map(|v| v.to_string()).unwrap_or_default(),
                "limit_price": lim.map(|v| v.to_string()).unwrap_or_default(),
                "fill_price": fill_price.map(|v| v.to_string()).unwrap_or_default(),
                "fill_time": fmt_datetime(fill_time).unwrap_or_default(),
                "planned": {
                    "target_price": target_price.map(|v| v.to_string()),
                    "price_upper": upper.map(|v| v.to_string()),
                    "price_lower": lower.map(|v| v.to_string()),
                    "slippage_pct": slip.map(|v| v.to_string()),
                },
                "fill_quantity": fill_qty.map(|v| v.to_string()),
                "fill_amount": fill_amt.map(|v| v.to_string()),
            })
        },
    )
    .collect();

    Json(serde_json::json!({
        "code": 0, "data": {
            "account_id": aid, "account_type": at, "name": name,
            "initial_capital": cap, "current_nav": nav,
            "cash": cash, "margin_amount": margin,
            "leverage_enabled": le, "leverage_mode": lm,
            "leverage_multiplier": lmp, "signal_source": ss,
            "status": st, "owner": uid.unwrap_or_default(),
            "created_at": fmt_datetime(created.map(|t| t)),
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
    }))
    .into_response()
}

// ── NAV 历史（P3-1 dashboard 画曲线用）─────────────────────

#[derive(Deserialize)]
pub struct NavHistoryParams {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

/// GET /api/v1/accounts/{id}/nav-history
///
/// 返回 paper_nav_snapshot 的逐日 NAV 序列 + 回测对比 + 基准（沪深300）曲线。
/// 可选 start_date / end_date 限定时间范围，供前端日期选择器用。
pub async fn nav_history(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
    Query(params): Query<NavHistoryParams>,
) -> impl IntoResponse {
    if let Err(resp) = check_account_access(&state.db, &user, &account_id).await {
        return resp;
    }

    // 解析可选日期范围（格式 YYYY-MM-DD），None 时用极值兜底保证 SQL 统一
    let parse_date = |s: &Option<String>| -> Option<NaiveDate> {
        s.as_ref()
            .and_then(|v| NaiveDate::parse_from_str(v, "%Y-%m-%d").ok())
    };
    let start_date = parse_date(&params.start_date)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap());
    let end_date = parse_date(&params.end_date)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(2099, 12, 31).unwrap());

    // 从 paper_nav_snapshot 读取逐日 NAV 序列
    let nav_rows: Vec<(NaiveDate, f64, f64, f64, f64)> = sqlx::query_as(
        "SELECT snapshot_date, nav::double precision,
                COALESCE(daily_return, 0)::double precision,
                COALESCE(cumulative_return, 0)::double precision,
                COALESCE(max_drawdown, 0)::double precision
         FROM paper_nav_snapshot
         WHERE paper_account_id = $1
           AND snapshot_date >= $2
           AND snapshot_date <= $3
         ORDER BY snapshot_date",
    )
    .bind(&account_id)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    // 与回测对比
    let backtest_curve = query_backtest_comparison(&state.db, &account_id, &nav_rows).await;

    // 基准曲线（沪深300）：以 NAV 序列首日~末日为区间，索引收盘价归一化为累计收益率
    let benchmark = query_benchmark_curve(&state.db, &nav_rows).await;

    let nav_data: Vec<serde_json::Value> = nav_rows
        .into_iter()
        .map(|(d, nav, dr, cr, mdd)| {
            // 收益率类字段转百分比数值（前端图表直接当 % 画，如 54.98 表示 54.98%），
            // 保留 2 位小数（0.01% 精度），避免乘 100 round 再除 100 丢精度致曲线呈阶梯/平台。
            serde_json::json!({
                "date": d.format("%Y-%m-%d").to_string(),
                "nav": (nav * 100.0).round() / 100.0,
                "daily_return": (dr * 10000.0).round() / 100.0,
                "cumulative_return": (cr * 10000.0).round() / 100.0,
                "max_drawdown": (mdd * 10000.0).round() / 100.0,
            })
        })
        .collect();

    Json(serde_json::json!({
        "code": 0,
        "data": {
            "nav_history": nav_data,
            "backtest_comparison": backtest_curve,
            "benchmark": benchmark,
        }
    }))
    .into_response()
}

/// 按 NAV 快照起点，取回测同期 backtest_equity_curve 归一化后的累计收益序列，
/// 供前端画"实盘 vs 回测"双线对比图。归一化基准 = 双方在起始日的值都设为 0%。
async fn query_backtest_comparison(
    db: &sqlx::PgPool,
    account_id: &str,
    nav_rows: &[(NaiveDate, f64, f64, f64, f64)],
) -> serde_json::Value {
    let Some((from_date, ..)) = nav_rows.first().copied() else {
        return serde_json::Value::Null;
    };
    let Some((to_date, ..)) = nav_rows.last().copied() else {
        return serde_json::Value::Null;
    };

    let acct_meta: Option<(Option<String>, f64)> = sqlx::query_as(
        "SELECT strategy_version_id, COALESCE(leverage_multiplier, 1.0)::double precision \
         FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    let Some((Some(sv_id), leverage_multiplier)) = acct_meta else {
        return serde_json::Value::Null;
    };

    // P1 修复:优先读 composite 合成曲线(消除纯 A 股曲线的结构性偏差),
    // 按账号 leverage_multiplier 放大(composite 曲线本身无杠杆)。缺失则回退 A 股曲线。
    let composite_rows: Vec<(chrono::NaiveDate, f64)> = sqlx::query_as(
        "SELECT trade_date, portfolio_value::double precision FROM backtest_composite_equity_curve
         WHERE strategy_id = $1 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date",
    )
    .bind(&sv_id)
    .bind(from_date)
    .bind(to_date)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let bt_rows: Vec<(chrono::NaiveDate, f64)> = if !composite_rows.is_empty() {
        composite_rows
    } else {
        let Ok(rs) = crate::routes::strategy::load_resolved_strategy(db, &sv_id).await else {
            return serde_json::Value::Null;
        };
        let a_share = rs
            .assets
            .iter()
            .find(|a| a.asset_class == crate::routes::strategy::AssetClass::AShare);
        let Some(task_id) = a_share.and_then(|a| a.security.equity_curve_task_id.clone()) else {
            return serde_json::Value::Null;
        };
        sqlx::query_as(
            "SELECT trade_date, portfolio_value::double precision FROM backtest_equity_curve
             WHERE task_id = $1 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date",
        )
        .bind(&task_id)
        .bind(from_date)
        .bind(to_date)
        .fetch_all(db)
        .await
        .unwrap_or_default()
    };

    if bt_rows.is_empty() {
        return serde_json::Value::Null;
    }
    let base = bt_rows[0].1;
    if base <= 0.0 {
        return serde_json::Value::Null;
    }

    let series: Vec<serde_json::Value> = bt_rows
        .into_iter()
        .map(|(d, pv)| {
            let ret_pct = (pv / base - 1.0) * leverage_multiplier * 100.0;
            serde_json::json!({
                "date": d.format("%Y-%m-%d").to_string(),
                "cumulative_return": (ret_pct * 100.0).round() / 100.0,
            })
        })
        .collect();

    serde_json::json!(series)
}

/// 查询基准（沪深300 `000300.SH`）在 NAV 序列区间内的日线收盘价，归一化为累计收益率序列。
///
/// 基准固定为 `000300.SH`（与 `paper.rs` 中 `req.benchmark.unwrap_or("000300.SH")` 默认一致）。
/// 归一化：首日 close 为基准 0%，后续 `cumulative_return = (close_i / close_first - 1.0) * 100.0`。
/// 若区间内无指数数据，返回 Null。
async fn query_benchmark_curve(
    db: &sqlx::PgPool,
    nav_rows: &[(NaiveDate, f64, f64, f64, f64)],
) -> serde_json::Value {
    let Some((from_date, ..)) = nav_rows.first().copied() else {
        return serde_json::Value::Null;
    };
    let Some((to_date, ..)) = nav_rows.last().copied() else {
        return serde_json::Value::Null;
    };

    let bench_symbol = "000300.SH"; // 与 paper.rs 默认一致

    let bp_rows: Vec<(NaiveDate, f64)> = sqlx::query_as(
        "SELECT trade_date, close::double precision
         FROM market_index_daily_bar
         WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3
         ORDER BY trade_date",
    )
    .bind(bench_symbol)
    .bind(from_date)
    .bind(to_date)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    if bp_rows.len() < 2 {
        return serde_json::Value::Null;
    }
    let first_close = bp_rows[0].1;
    if first_close <= 0.0 {
        return serde_json::Value::Null;
    }

    let curve: Vec<serde_json::Value> = bp_rows
        .into_iter()
        .map(|(d, close)| {
            let cum_ret = (close / first_close - 1.0) * 100.0;
            serde_json::json!({
                "date": d.format("%Y-%m-%d").to_string(),
                "cumulative_return": (cum_ret * 100.0).round() / 100.0,
            })
        })
        .collect();

    serde_json::json!({
        "symbol": bench_symbol,
        "name": "沪深300",
        "curve": curve,
    })
}

// ── 调仓历史（P3-3）───────────────────────────────────────

/// GET /api/v1/accounts/{id}/rebalance-history
///
/// 按交易日分组汇总调仓记录。从 paper_order 表聚合，每交易日一组：
/// - 交易日 / 买笔数+金额 / 卖笔数+金额 / 每笔交易明细（trades 数组）
/// 点击日期可展开查看当日所有成交明细。
pub async fn rebalance_history(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Path(account_id): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = check_account_access(&state.db, &user, &account_id).await {
        return resp;
    }

    // 1. 聚合摘要（按日期）
    let rows: Vec<(chrono::NaiveDate, i64, i64, f64, f64)> = sqlx::query_as(
        "SELECT DATE(created_at),
                COUNT(*) FILTER (WHERE side='buy')::bigint,
                COUNT(*) FILTER (WHERE side='sell')::bigint,
                COALESCE(SUM(target_value) FILTER (WHERE side='buy'), 0)::double precision,
                COALESCE(SUM(target_value) FILTER (WHERE side='sell'), 0)::double precision
         FROM paper_order
         WHERE paper_account_id = $1 AND status = 'filled'
         GROUP BY 1 ORDER BY 1 DESC LIMIT 60",
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    // 2. 交易明细（全部 filled 订单，含成交价）
    let trades: Vec<(chrono::NaiveDate, String, String, String, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>, String)> = sqlx::query_as(
        "SELECT DATE(o.created_at) as trade_date,
                o.order_id, o.symbol, o.side,
                o.quantity, o.target_price,
                f.price as fill_price,
                o.status
         FROM paper_order o
         LEFT JOIN paper_fill f ON o.order_id = f.planned_order_id
         WHERE o.paper_account_id = $1 AND o.status = 'filled'
         ORDER BY o.created_at DESC",
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    // 3. 按日期分组交易明细
    let mut trades_by_date: std::collections::HashMap<String, Vec<serde_json::Value>> =
        std::collections::HashMap::new();
    for (trade_date, oid, symbol, side, qty, target_price, fill_price, status) in trades {
        let key = trade_date.format("%Y-%m-%d").to_string();
        trades_by_date.entry(key).or_default().push(serde_json::json!({
            "order_id": oid,
            "symbol": symbol,
            "side": side,
            "quantity": qty.map(|v| v.to_string()).unwrap_or_default(),
            "target_price": target_price.map(|v| v.to_string()),
            "fill_price": fill_price.map(|v| v.to_string()),
            "status": status,
        }));
    }

    let history: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(date, buy_n, sell_n, buy_amt, sell_amt)| {
            let total = buy_n + sell_n;
            let turnover = buy_amt + sell_amt;
            let date_str = date.format("%Y-%m-%d").to_string();
            let day_trades = trades_by_date.remove(&date_str).unwrap_or_default();
            serde_json::json!({
                "date": date_str,
                "buy_count": buy_n,
                "sell_count": sell_n,
                "total_count": total,
                "buy_amount": (buy_amt * 100.0).round() / 100.0,
                "sell_amount": (sell_amt * 100.0).round() / 100.0,
                "turnover": (turnover * 100.0).round() / 100.0,
                "trades": day_trades,
            })
        })
        .collect();

    Json(serde_json::json!({
        "code": 0,
        "data": { "rebalance_history": history }
    }))
    .into_response()
}

// ── 创建 ────────────────────────────────────────────────

/// POST /api/v1/accounts — 创建新账号
pub async fn create_account(
    State(state): State<Arc<AppState>>,
    user: UserContext,
    Json(req): Json<CreateAccountRequest>,
) -> impl IntoResponse {
    let aid = format!("pa-{}", Uuid::new_v4().simple()); // paper_account 前缀
    let input = crate::routes::shared::CreateAccountInput {
        name: req.name.clone(),
        base_currency: "CNY".to_string(),
        account_type: req.account_type.as_deref().unwrap_or("simulated").to_string(),
        initial_capital: req.initial_capital.unwrap_or(1_000_000.0),
        leverage_enabled: req.leverage_enabled.unwrap_or(false),
        leverage_mode: req.leverage_mode.as_deref().unwrap_or("fixed").to_string(),
        leverage_multiplier: req.leverage_multiplier.unwrap_or(1.0),
        signal_source: req.signal_source.as_deref().unwrap_or("factor").to_string(),
        user_id: Some(user.user_id.clone()),
        dingtalk_webhook_url: None,
    };

    match PgPaperAccountRepo::new(&state.db).create(&aid, &input).await {
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
        let owner = PgPaperAccountRepo::new(&state.db).find_user_id(&account_id).await;
        match owner {
            Ok(Some(oid)) if oid == user.user_id => {}
            Ok(None) => {} // 无主账号允许修改
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
         WHERE paper_account_id = $11",
    )
    .bind(&req.name)
    .bind(req.leverage_enabled)
    .bind(&req.leverage_mode)
    .bind(req.leverage_multiplier)
    .bind(&req.signal_source)
    .bind(&req.status)
    .bind(&req.dingtalk_webhook_url)
    .bind(req.margin_amount)
    .bind(req.cash)
    .bind(req.reserve_amount)
    .bind(&account_id)
    .bind(&req.strategy_version_id)
    .execute(&state.db)
    .await;

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
        let owner = PgPaperAccountRepo::new(&state.db).find_user_id(&account_id).await;
        match owner {
            Ok(Some(oid)) if oid == user.user_id => {}
            Ok(None) => {}
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
        let owner = PgPaperAccountRepo::new(&state.db).find_user_id(&account_id).await;
        match owner {
            Ok(Some(oid)) if oid == user.user_id => {}
            Ok(None) => {}
            _ => {
                return Json(serde_json::json!({"code": 403, "message": "只能重置自己的账号"}))
                    .into_response()
            }
        }
    }

    let start_date = match chrono::NaiveDate::parse_from_str(&req.start_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return Json(serde_json::json!({"code": 1, "message": "日期格式错误，需要 YYYY-MM-DD"}))
                .into_response()
        }
    };

    // 清空关联数据
    for table in &[
        "paper_order",
        "paper_fill",
        "paper_position",
        "paper_nav_snapshot",
        "paper_replay",
        "paper_margin_trade",
    ] {
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
         WHERE paper_account_id = $3",
    )
    .bind(cap)
    .bind(start_date)
    .bind(&account_id)
    .execute(&state.db)
    .await;

    Json(serde_json::json!({"code": 0, "message": format!("账号已重置，起始资金¥{}，起始日期{}", cap as i64, req.start_date)})).into_response()
}

/// 根据持仓列表计算资产大类占比
fn asset_allocation(positions: &[serde_json::Value]) -> Vec<serde_json::Value> {
    use std::collections::HashMap;
    let mut categories: HashMap<String, f64> = HashMap::new();
    for p in positions {
        let sym = p.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
        let mv_str = p
            .get("market_value")
            .and_then(|v| v.as_str())
            .unwrap_or("0");
        let mv: f64 = mv_str.parse().unwrap_or(0.0);
        let cat = crate::routes::asset_meta::classify_asset(sym);
        *categories.entry(cat).or_default() += mv;
    }
    let total: f64 = categories.values().sum();
    let mut result: Vec<serde_json::Value> = categories
        .into_iter()
        .filter(|(_, v)| *v > 0.0)
        .map(|(name, value)| {
            serde_json::json!({
                "name": name, "market_value": (value * 100.0).round() / 100.0,
                "pct": if total > 0.0 { (value / total * 10000.0).round() / 100.0 } else { 0.0 }
            })
        })
        .collect();
    result.sort_by(|a, b| {
        b["pct"]
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&a["pct"].as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    result
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
        let owner = PgPaperAccountRepo::new(&state.db).find_user_id(&account_id).await;
        match owner {
            Ok(Some(oid)) if oid == user.user_id => {}
            Ok(None) => {}
            _ => {
                return Json(serde_json::json!({"code": 403, "message": "无权操作"}))
                    .into_response()
            }
        }
    }

    // 查询账号信息
    let acc: Option<(String, String, Option<String>, f64, f64, f64)> = sqlx::query_as(
        "SELECT name, account_type, dingtalk_webhook_url,
                COALESCE(current_nav, initial_capital)::double precision,
                COALESCE(cash, initial_capital)::double precision,
                COALESCE(margin_amount, 0)::double precision
         FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let (name, acc_type, webhook_url, nav, cash, margin) = match acc {
        Some(a) => a,
        None => {
            return Json(serde_json::json!({"code": 404, "message": "账号不存在"})).into_response()
        }
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

    let trade_date = snap
        .map(|(d,)| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| chrono::Local::now().format("%Y-%m-%d").to_string());

    // 累计收益 & 最大回撤
    let perf: Option<(Option<f64>,)> = sqlx::query_as(
        "SELECT max_drawdown_pct::double precision FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten();

    let mdd = perf.and_then(|(m,)| m).unwrap_or(0.0); // DB 存的是小数（0.1238 = 12.38%），dingtalk.rs 会 ×100 显示为百分比
    let cum_ret = if nav > 0.0 {
        // 从 paper_account 获取 initial_capital 计算
        let cap = PgPaperAccountRepo::new(&state.db).find_initial_capital(&account_id).await.ok().flatten();
        let init = cap.unwrap_or(nav);
        if init > 0.0 {
            nav / init - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    // 当前持仓（LEFT JOIN market_stock 取中文名）
    let positions: Vec<serde_json::Value> = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
        ),
    >(
        "SELECT pp.symbol, ms.name, pp.quantity, pp.avg_cost, pp.market_price
         FROM paper_position pp
         LEFT JOIN market_stock ms ON ms.symbol = pp.symbol
         WHERE pp.paper_account_id = $1 AND ABS(pp.quantity) > 0",
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(sym, name, qty, _cost, price)| {
        let q: f64 = qty
            .as_ref()
            .and_then(|v| v.to_string().parse().ok())
            .unwrap_or(0.0);
        let p: f64 = price
            .as_ref()
            .and_then(|v| v.to_string().parse().ok())
            .unwrap_or(0.0);
        let mv = (q * p).to_string();
        serde_json::json!({
            "symbol": sym,
            "name": name.unwrap_or_default(),
            "quantity": qty.map(|v| v.to_string()).unwrap_or_default(),
            "current_price": price.map(|v| v.to_string()).unwrap_or_default(),
            "market_value": mv,
        })
    })
    .collect();

    // 资产大类分布
    let class_breakdown = asset_allocation(&positions);

    let mv: f64 = positions
        .iter()
        .filter_map(|p| {
            p.get("market_value")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
        })
        .sum();
    let net_worth = mv + cash - margin;
    let total_assets = mv + cash; // 总资产=持仓市值+现金(含融资买入部分)；净资产=总资产-融资额

    let text = dingtalk::build_position_summary_notification(
        &name,
        &acc_type,
        &trade_date,
        total_assets,
        cash,
        margin,
        mv,
        net_worth,
        &positions,
        cum_ret,
        mdd,
        &class_breakdown,
    );

    match dingtalk::send_dingtalk_markdown(&webhook, "持仓摘要", &text).await {
        Ok(()) => Json(serde_json::json!({"code": 0, "message": "推送成功"})).into_response(),
        Err(e) => Json(serde_json::json!({"code": 1, "message": format!("推送失败: {}", e)}))
            .into_response(),
    }
}
