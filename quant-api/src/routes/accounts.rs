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
    volatility_pct: Option<f64>,
    win_rate_pct: Option<f64>,
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
            volatility_pct: None,
            win_rate_pct: None,
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
        // 波动率/胜率（2026-09-21 账号页绩效指标补齐）：compute_metrics 已算出，
        // 小数×100 转百分数，口径与 historical_replay 响应一致
        volatility_pct: Some((m.volatility * 10000.0).round() / 100.0),
        win_rate_pct: Some((m.win_rate * 10000.0).round() / 100.0),
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
// Result<(), Response> 的 Err 为 axum Response（128B）是中间件惯用形态，显式豁免。
#[allow(clippy::result_large_err)]
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
    let volatility = perf.volatility_pct;
    let win_rate = perf.win_rate_pct;
    // 最大回撤：优先用从 NAV 序列实时算的值；无快照时回退账号字段
    let mdd = perf.max_drawdown_pct.or(mdd);
    let replay_days = perf.trading_days;
    let yearly_returns_json = perf.yearly_returns;
    // benchmarks 为另一套回放(paper.rs)的产物，v19/v21 回放本就未写入，保持 None
    let benchmarks_json: Option<serde_json::Value> = None;

    // 当前持仓（2026-09-25: LEFT JOIN market_stock 带出证券名称——用户要求持仓
    // 表同时展示标的名称；无主档记录的 symbol 名称留空由前端回落 symbol 展示）
    let positions: Vec<serde_json::Value> = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
            Option<rust_decimal::Decimal>,
        ),
    >(
        "SELECT p.symbol, ms.name, p.quantity, p.avg_cost, p.market_price, p.market_value
         FROM paper_position p
         LEFT JOIN market_stock ms ON ms.symbol = p.symbol
         WHERE p.paper_account_id = $1 AND p.quantity > 0
         ORDER BY p.market_value DESC NULLS LAST LIMIT 100",
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(sym, name, qty, cost, price, mv)| {
        serde_json::json!({
            "symbol": sym,
            "name": name.unwrap_or_default(),
            "quantity": qty.map(|v| v.to_string()).unwrap_or_default(),
            "avg_cost": cost.map(|v| v.to_string()).unwrap_or_default(),
            "market_price": price.map(|v| v.to_string()).unwrap_or_default(),
            "market_value": mv.map(|v| v.to_string()).unwrap_or_default(),
        })
    })
    .collect();

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
            Option<String>,
        ),
    >(
        "SELECT o.order_id, o.symbol, o.side, o.status, o.quantity, o.limit_price,
                f.price as fill_price, f.fill_time,
                o.target_price, o.price_upper_limit, o.price_lower_limit,
                o.slippage_pct, f.quantity as fill_quantity, f.amount as fill_amount,
                ms.name as stock_name
         FROM paper_order o LEFT JOIN paper_fill f ON o.order_id = f.planned_order_id
         LEFT JOIN market_stock ms ON ms.symbol = o.symbol
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
            stock_name,
        )| {
            serde_json::json!({
                "order_id": oid, "symbol": sym, "side": side, "status": sts,
                "name": stock_name.unwrap_or_default(),
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
            "created_at": fmt_datetime(created),
            "metrics": {
                "annual_return_pct": annual_return,
                "cumulative_return_pct": (cum_ret * 100.0).round() / 100.0,
                "sharpe_ratio": (sharpe * 100.0).round() / 100.0,
                "sortino_ratio": (sortino * 100.0).round() / 100.0,
                "max_drawdown_pct": mdd.unwrap_or(0.0),
                "calmar_ratio": (calmar * 100.0).round() / 100.0,
                "volatility_pct": volatility,
                "win_rate_pct": win_rate,
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

    // 账号曲线归一化基准：选中时间段首日 NAV。
    // cumulative_return 是自初始资金的绝对累计收益（如 54.98%），
    // relative_return 是相对选中时间段首日的累计收益（首日=0%），与基准曲线对齐，
    // 供 accounts 页面「收益率曲线 vs 沪深300」图用（两条曲线起点都归零）。
    let first_nav = nav_rows
        .first()
        .map(|(_, nav, _, _, _)| *nav)
        .filter(|v| *v > 0.0);
    let nav_data: Vec<serde_json::Value> = nav_rows
        .into_iter()
        .map(|(d, nav, dr, cr, mdd)| {
            let relative_return = first_nav
                .map(|base| ((nav / base - 1.0) * 10000.0).round() / 100.0)
                .unwrap_or(0.0);
            // 收益率类字段转百分比数值（前端图表直接当 % 画，如 54.98 表示 54.98%），
            // 保留 2 位小数（0.01% 精度），避免乘 100 round 再除 100 丢精度致曲线呈阶梯/平台。
            serde_json::json!({
                "date": d.format("%Y-%m-%d").to_string(),
                "nav": (nav * 100.0).round() / 100.0,
                "daily_return": (dr * 10000.0).round() / 100.0,
                "cumulative_return": (cr * 10000.0).round() / 100.0,
                "relative_return": relative_return,
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

    let acct_meta: Option<(Option<String>, f64)> = PgPaperAccountRepo::new(db)
        .find_strategy_and_leverage(account_id)
        .await
        .ok()
        .flatten();
    let Some((Some(sv_id), leverage_multiplier)) = acct_meta else {
        return serde_json::Value::Null;
    };

    // 优先读与账号同杠杆的 MVO 动态基准(真带杠杆跑,精确,不放大);缺失则回退无杠杆(1.0)
    // 基准 × leverage_multiplier(线性放大,近似杠杆复利)。两者皆无才回退 A 股 BC3 曲线。
    let mut mvo_rows: Vec<(chrono::NaiveDate, f64)> = sqlx::query_as(
        "SELECT trade_date, portfolio_value::double precision FROM backtest_mvo_equity_curve
         WHERE strategy_id = $1 AND leverage_multiplier = $2 AND trade_date >= $3 AND trade_date <= $4 ORDER BY trade_date",
    )
    .bind(&sv_id)
    .bind(leverage_multiplier)
    .bind(from_date)
    .bind(to_date)
    .fetch_all(db)
    .await
    .unwrap_or_default();
    // 精确 lev 基准存在则 amplify=1.0(不放大);否则回退 unlev(1.0)基准线性放大
    let amplify = if mvo_rows.is_empty() {
        mvo_rows = sqlx::query_as(
            "SELECT trade_date, portfolio_value::double precision FROM backtest_mvo_equity_curve
             WHERE strategy_id = $1 AND leverage_multiplier = 1.0 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date",
        )
        .bind(&sv_id)
        .bind(from_date)
        .bind(to_date)
        .fetch_all(db)
        .await
        .unwrap_or_default();
        leverage_multiplier
    } else {
        1.0
    };

    let bt_rows: Vec<(chrono::NaiveDate, f64)> = if !mvo_rows.is_empty() {
        mvo_rows
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
            let ret_pct = (pv / base - 1.0) * amplify * 100.0;
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
///   点击日期可展开查看当日所有成交明细。
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
    let trades: Vec<(
        chrono::NaiveDate,
        String,
        String,
        String,
        Option<rust_decimal::Decimal>,
        Option<rust_decimal::Decimal>,
        Option<rust_decimal::Decimal>,
        String,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT DATE(o.created_at) as trade_date,
                o.order_id, o.symbol, o.side,
                o.quantity, o.target_price,
                f.price as fill_price,
                o.status,
                ms.name as stock_name
         FROM paper_order o
         LEFT JOIN paper_fill f ON o.order_id = f.planned_order_id
         LEFT JOIN market_stock ms ON ms.symbol = o.symbol
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
    for (trade_date, oid, symbol, side, qty, target_price, fill_price, status, stock_name) in trades
    {
        let key = trade_date.format("%Y-%m-%d").to_string();
        trades_by_date
            .entry(key)
            .or_default()
            .push(serde_json::json!({
                "order_id": oid,
                "symbol": symbol,
                "name": stock_name.unwrap_or_default(),
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
        account_type: req
            .account_type
            .as_deref()
            .unwrap_or("simulated")
            .to_string(),
        initial_capital: req.initial_capital.unwrap_or(1_000_000.0),
        leverage_enabled: req.leverage_enabled.unwrap_or(false),
        leverage_mode: req.leverage_mode.as_deref().unwrap_or("fixed").to_string(),
        leverage_multiplier: req.leverage_multiplier.unwrap_or(1.0),
        signal_source: req.signal_source.as_deref().unwrap_or("factor").to_string(),
        user_id: Some(user.user_id.clone()),
        dingtalk_webhook_url: None,
    };

    match PgPaperAccountRepo::new(&state.db)
        .create(&aid, &input)
        .await
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
        let owner = PgPaperAccountRepo::new(&state.db)
            .find_user_id(&account_id)
            .await;
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
        let owner = PgPaperAccountRepo::new(&state.db)
            .find_user_id(&account_id)
            .await;
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
        let owner = PgPaperAccountRepo::new(&state.db)
            .find_user_id(&account_id)
            .await;
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

    // 清空关联数据（原逐表循环 DELETE，收敛为 repo 的 wipe_account_tables 防表清单漂移）
    let _ = PgPaperAccountRepo::new(&state.db)
        .wipe_account_tables(&account_id)
        .await;

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
        let owner = PgPaperAccountRepo::new(&state.db)
            .find_user_id(&account_id)
            .await;
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
        let cap = PgPaperAccountRepo::new(&state.db)
            .find_initial_capital(&account_id)
            .await
            .ok()
            .flatten();
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

// ── 连库测试（覆盖率补测第一批）────────────────────────────
//
// 模式沿用 users.rs third_batch / rebalance.rs fifth_batch 先例：
// - test_state() 连真实本机 PG（postgres://gaocheng@localhost/quant），handler 直调
//   跳过 axum HTTP 层；Tushare 客户端经 dotenv 加载 quant/.env（cwd=quant-api 时 ../.env 命中）。
// - zzz_test_ 前缀独占键自造数据；paper_account 子表（snapshot/position/order→fill）
//   均为 FK ON DELETE CASCADE，测试尾删主表行即全清。
// - UserContext 不落库可任意构造；paper_account.user_id 有 FK→quant_user，
//   造数挂 owner 用实存用户（OWNER=gaocheng / OTHER=admin）。
// - 推送测试只走 403/404/不可达 webhook 分支——../.env 配有 DINGTALK_CLIENT_ID，
//   fallback 会构造真实钉钉 URL，禁止让测试走到真实外发。
#[cfg(test)]
mod db_tests {
    use super::*;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use serde_json::Value;

    /// 实存 quant_user(gaocheng, role=admin)——造数 owner。
    const OWNER: &str = "47c7a8cd1595499da8724a7be70226e8";
    /// 实存 quant_user(admin)——"他人账号" owner。
    const OTHER: &str = "admin";

    async fn test_state() -> Arc<AppState> {
        let _ = dotenv::from_filename("../.env");
        let _ = dotenv::dotenv();
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("test db connect");
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("Tushare client init (需 TUSHARE_TOKEN: source ../.env)");
        Arc::new(AppState {
            start_time: chrono::Utc::now(),
            db,
            tushare,
            sync_tasks: crate::sync_task_registry::new_registry(),
        })
    }

    /// handler 直调后解包响应体为 serde_json::Value（users.rs third_batch 先例）。
    async fn resp_json(resp: impl IntoResponse) -> Value {
        let body = resp.into_response().into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("json response body")
    }

    fn ctx(id: &str, role: &str) -> UserContext {
        UserContext {
            user_id: id.to_string(),
            username: "zzz_test".to_string(),
            role: role.to_string(),
        }
    }

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).expect("合法日期")
    }

    fn days(i: usize) -> chrono::Duration {
        chrono::Duration::days(i as i64)
    }

    fn utc_at(y: i32, m: u32, d: u32, h: u32) -> chrono::DateTime<chrono::Utc> {
        use chrono::TimeZone;
        chrono::Utc
            .with_ymd_and_hms(y, m, d, h, 0, 0)
            .single()
            .expect("合法时刻")
    }

    /// 清残留并插入 zzz 账号（owner=OWNER, active/simulated/factor, 初始资金 10 万）。
    async fn mk_account(db: &sqlx::PgPool, aid: &str) {
        let _ = sqlx::query("DELETE FROM paper_account WHERE paper_account_id = $1")
            .bind(aid)
            .execute(db)
            .await;
        sqlx::query(
            "INSERT INTO paper_account
               (paper_account_id, name, initial_capital, cash, status, account_type, signal_source, user_id)
             VALUES ($1, $2, 100000, 100000, 'active', 'simulated', 'factor', $3)",
        )
        .bind(aid)
        .bind(format!("zzz_{aid}"))
        .bind(OWNER)
        .execute(db)
        .await
        .expect("insert zzz paper_account");
    }

    /// NAV 快照（daily_return/cumulative_return/max_drawdown 以小数入库存口径）。
    async fn mk_nav(
        db: &sqlx::PgPool,
        aid: &str,
        d: chrono::NaiveDate,
        nav: f64,
        dr: f64,
        cr: f64,
        mdd: f64,
    ) {
        sqlx::query(
            "INSERT INTO paper_nav_snapshot
               (nav_snapshot_id, paper_account_id, snapshot_date, nav, cash, market_value,
                daily_return, cumulative_return, max_drawdown)
             VALUES ($1, $2, $3, $4, 0, $4, $5, $6, $7)",
        )
        .bind(format!("zzz_snap_{}_{}", aid, d.format("%Y%m%d")))
        .bind(aid)
        .bind(d)
        .bind(nav)
        .bind(dr)
        .bind(cr)
        .bind(mdd)
        .execute(db)
        .await
        .expect("insert zzz nav snapshot");
    }

    async fn mk_position(db: &sqlx::PgPool, aid: &str, symbol: &str, qty: f64, price: f64) {
        sqlx::query(
            "INSERT INTO paper_position
               (paper_position_id, paper_account_id, symbol, quantity, avg_cost, market_price, market_value)
             VALUES ($1, $2, $3, $4, $5, $5, $6)",
        )
        .bind(format!("zzz_pos_{}_{}", aid, symbol))
        .bind(aid)
        .bind(symbol)
        .bind(qty)
        .bind(price)
        .bind(qty * price)
        .execute(db)
        .await
        .expect("insert zzz paper_position");
    }

    /// 订单（target_value 供调仓历史聚合；created_at 统一 04:00 UTC，任何时区 DATE() 同日）。
    async fn mk_order(
        db: &sqlx::PgPool,
        oid: &str,
        aid: &str,
        symbol: &str,
        side: &str,
        status: &str,
        created: chrono::DateTime<chrono::Utc>,
        target_value: f64,
    ) {
        sqlx::query(
            "INSERT INTO paper_order
               (order_id, paper_account_id, symbol, side, order_type, quantity, status, created_at, target_value)
             VALUES ($1, $2, $3, $4, 'limit', 100, $5, $6, $7)",
        )
        .bind(oid)
        .bind(aid)
        .bind(symbol)
        .bind(side)
        .bind(status)
        .bind(created)
        .bind(target_value)
        .execute(db)
        .await
        .expect("insert zzz paper_order");
    }

    async fn mk_fill(
        db: &sqlx::PgPool,
        fid: &str,
        oid: &str,
        aid: &str,
        symbol: &str,
        side: &str,
        time: chrono::DateTime<chrono::Utc>,
        price: f64,
    ) {
        sqlx::query(
            "INSERT INTO paper_fill
               (fill_id, order_id, paper_account_id, symbol, fill_time, side, quantity, price, amount, planned_order_id)
             VALUES ($1, $2, $3, $4, $5, $6, 100, $7, $8, $2)",
        )
        .bind(fid)
        .bind(oid)
        .bind(aid)
        .bind(symbol)
        .bind(time)
        .bind(side)
        .bind(price)
        .bind(100.0 * price)
        .execute(db)
        .await
        .expect("insert zzz paper_fill");
    }

    /// 证券主档（无 FK 依赖，symbol 用 zzz 前缀防撞真实数据）。
    async fn mk_stock(db: &sqlx::PgPool, symbol: &str, name: &str) {
        let _ = sqlx::query("DELETE FROM market_stock WHERE symbol = $1")
            .bind(symbol)
            .execute(db)
            .await;
        sqlx::query(
            "INSERT INTO market_stock (symbol, name, exchange, list_status) VALUES ($1, $2, 'SSE', 'L')",
        )
        .bind(symbol)
        .bind(name)
        .execute(db)
        .await
        .expect("insert zzz market_stock");
    }

    /// 活跃策略配置（leverage_cap 供列表 JOIN；strategy_id 由调用方给 zzz 键）。
    async fn mk_strategy_config(db: &sqlx::PgPool, sid: &str, leverage_cap: f64) {
        let _ = sqlx::query("DELETE FROM strategy_config WHERE strategy_id = $1")
            .bind(sid)
            .execute(db)
            .await;
        sqlx::query(
            "INSERT INTO strategy_config (strategy_id, name, status, leverage_cap) VALUES ($1, 'zzz策略', 'active', $2)",
        )
        .bind(sid)
        .bind(leverage_cap)
        .execute(db)
        .await
        .expect("insert zzz strategy_config");
    }

    async fn cleanup_account(db: &sqlx::PgPool, aid: &str) {
        let _ = sqlx::query("DELETE FROM paper_account WHERE paper_account_id = $1")
            .bind(aid)
            .execute(db)
            .await;
    }

    async fn cleanup_strategy_config(db: &sqlx::PgPool, sid: &str) {
        let _ = sqlx::query("DELETE FROM strategy_config WHERE strategy_id = $1")
            .bind(sid)
            .execute(db)
            .await;
    }

    async fn cleanup_mvo_curve(db: &sqlx::PgPool, sid: &str) {
        let _ =
            sqlx::query("DELETE FROM backtest_mvo_equity_curve WHERE strategy_id = $1 AND trade_date >= '2027-01-01'")
                .bind(sid)
                .execute(db)
                .await;
    }

    async fn cleanup_benchmark_bars(db: &sqlx::PgPool) {
        let _ = sqlx::query(
            "DELETE FROM market_index_daily_bar WHERE symbol = '000300.SH' AND trade_date >= '2027-01-01'",
        )
        .execute(db)
        .await;
    }

    /// 列表断言辅助：从 data 数组中取指定账号行。
    fn row_of<'a>(data: &'a Value, aid: &str) -> &'a Value {
        data.as_array()
            .expect("data 为数组")
            .iter()
            .find(|r| r["account_id"] == aid)
            .unwrap_or_else(|| panic!("列表中未找到 {aid}: {data}"))
    }

    // ── list_accounts ──────────────────────────────────────

    /// 非 admin 只见自己的 + 无主 active 账号，他人的不可见
    #[tokio::test]
    async fn list_accounts_scopes_non_admin_to_own_and_public() {
        let state = test_state().await;
        let db = state.db.clone();
        let own = "zzz_test_acc_list_own";
        let other = "zzz_test_acc_list_other";
        let public = "zzz_test_acc_list_public";
        mk_account(&db, own).await;
        mk_account(&db, other).await;
        sqlx::query("UPDATE paper_account SET user_id = $2 WHERE paper_account_id = $1")
            .bind(other)
            .bind(OTHER)
            .execute(&db)
            .await
            .unwrap();
        mk_account(&db, public).await;
        sqlx::query("UPDATE paper_account SET user_id = NULL WHERE paper_account_id = $1")
            .bind(public)
            .execute(&db)
            .await
            .unwrap();

        let body = resp_json(
            list_accounts(
                State(state.clone()),
                ctx(OWNER, "user"),
                Query(AccountFilter::default()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");
        let data = &body["data"];
        assert!(row_of(data, own)["account_id"] == own, "{data}");
        assert!(row_of(data, public)["account_id"] == public, "{data}");
        assert!(
            data.as_array()
                .unwrap()
                .iter()
                .all(|r| r["account_id"] != other),
            "他人账号不应可见: {data}"
        );

        cleanup_account(&db, own).await;
        cleanup_account(&db, other).await;
        cleanup_account(&db, public).await;
    }

    /// admin 无权限过滤，全部可见（含挂他人 user_id 的账号）
    #[tokio::test]
    async fn list_accounts_admin_sees_all() {
        let state = test_state().await;
        let db = state.db.clone();
        let other = "zzz_test_acc_list_adm";
        mk_account(&db, other).await;
        sqlx::query("UPDATE paper_account SET user_id = $2 WHERE paper_account_id = $1")
            .bind(other)
            .bind(OTHER)
            .execute(&db)
            .await
            .unwrap();

        let body = resp_json(
            list_accounts(
                State(state.clone()),
                ctx(OWNER, "admin"),
                Query(AccountFilter::default()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");
        // 挂他人 user_id 的账号对 admin 可见
        let row = row_of(&body["data"], other);
        assert_eq!(row["owner"], OTHER, "{row}");

        cleanup_account(&db, other).await;
    }

    /// 名称模糊过滤（zzz 独占名精确圈定）
    #[tokio::test]
    async fn list_accounts_filters_by_name() {
        let state = test_state().await;
        let db = state.db.clone();
        let hit = "zzz_test_acc_name_hit";
        let miss = "zzz_test_acc_name_miss";
        mk_account(&db, hit).await;
        mk_account(&db, miss).await;
        sqlx::query("UPDATE paper_account SET name = 'zzz独占命中名' WHERE paper_account_id = $1")
            .bind(hit)
            .execute(&db)
            .await
            .unwrap();

        let body = resp_json(
            list_accounts(
                State(state.clone()),
                ctx(OWNER, "admin"),
                Query(AccountFilter {
                    name: Some("zzz独占命中名".into()),
                    ..AccountFilter::default()
                }),
            )
            .await,
        )
        .await;
        let arr = body["data"].as_array().expect("data 为数组");
        assert!(!arr.is_empty(), "{body}");
        assert!(arr.iter().all(|r| r["name"] == "zzz独占命中名"), "{body}");

        cleanup_account(&db, hit).await;
        cleanup_account(&db, miss).await;
    }

    /// 杠杆开关 + 信号源 + 状态过滤组合
    #[tokio::test]
    async fn list_accounts_filters_leverage_signal_status() {
        let state = test_state().await;
        let db = state.db.clone();
        let lev = "zzz_test_acc_flt_lev";
        let pred = "zzz_test_acc_flt_pred";
        let stopped = "zzz_test_acc_flt_stop";
        mk_account(&db, lev).await;
        sqlx::query(
            "UPDATE paper_account SET leverage_enabled = true, leverage_multiplier = 2.0 WHERE paper_account_id = $1",
        )
        .bind(lev)
        .execute(&db)
        .await
        .unwrap();
        mk_account(&db, pred).await;
        sqlx::query(
            "UPDATE paper_account SET signal_source = 'prediction' WHERE paper_account_id = $1",
        )
        .bind(pred)
        .execute(&db)
        .await
        .unwrap();
        mk_account(&db, stopped).await;
        sqlx::query("UPDATE paper_account SET status = 'inactive' WHERE paper_account_id = $1")
            .bind(stopped)
            .execute(&db)
            .await
            .unwrap();

        let query = |filter: AccountFilter| {
            let state = state.clone();
            async move {
                resp_json(list_accounts(State(state), ctx(OWNER, "admin"), Query(filter)).await)
                    .await
            }
        };

        // leverage=enabled 只回杠杆账号
        let body = query(AccountFilter {
            leverage: Some("enabled".into()),
            ..AccountFilter::default()
        })
        .await;
        let arr = body["data"].as_array().unwrap();
        assert!(arr.iter().any(|r| r["account_id"] == lev), "{body}");
        assert!(arr.iter().all(|r| r["leverage_enabled"] == true), "{body}");

        // signal_source=prediction 只回预测账号
        let body = query(AccountFilter {
            signal_source: Some("prediction".into()),
            ..AccountFilter::default()
        })
        .await;
        let arr = body["data"].as_array().unwrap();
        assert!(arr.iter().any(|r| r["account_id"] == pred), "{body}");
        // 真实库可能有无主 prediction 账号被合法圈入,断言限定 zzz 造数行集合
        // (list 序列化不含 signal_source 字段,以结果集合验证过滤生效)
        let zzz_ids: Vec<&str> = arr
            .iter()
            .filter_map(|r| r["account_id"].as_str())
            .filter(|id| id.starts_with("zzz_test_"))
            .collect();
        assert!(zzz_ids.contains(&pred), "{body}");
        assert!(zzz_ids.iter().all(|id| !id.contains("factor")), "{body}");

        // status=inactive 只回停用账号
        let body = query(AccountFilter {
            status: Some("inactive".into()),
            ..AccountFilter::default()
        })
        .await;
        let arr = body["data"].as_array().unwrap();
        assert!(arr.iter().any(|r| r["account_id"] == stopped), "{body}");
        assert!(arr.iter().all(|r| r["status"] == "inactive"), "{body}");

        // lev_mult 范围:2.0~2.5 含杠杆账号、排除 1.0
        let body = query(AccountFilter {
            lev_mult_min: Some(2.0),
            lev_mult_max: Some(2.5),
            ..AccountFilter::default()
        })
        .await;
        let arr = body["data"].as_array().unwrap();
        assert!(arr.iter().any(|r| r["account_id"] == lev), "{body}");
        assert!(arr.iter().all(|r| r["account_id"] != pred), "{body}");

        cleanup_account(&db, lev).await;
        cleanup_account(&db, pred).await;
        cleanup_account(&db, stopped).await;
    }

    /// 列表行内嵌 NAV 实时绩效：3 天快照 → 累计 21% / 胜率 100% / trading_days=2
    #[tokio::test]
    async fn list_accounts_embeds_nav_perf() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_perf";
        mk_account(&db, aid).await;
        let base = date(2027, 5, 3);
        for (i, nav) in [100000.0, 110000.0, 121000.0].into_iter().enumerate() {
            mk_nav(&db, aid, base + days(i), nav, 0.1, 0.0, 0.0).await;
        }

        let body = resp_json(
            list_accounts(
                State(state.clone()),
                ctx(OWNER, "admin"),
                Query(AccountFilter::default()),
            )
            .await,
        )
        .await;
        let row = row_of(&body["data"], aid);
        assert_eq!(row["cumulative_return_pct"], 21.0, "{row}");
        assert!(
            row["annual_return_pct"].as_f64().unwrap_or(0.0) > 0.0,
            "年化应为正: {row}"
        );
        assert_eq!(row["max_drawdown"], 0.0, "单调上涨无回撤: {row}");
        assert_eq!(row["owner"], OWNER, "{row}");
        assert_eq!(row["initial_capital"], 100000.0, "{row}");

        cleanup_account(&db, aid).await;
    }

    /// strategy_config(active) JOIN 带出 leverage_cap
    #[tokio::test]
    async fn list_accounts_joins_leverage_cap() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_cap";
        let sid = "zzz_test_acc_cap_sc";
        mk_account(&db, aid).await;
        sqlx::query(
            "UPDATE paper_account SET strategy_version_id = $2 WHERE paper_account_id = $1",
        )
        .bind(aid)
        .bind(sid)
        .execute(&db)
        .await
        .unwrap();
        mk_strategy_config(&db, sid, 1.6).await;

        let body = resp_json(
            list_accounts(
                State(state.clone()),
                ctx(OWNER, "admin"),
                Query(AccountFilter::default()),
            )
            .await,
        )
        .await;
        let row = row_of(&body["data"], aid);
        assert_eq!(row["leverage_cap"], 1.6, "{row}");

        cleanup_account(&db, aid).await;
        cleanup_strategy_config(&db, sid).await;
    }

    // ── account_detail ─────────────────────────────────────

    #[tokio::test]
    async fn account_detail_404_for_missing_id() {
        let state = test_state().await;
        let body = resp_json(
            account_detail(
                State(state.clone()),
                ctx(OWNER, "admin"),
                Path("zzz_test_acc_missing".into()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 404, "{body}");
        assert_eq!(body["message"], "账号不存在");
    }

    /// 非 owner 访问他人账号 → 403（check_account_access 三端点共用规则）
    #[tokio::test]
    async fn account_detail_403_for_non_owner() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_403";
        mk_account(&db, aid).await;
        let body = resp_json(
            account_detail(
                State(state.clone()),
                ctx("zzz_stranger", "user"),
                Path(aid.into()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 403, "{body}");
        assert_eq!(body["message"], "无权访问");
        cleanup_account(&db, aid).await;
    }

    /// 详情完整载荷：基本信息 + NAV 绩效 + 持持(带证券名) + 成交记录 + 资产分布
    #[tokio::test]
    async fn account_detail_returns_full_payload() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_detail";
        mk_account(&db, aid).await;
        sqlx::query(
            "UPDATE paper_account SET current_nav = 121000, max_drawdown_pct = 0.05, cash = 5000 WHERE paper_account_id = $1",
        )
        .bind(aid)
        .execute(&db)
        .await
        .unwrap();
        let base = date(2027, 5, 3);
        for (i, nav) in [100000.0, 110000.0, 121000.0].into_iter().enumerate() {
            mk_nav(&db, aid, base + days(i), nav, 0.1, 0.21, 0.0).await;
        }
        mk_stock(&db, "zzz600.SH", "zzz测试股").await;
        mk_stock(&db, "zzz999.SH", "zzz二号股").await;
        mk_position(&db, aid, "zzz600.SH", 1000.0, 50.0).await;
        mk_position(&db, aid, "zzz999.SH", 200.0, 10.0).await;
        let t = utc_at(2027, 5, 5, 4);
        mk_order(
            &db,
            "zzz_ord_d1",
            aid,
            "zzz600.SH",
            "buy",
            "filled",
            t,
            50000.0,
        )
        .await;
        mk_fill(
            &db,
            "zzz_fill_d1",
            "zzz_ord_d1",
            aid,
            "zzz600.SH",
            "buy",
            t,
            50.0,
        )
        .await;

        let body = resp_json(
            account_detail(State(state.clone()), ctx(OWNER, "user"), Path(aid.into())).await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");
        let d = &body["data"];
        assert_eq!(d["account_id"], aid, "{d}");
        assert_eq!(d["name"], format!("zzz_{aid}"), "{d}");
        assert_eq!(d["current_nav"], 121000.0, "{d}");
        assert_eq!(d["cash"], 5000.0, "{d}");
        assert_eq!(d["signal_source"], "factor", "{d}");
        assert_eq!(d["owner"], OWNER, "{d}");
        assert!(
            d["created_at"].as_str().is_some(),
            "created_at 已格式化: {d}"
        );

        let m = &d["metrics"];
        assert_eq!(m["cumulative_return_pct"], 21.0, "{m}");
        assert_eq!(m["nav_history_days"], 2, "{m}");
        assert_eq!(m["win_rate_pct"], 100.0, "{m}");
        assert_eq!(m["max_drawdown_pct"], 0.0, "NAV 序列优先于账号字段: {m}");

        // 持仓按市值降序（zzz999 2000 < zzz600 50000），证券名经 market_stock JOIN 带出
        let positions = d["positions"].as_array().expect("positions");
        assert_eq!(positions.len(), 2, "{d}");
        assert_eq!(positions[0]["symbol"], "zzz600.SH", "{positions:?}");
        assert_eq!(positions[0]["name"], "zzz测试股", "{positions:?}");
        assert_eq!(positions[1]["name"], "zzz二号股", "{positions:?}");

        // 资产分布：两持仓均分类 A 股 → 单类 100%
        let alloc = d["asset_allocation"].as_array().expect("asset_allocation");
        assert_eq!(alloc.len(), 1, "{alloc:?}");
        assert_eq!(alloc[0]["name"], "A股", "{alloc:?}");
        assert!(
            (alloc[0]["pct"].as_f64().unwrap() - 100.0).abs() < 0.01,
            "{alloc:?}"
        );

        // 交易记录含成交价（NUMERIC 经 serde 为字符串形态，parse 后近似比较）
        let trades = d["trades"].as_array().expect("trades");
        assert_eq!(trades.len(), 1, "{d}");
        assert_eq!(trades[0]["order_id"], "zzz_ord_d1", "{trades:?}");
        let fp = trades[0]["fill_price"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!((fp - 50.0).abs() < 1e-6, "{trades:?}");
        assert_eq!(trades[0]["name"], "zzz测试股", "{trades:?}");

        cleanup_account(&db, aid).await;
        let _ = sqlx::query("DELETE FROM market_stock WHERE symbol IN ('zzz600.SH', 'zzz999.SH')")
            .execute(&db)
            .await
            .unwrap();
    }

    // ── nav_history ────────────────────────────────────────

    /// start/end 裁剪 + relative_return 以区间首日归零
    #[tokio::test]
    async fn nav_history_range_and_relative_return() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_navrng";
        mk_account(&db, aid).await;
        let base = date(2027, 5, 3);
        let navs = [100000.0, 102000.0, 104040.0, 106121.0];
        for (i, nav) in navs.into_iter().enumerate() {
            mk_nav(&db, aid, base + days(i), nav, 0.02, 0.06, 0.0).await;
        }

        let body = resp_json(
            nav_history(
                State(state.clone()),
                ctx(OWNER, "user"),
                Path(aid.into()),
                Query(NavHistoryParams {
                    start_date: Some("2027-05-04".into()),
                    end_date: Some("2027-05-05".into()),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");
        let hist = body["data"]["nav_history"].as_array().expect("nav_history");
        assert_eq!(hist.len(), 2, "日期裁剪应留中间两天: {hist:?}");
        assert_eq!(hist[0]["date"], "2027-05-04", "{hist:?}");
        // 首日归零、次日 = 104040/102000-1 = 2%
        assert_eq!(hist[0]["relative_return"], 0.0, "{hist:?}");
        assert_eq!(hist[1]["relative_return"], 2.0, "{hist:?}");
        assert_eq!(hist[0]["daily_return"], 2.0, "dr=0.02 转百分数: {hist:?}");
        assert_eq!(
            hist[0]["cumulative_return"], 6.0,
            "cr=0.06 转百分数: {hist:?}"
        );
        // 无回测数据时为 Null(benchmark 不判——与并行基准测试的 000300.SH 造数竞态)
        assert!(body["data"]["backtest_comparison"].is_null(), "{body}");

        cleanup_account(&db, aid).await;
    }

    /// 基准曲线（000300.SH 归一化累计收益），2027 冷门日期避免撞真实数据
    #[tokio::test]
    async fn nav_history_benchmark_curve() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_bench";
        cleanup_benchmark_bars(&db).await;
        mk_account(&db, aid).await;
        let base = date(2027, 5, 3);
        for (i, nav) in [100000.0, 102000.0, 104040.0].into_iter().enumerate() {
            mk_nav(&db, aid, base + days(i), nav, 0.02, 0.04, 0.0).await;
        }
        for (i, close) in [4000.0, 4040.0, 4080.4].into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO market_index_daily_bar (symbol, trade_date, close, source) VALUES ('000300.SH', $1, $2, 'zzz_test')",
            )
            .bind(base + days(i))
            .bind(close)
            .execute(&db)
            .await
            .expect("insert zzz benchmark bar");
        }

        let body = resp_json(
            nav_history(
                State(state.clone()),
                ctx(OWNER, "user"),
                Path(aid.into()),
                Query(NavHistoryParams {
                    start_date: None,
                    end_date: None,
                }),
            )
            .await,
        )
        .await;
        let bench = &body["data"]["benchmark"];
        assert_eq!(bench["symbol"], "000300.SH", "{bench}");
        assert_eq!(bench["name"], "沪深300", "{bench}");
        let curve = bench["curve"].as_array().expect("curve");
        assert_eq!(curve.len(), 3, "{bench}");
        assert_eq!(curve[0]["cumulative_return"], 0.0, "{bench}");
        assert_eq!(curve[1]["cumulative_return"], 1.0, "{bench}");
        assert_eq!(curve[2]["cumulative_return"], 2.01, "{bench}");

        cleanup_account(&db, aid).await;
        cleanup_benchmark_bars(&db).await;
    }

    /// 回测对比：精确同杠杆 MVO 曲线直接用（amplify=1.0）
    #[tokio::test]
    async fn nav_history_backtest_comparison_exact_leverage() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_btex";
        let sid = "zzz_test_acc_btex_sc";
        cleanup_mvo_curve(&db, sid).await;
        mk_account(&db, aid).await;
        sqlx::query(
            "UPDATE paper_account SET strategy_version_id = $2, leverage_multiplier = 2.0 WHERE paper_account_id = $1",
        )
        .bind(aid)
        .bind(sid)
        .execute(&db)
        .await
        .unwrap();
        mk_strategy_config(&db, sid, 2.3).await;
        let base = date(2027, 5, 3);
        for (i, nav) in [100000.0, 102000.0].into_iter().enumerate() {
            mk_nav(&db, aid, base + days(i), nav, 0.02, 0.02, 0.0).await;
        }
        for (i, pv) in [100.0, 110.0].into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO backtest_mvo_equity_curve (strategy_id, trade_date, portfolio_value, leverage_multiplier) VALUES ($1, $2, $3, 2.0)",
            )
            .bind(sid)
            .bind(base + days(i))
            .bind(pv)
            .execute(&db)
            .await
            .expect("insert zzz mvo curve");
        }

        let body = resp_json(
            nav_history(
                State(state.clone()),
                ctx(OWNER, "user"),
                Path(aid.into()),
                Query(NavHistoryParams {
                    start_date: None,
                    end_date: None,
                }),
            )
            .await,
        )
        .await;
        let cmp = body["data"]["backtest_comparison"]
            .as_array()
            .expect("对比曲线");
        assert_eq!(cmp.len(), 2, "{body}");
        assert_eq!(cmp[0]["cumulative_return"], 0.0, "{cmp:?}");
        assert_eq!(
            cmp[1]["cumulative_return"], 10.0,
            "精确杠杆匹配不放大: {cmp:?}"
        );

        cleanup_account(&db, aid).await;
        cleanup_strategy_config(&db, sid).await;
        cleanup_mvo_curve(&db, sid).await;
    }

    /// 回测对比：无同杠杆曲线时回退 1.0 杠杆 × leverage_multiplier 线性放大
    #[tokio::test]
    async fn nav_history_backtest_comparison_amplified_fallback() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_btfb";
        let sid = "zzz_test_acc_btfb_sc";
        cleanup_mvo_curve(&db, sid).await;
        mk_account(&db, aid).await;
        sqlx::query(
            "UPDATE paper_account SET strategy_version_id = $2, leverage_multiplier = 2.0 WHERE paper_account_id = $1",
        )
        .bind(aid)
        .bind(sid)
        .execute(&db)
        .await
        .unwrap();
        mk_strategy_config(&db, sid, 2.3).await;
        let base = date(2027, 5, 3);
        for (i, nav) in [100000.0, 102000.0].into_iter().enumerate() {
            mk_nav(&db, aid, base + days(i), nav, 0.02, 0.02, 0.0).await;
        }
        // 只有 1.0 杠杆曲线（10% 涨幅）→ amplify=2.0 → 20%
        for (i, pv) in [100.0, 110.0].into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO backtest_mvo_equity_curve (strategy_id, trade_date, portfolio_value, leverage_multiplier) VALUES ($1, $2, $3, 1.0)",
            )
            .bind(sid)
            .bind(base + days(i))
            .bind(pv)
            .execute(&db)
            .await
            .expect("insert zzz mvo curve");
        }

        let body = resp_json(
            nav_history(
                State(state.clone()),
                ctx(OWNER, "user"),
                Path(aid.into()),
                Query(NavHistoryParams {
                    start_date: None,
                    end_date: None,
                }),
            )
            .await,
        )
        .await;
        let cmp = body["data"]["backtest_comparison"]
            .as_array()
            .expect("对比曲线");
        assert_eq!(cmp.len(), 2, "{body}");
        assert_eq!(
            cmp[1]["cumulative_return"], 20.0,
            "1.0 曲线应×2 放大: {cmp:?}"
        );

        cleanup_account(&db, aid).await;
        cleanup_strategy_config(&db, sid).await;
        cleanup_mvo_curve(&db, sid).await;
    }

    // ── rebalance_history ──────────────────────────────────

    /// 按日分组聚合 filled 订单（buy/sell 计数与金额），pending 不计入
    #[tokio::test]
    async fn rebalance_history_groups_filled_orders_by_date() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_reb";
        mk_account(&db, aid).await;
        let t = utc_at(2026, 1, 5, 4); // Asia/Shanghai 12:00 同日，DATE() 稳定
        mk_order(
            &db,
            "zzz_ord_rb1",
            aid,
            "zzz600.SH",
            "buy",
            "filled",
            t,
            10000.0,
        )
        .await;
        mk_order(
            &db,
            "zzz_ord_rb2",
            aid,
            "zzz600.SH",
            "sell",
            "filled",
            t,
            8000.0,
        )
        .await;
        mk_order(
            &db,
            "zzz_ord_rb3",
            aid,
            "zzz600.SH",
            "buy",
            "pending",
            t,
            9999.0,
        )
        .await;
        mk_fill(
            &db,
            "zzz_fill_rb1",
            "zzz_ord_rb1",
            aid,
            "zzz600.SH",
            "buy",
            t,
            10.5,
        )
        .await;
        mk_fill(
            &db,
            "zzz_fill_rb2",
            "zzz_ord_rb2",
            aid,
            "zzz600.SH",
            "sell",
            t,
            9.8,
        )
        .await;

        let body = resp_json(
            rebalance_history(State(state.clone()), ctx(OWNER, "user"), Path(aid.into())).await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");
        let hist = body["data"]["rebalance_history"]
            .as_array()
            .expect("history");
        // 只有一组（2026-01-05），pending 不产生分组
        assert_eq!(hist.len(), 1, "{hist:?}");
        let day = &hist[0];
        assert_eq!(day["date"], "2026-01-05", "{day}");
        assert_eq!(day["buy_count"], 1, "{day}");
        assert_eq!(day["sell_count"], 1, "{day}");
        assert_eq!(day["total_count"], 2, "{day}");
        assert_eq!(day["buy_amount"], 10000.0, "{day}");
        assert_eq!(day["sell_amount"], 8000.0, "{day}");
        assert_eq!(day["turnover"], 18000.0, "{day}");
        let trades = day["trades"].as_array().expect("trades");
        assert_eq!(trades.len(), 2, "{day}");
        let prices: Vec<f64> = trades
            .iter()
            .filter_map(|x| x["fill_price"].as_str())
            .filter_map(|s| s.parse::<f64>().ok())
            .collect();
        assert!(
            prices.contains(&10.5) && prices.contains(&9.8),
            "{trades:?}"
        );

        cleanup_account(&db, aid).await;
    }

    // ── create_account ─────────────────────────────────────

    /// 最小请求：全默认值落库（simulated/100万/factor/fixed/1.0/挂当前用户）
    #[tokio::test]
    async fn create_account_with_defaults() {
        let state = test_state().await;
        let req = CreateAccountRequest {
            strategy_id: "zzz_test_strat".into(),
            account_type: None,
            name: "zzz创建默认".into(),
            initial_capital: None,
            leverage_enabled: None,
            leverage_mode: None,
            leverage_multiplier: None,
            signal_source: None,
        };
        let body =
            resp_json(create_account(State(state.clone()), ctx(OWNER, "user"), Json(req)).await)
                .await;
        assert_eq!(body["code"], 0, "{body}");
        let aid = body["data"]["account_id"]
            .as_str()
            .expect("account_id")
            .to_string();
        assert!(aid.starts_with("pa-"), "pa- 前缀: {aid}");

        let (name, atype, cap, cash, lev_en, lev_mode, lev_mult, ss, uid, sv, status): (
            String, String, f64, f64, bool, String, f64, String, Option<String>, Option<String>, String,
        ) = sqlx::query_as(
            "SELECT name, account_type, initial_capital::float8, cash::float8, leverage_enabled,
                    leverage_mode, leverage_multiplier, signal_source, user_id, strategy_version_id, status
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(&aid)
        .fetch_one(&state.db)
        .await
        .unwrap();
        assert_eq!(name, "zzz创建默认");
        assert_eq!(atype, "simulated");
        assert!(
            (cap - 1_000_000.0).abs() < 1e-6,
            "默认初始资金 100 万: {cap}"
        );
        assert!((cash - cap).abs() < 1e-6, "cash=initial_capital: {cash}");
        assert!(!lev_en);
        assert_eq!(lev_mode, "fixed");
        assert!((lev_mult - 1.0).abs() < 1e-9);
        assert_eq!(ss, "factor");
        assert_eq!(uid.as_deref(), Some(OWNER));
        assert!(sv.is_none(), "req.strategy_id 不落库(现行行为)");
        assert_eq!(status, "active");

        cleanup_account(&state.db, &aid).await;
    }

    /// 显式参数全量落库
    #[tokio::test]
    async fn create_account_with_full_params() {
        let state = test_state().await;
        let req = CreateAccountRequest {
            strategy_id: "zzz_test_strat".into(),
            account_type: Some("real".into()),
            name: "zzz创建全参".into(),
            initial_capital: Some(500000.0),
            leverage_enabled: Some(true),
            leverage_mode: Some("dynamic".into()),
            leverage_multiplier: Some(2.0),
            signal_source: Some("prediction".into()),
        };
        let body =
            resp_json(create_account(State(state.clone()), ctx(OWNER, "user"), Json(req)).await)
                .await;
        assert_eq!(body["code"], 0, "{body}");
        let aid = body["data"]["account_id"]
            .as_str()
            .expect("account_id")
            .to_string();

        let (atype, cap, lev_en, lev_mode, lev_mult, ss): (
            String, f64, bool, String, f64, String,
        ) = sqlx::query_as(
            "SELECT account_type, initial_capital::float8, leverage_enabled, leverage_mode, leverage_multiplier, signal_source
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(&aid)
        .fetch_one(&state.db)
        .await
        .unwrap();
        assert_eq!(atype, "real");
        assert!((cap - 500000.0).abs() < 1e-6);
        assert!(lev_en);
        assert_eq!(lev_mode, "dynamic");
        assert!((lev_mult - 2.0).abs() < 1e-9);
        assert_eq!(ss, "prediction");

        cleanup_account(&state.db, &aid).await;
    }

    /// 超长名称触发 DB 列宽拒绝 → code=1 创建失败
    #[tokio::test]
    async fn create_account_rejects_oversized_name() {
        let state = test_state().await;
        let req = CreateAccountRequest {
            strategy_id: "zzz_test_strat".into(),
            account_type: None,
            name: "z".repeat(130), // name varchar(128)
            initial_capital: None,
            leverage_enabled: None,
            leverage_mode: None,
            leverage_multiplier: None,
            signal_source: None,
        };
        let body =
            resp_json(create_account(State(state.clone()), ctx(OWNER, "user"), Json(req)).await)
                .await;
        assert_eq!(body["code"], 1, "{body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains("创建失败"),
            "{body}"
        );
    }

    // ── update_account ─────────────────────────────────────

    #[tokio::test]
    async fn update_account_changes_fields() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_upd";
        mk_account(&db, aid).await;
        let req = UpdateAccountRequest {
            name: Some("zzz改名后".into()),
            initial_capital: None,
            leverage_enabled: Some(true),
            leverage_mode: None,
            leverage_multiplier: Some(2.5),
            signal_source: Some("prediction".into()),
            status: Some("inactive".into()),
            dingtalk_webhook_url: None,
            margin_amount: Some(1000.0),
            cash: Some(99000.0),
            reserve_amount: None,
            strategy_version_id: Some("zzz_test_sv_upd".into()),
        };
        let body = resp_json(
            update_account(
                State(state.clone()),
                ctx(OWNER, "user"),
                Path(aid.into()),
                Json(req),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");

        let (name, lev_en, lev_mult, ss, status, margin, cash, sv): (
            String,
            bool,
            f64,
            String,
            String,
            f64,
            f64,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT name, leverage_enabled, leverage_multiplier, signal_source, status,
                    margin_amount::float8, cash::float8, strategy_version_id
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(aid)
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!(name, "zzz改名后");
        assert!(lev_en);
        assert!((lev_mult - 2.5).abs() < 1e-9);
        assert_eq!(ss, "prediction");
        assert_eq!(status, "inactive");
        assert!((margin - 1000.0).abs() < 1e-6);
        assert!((cash - 99000.0).abs() < 1e-6);
        assert_eq!(sv.as_deref(), Some("zzz_test_sv_upd"));

        cleanup_account(&db, aid).await;
    }

    #[tokio::test]
    async fn update_account_403_for_non_owner() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_upd403";
        mk_account(&db, aid).await;
        let req = UpdateAccountRequest {
            name: Some("zzz越权".into()),
            initial_capital: None,
            leverage_enabled: None,
            leverage_mode: None,
            leverage_multiplier: None,
            signal_source: None,
            status: None,
            dingtalk_webhook_url: None,
            margin_amount: None,
            cash: None,
            reserve_amount: None,
            strategy_version_id: None,
        };
        let body = resp_json(
            update_account(
                State(state.clone()),
                ctx("zzz_stranger", "user"),
                Path(aid.into()),
                Json(req),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 403, "{body}");
        assert_eq!(body["message"], "只能修改自己的账号");
        // 越权请求不落任何改动
        let name: String =
            sqlx::query_scalar("SELECT name FROM paper_account WHERE paper_account_id = $1")
                .bind(aid)
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(name, format!("zzz_{aid}"));
        cleanup_account(&db, aid).await;
    }

    // ── delete_account ─────────────────────────────────────

    #[tokio::test]
    async fn delete_account_soft_deletes_to_inactive() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_del";
        mk_account(&db, aid).await;
        let body = resp_json(
            delete_account(State(state.clone()), ctx(OWNER, "user"), Path(aid.into())).await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");
        assert_eq!(body["message"], "已停用");
        let status: String =
            sqlx::query_scalar("SELECT status FROM paper_account WHERE paper_account_id = $1")
                .bind(aid)
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(status, "inactive", "软删除只停用不物理删");
        cleanup_account(&db, aid).await;
    }

    #[tokio::test]
    async fn delete_account_403_for_non_owner() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_del403";
        mk_account(&db, aid).await;
        let body = resp_json(
            delete_account(
                State(state.clone()),
                ctx("zzz_stranger", "user"),
                Path(aid.into()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 403, "{body}");
        assert_eq!(body["message"], "只能删除自己的账号");
        let status: String =
            sqlx::query_scalar("SELECT status FROM paper_account WHERE paper_account_id = $1")
                .bind(aid)
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(status, "active");
        cleanup_account(&db, aid).await;
    }

    // ── reset_account ──────────────────────────────────────

    /// 重置：清空关联表 + 资金四字段重置 + created_at 改起始日期
    #[tokio::test]
    async fn reset_account_wipes_and_reinitializes() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_reset";
        mk_account(&db, aid).await;
        mk_nav(&db, aid, date(2027, 5, 3), 100000.0, 0.0, 0.0, 0.0).await;
        mk_nav(&db, aid, date(2027, 5, 4), 102000.0, 0.02, 0.02, 0.0).await;
        mk_position(&db, aid, "zzz600.SH", 100.0, 50.0).await;
        mk_order(
            &db,
            "zzz_ord_rs1",
            aid,
            "zzz600.SH",
            "buy",
            "filled",
            utc_at(2027, 5, 4, 4),
            5000.0,
        )
        .await;

        let body = resp_json(
            reset_account(
                State(state.clone()),
                ctx(OWNER, "user"),
                Path(aid.into()),
                Json(ResetAccountRequest {
                    initial_capital: 500000.0,
                    start_date: "2025-01-06".into(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 0, "{body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains("500000"),
            "提示含起始资金: {body}"
        );

        let (snap_n, order_n, pos_n): (i64, i64, i64) = sqlx::query_as(
            "SELECT
               (SELECT COUNT(*) FROM paper_nav_snapshot WHERE paper_account_id = $1),
               (SELECT COUNT(*) FROM paper_order WHERE paper_account_id = $1),
               (SELECT COUNT(*) FROM paper_position WHERE paper_account_id = $1)",
        )
        .bind(aid)
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!((snap_n, order_n, pos_n), (0, 0, 0), "关联数据应清空");

        let (cap, cash, nav, peak, margin, mdd, created): (
            f64,
            f64,
            f64,
            f64,
            f64,
            f64,
            chrono::NaiveDate,
        ) = sqlx::query_as(
            "SELECT initial_capital::float8, cash::float8, current_nav::float8, peak_nav::float8,
                    margin_amount::float8, max_drawdown_pct::float8, created_at::date
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(aid)
        .fetch_one(&db)
        .await
        .unwrap();
        assert!((cap - 500000.0).abs() < 1e-6, "{cap}");
        assert!((cash - 500000.0).abs() < 1e-6, "{cash}");
        assert!((nav - 500000.0).abs() < 1e-6, "{nav}");
        assert!((peak - 500000.0).abs() < 1e-6, "{peak}");
        assert!((margin - 0.0).abs() < 1e-6, "{margin}");
        assert!((mdd - 0.0).abs() < 1e-9, "{mdd}");
        assert_eq!(created, date(2025, 1, 6), "起始日期应写入 created_at");

        cleanup_account(&db, aid).await;
    }

    #[tokio::test]
    async fn reset_account_rejects_bad_date_format() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_rstbad";
        mk_account(&db, aid).await;
        let body = resp_json(
            reset_account(
                State(state.clone()),
                ctx(OWNER, "user"),
                Path(aid.into()),
                Json(ResetAccountRequest {
                    initial_capital: 100000.0,
                    start_date: "2025/01/06".into(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 1, "{body}");
        assert_eq!(body["message"], "日期格式错误，需要 YYYY-MM-DD");
        cleanup_account(&db, aid).await;
    }

    #[tokio::test]
    async fn reset_account_403_for_non_owner() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_rst403";
        mk_account(&db, aid).await;
        let body = resp_json(
            reset_account(
                State(state.clone()),
                ctx("zzz_stranger", "user"),
                Path(aid.into()),
                Json(ResetAccountRequest {
                    initial_capital: 100000.0,
                    start_date: "2025-01-06".into(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 403, "{body}");
        assert_eq!(body["message"], "只能重置自己的账号");
        cleanup_account(&db, aid).await;
    }

    // ── push_account_dingtalk ──────────────────────────────
    // ../.env 配有 DINGTALK_CLIENT_ID（fallback 真实 URL），成功分支会外发钉钉——
    // 只测 403/404 与不可达 webhook 的失败分支。

    #[tokio::test]
    async fn push_dingtalk_403_for_non_owner() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_push403";
        mk_account(&db, aid).await;
        let body = resp_json(
            push_account_dingtalk(
                State(state.clone()),
                ctx("zzz_stranger", "user"),
                Path(aid.into()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 403, "{body}");
        assert_eq!(body["message"], "无权操作");
        cleanup_account(&db, aid).await;
    }

    #[tokio::test]
    async fn push_dingtalk_404_for_missing_account() {
        let state = test_state().await;
        let body = resp_json(
            push_account_dingtalk(
                State(state.clone()),
                ctx(OWNER, "admin"),
                Path("zzz_test_acc_push_missing".into()),
            )
            .await,
        )
        .await;
        assert_eq!(body["code"], 404, "{body}");
        assert_eq!(body["message"], "账号不存在");
    }

    /// 账号级 webhook 指向本机不可达端口 → 发送失败 → code=1（不外发真实钉钉）
    #[tokio::test]
    async fn push_dingtalk_reports_failure_on_unreachable_webhook() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_push_bad";
        mk_account(&db, aid).await;
        sqlx::query("UPDATE paper_account SET dingtalk_webhook_url = 'http://127.0.0.1:1/zzz' WHERE paper_account_id = $1")
            .bind(aid)
            .execute(&db)
            .await
            .unwrap();
        let body = resp_json(
            push_account_dingtalk(State(state.clone()), ctx(OWNER, "user"), Path(aid.into())).await,
        )
        .await;
        assert_eq!(body["code"], 1, "{body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains("推送失败"),
            "{body}"
        );
        cleanup_account(&db, aid).await;
    }

    // ── 私有辅助函数 ───────────────────────────────────────

    /// check_account_access 四分支：admin 放行 / owner 放行 / 无主放行 / 他人 403
    #[tokio::test]
    async fn check_account_access_rules() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_access";
        let orphan = "zzz_test_acc_access_null";
        mk_account(&db, aid).await;
        mk_account(&db, orphan).await;
        sqlx::query("UPDATE paper_account SET user_id = NULL WHERE paper_account_id = $1")
            .bind(orphan)
            .execute(&db)
            .await
            .unwrap();

        assert!(
            check_account_access(&db, &ctx(OWNER, "admin"), aid)
                .await
                .is_ok(),
            "admin 直通"
        );
        assert!(
            check_account_access(&db, &ctx(OWNER, "user"), aid)
                .await
                .is_ok(),
            "owner 放行"
        );
        assert!(
            check_account_access(&db, &ctx("zzz_stranger", "user"), orphan)
                .await
                .is_ok(),
            "无主账号放行"
        );
        assert!(
            check_account_access(&db, &ctx("zzz_stranger", "user"), aid)
                .await
                .is_err(),
            "他人账号 403"
        );

        cleanup_account(&db, aid).await;
        cleanup_account(&db, orphan).await;
    }

    /// compute_perf_from_nav：<2 个快照 → 空绩效（None 指标 + 0 天）
    #[tokio::test]
    async fn perf_from_nav_empty_below_two_snapshots() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_perf1";
        mk_account(&db, aid).await;
        mk_nav(&db, aid, date(2027, 5, 3), 100000.0, 0.0, 0.0, 0.0).await;

        let perf = compute_perf_from_nav(&db, aid, 100000.0)
            .await
            .expect("Some");
        assert_eq!(perf.trading_days, 0);
        assert!(perf.annual_return_pct.is_none());
        assert!(perf.cumulative_return_pct.is_none());
        assert!(perf.yearly_returns.is_none());

        cleanup_account(&db, aid).await;
    }

    /// compute_perf_from_nav：3 天单调上涨 → 累计 21% / 胜率 100% / 交易日 2
    #[tokio::test]
    async fn perf_from_nav_computes_metrics() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_perf3";
        mk_account(&db, aid).await;
        let base = date(2027, 5, 3);
        for (i, nav) in [100000.0, 110000.0, 121000.0].into_iter().enumerate() {
            mk_nav(&db, aid, base + days(i), nav, 0.1, 0.0, 0.0).await;
        }

        let perf = compute_perf_from_nav(&db, aid, 100000.0)
            .await
            .expect("Some");
        assert_eq!(perf.trading_days, 2, "收益天数 = NAV 天数-1");
        assert!((perf.cumulative_return_pct.unwrap() - 21.0).abs() < 1e-9);
        assert!((perf.win_rate_pct.unwrap() - 100.0).abs() < 1e-9);
        assert!(
            (perf.max_drawdown_pct.unwrap() - 0.0).abs() < 1e-9,
            "单调涨无回撤"
        );
        assert!(
            (perf.volatility_pct.unwrap() - 0.0).abs() < 1e-9,
            "恒定收益波动为 0"
        );
        assert!(perf.annual_return_pct.unwrap() > 0.0);
        assert_eq!(
            perf.yearly_returns
                .as_ref()
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            1
        );

        cleanup_account(&db, aid).await;
    }

    /// compute_perf_from_nav：跨年序列 → 逐年收益分段（2025 段 -5% / 2026 段 ≈4.2%）
    #[tokio::test]
    async fn perf_from_nav_yearly_returns_across_years() {
        let state = test_state().await;
        let db = state.db.clone();
        let aid = "zzz_test_acc_perfyr";
        mk_account(&db, aid).await;
        mk_nav(&db, aid, date(2025, 12, 30), 100000.0, 0.0, 0.0, 0.0).await;
        mk_nav(&db, aid, date(2025, 12, 31), 95000.0, -0.05, -0.05, 0.05).await;
        mk_nav(&db, aid, date(2026, 1, 2), 99000.0, 0.042105, -0.01, 0.05).await;

        let perf = compute_perf_from_nav(&db, aid, 100000.0)
            .await
            .expect("Some");
        let yearly = perf.yearly_returns.as_ref().unwrap().as_array().unwrap();
        assert_eq!(yearly.len(), 2, "{yearly:?}");
        assert_eq!(yearly[0]["year"], "2025");
        assert_eq!(yearly[0]["return_pct"], -5.0, "{yearly:?}");
        assert_eq!(yearly[1]["year"], "2026");
        assert_eq!(yearly[1]["return_pct"], 4.2, "{yearly:?}");

        cleanup_account(&db, aid).await;
    }

    // ── asset_allocation（纯函数）──────────────────────────

    #[test]
    fn asset_allocation_classifies_and_sorts() {
        let positions = vec![
            serde_json::json!({"symbol": "600000.SH", "market_value": "60000"}),
            serde_json::json!({"symbol": "518880.SH", "market_value": "20000"}),
            serde_json::json!({"symbol": "000000.NULL", "market_value": "0"}),
        ];
        let alloc = asset_allocation(&positions);
        assert_eq!(alloc.len(), 2, "零市值类被过滤: {alloc:?}");
        assert_eq!(alloc[0]["name"], "A股", "{alloc:?}");
        assert_eq!(alloc[0]["market_value"], 60000.0, "{alloc:?}");
        assert!(
            (alloc[0]["pct"].as_f64().unwrap() - 75.0).abs() < 1e-9,
            "{alloc:?}"
        );
        assert_eq!(alloc[1]["name"], "黄金ETF", "{alloc:?}");
        assert!(
            (alloc[1]["pct"].as_f64().unwrap() - 25.0).abs() < 1e-9,
            "{alloc:?}"
        );
    }

    #[test]
    fn asset_allocation_empty_positions() {
        assert!(asset_allocation(&[]).is_empty());
    }
}
