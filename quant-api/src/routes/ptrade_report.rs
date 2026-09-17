//! PTrade 实盘回报抓取（B1' 回报回流链, 2026-09-11）
//!
//! 16:30 定时任务(scheduled_task_config: ptrade_report_fetch):
//! python3 拉 imap.qq.com 当日 ptrade_exec_*/ptrade_heartbeat_* 邮件 →
//! exec 附件解析入库(ptrade_execution_report) → 钉钉实盘日报。
//! 心跳缺失 = 策略挂了或通道故障 → 告警(区分"无交易"与"故障")。
//!
//! Python 脚本经 include_str! 嵌入二进制, 运行时落盘 /tmp 执行——
//! 免 Dockerfile COPY, 免镜像重建依赖脚本更新(换脚本需重编 Rust)。

use sqlx::PgPool;
use tracing::error;

const FETCH_SCRIPT: &str = include_str!("../../../scripts/ptrade_report_fetch.py");

/// 定时任务入口(scheduler.rs "ptrade_report_fetch" 分支调用)。
pub async fn run_ptrade_report_fetch(db: &PgPool) {
    if let Err(e) = ensure_report_table(db).await {
        error!("[PTrade回报] 建表失败: {}", e);
        crate::routes::shared::send_dingtalk_alert(db, &format!("⛔ [PTrade回报] 建表失败: {}", e)).await;
        return;
    }
    let fetch = fetch_mail_reports().await;
    match fetch {
        Ok(summary) => {
            let report = ingest_reports(db, &summary).await;
            crate::routes::shared::send_dingtalk_alert(db, &report).await;
        }
        Err(e) => {
            crate::routes::shared::send_dingtalk_alert(
                db,
                &format!("⛔ [PTrade回报] 邮件拉取失败(IMAP 通道故障?): {}", e),
            )
            .await;
        }
    }
}

#[derive(serde::Deserialize, Default)]
struct FetchSummary {
    execs: Vec<ExecMail>,
    heartbeats: Vec<String>,
    error: Option<String>,
}

#[derive(serde::Deserialize, Default)]
struct ExecMail {
    path: String,
    subject: String,
}

async fn fetch_mail_reports() -> Result<FetchSummary, String> {
    // 脚本落盘 + 执行(容器内 python3 由 Dockerfile 提供)
    let script_path = "/tmp/ptrade_report_fetch.py";
    std::fs::write(script_path, FETCH_SCRIPT).map_err(|e| format!("script write: {}", e))?;
    let out = tokio::process::Command::new("python3")
        .arg(script_path)
        .output()
        .await
        .map_err(|e| format!("python3 spawn: {}", e))?;
    if !out.status.success() {
        return Err(format!(
            "python3 exit {:?}: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str::<FetchSummary>(stdout.trim())
        .map_err(|e| format!("summary parse: {} raw={}", e, stdout.trim()))
}

/// exec json 入库 + 组装日报文本。
async fn ingest_reports(db: &PgPool, summary: &FetchSummary) -> String {
    let mut lines: Vec<String> = Vec::new();
    if let Some(e) = &summary.error {
        lines.push(format!("⚠️ 拉取部分异常: {}", e));
    }
    if summary.execs.is_empty() && summary.heartbeats.is_empty() {
        // 当日既无回报也无心跳: 策略未运行/邮件未发/通道故障——按设计告警
        return "⚠️ [PTrade实盘日报] 当日无 exec 回报且无心跳邮件——请检查 PTrade 策略状态与邮件通道(策略挂了? 15:00 后未发?)".to_string();
    }
    for hb in &summary.heartbeats {
        lines.push(format!("🫀 心跳: {}", hb));
    }
    for m in &summary.execs {
        match ingest_one(db, &m.path, &m.subject).await {
            Ok(desc) => lines.push(desc),
            Err(e) => lines.push(format!("⚠️ {} 入库失败: {}", m.path, e)),
        }
    }
    format!("📊 [PTrade实盘日报]\n{}", lines.join("\n"))
}

/// 从邮件主题提取通道tag: ptrade_exec_{sim|live}_{date} → "sim"/"live"。
fn channel_tag(subject: &str) -> Option<&str> {
    let rest = subject.strip_prefix("ptrade_exec_")?;
    match rest.split('_').next() {
        Some(t @ ("sim" | "live")) => Some(t),
        _ => None, // 旧格式(无tag)或未知
    }
}

/// 镜像账户回写: 按通道tag找 ptrade_channel_config 对应账户, 同步 nav/cash/持仓。
/// 首次回写且账户为空仓时顺带校准 initial_capital(以 PTrade 真实值为基准)。
async fn sync_mirror_account(
    db: &PgPool,
    tag: &str,
    v: &serde_json::Value,
) -> Result<Option<String>, String> {
    let is_prod = tag == "live";
    let account: Option<(String,)> = sqlx::query_as(
        "SELECT paper_account_id FROM ptrade_channel_config
         WHERE is_production = $1 AND enabled = true LIMIT 1",
    )
    .bind(is_prod)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("mirror account: {}", e))?;
    let Some((account_id,)) = account else {
        return Ok(None); // 无对应通道配置: 仅入库不回写
    };
    let nav = v.get("nav_after").and_then(|x| x.as_f64());
    let cash = v.get("cash_after").and_then(|x| x.as_f64());
    let pos_arr = v.get("positions").and_then(|x| x.as_array());
    let trade_date = v
        .get("trade_date")
        .and_then(|x| x.as_str())
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());
    // PTrade .SS → quant .SH(quant 体系统一 .SH/.SZ, bar 表/估值组件依赖此格式)
    let norm_sym = |s: &str| -> String {
        if let Some(c) = s.strip_suffix(".SS") {
            format!("{}.SH", c)
        } else {
            s.to_string()
        }
    };
    let dec = |x: f64| rust_decimal::Decimal::from_f64_retain(x).unwrap_or_default();

    // 有效持仓数(回报 positions 含当日清仓的 amount=0 零头行, 不能用数组 is_empty 判断)
    let n_valid_pos = pos_arr
        .map(|a| {
            a.iter()
                .filter(|p| p.get("amount").and_then(|x| x.as_f64()).unwrap_or(0.0) > 0.0)
                .count()
        })
        .unwrap_or(0);
    let empty_before: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM paper_position WHERE paper_account_id=$1 AND quantity>0",
    )
    .bind(&account_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("mirror count: {}", e))?;
    sqlx::query("UPDATE paper_account SET current_nav=$2, cash=$3, updated_at=now(),
                 initial_capital = CASE WHEN $4 AND $5 THEN $2 ELSE initial_capital END,
                 total_trades = total_trades + $6
                 WHERE paper_account_id=$1")
        .bind(&account_id)
        .bind(nav.map(dec))
        .bind(cash.map(dec))
        // 首次回写校准: 账户空仓且回报也无有效持仓(以 PTrade 真实值为基准)
        .bind(empty_before.0 == 0 && n_valid_pos == 0)
        .bind(nav.is_some())
        .bind(
            v.get("orders")
                .and_then(|x| x.as_array())
                .map(|a| a.len() as i32)
                .unwrap_or(0),
        )
        .execute(db)
        .await
        .map_err(|e| format!("mirror nav: {}", e))?;

    // ── 持仓重建(幂等: DELETE+INSERT; symbol 规范化 .SS→.SH) ──
    let mut n_pos = 0i32;
    if let Some(positions) = pos_arr {
        sqlx::query("DELETE FROM paper_position WHERE paper_account_id=$1")
            .bind(&account_id)
            .execute(db)
            .await
            .map_err(|e| format!("mirror del: {}", e))?;
        for p in positions {
            let sym = p.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let qty = p.get("amount").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let px = p.get("last_sale_price").and_then(|x| x.as_f64()).unwrap_or(0.0);
            if sym.is_empty() || qty <= 0.0 {
                continue;
            }
            // avg_cost 用现价近似(回报无成本字段); 绩效口径以 NAV 为准
            sqlx::query(
                "INSERT INTO paper_position (paper_position_id, paper_account_id, symbol,
                   quantity, avg_cost, market_price, market_value, created_at, updated_at)
                 VALUES ($1,$2,$3,$4,$5,$5,$6,now(),now())",
            )
            .bind(format!("pp-{}", uuid::Uuid::new_v4()))
            .bind(&account_id)
            .bind(norm_sym(sym))
            .bind(dec(qty))
            .bind(dec(px))
            .bind(dec(qty * px))
            .execute(db)
            .await
            .map_err(|e| format!("mirror pos {}: {}", sym, e))?;
            n_pos += 1;
        }
    }

    // ── 交易明细回写(2026-09-17 v2: orders→paper_fill, 镜像账户与 PTrade 全量一致) ──
    // 成交价优先级: 执行器 avg_price(新版) > limit_price > 当日收盘价兜底。
    if let (Some(orders), Some(td)) = (v.get("orders").and_then(|x| x.as_array()), trade_date) {
        for o in orders {
            let sym = o.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let filled = o.get("filled").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let status = o.get("status").and_then(|x| x.as_str()).unwrap_or("");
            if sym.is_empty() || filled.abs() < 1.0 || status != "8" {
                continue; // 只回写已全部成交的委托(状态8)
            }
            let entrust = o.get("entrust_no").and_then(|x| x.as_str()).unwrap_or("");
            let mut px = o.get("avg_price").and_then(|x| x.as_f64()).unwrap_or(0.0);
            if px <= 0.0 {
                px = o.get("limit_price").and_then(|x| x.as_f64()).unwrap_or(0.0);
            }
            if px <= 0.0 {
                let fallback: Option<rust_decimal::Decimal> = sqlx::query_scalar(
                    "SELECT close FROM market_stock_daily_bar WHERE symbol=$1 AND trade_date=$2",
                )
                .bind(norm_sym(sym))
                .bind(td)
                .fetch_optional(db)
                .await
                .map_err(|e| format!("fill px fallback {}: {}", sym, e))?
                .flatten();
                px = fallback.map(|d| d.to_string().parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
            }
            let qty = dec(filled.abs());
            let amt = dec(filled.abs() * px);
            sqlx::query(
                "INSERT INTO paper_fill (fill_id, order_id, paper_account_id, symbol, fill_time,
                   side, quantity, price, amount, commission, tax, slippage)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,0,0)
                 ON CONFLICT (fill_id, fill_time) DO NOTHING",  // 分区表唯一键须含 fill_time
            )
            .bind(format!("pf-{}-{}", v.get("signal_id").and_then(|x| x.as_str()).unwrap_or(""), entrust))
            .bind(format!("po-{}", entrust))
            .bind(&account_id)
            .bind(norm_sym(sym))
            .bind(chrono::Utc::now())  // 回报处理时刻(成交时点回报未提供, 用入库时间)
            .bind(if filled > 0.0 { "buy" } else { "sell" })
            .bind(qty)
            .bind(dec(px))
            .bind(amt)
            .execute(db)
            .await
            .map_err(|e| format!("mirror fill {}: {}", sym, e))?;
        }
    }

    // ── NAV 快照回写(v2: nav-history 曲线/日报依赖, 与模拟盘同表) ──
    if let (Some(td), Some(nav_v)) = (trade_date, nav) {
        let prev_nav: Option<rust_decimal::Decimal> = sqlx::query_scalar(
            "SELECT nav FROM paper_nav_snapshot WHERE paper_account_id=$1 AND snapshot_date < $2
             ORDER BY snapshot_date DESC LIMIT 1",
        )
        .bind(&account_id)
        .bind(td)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("mirror prev nav: {}", e))?
        .flatten();
        let daily_ret = prev_nav
            .and_then(|p| p.to_string().parse::<f64>().ok().filter(|p| *p > 0.0))
            .map(|p| (nav_v / p - 1.0) as f64);
        let mv = positions_value(v);
        sqlx::query(
            "INSERT INTO paper_nav_snapshot (nav_snapshot_id, paper_account_id, snapshot_date,
               nav, cash, market_value, position_count, daily_return)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT (paper_account_id, snapshot_date) DO UPDATE SET
               nav = EXCLUDED.nav, cash = EXCLUDED.cash,
               market_value = EXCLUDED.market_value, position_count = EXCLUDED.position_count,
               daily_return = EXCLUDED.daily_return",
        )
        .bind(format!("pns-{}-{}", account_id, td.format("%Y%m%d")))
        .bind(&account_id)
        .bind(td)
        .bind(dec(nav_v))
        .bind(cash.map(dec))
        .bind(dec(mv))
        .bind(n_pos)
        .bind(daily_ret.map(dec))
        .execute(db)
        .await
        .map_err(|e| format!("mirror snapshot: {}", e))?;
    }
    Ok(Some(account_id))
}

/// 回报 positions_value 字段(持仓市值合计), 缺失时由 positions 求和兜底。
fn positions_value(v: &serde_json::Value) -> f64 {
    if let Some(mv) = v.get("positions_value").and_then(|x| x.as_f64()) {
        return mv;
    }
    v.get("positions")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .map(|p| p.get("market_value").and_then(|x| x.as_f64()).unwrap_or(0.0))
                .sum()
        })
        .unwrap_or(0.0)
}

async fn ingest_one(db: &PgPool, path: &str, subject: &str) -> Result<String, String> {
    let raw_str = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let v: serde_json::Value =
        serde_json::from_str(&raw_str).map_err(|e| format!("json: {}", e))?;
    let signal_id = v
        .get("signal_id")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let trade_date = v
        .get("trade_date")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let num = |k: &str| {
        v.get(k)
            .and_then(|x| x.as_f64())
            .map(|f| rust_decimal::Decimal::from_f64_retain(f))
            .flatten()
    };
    let orders = v.get("orders").cloned().unwrap_or(serde_json::json!([]));
    let positions = v.get("positions").cloned().unwrap_or(serde_json::json!([]));
    let unfilled = v.get("unfilled").cloned().unwrap_or(serde_json::json!([]));
    let n_orders = orders.as_array().map(|a| a.len()).unwrap_or(0);
    let n_unfilled = unfilled.as_array().map(|a| a.len()).unwrap_or(0);

    sqlx::query(
        "INSERT INTO ptrade_execution_report
           (signal_id, trade_date, nav_after, cash_after, turnover_today,
            orders, positions, unfilled, raw)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (signal_id) DO UPDATE SET
           nav_after=EXCLUDED.nav_after, cash_after=EXCLUDED.cash_after,
           turnover_today=EXCLUDED.turnover_today, orders=EXCLUDED.orders,
           positions=EXCLUDED.positions, unfilled=EXCLUDED.unfilled, raw=EXCLUDED.raw",
    )
    .bind(&signal_id)
    .bind(&trade_date)
    .bind(num("nav_after"))
    .bind(num("cash_after"))
    .bind(num("turnover_today"))
    .bind(&orders)
    .bind(&positions)
    .bind(&unfilled)
    .bind(&v)
    .execute(db)
    .await
    .map_err(|e| format!("db: {}", e))?;

    let mirror = match channel_tag(subject) {
        Some(tag) => sync_mirror_account(db, tag, &v).await?,
        None => None, // 旧格式主题(无通道tag): 仅入库
    };
    Ok(format!(
        "📈 {} ({}){}: 订单 {} 笔, 未成交 {}, NAV {:.0}, 换手 {:.0}",
        signal_id,
        trade_date,
        mirror.map(|a| format!(" →已同步{}", a)).unwrap_or_default(),
        n_orders,
        n_unfilled,
        v.get("nav_after").and_then(|x| x.as_f64()).unwrap_or(0.0),
        v.get("turnover_today")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0),
    ))
}

async fn ensure_report_table(db: &PgPool) -> Result<(), String> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ptrade_execution_report (
           id BIGSERIAL PRIMARY KEY,
           signal_id TEXT NOT NULL UNIQUE,
           trade_date TEXT NOT NULL,
           received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
           nav_after NUMERIC,
           cash_after NUMERIC,
           turnover_today NUMERIC,
           orders JSONB NOT NULL DEFAULT '[]',
           positions JSONB NOT NULL DEFAULT '[]',
           unfilled JSONB NOT NULL DEFAULT '[]',
           raw JSONB NOT NULL
         )",
    )
    .execute(db)
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

