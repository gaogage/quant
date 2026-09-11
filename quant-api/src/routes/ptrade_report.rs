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
    exec_files: Vec<String>,
    heartbeats: Vec<String>,
    error: Option<String>,
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
    if summary.exec_files.is_empty() && summary.heartbeats.is_empty() {
        // 当日既无回报也无心跳: 策略未运行/邮件未发/通道故障——按设计告警
        return "⚠️ [PTrade实盘日报] 当日无 exec 回报且无心跳邮件——请检查 PTrade 策略状态与邮件通道(策略挂了? 15:00 后未发?)".to_string();
    }
    for hb in &summary.heartbeats {
        lines.push(format!("🫀 心跳: {}", hb));
    }
    for f in &summary.exec_files {
        match ingest_one(db, f).await {
            Ok(desc) => lines.push(desc),
            Err(e) => lines.push(format!("⚠️ {} 入库失败: {}", f, e)),
        }
    }
    format!("📊 [PTrade实盘日报]\n{}", lines.join("\n"))
}

async fn ingest_one(db: &PgPool, path: &str) -> Result<String, String> {
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

    Ok(format!(
        "📈 {} ({}): 订单 {} 笔, 未成交 {}, NAV {:.0}, 换手 {:.0}",
        signal_id,
        trade_date,
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

