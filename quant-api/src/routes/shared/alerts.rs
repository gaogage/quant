//! 质量告警模块（DDD Step 6b 从 scheduler.rs 迁出，Step 6c-1 合并钉钉告警通道）。
//!
//! 包含：
//! - [`send_dingtalk_alert_titled`]：通用钉钉 markdown 告警（查 paper_account webhook，自定义标题）
//! - [`send_dingtalk_alert`]：调仓告警的便捷封装（标题固定"调仓告警"）
//! - [`send_quality_alert`]：数据缺口告警（标题固定"数据质量告警"，调 send_dingtalk_alert_titled）
//!
//! 合并前 send_quality_alert 与 send_dingtalk_alert_titled 是两份几乎相同的代码
//! （都查 paper_account webhook + 发 markdown），Step 6c-1 统一为单一底层。

use sqlx::PgPool;

/// 通用钉钉 markdown 告警：查所有 active 且配了 webhook 的账号，逐个推送。
///
/// P2-3:调仓成功/失败/零信号/数据质量告警复用同一推送通道，用 title 区分场景。
pub(crate) async fn send_dingtalk_alert_titled(db: &PgPool, title: &str, msg: &str) {
    let accounts = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT name, dingtalk_webhook_url FROM paper_account WHERE status='active' AND dingtalk_webhook_url IS NOT NULL"
    ).fetch_all(db).await.unwrap_or_default();

    for (_name, webhook_url) in &accounts {
        if let Some(url) = webhook_url {
            let payload = serde_json::json!({
                "msgtype": "markdown",
                "markdown": {"title": title, "text": msg}
            });
            let _ = reqwest::Client::new().post(url).json(&payload).send().await;
        }
    }
}

/// 调仓告警便捷封装（标题固定"调仓告警"）。
pub(crate) async fn send_dingtalk_alert(db: &PgPool, msg: &str) {
    send_dingtalk_alert_titled(db, "调仓告警", msg).await;
}

/// 数据缺口钉钉 markdown 告警（标题固定"数据质量告警"）。
pub(crate) async fn send_quality_alert(db: &PgPool, gaps: &[String]) {
    if gaps.is_empty() {
        return;
    }
    let gap_text = gaps.join("\n- ");
    let msg = format!(
        "## ⚠️ 数据质量告警\n\n发现 {} 个数据缺口:\n- {}\n\n请检查数据同步状态。",
        gaps.len(),
        gap_text
    );
    send_dingtalk_alert_titled(db, "数据质量告警", &msg).await;
}
