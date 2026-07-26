//! 质量告警模块（DDD Step 6b 从 scheduler.rs 迁出）。
//!
//! 包含：
//! - [`send_quality_alert`]：数据缺口钉钉 markdown 告警
//!
//! 原位置：scheduler.rs:3487-3513。

use sqlx::PgPool;

pub(crate) async fn send_quality_alert(db: &PgPool, gaps: &[String]) {
    let accounts = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT name, dingtalk_webhook_url FROM paper_account WHERE status='active' AND dingtalk_webhook_url IS NOT NULL"
    )
    .fetch_all(db).await.unwrap_or_default();

    if accounts.is_empty() {
        return;
    }

    let gap_text = gaps.join("\n- ");
    let msg = format!(
        "## ⚠️ 数据质量告警\n\n发现 {} 个数据缺口:\n- {}\n\n请检查数据同步状态。",
        gaps.len(),
        gap_text
    );

    for (_name, webhook_url) in &accounts {
        if let Some(url) = webhook_url {
            let payload = serde_json::json!({
                "msgtype": "markdown",
                "markdown": {"title": "数据质量告警", "text": msg}
            });
            let _ = reqwest::Client::new().post(url).json(&payload).send().await;
        }
    }
}
