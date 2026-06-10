//! 钉钉（DingTalk）Webhook 通知模块。
//!
//! 支持两种消息类型：
//! - 交易信号通知：期望的买卖标的、价格、数量
//! - 持仓摘要通知：交易完成后的持仓分布
//!
//! 配置方式（.env）：
//! - DINGTALK_CLIENT_ID: 钉钉机器人 Client ID（Webhook access_token）
//! - DINGTALK_CLIENT_SECRET: 钉钉机器人 Client Secret（预留，HMAC 签名待实现）
//!
//! Webhook URL 格式：https://oapi.dingtalk.com/robot/send?access_token={TOKEN}

use serde::Serialize;
#[cfg(test)] use serde_json::json;
use serde_json::Value;

/// 钉钉 Webhook 消息体。
#[derive(Debug, Serialize)]
struct DingTalkMessage {
    msgtype: String,
    markdown: DingTalkMarkdown,
}

#[derive(Debug, Serialize)]
struct DingTalkMarkdown {
    title: String,
    text: String,
}

/// 从环境变量获取钉钉 Client ID（Webhook access_token）。
pub fn dingtalk_client_id() -> Option<String> {
    std::env::var("DINGTALK_CLIENT_ID").ok().filter(|v| !v.is_empty())
}

/// 从环境变量获取钉钉 Client Secret（预留，用于 HMAC-SHA256 签名）。
#[allow(dead_code)]
pub fn dingtalk_client_secret() -> Option<String> {
    std::env::var("DINGTALK_CLIENT_SECRET").ok().filter(|v| !v.is_empty())
}

/// 构建钉钉 Webhook URL（从 .env 配置自动拼接）。
/// 格式：https://oapi.dingtalk.com/robot/send?access_token={TOKEN}
pub fn build_dingtalk_webhook_url() -> Option<String> {
    let client_id = dingtalk_client_id()?;
    Some(format!(
        "https://oapi.dingtalk.com/robot/send?access_token={}",
        client_id
    ))
}

/// 钉钉应用消息发送（需 DINGTALK_CLIENT_ID + DINGTALK_CLIENT_SECRET）。
/// 流程：获取 access_token → 通过工作通知发送消息。
pub async fn send_dingtalk_app_message(
    title: &str,
    text: &str,
) -> Result<(), String> {
    let client_id = dingtalk_client_id()
        .ok_or_else(|| "DINGTALK_CLIENT_ID not configured".to_string())?;
    let client_secret = dingtalk_client_secret()
        .ok_or_else(|| "DINGTALK_CLIENT_SECRET not configured".to_string())?;

    // Step 1: Get access_token
    let token_url = format!(
        "https://oapi.dingtalk.com/gettoken?appkey={}&appsecret={}",
        client_id, client_secret
    );

    let http_client = reqwest::Client::new();
    let token_resp: Value = http_client
        .get(&token_url)
        .send()
        .await
        .map_err(|e| format!("DingTalk gettoken failed: {}", e))?
        .json()
        .await
        .map_err(|e| format!("DingTalk gettoken parse failed: {}", e))?;

    let errcode = token_resp.get("errcode").and_then(|v| v.as_i64()).unwrap_or(-1);
    if errcode != 0 {
        let errmsg = token_resp.get("errmsg").and_then(|v| v.as_str()).unwrap_or("unknown");
        return Err(format!("DingTalk gettoken error {}: {}", errcode, errmsg));
    }

    let access_token = token_resp
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "DingTalk gettoken missing access_token".to_string())?;

    // Step 2: Send markdown via robot/send (webhook-style with access_token)
    let send_url = format!(
        "https://oapi.dingtalk.com/robot/send?access_token={}",
        access_token
    );

    let msg = DingTalkMessage {
        msgtype: "markdown".to_string(),
        markdown: DingTalkMarkdown {
            title: title.to_string(),
            text: text.to_string(),
        },
    };

    let resp = http_client
        .post(&send_url)
        .json(&msg)
        .send()
        .await
        .map_err(|e| format!("DingTalk send failed: {}", e))?;

    let body: Value = resp
        .json()
        .await
        .map_err(|e| format!("DingTalk response parse failed: {}", e))?;

    let send_errcode = body.get("errcode").and_then(|v| v.as_i64()).unwrap_or(-1);
    if send_errcode != 0 {
        let send_errmsg = body.get("errmsg").and_then(|v| v.as_str()).unwrap_or("unknown");
        return Err(format!("DingTalk send error {}: {}", send_errcode, send_errmsg));
    }

    Ok(())
}

/// 发送 Markdown 消息到钉钉 Webhook。
pub async fn send_dingtalk_markdown(
    webhook_url: &str,
    title: &str,
    text: &str,
) -> Result<(), String> {
    if webhook_url.is_empty() {
        return Ok(()); // No webhook configured — skip silently
    }

    let msg = DingTalkMessage {
        msgtype: "markdown".to_string(),
        markdown: DingTalkMarkdown {
            title: title.to_string(),
            text: text.to_string(),
        },
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(webhook_url)
        .json(&msg)
        .send()
        .await
        .map_err(|e| format!("DingTalk webhook failed: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("DingTalk returned {}: {}", status, body));
    }

    Ok(())
}

/// 构建交易信号通知的 Markdown 内容。
#[allow(dead_code)]
pub fn build_trade_signal_notification(
    account_name: &str,
    account_type: &str,
    signal_date: &str,
    strategy_version: &str,
    signals: &[Value],
) -> String {
    let type_label = if account_type == "real" { "🔴 实盘" } else { "🟡 模拟" };
    let mut text = format!(
        "## {} 交易信号 — {}  \n\n\
         **账号**: {} | **日期**: {} | **策略**: {}  \n\n\
         | 标的 | 操作 | 价格 | 数量 | 仓位占比 |  \n\
         |:-----|:----:|:----:|:----:|:-------:|  \n",
        type_label,
        account_name,
        account_name,
        signal_date,
        strategy_version,
    );

    for sig in signals {
        let symbol = sig.get("symbol").and_then(|v| v.as_str()).unwrap_or("?");
        let action = sig.get("action").and_then(|v| v.as_str()).unwrap_or("?");
        let price = sig.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let quantity = sig.get("quantity").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let weight = sig.get("target_weight").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let action_icon = match action {
            "buy" | "BUY" => "🟢 买入",
            "sell" | "SELL" => "🔴 卖出",
            _ => action,
        };

        text.push_str(&format!(
            "| {} | {} | {:.2} | {:.0} | {:.1}% |  \n",
            symbol,
            action_icon,
            price,
            quantity,
            weight * 100.0,
        ));
    }

    text.push_str(&format!(
        "\n\n> 📊 共 {} 条信号 | 自动生成于 {}",
        signals.len(),
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
    ));

    text
}

/// 构建持仓摘要通知的 Markdown 内容。
pub fn build_position_summary_notification(
    account_name: &str,
    account_type: &str,
    trade_date: &str,
    total_nav: f64,
    cash: f64,
    margin_amount: f64,
    market_value: f64,
    net_worth: f64,
    positions: &[Value],
    cumulative_return: f64,
    max_drawdown: f64,
    class_breakdown: &[Value],
) -> String {
    let type_label = if account_type == "real" { "🔴 实盘" } else { "🟡 模拟" };
    let position_pct = if net_worth > 0.0 { market_value / net_worth * 100.0 } else { 0.0 };
    let cash_pct = if net_worth > 0.0 { cash / net_worth * 100.0 } else { 0.0 };
    let margin_pct = if net_worth > 0.0 { margin_amount / net_worth * 100.0 } else { 0.0 };

    let mut text = format!(
        "## {} 持仓摘要 — {}  \n\n\
         **账号**: {} | **日期**: {}  \n\n\
         **净资产**: ¥{:.2}  \n\
         **总资产**: ¥{:.2} | **持仓市值**: ¥{:.2} ({:.1}%)  \n\
         **现金**: ¥{:.2} ({:.1}%) | **融资金额**: ¥{:.2} ({:.1}%)  \n\
         **累计收益**: {:.2}% | **最大回撤**: {:.2}%  \n\n\
         | 标的 | 名称 | 持仓量 | 现价 | 市值 | 占比 |  \n\
         |:-----|:-----|:------:|:----:|:----:|:----:|  \n",
        type_label, account_name,
        account_name, trade_date,
        net_worth,
        total_nav, market_value, position_pct,
        cash, cash_pct, margin_amount, margin_pct,
        cumulative_return * 100.0, max_drawdown * 100.0,
    );

    for pos in positions {
        let symbol = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("?");
        let name = pos.get("name").and_then(|v| v.as_str()).unwrap_or("");
        // Truncate long names to max 6 chars
        let short = if name.chars().count() > 6 {
            format!("{}…", name.chars().take(5).collect::<String>())
        } else { name.to_string() };
        let qty = pos.get("quantity").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let price = pos.get("current_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let mval = pos.get("market_value").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let w = if net_worth > 0.0 { mval / net_worth * 100.0 } else { 0.0 };
        text.push_str(&format!("| {} | {} | {:.0} | {:.2} | ¥{:.0} | {:.1}% |  \n",
            symbol, short, qty, price, mval, w));
    }

    // MVO 资产大类分布
    if !class_breakdown.is_empty() {
        text.push_str("\n\n**资产大类分布**:  \n");
        for cls in class_breakdown {
            let cn = cls.get("class").and_then(|v| v.as_str()).unwrap_or("?");
            let pct = cls.get("weight_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let indent = if cn.starts_with("  ") { "" } else { "" };
            text.push_str(&format!("{}- {}: {:.1}%  \n", indent, cn, pct));
        }
    }

    text.push_str(&format!(
        "\n\n> 📊 共 {} 个持仓 | 仓位 {:.1}% | 更新于 {}",
        positions.len(), position_pct,
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
    ));
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_signal_notification_contains_key_info() {
        let signals = vec![
            json!({"symbol": "000001.SZ", "action": "buy", "price": 12.50, "quantity": 1000.0, "target_weight": 0.05}),
            json!({"symbol": "600519.SH", "action": "sell", "price": 1850.0, "quantity": 100.0, "target_weight": 0.0}),
        ];

        let text = build_trade_signal_notification(
            "TestAccount", "simulated", "2026-06-02", "phase7_s4", &signals,
        );

        assert!(text.contains("🟡 模拟"));
        assert!(text.contains("TestAccount"));
        assert!(text.contains("000001.SZ"));
        assert!(text.contains("🟢 买入"));
        assert!(text.contains("600519.SH"));
        assert!(text.contains("🔴 卖出"));
        assert!(text.contains("2 条信号"));
    }

    #[test]
    fn test_build_position_summary_contains_metrics() {
        let positions = vec![
            json!({"symbol": "000001.SZ", "quantity": 5000.0, "current_price": 12.50, "market_value": 62500.0}),
            json!({"symbol": "600519.SH", "quantity": 200.0, "current_price": 1850.0, "market_value": 370000.0}),
        ];

        let text = build_position_summary_notification(
            "TestAccount", "simulated", "2026-06-02",
            1_000_000.0, 567_500.0, 0.0, 432_500.0, 1_000_000.0,
            &positions, 0.125, -0.08, &vec![],
        );

        assert!(text.contains("🟡 模拟"));
        assert!(text.contains("¥1000000.00")); // total NAV
        assert!(text.contains("¥567500.00"));  // cash
        assert!(text.contains("12.50%"));      // cumulative return
        assert!(text.contains("8.00%"));       // max drawdown
        assert!(text.contains("2 个持仓"));
    }

    #[test]
    fn test_real_account_shows_red_icon() {
        let text = build_trade_signal_notification(
            "RealAccount", "real", "2026-06-02", "phase7_s4", &[],
        );
        assert!(text.contains("🔴 实盘"));
        assert!(!text.contains("🟡 模拟"));
    }
}
