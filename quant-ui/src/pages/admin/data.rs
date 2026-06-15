use dioxus::prelude::*;
use serde_json::Value;

use crate::api;

#[component]
pub fn DataSyncPage() -> Element {
    let mut sync = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);

    let load = move || {
        spawn(async move {
            if let Ok(v) = api::admin_sync_status().await {
                if let Some(arr) = v.get("data").and_then(|d| d.as_array()) {
                    sync.set(arr.clone());
                }
            }
            loading.set(false);
        });
    };

    use_effect(move || { load(); });

    if *loading.read() {
        return rsx! {
            div { class: "p-6", div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" } }
        };
    }

    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            h1 { class: "text-2xl font-bold text-gray-900 dark:text-white mb-6", "数据同步" }

            div { class: "space-y-2",
                for s in sync.read().iter() {
                    DataSyncItem {
                        data: s.clone(),
                        on_repaired: Callback::new(move |_| { load(); }),
                    }
                }
            }

            AccountDataHealthSection {}
        }
    }
}

/// 每个数据项独立管理自己的修复状态，互不干扰
#[component]
fn DataSyncItem(data: Value, on_repaired: Callback<()>) -> Element {
    let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("-").to_string();
    let healthy = data.get("healthy").and_then(|v| v.as_bool()).unwrap_or(false);
    let gap = data.get("current_gap_days").and_then(|v| v.as_i64()).unwrap_or(0);
    let max_gap = data.get("max_gap_days").and_then(|v| v.as_i64()).unwrap_or(0);
    let extra = data.get("extra").and_then(|v| v.as_str()).unwrap_or("").to_string();

    // ── 每个项独立的修复状态 ──────────────────────────
    let mut repairing = use_signal(|| false);
    let mut logs = use_signal(|| Vec::<String>::new());
    let mut result_ok = use_signal(|| Option::<bool>::None); // None=无结果, Some(true)=成功, Some(false)=失败

    let name_for_repair = name.clone();
    let do_repair = move |_| {
        if *repairing.read() {
            return;
        }
        repairing.set(true);
        result_ok.set(None);
        logs.set(vec![format!("⏳ 开始修复「{}」…", name_for_repair)]);

        let n = name_for_repair.clone();
        // 因子、ML、权益曲线会触发后台任务，需要更长轮询；其他同步项也会短轮询复查。
        let is_async_repair = n.contains("因子") || n == "ML预测" || n == "权益曲线";

        spawn(async move {
            let outcome = match api::admin_repair_data(&n).await {
                Ok(v) => {
                    if v["code"].as_i64().unwrap_or(-1) == 0 {
                        let msg = v["message"].as_str().unwrap_or("");
                        {
                            // 修复后持续轮询状态，直到变为正常或超时。避免“HTTP成功但数据仍异常”。
                            logs.set(vec![
                                format!("⏳ 修复「{}」…", n),
                                format!("📡 {} — 自动检测中…", msg),
                            ]);
                            let mut poll_count = 0u32;
                            let max_polls = if is_async_repair { 120u32 } else { 12u32 };
                            loop {
                                gloo_timers::future::TimeoutFuture::new(5_000).await;
                                poll_count += 1;
                                // 检查当前项的状态
                                if let Ok(status) = api::admin_sync_status().await {
                                    if let Some(arr) = status.get("data").and_then(|d| d.as_array()) {
                                        let current = arr.iter()
                                            .find(|item| item.get("name").and_then(|v| v.as_str()) == Some(n.as_str()));
                                        let now_healthy = current
                                            .and_then(|item| item.get("healthy").and_then(|v| v.as_bool()))
                                            .unwrap_or(false);
                                        if now_healthy {
                                            result_ok.set(Some(true));
                                            logs.set(vec![
                                                format!("⏳ 修复「{}」…", n),
                                                format!("✅ 修复完成（耗时 {} 秒）", poll_count * 5),
                                            ]);
                                            repairing.set(false);
                                            on_repaired(());
                                            return;
                                        }
                                        if poll_count >= max_polls {
                                            let detail = current
                                                .and_then(|item| item.get("extra").and_then(|v| v.as_str()))
                                                .unwrap_or("修复后状态仍未达标");
                                            result_ok.set(Some(false));
                                            logs.set(vec![
                                                format!("⏳ 修复「{}」…", n),
                                                format!("⚠️ 已触发但仍异常: {}", detail),
                                            ]);
                                            repairing.set(false);
                                            on_repaired(());
                                            return;
                                        }
                                    }
                                }
                                if poll_count >= max_polls {
                                    result_ok.set(Some(false));
                                    logs.set(vec![
                                        format!("⏳ 修复「{}」…", n),
                                        format!("⚠️ 轮询超时（{} 秒），请手动刷新查看状态", poll_count * 5),
                                    ]);
                                    repairing.set(false);
                                    on_repaired(());
                                    return;
                                }
                                // 更新轮询日志
                                logs.set(vec![
                                    format!("⏳ 修复「{}」…", n),
                                    format!("📡 检测中…（{}/{} 次，已等 {} 秒）", poll_count, max_polls, poll_count * 5),
                                ]);
                            }
                        }
                    }
                    // 失败处理
                    let msg = v["message"].as_str().unwrap_or("未知错误");
                    result_ok.set(Some(false));
                    logs.set(vec![
                        format!("⏳ 修复「{}」…", n),
                        format!("❌ 修复失败: {}", msg),
                    ]);
                    repairing.set(false);
                    on_repaired(());
                    false
                }
                Err(e) => {
                    result_ok.set(Some(false));
                    logs.set(vec![
                        format!("⏳ 修复「{}」…", n),
                        format!("❌ 修复失败: {}", e),
                    ]);
                    repairing.set(false);
                    on_repaired(());
                    false
                }
            };
            let _ = outcome;
        });
    };

    let show_log = *repairing.read() || (*result_ok.read()).is_some();

    rsx! {
        div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden",
            // 行主体
            div { class: "p-4 flex items-center justify-between",
                div {
                    div {
                        span { class: "text-gray-900 dark:text-white text-sm font-medium", "{name}" }
                        span { class: "text-xs text-gray-500 dark:text-gray-400 ml-2", "（允许 ≤{max_gap}天）" }
                    }
                    if !extra.is_empty() {
                        div { class: "text-xs text-gray-500 dark:text-gray-400 mt-1", "{extra}" }
                    }
                }
                div { class: "flex items-center gap-3",
                    span { class: "text-sm text-gray-500 dark:text-gray-400", "间隔: {gap}天" }
                    if healthy {
                        span { class: "text-xs px-2 py-0.5 rounded-full bg-green-100 dark:bg-green-900/50 text-green-700 dark:text-green-400", "正常" }
                    } else {
                        button {
                            class: "text-xs px-2 py-0.5 rounded-full bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-400 hover:bg-red-200 dark:hover:bg-red-800 cursor-pointer transition disabled:opacity-50",
                            disabled: *repairing.read(),
                            onclick: do_repair,
                            if *repairing.read() {
                                span { class: "inline-flex items-center gap-1",
                                    span { class: "animate-spin inline-block w-3 h-3 border-2 border-red-400 border-t-transparent rounded-full" }
                                    "修复中"
                                }
                            } else {
                                "异常"
                            }
                        }
                    }
                }
            }

            // 修复日志区（仅本项可见）
            if show_log {
                div { class: "border-t border-gray-100 dark:border-gray-800 px-4 py-3 bg-gray-50 dark:bg-gray-800/50",
                    div { class: "text-xs font-mono space-y-1",
                        for log in logs.read().iter() {
                            {
                                let cls = if log.starts_with("✅") { "text-green-600 dark:text-green-400" }
                                    else if log.starts_with("❌") { "text-red-600 dark:text-red-400" }
                                    else { "text-gray-500 dark:text-gray-400" };
                                rsx! { div { class: "{cls}", "{log}" } }
                            }
                        }
                    }
                    // 失败时显示关闭按钮
                    if *result_ok.read() == Some(false) {
                        button {
                            class: "mt-2 text-xs text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 underline",
                            onclick: move |_| result_ok.set(None),
                            "关闭"
                        }
                    }
                }
            }
        }
    }
}

/// 组件4: 账号依赖加工数据健康检查区块。
/// 轻量模式(默认)查新鲜度；输入时间段则逐年深度扫描。异常项可点击修复。
#[component]
pub fn AccountDataHealthSection() -> Element {
    let mut checks = use_signal(Vec::<serde_json::Value>::new);
    let mut summary = use_signal(String::new);
    let mut loading = use_signal(|| false);
    let mut start_date = use_signal(String::new);
    let mut end_date = use_signal(String::new);

    let mut run = move || {
        loading.set(true);
        let s = start_date.read().clone();
        let e = end_date.read().clone();
        spawn(async move {
            let (sd, ed) = if s.is_empty() || e.is_empty() {
                (None, None)
            } else {
                (Some(s), Some(e))
            };
            match crate::api::admin_account_data_health(sd, ed).await {
                Ok(v) => {
                    let d = &v["data"];
                    let arr = d["checks"].as_array().cloned().unwrap_or_default();
                    summary.set(format!(
                        "{} 账号 · {} · 红{} 黄{}",
                        d["accounts_checked"].as_i64().unwrap_or(0),
                        if d["mode"]=="range_coverage" {"区间覆盖"} else if d["mode"]=="deep_yearly" {"逐年深度"} else {"新鲜度"},
                        d["red"].as_i64().unwrap_or(0),
                        d["yellow"].as_i64().unwrap_or(0),
                    ));
                    checks.set(arr);
                }
                Err(e) => summary.set(format!("检查失败: {}", e)),
            }
            loading.set(false);
        });
    };

    rsx! {
        div { class: "mt-6 border-t border-gray-200 dark:border-gray-700 pt-4",
            div { class: "flex items-center justify-between mb-3",
                h3 { class: "text-lg font-semibold text-gray-800 dark:text-gray-100",
                    "账号依赖加工数据检查"
                }
                div { class: "flex items-center gap-2",
                    input {
                        class: "px-2 py-1 text-sm border rounded dark:bg-gray-800 dark:border-gray-600",
                        r#type: "date", value: "{start_date}",
                        oninput: move |e| start_date.set(e.value()),
                    }
                    span { class: "text-gray-400", "~" }
                    input {
                        class: "px-2 py-1 text-sm border rounded dark:bg-gray-800 dark:border-gray-600",
                        r#type: "date", value: "{end_date}",
                        oninput: move |e| end_date.set(e.value()),
                    }
                    button {
                        class: "px-3 py-1 text-sm bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50",
                        disabled: loading(),
                        onclick: move |_| run(),
                        if loading() { "检查中..." } else { "检查" }
                    }
                }
            }
            if !summary.read().is_empty() {
                p { class: "text-sm text-gray-600 dark:text-gray-400 mb-2", "{summary}" }
            }
            div { class: "space-y-1",
                for c in checks.read().iter() {
                    AccountDataHealthItem { check: c.clone() }
                }
            }
        }
    }
}

/// 组件4: 单个账号数据检查项（红黄绿 + 异常可点击修复）。
#[component]
fn AccountDataHealthItem(check: serde_json::Value) -> Element {
    let mut fixing = use_signal(|| false);
    let mut fix_msg = use_signal(String::new);

    let level = check["level"].as_str().unwrap_or("green").to_string();
    let account = check["account"].as_str().unwrap_or("-").to_string();
    let item = check["item"].as_str().unwrap_or("-").to_string();
    let detail = check["detail"].as_str().unwrap_or("").to_string();
    let fix_ep = check["fix_endpoint"].as_str().map(|s| s.to_string());
    let fix_params = check.get("fix_params").cloned();
    let fix_reason = check["fix_reason"].as_str().unwrap_or("").to_string();

    let (dot, badge) = match level.as_str() {
        "red" => ("bg-red-500", "bg-red-100 text-red-700 dark:bg-red-900/50 dark:text-red-400"),
        "yellow" => ("bg-yellow-500", "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/50 dark:text-yellow-400"),
        _ => ("bg-green-500", "bg-green-100 text-green-700 dark:bg-green-900/50 dark:text-green-400"),
    };
    let can_fix = level != "green" && fix_ep.is_some();

    let do_fix = move |_| {
        if *fixing.read() { return; }
        let (Some(ep), Some(params)) = (fix_ep.clone(), fix_params.clone()) else { return; };
        fixing.set(true);
        fix_msg.set("修复触发中...".into());
        spawn(async move {
            match crate::api::admin_repair_by_endpoint(&ep, &params).await {
                Ok(v) => {
                    if v["code"].as_i64().unwrap_or(-1) == 0 {
                        let msg = v["message"].as_str().unwrap_or("已触发修复");
                        fix_msg.set(format!("✅ {}", msg));
                    } else {
                        let msg = v["message"].as_str().unwrap_or("修复失败");
                        fix_msg.set(format!("❌ {}", msg));
                    }
                }
                Err(e) => fix_msg.set(format!("❌ {}", e)),
            }
            fixing.set(false);
        });
    };

    rsx! {
        div { class: "flex items-center justify-between py-1.5 px-3 rounded bg-white dark:bg-gray-900 border border-gray-100 dark:border-gray-800",
            div { class: "flex items-center gap-2 min-w-0",
                span { class: "w-2 h-2 rounded-full flex-shrink-0 {dot}" }
                span { class: "text-xs text-gray-500 dark:text-gray-500 w-32 truncate", "{account}" }
                span { class: "text-sm text-gray-800 dark:text-gray-200 font-medium", "{item}" }
                span { class: "text-xs text-gray-400 truncate", "{detail}" }
            }
            div { class: "flex items-center gap-2 flex-shrink-0",
                if !fix_msg.read().is_empty() {
                    span { class: "text-xs text-gray-500", "{fix_msg}" }
                }
                if !can_fix && level != "green" && !fix_reason.is_empty() {
                    span { class: "text-xs text-gray-500 max-w-sm truncate", "{fix_reason}" }
                }
                span { class: "text-xs px-2 py-0.5 rounded-full {badge}", "{level}" }
                if can_fix {
                    button {
                        class: "text-xs px-2 py-0.5 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50",
                        disabled: fixing(),
                        onclick: do_fix,
                        if fixing() { "..." } else { "修复" }
                    }
                }
            }
        }
    }
}
