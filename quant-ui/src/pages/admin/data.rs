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
        // 判断是否为异步修复（因子/ML 预测需要等待后台任务）
        let is_async_repair = n == "因子(pv)" || n == "ML预测";

        spawn(async move {
            let outcome = match api::admin_repair_data(&n).await {
                Ok(v) => {
                    if v["code"].as_i64().unwrap_or(-1) == 0 {
                        let msg = v["message"].as_str().unwrap_or("");
                        if is_async_repair {
                            // 异步修复：持续轮询状态直到变为正常或超时
                            logs.set(vec![
                                format!("⏳ 修复「{}」…", n),
                                format!("📡 {} — 自动检测中…", msg),
                            ]);
                            let mut poll_count = 0u32;
                            let max_polls = 30u32; // 最多轮询 30 次（30×5=150秒）
                            loop {
                                gloo_timers::future::TimeoutFuture::new(5_000).await;
                                poll_count += 1;
                                // 检查当前项的状态
                                if let Ok(status) = api::admin_sync_status().await {
                                    if let Some(arr) = status.get("data").and_then(|d| d.as_array()) {
                                        let now_healthy = arr.iter()
                                            .find(|item| item.get("name").and_then(|v| v.as_str()) == Some(n.as_str()))
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
                        } else {
                            result_ok.set(Some(true));
                            logs.set(vec![
                                format!("⏳ 修复「{}」…", n),
                                format!("✅ 修复完成: {}", msg),
                            ]);
                            repairing.set(false);
                            on_repaired(());
                            return;
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
                    span { class: "text-gray-900 dark:text-white text-sm font-medium", "{name}" }
                    span { class: "text-xs text-gray-500 dark:text-gray-400 ml-2", "（允许 ≤{max_gap}天）" }
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
