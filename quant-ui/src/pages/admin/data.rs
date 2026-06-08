use dioxus::prelude::*;
use serde_json::Value;
use crate::api;

#[component]
pub fn DataSyncPage() -> Element {
    let mut sync = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);
    use_effect(move || { spawn(async move {
        if let Ok(v) = api::admin_sync_status().await {
            if let Some(arr) = v.get("data").and_then(|d| d.as_array()) { sync.set(arr.clone()); }
        }
        loading.set(false);
    }); });
    if *loading.read() { return rsx! { div { class: "p-6", div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" } } }; }
    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            h1 { class: "text-2xl font-bold text-gray-900 dark:text-white mb-6", "数据同步" }
            div { class: "space-y-2",
                for s in sync.read().iter() {
                    DataSyncRow { data: s.clone() }
                }
            }
        }
    }
}

#[component]
fn DataSyncRow(data: Value) -> Element {
    let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("-");
    let healthy = data.get("healthy").and_then(|v| v.as_bool()).unwrap_or(false);
    let gap = data.get("current_gap_days").and_then(|v| v.as_i64()).unwrap_or(0);
    let max_gap = data.get("max_gap_days").and_then(|v| v.as_i64()).unwrap_or(0);
    rsx! {
        div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-4 flex items-center justify-between",
            div { span { class: "text-gray-900 dark:text-white text-sm", "{name}" } span { class: "text-xs text-gray-500 dark:text-gray-400 ml-2", "（允许 ≤{max_gap}天）" } }
            div { class: "flex items-center gap-3",
                span { class: "text-sm text-gray-500 dark:text-gray-400", "间隔: {gap}天" }
                if healthy {
                    span { class: "text-xs px-2 py-0.5 rounded-full bg-green-100 dark:bg-green-900/50 text-green-700 dark:text-green-400", "正常" }
                } else {
                    span { class: "text-xs px-2 py-0.5 rounded-full bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-400", "异常" }
                }
            }
        }
    }
}
