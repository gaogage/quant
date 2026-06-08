use dioxus::prelude::*;
use serde_json::Value;
use crate::api;

#[component]
pub fn TasksPage() -> Element {
    let mut tasks = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);
    let mut msg = use_signal(|| String::new());
    let mut err = use_signal(|| String::new());
    use_effect(move || { spawn(async move {
        if let Ok(v) = api::admin_list_tasks().await {
            if let Some(arr) = v.get("data").and_then(|d| d.as_array()) { tasks.set(arr.clone()); }
        }
        loading.set(false);
    }); });
    if *loading.read() { return rsx! { div { class: "p-6", div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" } } }; }
    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            h1 { class: "text-2xl font-bold text-white mb-6", "定时任务" }
            if !msg.read().is_empty() { div { class: "mb-4 p-3 bg-green-900/50 border border-green-700 rounded-lg text-green-300 text-sm flex justify-between", span { "{msg}" } button { class: "text-green-400", onclick: move |_| msg.set(String::new()), "✕" } } }
            if !err.read().is_empty() { div { class: "mb-4 p-3 bg-red-900/50 border border-red-700 rounded-lg text-red-300 text-sm flex justify-between", span { "{err}" } button { class: "text-red-400", onclick: move |_| err.set(String::new()), "✕" } } }
            div { class: "mb-4 p-4 bg-gray-900 rounded-xl border border-gray-800 flex items-center justify-between",
                div { div { class: "text-white text-sm font-medium", "钉钉推送" } div { class: "text-xs text-gray-500 mt-1", "重新发送当前持仓摘要到钉钉" } }
                button { class: "px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm transition",
                    onclick: move |_| { spawn(async move {
                        match api::trigger_dingtalk_notify().await {
                            Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => msg.set("推送成功".into()),
                            Ok(v) => err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("失败").into()),
                            Err(e) => err.set(e),
                        }
                    }); },
                    "重新推送"
                }
            }
            div { class: "space-y-2",
                for t in tasks.read().iter() { TaskRow { data: t.clone() } }
            }
        }
    }
}

#[component]
fn TaskRow(data: Value) -> Element {
    let name = data.get("task_name").and_then(|v| v.as_str()).unwrap_or("-");
    let enabled = data.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let cron = data.get("schedule_cron").and_then(|v| v.as_str()).unwrap_or("-");
    let last_run = data.get("last_run_at").and_then(|v| v.as_str()).unwrap_or("-");
    let count = data.get("run_count").and_then(|v| v.as_i64()).unwrap_or(0);
    rsx! {
        div { class: "bg-gray-900 rounded-xl border border-gray-800 p-4 flex items-center justify-between",
            div { span { class: "text-white text-sm font-medium", "{name}" } div { class: "text-xs text-gray-500 mt-1", "Cron: {cron} · 上次: {last_run} · 次数: {count}" } }
            if enabled {
                span { class: "text-xs px-2 py-0.5 rounded-full bg-green-900/50 text-green-400", "已启用" }
            } else {
                span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-800 text-gray-500", "已禁用" }
            }
        }
    }
}
