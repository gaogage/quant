use dioxus::prelude::*;
use serde_json::Value;

use crate::api;

#[component]
pub fn TasksPage() -> Element {
    let mut tasks = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);
    let mut msg = use_signal(|| String::new());
    let mut err = use_signal(|| String::new());
    let mut editing = use_signal(|| String::new());
    let mut edit_cron = use_signal(|| String::new());
    let mut edit_saving = use_signal(|| false);
    let mut dep_check_result = use_signal(|| Vec::<String>::new());
    let mut dep_checking = use_signal(|| false);

    use_effect(move || {
        spawn(async move {
            if let Ok(v) = api::admin_list_tasks().await {
                if let Some(arr) = v.get("data").and_then(|d| d.as_array()) {
                    tasks.set(arr.clone());
                }
            }
            loading.set(false);
        });
    });

    if *loading.read() {
        return rsx! {
            div { class: "p-6",
                div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" }
            }
        };
    }

    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            h1 { class: "text-2xl font-bold text-gray-900 dark:text-white mb-6", "定时任务" }
            if !msg.read().is_empty() { div { class: "mb-4 p-3 bg-green-50 dark:bg-green-900/50 border border-green-300 dark:border-green-700 rounded-lg text-green-700 dark:text-green-300 text-sm flex justify-between", span { "{msg}" } button { class: "text-green-600 dark:text-green-400", onclick: move |_| msg.set(String::new()), "✕" } } }
            if !err.read().is_empty() { div { class: "mb-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm flex justify-between", span { "{err}" } button { class: "text-red-600 dark:text-red-400", onclick: move |_| err.set(String::new()), "✕" } } }

            div { class: "mb-4 flex gap-3",
                div { class: "flex-1 p-4 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 flex items-center justify-between",
                    div { div { class: "text-gray-900 dark:text-white text-sm font-medium", "钉钉推送" } div { class: "text-xs text-gray-500 dark:text-gray-400 mt-1", "重新发送当前持仓摘要到钉钉" } }
                    button { class: "px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm text-white transition",
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
                div { class: "p-4 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 flex items-center justify-between",
                    div { div { class: "text-gray-900 dark:text-white text-sm font-medium", "依赖检查" } div { class: "text-xs text-gray-500 dark:text-gray-400 mt-1", "检查定时任务 CRON 触发时间顺序" } }
                    button { class: "px-4 py-2 bg-orange-600 hover:bg-orange-500 disabled:bg-gray-300 rounded-lg text-sm text-white transition ml-4",
                        disabled: *dep_checking.read(),
                        onclick: move |_| {
                            dep_checking.set(true);
                            dep_check_result.set(Vec::new());
                            spawn(async move {
                                match api::admin_check_task_deps().await {
                                    Ok(v) => {
                                        if let Some(arr) = v.get("data").and_then(|d| d.as_array()) {
                                            let issues: Vec<String> = arr.iter().filter_map(|s| s.as_str().map(|x| x.to_string())).collect();
                                            dep_check_result.set(issues);
                                        }
                                        if dep_check_result.read().is_empty() {
                                            msg.set("依赖顺序正常".into());
                                        }
                                    }
                                    Err(e) => err.set(e),
                                }
                                dep_checking.set(false);
                            });
                        },
                        if *dep_checking.read() { "检查中…" } else { "检查依赖顺序" }
                    }
                }
            }

            // 依赖检查结果
            if !dep_check_result.read().is_empty() {
                div { class: "mb-4 p-3 bg-orange-50 dark:bg-orange-900/30 border border-orange-300 dark:border-orange-700 rounded-lg",
                    div { class: "text-sm font-medium text-orange-700 dark:text-orange-400 mb-1", "⚠ 定时任务依赖顺序异常" }
                    for issue in dep_check_result.read().iter() {
                        div { class: "text-xs text-orange-600 dark:text-orange-400 ml-2", "• {issue}" }
                    }
                }
            }

            div { class: "space-y-2",
                for t in tasks.read().iter() {
                    TaskCard {
                        data: t.clone(),
                        editing: editing.read().clone(),
                        edit_cron_val: edit_cron.read().clone(),
                        edit_saving: *edit_saving.read(),
                        on_edit: {
                            let name = t.get("task_name").and_then(|v| v.as_str()).unwrap_or("-").to_string();
                            let cron = t.get("schedule_cron").and_then(|v| v.as_str()).unwrap_or("-").to_string();
                            Callback::new(move |_| {
                                if *editing.read() == name { editing.set(String::new()); return; }
                                edit_cron.set(cron.clone());
                                editing.set(name.clone());
                            })
                        },
                        on_cron_change: Callback::new(move |v: String| edit_cron.set(v)),
                        on_toggle: {
                            let name = t.get("task_name").and_then(|v| v.as_str()).unwrap_or("-").to_string();
                            let enabled = t.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                            Callback::new(move |_| {
                                let n = name.clone();
                                let new_enabled = !enabled;
                                spawn(async move {
                                    match api::admin_update_task(&n, Some(new_enabled), None).await {
                                        Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => {
                                            msg.set(format!("{} {}", n, if new_enabled { "已启用" } else { "已禁用" }));
                                            if let Ok(v2) = api::admin_list_tasks().await {
                                                if let Some(arr) = v2.get("data").and_then(|d| d.as_array()) {
                                                    tasks.set(arr.clone());
                                                }
                                            }
                                        }
                                        Ok(v) => err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("失败").into()),
                                        Err(e) => err.set(e),
                                    }
                                });
                            })
                        },
                        on_save: {
                            let name = t.get("task_name").and_then(|v| v.as_str()).unwrap_or("-").to_string();
                            Callback::new(move |_| {
                                let n = name.clone();
                                let c = edit_cron.read().clone();
                                edit_saving.set(true);
                                spawn(async move {
                                    match api::admin_update_task(&n, None, Some(&c)).await {
                                        Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => {
                                            msg.set(format!("{} CRON 已更新", n));
                                            editing.set(String::new());
                                            if let Ok(v2) = api::admin_list_tasks().await {
                                                if let Some(arr) = v2.get("data").and_then(|d| d.as_array()) {
                                                    tasks.set(arr.clone());
                                                }
                                            }
                                        }
                                        Ok(v) => err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("失败").into()),
                                        Err(e) => err.set(e),
                                    }
                                    edit_saving.set(false);
                                });
                            })
                        },
                    }
                }
            }
        }
    }
}

#[component]
fn TaskCard(
    data: Value,
    editing: String,
    edit_cron_val: String,
    edit_saving: bool,
    on_edit: Callback<()>,
    on_toggle: Callback<()>,
    on_save: Callback<()>,
    on_cron_change: Callback<String>,
) -> Element {
    let name = data.get("task_name").and_then(|v| v.as_str()).unwrap_or("-");
    let enabled = data.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let cron = data.get("schedule_cron").and_then(|v| v.as_str()).unwrap_or("-");
    let last_run = data.get("last_run_at").and_then(|v| v.as_str()).unwrap_or("-");
    let count = data.get("run_count").and_then(|v| v.as_i64()).unwrap_or(0);
    let task_type = data.get("task_type").and_then(|v| v.as_str()).unwrap_or("-");
    let is_editing = editing == name;

    rsx! {
        div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden",
            div { class: "p-4 flex items-center justify-between",
                div { class: "flex-1",
                    div { class: "flex items-center gap-2",
                        span { class: "text-gray-900 dark:text-white text-sm font-medium", "{name}" }
                        span { class: "text-xs px-1.5 py-0.5 rounded bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400 font-mono", "{task_type}" }
                    }
                    div { class: "text-xs text-gray-500 dark:text-gray-400 mt-1", "CRON: {cron} · 上次: {last_run} · 次数: {count}" }
                }
                div { class: "flex items-center gap-2",
                    button { class: "text-xs px-3 py-1.5 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg text-gray-600 dark:text-gray-400 transition",
                        onclick: move |_| on_edit(()),
                        if is_editing { "取消" } else { "编辑" }
                    }
                    button {
                        class: if enabled { "text-xs px-3 py-1.5 rounded-lg transition bg-green-100 dark:bg-green-900/50 text-green-700 dark:text-green-400 hover:bg-green-200 dark:hover:bg-green-800" }
                               else { "text-xs px-3 py-1.5 rounded-lg transition bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700" },
                        onclick: move |_| on_toggle(()),
                        if enabled { "已启用" } else { "已禁用" }
                    }
                }
            }
            if is_editing {
                div { class: "border-t border-gray-100 dark:border-gray-800 px-4 py-3 bg-gray-50 dark:bg-gray-800/50 flex items-center gap-3",
                    span { class: "text-xs text-gray-500", "CRON" }
                    input { class: "flex-1 px-3 py-1.5 bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm font-mono",
                        value: "{edit_cron_val}",
                        oninput: move |e| on_cron_change(e.value()),
                    }
                    button { class: "px-4 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-300 dark:disabled:bg-gray-600 disabled:text-gray-500 rounded-lg text-sm text-white transition",
                        disabled: edit_saving,
                        onclick: move |_| on_save(()),
                        if edit_saving { "保存中…" } else { "保存" }
                    }
                }
            }
        }
    }
}
