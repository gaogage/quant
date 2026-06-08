//! 策略管理页面 — 查看系统/用户策略，派生新策略

use dioxus::prelude::*;
use serde_json::{json, Value};

use crate::api;

#[component]
pub fn StrategiesContent() -> Element {
    let mut strategies = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);
    let mut error = use_signal(|| String::new());
    let mut selected_id = use_signal(|| String::new());

    use_effect(move || {
        spawn(async move {
            match api::list_strategies().await {
                Ok(v) => {
                    if let Some(arr) = v["data"].as_array() {
                        strategies.set(arr.clone());
                    }
                }
                Err(e) => error.set(e),
            }
            loading.set(false);
        });
    });

    if *loading.read() {
        return rsx! {
            div { class: "p-6 max-w-6xl mx-auto",
                div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" }
            }
        };
    }

    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            div { class: "flex items-center justify-between mb-6",
                h1 { class: "text-2xl font-bold text-gray-900 dark:text-white", "策略管理" }
            }

            if !error.read().is_empty() {
                div { class: "mb-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm",
                    "{error}"
                }
            }

            div { class: "space-y-3",
                for s in strategies.read().iter() {
                    StrategyCard {
                        data: s.clone(),
                        is_selected: *selected_id.read() == s["strategy_id"].as_str().unwrap_or(""),
                        on_select: {
                            let sid = s["strategy_id"].as_str().unwrap_or("").to_string();
                            Callback::new(move |_| {
                                if *selected_id.read() == sid {
                                    selected_id.set(String::new());
                                } else {
                                    selected_id.set(sid.clone());
                                }
                            })
                        },
                        on_derive: {
                            let sid = s["strategy_id"].as_str().unwrap_or("").to_string();
                            let name = s["name"].as_str().unwrap_or("").to_string();
                            let mut params_clone = s["params"].clone();
                            if let Some(obj) = params_clone.as_object_mut() {
                                obj.insert("derived_by".to_string(), json!("user"));
                            }
                            Callback::new(move |_| {
                                let sid = sid.clone();
                                let name = name.clone();
                                let params = params_clone.clone();
                                spawn(async move {
                                    let derived_name = format!("{}-派生", name);
                                    match api::create_strategy(&sid, &derived_name, "", &params).await {
                                        Ok(_) => { selected_id.set(String::new()); }
                                        Err(e) => error.set(e),
                                    }
                                });
                            })
                        },
                        on_delete: {
                            let sid = s["strategy_id"].as_str().unwrap_or("").to_string();
                            Callback::new(move |_| {
                                let sid = sid.clone();
                                spawn(async move {
                                    match api::delete_strategy(&sid).await {
                                        Ok(_) => {
                                            selected_id.set(String::new());
                                            if let Ok(v) = api::list_strategies().await {
                                                if let Some(arr) = v["data"].as_array() {
                                                    strategies.set(arr.clone());
                                                }
                                            }
                                        }
                                        Err(e) => error.set(e),
                                    }
                                });
                            })
                        },
                    }
                }
            }
        }
    }
}

// ── 策略卡片子组件 ──────────────────────────────────────

#[component]
fn StrategyCard(
    data: Value,
    is_selected: bool,
    on_select: Callback<()>,
    on_derive: Callback<()>,
    on_delete: Callback<()>,
) -> Element {
    let name = data["name"].as_str().unwrap_or("-").to_string();
    let owner = data["owner"].as_str().unwrap_or("-").to_string();
    let status = data["status"].as_str().unwrap_or("-").to_string();
    let desc = data["description"].as_str().unwrap_or("").to_string();
    let params = data["params"].clone();
    let is_system = owner == "system";
    let is_mine = owner == "me";

    rsx! {
        div {
            class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-5 cursor-pointer hover:border-blue-500 transition",
            onclick: move |_| on_select(()),
            div { class: "flex items-center justify-between mb-2",
                div { class: "flex items-center gap-3",
                    h3 { class: "font-semibold text-gray-900 dark:text-white", "{name}" }
                    span { class: "text-xs px-2 py-0.5 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300",
                        if is_system { "系统" } else if is_mine { "我的" } else { "共享" }
                    }
                }
                span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400",
                    "{status}"
                }
            }
            if !desc.is_empty() {
                p { class: "text-sm text-gray-500 dark:text-gray-400 mb-2", "{desc}" }
            }
            div { class: "text-xs text-gray-400 dark:text-gray-600 font-mono truncate",
                "{params.to_string()}"
            }
        }

        // 展开详情
        if is_selected {
            div { class: "bg-gray-50 dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 mb-3",
                h4 { class: "text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3", "策略参数" }
                pre { class: "text-xs text-gray-600 dark:text-gray-400 bg-gray-100 dark:bg-gray-950 p-3 rounded-lg overflow-x-auto max-h-64",
                    "{serde_json::to_string_pretty(&params).unwrap_or_default()}"
                }
                if is_system {
                    button {
                        class: "mt-3 text-sm px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-white transition",
                        onclick: move |_| on_derive(()),
                        "派生此策略"
                    }
                }
                if is_mine {
                    button {
                        class: "mt-3 ml-2 text-sm px-4 py-2 bg-red-600 hover:bg-red-500 rounded-lg text-white transition",
                        onclick: move |_| on_delete(()),
                        "删除"
                    }
                }
            }
        }
    }
}
