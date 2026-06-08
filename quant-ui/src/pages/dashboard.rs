//! 仪表盘页面 — 用户概览 + 策略/账号摘要

use dioxus::prelude::*;
use dioxus_router::prelude::*;
use serde_json::Value;

use crate::api;
use crate::auth::AuthState;
use crate::Route;

#[component]
pub fn DashboardContent() -> Element {
    let nav = use_navigator();
    let user = AuthState::user();

    let mut accounts = use_signal(|| Vec::<Value>::new());
    let mut strategies = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);
    let mut error = use_signal(|| String::new());

    // 加载数据
    use_effect(move || {
        spawn(async move {
            let acc_res = api::list_accounts("").await;
            let strat_res = api::list_strategies().await;

            let mut accs = Vec::new();
            let mut strats = Vec::new();
            let mut err = String::new();

            match acc_res {
                Ok(v) => {
                    if let Some(arr) = v["data"].as_array() {
                        accs = arr.clone();
                    }
                }
                Err(e) => err = e,
            }

            match strat_res {
                Ok(v) => {
                    if let Some(arr) = v["data"].as_array() {
                        strats = arr.clone();
                    }
                }
                Err(e) => if err.is_empty() { err = e; }
            }

            accounts.set(accs);
            strategies.set(strats);
            error.set(err);
            loading.set(false);
        });
    });

    let display_name = user.as_ref()
        .and_then(|u| u.display_name.as_deref())
        .unwrap_or_else(|| user.as_ref().map(|u| u.username.as_str()).unwrap_or("用户"));

    // 加载中
    if *loading.read() {
        return rsx! {
            div { class: "p-6 max-w-6xl mx-auto",
                div { class: "flex items-center justify-center py-12",
                    div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" }
                }
            }
        };
    }

    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            // 欢迎区
            div { class: "mb-8",
                h1 { class: "text-3xl font-bold text-gray-900 dark:text-white mb-2", "你好, {display_name}" }
                p { class: "text-gray-500 dark:text-gray-400",
                    if user.as_ref().map(|u| u.role == "admin").unwrap_or(false) {
                        "系统管理员"
                    } else {
                        "量化交易用户"
                    }
                }
            }

            // 错误提示
            if !error.read().is_empty() {
                div { class: "mb-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm",
                    "{error}"
                }
            }

            // 统计卡片
            div { class: "grid grid-cols-3 gap-4 mb-8",
                div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-5",
                    div { class: "text-3xl font-bold text-blue-600 dark:text-blue-400", "{accounts.read().len()}" }
                    div { class: "text-sm text-gray-500 dark:text-gray-400 mt-1", "投资账号" }
                }
                div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-5",
                    div { class: "text-3xl font-bold text-green-600 dark:text-green-400", "{strategies.read().len()}" }
                    div { class: "text-sm text-gray-500 dark:text-gray-400 mt-1", "可用策略" }
                }
                div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-5",
                    div { class: "text-3xl font-bold text-yellow-600 dark:text-yellow-400",
                        if user.as_ref().map(|u| u.role == "admin").unwrap_or(false) { "管理员" } else { "用户" }
                    }
                    div { class: "text-sm text-gray-500 dark:text-gray-400 mt-1", "权限级别" }
                }
            }

            // 账号列表
            div { class: "mb-6",
                div { class: "flex items-center justify-between mb-3",
                    h2 { class: "text-lg font-semibold text-gray-900 dark:text-white", "投资账号" }
                    button {
                        class: "text-sm px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded-lg text-white transition",
                        onclick: move |_| { let _ = nav.push(Route::AccountsPage {}); },
                        "管理账号"
                    }
                }
                if accounts.read().is_empty() {
                    div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-6 text-center text-gray-400 dark:text-gray-500",
                        "暂无投资账号，点击上方按钮创建"
                    }
                } else {
                    for acc in accounts.read().iter() {
                        AccountCard { data: acc.clone() }
                    }
                }
            }

            // 策略列表
            div {
                div { class: "flex items-center justify-between mb-3",
                    h2 { class: "text-lg font-semibold text-gray-900 dark:text-white", "交易策略" }
                    button {
                        class: "text-sm px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded-lg text-white transition",
                        onclick: move |_| { let _ = nav.push(Route::StrategiesPage {}); },
                        "查看策略"
                    }
                }
                if strategies.read().is_empty() {
                    div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-6 text-center text-gray-400 dark:text-gray-500",
                        "暂无可用策略"
                    }
                } else {
                    div { class: "grid grid-cols-2 gap-3",
                        for strat in strategies.read().iter() {
                            {
                                let name = strat["name"].as_str().unwrap_or("-");
                                let owner = strat["owner"].as_str().unwrap_or("-");
                                let status = strat["status"].as_str().unwrap_or("-");
                                rsx! {
                                    div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-4",
                                        div { class: "font-medium text-gray-900 dark:text-white text-sm", "{name}" }
                                        div { class: "text-xs text-gray-500 dark:text-gray-400 mt-1", "{owner}" }
                                        span { class: "inline-block mt-2 text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400",
                                            "{status}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// ── 子组件 ──────────────────────────────────────────────

#[component]
fn AccountCard(data: Value) -> Element {
    let name = data["name"].as_str().unwrap_or("-");
    let acc_type = data["account_type"].as_str().unwrap_or("-");
    let signal = data["signal_source"].as_str().unwrap_or("-");
    let cap = data["initial_capital"].as_f64().unwrap_or(0.0) as i64;
    let status = data["status"].as_str().unwrap_or("-");
    rsx! {
        div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-4 flex items-center justify-between",
            div {
                div { class: "font-medium text-gray-900 dark:text-white text-sm", "{name}" }
                div { class: "text-xs text-gray-500 dark:text-gray-400 mt-1", "{acc_type} · {signal}" }
            }
            div { class: "text-right",
                div { class: "text-sm text-gray-700 dark:text-gray-300", "¥{cap}" }
                span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400", "{status}" }
            }
        }
    }
}
