//! 仪表盘页面 — 用户概览 + 策略/账号摘要

use dioxus::prelude::*;
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
    let mut blueprint = use_signal(|| Value::Null);
    let mut loading = use_signal(|| true);
    let mut error = use_signal(|| String::new());

    // 加载数据
    use_effect(move || {
        spawn(async move {
            let acc_res = api::list_accounts("").await;
            let strat_res = api::list_strategies().await;
            let blueprint_res = api::blueprint_progress().await;

            let mut accs = Vec::new();
            let mut strats = Vec::new();
            let mut blueprint_data = Value::Null;
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

            match blueprint_res {
                Ok(v) => {
                    blueprint_data = v["data"].clone();
                }
                Err(e) => if err.is_empty() { err = e; }
            }

            accounts.set(accs);
            strategies.set(strats);
            blueprint.set(blueprint_data);
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

            if !blueprint.read().is_null() {
                BlueprintProgressPanel { data: blueprint.read().clone() }
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
    let strategy = data["strategy_version_id"].as_str().filter(|s| !s.is_empty()).unwrap_or("-");
    let cap = data["initial_capital"].as_f64().unwrap_or(0.0) as i64;
    let status = data["status"].as_str().unwrap_or("-");
    rsx! {
        div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-4 flex items-center justify-between",
            div {
                div { class: "font-medium text-gray-900 dark:text-white text-sm", "{name}" }
                div { class: "text-xs text-gray-500 dark:text-gray-400 mt-1", "{acc_type} · {strategy}" }
            }
            div { class: "text-right",
                div { class: "text-sm text-gray-700 dark:text-gray-300", "¥{cap}" }
                span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400", "{status}" }
            }
        }
    }
}

#[component]
fn BlueprintProgressPanel(data: Value) -> Element {
    let progress = &data["progress"];
    let professional = &data["professional"];
    let elite = &data["elite"];
    let storage = &data["storage"];

    let overall_pct = progress["overall_progress_pct"].as_f64().unwrap_or(0.0);
    let professional_pct = progress["professional_metric_progress_pct"].as_f64().unwrap_or(0.0);
    let elite_pct = progress["elite_metric_progress_pct"].as_f64().unwrap_or(0.0);
    let system_pct = progress["system_build_progress_pct"].as_f64().unwrap_or(0.0);
    let current_phase = progress["current_phase"].as_str().unwrap_or("-");
    let pro_pass = professional["hard_gate_passed"].as_bool().unwrap_or(false);
    let elite_pass = elite["hard_gate_passed"].as_bool().unwrap_or(false);
    let storage_level = storage["pressure_level"].as_str().unwrap_or("unknown");
    let storage_size = storage["top_cleanable_size_pretty"].as_str().unwrap_or("-");
    let selected_name = data["selected_account"]["name"].as_str().unwrap_or("-");
    let selected_annual = data["selected_account"]["annual_return_pct"].as_f64().unwrap_or(0.0);
    let selected_sharpe = data["selected_account"]["sharpe_ratio"].as_f64().unwrap_or(0.0);
    let selected_sortino = data["selected_account"]["sortino_ratio"].as_f64().unwrap_or(0.0);
    let selected_drawdown = data["selected_account"]["max_drawdown_pct"].as_f64().unwrap_or(0.0);
    let blockers: Vec<Value> = data["blockers"]
        .as_array()
        .map(|items| items.iter().take(6).cloned().collect())
        .unwrap_or_default();
    let top_tables: Vec<Value> = storage["top_tables"]
        .as_array()
        .map(|items| items.iter().take(4).cloned().collect())
        .unwrap_or_default();
    let pro_badge = if pro_pass { "通过" } else { "未通过" };
    let elite_badge = if elite_pass { "通过" } else { "未通过" };
    let storage_cls = match storage_level {
        "red" => "text-red-600 dark:text-red-400",
        "yellow" => "text-yellow-600 dark:text-yellow-400",
        _ => "text-green-600 dark:text-green-400",
    };

    rsx! {
        section { class: "mb-8 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg p-5",
            div { class: "flex items-start justify-between gap-4 mb-5",
                div {
                    h2 { class: "text-lg font-semibold text-gray-900 dark:text-white", "蓝图进度" }
                    div { class: "text-sm text-gray-500 dark:text-gray-400 mt-1", "{current_phase}" }
                }
                div { class: "text-right",
                    div { class: "text-2xl font-bold text-gray-900 dark:text-white", "{overall_pct:.1}%" }
                    div { class: "text-xs text-gray-500 dark:text-gray-400", "综合进度" }
                }
            }

            div { class: "grid grid-cols-3 gap-4 mb-5",
                ProgressTile { title: "工程成熟度".to_string(), value: system_pct, status: "门禁/调度/审计".to_string() }
                ProgressTile { title: "专业目标".to_string(), value: professional_pct, status: pro_badge.to_string() }
                ProgressTile { title: "精英目标".to_string(), value: elite_pct, status: elite_badge.to_string() }
            }

            div { class: "grid grid-cols-4 gap-3 mb-5",
                MetricBox { label: "当前样本".to_string(), value: selected_name.to_string() }
                MetricBox { label: "年化".to_string(), value: format!("{selected_annual:.2}%") }
                MetricBox { label: "Sharpe / Sortino".to_string(), value: format!("{selected_sharpe:.2} / {selected_sortino:.2}") }
                MetricBox { label: "最大回撤".to_string(), value: format!("{selected_drawdown:.2}%") }
            }

            div { class: "grid grid-cols-2 gap-5",
                div {
                    div { class: "text-sm font-medium text-gray-900 dark:text-white mb-2", "阻断项" }
                    if blockers.is_empty() {
                        div { class: "text-sm text-green-600 dark:text-green-400", "暂无阻断项" }
                    } else {
                        div { class: "space-y-2",
                            for blocker in blockers {
                                {
                                    let scope = blocker["scope"].as_str().unwrap_or("-");
                                    let name = blocker["name"].as_str().or_else(|| blocker["metric"].as_str()).unwrap_or("-");
                                    let reason = blocker["reason"].as_str().unwrap_or("-");
                                    rsx! {
                                        div { class: "flex items-center justify-between gap-3 text-sm border border-gray-100 dark:border-gray-800 rounded-md px-3 py-2",
                                            span { class: "text-gray-700 dark:text-gray-300 truncate", "{scope} · {name}" }
                                            span { class: "text-xs text-red-600 dark:text-red-400 whitespace-nowrap", "{reason}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                div {
                    div { class: "flex items-center justify-between mb-2",
                        div { class: "text-sm font-medium text-gray-900 dark:text-white", "存储压力" }
                        div { class: "text-sm {storage_cls}", "{storage_level} · 可治理 {storage_size}" }
                    }
                    div { class: "space-y-2",
                        for table in top_tables {
                            {
                                let name = table["table"].as_str().unwrap_or("-");
                                let size = table["size_pretty"].as_str().unwrap_or("-");
                                let category = table["category"].as_str().unwrap_or("-");
                                rsx! {
                                    div { class: "flex items-center justify-between gap-3 text-sm border border-gray-100 dark:border-gray-800 rounded-md px-3 py-2",
                                        span { class: "text-gray-700 dark:text-gray-300 truncate", "{name}" }
                                        span { class: "text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap", "{category} · {size}" }
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

#[component]
fn ProgressTile(title: String, value: f64, status: String) -> Element {
    let width = format!("width: {:.2}%;", value.clamp(0.0, 100.0));
    rsx! {
        div { class: "border border-gray-100 dark:border-gray-800 rounded-md p-3",
            div { class: "flex items-center justify-between mb-2",
                div { class: "text-sm font-medium text-gray-900 dark:text-white", "{title}" }
                div { class: "text-sm font-semibold text-blue-600 dark:text-blue-400", "{value:.1}%" }
            }
            div { class: "h-2 bg-gray-100 dark:bg-gray-800 rounded-full overflow-hidden",
                div { class: "h-full bg-blue-600 dark:bg-blue-400 rounded-full", style: "{width}" }
            }
            div { class: "text-xs text-gray-500 dark:text-gray-400 mt-2", "{status}" }
        }
    }
}

#[component]
fn MetricBox(label: String, value: String) -> Element {
    rsx! {
        div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-md p-3 min-w-0",
            div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "{label}" }
            div { class: "text-sm font-medium text-gray-900 dark:text-white truncate", "{value}" }
        }
    }
}
