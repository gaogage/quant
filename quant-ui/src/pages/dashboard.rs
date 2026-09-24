//! 仪表盘页面 — 用户概览 + 策略/账号摘要

use dioxus::prelude::*;
use serde_json::Value;

use crate::api;
use crate::auth::AuthState;
use crate::components::charts::NavComparisonChart;
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

            // 统计卡片（2026-09-24：账号数=活跃口径——54 全量含 50 历史研究账号，
            // 驾驶舱要当前生产规模而非台账规模）
            div { class: "grid grid-cols-3 gap-4 mb-8",
                {
                    let active_cnt = accounts.read().iter().filter(|a| a["status"].as_str() == Some("active")).count();
                    let total_cnt = accounts.read().len();
                    rsx! {
                        div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-5",
                            div { class: "text-3xl font-bold text-blue-600 dark:text-blue-400", "{active_cnt}" }
                            div { class: "text-sm text-gray-500 dark:text-gray-400 mt-1", "活跃投资账号（共 {total_cnt}）" }
                        }
                    }
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

            // P3-1: v24 实盘绩效 Dashboard（NAV 曲线 + 关键指标）
            V24PerformanceSection {
                accounts: accounts.read().clone(),
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
                ActiveAccountList { accounts: accounts.read().clone() }
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
                StrategyListGroups { strategies: strategies.read().clone() }
            }
        }
    }
}

// ── 子组件 ──────────────────────────────────────────────

/// 活跃账号列表（2026-09-24 仪表盘优化）：驾驶舱只展示 active 账号（生产状态），
/// 停用的历史研究账号（50+）归账号台账页——信号不被噪声淹没。
#[component]
fn ActiveAccountList(accounts: Vec<Value>) -> Element {
    let nav = use_navigator();
    if accounts.is_empty() {
        return rsx! {
            div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-6 text-center text-gray-400 dark:text-gray-500",
                "暂无投资账号，点击上方按钮创建"
            }
        };
    }
    let active: Vec<&Value> = accounts.iter().filter(|a| a["status"].as_str() == Some("active")).collect();
    let inactive_cnt = accounts.len() - active.len();
    rsx! {
        for acc in active {
            AccountCard { data: acc.clone() }
        }
        if inactive_cnt > 0 {
            button {
                class: "w-full text-sm text-gray-400 dark:text-gray-500 hover:text-blue-500 dark:hover:text-blue-400 py-3 text-center border border-dashed border-gray-200 dark:border-gray-800 rounded-xl transition",
                onclick: move |_| { let _ = nav.push(Route::AccountsPage {}); },
                "另有 {inactive_cnt} 个停用账号（历史研究）——进入账号管理查看"
            }
        }
    }
}

/// 策略分组列表（2026-09-24 仪表盘优化）：sleeve 载体（曲线引用的内部结构，
/// 非独立交易策略）折叠为独立组——语义不混淆。
#[component]
fn StrategyListGroups(strategies: Vec<Value>) -> Element {
    if strategies.is_empty() {
        return rsx! {
            div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-6 text-center text-gray-400 dark:text-gray-500",
                "暂无可用策略"
            }
        };
    }
    let is_sleeve = |v: &Value| {
        let n = v["name"].as_str().unwrap_or("");
        n.contains("sleeve") || n.contains("载体")
    };
    let prod: Vec<&Value> = strategies.iter().filter(|v| !is_sleeve(v)).collect();
    let sleeves: Vec<&Value> = strategies.iter().filter(|v| is_sleeve(v)).collect();
    rsx! {
        div { class: "space-y-4",
            div { class: "grid grid-cols-2 gap-3",
                for strat in prod {
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
            if !sleeves.is_empty() {
                details {
                    class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-4",
                    summary { class: "text-sm text-gray-500 dark:text-gray-400 cursor-pointer select-none",
                        "资产 Sleeve 载体（{sleeves.len()}）——曲线引用的内部结构，非独立交易策略"
                    }
                    div { class: "grid grid-cols-2 gap-3 mt-3",
                        for strat in sleeves {
                            {
                                let name = strat["name"].as_str().unwrap_or("-");
                                let owner = strat["owner"].as_str().unwrap_or("-");
                                rsx! {
                                    div { class: "border border-dashed border-gray-200 dark:border-gray-800 rounded-lg p-3",
                                        div { class: "text-xs font-medium text-gray-600 dark:text-gray-400", "{name}" }
                                        div { class: "text-xs text-gray-400 dark:text-gray-500 mt-1", "{owner}" }
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
fn AccountCard(data: Value) -> Element {
    let name = data["name"].as_str().unwrap_or("-");
    let acc_type = data["account_type"].as_str().unwrap_or("-");
    let strategy = data["strategy_version_id"].as_str().filter(|s| !s.is_empty()).unwrap_or("-");
    // 2026-09-09: 卡片展示当前净值(原显示初始资金 100 万,与账户状态脱节)
    let cap = data["current_nav"]
        .as_f64()
        .filter(|v| *v > 0.0)
        .unwrap_or_else(|| data["initial_capital"].as_f64().unwrap_or(0.0)) as i64;
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
                                    let scope_raw = blocker["scope"].as_str().unwrap_or("-");
                                    let scope = match scope_raw { "professional" => "专业档", "elite" => "精英档", other => other };
                                    let name = blocker["name"].as_str().or_else(|| blocker["metric"].as_str()).unwrap_or("-");
                                    let reason_raw = blocker["reason"].as_str().unwrap_or("-");
                                    // 2026-09-24: 阻断原因中文化（原文标识符对非工程读者不可读）
                                    let reason = match reason_raw {
                                        "missing_metric" => "指标缺失",
                                        "below_gate" => "低于门槛",
                                        other => other,
                                    };
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
                    // 2026-09-24: 明细默认折叠（状态灯一眼可见，治理明细按需展开——
                    // 明细属于数据治理页职责，驾驶舱只留态势）
                    details {
                        summary { class: "text-xs text-gray-400 dark:text-gray-500 cursor-pointer select-none py-1", "前 {top_tables.len()} 大可治理表" }
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

/// P3-1: v24 实盘绩效面板 — 从账号列表中选第一个挂了策略的活跃模拟账号，
/// 加载其 NAV 历史(paper_nav_snapshot) + 回测同期对比曲线。
#[component]
fn V24PerformanceSection(accounts: Vec<Value>) -> Element {
    // 选取第一个 active 模拟盘且挂了策略的账号作为展示对象
    let target = accounts.iter().find(|a| {
        a["status"].as_str() == Some("active")
            && a["account_type"].as_str() == Some("simulated")
            && a["strategy_version_id"].as_str().map(|s| !s.is_empty()).unwrap_or(false)
    }).cloned();

    let Some(acc) = target else {
        return rsx! { div {} };
    };
    let account_id = acc["account_id"].as_str().unwrap_or("").to_string();
    let acc_name = acc["name"].as_str().unwrap_or("-").to_string();
    if account_id.is_empty() {
        return rsx! { div {} };
    }

    let mut nav_history = use_signal(|| Value::Null);
    let mut nh_loading = use_signal(|| true);
    let aid_for_effect = account_id.clone();

    use_effect(move || {
        let aid = aid_for_effect.clone();
        spawn(async move {
            if let Ok(v) = api::get_nav_history(&aid).await {
                nav_history.set(v["data"].clone());
            }
            nh_loading.set(false);
        });
    });

    if *nh_loading.read() {
        return rsx! {
            div { class: "mb-8 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg p-5",
                div { class: "animate-spin h-6 w-6 border-4 border-blue-500 border-t-transparent rounded-full" }
            }
        };
    }

    let data = nav_history.read();
    let nav_points: Vec<Value> = data["nav_history"].as_array().cloned().unwrap_or_default();
    if nav_points.len() < 2 {
        return rsx! { div {} };
    }
    let bt_points: Vec<Value> = data["backtest_comparison"].as_array().cloned().unwrap_or_default();

    let dates: Vec<String> = nav_points.iter().map(|p| p["date"].as_str().unwrap_or("").to_string()).collect();
    let live_ret: Vec<f64> = nav_points.iter().map(|p| p["cumulative_return"].as_f64().unwrap_or(0.0)).collect();
    // hover tooltip 数据：当日净值与当日收益
    let live_nav: Vec<f64> = nav_points.iter().map(|p| p["nav"].as_f64().unwrap_or(0.0)).collect();
    let live_daily: Vec<f64> = nav_points.iter().map(|p| p["daily_return"].as_f64().unwrap_or(0.0)).collect();
    let bt_dates: Vec<String> = bt_points.iter().map(|p| p["date"].as_str().unwrap_or("").to_string()).collect();
    let bt_ret: Vec<f64> = bt_points.iter().map(|p| p["cumulative_return"].as_f64().unwrap_or(0.0)).collect();

    let last = nav_points.last().cloned().unwrap_or(Value::Null);
    let cur_nav = last["nav"].as_f64().unwrap_or(0.0);
    let cur_cum_ret = last["cumulative_return"].as_f64().unwrap_or(0.0);
    // 2026-09-09: 快照 max_drawdown 列长期无人写入(恒 0),改为 NAV 序列现场计算峰值回撤
    let cur_mdd = {
        let mut peak = 0.0_f64;
        let mut mdd = 0.0_f64;
        for p in &nav_points {
            let nav = p["nav"].as_f64().unwrap_or(0.0);
            if nav > peak { peak = nav; }
            if peak > 0.0 {
                let dd = nav / peak - 1.0;
                if dd < mdd { mdd = dd; }
            }
        }
        mdd * 100.0
    };
    let sharpe = acc["sharpe_ratio"].as_f64().unwrap_or(0.0);

    // 与回测偏离(2026-09-09 修正口径): 原实现拿"实盘自初始资金累计收益"减
    // "回测对比曲线累计",两者锚点不同(346% vs ~68% 出 +278% 假偏离)。
    // 改为两条曲线各自从自身首点归一(relative 口径)后取末点差——同锚可比。
    let norm_last = |series: &[f64]| -> f64 {
        if series.len() < 2 { return 0.0; }
        let base = series[0];
        if base.is_finite() && (base - 1.0).abs() > f64::EPSILON {
            series.last().copied().unwrap_or(0.0) - base
        } else {
            series.last().copied().unwrap_or(0.0)
        }
    };
    let live_rel: Vec<f64> = nav_points.iter().map(|p| p["relative_return"].as_f64().unwrap_or(0.0)).collect();
    let deviation = norm_last(&live_rel) - norm_last(&bt_ret);
    let dev_cls = if deviation.abs() > 2.0 { "text-red-600 dark:text-red-400" } else { "text-gray-500 dark:text-gray-400" };
    // 口径: bt 曲线是 MVO 动态基准(每日重算,非实盘 fixed 配置),差值反映
    // "fixed DEF10 vs MVO 动态"的策略间差异,不是执行偏离。

    rsx! {
        section { class: "mb-8 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg p-5",
            div { class: "flex items-center justify-between mb-4",
                h2 { class: "text-lg font-semibold text-gray-900 dark:text-white", "v24 实盘绩效 — {acc_name}" }
                if !bt_points.is_empty() {
                    span { class: "text-xs {dev_cls}", "vs MVO动态基准 {deviation:+.2}%" }
                }
            }

            div { class: "grid grid-cols-4 gap-3 mb-5",
                MetricBox { label: "当前净值".to_string(), value: format!("¥{:.0}", cur_nav) }
                MetricBox { label: "累计收益".to_string(), value: format!("{:+.2}%", cur_cum_ret) }
                MetricBox { label: "最大回撤".to_string(), value: format!("{:.2}%", cur_mdd) }
                MetricBox { label: "Sharpe".to_string(), value: format!("{:.2}", sharpe) }
            }

            NavComparisonChart {
                dates: dates,
                live_ret: live_ret,
                live_nav: live_nav,
                live_daily: live_daily,
                bt_dates: bt_dates,
                bt_ret: bt_ret,
                canvas_id: "v24-nav-chart".to_string(),
            }
        }
    }
}
