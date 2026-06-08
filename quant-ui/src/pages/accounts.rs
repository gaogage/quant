//! 账号管理页面 — 可过滤、可展开详情(绩效+持仓+交易)、可停用

use dioxus::prelude::*;
use serde_json::Value;

use crate::api;

#[component]
pub fn AccountsContent() -> Element {
    let mut accounts = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);
    let mut error = use_signal(|| String::new());
    let mut message = use_signal(|| String::new());
    let mut f_name = use_signal(|| String::new());
    let mut f_leverage = use_signal(|| String::from("all"));
    let mut f_signal = use_signal(|| String::from("all"));
    let mut f_lev_min = use_signal(|| String::new());
    let mut f_lev_max = use_signal(|| String::new());
    let mut f_status = use_signal(|| String::from("all"));
    let mut expanded = use_signal(|| String::new());
    let mut detail = use_signal(|| Option::<Value>::None);
    let mut detail_loading = use_signal(|| false);

    let mut load = move || {
        loading.set(true);
        let mut params: Vec<String> = Vec::new();
        let n: String = f_name.read().clone(); if !n.is_empty() { params.push(format!("name={}", n)); }
        let l: String = f_leverage.read().clone(); if l != "all" { params.push(format!("leverage={}", l)); }
        let s: String = f_signal.read().clone(); if s != "all" { params.push(format!("signal_source={}", s)); }
        let lmin: String = f_lev_min.read().clone(); if !lmin.is_empty() { params.push(format!("lev_mult_min={}", lmin)); }
        let lmax: String = f_lev_max.read().clone(); if !lmax.is_empty() { params.push(format!("lev_mult_max={}", lmax)); }
        let st: String = f_status.read().clone(); if st != "all" { params.push(format!("status={}", st)); }
        spawn(async move {
            match api::list_accounts(&params.join("&")).await {
                Ok(v) => { if let Some(arr) = v["data"].as_array() { accounts.set(arr.clone()); } }
                Err(e) => error.set(e),
            }
            loading.set(false);
        });
    };

    use_effect(move || { load(); });

    if *loading.read() {
        return rsx! { div { class: "p-6", div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" } } };
    }

    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            div { class: "flex items-center justify-between mb-4",
                h1 { class: "text-2xl font-bold text-gray-900 dark:text-white", "投资账号" }
                button { class: "px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm text-white transition", onclick: move |_| load(), "查询" }
            }
            if !message.read().is_empty() { div { class: "mb-4 p-3 bg-green-50 dark:bg-green-900/50 border border-green-300 dark:border-green-700 rounded-lg text-green-700 dark:text-green-300 text-sm flex justify-between", span { "{message}" } button { class: "text-green-600 dark:text-green-400", onclick: move |_| message.set(String::new()), "✕" } } }
            if !error.read().is_empty() { div { class: "mb-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm flex justify-between", span { "{error}" } button { class: "text-red-600 dark:text-red-400", onclick: move |_| error.set(String::new()), "✕" } } }
            // 过滤器
            div { class: "mb-4 flex gap-3 flex-wrap items-end",
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "名称" } input { class: "w-36 px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm", value: "{f_name}", oninput: move |e| f_name.set(e.value()) } }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "杠杆" } select { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm", value: "{f_leverage}", onchange: move |e| f_leverage.set(e.value()), option { value: "all", "全部" } option { value: "enabled", "已启用" } option { value: "disabled", "未启用" } } }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "信号源" } select { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm", value: "{f_signal}", onchange: move |e| f_signal.set(e.value()), option { value: "all", "全部" } option { value: "factor", "因子" } option { value: "prediction", "ML预测" } option { value: "prediction_blend", "ML混合" } } }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "倍率从" } input { class: "w-16 px-2 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm text-center", value: "{f_lev_min}", oninput: move |e| f_lev_min.set(e.value()) } }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "到" } input { class: "w-16 px-2 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm text-center", value: "{f_lev_max}", oninput: move |e| f_lev_max.set(e.value()) } }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "状态" } select { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm", value: "{f_status}", onchange: move |e| f_status.set(e.value()), option { value: "all", "全部" } option { value: "active", "活跃" } option { value: "inactive", "已停用" } } }
            }
            if accounts.read().is_empty() {
                div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-8 text-center text-gray-400 dark:text-gray-500", "无匹配账号" }
            } else {
                div { class: "space-y-4",
                    for acc in accounts.read().iter() {
                        {
                            let name = acc["name"].as_str().unwrap_or("-").to_string();
                            let acc_type = acc["account_type"].as_str().unwrap_or("simulated").to_string();
                            let status = acc["status"].as_str().unwrap_or("active").to_string();
                            let aid = acc["account_id"].as_str().unwrap_or("").to_string();
                            let cap = acc["initial_capital"].as_f64().unwrap_or(0.0);
                            let nav = acc["current_nav"].as_f64().unwrap_or(cap);
                            let mdd = acc["max_drawdown"].as_f64().unwrap_or(0.0);
                            let leverage = acc["leverage_enabled"].as_bool().unwrap_or(false);
                            let lev_mode = acc["leverage_mode"].as_str().unwrap_or("fixed");
                            let lev_mult = acc["leverage_multiplier"].as_f64().unwrap_or(1.0);
                            let signal = acc["signal_source"].as_str().unwrap_or("factor");
                            let owner = acc["owner"].as_str().unwrap_or("");
                            let type_label = if acc_type == "real" { "🔴 实盘" } else { "🟡 模拟" };
                            let is_open = *expanded.read() == aid;
                            let aid_toggle = aid.clone();
                            let aid_delete = aid.clone();
                            let aid_detail = aid.clone();
                            rsx! {
                                div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden",
                                    // 头部
                                    div { class: "p-5 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800/50 transition",
                                        onclick: move |_| {
                                            if is_open { expanded.set(String::new()); detail.set(None); return; }
                                            expanded.set(aid_toggle.clone());
                                            detail_loading.set(true);
                                            let ad = aid_detail.clone();
                                            spawn(async move {
                                                match api::get_account_detail(&ad).await {
                                                    Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => detail.set(Some(v["data"].clone())),
                                                    Ok(v) => error.set(v["message"].as_str().unwrap_or("失败").to_string()),
                                                    Err(e) => error.set(e),
                                                }
                                                detail_loading.set(false);
                                            });
                                        },
                                        div { class: "flex items-center justify-between mb-4",
                                            div { class: "flex items-center gap-3",
                                                h3 { class: "font-semibold text-gray-900 dark:text-white text-lg", "{name}" }
                                                span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400", "{type_label}" }
                                                if status == "active" { span { class: "text-xs px-2 py-0.5 rounded-full bg-green-100 dark:bg-green-900/50 text-green-700 dark:text-green-400", "正常" } }
                                                else { span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-400 dark:text-gray-500", "已停用" } }
                                            }
                                            div { class: "flex items-center gap-2",
                                                span { class: "text-xs text-gray-400 dark:text-gray-600", if is_open { "收起 ▲" } else { "展开 ▼" } }
                                                button { class: "text-xs px-3 py-1.5 bg-red-100 dark:bg-red-900/50 hover:bg-red-200 dark:hover:bg-red-800 rounded-lg text-red-600 dark:text-red-400 transition",
                                                    onclick: move |evt| {
                                                        evt.stop_propagation();
                                                        let a = aid_delete.clone(); let n = name.clone();
                                                        spawn(async move {
                                                            if !web_sys::window().and_then(|w| w.confirm_with_message(&format!("确认停用 {}？", n)).ok()).unwrap_or(false) { return; }
                                                            match api::delete_account(&a).await {
                                                                Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => { message.set(format!("已停用 {}", n)); load(); }
                                                                Ok(v) => error.set(v["message"].as_str().unwrap_or("失败").to_string()),
                                                                Err(e) => error.set(e),
                                                            }
                                                        });
                                                    }, "删除"
                                                }
                                            }
                                        }
                                        div { class: "grid grid-cols-2 md:grid-cols-4 gap-4 text-sm",
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "资金/净值" } div { class: "text-gray-900 dark:text-white font-mono text-sm", "¥{cap as i64}/¥{nav as i64}" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "杠杆" } if leverage && lev_mult > 1.0 { div { class: "text-yellow-600 dark:text-yellow-400 text-sm", "{lev_mode} ×{lev_mult}" } } else { div { class: "text-gray-500 dark:text-gray-400 text-sm", "无杠杆" } } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "信号源" } if signal == "prediction_blend" { div { class: "text-blue-600 dark:text-blue-400 text-sm", "ML混合" } } else if signal == "prediction" { div { class: "text-blue-600 dark:text-blue-400 text-sm", "ML预测" } } else { div { class: "text-gray-700 dark:text-gray-300 text-sm", "因子选股" } } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "最大回撤" } div { class: "text-red-600 dark:text-red-400 text-sm", "{mdd}%" } }
                                        }
                                    }
                                    // 展开详情
                                    if is_open {
                                        div { class: "border-t border-gray-200 dark:border-gray-800 p-5 bg-gray-50/80 dark:bg-gray-900/80",
                                            if *detail_loading.read() {
                                                div { class: "flex justify-center py-8", div { class: "animate-spin h-6 w-6 border-2 border-blue-500 border-t-transparent rounded-full" } }
                                            } else if let Some(ref d) = *detail.read() {
                                                if let Some(m) = d.get("metrics") {
                                                    {
                                                        let ar = m["annual_return_pct"].as_f64().unwrap_or(0.0);
                                                        let cr = m["cumulative_return_pct"].as_f64().unwrap_or(0.0);
                                                        let sh = m["sharpe_ratio"].as_f64().unwrap_or(0.0);
                                                        let so = m["sortino_ratio"].as_f64().unwrap_or(0.0);
                                                        let md = m["max_drawdown_pct"].as_f64().unwrap_or(0.0);
                                                        let ca = m["calmar_ratio"].as_f64().unwrap_or(0.0);
                                                        let ar_color = if ar >= 0.0 { "text-green-600 dark:text-green-400" } else { "text-red-600 dark:text-red-400" };
                                                        let cr_color = if cr >= 0.0 { "text-green-600 dark:text-green-400" } else { "text-red-600 dark:text-red-400" };
                                                        let created = d["created_at"].as_str().unwrap_or("-");
                                                        let days = m["nav_history_days"].as_i64().unwrap_or(0);
                                                        rsx! {
                                                            div { class: "mb-4",
                                                                h4 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-3", "绩效指标" }
                                                                div { class: "grid grid-cols-3 md:grid-cols-6 gap-3",
                                                                    div { class: "bg-gray-100 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "年化收益" } div { class: "font-mono font-semibold {ar_color}", "{ar:.2}%" } }
                                                                    div { class: "bg-gray-100 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "累计收益" } div { class: "font-mono font-semibold {cr_color}", "{cr:.2}%" } }
                                                                    div { class: "bg-gray-100 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "Sharpe" } div { class: "font-mono font-semibold text-blue-600 dark:text-blue-400", "{sh:.2}" } }
                                                                    div { class: "bg-gray-100 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "Sortino" } div { class: "font-mono font-semibold text-blue-600 dark:text-blue-400", "{so:.2}" } }
                                                                    div { class: "bg-gray-100 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "最大回撤" } div { class: "font-mono font-semibold text-red-600 dark:text-red-400", "{md:.2}%" } }
                                                                    div { class: "bg-gray-100 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "Calmar" } div { class: "font-mono font-semibold text-yellow-600 dark:text-yellow-400", "{ca:.2}" } }
                                                                }
                                                                div { class: "text-xs text-gray-400 dark:text-gray-600 mt-2", "启动: {created} · 样本: {days}天" }
                                                            }
                                                        }
                                                    }
                                                }
                                                div { class: "grid grid-cols-1 md:grid-cols-2 gap-4",
                                                    div {
                                                        h4 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-2", "当前持仓" }
                                                        if let Some(positions) = d["positions"].as_array() {
                                                            if positions.is_empty() {
                                                                div { class: "text-xs text-gray-400 dark:text-gray-500 py-4 text-center", "暂无持仓" }
                                                            } else {
                                                                div { class: "overflow-x-auto max-h-64",
                                                                    table { class: "w-full text-xs",
                                                                        thead { tr { class: "text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-800 sticky top-0 bg-gray-50 dark:bg-gray-900",
                                                                            th { class: "text-left py-2 pr-2", "标的" } th { class: "text-right py-2 px-2", "数量" } th { class: "text-right py-2 px-2", "成本" } th { class: "text-right py-2 px-2", "现价" } th { class: "text-right py-2 pl-2", "市值" }
                                                                        } }
                                                                        tbody {
                                                                            for p in positions.iter().take(30) {
                                                                                {
                                                                                    let sym = p["symbol"].as_str().unwrap_or("-");
                                                                                    let qty = p["quantity"].as_str().unwrap_or("0");
                                                                                    let cost = p["avg_cost"].as_str().unwrap_or("0");
                                                                                    let price = p["market_price"].as_str().unwrap_or("0");
                                                                                    let mv = p["market_value"].as_str().unwrap_or("0");
                                                                                    rsx! {
                                                                                        tr { class: "border-b border-gray-100 dark:border-gray-800/50",
                                                                                            td { class: "py-1.5 pr-2 text-gray-900 dark:text-white font-mono", "{sym}" }
                                                                                            td { class: "py-1.5 px-2 text-right text-gray-700 dark:text-gray-300", "{qty}" }
                                                                                            td { class: "py-1.5 px-2 text-right text-gray-500 dark:text-gray-400", "{cost}" }
                                                                                            td { class: "py-1.5 px-2 text-right text-gray-700 dark:text-gray-300", "{price}" }
                                                                                            td { class: "py-1.5 pl-2 text-right text-gray-900 dark:text-white font-mono", "{mv}" }
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
                                                    div {
                                                        h4 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-2", "最近交易" }
                                                        if let Some(trades) = d["trades"].as_array() {
                                                            if trades.is_empty() {
                                                                div { class: "text-xs text-gray-400 dark:text-gray-500 py-4 text-center", "暂无交易" }
                                                            } else {
                                                                div { class: "overflow-x-auto max-h-64",
                                                                    table { class: "w-full text-xs",
                                                                        thead { tr { class: "text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-800 sticky top-0 bg-gray-50 dark:bg-gray-900",
                                                                            th { class: "text-left py-2 pr-2", "标的" } th { class: "text-center py-2 px-1", "方向" } th { class: "text-right py-2 pl-2", "数量/价格" } th { class: "text-right py-2 pl-2", "时间" }
                                                                        } }
                                                                        tbody {
                                                                            for t in trades.iter().take(25) {
                                                                                {
                                                                                    let side = t["side"].as_str().unwrap_or("-");
                                                                                    let side_class = if side == "buy" { "text-green-600 dark:text-green-400" } else { "text-red-600 dark:text-red-400" };
                                                                                    let sym = t["symbol"].as_str().unwrap_or("-");
                                                                                    let qty = t["quantity"].as_str().unwrap_or("0");
                                                                                    let price = t["fill_price"].as_str().unwrap_or("-");
                                                                                    let ftime = t["fill_time"].as_str().unwrap_or("-");
                                                                                    rsx! {
                                                                                        tr { class: "border-b border-gray-100 dark:border-gray-800/50",
                                                                                            td { class: "py-1.5 pr-2 text-gray-900 dark:text-white font-mono", "{sym}" }
                                                                                            td { class: "py-1.5 px-1 text-center", span { class: side_class, "{side}" } }
                                                                                            td { class: "py-1.5 pl-2 text-right text-gray-700 dark:text-gray-300", "{qty}@{price}" }
                                                                                            td { class: "py-1.5 pl-2 text-right text-gray-500 dark:text-gray-400", "{ftime}" }
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
}
