//! 账号管理页面 — 可过滤、可展开详情(绩效+持仓+交易)、可停用

use dioxus::prelude::*;
use serde_json::Value;

use crate::api;
use crate::components::charts::CumulativeLineChart;
use dioxus::events::{Key, KeyboardEvent};

// ── 过滤器子组件：使用 Dioxus 信号管理状态，通过 use_callback 稳定回调避免重渲染 ──

#[component]
fn FilterBar(on_search: Callback<String>) -> Element {
    let mut f_name = use_signal(String::new);
    let mut f_leverage = use_signal(|| "all".to_string());
    let mut f_signal = use_signal(|| "all".to_string());
    let mut f_lev_min = use_signal(String::new);
    let mut f_lev_max = use_signal(String::new);
    let mut f_status = use_signal(|| "all".to_string());

    let build_filter = move || {
        let mut p = Vec::new();
        let n = f_name.read(); if !n.is_empty() { p.push(format!("name={}", &*n)); }
        let l = f_leverage.read(); if *l != "all" { p.push(format!("leverage={}", &*l)); }
        let s = f_signal.read(); if *s != "all" { p.push(format!("signal_source={}", &*s)); }
        let lmin = f_lev_min.read(); if !lmin.is_empty() { p.push(format!("lev_mult_min={}", &*lmin)); }
        let lmax = f_lev_max.read(); if !lmax.is_empty() { p.push(format!("lev_mult_max={}", &*lmax)); }
        let st = f_status.read(); if *st != "all" { p.push(format!("status={}", &*st)); }
        p.join("&")
    };
    let do_search = move || on_search(build_filter());
    let mut do_reset = move || {
        f_name.set(String::new());
        f_leverage.set("all".to_string());
        f_signal.set("all".to_string());
        f_lev_min.set(String::new());
        f_lev_max.set(String::new());
        f_status.set("all".to_string());
        on_search(String::new());
    };

    rsx! {
        div { class: "mb-4 p-3 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800",
            div { class: "flex gap-3 items-end flex-wrap",
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "名称" }
                    input { class: "w-32 px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm",
                        value: "{f_name}", oninput: move |e| f_name.set(e.value()),
                        onkeydown: move |e: KeyboardEvent| if e.key() == Key::Enter { do_search(); },
                    }
                }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "杠杆" }
                    select { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm",
                        value: "{f_leverage}", onchange: move |e| f_leverage.set(e.value()),
                        option { value: "all", "全部" } option { value: "enabled", "已启用" } option { value: "disabled", "未启用" }
                    }
                }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "信号源" }
                    select { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm",
                        value: "{f_signal}", onchange: move |e| f_signal.set(e.value()),
                        option { value: "all", "全部" } option { value: "factor", "因子" } option { value: "prediction", "ML预测" } option { value: "prediction_blend", "ML混合" }
                    }
                }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "倍率从" }
                    input { class: "w-16 px-2 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm text-center",
                        value: "{f_lev_min}", oninput: move |e| f_lev_min.set(e.value()),
                    }
                }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "到" }
                    input { class: "w-16 px-2 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm text-center",
                        value: "{f_lev_max}", oninput: move |e| f_lev_max.set(e.value()),
                    }
                }
                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "状态" }
                    select { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm",
                        value: "{f_status}", onchange: move |e| f_status.set(e.value()),
                        option { value: "all", "全部" } option { value: "active", "活跃" } option { value: "inactive", "已停用" }
                    }
                }
                button { class: "px-3 py-2 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg text-sm text-gray-600 dark:text-gray-300 transition self-end",
                    onclick: move |_| do_reset(),
                    "重置"
                }
                button { class: "px-5 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm text-white transition self-end",
                    onclick: move |_| do_search(),
                    "查询"
                }
            }
        }
    }
}

#[component]
pub fn AccountsContent() -> Element {
    let mut accounts = use_signal(Vec::<Value>::new);
    let mut loading = use_signal(|| true);
    let mut error = use_signal(String::new);
    let mut message = use_signal(String::new);
    let mut expanded = use_signal(String::new);
    let mut detail = use_signal(|| Option::<Value>::None);
    let mut detail_loading = use_signal(|| false);

    // ── 回放弹窗状态 ──────────────────────────────────
    let mut replay_modal = use_signal(|| Option::<Value>::None);
    let mut replay_start = use_signal(String::new);
    let mut replay_end = use_signal(String::new);
    let mut replay_loading = use_signal(|| false);
    let mut replay_result = use_signal(|| Option::<Value>::None);

    // ── 编辑弹窗状态 ──────────────────────────────────
    let mut edit_modal = use_signal(|| Option::<Value>::None);
    let mut edit_name = use_signal(String::new);
    let mut edit_signal = use_signal(|| "factor".to_string());
    let mut edit_leverage = use_signal(|| false);
    let mut edit_lev_mode = use_signal(|| "fixed".to_string());
    let mut edit_lev_mult = use_signal(String::new);
    let mut edit_dingtalk = use_signal(String::new);
    let mut edit_margin = use_signal(String::new);
    let mut edit_cash = use_signal(String::new);
    let mut edit_saving = use_signal(|| false);
    let mut edit_result = use_signal(String::new);

    let mut inited = use_signal(|| false);

    // 加载账号列表（接收过滤参数），保持 FilterBar 始终挂载
    let mut load = move |filter: String| {
        loading.set(true);
        spawn(async move {
            match api::list_accounts(&filter).await {
                Ok(v) => { if let Some(arr) = v["data"].as_array() { accounts.set(arr.clone()); } }
                Err(e) => error.set(e),
            }
            loading.set(false);
        });
    };

    use_effect(move || {
        if !*inited.read() { inited.set(true); load(String::new()); }
    });

    // 用 use_memo 创建稳定回调引用，避免 FilterBar 因父组件重渲染而丢失输入值
    let on_filter_search = use_memo(move || Callback::new(move |f: String| load(f)));

    // ── 编辑弹窗（提前返回，避免在 rsx! 中使用 if let）───
    let show_edit = edit_modal.read().is_some();
    if show_edit {
        let edit_acc = edit_modal.read().clone().unwrap();
        let acc_id = edit_acc.get("paper_account_id").or(edit_acc.get("account_id"))
            .and_then(|v| v.as_str()).unwrap_or("").to_string();
        let acc_name = edit_acc.get("name").and_then(|v| v.as_str()).unwrap_or("-").to_string();
        let name_val = edit_name.read().clone();
        let signal_val = edit_signal.read().clone();
        let leverage_val = *edit_leverage.read();
        let lev_mode_val = edit_lev_mode.read().clone();
        let lev_mult_val = edit_lev_mult.read().clone();
        let dingtalk_val = edit_dingtalk.read().clone();
        let margin_val = edit_margin.read().clone();
        let cash_val = edit_cash.read().clone();
        let saving = *edit_saving.read();
        let result_msg = edit_result.read().clone();

        return rsx! {
            div { class: "fixed inset-0 bg-black/60 z-50 flex items-center justify-center",
                onclick: move |_| { edit_modal.set(None); },
                div { class: "bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-2xl p-6 w-full max-w-lg mx-4 max-h-[90vh] overflow-y-auto shadow-2xl",
                    onclick: move |e| e.stop_propagation(),
                    h2 { class: "text-lg font-bold text-gray-900 dark:text-white mb-1", "编辑账号" }
                    p { class: "text-sm text-gray-500 dark:text-gray-400 mb-5", "{acc_name}" }
                    div { class: "space-y-4",
                        // 名称
                        div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "名称" }
                            input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                value: "{name_val}", oninput: move |e| edit_name.set(e.value()),
                            }
                        }
                        // 信号源
                        div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "信号源" }
                            select { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                value: "{signal_val}", onchange: move |e| edit_signal.set(e.value()),
                                option { value: "factor", "因子选股" }
                                option { value: "prediction", "ML预测" }
                                option { value: "prediction_blend", "ML混合" }
                            }
                        }
                        // 杠杆开关
                        div { class: "flex items-center gap-3",
                            label { class: "text-xs text-gray-500 dark:text-gray-400", "启用杠杆" }
                            input { r#type: "checkbox", checked: leverage_val,
                                onchange: move |e| edit_leverage.set(e.value() == "true"),
                            }
                        }
                        // 杠杆模式 + 倍率
                        if leverage_val {
                            div { class: "grid grid-cols-2 gap-3",
                                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "杠杆模式" }
                                    select { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                        value: "{lev_mode_val}", onchange: move |e| edit_lev_mode.set(e.value()),
                                        option { value: "fixed", "固定倍率" }
                                        option { value: "vol_target", "波动率目标" }
                                    }
                                }
                                div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "倍率" }
                                    input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                        value: "{lev_mult_val}", oninput: move |e| edit_lev_mult.set(e.value()),
                                    }
                                }
                            }
                        }
                        // 钉钉 Webhook URL
                        div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "钉钉 Webhook URL" }
                            input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                placeholder: "https://oapi.dingtalk.com/robot/send?access_token=...",
                                value: "{dingtalk_val}", oninput: move |e| edit_dingtalk.set(e.value()),
                            }
                        }
                        // 现金
                        div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "现金" }
                            input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                value: "{cash_val}", oninput: move |e| edit_cash.set(e.value()),
                            }
                        }
                        // 融资金额
                        div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "融资金额（无杠杆则为0）" }
                            input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                value: "{margin_val}", oninput: move |e| edit_margin.set(e.value()),
                            }
                        }
                    }
                    // 操作按钮
                    div { class: "flex gap-3 mt-6",
                        button { class: "flex-1 py-2.5 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 transition",
                            onclick: move |_| edit_modal.set(None), "取消"
                        }
                        button { class: "flex-1 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-300 dark:disabled:bg-gray-600 disabled:text-gray-500 rounded-lg text-sm text-white transition",
                            disabled: saving,
                            onclick: {
                                let aid = acc_id.clone();
                                let n = name_val.clone();
                                move |_| {
                                    edit_saving.set(true);
                                    edit_result.set(String::new());
                                    let id = aid.clone();
                                    let name = edit_name.read().clone();
                                    let sig = edit_signal.read().clone();
                                    let lev = *edit_leverage.read();
                                    let lmode = edit_lev_mode.read().clone();
                                    let lmult = edit_lev_mult.read().clone();
                                    let dt = edit_dingtalk.read().clone();
                                    let mg = edit_margin.read().clone();
                                    let ca = edit_cash.read().clone();
                                    spawn(async move {
                                        let lev_mult: Option<f64> = lmult.parse().ok();
                                        let margin: Option<f64> = mg.parse().ok();
                                        let cash: Option<f64> = ca.parse().ok();
                                        let payload = serde_json::json!({
                                            "name": name,
                                            "signal_source": sig,
                                            "leverage_enabled": lev,
                                            "leverage_mode": lmode,
                                            "leverage_multiplier": lev_mult,
                                            "dingtalk_webhook_url": if dt.is_empty() { None::<String> } else { Some(dt) },
                                            "margin_amount": margin,
                                            "cash": cash,
                                        });
                                        match api::update_account(&id, &payload).await {
                                            Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => {
                                                edit_result.set("✅ 保存成功".to_string());
                                                edit_modal.set(None);
                                                load(String::new());
                                            }
                                            Ok(v) => { edit_result.set(format!("❌ {}", v["message"].as_str().unwrap_or("失败"))); edit_saving.set(false); }
                                            Err(e) => { edit_result.set(format!("❌ {}", e)); edit_saving.set(false); }
                                        }
                                    });
                                }
                            },
                            if saving { "保存中…" } else { "保存" }
                        }
                    }
                    if !result_msg.is_empty() {
                        {
                            let cls = if result_msg.starts_with("✅") { "text-green-600" } else { "text-red-600" };
                            rsx! { div { class: "mt-3 text-sm {cls}", "{result_msg}" } }
                        }
                    }
                }
            }
        };
    }

    // ── 回放弹窗（提前返回，避免在 rsx! 中使用 if let）───
    let show_replay = replay_modal.read().is_some();
    if show_replay {
        let replay_acc = replay_modal.read().clone().unwrap();
        let acc_name = replay_acc.get("name").and_then(|v| v.as_str()).unwrap_or("-").to_string();
        let acc_id = replay_acc.get("paper_account_id").or(replay_acc.get("account_id"))
            .and_then(|v| v.as_str()).unwrap_or("").to_string();
        let start_val = replay_start.read().clone();
        let end_val = replay_end.read().clone();
        let is_loading = *replay_loading.read();
        let result = replay_result.read().clone();
        let cap = replay_acc.get("initial_capital").and_then(|v| v.as_f64()).unwrap_or(1000000.0) as i64;

        return rsx! {
            div { class: "fixed inset-0 bg-black/60 z-50 flex items-center justify-center",
                onclick: move |_| { replay_modal.set(None); },
                div { class: "bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-2xl p-6 w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto shadow-2xl",
                    onclick: move |e| e.stop_propagation(),
                    h2 { class: "text-lg font-bold text-gray-900 dark:text-white mb-1", "模拟回放" }
                    p { class: "text-sm text-gray-500 dark:text-gray-400 mb-5", "{acc_name}" }
                    div { class: "grid grid-cols-2 gap-4 mb-5",
                        div {
                            label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "启动时间" }
                            input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                r#type: "date", value: "{start_val}",
                                oninput: move |e| replay_start.set(e.value()),
                            }
                        }
                        div {
                            label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "回放截止时间" }
                            input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                r#type: "date", value: "{end_val}",
                                oninput: move |e| replay_end.set(e.value()),
                            }
                        }
                        div {
                            label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "初始资金" }
                            input { class: "w-full px-3 py-2 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm font-mono",
                                value: "{cap}", disabled: true,
                            }
                            div { class: "text-xs text-gray-400 mt-0.5", "（使用账号配置的初始资金）" }
                        }
                    }
                    div { class: "flex gap-3 mb-5",
                        button { class: "flex-1 py-2.5 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 transition",
                            onclick: move |_| replay_modal.set(None), "取消"
                        }
                        button { class: "flex-1 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-300 dark:disabled:bg-gray-600 disabled:text-gray-500 rounded-lg text-sm text-white transition",
                            disabled: is_loading,
                            onclick: {
                                let aid = acc_id.clone();
                                let s = start_val.clone();
                                let e = end_val.clone();
                                move |_| {
                                    replay_loading.set(true);
                                    replay_result.set(None);
                                    let a = aid.clone(); let s2 = s.clone(); let e2 = e.clone();
                                    spawn(async move {
                                        match api::run_historical_replay(&a, &s2, &e2).await {
                                            Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => {
                                                replay_result.set(Some(v.clone()));
                                            }
                                            Ok(v) => {
                                                error.set(v["message"].as_str().unwrap_or("失败").to_string());
                                                replay_result.set(Some(v));
                                            }
                                            Err(e) => { error.set(e); }
                                        }
                                        replay_loading.set(false);
                                    });
                                }
                            },
                            if is_loading { span { class: "inline-flex items-center gap-2",
                                span { class: "animate-spin inline-block w-4 h-4 border-2 border-white border-t-transparent rounded-full" }
                                "回放中…"
                            } } else { "开始回放" }
                        }
                    }
                    if is_loading {
                        div { class: "flex justify-center py-8",
                            div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" }
                        }
                    }
                    if let Some(ref res) = result {
                        if let Some(data) = res.get("data") {
                            {
                                let ar = data["annual_return_pct"].as_f64().unwrap_or(0.0);
                                let cr = data["cumulative_return_pct"].as_f64().unwrap_or(0.0);
                                let sh = data["sharpe_ratio"].as_f64().unwrap_or(0.0);
                                let so = data["sortino_ratio"].as_f64().unwrap_or(0.0);
                                let mdd = data["max_drawdown_pct"].as_f64().unwrap_or(0.0);
                                let ca = data["calmar_ratio"].as_f64().unwrap_or(0.0);
                                let vol = data["volatility_pct"].as_f64().unwrap_or(0.0);
                                let wr = data["win_rate_pct"].as_f64().unwrap_or(0.0);
                                let td = data["trading_days"].as_i64().unwrap_or(0);
                                let sd = data["start_date"].as_str().unwrap_or("-");
                                let ed = data["end_date"].as_str().unwrap_or("-");
                                let ar_cls = if ar >= 0.0 { "text-green-600 dark:text-green-400" } else { "text-red-600 dark:text-red-400" };
                                let cr_cls = if cr >= 0.0 { "text-green-600 dark:text-green-400" } else { "text-red-600 dark:text-red-400" };
                                rsx! {
                                    div { class: "border-t border-gray-200 dark:border-gray-700 pt-4",
                                        h4 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-3", "回放结果" }
                                        div { class: "text-xs text-gray-500 dark:text-gray-400 mb-3", "{sd} → {ed} · {td} 个交易日" }
                                        div { class: "grid grid-cols-3 gap-3",
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "年化收益" } div { class: "font-mono font-semibold {ar_cls}", "{ar:.1}%" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "累计收益" } div { class: "font-mono font-semibold {cr_cls}", "{cr:.1}%" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "最大回撤" } div { class: "font-mono font-semibold text-red-600 dark:text-red-400", "{mdd:.1}%" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "Sharpe" } div { class: "font-mono font-semibold text-blue-600 dark:text-blue-400", "{sh:.2}" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "Sortino" } div { class: "font-mono font-semibold text-blue-600 dark:text-blue-400", "{so:.2}" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "Calmar" } div { class: "font-mono font-semibold text-yellow-600 dark:text-yellow-400", "{ca:.2}" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "波动率" } div { class: "font-mono font-semibold text-gray-700 dark:text-gray-300", "{vol:.1}%" } }
                                            div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3 text-center", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "胜率" } div { class: "font-mono font-semibold text-green-600 dark:text-green-400", "{wr:.1}%" } }
                                        }
                                    }
                                }
                            }
                        } else if let Some(msg) = res.get("message").and_then(|v| v.as_str()) {
                            div { class: "border-t border-gray-200 dark:border-gray-700 pt-4",
                                div { class: "p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm", "{msg}" }
                            }
                        }
                    }
                }
            }
        };
    }

    // 注意：不在此处提前返回 loading spinner！否则 FilterBar 会被卸载，信号重置，查询条件丢失。
    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            div { class: "flex items-center justify-between mb-4",
                h1 { class: "text-2xl font-bold text-gray-900 dark:text-white", "投资账号" }
            }
            if !message.read().is_empty() { div { class: "mb-4 p-3 bg-green-50 dark:bg-green-900/50 border border-green-300 dark:border-green-700 rounded-lg text-green-700 dark:text-green-300 text-sm flex justify-between", span { "{message}" } button { class: "text-green-600 dark:text-green-400", onclick: move |_| message.set(String::new()), "✕" } } }
            if !error.read().is_empty() { div { class: "mb-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm flex justify-between", span { "{error}" } button { class: "text-red-600 dark:text-red-400", onclick: move |_| error.set(String::new()), "✕" } } }
            FilterBar {
                on_search: on_filter_search.cloned(),
            }
            if *loading.read() {
                div { class: "flex justify-center py-12",
                    div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" }
                }
            } else if accounts.read().is_empty() {
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
                            let _owner = acc["owner"].as_str().unwrap_or("");
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
                                                // 编辑按钮
                                                {
                                                    let a_clone = acc.clone();
                                                    rsx! {
                                                        button { class: "text-xs px-3 py-1.5 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg text-gray-600 dark:text-gray-300 transition",
                                                            onclick: move |evt| {
                                                                evt.stop_propagation();
                                                                edit_name.set(a_clone.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string());
                                                                edit_signal.set(a_clone.get("signal_source").and_then(|v| v.as_str()).unwrap_or("factor").to_string());
                                                                edit_leverage.set(a_clone.get("leverage_enabled").and_then(|v| v.as_bool()).unwrap_or(false));
                                                                edit_lev_mode.set(a_clone.get("leverage_mode").and_then(|v| v.as_str()).unwrap_or("fixed").to_string());
                                                                edit_lev_mult.set(a_clone.get("leverage_multiplier").and_then(|v| v.as_f64()).map(|v| v.to_string()).unwrap_or_default());
                                                                edit_dingtalk.set(a_clone.get("dingtalk_webhook_url").and_then(|v| v.as_str()).unwrap_or("").to_string());
                                                                edit_margin.set(a_clone.get("margin_amount").and_then(|v| v.as_f64()).map(|v| v.to_string()).unwrap_or_default());
                                                                edit_cash.set(a_clone.get("cash").and_then(|v| v.as_f64()).map(|v| v.to_string()).unwrap_or_default());
                                                                edit_result.set(String::new());
                                                                edit_modal.set(Some(a_clone.clone()));
                                                            },
                                                            "编辑"
                                                        }
                                                    }
                                                }
                                                // 回放按钮
                                                {
                                                    let a_clone = acc.clone();
                                                    rsx! {
                                                        button { class: "text-xs px-3 py-1.5 bg-blue-100 dark:bg-blue-900/50 hover:bg-blue-200 dark:hover:bg-blue-800 rounded-lg text-blue-600 dark:text-blue-400 transition",
                                                            onclick: move |evt| {
                                                                evt.stop_propagation();
                                                                let created = a_clone.get("created_at").and_then(|v| v.as_str()).unwrap_or("2024-01-01").to_string().split('T').next().unwrap_or("2024-01-01").to_string();
                                                                replay_start.set(created);
                                                                replay_end.set("2026-06-09".to_string());
                                                                replay_result.set(None);
                                                                replay_modal.set(Some(a_clone.clone()));
                                                            },
                                                            "回放"
                                                        }
                                                    }
                                                }
                                                // 推送按钮
                                                {
                                                    let a_id = aid.clone();
                                                    rsx! {
                                                        button { class: "text-xs px-3 py-1.5 bg-green-100 dark:bg-green-900/50 hover:bg-green-200 dark:hover:bg-green-800 rounded-lg text-green-600 dark:text-green-400 transition",
                                                            onclick: move |evt| {
                                                                evt.stop_propagation();
                                                                let id = a_id.clone();
                                                                spawn(async move {
                                                                    match api::account_push_dingtalk(&id).await {
                                                                        Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => message.set(v["message"].as_str().unwrap_or("推送成功").to_string()),
                                                                        Ok(v) => error.set(v["message"].as_str().unwrap_or("推送失败").to_string()),
                                                                        Err(e) => error.set(e),
                                                                    }
                                                                });
                                                            },
                                                            "推送"
                                                        }
                                                    }
                                                }
                                                // 删除按钮
                                                button { class: "text-xs px-3 py-1.5 bg-red-100 dark:bg-red-900/50 hover:bg-red-200 dark:hover:bg-red-800 rounded-lg text-red-600 dark:text-red-400 transition",
                                                    onclick: move |evt| {
                                                        evt.stop_propagation();
                                                        let a = aid_delete.clone(); let n = name.clone();
                                                        spawn(async move {
                                                            if !web_sys::window().and_then(|w| w.confirm_with_message(&format!("确认停用 {}？", n)).ok()).unwrap_or(false) { return; }
                                                            match api::delete_account(&a).await {
                                                                Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => { message.set(format!("已停用 {}", n)); load(String::new()); }
                                                                Ok(v) => error.set(v["message"].as_str().unwrap_or("失败").to_string()),
                                                                Err(e) => error.set(e),
                                                            }
                                                        });
                                                    }, "删除"
                                                }
                                            }
                                        }
                                        div { class: "grid grid-cols-2 md:grid-cols-4 gap-4 text-sm",
                                            {
                                                let cash_val = acc["cash"].as_f64().unwrap_or(cap);
                                                rsx! {
                                                    div { class: "bg-gray-50 dark:bg-gray-800/50 rounded-lg p-3", div { class: "text-xs text-gray-500 dark:text-gray-400 mb-1", "总资产/净值/现金" } div { class: "text-gray-900 dark:text-white font-mono text-sm", "¥{nav as i64} / ¥{cap as i64} / ¥{cash_val as i64}" } }
                                                }
                                            }
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
                                                // ── 年度收益柱状图 + 累计收益率曲线 ──
                                                div { class: "mb-4",
                                                    // 柱状图：年度收益对比
                                                    if let (Some(yearly), Some(benchmarks)) = (d["yearly_returns"].as_array(), d["benchmarks"].as_object()) {
                                                        {
                                                            let max_abs = yearly.iter()
                                                                .chain(benchmarks.get("csi300").and_then(|b| b["yearly_returns"].as_array()).into_iter().flatten())
                                                                .chain(benchmarks.get("gold").and_then(|b| b["yearly_returns"].as_array()).into_iter().flatten())
                                                                .chain(benchmarks.get("sp500").and_then(|b| b["yearly_returns"].as_array()).into_iter().flatten())
                                                                .filter_map(|y| y["return_pct"].as_f64())
                                                                .fold(0.0_f64, |a, b| a.max(b.abs())).max(5.0);
                                                            let bar_h: f64 = 120.0;
                                                            let yr_list: Vec<&serde_json::Value> = yearly.iter().collect();
                                                            let years: Vec<String> = yr_list.iter().map(|y| y["year"].as_str().unwrap_or("-").to_string()).collect();
                                                            let acct_rets: Vec<f64> = yr_list.iter().map(|y| y["return_pct"].as_f64().unwrap_or(0.0)).collect();
                                                            let csi_rets: Vec<f64> = years.iter().map(|y| benchmarks.get("csi300").and_then(|b| b["yearly_returns"].as_array()).and_then(|a| a.iter().find(|x| x["year"].as_str()==Some(y))).and_then(|x| x["return_pct"].as_f64()).unwrap_or(0.0)).collect();
                                                            let gold_rets: Vec<f64> = years.iter().map(|y| benchmarks.get("gold").and_then(|b| b["yearly_returns"].as_array()).and_then(|a| a.iter().find(|x| x["year"].as_str()==Some(y))).and_then(|x| x["return_pct"].as_f64()).unwrap_or(0.0)).collect();
                                                            let sp_rets: Vec<f64> = years.iter().map(|y| benchmarks.get("sp500").and_then(|b| b["yearly_returns"].as_array()).and_then(|a| a.iter().find(|x| x["year"].as_str()==Some(y))).and_then(|x| x["return_pct"].as_f64()).unwrap_or(0.0)).collect();
                                                            // 累计收益
                                                            let cum_acct: Vec<f64> = acct_rets.iter().scan(1.0, |cum, &r| { *cum *= 1.0+r/100.0; Some((*cum-1.0)*100.0) }).collect();
                                                            let cum_csi: Vec<f64> = csi_rets.iter().scan(1.0, |cum, &r| { *cum *= 1.0+r/100.0; Some((*cum-1.0)*100.0) }).collect();
                                                            let cum_gold: Vec<f64> = gold_rets.iter().scan(1.0, |cum, &r| { *cum *= 1.0+r/100.0; Some((*cum-1.0)*100.0) }).collect();
                                                            let cum_sp: Vec<f64> = sp_rets.iter().scan(1.0, |cum, &r| { *cum *= 1.0+r/100.0; Some((*cum-1.0)*100.0) }).collect();
                                                            let _max_cum = cum_acct.iter().chain(cum_csi.iter()).chain(cum_gold.iter()).chain(cum_sp.iter()).fold(0.0_f64, |a: f64, &v| a.max(v.abs())).max(1.0_f64);
                                                            rsx! {
                                                                h4 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-2", "年度收益对比（柱状图）" }
                                                                div { class: "flex items-center gap-3 text-xs mb-2",
                                                                    span { class: "inline-block w-3 h-3 rounded-sm bg-blue-500" } span { class: "text-gray-500 dark:text-gray-400", "账号" }
                                                                    span { class: "inline-block w-3 h-3 rounded-sm bg-gray-400 ml-1" } span { class: "text-gray-500 dark:text-gray-400", "CSI300" }
                                                                    span { class: "inline-block w-3 h-3 rounded-sm bg-yellow-500 ml-1" } span { class: "text-gray-500 dark:text-gray-400", "黄金" }
                                                                    span { class: "inline-block w-3 h-3 rounded-sm bg-green-500 ml-1" } span { class: "text-gray-500 dark:text-gray-400", "SP500" }
                                                                }
                                                                div { class: "relative mb-6", style: "height:{bar_h}px",
                                                                    // 零线
                                                                    div { class: "absolute left-0 right-0 border-t border-gray-300 dark:border-gray-500", style: "top:{bar_h/2.0}px" }
                                                                    div { class: "flex items-end justify-around h-full",
                                                                        for (i, y) in years.iter().enumerate() {
                                                                            {
                                                                                let bh_acct = (acct_rets[i].abs() / max_abs * bar_h / 2.0).max(1.0);
                                                                                let bh_csi = (csi_rets[i].abs() / max_abs * bar_h / 2.0).max(1.0);
                                                                                let bh_gold = (gold_rets[i].abs() / max_abs * bar_h / 2.0).max(1.0);
                                                                                let bh_sp = (sp_rets[i].abs() / max_abs * bar_h / 2.0).max(1.0);
                                                                                let half = bar_h / 2.0;
                                                                                let mb_acct = if acct_rets[i] >= 0.0 { half } else { half - bh_acct };
                                                                                let mb_csi = if csi_rets[i] >= 0.0 { half } else { half - bh_csi };
                                                                                let mb_gold = if gold_rets[i] >= 0.0 { half } else { half - bh_gold };
                                                                                let mb_sp = if sp_rets[i] >= 0.0 { half } else { half - bh_sp };
                                                                                rsx! {
                                                                                    div { class: "flex flex-col items-center gap-0.5",
                                                                                        div { class: "flex items-end gap-0.5",
                                                                                            div { class: "w-3 bg-blue-500 rounded-t", style: "height:{bh_acct}px; margin-bottom:{mb_acct}px" }
                                                                                            div { class: "w-3 bg-gray-400 rounded-t", style: "height:{bh_csi}px; margin-bottom:{mb_csi}px" }
                                                                                            div { class: "w-3 bg-yellow-500 rounded-t", style: "height:{bh_gold}px; margin-bottom:{mb_gold}px" }
                                                                                            div { class: "w-3 bg-green-500 rounded-t", style: "height:{bh_sp}px; margin-bottom:{mb_sp}px" }
                                                                                        }
                                                                                        span { class: "text-xs text-gray-500", "{y}" }
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                                // 累计收益率曲线（Canvas 二维折线图）
                                                                {
                                                                    let chart_id = format!("cumchart-{}", aid_detail.clone());
                                                                    rsx! {
                                                                        h4 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-2 mt-4", "累计收益率曲线" }
                                                                        div { class: "flex items-center gap-3 text-xs mb-2",
                                                                            span { class: "inline-block w-3 h-0.5 bg-blue-500 rounded" } span { class: "text-gray-500 dark:text-gray-400", "账号" }
                                                                            span { class: "inline-block w-3 h-0.5 bg-gray-400 rounded", style: "border-top:1.5px dashed #9ca3af" } span { class: "text-gray-500 dark:text-gray-400", "CSI300" }
                                                                            span { class: "inline-block w-3 h-0.5 bg-yellow-500 rounded", style: "border-top:1.5px dashed #eab308" } span { class: "text-gray-500 dark:text-gray-400", "黄金" }
                                                                            span { class: "inline-block w-3 h-0.5 bg-green-500 rounded", style: "border-top:1.5px dashed #22c55e" } span { class: "text-gray-500 dark:text-gray-400", "SP500" }
                                                                        }
                                                                        CumulativeLineChart {
                                                                            years: years.clone(),
                                                                            acct: cum_acct.clone(),
                                                                            csi300: cum_csi.clone(),
                                                                            gold: cum_gold.clone(),
                                                                            sp500: cum_sp.clone(),
                                                                            canvas_id: chart_id,
                                                                        }
                                                                    }
                                                                }
                                                                // 数值表格
                                                                div { class: "mt-2 text-xs overflow-x-auto",
                                                                    table { class: "w-full",
                                                                        thead { tr { class: "text-gray-500",
                                                                            th { class: "text-left py-1", "年度" }
                                                                            th { class: "text-right py-1 px-1", "账号" }
                                                                            th { class: "text-right py-1 px-1", "CSI300" }
                                                                            th { class: "text-right py-1 px-1", "黄金" }
                                                                            th { class: "text-right py-1 px-1", "SP500" }
                                                                            th { class: "text-right py-1 px-1", "累计" }
                                                                        }}
                                                                        tbody {
                                                                            for (i, y) in years.iter().enumerate() {
                                                                                {
                                                                                    let ret = acct_rets[i];
                                                                                    let cr = csi_rets[i];
                                                                                    let gr = gold_rets[i];
                                                                                    let sr = sp_rets[i];
                                                                                    let cum = cum_acct[i];
                                                                                    let rc = if ret>=0.0{"text-green-600"}else{"text-red-600"};
                                                                                    let cc = if cr>=0.0{"text-gray-600"}else{"text-red-500"};
                                                                                    let gc = if gr>=0.0{"text-yellow-600"}else{"text-red-500"};
                                                                                    let sc = if sr>=0.0{"text-green-600"}else{"text-red-500"};
                                                                                    let cuc = if cum>=0.0{"text-green-600 dark:text-green-400"}else{"text-red-600 dark:text-red-400"};
                                                                                    rsx! {
                                                                                        tr { class: "border-t border-gray-100 dark:border-gray-800",
                                                                                            td { class: "py-1 text-gray-500", "{y}" }
                                                                                            td { class: "py-1 text-right px-1 font-mono {rc}", "{ret:.1}%" }
                                                                                            td { class: "py-1 text-right px-1 font-mono {cc}", "{cr:.1}%" }
                                                                                            td { class: "py-1 text-right px-1 font-mono {gc}", "{gr:.1}%" }
                                                                                            td { class: "py-1 text-right px-1 font-mono {sc}", "{sr:.1}%" }
                                                                                            td { class: "py-1 text-right px-1 font-mono font-semibold {cuc}", "{cum:.1}%" }
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    } else {
                                                        div { class: "text-xs text-gray-400 py-4 text-center", "回放后显示收益图表" }
                                                    }
                                                }
                                                // ── 资产大类占比 ──
                                                div { class: "mb-4 grid grid-cols-1 md:grid-cols-2 gap-4",
                                                    div {
                                                        h4 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-2", "资产大类占比" }
                                                        if let Some(alloc) = d["asset_allocation"].as_array() {
                                                            if alloc.is_empty() {
                                                                div { class: "text-xs text-gray-400 py-4 text-center", "暂无持仓" }
                                                            } else {
                                                                {
                                                                    rsx! {
                                                                        div { class: "space-y-2",
                                                                            for item in alloc.iter() {
                                                                                {
                                                                                    let name = item["name"].as_str().unwrap_or("-");
                                                                                    let pct = item["pct"].as_f64().unwrap_or(0.0);
                                                                                    let mv = item["market_value"].as_f64().unwrap_or(0.0);
                                                                                    let color = match name {
                                                                                        "A股" => "bg-gray-500",
                                                                                        "黄金ETF" => "bg-yellow-500",
                                                                                        "国债ETF" => "bg-blue-400",
                                                                                        "纳指ETF" | "标普ETF" => "bg-green-500",
                                                                                        "原油LOF" => "bg-orange-500",
                                                                                        "商品ETF" => "bg-purple-500",
                                                                                        _ => "bg-gray-400",
                                                                                    };
                                                                                    rsx! {
                                                                                        div {
                                                                                            div { class: "flex justify-between text-xs mb-0.5",
                                                                                                span { class: "text-gray-700 dark:text-gray-300", "{name}" }
                                                                                                span { class: "text-gray-500 dark:text-gray-400", "{pct:.1}% · ¥{mv as i64}" }
                                                                                            }
                                                                                            div { class: "w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2",
                                                                                                div { class: "{color} h-2 rounded-full transition-all", style: "width:{pct}%" }
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
