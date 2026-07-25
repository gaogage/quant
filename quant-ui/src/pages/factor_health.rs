//! P3-2: v24 因子健康监控页 — 展示 14 活跃因子每日覆盖率 + 滞缓状态
//!
//! 数据源: GET /api/v1/admin/factor-health
//! 与 scheduler.rs run_data_quality_check 的 v24 因子检查块同口径:
//!   - 滞缓: factor_value 最新 trade_date < 最近交易日
//!   - 覆盖率: 最新日覆盖数 / 近 30 日最大覆盖数 < 80% 视为骤降
//! 前者红色高亮，后者橙色提示。

use dioxus::prelude::*;
use serde_json::Value;

use crate::api;

#[component]
pub fn FactorHealthPage() -> Element {
    let mut data = use_signal(|| Value::Null);
    let mut loading = use_signal(|| true);
    let mut error = use_signal(|| String::new());

    let load = move || {
        spawn(async move {
            match api::admin_factor_health().await {
                Ok(v) => {
                    if v["code"].as_i64().unwrap_or(-1) == 0 {
                        data.set(v["data"].clone());
                        error.set(String::new());
                    } else {
                        error.set(v["message"].as_str().unwrap_or("加载失败").to_string());
                    }
                }
                Err(e) => error.set(e),
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

    if !error.read().is_empty() {
        return rsx! {
            div { class: "p-6 max-w-6xl mx-auto",
                div { class: "p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm",
                    "{error}"
                }
            }
        };
    }

    let d = data.read();
    let check_date = d["check_date"].as_str().unwrap_or("-");
    let latest_trade = d["latest_trade_date"].as_str().unwrap_or("-");
    let factors: Vec<Value> = d["factors"].as_array().cloned().unwrap_or_default();

    let stale_count = factors.iter().filter(|f| f["is_stale"].as_bool().unwrap_or(false)).count();
    let low_cov_count = factors.iter().filter(|f| f["coverage_pct"].as_f64().unwrap_or(100.0) < 80.0).count();
    let healthy = stale_count == 0 && low_cov_count == 0;

    let summary_cls = if healthy {
        "bg-green-50 dark:bg-green-900/30 border-green-300 dark:border-green-700 text-green-700 dark:text-green-300"
    } else {
        "bg-yellow-50 dark:bg-yellow-900/30 border-yellow-300 dark:border-yellow-700 text-yellow-700 dark:text-yellow-300"
    };
    let summary_text = if healthy {
        format!("✓ 14 因子全部健康（检查日 {}，最近交易日 {}）", check_date, latest_trade)
    } else {
        format!("⚠ {} 个因子滞缓，{} 个覆盖率骤降（<80%）— 检查日 {} / 最近交易日 {}", stale_count, low_cov_count, check_date, latest_trade)
    };

    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            div { class: "flex items-center justify-between mb-6",
                h1 { class: "text-2xl font-bold text-gray-900 dark:text-white", "v24 因子健康监控" }
                button {
                    class: "text-sm px-3 py-1.5 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg text-gray-700 dark:text-gray-300 transition",
                    onclick: move |_| { load(); },
                    "刷新"
                }
            }

            div { class: "mb-4 p-3 border rounded-lg text-sm {summary_cls}", "{summary_text}" }

            div { class: "bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-xl overflow-hidden",
                table { class: "w-full text-sm",
                    thead {
                        tr { class: "bg-gray-50 dark:bg-gray-800 text-gray-500 dark:text-gray-400 text-xs uppercase",
                            th { class: "px-4 py-3 text-left", "因子代码" }
                            th { class: "px-4 py-3 text-right", "最新数据日" }
                            th { class: "px-4 py-3 text-right", "当日覆盖" }
                            th { class: "px-4 py-3 text-right", "近30日峰值" }
                            th { class: "px-4 py-3 text-right", "覆盖率" }
                            th { class: "px-4 py-3 text-center", "状态" }
                        }
                    }
                    tbody {
                        for f in factors {
                            FactorRow { data: f, latest_trade_date: latest_trade.to_string() }
                        }
                    }
                }
            }

            div { class: "mt-4 text-xs text-gray-400 dark:text-gray-500",
                p { "覆盖率 = 当日覆盖标的数 / 近 30 日单日最大覆盖数。<80% 视为相对自身正常水平的骤降(真实数据缺口)。" }
                p { "部分因子(研报评级/回购/大宗交易)天然只覆盖部分标的，故不用全市场做分母——避免天天误报。" }
            }
        }
    }
}

#[component]
fn FactorRow(data: Value, latest_trade_date: String) -> Element {
    let code = data["factor_code"].as_str().unwrap_or("-");
    let latest_date = data["latest_date"].as_str().unwrap_or("-");
    let latest_count = data["latest_count"].as_i64().unwrap_or(0);
    let recent_max = data["recent_max_count"].as_i64().unwrap_or(0);
    let coverage = data["coverage_pct"].as_f64().unwrap_or(0.0);
    let is_stale = data["is_stale"].as_bool().unwrap_or(false);
    let gap_days = data["gap_days"].as_i64().unwrap_or(0);

    let is_low_cov = coverage < 80.0 && !is_stale;
    let (status_text, status_cls) = if is_stale {
        (format!("滞缓 {} 天", gap_days), "bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-300")
    } else if is_low_cov {
        ("覆盖率骤降".to_string(), "bg-yellow-100 dark:bg-yellow-900/50 text-yellow-700 dark:text-yellow-300")
    } else {
        ("健康".to_string(), "bg-green-100 dark:bg-green-900/50 text-green-700 dark:text-green-300")
    };
    let row_cls = if is_stale {
        "bg-red-50/50 dark:bg-red-900/20"
    } else if is_low_cov {
        "bg-yellow-50/50 dark:bg-yellow-900/20"
    } else {
        ""
    };
    let latest_date_cls = if is_stale {
        "text-red-600 dark:text-red-400 font-medium"
    } else {
        "text-gray-700 dark:text-gray-300"
    };
    let cov_cls = if coverage < 80.0 {
        "text-yellow-600 dark:text-yellow-400 font-medium"
    } else if coverage < 95.0 {
        "text-gray-700 dark:text-gray-300"
    } else {
        "text-green-600 dark:text-green-400"
    };

    rsx! {
        tr { class: "border-t border-gray-100 dark:border-gray-800 {row_cls}",
            td { class: "px-4 py-3 font-mono text-xs text-gray-900 dark:text-white", "{code}" }
            td { class: "px-4 py-3 text-right font-mono text-xs {latest_date_cls}", "{latest_date}" }
            td { class: "px-4 py-3 text-right text-gray-700 dark:text-gray-300", "{latest_count}" }
            td { class: "px-4 py-3 text-right text-gray-500 dark:text-gray-400", "{recent_max}" }
            td { class: "px-4 py-3 text-right font-mono {cov_cls}", "{coverage:.1}%" }
            td { class: "px-4 py-3 text-center",
                span { class: "text-xs px-2 py-1 rounded-full {status_cls}", "{status_text}" }
            }
        }
    }
}
