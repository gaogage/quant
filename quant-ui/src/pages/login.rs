//! 登录页面 — 用户名/密码表单 + JWT 认证

use dioxus::prelude::*;

use crate::api;
use crate::auth::AuthState;
use crate::Route;

#[component]
pub fn LoginPage() -> Element {
    let nav = use_navigator();

    // 已登录直接跳转仪表盘
    if AuthState::is_logged_in() {
        nav.replace(Route::DashboardPage {});
        return rsx! { div {} };
    }

    let mut username = use_signal(|| String::new());
    let mut password = use_signal(|| String::new());
    let mut error = use_signal(|| String::new());
    let mut loading = use_signal(|| false);

    let on_keydown = move |evt: KeyboardEvent| {
        if evt.key() == Key::Enter {
            if loading() { return; }
            let uname = username.read().clone();
            let passwd = password.read().clone();
            if uname.is_empty() || passwd.is_empty() {
                error.set("请输入用户名和密码".to_string());
                return;
            }
            loading.set(true);
            error.set(String::new());
            let nav = nav.clone();
            spawn(async move {
                match api::login(&uname, &passwd).await {
                    Ok(data) => {
                        AuthState::login(&data.access_token, &data.refresh_token, data.user);
                        nav.replace(Route::DashboardPage {});
                    }
                    Err(e) => {
                        error.set(e);
                        loading.set(false);
                    }
                }
            });
        }
    };

    rsx! {
        div { class: "min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-950",
            div { class: "w-full max-w-md p-8 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-2xl",
                // 标题
                div { class: "text-center mb-8",
                    h1 { class: "text-3xl font-bold text-blue-600 dark:text-blue-400 mb-2", "Quant Platform" }
                    p { class: "text-gray-500 dark:text-gray-400 text-sm", "量化交易管理平台" }
                }

                // 错误提示
                if !error.read().is_empty() {
                    div { class: "mb-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm",
                        "{error}"
                    }
                }

                // 表单
                div { class: "space-y-4",
                    // 用户名
                    div {
                        label { class: "block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1", "用户名" }
                        input {
                            class: "w-full px-4 py-2.5 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg
                                    text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:border-blue-500
                                    focus:ring-1 focus:ring-blue-500 transition",
                            r#type: "text",
                            placeholder: "请输入用户名",
                            value: "{username}",
                            oninput: move |evt| username.set(evt.value()),
                            onkeydown: on_keydown,
                        }
                    }
                    // 密码
                    div {
                        label { class: "block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1", "密码" }
                        input {
                            class: "w-full px-4 py-2.5 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg
                                    text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:border-blue-500
                                    focus:ring-1 focus:ring-blue-500 transition",
                            r#type: "password",
                            placeholder: "请输入密码",
                            value: "{password}",
                            oninput: move |evt| password.set(evt.value()),
                            onkeydown: on_keydown,
                        }
                    }
                    // 提交按钮
                    button {
                        class: "w-full py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-300 dark:disabled:bg-gray-700
                                disabled:text-gray-500 text-white font-medium rounded-lg transition
                                focus:outline-none focus:ring-2 focus:ring-blue-500",
                        disabled: *loading.read(),
                        onclick: move |_| {
                            if loading() { return; }
                            let uname = username.read().clone();
                            let passwd = password.read().clone();
                            if uname.is_empty() || passwd.is_empty() {
                                error.set("请输入用户名和密码".to_string());
                                return;
                            }
                            loading.set(true);
                            error.set(String::new());
                            let nav_clone = nav.clone();
                            spawn(async move {
                                match api::login(&uname, &passwd).await {
                                    Ok(data) => {
                                        AuthState::login(&data.access_token, &data.refresh_token, data.user);
                                        nav_clone.replace(Route::DashboardPage {});
                                    }
                                    Err(e) => {
                                        error.set(e);
                                        loading.set(false);
                                    }
                                }
                            });
                        },
                        if *loading.read() {
                            span { class: "inline-flex items-center gap-2",
                                svg {
                                    class: "animate-spin h-4 w-4",
                                    view_box: "0 0 24 24",
                                    circle { class: "opacity-25", cx: "12", cy: "12", r: "10",
                                        stroke: "currentColor", stroke_width: "4", fill: "none" }
                                    path { class: "opacity-75", fill: "currentColor",
                                        d: "M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                                    }
                                }
                                "登录中..."
                            }
                        } else {
                            "登录"
                        }
                    }
                }

                // 底部信息
                p { class: "mt-6 text-center text-xs text-gray-400 dark:text-gray-600",
                    "Quant Platform v0.1 · SCP Quantitative Trading"
                }
            }
        }
    }
}
