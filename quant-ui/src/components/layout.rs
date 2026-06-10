//! 应用布局组件 — 顶部导航栏 + 连接状态 + 内容区域

use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;

use crate::api;
use crate::auth::AuthState;
use crate::components::connection::ConnectionDot;
use crate::theme::ThemeToggle;
use crate::Route;

#[component]
pub fn AppLayout() -> Element {
    let nav = use_navigator();
    let route = use_route::<Route>();
    let logged_in = AuthState::is_logged_in();
    let is_admin = AuthState::is_admin();
    let username = AuthState::user().map(|u| u.username).unwrap_or_default();

    // ── 服务器连接状态 ──────────────────────────────────
    let mut connected = use_signal(|| true);
    let mut first_check = use_signal(|| true);

    use_effect(move || {
        if !*first_check.read() {
            return;
        }
        first_check.set(false);

        spawn(async move {
            // 首次检查
            connected.set(api::health_check().await.is_ok());

            loop {
                TimeoutFuture::new(30_000).await;
                connected.set(api::health_check().await.is_ok());
            }
        });
    });

    fn link_css(is_active: bool, is_admin: bool) -> &'static str {
        if is_active && is_admin { "text-sm text-yellow-500 dark:text-yellow-400 font-bold transition" }
        else if is_active { "text-sm text-blue-600 dark:text-blue-400 font-bold transition" }
        else if is_admin { "text-sm text-yellow-600 dark:text-yellow-300 hover:text-yellow-500 dark:hover:text-yellow-200 transition" }
        else { "text-sm text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white transition" }
    }

    let dashboard_cls = link_css(route == Route::DashboardPage {}, false);
    let strategies_cls = link_css(route == Route::StrategiesPage {}, false);
    let accounts_cls = link_css(route == Route::AccountsPage {}, false);
    let data_cls = link_css(route == Route::DataPage {}, false);
    let tasks_cls = link_css(route == Route::TasksPage {}, false);
    let users_cls = link_css(route == Route::UsersPage {}, false);

    rsx! {
        div { class: "min-h-screen flex flex-col",
            nav { class: "bg-white dark:bg-gray-900 border-b border-gray-200 dark:border-gray-800 px-6 py-3 flex items-center justify-between transition-colors",
                div { class: "flex items-center gap-4",
                    Link {
                        to: Route::DashboardPage {},
                        class: "text-lg font-bold text-blue-400 hover:text-blue-300 transition",
                        span { class: "inline-flex items-center gap-1",
                            "Quant"
                            if logged_in {
                                ConnectionDot { connected: *connected.read() }
                            }
                        }
                    }
                    if logged_in {
                        Link { to: Route::DashboardPage {}, class: "{dashboard_cls}", "仪表盘" }
                        Link { to: Route::StrategiesPage {}, class: "{strategies_cls}", "策略" }
                        Link { to: Route::AccountsPage {}, class: "{accounts_cls}", "账号" }
                        if is_admin {
                            Link { to: Route::DataPage {}, class: "{data_cls}", "数据" }
                            Link { to: Route::TasksPage {}, class: "{tasks_cls}", "任务" }
                            Link { to: Route::UsersPage {}, class: "{users_cls}", "用户" }
                        }
                    }
                }
                div { class: "flex items-center gap-4",
                    if logged_in {
                        ThemeToggle {}
                        span { class: "text-sm text-gray-500 dark:text-gray-400", "{username}" }
                        button {
                            class: "text-sm text-red-400 hover:text-red-300 transition",
                            onclick: {
                                let nav = nav.clone();
                                move |_| {
                                    AuthState::logout();
                                    let _ = nav.replace(Route::LoginPage {});
                                }
                            },
                            "退出"
                        }
                    } else {
                        Link {
                            to: Route::LoginPage {},
                            class: "text-sm text-blue-400 hover:text-blue-300 transition",
                            "登录"
                        }
                    }
                }
            }

            // 断连警告条
            if !*connected.read() {
                div { class: "bg-red-50 dark:bg-red-900/30 border-b border-red-300 dark:border-red-700 px-6 py-2 text-center transition",
                    span { class: "text-sm text-red-600 dark:text-red-400 font-medium",
                        "⚠ 服务连接不可用 — 请检查后台服务是否正常运行"
                    }
                    span { class: "text-xs text-red-400 dark:text-red-500 ml-3", "（每30秒自动重试）" }
                }
            }

            main { class: "flex-1",
                Outlet::<Route> {}
            }
        }
    }
}
