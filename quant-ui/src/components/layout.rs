//! 应用布局组件 — 顶部导航栏 + 内容区域 (暂时最小化)

use dioxus::prelude::*;
use dioxus_router::prelude::*;

use crate::auth::AuthState;
use crate::Route;

#[component]
pub fn AppLayout() -> Element {
    let nav = use_navigator();
    let route = use_route::<Route>();
    let logged_in = AuthState::is_logged_in();
    let is_admin = AuthState::is_admin();
    let username = AuthState::user().map(|u| u.username).unwrap_or_default();

    fn link_css(is_active: bool, is_admin: bool) -> &'static str {
        if is_active && is_admin { "text-sm text-yellow-400 font-medium transition" }
        else if is_active { "text-sm text-blue-400 font-medium transition" }
        else if is_admin { "text-sm text-yellow-300 hover:text-yellow-200 transition" }
        else { "text-sm text-gray-300 hover:text-white transition" }
    }

    let dashboard_cls = link_css(route == Route::DashboardPage {}, false);
    let strategies_cls = link_css(route == Route::StrategiesPage {}, false);
    let accounts_cls = link_css(route == Route::AccountsPage {}, false);
    let data_cls = link_css(route == Route::DataPage {}, false);
    let tasks_cls = link_css(route == Route::TasksPage {}, false);
    let users_cls = link_css(route == Route::UsersPage {}, false);

    rsx! {
        div { class: "min-h-screen flex flex-col",
            nav { class: "bg-gray-900 border-b border-gray-800 px-6 py-3 flex items-center justify-between",
                div { class: "flex items-center gap-4",
                    Link {
                        to: Route::DashboardPage {},
                        class: "text-lg font-bold text-blue-400 hover:text-blue-300 transition",
                        "Quant"
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
                        span { class: "text-sm text-gray-400", "{username}" }
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
            main { class: "flex-1",
                Outlet::<Route> {}
            }
        }
    }
}
