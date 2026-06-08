//! 应用布局组件 — 顶部导航栏 + 内容区域 (暂时最小化)

use dioxus::prelude::*;
use dioxus_router::prelude::*;

use crate::auth::AuthState;
use crate::Route;

#[component]
pub fn AppLayout() -> Element {
    let nav = use_navigator();
    let logged_in = AuthState::is_logged_in();
    let is_admin = AuthState::is_admin();
    let username = AuthState::user().map(|u| u.username).unwrap_or_default();

    rsx! {
        div { class: "min-h-screen flex flex-col",
            nav { class: "bg-gray-900 border-b border-gray-800 px-6 py-3 flex items-center justify-between",
                div { class: "flex items-center gap-6",
                    Link {
                        to: Route::DashboardPage {},
                        class: "text-lg font-bold text-blue-400 hover:text-blue-300 transition",
                        "Quant Platform"
                    }
                    if logged_in {
                        Link {
                            to: Route::DashboardPage {},
                            class: "text-sm text-gray-300 hover:text-white transition",
                            "仪表盘"
                        }
                        Link {
                            to: Route::StrategiesPage {},
                            class: "text-sm text-gray-300 hover:text-white transition",
                            "策略"
                        }
                        Link {
                            to: Route::AccountsPage {},
                            class: "text-sm text-gray-300 hover:text-white transition",
                            "账号"
                        }
                        if is_admin {
                            Link {
                                to: Route::AdminPage {},
                                class: "text-sm text-yellow-400 hover:text-yellow-300 transition",
                                "管理"
                            }
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
