//! Dioxus SPA 入口 — Router + 全局样式

#![allow(non_snake_case)]

use dioxus::prelude::*;
use dioxus_router::prelude::*;

mod api;
mod auth;
mod components;
mod pages;

use components::layout::AppLayout;

// ── 页面组件 (必须在 Route enum 所在作用域内) ──────────────

#[component]
fn DashboardPage() -> Element {
    let nav = use_navigator();
    if !auth::AuthState::is_logged_in() {
        nav.replace(Route::LoginPage {});
        return rsx! { div {} };
    }
    pages::dashboard::DashboardContent()
}

#[component]
fn StrategiesPage() -> Element {
    pages::strategies::StrategiesContent()
}

#[component]
fn AccountsPage() -> Element {
    pages::accounts::AccountsContent()
}

#[component]
fn AdminPage() -> Element {
    let nav = use_navigator();
    if !auth::AuthState::is_admin() {
        nav.replace(Route::LoginPage {});
        return rsx! { div {} };
    }
    pages::admin::AdminContent()
}

// 登录页面 — 委托到 pages::login 的完整实现
#[component]
fn LoginPage() -> Element {
    pages::login::LoginPage()
}

// ── 路由 ──────────────────────────────────────────────

#[rustfmt::skip]
#[derive(Clone, Debug, PartialEq, Routable)]
enum Route {
    #[layout(AppLayout)]
        #[route("/")]
        DashboardPage {},
        #[route("/strategies")]
        StrategiesPage {},
        #[route("/accounts")]
        AccountsPage {},
        #[route("/admin")]
        AdminPage {},
    #[end_layout]
    #[route("/login")]
    LoginPage {},
}

#[component]
fn App() -> Element {
    rsx! {
        Router::<Route> {}
    }
}

fn main() {
    dioxus::launch(App);
}
