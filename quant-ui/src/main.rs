//! Dioxus SPA 入口 — Router + 全局样式

#![allow(non_snake_case)]

use dioxus::prelude::*;

mod api;
mod auth;
mod components;
mod pages;
mod theme;

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
fn DataPage() -> Element {
    if !auth::AuthState::is_admin() { let nav = use_navigator(); nav.replace(Route::LoginPage {}); return rsx! { div {} }; }
    pages::admin::data::DataSyncPage()
}

#[component]
fn TasksPage() -> Element {
    if !auth::AuthState::is_admin() { let nav = use_navigator(); nav.replace(Route::LoginPage {}); return rsx! { div {} }; }
    pages::admin::tasks::TasksPage()
}

#[component]
fn UsersPage() -> Element {
    if !auth::AuthState::is_admin() { let nav = use_navigator(); nav.replace(Route::LoginPage {}); return rsx! { div {} }; }
    pages::admin::users::UsersPage()
}

// 登录页面
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
        #[route("/data")]
        DataPage {},
        #[route("/tasks")]
        TasksPage {},
        #[route("/users")]
        UsersPage {},
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
