//! 主题管理 — 日/夜间模式切换
//! - 页面加载时由 index.html 内联脚本恢复持久化模式（防闪烁）
//! - 系统切换 = 手动切换，始终写入 localStorage
//! - Rust 侧负责：手动切换按钮、监听系统变更同步图标

use dioxus::prelude::*;
use gloo_storage::{LocalStorage, Storage};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

fn is_html_dark() -> bool {
    web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.document_element())
        .map(|el| el.class_name().contains("dark"))
        .unwrap_or(false)
}

fn set_html_dark(dark: bool) {
    if let Some(el) = web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.document_element())
    {
        if dark {
            let _ = el.set_class_name("dark");
        } else {
            let _ = el.set_class_name("");
        }
    }
}

#[component]
pub fn ThemeToggle() -> Element {
    let mut dark = use_signal(|| is_html_dark());
    let mut listener_done = use_signal(|| false);

    // 监听系统日/夜间模式切换（index.html 同步更新 DOM + localStorage，这里只同步图标）
    use_effect(move || {
        if *listener_done.read() {
            return;
        }
        listener_done.set(true);

        let window = match web_sys::window() {
            Some(w) => w,
            None => return,
        };
        let mql = match window.match_media("(prefers-color-scheme: dark)") {
            Ok(Some(mql)) => mql,
            _ => return,
        };

        let mut dark_signal = dark;
        let cb = Closure::wrap(Box::new(move || {
            dark_signal.set(is_html_dark());
        }) as Box<dyn FnMut()>);

        let _ = mql.add_event_listener_with_callback("change", cb.as_ref().unchecked_ref());
        cb.forget();
    });

    rsx! {
        button {
            class: "text-lg px-1 py-0.5 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-800 transition leading-none",
            title: if *dark.read() { "切换到日间模式" } else { "切换到夜间模式" },
            onclick: move |_| {
                let new = !*dark.read();
                set_html_dark(new);
                // 手动切换同样持久化，刷新时由 index.html 恢复
                let _ = LocalStorage::set("theme", if new { "dark" } else { "light" });
                dark.set(new);
            },
            span { if *dark.read() { "☀️" } else { "🌙" } }
        }
    }
}
