//! 服务器连接状态指示器 — 小圆点，绿色=正常，红色=不可用

use dioxus::prelude::*;

/// 连接状态圆点
#[component]
pub fn ConnectionDot(connected: bool) -> Element {
    let dot_color = if connected {
        "bg-green-500"
    } else {
        "bg-red-500 animate-pulse"
    };
    let tooltip = if connected {
        "服务正常"
    } else {
        "服务连接不可用"
    };

    rsx! {
        span {
            class: "inline-block w-2.5 h-2.5 rounded-full {dot_color}",
            title: tooltip,
        }
    }
}
