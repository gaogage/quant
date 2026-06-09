//! 简易图表组件 — 基于 HTML Canvas 绘制

use dioxus::prelude::*;
use wasm_bindgen::JsCast;

/// 累计收益率折线图（账号 + 3 基准对比）
#[component]
pub fn CumulativeLineChart(
    years: Vec<String>,
    acct: Vec<f64>,
    csi300: Vec<f64>,
    gold: Vec<f64>,
    sp500: Vec<f64>,
    canvas_id: String,
) -> Element {
    let cid = canvas_id.clone();
    let n = years.len();
    let points_acct = acct.clone();
    let points_csi = csi300.clone();
    let points_gold = gold.clone();
    let points_sp = sp500.clone();

    use_effect(move || {
        if n < 2 { return; }
        let window = web_sys::window().unwrap();
        let document = window.document().unwrap();
        let canvas_el = document.get_element_by_id(&cid);
        if canvas_el.is_none() { return; }
        let canvas: web_sys::HtmlCanvasElement = canvas_el.unwrap().dyn_into().unwrap();
        let ctx = canvas.get_context("2d").unwrap().unwrap();
        let ctx: web_sys::CanvasRenderingContext2d = ctx.dyn_into().unwrap();

        let w = canvas.width() as f64;
        let h = canvas.height() as f64;
        if w < 10.0 || h < 10.0 { return; }

        let all_vals: Vec<f64> = points_acct.iter().chain(points_csi.iter()).chain(points_gold.iter()).chain(points_sp.iter()).copied().collect();
        let min_v = all_vals.iter().cloned().fold(0.0_f64, f64::min);
        let max_v = all_vals.iter().cloned().fold(0.0_f64, f64::max);
        let range = (max_v - min_v).max(1.0_f64);
        // 留 10% 上下边距
        let top_v = max_v + range * 0.1;
        let bot_v = min_v - range * 0.1;
        let v_range = (top_v - bot_v).max(1.0_f64);

        let to_x = |i: usize| -> f64 { 40.0 + (i as f64 / (n - 1) as f64) * (w - 60.0) };
        let to_y = |v: f64| -> f64 { h - 20.0 - ((v - bot_v) / v_range) * (h - 40.0) };

        // 清空
        ctx.clear_rect(0.0, 0.0, w, h);

        // 零线
        let zero_y = to_y(0.0);
        ctx.set_stroke_style_str("#9ca3af");
        ctx.set_line_width(0.5);
        ctx.begin_path();
        ctx.move_to(40.0, zero_y);
        ctx.line_to(w - 20.0, zero_y);
        ctx.stroke();

        // 网格线
        ctx.set_stroke_style_str("#e5e7eb");
        ctx.set_line_width(0.3);
        for i in 0..=4 {
            let y = 20.0 + (i as f64 / 4.0) * (h - 40.0);
            ctx.begin_path();
            ctx.move_to(40.0, y);
            ctx.line_to(w - 20.0, y);
            ctx.stroke();
        }

        // Y 轴标签
        ctx.set_font("10px monospace");
        ctx.set_fill_style_str("#9ca3af");
        ctx.set_text_align("right");
        ctx.set_text_baseline("middle");
        for i in 0..=3 {
            let v = bot_v + (i as f64 / 3.0) * v_range;
            let y = to_y(v);
            let _ = ctx.fill_text_with_max_width(&format!("{:.0}%", v), 35.0, y, 40.0);
        }

        // 绘制折线
        let draw_line = |data: &[f64], color: &str, width: f64| {
            ctx.set_stroke_style_str(color);
            ctx.set_line_width(width);
            ctx.begin_path();
            for (i, &v) in data.iter().enumerate() {
                let x = to_x(i);
                let y = to_y(v);
                if i == 0 { ctx.move_to(x, y); } else { ctx.line_to(x, y); }
            }
            ctx.stroke();
        };

        draw_line(&points_csi, "#d1d5db", 1.5);
        draw_line(&points_gold, "#facc15", 1.5);
        draw_line(&points_sp, "#4ade80", 1.5);
        draw_line(&points_acct, "#3b82f6", 2.5);

        // X 轴年份标签
        ctx.set_font("9px monospace");
        ctx.set_fill_style_str("#9ca3af");
        ctx.set_text_align("center");
        ctx.set_text_baseline("top");
        for (i, y) in years.iter().enumerate() {
            if i % 1 == 0 || i == n-1 {
                let x = to_x(i);
                let _ = ctx.fill_text_with_max_width(&y[2..], x, h - 18.0, 40.0);
            }
        }
    });

    rsx! {
        div { class: "relative",
            canvas {
                id: "{canvas_id}",
                width: "600",
                height: "200",
                class: "w-full h-auto border border-gray-200 dark:border-gray-700 rounded-lg"
            }
        }
    }
}
