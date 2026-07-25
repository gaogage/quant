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

/// P3-1: 实盘 NAV 累计收益 vs 回测同期累计收益 双线对比图。
/// dates 与 live_ret 一一对应（实盘 paper_nav_snapshot 序列）；
/// bt_dates 与 bt_ret 一一对应（回测 backtest_equity_curve 序列，日期范围可能不完全重合，独立画）。
#[component]
pub fn NavComparisonChart(
    dates: Vec<String>,
    live_ret: Vec<f64>,
    bt_dates: Vec<String>,
    bt_ret: Vec<f64>,
    canvas_id: String,
) -> Element {
    let cid = canvas_id.clone();
    let n = dates.len();
    let live = live_ret.clone();
    let bt = bt_ret.clone();
    let bt_n = bt_dates.len();
    let dates_for_label = dates.clone();

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

        let all_vals: Vec<f64> = live.iter().chain(bt.iter()).copied().collect();
        let min_v = all_vals.iter().cloned().fold(0.0_f64, f64::min);
        let max_v = all_vals.iter().cloned().fold(0.0_f64, f64::max);
        let range = (max_v - min_v).max(1.0_f64);
        let top_v = max_v + range * 0.1;
        let bot_v = min_v - range * 0.1;
        let v_range = (top_v - bot_v).max(1.0_f64);

        let to_x = |i: usize, len: usize| -> f64 { 40.0 + (i as f64 / (len.max(2) - 1) as f64) * (w - 60.0) };
        let to_y = |v: f64| -> f64 { h - 20.0 - ((v - bot_v) / v_range) * (h - 40.0) };

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
            let _ = ctx.fill_text_with_max_width(&format!("{:.1}%", v), 35.0, y, 40.0);
        }

        // 回测线（灰色虚线风格：用细线区分）
        if bt_n >= 2 {
            ctx.set_stroke_style_str("#9ca3af");
            ctx.set_line_width(1.5);
            ctx.begin_path();
            for (i, &v) in bt.iter().enumerate() {
                let x = to_x(i, bt_n);
                let y = to_y(v);
                if i == 0 { ctx.move_to(x, y); } else { ctx.line_to(x, y); }
            }
            ctx.stroke();
        }

        // 实盘线（蓝色实线，更粗）
        ctx.set_stroke_style_str("#3b82f6");
        ctx.set_line_width(2.5);
        ctx.begin_path();
        for (i, &v) in live.iter().enumerate() {
            let x = to_x(i, n);
            let y = to_y(v);
            if i == 0 { ctx.move_to(x, y); } else { ctx.line_to(x, y); }
        }
        ctx.stroke();

        // X 轴日期标签（首/中/末）
        ctx.set_font("9px monospace");
        ctx.set_fill_style_str("#9ca3af");
        ctx.set_text_align("center");
        ctx.set_text_baseline("top");
        let label_idxs = [0usize, n / 2, n - 1];
        for &i in &label_idxs {
            if let Some(d) = dates_for_label.get(i) {
                let x = to_x(i, n);
                let short = if d.len() >= 10 { &d[5..10] } else { d.as_str() };
                let _ = ctx.fill_text_with_max_width(short, x, h - 18.0, 50.0);
            }
        }
    });

    rsx! {
        div { class: "relative",
            canvas {
                id: "{canvas_id}",
                width: "700",
                height: "220",
                class: "w-full h-auto border border-gray-200 dark:border-gray-700 rounded-lg"
            }
            div { class: "flex gap-4 mt-2 text-xs text-gray-500 dark:text-gray-400",
                span { class: "flex items-center gap-1",
                    span { class: "inline-block w-3 h-0.5 bg-blue-500" } "实盘累计收益"
                }
                span { class: "flex items-center gap-1",
                    span { class: "inline-block w-3 h-0.5 bg-gray-400" } "回测同期累计收益"
                }
            }
        }
    }
}

/// 账号累计收益 vs 沪深300 基准累计收益双线对比图。
/// 两条曲线已各自归一化为累计收益率（%），共用同一日期-收益坐标系。
/// dates/acct_ret 为账号序列，bench_dates/bench_ret 为基准序列（日期可能不完全重合，独立按索引画）。
#[component]
pub fn ReturnVsBenchmarkChart(
    dates: Vec<String>,
    acct_ret: Vec<f64>,
    bench_dates: Vec<String>,
    bench_ret: Vec<f64>,
    canvas_id: String,
) -> Element {
    let cid = canvas_id.clone();
    let n = dates.len();
    let acct = acct_ret.clone();
    let bench = bench_ret.clone();
    let bench_n = bench_dates.len();
    let dates_for_label = dates.clone();

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

        let all_vals: Vec<f64> = acct.iter().chain(bench.iter()).copied().collect();
        let min_v = all_vals.iter().cloned().fold(0.0_f64, f64::min);
        let max_v = all_vals.iter().cloned().fold(0.0_f64, f64::max);
        let range = (max_v - min_v).max(1.0_f64);
        let top_v = max_v + range * 0.1;
        let bot_v = min_v - range * 0.1;
        let v_range = (top_v - bot_v).max(1.0_f64);

        let to_x = |i: usize, len: usize| -> f64 { 40.0 + (i as f64 / (len.max(2) - 1) as f64) * (w - 60.0) };
        let to_y = |v: f64| -> f64 { h - 20.0 - ((v - bot_v) / v_range) * (h - 40.0) };

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
            let _ = ctx.fill_text_with_max_width(&format!("{:.1}%", v), 35.0, y, 40.0);
        }

        // 基准线（沪深300，灰色细线）
        if bench_n >= 2 {
            ctx.set_stroke_style_str("#9ca3af");
            ctx.set_line_width(1.5);
            ctx.begin_path();
            for (i, &v) in bench.iter().enumerate() {
                let x = to_x(i, bench_n);
                let y = to_y(v);
                if i == 0 { ctx.move_to(x, y); } else { ctx.line_to(x, y); }
            }
            ctx.stroke();
        }

        // 账号线（蓝色粗线）
        ctx.set_stroke_style_str("#3b82f6");
        ctx.set_line_width(2.5);
        ctx.begin_path();
        for (i, &v) in acct.iter().enumerate() {
            let x = to_x(i, n);
            let y = to_y(v);
            if i == 0 { ctx.move_to(x, y); } else { ctx.line_to(x, y); }
        }
        ctx.stroke();

        // X 轴日期标签（首/中/末）
        ctx.set_font("9px monospace");
        ctx.set_fill_style_str("#9ca3af");
        ctx.set_text_align("center");
        ctx.set_text_baseline("top");
        let label_idxs = [0usize, n / 2, n - 1];
        for &i in &label_idxs {
            if let Some(d) = dates_for_label.get(i) {
                let x = to_x(i, n);
                let short = if d.len() >= 10 { &d[5..10] } else { d.as_str() };
                let _ = ctx.fill_text_with_max_width(short, x, h - 18.0, 50.0);
            }
        }
    });

    rsx! {
        div { class: "relative",
            canvas {
                id: "{canvas_id}",
                width: "700",
                height: "220",
                class: "w-full h-auto border border-gray-200 dark:border-gray-700 rounded-lg"
            }
            div { class: "flex gap-4 mt-2 text-xs text-gray-500 dark:text-gray-400",
                span { class: "flex items-center gap-1",
                    span { class: "inline-block w-3 h-0.5 bg-blue-500" } "账号累计收益"
                }
                span { class: "flex items-center gap-1",
                    span { class: "inline-block w-3 h-0.5 bg-gray-400" } "沪深300"
                }
            }
        }
    }
}
