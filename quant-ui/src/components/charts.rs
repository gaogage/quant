//! 简易图表组件 - 基于 HTML Canvas 绘制

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
/// live_nav/live_daily 与 dates 一一对应（当日净值与当日收益），仅用于 hover tooltip。
/// 鼠标 hover 显示竖线 + 双线交点标记 + 跟随 tooltip（日期/净值/当日收益/累计收益/回测同期/偏离）。
#[derive(Clone)]
struct NavHoverInfo {
    date: String,
    x_css: f64,
    live_y_css: f64,
    bt_y_css: Option<f64>,
    css_w: f64,
    css_h: f64,
    nav: f64,
    daily: f64,
    live_cum: f64,
    bt_cum: Option<f64>,
}

#[component]
pub fn NavComparisonChart(
    dates: Vec<String>,
    live_ret: Vec<f64>,
    live_nav: Vec<f64>,
    live_daily: Vec<f64>,
    bt_dates: Vec<String>,
    bt_ret: Vec<f64>,
    canvas_id: String,
) -> Element {
    let cid = canvas_id.clone();
    let n = dates.len();
    let live = live_ret.clone();
    let navs = live_nav.clone();
    let dailies = live_daily.clone();
    let bt = bt_ret.clone();
    let bt_n = bt_dates.len();
    let dates_for_label = dates.clone();

    // hover 状态：竖线/交点/tooltip 均由 Dioxus div 渲染
    let tooltip = use_signal(|| Option::<NavHoverInfo>::None);
    let tooltip_data = (*tooltip.read()).clone();

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

        // hover 事件：mousemove 反算最近数据点，更新 tooltip signal（Dioxus div 渲染竖线/交点/面板）
        let live_c = live.clone();
        let navs_c = navs.clone();
        let dailies_c = dailies.clone();
        let bt_c = bt.clone();
        let bt_dates_c = bt_dates.clone();
        let dates_c = dates_for_label.clone();
        let mut tt = tooltip.clone();
        let nn = n;
        let btn = bt_n;

        let canvas_for_events = canvas.clone();
        let closure = wasm_bindgen::closure::Closure::wrap(Box::new(move |event: web_sys::MouseEvent| {
            let rect = canvas.get_bounding_client_rect();
            let css_w = rect.width();
            let css_h = rect.height();
            if css_w <= 0.0 || css_h <= 0.0 { return; }
            // canvas 内部绘图分辨率(固定 700x220)与 CSS 实际渲染尺寸(因 w-full 响应式缩放)不一致。
            // 鼠标事件坐标是 CSS 像素，乘 scale 换算进内部坐标系后才能套用 to_x/to_y 反算，
            // 反算出的内部坐标再除以 scale 回到 CSS 像素供 DOM 定位。
            let scale_x = w / css_w;
            let scale_y = h / css_h;
            let mx = (event.client_x() as f64 - rect.left()) * scale_x;
            if mx < 40.0 || mx > w - 20.0 || nn < 2 {
                tt.set(None);
                return;
            }
            let frac = (mx - 40.0) / (w - 60.0);
            let idx = ((frac * (nn - 1) as f64).round() as usize).min(nn - 1);

            // 重建内部坐标系映射（w/h/bot_v/v_range 均为 Copy 捕获）
            let to_x = |i: usize, len: usize| -> f64 { 40.0 + (i as f64 / (len.max(2) - 1) as f64) * (w - 60.0) };
            let to_y = |v: f64| -> f64 { h - 20.0 - ((v - bot_v) / v_range) * (h - 40.0) };

            let date = dates_c.get(idx).cloned().unwrap_or_default();
            let live_v = live_c.get(idx).copied().unwrap_or(0.0);
            // 回测线按日期对齐取同期点（两条序列日期范围可能不完全重合）
            let bt_point = bt_dates_c.iter().position(|d| d == &date)
                .filter(|&bi| bi < btn)
                .map(|bi| (bi, bt_c.get(bi).copied().unwrap_or(0.0)));

            let x_css = to_x(idx, nn) / scale_x;
            let live_y_css = to_y(live_v) / scale_y;
            let bt_y_css = bt_point.map(|(bi, bv)| to_y(bv) / scale_y);

            tt.set(Some(NavHoverInfo {
                date,
                x_css,
                live_y_css,
                bt_y_css,
                css_w,
                css_h,
                nav: navs_c.get(idx).copied().unwrap_or(0.0),
                daily: dailies_c.get(idx).copied().unwrap_or(0.0),
                live_cum: live_v,
                bt_cum: bt_point.map(|(_, bv)| bv),
            }));
        }) as Box<dyn FnMut(web_sys::MouseEvent)>);

        canvas_for_events.add_event_listener_with_callback("mousemove", closure.as_ref().unchecked_ref()).unwrap();
        let mut tt2 = tooltip.clone();
        let leave_closure = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
            tt2.set(None);
        }) as Box<dyn FnMut()>);
        canvas_for_events.add_event_listener_with_callback("mouseleave", leave_closure.as_ref().unchecked_ref()).unwrap();
        closure.forget();
        leave_closure.forget();
    });

    rsx! {
        div { class: "relative",
            canvas {
                id: "{canvas_id}",
                width: "700",
                height: "220",
                class: "w-full h-auto border border-gray-200 dark:border-gray-700 rounded-lg"
            }
            // hover 指示层：竖线 + 双线交点 + 跟随 tooltip（均不拦截鼠标事件）
            if let Some(h) = tooltip_data.as_ref() {
                div { style: "position:absolute;left:{h.x_css:.1}px;top:0;width:1px;height:{h.css_h:.0}px;background:rgba(59,130,246,0.35);pointer-events:none;" }
                div { style: "position:absolute;left:{h.x_css - 5.0:.1}px;top:{h.live_y_css - 5.0:.1}px;width:10px;height:10px;border-radius:50%;background:#3b82f6;border:2px solid #fff;box-shadow:0 0 3px rgba(0,0,0,0.4);pointer-events:none;" }
                if let Some(bt_y) = h.bt_y_css {
                    div { style: "position:absolute;left:{h.x_css - 4.0:.1}px;top:{bt_y - 4.0:.1}px;width:9px;height:9px;border-radius:50%;background:#9ca3af;border:2px solid #fff;box-shadow:0 0 3px rgba(0,0,0,0.4);pointer-events:none;" }
                }
                {
                    // tooltip 靠右边界时翻到左侧；纵向贴实盘交点上方，超界则下移
                    let flip = h.x_css + 200.0 > h.css_w;
                    let left = if flip { (h.x_css - 200.0 - 12.0).max(4.0) } else { h.x_css + 12.0 };
                    let top = (h.live_y_css - 108.0).max(4.0).min((h.css_h - 112.0).max(4.0));
                    let dev = h.bt_cum.map(|bv| h.live_cum - bv);
                    let dev_line = dev.map(|d| {
                        let cls = if d.abs() > 2.0 { "text-red-600" } else { "text-gray-500" };
                        rsx! { div { class: "{cls}", "偏离: {d:+.2}%" } }
                    });
                    let bt_line = h.bt_cum.map(|bv| {
                        rsx! { div { class: "text-gray-500", "回测同期: {bv:+.2}%" } }
                    });
                    rsx! {
                        div {
                            style: "position:absolute;left:{left:.0}px;top:{top:.0}px;width:188px;padding:6px 8px;background:rgba(255,255,255,0.97);border:1px solid #d1d5db;border-radius:6px;box-shadow:0 2px 8px rgba(0,0,0,0.15);pointer-events:none;z-index:10;font-size:11px;line-height:1.6;",
                            class: "text-gray-700",
                            div { class: "font-mono text-gray-500 border-b border-gray-100 mb-1 pb-0.5", "{h.date}" }
                            div { "净值: ¥{h.nav:.0}" }
                            div { "当日收益: {h.daily:+.2}%" }
                            div { class: "text-blue-600", "实盘累计: {h.live_cum:+.2}%" }
                            {bt_line}
                            {dev_line}
                        }
                    }
                }
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
/// 鼠标 hover 显示 tooltip（日期/本策略收益/沪深300收益）。
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

    // tooltip 状态：鼠标 hover 时显示（日期, 本策略收益, 沪深300收益）
    let tooltip = use_signal(|| Option::<(String, f64, Option<f64>)>::None);
    let tooltip_data = (*tooltip.read()).clone();

    use_effect(move || {
        if n < 2 { return; }
        let window = web_sys::window().unwrap();
        let document = window.document().unwrap();
        let canvas_el = document.get_element_by_id(&cid);
        if canvas_el.is_none() { return; }
        let canvas: web_sys::HtmlCanvasElement = canvas_el.unwrap().dyn_into().unwrap();

        let w = canvas.width() as f64;
        let h = canvas.height() as f64;
        if w < 10.0 || h < 10.0 { return; }

        // 先绘图（用 canvas 引用，绘完释放，再 move canvas 进事件闭包）
        {
            let ctx = canvas.get_context("2d").unwrap().unwrap();
            let ctx: web_sys::CanvasRenderingContext2d = ctx.dyn_into().unwrap();

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
        } // ctx 在此 drop，canvas 可 move 进闭包

        // hover 事件：mousemove 更新 tooltip signal（tooltip 由 Dioxus div 渲染）
        let acct_c = acct.clone();
        let bench_c = bench.clone();
        let dates_c = dates_for_label.clone();
        let bench_dates_c = bench_dates.clone();
        let mut tt = tooltip.clone();
        let nn = n;

        let canvas_for_events = canvas.clone();
        let closure = wasm_bindgen::closure::Closure::wrap(Box::new(move |event: web_sys::MouseEvent| {
            let rect = canvas.get_bounding_client_rect();
            // canvas 内部绘图分辨率(w，固定 700)与 CSS 实际渲染宽度(rect.width()，因
            // w-full 响应式布局而缩放，如 587px)不一致。鼠标事件坐标是 CSS 像素，
            // 必须按 内部宽度/CSS宽度 缩放后才能套用为内部坐标系设计的 to_x 反算公式，
            // 否则索引系统性偏移(偏移量随位置线性增大)，hover 到的点与视觉位置不一致。
            let css_w = rect.width();
            let scale_x = if css_w > 0.0 { w / css_w } else { 1.0 };
            let mx = (event.client_x() as f64 - rect.left()) * scale_x;
            if mx < 40.0 || mx > w - 20.0 || nn < 2 {
                tt.set(None);
                return;
            }
            let frac = (mx - 40.0) / (w - 60.0);
            let idx = ((frac * (nn - 1) as f64).round() as usize).min(nn - 1);

            let date = dates_c.get(idx).cloned().unwrap_or_default();
            let acct_v = acct_c.get(idx).copied().unwrap_or(0.0);
            let bench_v = bench_dates_c.iter().position(|d| d == &date).and_then(|bi| bench_c.get(bi).copied());

            tt.set(Some((date, acct_v, bench_v)));
        }) as Box<dyn FnMut(web_sys::MouseEvent)>);

        canvas_for_events.add_event_listener_with_callback("mousemove", closure.as_ref().unchecked_ref()).unwrap();
        let mut tt2 = tooltip.clone();
        let leave_closure = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
            tt2.set(None);
        }) as Box<dyn FnMut()>);
        canvas_for_events.add_event_listener_with_callback("mouseleave", leave_closure.as_ref().unchecked_ref()).unwrap();
        closure.forget();
        leave_closure.forget();
    });

    rsx! {
        div { class: "relative",
            canvas {
                id: "{canvas_id}",
                width: "700",
                height: "220",
                class: "w-full h-auto border border-gray-200 dark:border-gray-700 rounded-lg"
            }
            // hover tooltip（Dioxus div 渲染，右上角显示数值）
            if let Some((date, acct_v, bench_v)) = tooltip_data.as_ref() {
                div {
                    class: "absolute top-2 right-2 px-2 py-1 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded shadow text-xs text-gray-700 dark:text-gray-300 pointer-events-none z-10",
                    div { class: "font-mono text-gray-500 mb-0.5", "{date}" }
                    div { class: "text-blue-600 dark:text-blue-400", "本策略: {acct_v:.2}%" }
                    if let Some(bv) = bench_v {
                        div { class: "text-gray-500", "沪深300: {bv:.2}%" }
                    }
                }
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
