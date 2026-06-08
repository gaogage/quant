//! 管理员页面 — 用户管理（表格+编辑弹窗）+ 定时任务 + 数据同步

use dioxus::prelude::*;
use serde_json::{json, Value};

use crate::api;

#[component]
pub fn AdminContent() -> Element {
    let mut users = use_signal(|| Vec::<Value>::new());
    let mut tasks = use_signal(|| Vec::<Value>::new());
    let mut sync = use_signal(|| Vec::<Value>::new());
    let mut tab = use_signal(|| String::from("users"));
    let mut loading = use_signal(|| true);
    let mut error = use_signal(|| String::new());
    let mut message = use_signal(|| String::new());

    // 创建用户
    let mut new_name = use_signal(|| String::new());
    let mut new_pass = use_signal(|| String::new());
    let mut new_role = use_signal(|| String::from("user"));
    let mut new_saving = use_signal(|| false);

    // 编辑弹窗
    let mut editing = use_signal(|| Option::<Value>::None);
    let mut edit_role = use_signal(|| String::new());
    let mut edit_status = use_signal(|| String::new());
    let mut edit_password = use_signal(|| String::new());
    let mut edit_saving = use_signal(|| false);

    let load = move || {
        spawn(async move {
            let u = api::admin_list_users().await;
            let t = api::admin_list_tasks().await;
            let s = api::admin_sync_status().await;
            if let Ok(v) = u { if let Some(arr) = v["data"].as_array() { users.set(arr.clone()); } }
            if let Ok(v) = t { if let Some(arr) = v["data"].as_array() { tasks.set(arr.clone()); } }
            if let Ok(v) = s { if let Some(arr) = v["data"].as_array() { sync.set(arr.clone()); } }
            loading.set(false);
        });
    };

    use_effect(move || { load(); });

    // 创建用户
    let do_create = move |_| {
        if new_name.read().is_empty() || new_pass.read().is_empty() {
            error.set("请输入用户名和密码".to_string());
            return;
        }
        new_saving.set(true);
        error.set(String::new());
        let payload = json!({
            "username": new_name.read().clone(),
            "password": new_pass.read().clone(),
            "role": new_role.read().clone(),
        });
        spawn(async move {
            match api::admin_create_user(&payload).await {
                Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => {
                    new_name.set(String::new());
                    new_pass.set(String::new());
                    message.set("用户创建成功".to_string());
                    load();
                }
                Ok(v) => error.set(v["message"].as_str().unwrap_or("创建失败").to_string()),
                Err(e) => error.set(e),
            }
            new_saving.set(false);
        });
    };

    // 打开编辑
    let mut open_edit = move |user: Value| {
        edit_role.set(user["role"].as_str().unwrap_or("user").to_string());
        edit_status.set(user["status"].as_str().unwrap_or("active").to_string());
        edit_password.set(String::new());
        editing.set(Some(user));
        error.set(String::new());
        message.set(String::new());
    };

    // 保存编辑
    let save_edit = move |_| {
        let user = editing.read().clone();
        if let Some(ref u) = user {
            let uid = u["user_id"].as_str().unwrap_or("").to_string();
            let role = edit_role.read().clone();
            let status = edit_status.read().clone();
            let pass = edit_password.read().clone();
            edit_saving.set(true);
            error.set(String::new());
            let payload = json!({"role": role, "status": status});
            spawn(async move {
                match api::admin_update_user(&uid, &payload).await {
                    Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => {}
                    Ok(v) => { error.set(v["message"].as_str().unwrap_or("修改失败").to_string()); edit_saving.set(false); return; }
                    Err(e) => { error.set(e); edit_saving.set(false); return; }
                }
                if !pass.is_empty() {
                    match api::admin_reset_password(&uid, &pass).await {
                        Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => {}
                        Ok(v) => { error.set(v["message"].as_str().unwrap_or("密码重置失败").to_string()); edit_saving.set(false); return; }
                        Err(e) => { error.set(e); edit_saving.set(false); return; }
                    }
                }
                message.set("用户信息已更新".to_string());
                editing.set(None);
                edit_saving.set(false);
                load();
            });
        }
    };

    // 删除用户
    let do_delete = move |uid: String, username: String| {
        spawn(async move {
            let confirm = web_sys::window()
                .and_then(|w| w.confirm_with_message(&format!("确认删除用户 {}？此操作不可撤销。", username)).ok())
                .unwrap_or(false);
            if !confirm { return; }
            match api::admin_delete_user(&uid).await {
                Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => { message.set("用户已删除".to_string()); load(); }
                Ok(v) => error.set(v["message"].as_str().unwrap_or("删除失败").to_string()),
                Err(e) => error.set(e),
            }
        });
    };

    // 加载中
    if *loading.read() {
        return rsx! {
            div { class: "p-6 max-w-6xl mx-auto",
                div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" }
            }
        };
    }

    // 编辑弹窗（独立 rsx! 分支，避免 rsx! 内 if-let 的引号冲突）
    if let Some(ref edit_user) = editing.read().clone() {
        let edit_username = edit_user["username"].as_str().unwrap_or("-").to_string();
        return rsx! {
            div { class: "p-6 max-w-6xl mx-auto",
                button {
                    class: "mb-4 text-sm text-blue-400 hover:text-blue-300 transition",
                    onclick: move |_| editing.set(None),
                    "← 返回用户列表"
                }
            }
            // 遮罩
            div { class: "fixed inset-0 bg-black/60 z-50 flex items-center justify-center",
                onclick: move |_| editing.set(None),
                // 弹窗
                div { class: "bg-gray-900 border border-gray-700 rounded-2xl p-6 w-full max-w-md mx-4 shadow-2xl",
                    onclick: move |evt| evt.stop_propagation(),
                    h2 { class: "text-lg font-bold text-white mb-1", "编辑用户" }
                    p { class: "text-sm text-gray-500 mb-5", "{edit_username}" }
                    div { class: "space-y-4",
                        div {
                            label { class: "block text-sm text-gray-300 mb-1.5", "角色" }
                            select { class: "w-full px-3 py-2.5 bg-gray-800 border border-gray-700 rounded-lg text-white text-sm focus:outline-none focus:border-blue-500",
                                value: "{edit_role}",
                                onchange: move |e| edit_role.set(e.value()),
                                option { value: "user", "user — 普通用户" }
                                option { value: "admin", "admin — 管理员" }
                            }
                        }
                        div {
                            label { class: "block text-sm text-gray-300 mb-1.5", "状态" }
                            select { class: "w-full px-3 py-2.5 bg-gray-800 border border-gray-700 rounded-lg text-white text-sm focus:outline-none focus:border-blue-500",
                                value: "{edit_status}",
                                onchange: move |e| edit_status.set(e.value()),
                                option { value: "active", "active — 正常" }
                                option { value: "locked", "locked — 锁定（禁止登录）" }
                                option { value: "disabled", "disabled — 禁用" }
                            }
                        }
                        div {
                            label { class: "block text-sm text-gray-300 mb-1.5", "新密码（留空不修改）" }
                            input { class: "w-full px-3 py-2.5 bg-gray-800 border border-gray-700 rounded-lg text-white text-sm focus:outline-none focus:border-blue-500",
                                r#type: "password",
                                value: "{edit_password}",
                                oninput: move |e| edit_password.set(e.value()),
                                placeholder: "至少4位"
                            }
                        }
                    }
                    if !error.read().is_empty() {
                        div { class: "mt-4 p-3 bg-red-900/50 border border-red-700 rounded-lg text-red-300 text-sm",
                            "{error}"
                        }
                    }
                    div { class: "flex gap-3 mt-6",
                        button {
                            class: "flex-1 py-2.5 bg-gray-700 hover:bg-gray-600 rounded-lg text-sm text-gray-300 transition",
                            onclick: move |_| editing.set(None),
                            "取消"
                        }
                        button {
                            class: "flex-1 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-600 rounded-lg text-sm text-white transition",
                            disabled: *edit_saving.read(),
                            onclick: save_edit.clone(),
                            if *edit_saving.read() { "保存中..." } else { "保存修改" }
                        }
                    }
                }
            }
        };
    }

    // 主页面
    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            h1 { class: "text-2xl font-bold text-white mb-6", "系统管理" }

            // 消息
            if !message.read().is_empty() {
                div { class: "mb-4 p-3 bg-green-900/50 border border-green-700 rounded-lg text-green-300 text-sm flex justify-between items-center",
                    span { "{message}" }
                    button { class: "text-green-400 hover:text-green-300", onclick: move |_| message.set(String::new()), "✕" }
                }
            }
            if !error.read().is_empty() {
                div { class: "mb-4 p-3 bg-red-900/50 border border-red-700 rounded-lg text-red-300 text-sm flex justify-between items-center",
                    span { "{error}" }
                    button { class: "text-red-400 hover:text-red-300", onclick: move |_| error.set(String::new()), "✕" }
                }
            }

            // Tabs
            div { class: "flex gap-2 mb-6",
                for t in ["users", "tasks", "sync"] {
                    {
                        let label = match t { "users" => "用户管理", "tasks" => "定时任务", "sync" => "数据同步", _ => "" };
                        let key = t.to_string();
                        let active = *tab.read() == key;
                        rsx! {
                            button {
                                class: format!("px-4 py-2 rounded-lg text-sm transition {}",
                                    if active { "bg-blue-600 text-white" } else { "bg-gray-800 text-gray-400 hover:text-white" }
                                ),
                                onclick: move |_| tab.set(key.clone()),
                                "{label}"
                            }
                        }
                    }
                }
            }

            // ── 用户管理 ──
            if *tab.read() == "users" {
                // 创建表单
                div { class: "mb-6 bg-gray-900 rounded-xl border border-gray-800 p-5",
                    h3 { class: "text-sm font-semibold text-white mb-3", "创建新用户" }
                    div { class: "flex gap-3 items-end flex-wrap",
                        div {
                            label { class: "block text-xs text-gray-500 mb-1", "用户名" }
                            input { class: "px-3 py-2 bg-gray-800 rounded-lg border border-gray-700 text-white text-sm w-36",
                                value: "{new_name}", oninput: move |e| new_name.set(e.value()), placeholder: "用户名"
                            }
                        }
                        div {
                            label { class: "block text-xs text-gray-500 mb-1", "密码" }
                            input { class: "px-3 py-2 bg-gray-800 rounded-lg border border-gray-700 text-white text-sm w-36",
                                r#type: "password", value: "{new_pass}", oninput: move |e| new_pass.set(e.value()),
                                placeholder: "至少4位"
                            }
                        }
                        div {
                            label { class: "block text-xs text-gray-500 mb-1", "角色" }
                            select { class: "px-3 py-2 bg-gray-800 rounded-lg border border-gray-700 text-white text-sm",
                                onchange: move |e| new_role.set(e.value()),
                                option { value: "user", selected: true, "user（普通用户）" }
                                option { value: "admin", "admin（管理员）" }
                            }
                        }
                        button {
                            class: "px-5 py-2 bg-green-600 hover:bg-green-500 disabled:bg-gray-600 rounded-lg text-sm transition",
                            disabled: *new_saving.read(), onclick: do_create,
                            if *new_saving.read() { "创建中..." } else { "创建用户" }
                        }
                    }
                }

                // 用户表格
                div { class: "bg-gray-900 rounded-xl border border-gray-800 overflow-hidden",
                    if users.read().is_empty() {
                        div { class: "p-8 text-center text-gray-500", "暂无用户数据" }
                    } else {
                        div { class: "grid grid-cols-5 gap-4 px-5 py-3 bg-gray-800/50 text-xs font-medium text-gray-400 uppercase",
                            div { "用户名" } div { "角色" } div { "状态" }
                            div { "邮箱 / 最后登录" } div { class: "text-right", "操作" }
                        }
                        for u in users.read().iter() {
                            {
                                let username = u["username"].as_str().unwrap_or("-").to_string();
                                let user_id = u["user_id"].as_str().unwrap_or("").to_string();
                                let role = u["role"].as_str().unwrap_or("-").to_string();
                                let status_val = u["status"].as_str().unwrap_or("active").to_string();
                                let email = u["email"].as_str().unwrap_or("").to_string();
                                let display = u["display_name"].as_str().unwrap_or("").to_string();
                                let last_login = u["last_login_at"].as_str().map(|s| s.to_string()).unwrap_or_default();
                                let is_admin_user = username == "admin";
                                let user_clone = u.clone();
                                rsx! {
                                    div { class: "grid grid-cols-5 gap-4 px-5 py-3 border-t border-gray-800 items-center hover:bg-gray-800/30 transition",
                                        div {
                                            div { class: "text-white text-sm font-medium", "{username}" }
                                            if !display.is_empty() {
                                                div { class: "text-xs text-gray-500", "{display}" }
                                            }
                                        }
                                        div {
                                            if role == "admin" {
                                                span { class: "text-xs px-2 py-0.5 rounded-full bg-yellow-900/50 text-yellow-300", "管理员" }
                                            } else {
                                                span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-800 text-gray-400", "用户" }
                                            }
                                        }
                                        div {
                                            if status_val == "active" {
                                                span { class: "text-xs px-2 py-0.5 rounded-full bg-green-900/50 text-green-400", "正常" }
                                            } else if status_val == "locked" {
                                                span { class: "text-xs px-2 py-0.5 rounded-full bg-red-900/50 text-red-400", "已锁定" }
                                            } else {
                                                span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-800 text-gray-400", "{status_val}" }
                                            }
                                        }
                                        div { class: "text-xs text-gray-500",
                                            if !email.is_empty() { div { "{email}" } }
                                            if !last_login.is_empty() {
                                                div { class: "text-gray-600", "登录: {last_login}" }
                                            }
                                        }
                                        div { class: "flex items-center justify-end gap-2",
                                            button {
                                                class: "text-xs px-3 py-1.5 bg-gray-700 hover:bg-gray-600 rounded-lg text-gray-300 transition",
                                                onclick: move |_| open_edit(user_clone.clone()),
                                                "编辑"
                                            }
                                            if !is_admin_user {
                                                button {
                                                    class: "text-xs px-3 py-1.5 bg-red-900/50 hover:bg-red-800 rounded-lg text-red-400 transition",
                                                    onclick: {
                                                        let uid = user_id.clone();
                                                        let uname = username.clone();
                                                        let del = do_delete.clone();
                                                        move |_| del(uid.clone(), uname.clone())
                                                    },
                                                    "删除"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── 定时任务 ──
            if *tab.read() == "tasks" {
                // 钉钉推送按钮
                div { class: "mb-4 p-4 bg-gray-900 rounded-xl border border-gray-800 flex items-center justify-between",
                    div {
                        div { class: "text-white text-sm font-medium", "钉钉推送" }
                        div { class: "text-xs text-gray-500 mt-1", "重新发送当前持仓摘要到钉钉" }
                    }
                    button {
                        class: "px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm transition",
                        onclick: move |_| {
                            let mut msg = message.clone();
                            let mut err = error.clone();
                            spawn(async move {
                                match api::trigger_dingtalk_notify().await {
                                    Ok(v) if v["code"].as_i64().unwrap_or(-1) == 0 => msg.set("钉钉推送成功".to_string()),
                                    Ok(v) => err.set(v["message"].as_str().unwrap_or("推送失败").to_string()),
                                    Err(e) => err.set(e),
                                }
                            });
                        },
                        "重新推送"
                    }
                }
                div { class: "space-y-2",
                    for t in tasks.read().iter() {
                        {
                            let name = t["task_name"].as_str().unwrap_or("-").to_string();
                            let enabled = t["enabled"].as_bool().unwrap_or(false);
                            let cron = t["schedule_cron"].as_str().unwrap_or("-").to_string();
                            let last_run = t["last_run_at"].as_str().map(|s| s.to_string()).unwrap_or_default();
                            let count = t["run_count"].as_i64().unwrap_or(0);
                            rsx! {
                                div { class: "bg-gray-900 rounded-xl border border-gray-800 p-4 flex items-center justify-between",
                                    div {
                                        span { class: "text-white text-sm font-medium", "{name}" }
                                        div { class: "text-xs text-gray-500 mt-1", "Cron: {cron} · 上次: {last_run} · 次数: {count}" }
                                    }
                                    if enabled {
                                        span { class: "text-xs px-2 py-0.5 rounded-full bg-green-900/50 text-green-400", "已启用" }
                                    } else {
                                        span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-800 text-gray-500", "已禁用" }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── 数据同步 ──
            if *tab.read() == "sync" {
                div { class: "space-y-2",
                    for s in sync.read().iter() {
                        {
                            let name = s["name"].as_str().unwrap_or("-").to_string();
                            let healthy = s["healthy"].as_bool().unwrap_or(false);
                            let gap = s["current_gap_days"].as_i64().unwrap_or(0);
                            let max_gap = s["max_gap_days"].as_i64().unwrap_or(0);
                            rsx! {
                                div { class: "bg-gray-900 rounded-xl border border-gray-800 p-4 flex items-center justify-between",
                                    div {
                                        span { class: "text-white text-sm", "{name}" }
                                        span { class: "text-xs text-gray-500 ml-2", "（允许间隔 ≤{max_gap}天）" }
                                    }
                                    div { class: "flex items-center gap-3",
                                        span { class: "text-sm text-gray-400", "当前间隔: {gap}天" }
                                        if healthy {
                                            span { class: "text-xs px-2 py-0.5 rounded-full bg-green-900/50 text-green-400", "正常" }
                                        } else {
                                            span { class: "text-xs px-2 py-0.5 rounded-full bg-red-900/50 text-red-400", "异常" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
