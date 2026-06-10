use dioxus::prelude::*;
use serde_json::{json, Value};
use crate::api;

#[component]
pub fn UsersPage() -> Element {
    let mut users = use_signal(|| Vec::<Value>::new());
    let mut loading = use_signal(|| true);
    let mut err = use_signal(|| String::new());
    let mut msg = use_signal(|| String::new());
    let mut n_name = use_signal(|| String::new());
    let mut n_pass = use_signal(|| String::new());
    let mut n_role = use_signal(|| String::from("user"));
    let mut n_save = use_signal(|| false);
    let mut editing = use_signal(|| Option::<Value>::None);
    let mut e_role = use_signal(|| String::new());
    let mut e_st = use_signal(|| String::new());
    let mut e_pw = use_signal(|| String::new());
    let mut e_save = use_signal(|| false);

    let load = move || { spawn(async move {
        if let Ok(v) = api::admin_list_users().await {
            if let Some(arr) = v.get("data").and_then(|d| d.as_array()) { users.set(arr.clone()); }
        }
        loading.set(false);
    }); };
    use_effect(move || { load(); });

    let do_create = move |_| {
        if n_name.read().is_empty() || n_pass.read().is_empty() { err.set("请输入用户名和密码".into()); return; }
        n_save.set(true);
        let p = json!({"username": n_name.read().clone(), "password": n_pass.read().clone(), "role": n_role.read().clone()});
        spawn(async move {
            match api::admin_create_user(&p).await {
                Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => { n_name.set(String::new()); n_pass.set(String::new()); msg.set("创建成功".into()); load(); }
                Ok(v) => err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("失败").into()),
                Err(e) => err.set(e),
            }
            n_save.set(false);
        });
    };

    let _do_delete = move |uid: String, uname: String| {
        spawn(async move {
            if !web_sys::window().and_then(|w| w.confirm_with_message(&format!("确认删除用户 {}？", uname)).ok()).unwrap_or(false) { return; }
            match api::admin_delete_user(&uid).await {
                Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => { msg.set("已删除".into()); load(); }
                Ok(v) => err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("失败").into()),
                Err(e) => err.set(e),
            }
        });
    };

    let _open_edit = move |u: Value| {
        e_role.set(u.get("role").and_then(|v| v.as_str()).unwrap_or("user").into());
        e_st.set(u.get("status").and_then(|v| v.as_str()).unwrap_or("active").into());
        e_pw.set(String::new());
        editing.set(Some(u));
    };

    let save_edit = move |_| {
        let eu_opt = editing.read().clone();
        if let Some(ref u) = eu_opt {
            let uid = u.get("user_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let r = e_role.read().clone();
            let s = e_st.read().clone();
            let pw = e_pw.read().clone();
            e_save.set(true);
            spawn(async move {
                match api::admin_update_user(&uid, &json!({"role": r, "status": s})).await {
                    Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => {}
                    Ok(v) => { err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("失败").into()); e_save.set(false); return; }
                    Err(e) => { err.set(e); e_save.set(false); return; }
                }
                if !pw.is_empty() {
                    match api::admin_reset_password(&uid, &pw).await {
                        Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => {}
                        Ok(v) => { err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("密码重置失败").into()); e_save.set(false); return; }
                        Err(e) => { err.set(e); e_save.set(false); return; }
                    }
                }
                msg.set("已更新".into());
                editing.set(None);
                e_save.set(false);
                load();
            });
        }
    };

    if *loading.read() {
        return rsx! { div { class: "p-6", div { class: "animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" } } };
    }

    // 编辑弹窗独立处理
    let show_modal = editing.read().is_some();
    if show_modal {
        let eu = editing.read().clone().unwrap();
        let eun = eu.get("username").and_then(|v| v.as_str()).unwrap_or("-");
        return rsx! {
            div { class: "p-6 max-w-6xl mx-auto", h1 { class: "text-2xl font-bold text-gray-900 dark:text-white mb-6", "用户管理" } }
            div { class: "fixed inset-0 bg-black/60 z-50 flex items-center justify-center", onclick: move |_| editing.set(None),
                div { class: "bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-2xl p-6 w-full max-w-md mx-4 shadow-2xl",
                    onclick: move |e| e.stop_propagation(),
                    h2 { class: "text-lg font-bold text-gray-900 dark:text-white mb-1", "编辑用户" }
                    p { class: "text-sm text-gray-500 dark:text-gray-400 mb-5", "{eun}" }
                    div { class: "space-y-4",
                        div { label { class: "block text-sm text-gray-700 dark:text-gray-300 mb-1.5", "角色" }
                            select { class: "w-full px-3 py-2.5 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                value: "{e_role}", onchange: move |e| e_role.set(e.value()),
                                option { value: "user", "user" } option { value: "admin", "admin" }
                            }
                        }
                        div { label { class: "block text-sm text-gray-700 dark:text-gray-300 mb-1.5", "状态" }
                            select { class: "w-full px-3 py-2.5 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                value: "{e_st}", onchange: move |e| e_st.set(e.value()),
                                option { value: "active", "active — 正常" } option { value: "locked", "locked — 锁定" } option { value: "disabled", "disabled — 禁用" }
                            }
                        }
                        div { label { class: "block text-sm text-gray-700 dark:text-gray-300 mb-1.5", "新密码(留空不修改)" }
                            input { class: "w-full px-3 py-2.5 bg-gray-100 dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white text-sm",
                                r#type: "password", value: "{e_pw}", oninput: move |e| e_pw.set(e.value()), placeholder: "至少4位"
                            }
                        }
                    }
                    if !err.read().is_empty() { div { class: "mt-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm", "{err}" } }
                    div { class: "flex gap-3 mt-6",
                        button { class: "flex-1 py-2.5 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 transition", onclick: move |_| editing.set(None), "取消" }
                        button { class: "flex-1 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-300 dark:disabled:bg-gray-600 rounded-lg text-sm text-white transition",
                            disabled: *e_save.read(), onclick: save_edit,
                            if *e_save.read() { "保存中..." } else { "保存修改" }
                        }
                    }
                }
            }
        };
    }

    rsx! {
        div { class: "p-6 max-w-6xl mx-auto",
            h1 { class: "text-2xl font-bold text-gray-900 dark:text-white mb-6", "用户管理" }
            if !msg.read().is_empty() { div { class: "mb-4 p-3 bg-green-50 dark:bg-green-900/50 border border-green-300 dark:border-green-700 rounded-lg text-green-700 dark:text-green-300 text-sm flex justify-between", span { "{msg}" } button { class: "text-green-600 dark:text-green-400", onclick: move |_| msg.set(String::new()), "✕" } } }
            if !err.read().is_empty() { div { class: "mb-4 p-3 bg-red-50 dark:bg-red-900/50 border border-red-300 dark:border-red-700 rounded-lg text-red-600 dark:text-red-300 text-sm flex justify-between", span { "{err}" } button { class: "text-red-600 dark:text-red-400", onclick: move |_| err.set(String::new()), "✕" } } }
            // 创建表单
            div { class: "mb-6 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-5",
                h3 { class: "text-sm font-semibold text-gray-900 dark:text-white mb-3", "创建新用户" }
                div { class: "flex gap-3 items-end flex-wrap",
                    div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "用户名" } input { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm w-36", value: "{n_name}", oninput: move |e| n_name.set(e.value()), placeholder: "用户名" } }
                    div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "密码" } input { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm w-36", r#type: "password", value: "{n_pass}", oninput: move |e| n_pass.set(e.value()), placeholder: "至少4位" } }
                    div { label { class: "block text-xs text-gray-500 dark:text-gray-400 mb-1", "角色" } select { class: "px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-700 text-gray-900 dark:text-white text-sm", onchange: move |e| n_role.set(e.value()), option { value: "user", selected: true, "user" } option { value: "admin", "admin" } } }
                    button { class: "px-5 py-2 bg-green-600 hover:bg-green-500 disabled:bg-gray-300 dark:disabled:bg-gray-600 disabled:text-gray-500 rounded-lg text-sm text-white transition", disabled: *n_save.read(), onclick: do_create, if *n_save.read() { "创建中..." } else { "创建用户" } }
                }
            }
            // 用户表格
            div { class: "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden",
                if users.read().is_empty() {
                    div { class: "p-8 text-center text-gray-400 dark:text-gray-500", "暂无用户" }
                } else {
                    div { class: "grid grid-cols-5 gap-4 px-5 py-3 bg-gray-50 dark:bg-gray-800/50 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase",
                        div { "用户名" } div { "角色" } div { "状态" } div { "邮箱/最后登录" } div { class: "text-right", "操作" }
                    }
                    for u in users.read().iter() {
                        {
                            let un = u.get("username").and_then(|v| v.as_str()).unwrap_or("-").to_string();
                            let uid = u.get("user_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let role = u.get("role").and_then(|v| v.as_str()).unwrap_or("-");
                            let sv = u.get("status").and_then(|v| v.as_str()).unwrap_or("active");
                            let email = u.get("email").and_then(|v| v.as_str()).unwrap_or("");
                            let dn = u.get("display_name").and_then(|v| v.as_str()).unwrap_or("");
                            let ll = u.get("last_login_at").and_then(|v| v.as_str()).unwrap_or("");
                            let is_admin = un == "admin";
                            let uc = u.clone();
                            rsx! {
                                div { class: "grid grid-cols-5 gap-4 px-5 py-3 border-t border-gray-100 dark:border-gray-800 items-center hover:bg-gray-50 dark:hover:bg-gray-800/30 transition",
                                    div { div { class: "text-gray-900 dark:text-white text-sm font-medium", "{un}" } if !dn.is_empty() { div { class: "text-xs text-gray-500 dark:text-gray-400", "{dn}" } } }
                                    div { if role == "admin" { span { class: "text-xs px-2 py-0.5 rounded-full bg-yellow-100 dark:bg-yellow-900/50 text-yellow-700 dark:text-yellow-300", "管理员" } } else { span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400", "用户" } } }
                                    div {
                                        if sv == "active" { span { class: "text-xs px-2 py-0.5 rounded-full bg-green-100 dark:bg-green-900/50 text-green-700 dark:text-green-400", "正常" } }
                                        else if sv == "locked" { span { class: "text-xs px-2 py-0.5 rounded-full bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-400", "已锁定" } }
                                        else { span { class: "text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400", "{sv}" } }
                                    }
                                    div { class: "text-xs text-gray-500 dark:text-gray-400", if !email.is_empty() { div { "{email}" } } if !ll.is_empty() { div { class: "text-gray-400 dark:text-gray-500", "登录:{ll}" } } }
                                    div { class: "flex items-center justify-end gap-2",
                                        button { class: "text-xs px-3 py-1.5 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg text-gray-700 dark:text-gray-300 transition",
                                            onclick: {
                                                let v = uc.clone();
                                                move |_| {
                                                    e_role.set(v.get("role").and_then(|r| r.as_str()).unwrap_or("user").into());
                                                    e_st.set(v.get("status").and_then(|s| s.as_str()).unwrap_or("active").into());
                                                    e_pw.set(String::new());
                                                    editing.set(Some(v.clone()));
                                                }
                                            },
                                            "编辑"
                                        }
                                        if !is_admin { button { class: "text-xs px-3 py-1.5 bg-red-100 dark:bg-red-900/50 hover:bg-red-200 dark:hover:bg-red-800 rounded-lg text-red-600 dark:text-red-400 transition",
                                            onclick: move |_| {
                                                let uid2 = uid.clone();
                                                let un2 = un.clone();
                                                spawn(async move {
                                                    if !web_sys::window().and_then(|w| w.confirm_with_message(&format!("确认删除用户 {}？", un2)).ok()).unwrap_or(false) { return; }
                                                    match api::admin_delete_user(&uid2).await {
                                                        Ok(v) if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 => { msg.set("已删除".into()); load(); }
                                                        Ok(v) => err.set(v.get("message").and_then(|m| m.as_str()).unwrap_or("失败").into()),
                                                        Err(e) => err.set(e),
                                                    }
                                                });
                                            },
                                            "删除"
                                        } }
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
