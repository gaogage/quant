//! 认证状态管理 — JWT token 存储 + 用户信息
//!
//! 使用 GlobalSignal 在组件间共享认证状态，同时持久化到 localStorage。

use dioxus::prelude::*;
use gloo_storage::{LocalStorage, Storage};
use serde::{Deserialize, Serialize};

use crate::api::UserInfo;

static AUTH_STATE: GlobalSignal<AuthInner> = Signal::global(|| AuthInner::load());

#[derive(Debug, Clone, PartialEq)]
struct AuthInner {
    access_token: String,
    refresh_token: String,
    user: Option<UserInfo>,
}

impl AuthInner {
    fn load() -> Self {
        let access = LocalStorage::get("access_token").unwrap_or_default();
        let refresh = LocalStorage::get("refresh_token").unwrap_or_default();
        let user_json: Option<String> = LocalStorage::get("user").ok();
        let user = user_json.and_then(|s| serde_json::from_str::<UserInfo>(&s).ok());
        Self {
            access_token: access,
            refresh_token: refresh,
            user,
        }
    }

    fn save(&self) {
        let _ = LocalStorage::set("access_token", &self.access_token);
        let _ = LocalStorage::set("refresh_token", &self.refresh_token);
        if let Some(ref u) = self.user {
            if let Ok(s) = serde_json::to_string(u) {
                let _ = LocalStorage::set("user", &s);
            }
        }
    }

    fn clear() {
        LocalStorage::delete("access_token");
        LocalStorage::delete("refresh_token");
        LocalStorage::delete("user");
    }
}

pub struct AuthState;

impl AuthState {
    /// 获取 access token（用于 HTTP header）
    pub fn access_token() -> String {
        AUTH_STATE.read().access_token.clone()
    }

    /// 获取 refresh token
    pub fn refresh_token() -> String {
        AUTH_STATE.read().refresh_token.clone()
    }

    /// 是否已登录
    pub fn is_logged_in() -> bool {
        !AUTH_STATE.read().access_token.is_empty()
    }

    /// 当前用户信息
    pub fn user() -> Option<UserInfo> {
        AUTH_STATE.read().user.clone()
    }

    /// 是否为管理员
    pub fn is_admin() -> bool {
        AUTH_STATE.read().user.as_ref().map(|u| u.role == "admin").unwrap_or(false)
    }

    /// 登录成功后保存
    pub fn login(access_token: &str, refresh_token: &str, user: UserInfo) {
        let mut state = AUTH_STATE.write();
        state.access_token = access_token.to_string();
        state.refresh_token = refresh_token.to_string();
        state.user = Some(user);
        state.save();
    }

    /// 刷新 token
    pub fn update_access_token(access_token: &str) {
        let mut state = AUTH_STATE.write();
        state.access_token = access_token.to_string();
        state.save();
    }

    /// 登出
    pub fn logout() {
        AuthInner::clear();
        let mut state = AUTH_STATE.write();
        state.access_token.clear();
        state.refresh_token.clear();
        state.user = None;
    }
}
