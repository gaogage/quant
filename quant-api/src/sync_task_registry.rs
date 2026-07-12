//! 后台同步任务注册表
//!
//! 解决 quant 后台同步僵尸任务 bug:
//! - tokio::spawn 丢弃 JoinHandle → 无法 abort 僵尸 task
//! - DB status 标 failed 但 tokio task 继续跑
//!
//! 本注册表在 spawn 时保存 AbortHandle,cancel_sync_task 路由可主动 abort,
//! 实现 DB flag + tokio abort 双保险取消。

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// 后台同步任务注册表:task_id → AbortHandle
///
/// 线程安全(Mutex),Arc 包装后放入 AppState 供多 handler 共享。
#[derive(Default)]
pub struct SyncTaskRegistry {
    handles: Mutex<HashMap<String, tokio::task::AbortHandle>>,
}

impl SyncTaskRegistry {
    pub fn new() -> Self {
        Self {
            handles: Mutex::new(HashMap::new()),
        }
    }

    /// 注册一个后台任务。spawn 后立即调用,保存 AbortHandle。
    ///
    /// 若 task_id 已存在(同 id 重复触发),旧 handle 先 abort 再覆盖。
    pub async fn register(&self, task_id: &str, handle: tokio::task::JoinHandle<()>) {
        let abort_handle = handle.abort_handle();
        let mut map = self.handles.lock().await;
        if let Some(old) = map.insert(task_id.to_string(), abort_handle) {
            warn!(task_id = %task_id, "重复注册 task,旧 handle 被 abort");
            old.abort();
        }
        // 释放锁后让 handle 在后台运行(不 await join,否则会阻塞)
        drop(handle);
    }

    /// 主动取消任务:abort tokio task 并移除注册。
    ///
    /// 返回 true 表示找到并 abort,false 表示 task 不在注册表(可能已完成或未注册)。
    pub async fn abort(&self, task_id: &str) -> bool {
        let mut map = self.handles.lock().await;
        match map.remove(task_id) {
            Some(handle) => {
                handle.abort();
                info!(task_id = %task_id, "已 abort 后台同步 task");
                true
            }
            None => false,
        }
    }

    /// 取消所有正在运行的后台任务(服务关闭/清理时用)。
    pub async fn abort_all(&self) -> usize {
        let mut map = self.handles.lock().await;
        let count = map.len();
        for (task_id, handle) in map.drain() {
            handle.abort();
            info!(task_id = %task_id, "关闭时 abort");
        }
        count
    }

    /// 当前注册的任务数(诊断/调试用)。
    pub async fn len(&self) -> usize {
        self.handles.lock().await.len()
    }

    /// 是否为空。
    pub async fn is_empty(&self) -> bool {
        self.handles.lock().await.is_empty()
    }
}

/// 构造 Arc 包装的注册表(放入 AppState)。
pub fn new_registry() -> Arc<SyncTaskRegistry> {
    Arc::new(SyncTaskRegistry::new())
}

/// 便捷封装:spawn 一个后台同步任务并注册到 registry。
///
/// 统一 11 处 `tokio::spawn` 的样板代码:
/// 1. spawn 取 JoinHandle
/// 2. 调 register 保存 AbortHandle(内部 drop JoinHandle,不阻塞)
///
/// # 参数
/// - `registry`:从 `state.sync_tasks` clone 出来的 `Arc<SyncTaskRegistry>`
/// - `task_id`:owned String(避免与闭包内 `async move` 的借用冲突)
/// - `future`:后台任务体
///
/// # 用法
/// ```ignore
/// let task_id = dv_id.clone();
/// spawn_sync_task(state.sync_tasks.clone(), task_id.clone(), async move {
///     // 原闭包体(用 move 进来的 clone,不借用外部 task_id)
/// })
/// .await;
/// ```
pub async fn spawn_sync_task<F>(registry: Arc<SyncTaskRegistry>, task_id: String, future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let handle = tokio::spawn(future);
    registry.register(&task_id, handle).await;
}

