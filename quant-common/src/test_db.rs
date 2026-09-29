//! 测试数据库连接池统一构建（2026-09-29 用户定版：池参数走 .env 环境配置，禁写死）。
//!
//! 全部连库测试的 `test_db()`/`test_state()` 经此单一出口建池，参数可经环境变量
//! 覆盖（dotenv 从 quant/.env 加载）：
//!
//! | 环境变量 | 默认 | 说明 |
//! |---|---|---|
//! | `TEST_DB_POOL_MAX` | 20 | 池最大连接数（与生产 max=20 对齐） |
//! | `TEST_DB_POOL_TIMEOUT_SECS` | 120 | acquire 超时秒（宽限防全量并行时后台任务瞬时打满） |
//!
//! 背景：sqlx `PgPool::connect` 裸建默认 10 连接/池且每测试模块各自建池——
//! 全量 cargo test 并行（10 核）时 34 后台回填任务打满单池触发 PoolTimedOut
//! （2026-09-29 实证）；且多池叠加理论上限 100 正好顶满 PG max_connections。

/// 构建测试池（参数经环境变量可调，见模块文档）。
pub async fn connect_test_pool(url: &str) -> sqlx::PgPool {
    let max = std::env::var("TEST_DB_POOL_MAX")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(20);
    let timeout_secs = std::env::var("TEST_DB_POOL_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120);
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(std::time::Duration::from_secs(timeout_secs))
        .connect(url)
        .await
        .expect("test db connect")
}
