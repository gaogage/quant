//! 数据库连接池统一构建（2026-09-29 用户定版：生产与测试单一出口，参数全走环境配置禁写死）。
//!
//! 生产（quant-data `pool_from_env`）与全部连库测试（`connect_test_pool`）共用
//! 同一构建逻辑与 after_connect 会话初始化——测试连接与生产连接行为完全同构
//! （statement_timeout + TZ 会话设置两边生效，杜绝"测试无 TZ 测不出时区类 bug"
//! 的历史温床，先例：fill_time 恒 08:00）。
//!
//! | 环境变量 | 默认 | 说明 |
//! |---|---|---|
//! | `DB_POOL_MAX` | 20 | 池最大连接数 |
//! | `DB_POOL_MIN` | 4 | 池最小空闲连接 |
//! | `DB_POOL_ACQUIRE_TIMEOUT_SECS` | 5 | acquire 超时秒（默认=生产快速失败语义；测试环境文件覆盖 120 宽限全量并行） |
//! | `QUANT_DB_STATEMENT_TIMEOUT_MS` | 0 | 会话 statement_timeout（0=不限） |
//!
//! `TZ` 环境变量存在且合法（IANA 名）时会话 SET TIME ZONE 跟随部署环境。

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 0;
const STATEMENT_TIMEOUT_ENV: &str = "QUANT_DB_STATEMENT_TIMEOUT_MS";

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

/// 创建 PostgreSQL 连接池（生产测试共用，参数见模块文档）。
pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let max = env_u64("DB_POOL_MAX", 20) as u32;
    let min = env_u64("DB_POOL_MIN", 4) as u32;
    let acquire_secs = env_u64("DB_POOL_ACQUIRE_TIMEOUT_SECS", 5);
    let statement_timeout_ms = configured_statement_timeout_ms();
    let statement_timeout = format!("SET statement_timeout = {}", statement_timeout_ms);
    // 会话时区跟随部署环境 TZ 环境变量(如容器 TZ=Asia/Shanghai),不写死——
    // 换时区部署自动跟随。仅接受 IANA 时区名格式(防 SQL 注入);
    // TZ 未设置或非法时跳过,保持服务器默认。
    let session_tz = std::env::var("TZ")
        .ok()
        .map(|tz| tz.trim().to_string())
        .filter(|tz| {
            !tz.is_empty()
                && tz.bytes().all(|b| {
                    b.is_ascii_alphanumeric() || b == b'/' || b == b'_' || b == b'-' || b == b'+'
                })
        });
    let pool = PgPoolOptions::new()
        .max_connections(max)
        .min_connections(min)
        .acquire_timeout(std::time::Duration::from_secs(acquire_secs))
        .after_connect(move |conn, _meta| {
            let statement_timeout = statement_timeout.clone();
            let session_tz = session_tz.clone();
            Box::pin(async move {
                sqlx::query(&statement_timeout).execute(&mut *conn).await?;
                if let Some(tz) = &session_tz {
                    sqlx::query(&format!("SET TIME ZONE '{}'", tz))
                        .execute(&mut *conn)
                        .await?;
                }
                Ok(())
            })
        })
        .connect(database_url)
        .await?;

    info!(
        max,
        min, acquire_secs, statement_timeout_ms, "数据库连接池已创建"
    );
    Ok(pool)
}

/// 从环境变量 DATABASE_URL 创建连接池。
pub async fn pool_from_env() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".to_string());
    create_pool(&url).await
}

/// 测试便捷出口：与生产同构建池（含 after_connect 会话初始化），仅错误处理
/// 差异（测试直接 expect）。测试环境的超时宽限经 .env 的
/// DB_POOL_ACQUIRE_TIMEOUT_SECS 覆盖，不在代码分叉。
pub async fn connect_test_pool(url: &str) -> PgPool {
    create_pool(url).await.expect("test db connect")
}

fn configured_statement_timeout_ms() -> u64 {
    parse_statement_timeout_ms(std::env::var(STATEMENT_TIMEOUT_ENV).ok().as_deref())
}

fn parse_statement_timeout_ms(value: Option<&str>) -> u64 {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_STATEMENT_TIMEOUT_MS)
}

#[cfg(test)]
mod tests {
    use super::parse_statement_timeout_ms;

    #[test]
    fn statement_timeout_defaults_to_unlimited_for_research_workloads() {
        assert_eq!(parse_statement_timeout_ms(None), 0);
        assert_eq!(parse_statement_timeout_ms(Some("")), 0);
        assert_eq!(parse_statement_timeout_ms(Some("not-a-number")), 0);
    }

    #[test]
    fn statement_timeout_accepts_positive_override() {
        assert_eq!(parse_statement_timeout_ms(Some("300000")), 300_000);
        assert_eq!(parse_statement_timeout_ms(Some(" 60000 ")), 60_000);
    }
}
