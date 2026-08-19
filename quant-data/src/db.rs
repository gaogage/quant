//! 数据库连接池

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 0;
const STATEMENT_TIMEOUT_ENV: &str = "QUANT_DB_STATEMENT_TIMEOUT_MS";

/// 创建 PostgreSQL 连接池
pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let statement_timeout_ms = configured_statement_timeout_ms();
    let statement_timeout = format!("SET statement_timeout = {}", statement_timeout_ms);
    // 会话时区跟随部署环境 TZ 环境变量(如容器 TZ=Asia/Shanghai),不写死——
    // 换时区部署自动跟随。PostgreSQL 连接协议不自动继承客户端系统时区,
    // 不显式 SET 则用服务器默认(曾导致 NaiveDate 写入按 UTC 解释,fill_time 恒 08:00)。
    // 仅接受 IANA 时区名格式(Asia/Shanghai、America/New_York、UTC 等),防 SQL 注入;
    // TZ 未设置或非法时跳过,保持服务器默认。
    let session_tz = std::env::var("TZ")
        .ok()
        .map(|tz| tz.trim().to_string())
        .filter(|tz| !tz.is_empty() && tz.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'/' || b == b'_' || b == b'-' || b == b'+'));
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(5))
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

    info!(statement_timeout_ms, "数据库连接池已创建 (max=20, min=4)");
    Ok(pool)
}

/// 从环境变量 DATABASE_URL 创建连接池
pub async fn pool_from_env() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".to_string());
    create_pool(&url).await
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
