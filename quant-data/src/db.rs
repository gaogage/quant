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
        .filter(|tz| {
            !tz.is_empty()
                && tz.bytes().all(|b| {
                    b.is_ascii_alphanumeric() || b == b'/' || b == b'_' || b == b'-' || b == b'+'
                })
        });
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
    use super::{create_pool, parse_statement_timeout_ms, pool_from_env};

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

    // ─── 第四批：连接池真实建连覆盖（本机 PG，用后即关防连接堆积）───
    //
    // 三测试互斥：create_pool max_connections=20，并行各建一池瞬时 60+ 连接
    // 会打爆本机 PG max_connections（全量并行跑实锤）——static Mutex 串行化。
    static DB_POOL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn local_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@127.0.0.1/quant".to_string())
    }

    #[tokio::test]
    async fn create_pool_connects_local_postgres() {
        let _db_pool_guard = DB_POOL_TEST_LOCK.lock().await;
        let pool = create_pool(&local_url()).await.expect("本机建池应成功");
        let one: i64 = sqlx::query_scalar("SELECT 1::int8")
            .fetch_one(&pool)
            .await
            .expect("连接应可查询");
        assert_eq!(one, 1);
        pool.close().await;
    }

    #[tokio::test]
    async fn pool_from_env_connects_and_queries() {
        let _db_pool_guard = DB_POOL_TEST_LOCK.lock().await;
        let pool = pool_from_env().await.expect("env 建池应成功");
        let one: i64 = sqlx::query_scalar("SELECT 1::int8")
            .fetch_one(&pool)
            .await
            .expect("连接应可查询");
        assert_eq!(one, 1);
        pool.close().await;
    }

    /// TZ / QUANT_DB_STATEMENT_TIMEOUT_MS 的 after_connect 分支矩阵。
    /// env 修改只被 create_pool 读取（local_pool 不读），且本函数内
    /// 串行设置-建池-断言，无并行竞争；结尾恢复环境。
    #[tokio::test]
    async fn create_pool_applies_session_env_matrix() {
        let _db_pool_guard = DB_POOL_TEST_LOCK.lock().await;
        // 合法 IANA 时区 → 会话时区跟随
        std::env::set_var("TZ", "Asia/Shanghai");
        let pool = create_pool(&local_url())
            .await
            .expect("TZ=Asia/Shanghai 建池");
        let tz: String = sqlx::query_scalar("SELECT current_setting('TimeZone')")
            .fetch_one(&pool)
            .await
            .expect("时区设置应可读");
        assert_eq!(tz, "Asia/Shanghai", "SET TIME ZONE 应生效");
        pool.close().await;

        // 非法时区（含引号/分号等）→ 跳过 SET，连接仍成功（服务器默认）
        std::env::set_var("TZ", "bad'tz; --");
        let pool = create_pool(&local_url())
            .await
            .expect("非法 TZ 应被过滤而非失败");
        let one: i64 = sqlx::query_scalar("SELECT 1::int8")
            .fetch_one(&pool)
            .await
            .expect("连接应可查询");
        assert_eq!(one, 1);
        pool.close().await;

        // TZ 未设置 → 保持服务器默认
        std::env::remove_var("TZ");
        let pool = create_pool(&local_url()).await.expect("无 TZ 建池应成功");
        let one: i64 = sqlx::query_scalar("SELECT 1::int8")
            .fetch_one(&pool)
            .await
            .expect("连接应可查询");
        assert_eq!(one, 1);
        pool.close().await;

        // statement_timeout 环境覆盖分支（值不断言——SHOW 格式随 PG 版本变化）
        std::env::set_var("QUANT_DB_STATEMENT_TIMEOUT_MS", "300000");
        let pool = create_pool(&local_url()).await.expect("超时覆盖建池应成功");
        let one: i64 = sqlx::query_scalar("SELECT 1::int8")
            .fetch_one(&pool)
            .await
            .expect("连接应可查询");
        assert_eq!(one, 1);
        pool.close().await;

        std::env::remove_var("QUANT_DB_STATEMENT_TIMEOUT_MS");
    }
}
