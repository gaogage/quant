//! 数据库连接池

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

/// 创建 PostgreSQL 连接池
pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(database_url)
        .await?;

    info!("数据库连接池已创建 (max=20, min=4)");
    Ok(pool)
}

/// 从环境变量 DATABASE_URL 创建连接池
pub async fn pool_from_env() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".to_string());
    create_pool(&url).await
}
