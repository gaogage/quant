//! 数据库连接池（薄壳：构建逻辑统一在 quant_common::db_pool，2026-09-29 用户定版
//! 生产测试单一出口消除重复；参数全走环境配置 DB_POOL_* / QUANT_DB_STATEMENT_TIMEOUT_MS）。

pub use quant_common::db_pool::{create_pool, pool_from_env};
