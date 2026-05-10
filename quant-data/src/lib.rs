/// Quant 数据层
///
/// 负责 Tushare 数据接入、标准化、验证和持久化。
pub mod tushare {
    pub mod client;
}
pub mod model {
    pub mod tushare_dto;
    pub mod entities;
}
pub mod db;
pub mod repository;
pub mod sync;
