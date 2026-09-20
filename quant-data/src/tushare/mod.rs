/// Tushare 数据源模块
pub mod client;
// 测试支撑（覆盖率专项）：仅 cfg(test) 编译，含 mock server 与三个测试模块挂载
#[cfg(test)]
pub(crate) mod test_support;
