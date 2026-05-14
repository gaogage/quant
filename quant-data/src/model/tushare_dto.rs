//! Tushare API 请求/响应 DTO
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TushareRequest {
    pub api_name: String,
    pub token: String,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TushareResponse<T> {
    pub code: i32,
    #[serde(default)]
    pub msg: Option<String>,
    #[serde(default)]
    pub data: Option<TushareData<T>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TushareData<T> {
    pub fields: Vec<String>,
    pub items: Vec<T>,
}

impl TushareData<Vec<serde_json::Value>> {
    /// 将行数组转为 Vec<Map>，用 fields 做 key
    pub fn to_maps(&self) -> Vec<serde_json::Map<String, serde_json::Value>> {
        self.items
            .iter()
            .map(|row| {
                self.fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        (
                            f.clone(),
                            row.get(i).cloned().unwrap_or(serde_json::Value::Null),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>()
            })
            .collect()
    }
}
