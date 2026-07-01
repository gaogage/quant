use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 启动时校验 JWT_SECRET(release 未设置则 panic)。
/// 在 main 初始化早期调用,使缺失密钥的服务启动即失败(fail-fast)。
pub fn validate_secret_at_startup() {
    let _ = jwt_secret();
}

/// JWT secret from env.
/// release 构建必须设置 JWT_SECRET,否则启动 panic;
/// debug 构建允许默认密钥但记录警告(仅限本地开发)。
fn jwt_secret() -> String {
    std::env::var("JWT_SECRET").unwrap_or_else(|_| {
        #[cfg(not(debug_assertions))]
        {
            panic!("JWT_SECRET 环境变量未设置(release 构建必须配置)");
        }
        #[cfg(debug_assertions)]
        {
            tracing::warn!("JWT_SECRET 未设置,使用开发默认密钥(仅限 debug 构建)");
            "quant-dev-secret-change-in-production".into()
        }
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String, // user_id
    pub username: String,
    pub role: String, // "admin" | "user"
    pub exp: usize,
    pub iat: usize,
    pub jti: String, // unique token id
}

/// Create a JWT access token (24h expiry).
pub fn create_access_token(user_id: &str, username: &str, role: &str) -> Result<String, String> {
    let now = Utc::now();
    let claims = Claims {
        sub: user_id.to_string(),
        username: username.to_string(),
        role: role.to_string(),
        iat: now.timestamp() as usize,
        exp: (now + Duration::hours(24)).timestamp() as usize,
        jti: Uuid::new_v4().to_string(),
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret().as_bytes()),
    )
    .map_err(|e| format!("JWT encode: {}", e))
}

/// Create a refresh token (7 days expiry).
pub fn create_refresh_token(user_id: &str, username: &str, role: &str) -> Result<String, String> {
    let now = Utc::now();
    let claims = Claims {
        sub: user_id.to_string(),
        username: username.to_string(),
        role: role.to_string(),
        iat: now.timestamp() as usize,
        exp: (now + Duration::days(7)).timestamp() as usize,
        jti: Uuid::new_v4().to_string(),
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret().as_bytes()),
    )
    .map_err(|e| format!("JWT encode: {}", e))
}

/// Verify and decode a JWT token.
pub fn verify_token(token: &str) -> Result<Claims, String> {
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret().as_bytes()),
        &Validation::default(),
    )
    .map(|data| data.claims)
    .map_err(|e| format!("JWT verify: {}", e))
}
