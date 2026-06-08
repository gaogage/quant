use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Json, Response},
};
use serde_json::json;

use super::jwt::verify_token;

/// User context injected by auth middleware.
#[derive(Debug, Clone)]
pub struct UserContext {
    pub user_id: String,
    pub username: String,
    pub role: String,
}

/// Axum extractor: parse JWT from Authorization header, inject UserContext.
impl<S> FromRequestParts<S> for UserContext
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));

        match auth_header {
            Some(token) => match verify_token(token) {
                Ok(claims) => Ok(UserContext {
                    user_id: claims.sub,
                    username: claims.username,
                    role: claims.role,
                }),
                Err(_) => Err((
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"code": 401, "message": "Token invalid or expired"})),
                )
                    .into_response()),
            },
            None => Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({"code": 401, "message": "Missing Authorization header"})),
            )
                .into_response()),
        }
    }
}

/// Require admin role. Returns 403 if user is not admin.
pub fn require_admin(user: &UserContext) -> Result<(), Response> {
    if user.role == "admin" {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            Json(json!({"code": 403, "message": "Admin permission required"})),
        )
            .into_response())
    }
}
