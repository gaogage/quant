//! Authentication and authorization module.
//! JWT-based: access token (15min) + refresh token (7 days, stored in user_session).

pub mod jwt;
pub mod middleware;

pub use middleware::{require_admin, UserContext};
