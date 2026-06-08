pub mod data;
pub mod tasks;
pub mod users;

// Re-export for backward compat
pub use users::UsersPage as AdminContent;
