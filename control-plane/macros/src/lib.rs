/// Compatibility layer between actix v2 and paperclip
pub mod actix {
    /// Expose macros to create resource handlers, allowing multiple HTTP
    /// method guards.
    pub use actix_openapi_macros::*;
}
