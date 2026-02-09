use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum QontrolError {
    #[error("profile not found: {0}")]
    ProfileNotFound(String),

    #[error("no default profile configured — use `qontrol profile add <name> --default` or `--profile <name>`")]
    NoDefaultProfile,

    #[error("API error ({status}): {body}")]
    ApiError { status: u16, body: String },

    #[error("config error: {0}")]
    ConfigError(String),
}
