use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::error::QontrolError;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileEntry {
    pub host: String,
    pub port: u16,
    pub token: String,
    #[serde(default)]
    pub insecure: bool,
}

/// Returns the config directory: ~/.config/qontrol/ on Linux, %APPDATA%\qontrol\ on Windows.
/// Override with QONTROL_CONFIG_DIR env var (used by test harness).
pub fn config_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("QONTROL_CONFIG_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let proj = directories::ProjectDirs::from("", "", "qontrol")
        .context("could not determine config directory")?;
    Ok(proj.config_dir().to_path_buf())
}

/// Returns the full path to config.toml
pub fn config_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.toml"))
}

/// Load config from disk, returning a default Config if the file doesn't exist
pub fn load_config() -> Result<Config> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(Config::default());
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let config: Config =
        toml::from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(config)
}

/// Save config to disk, creating the directory if needed
pub fn save_config(config: &Config) -> Result<()> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let contents = toml::to_string_pretty(config).context("failed to serialize config")?;
    std::fs::write(&path, contents)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Resolve which profile to use: --profile flag > QONTROL_PROFILE env (via clap) > default_profile > error
pub fn resolve_profile(
    config: &Config,
    flag_profile: &Option<String>,
) -> Result<(String, ProfileEntry)> {
    let name = match flag_profile {
        Some(name) => name.clone(),
        None => config
            .default_profile
            .clone()
            .ok_or(QontrolError::NoDefaultProfile)?,
    };
    let entry = config
        .profiles
        .get(&name)
        .ok_or_else(|| QontrolError::ProfileNotFound(name.clone()))?
        .clone();
    Ok((name, entry))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toml_roundtrip() {
        let mut config = Config::default();
        config.default_profile = Some("test".to_string());
        config.profiles.insert(
            "test".to_string(),
            ProfileEntry {
                host: "10.0.0.1".to_string(),
                port: 8000,
                token: "access-v1:abc123".to_string(),
                insecure: true,
            },
        );

        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: Config = toml::from_str(&serialized).unwrap();

        assert_eq!(deserialized.default_profile, Some("test".to_string()));
        assert!(deserialized.profiles.contains_key("test"));
        let entry = &deserialized.profiles["test"];
        assert_eq!(entry.host, "10.0.0.1");
        assert_eq!(entry.port, 8000);
        assert_eq!(entry.token, "access-v1:abc123");
        assert!(entry.insecure);
    }

    #[test]
    fn test_resolve_profile_with_flag() {
        let mut config = Config::default();
        config.profiles.insert(
            "prod".to_string(),
            ProfileEntry {
                host: "10.0.0.1".to_string(),
                port: 8000,
                token: "tok".to_string(),
                insecure: false,
            },
        );

        let flag = Some("prod".to_string());
        let (name, entry) = resolve_profile(&config, &flag).unwrap();
        assert_eq!(name, "prod");
        assert_eq!(entry.host, "10.0.0.1");
    }

    #[test]
    fn test_resolve_profile_with_default() {
        let mut config = Config::default();
        config.default_profile = Some("dev".to_string());
        config.profiles.insert(
            "dev".to_string(),
            ProfileEntry {
                host: "192.168.1.1".to_string(),
                port: 8000,
                token: "tok".to_string(),
                insecure: false,
            },
        );

        let flag = None;
        let (name, _) = resolve_profile(&config, &flag).unwrap();
        assert_eq!(name, "dev");
    }

    #[test]
    fn test_resolve_profile_no_default_error() {
        let config = Config::default();
        let flag = None;
        let result = resolve_profile(&config, &flag);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_profile_not_found_error() {
        let config = Config::default();
        let flag = Some("nonexistent".to_string());
        let result = resolve_profile(&config, &flag);
        assert!(result.is_err());
    }
}
