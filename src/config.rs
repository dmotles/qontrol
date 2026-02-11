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
    /// Cluster UUID fetched from GET /v1/node/state → cluster_id. Persisted for cache keying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_uuid: Option<String>,
    /// Override the base URL for this profile (e.g. "http://proxy:8080"). Used by test harness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
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

/// Backfill missing cluster UUIDs for all profiles.
/// Connects to each cluster that lacks a UUID, fetches it from /v1/node/state, and saves.
/// Failures are logged and skipped — this never blocks normal operation.
pub fn ensure_cluster_uuids(config: &mut Config, timeout_secs: u64) {
    let mut updated = false;
    for (name, entry) in config.profiles.iter_mut() {
        if entry.cluster_uuid.is_some() {
            continue;
        }
        let client = match crate::client::QumuloClient::new(entry, timeout_secs, None) {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(profile = %name, error = %e, "skipping UUID backfill: cannot connect");
                continue;
            }
        };
        match client.get_node_state() {
            Ok(state) => {
                if let Some(uuid) = state["cluster_id"].as_str() {
                    tracing::info!(profile = %name, uuid = %uuid, "backfilled cluster UUID");
                    entry.cluster_uuid = Some(uuid.to_string());
                    updated = true;
                }
            }
            Err(e) => {
                tracing::debug!(profile = %name, error = %e, "skipping UUID backfill: API call failed");
            }
        }
    }
    if updated {
        if let Err(e) = save_config(config) {
            tracing::warn!(error = %e, "failed to save config after UUID backfill");
        }
    }
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
                cluster_uuid: None,
                base_url: None,
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
                cluster_uuid: None,
                base_url: None,
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
                cluster_uuid: None,
                base_url: None,
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

    #[test]
    fn test_toml_roundtrip_with_cluster_uuid() {
        let mut config = Config::default();
        config.profiles.insert(
            "test".to_string(),
            ProfileEntry {
                host: "10.0.0.1".to_string(),
                port: 8000,
                token: "tok".to_string(),
                insecure: false,
                cluster_uuid: Some("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()),
                base_url: None,
            },
        );

        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("cluster_uuid"));
        let deserialized: Config = toml::from_str(&serialized).unwrap();
        let entry = &deserialized.profiles["test"];
        assert_eq!(
            entry.cluster_uuid.as_deref(),
            Some("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        );
    }

    #[test]
    fn test_toml_roundtrip_without_cluster_uuid() {
        // Old config files without cluster_uuid should deserialize with None
        let toml_str = r#"
[profiles.old]
host = "10.0.0.1"
port = 8000
token = "tok"
insecure = false
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let entry = &config.profiles["old"];
        assert_eq!(entry.cluster_uuid, None);
    }

    #[test]
    fn test_cluster_uuid_none_not_serialized() {
        let mut config = Config::default();
        config.profiles.insert(
            "test".to_string(),
            ProfileEntry {
                host: "10.0.0.1".to_string(),
                port: 8000,
                token: "tok".to_string(),
                insecure: false,
                cluster_uuid: None,
                base_url: None,
            },
        );

        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(!serialized.contains("cluster_uuid"));
    }
}
