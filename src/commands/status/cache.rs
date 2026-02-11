use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::types::{CachedClusterData, ClusterStatus, StatusCache};

/// Return the cache directory for status data.
/// Prefers QONTROL_CACHE_DIR env var, then XDG_CACHE_HOME/qontrol, then ~/.cache/qontrol.
fn cache_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("QONTROL_CACHE_DIR") {
        return Ok(PathBuf::from(dir));
    }
    if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(xdg).join("qontrol"));
    }
    let home = std::env::var("HOME").context("HOME not set")?;
    Ok(PathBuf::from(home).join(".cache").join("qontrol"))
}

fn cache_path() -> Result<PathBuf> {
    Ok(cache_dir()?.join("status-cache.json"))
}

/// Write cluster data to the per-profile cache.
pub fn write_cache(profile: &str, data: &ClusterStatus) -> Result<()> {
    write_cache_at(&cache_path()?, profile, data)
}

/// Read cached cluster data for a profile, returning None if missing or corrupt.
pub fn read_cache(profile: &str) -> Option<CachedClusterData> {
    read_cache_at(&cache_path().ok()?, profile)
}

/// Read cached data for all given profiles, returning only those with cached entries.
pub fn read_all_cache(profiles: &[String]) -> Vec<CachedClusterData> {
    let path = match cache_path() {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    read_all_cache_at(&path, profiles)
}

fn read_all_cache_at(path: &Path, profiles: &[String]) -> Vec<CachedClusterData> {
    profiles
        .iter()
        .filter_map(|p| read_cache_at(path, p))
        .collect()
}

fn write_cache_at(path: &Path, profile: &str, data: &ClusterStatus) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create cache dir: {}", parent.display()))?;
    }

    let mut cache = load_cache_at(path).unwrap_or_default();
    cache.clusters.insert(
        profile.to_string(),
        CachedClusterData {
            profile: profile.to_string(),
            data: data.clone(),
            cached_at: chrono::Utc::now().to_rfc3339(),
        },
    );

    let contents = serde_json::to_string_pretty(&cache).context("failed to serialize cache")?;
    std::fs::write(path, contents)
        .with_context(|| format!("failed to write cache: {}", path.display()))?;
    Ok(())
}

fn read_cache_at(path: &Path, profile: &str) -> Option<CachedClusterData> {
    let cache = load_cache_at(path).ok()?;
    cache.clusters.get(profile).cloned()
}

fn load_cache_at(path: &Path) -> Result<StatusCache> {
    if !path.exists() {
        return Ok(StatusCache::default());
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read cache: {}", path.display()))?;
    serde_json::from_str(&contents).with_context(|| "failed to parse cache")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::status::types::*;

    fn make_test_cluster(profile: &str) -> ClusterStatus {
        ClusterStatus {
            profile: profile.to_string(),
            name: format!("{}-cluster", profile),
            uuid: "test-uuid".to_string(),
            version: "7.7.2".to_string(),
            cluster_type: ClusterType::AnqAzure,
            reachable: true,
            stale: false,
            latency_ms: 50,
            nodes: NodeStatus {
                total: 1,
                online: 1,
                offline_nodes: vec![],
                details: vec![],
            },
            capacity: CapacityStatus::default(),
            activity: ActivityStatus::default(),
            files: FileStats::default(),
            health: HealthStatus {
                status: HealthLevel::Healthy,
                issues: vec![],
                disks_unhealthy: 0,
                psus_unhealthy: 0,
                data_at_risk: false,
                remaining_node_failures: None,
                remaining_drive_failures: None,
                protection_type: None,
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        }
    }

    #[test]
    fn test_cache_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("status-cache.json");

        let data = make_test_cluster("myprofile");
        write_cache_at(&path, "myprofile", &data).unwrap();

        let cached = read_cache_at(&path, "myprofile").expect("cache should exist");
        assert_eq!(cached.data.name, "myprofile-cluster");
        assert_eq!(cached.profile, "myprofile");
    }

    #[test]
    fn test_cache_missing_profile() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("status-cache.json");

        let result = read_cache_at(&path, "nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_corrupt_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("status-cache.json");

        std::fs::write(&path, "not valid json").unwrap();

        let result = read_cache_at(&path, "anything");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_no_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent-cache.json");

        let result = read_cache_at(&path, "anything");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_multiple_profiles() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("status-cache.json");

        let data1 = make_test_cluster("profile1");
        let data2 = make_test_cluster("profile2");

        write_cache_at(&path, "profile1", &data1).unwrap();
        write_cache_at(&path, "profile2", &data2).unwrap();

        let cached1 = read_cache_at(&path, "profile1").expect("profile1 should exist");
        let cached2 = read_cache_at(&path, "profile2").expect("profile2 should exist");
        assert_eq!(cached1.data.name, "profile1-cluster");
        assert_eq!(cached2.data.name, "profile2-cluster");
    }

    #[test]
    fn test_read_all_cache_returns_matching() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("status-cache.json");

        write_cache_at(&path, "a", &make_test_cluster("a")).unwrap();
        write_cache_at(&path, "b", &make_test_cluster("b")).unwrap();
        write_cache_at(&path, "c", &make_test_cluster("c")).unwrap();

        let profiles = vec!["a".to_string(), "c".to_string()];
        let results = read_all_cache_at(&path, &profiles);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].profile, "a");
        assert_eq!(results[1].profile, "c");
    }

    #[test]
    fn test_read_all_cache_skips_missing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("status-cache.json");

        write_cache_at(&path, "a", &make_test_cluster("a")).unwrap();

        let profiles = vec!["a".to_string(), "nonexistent".to_string()];
        let results = read_all_cache_at(&path, &profiles);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].profile, "a");
    }

    #[test]
    fn test_read_all_cache_empty_when_no_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent.json");

        let profiles = vec!["a".to_string()];
        let results = read_all_cache_at(&path, &profiles);
        assert!(results.is_empty());
    }
}
