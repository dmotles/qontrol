use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Envelope stored in cacache: wraps the API response with cache metadata.
#[derive(Serialize, Deserialize)]
struct CacheEntry {
    cached_at: String,
    ttl_secs: u64,
    response: Value,
}

/// Persistent, disk-based API response cache backed by cacache.
///
/// Each DiskCache is scoped to a single cluster (identified by UUID) and stores
/// responses in `~/.cache/qontrol/api/`. Thread-safe: cacache handles concurrent
/// access internally via content-addressed storage with atomic writes.
#[derive(Debug, Clone)]
pub struct DiskCache {
    pub(crate) cache_dir: PathBuf,
    pub(crate) cluster_uuid: String,
}

impl DiskCache {
    /// Create a new DiskCache for a specific cluster.
    pub fn new(cluster_uuid: &str) -> Result<Self> {
        let cache_dir = api_cache_dir()?;
        Ok(Self {
            cache_dir,
            cluster_uuid: cluster_uuid.to_string(),
        })
    }

    fn make_key(&self, path: &str) -> String {
        format!("{}:{}", self.cluster_uuid, path)
    }

    /// Read a cached API response if it exists and has not expired.
    pub fn get(&self, path: &str, max_age: Duration) -> Option<Value> {
        let key = self.make_key(path);
        let bytes = match cacache::read_sync(&self.cache_dir, &key) {
            Ok(b) => b,
            Err(_) => return None,
        };
        let entry: CacheEntry = match serde_json::from_slice(&bytes) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(key = %key, error = %e, "corrupt cache entry, treating as miss");
                return None;
            }
        };
        let cached_at = match chrono::DateTime::parse_from_rfc3339(&entry.cached_at) {
            Ok(dt) => dt,
            Err(_) => return None,
        };
        let age = chrono::Utc::now().signed_duration_since(cached_at);
        if age.num_seconds() < 0 || age.to_std().unwrap_or(Duration::MAX) > max_age {
            return None;
        }
        Some(entry.response)
    }

    /// Write an API response to the disk cache. Failures are logged, never propagated.
    pub fn put(&self, path: &str, ttl: Duration, value: &Value) {
        let key = self.make_key(path);
        let entry = CacheEntry {
            cached_at: chrono::Utc::now().to_rfc3339(),
            ttl_secs: ttl.as_secs(),
            response: value.clone(),
        };
        let bytes = match serde_json::to_vec(&entry) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(key = %key, error = %e, "failed to serialize cache entry");
                return;
            }
        };
        if let Err(e) = cacache::write_sync(&self.cache_dir, &key, &bytes) {
            tracing::warn!(key = %key, error = %e, "failed to write disk cache");
        }
    }
}

/// Return the cache directory for API response caching.
/// Respects: QONTROL_CACHE_DIR > XDG_CACHE_HOME/qontrol > ~/.cache/qontrol, then appends /api/.
fn api_cache_dir() -> Result<PathBuf> {
    let base = if let Ok(dir) = std::env::var("QONTROL_CACHE_DIR") {
        PathBuf::from(dir)
    } else if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
        PathBuf::from(xdg).join("qontrol")
    } else {
        let home = std::env::var("HOME").context("HOME not set")?;
        PathBuf::from(home).join(".cache").join("qontrol")
    };
    Ok(base.join("api"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cache_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "test-uuid".to_string(),
        };
        let value = json!({"cluster_name": "test"});
        cache.put("/v1/cluster/settings", Duration::from_secs(300), &value);

        let result = cache.get("/v1/cluster/settings", Duration::from_secs(300));
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_cache_miss_expired() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "test-uuid".to_string(),
        };
        let value = json!({"data": 42});
        cache.put("/v1/endpoint", Duration::from_secs(1), &value);

        // Request with 0-second max_age — always expired
        let result = cache.get("/v1/endpoint", Duration::from_secs(0));
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_miss_no_entry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "test-uuid".to_string(),
        };
        let result = cache.get("/v1/nonexistent", Duration::from_secs(300));
        assert!(result.is_none());
    }

    #[test]
    fn test_different_uuids_different_keys() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache_a = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "uuid-aaa".to_string(),
        };
        let cache_b = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "uuid-bbb".to_string(),
        };

        cache_a.put("/v1/data", Duration::from_secs(300), &json!({"from": "a"}));
        cache_b.put("/v1/data", Duration::from_secs(300), &json!({"from": "b"}));

        assert_eq!(
            cache_a.get("/v1/data", Duration::from_secs(300)),
            Some(json!({"from": "a"}))
        );
        assert_eq!(
            cache_b.get("/v1/data", Duration::from_secs(300)),
            Some(json!({"from": "b"}))
        );
    }

    #[test]
    fn test_cache_thread_safety() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "test-uuid".to_string(),
        };
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let c = cache.clone();
                std::thread::spawn(move || {
                    let path = format!("/v1/endpoint/{}", i);
                    let value = json!({"id": i});
                    c.put(&path, Duration::from_secs(300), &value);
                    let result = c.get(&path, Duration::from_secs(300));
                    assert_eq!(result, Some(value));
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_make_key() {
        let cache = DiskCache {
            cache_dir: PathBuf::from("/tmp/test"),
            cluster_uuid: "f83b970e-d7fd-4a2c-9e4f-87bb38990ee1".to_string(),
        };
        assert_eq!(
            cache.make_key("/v1/cluster/settings"),
            "f83b970e-d7fd-4a2c-9e4f-87bb38990ee1:/v1/cluster/settings"
        );
    }
}
