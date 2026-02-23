use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;

use crate::cache::DiskCache;
use crate::config::ProfileEntry;
use crate::error::QontrolError;

/// TTL for slow, rarely-changing endpoints (chassis PSU, cluster settings, disk slots).
const TTL_SLOW: Duration = Duration::from_secs(300); // 5 minutes

/// TTL for moderate endpoints (file aggregates, snapshots, capacity history).
const TTL_MODERATE: Duration = Duration::from_secs(30);

pub struct QumuloClient {
    client: Client,
    base_url: String,
    token: String,
    cache: Option<DiskCache>,
}

impl QumuloClient {
    /// Create a client for a host without a saved profile (used during login flow).
    pub fn from_host(
        host: &str,
        port: u16,
        insecure: bool,
        timeout_secs: u64,
        token: &str,
    ) -> Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(insecure)
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build HTTP client")?;

        let base_url = std::env::var("QONTROL_BASE_URL")
            .unwrap_or_else(|_| format!("https://{}:{}", host, port));

        Ok(Self {
            client,
            base_url,
            token: token.to_string(),
            cache: None,
        })
    }

    pub fn new(profile: &ProfileEntry, timeout_secs: u64, cache: Option<DiskCache>) -> Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(profile.insecure)
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build HTTP client")?;

        let base_url = profile
            .base_url
            .clone()
            .or_else(|| std::env::var("QONTROL_BASE_URL").ok())
            .unwrap_or_else(|| format!("https://{}:{}", profile.host, profile.port));

        Ok(Self {
            client,
            base_url,
            token: profile.token.clone(),
            cache,
        })
    }

    /// Make an API request and return the parsed JSON response
    pub fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);

        tracing::debug!(%method, %url, "sending request");

        let method = method
            .parse::<reqwest::Method>()
            .context("invalid HTTP method")?;

        let mut req = self
            .client
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.token));

        if let Some(body) = body {
            req = req.json(body);
        }

        let response = req
            .send()
            .with_context(|| format!("request to {} failed", url))?;

        let status = response.status();
        let response_body = response
            .text()
            .with_context(|| "failed to read response body")?;

        tracing::debug!(status = %status.as_u16(), body_len = response_body.len(), "received response");

        if !status.is_success() {
            return Err(QontrolError::ApiError {
                status: status.as_u16(),
                body: response_body,
            }
            .into());
        }

        // Handle empty responses (e.g. 204 No Content)
        if response_body.is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_str(&response_body).with_context(|| "failed to parse response as JSON")
    }

    /// Make an API request without the Authorization header (for unauthenticated endpoints like login).
    pub fn request_no_auth(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);

        tracing::debug!(%method, %url, "sending unauthenticated request");

        let method = method
            .parse::<reqwest::Method>()
            .context("invalid HTTP method")?;

        let mut req = self.client.request(method, &url);

        if let Some(body) = body {
            req = req.json(body);
        }

        let response = req
            .send()
            .with_context(|| format!("request to {} failed", url))?;

        let status = response.status();
        let response_body = response
            .text()
            .with_context(|| "failed to read response body")?;

        tracing::debug!(status = %status.as_u16(), body_len = response_body.len(), "received response");

        if !status.is_success() {
            return Err(QontrolError::ApiError {
                status: status.as_u16(),
                body: response_body,
            }
            .into());
        }

        if response_body.is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_str(&response_body).with_context(|| "failed to parse response as JSON")
    }

    /// Check disk cache for a GET response; if missing or expired, fetch from API and cache it.
    /// When no cache is configured, this is equivalent to a plain GET request.
    fn cached_get(&self, path: &str, ttl: Duration) -> Result<Value> {
        if let Some(ref cache) = self.cache {
            if let Some(value) = cache.get(path, ttl) {
                tracing::debug!(path = %path, "disk cache hit");
                return Ok(value);
            }
        }

        let result = self.request("GET", path, None)?;

        if let Some(ref cache) = self.cache {
            cache.put(path, ttl, &result);
        }

        Ok(result)
    }

    // Convenience methods for cluster commands

    pub fn get_cluster_settings(&self) -> Result<Value> {
        self.cached_get("/v1/cluster/settings", TTL_SLOW)
    }

    pub fn get_version(&self) -> Result<Value> {
        self.request("GET", "/v1/version", None)
    }

    pub fn get_node_state(&self) -> Result<Value> {
        self.request("GET", "/v1/node/state", None)
    }

    pub fn get_cluster_nodes(&self) -> Result<Value> {
        self.cached_get("/v1/cluster/nodes/", TTL_SLOW)
    }

    pub fn get_file_system(&self) -> Result<Value> {
        self.request("GET", "/v1/file-system", None)
    }

    /// Fetch capacity history for the last N days.
    pub fn get_capacity_history(&self, begin_time_epoch: i64) -> Result<Value> {
        let path = format!(
            "/v1/analytics/capacity-history/?begin-time={}&interval=DAILY",
            begin_time_epoch
        );
        self.cached_get(&path, TTL_MODERATE)
    }

    pub fn get_activity_by_type(&self, activity_type: &str) -> Result<Value> {
        self.request(
            "GET",
            &format!("/v1/analytics/activity/current?type={}", activity_type),
            None,
        )
    }

    // Health endpoints

    pub fn get_cluster_slots(&self) -> Result<Value> {
        self.cached_get("/v1/cluster/slots/", TTL_SLOW)
    }

    pub fn get_cluster_chassis(&self) -> Result<Value> {
        self.cached_get("/v1/cluster/nodes/chassis/", TTL_SLOW)
    }

    pub fn get_cluster_protection_status(&self) -> Result<Value> {
        self.request("GET", "/v1/cluster/protection/status", None)
    }

    pub fn get_cluster_restriper_status(&self) -> Result<Value> {
        self.request("GET", "/v1/cluster/restriper/status", None)
    }

    // Network endpoints

    pub fn get_network_connections(&self) -> Result<Value> {
        self.cached_get("/v2/network/connections/", TTL_MODERATE)
    }

    pub fn get_network_status(&self) -> Result<Value> {
        self.request("GET", "/v3/network/status", None)
    }

    // Snapshot methods

    pub fn get_snapshots(&self) -> Result<Value> {
        self.cached_get("/v2/snapshots/", TTL_MODERATE)
    }

    pub fn get_snapshots_total_capacity(&self) -> Result<Value> {
        self.cached_get("/v1/snapshots/total-used-capacity", TTL_MODERATE)
    }

    pub fn get_snapshot(&self, id: u64) -> Result<Value> {
        self.request("GET", &format!("/v2/snapshots/{}", id), None)
    }

    pub fn get_snapshot_capacity_per_snapshot(&self) -> Result<Value> {
        self.request("GET", "/v1/snapshots/capacity-used-per-snapshot/", None)
    }

    pub fn get_snapshot_policies(&self) -> Result<Value> {
        self.request("GET", "/v2/snapshots/policies/", None)
    }

    pub fn calculate_snapshot_capacity(&self, ids: &[u64]) -> Result<Value> {
        let body = Value::Array(ids.iter().map(|id| Value::from(*id)).collect());
        self.request("POST", "/v1/snapshots/calculate-used-capacity", Some(&body))
    }

    pub fn get_snapshot_diff(&self, newer_id: u64, older_id: u64) -> Result<Value> {
        self.request(
            "GET",
            &format!("/v2/snapshots/{}/changes-since/{}", newer_id, older_id),
            None,
        )
    }

    // Convenience methods for filesystem commands

    /// List directory entries at a given path (by ref like inode ID or path)
    pub fn get_file_entries(
        &self,
        path: &str,
        after: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Value> {
        let encoded = urlencoding::encode(path);
        let mut url = format!(
            "/v1/files/%2F{}/entries/",
            encoded.trim_start_matches("%2F")
        );
        // Root path is special - just /v1/files/%2F/entries/
        if path == "/" {
            url = "/v1/files/%2F/entries/".to_string();
        }
        let mut params = Vec::new();
        if let Some(after) = after {
            params.push(format!("after={}", urlencoding::encode(after)));
        }
        if let Some(limit) = limit {
            params.push(format!("limit={}", limit));
        }
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }
        self.request("GET", &url, None)
    }

    /// Get file/directory attributes
    pub fn get_file_attr(&self, path: &str) -> Result<Value> {
        let encoded = urlencoding::encode(path);
        let mut url = format!(
            "/v1/files/%2F{}/info/attributes",
            encoded.trim_start_matches("%2F")
        );
        if path == "/" {
            url = "/v1/files/%2F/info/attributes".to_string();
        }
        self.request("GET", &url, None)
    }

    /// Get aggregated data for a path (file count, size totals, etc.)
    /// Uses max-entries=0 to return only the root inode totals without walking children.
    pub fn get_file_aggregates(&self, path: &str) -> Result<Value> {
        let encoded = urlencoding::encode(path);
        let mut url = format!(
            "/v1/files/%2F{}/aggregates/?max-entries=0",
            encoded.trim_start_matches("%2F")
        );
        if path == "/" {
            url = "/v1/files/%2F/aggregates/?max-entries=0".to_string();
        }
        self.cached_get(&url, TTL_SLOW)
    }

    /// Fetch all directory entries by paginating through all pages.
    /// Returns a Vec of all file entry objects.
    pub fn get_all_file_entries(&self, path: &str) -> Result<Vec<Value>> {
        let mut all_entries = Vec::new();
        let mut after: Option<String> = None;

        loop {
            let response = self.get_file_entries(path, after.as_deref(), None)?;

            if let Some(files) = response.get("files").and_then(|v| v.as_array()) {
                all_entries.extend(files.iter().cloned());
            }

            // Check if there's a next page
            match response
                .get("paging")
                .and_then(|p| p.get("next"))
                .and_then(|n| n.as_str())
            {
                Some(next) if !next.is_empty() => {
                    after = Some(next.to_string());
                }
                _ => break,
            }
        }

        Ok(all_entries)
    }

    /// Get recursive aggregates for a path
    pub fn get_file_recursive_aggregates(&self, path: &str) -> Result<Value> {
        let encoded = urlencoding::encode(path);
        let mut url = format!(
            "/v1/files/%2F{}/recursive-aggregates/",
            encoded.trim_start_matches("%2F")
        );
        if path == "/" {
            url = "/v1/files/%2F/recursive-aggregates/".to_string();
        }
        self.cached_get(&url, TTL_MODERATE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::DiskCache;
    use serde_json::json;

    fn make_cache(uuid: &str) -> (tempfile::TempDir, DiskCache) {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: uuid.to_string(),
        };
        (tmp, cache)
    }

    #[test]
    fn test_cache_hit_within_ttl() {
        let (_tmp, cache) = make_cache("test-cluster");
        let value = json!({"cluster_name": "test"});
        cache.put("/v1/cluster/settings", TTL_SLOW, &value);

        let result = cache.get("/v1/cluster/settings", TTL_SLOW);
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_cache_miss_after_ttl() {
        let (_tmp, cache) = make_cache("test-cluster");
        let value = json!({"cluster_name": "test"});
        cache.put("/v1/cluster/settings", TTL_SLOW, &value);

        // Request with 0-second max_age — always expired
        let result = cache.get("/v1/cluster/settings", Duration::from_secs(0));
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_key_includes_cluster_uuid() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache1 = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "cluster1".to_string(),
        };
        let cache2 = DiskCache {
            cache_dir: tmp.path().join("api"),
            cluster_uuid: "cluster2".to_string(),
        };

        cache1.put("/v1/cluster/settings", TTL_SLOW, &json!({"from": "c1"}));
        cache2.put("/v1/cluster/settings", TTL_SLOW, &json!({"from": "c2"}));

        assert_eq!(
            cache1.get("/v1/cluster/settings", TTL_SLOW),
            Some(json!({"from": "c1"}))
        );
        assert_eq!(
            cache2.get("/v1/cluster/settings", TTL_SLOW),
            Some(json!({"from": "c2"}))
        );
    }

    #[test]
    fn test_cache_thread_safety() {
        let (_tmp, cache) = make_cache("test-cluster");
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let c = cache.clone();
                std::thread::spawn(move || {
                    let path = format!("/v1/endpoint/{}", i);
                    let value = json!({"id": i});
                    c.put(&path, TTL_SLOW, &value);
                    let result = c.get(&path, TTL_SLOW);
                    assert_eq!(result, Some(value));
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_ttl_constants() {
        assert_eq!(TTL_SLOW, Duration::from_secs(300));
        assert_eq!(TTL_MODERATE, Duration::from_secs(30));
    }
}
