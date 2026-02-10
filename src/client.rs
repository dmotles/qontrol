use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;

use crate::config::ProfileEntry;
use crate::error::QontrolError;

pub struct QumuloClient {
    client: Client,
    base_url: String,
    token: String,
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
        })
    }

    pub fn new(profile: &ProfileEntry, timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(profile.insecure)
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build HTTP client")?;

        let base_url = std::env::var("QONTROL_BASE_URL")
            .unwrap_or_else(|_| format!("https://{}:{}", profile.host, profile.port));

        Ok(Self {
            client,
            base_url,
            token: profile.token.clone(),
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

    // Convenience methods for cluster commands

    pub fn get_cluster_settings(&self) -> Result<Value> {
        self.request("GET", "/v1/cluster/settings", None)
    }

    pub fn get_version(&self) -> Result<Value> {
        self.request("GET", "/v1/version", None)
    }

    pub fn get_cluster_nodes(&self) -> Result<Value> {
        self.request("GET", "/v1/cluster/nodes/", None)
    }

    pub fn get_file_system(&self) -> Result<Value> {
        self.request("GET", "/v1/file-system", None)
    }

    pub fn get_activity_current(&self) -> Result<Value> {
        self.request("GET", "/v1/analytics/activity/current", None)
    }

    // Snapshot methods

    pub fn get_snapshots(&self) -> Result<Value> {
        self.request("GET", "/v2/snapshots/", None)
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

    /// Fetch all directory entries across all pages.
    pub fn get_all_file_entries(&self, path: &str) -> Result<Vec<Value>> {
        let mut all_entries = Vec::new();
        let mut after: Option<String> = None;

        loop {
            let response = self.get_file_entries(path, after.as_deref(), None)?;

            if let Some(files) = response.get("files").and_then(|v| v.as_array()) {
                all_entries.extend(files.iter().cloned());
            }

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
    #[allow(dead_code)]
    pub fn get_file_aggregates(&self, path: &str) -> Result<Value> {
        let encoded = urlencoding::encode(path);
        let mut url = format!(
            "/v1/files/%2F{}/aggregates/",
            encoded.trim_start_matches("%2F")
        );
        if path == "/" {
            url = "/v1/files/%2F/aggregates/".to_string();
        }
        self.request("GET", &url, None)
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
        self.request("GET", &url, None)
    }
}
