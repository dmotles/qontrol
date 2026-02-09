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
    pub fn new(profile: &ProfileEntry, timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(profile.insecure)
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build HTTP client")?;

        let base_url = format!("https://{}:{}", profile.host, profile.port);

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
}
