use anyhow::{Context, Result};
use serde_json::Value;

use crate::client::QumuloClient;

pub fn raw(client: &QumuloClient, method: &str, path: &str, body: Option<&str>) -> Result<()> {
    let body_value: Option<Value> = match body {
        Some(b) => {
            let parsed: Value =
                serde_json::from_str(b).context("failed to parse --body as JSON")?;
            Some(parsed)
        }
        None => None,
    };

    let result = client.request(method, path, body_value.as_ref())?;

    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string())
    );

    Ok(())
}
