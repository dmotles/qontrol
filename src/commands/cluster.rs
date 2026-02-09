use anyhow::Result;
use serde_json::json;

use crate::client::QumuloClient;
use crate::output::{print_table, print_value};

pub fn info(client: &QumuloClient, json_mode: bool) -> Result<()> {
    let settings = client.get_cluster_settings()?;
    let version = client.get_version()?;
    let nodes = client.get_cluster_nodes()?;

    if json_mode {
        let combined = json!({
            "cluster": settings,
            "version": version,
            "nodes": nodes,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&combined).unwrap_or_else(|_| combined.to_string())
        );
        return Ok(());
    }

    // Human-readable output
    let cluster_name = settings
        .get("cluster_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let revision = version
        .get("revision_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    println!("Cluster: {}", cluster_name);
    println!("Version: {}", revision);
    println!();

    // Node table
    print_value(&nodes, false, |nodes_val| {
        print_table(nodes_val, &["id", "node_name", "node_status"]);
    });

    Ok(())
}
