mod harness;

use predicates::prelude::*;

/// Test: 2 clusters both healthy → both returned in output.
#[tokio::test]
async fn test_status_two_clusters_healthy() {
    let mts = harness::MultiTestServer::start(&["cluster_a", "cluster_b"]).await;
    mts.mount_cluster_fixtures("cluster_a").await;
    mts.mount_cluster_fixtures("cluster_b").await;

    mts.command()
        .arg("status")
        .assert()
        .success()
        .stdout(predicate::str::contains("Environment:"))
        .stdout(predicate::str::contains("2 clusters"));
}

/// Test: JSON mode returns valid parseable JSON with cluster data.
#[tokio::test]
async fn test_status_json_output() {
    let mts = harness::MultiTestServer::start(&["cluster_a"]).await;
    mts.mount_cluster_fixtures("cluster_a").await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert!(json.get("aggregates").is_some());
    assert!(json.get("clusters").is_some());
    assert!(json.get("alerts").is_some());
    assert_eq!(json["aggregates"]["cluster_count"], 1);
}

/// Test: 1 healthy + 1 unreachable → healthy returned, unreachable flagged as alert.
#[tokio::test]
async fn test_status_one_healthy_one_unreachable() {
    let mts = harness::MultiTestServer::start(&["healthy", "broken"]).await;
    mts.mount_cluster_fixtures("healthy").await;
    // Don't mount anything on "broken" — all requests will get 404

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    // Should have 1 reachable cluster
    assert_eq!(json["aggregates"]["reachable_count"], 1);
    // Should have alerts about the broken cluster
    let alerts = json["alerts"].as_array().expect("alerts should be array");
    assert!(
        alerts
            .iter()
            .any(|a| a["cluster"].as_str() == Some("broken")),
        "should have alert for broken cluster"
    );
}

/// Test: --profile filter works — only queries the specified profile.
#[tokio::test]
async fn test_status_profile_filter() {
    let mts = harness::MultiTestServer::start(&["cluster_a", "cluster_b"]).await;
    mts.mount_cluster_fixtures("cluster_a").await;
    mts.mount_cluster_fixtures("cluster_b").await;

    let output = mts
        .command()
        .args(["status", "--json", "--cluster", "cluster_a"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    // Should only have 1 cluster
    assert_eq!(json["aggregates"]["cluster_count"], 1);
    let clusters = json["clusters"]
        .as_array()
        .expect("clusters should be array");
    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0]["profile"], "cluster_a");
}

/// Test: status command with the 'st' alias works.
#[tokio::test]
async fn test_status_alias() {
    let mts = harness::MultiTestServer::start(&["test_cluster"]).await;
    mts.mount_cluster_fixtures("test_cluster").await;

    mts.command()
        .arg("st")
        .assert()
        .success()
        .stdout(predicate::str::contains("Environment:"));
}

/// Test: cluster type detection shows up correctly in JSON output.
#[tokio::test]
async fn test_status_detects_cluster_type() {
    let mts = harness::MultiTestServer::start(&["azure_cluster"]).await;
    mts.mount_cluster_fixtures("azure_cluster").await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let clusters = json["clusters"].as_array().expect("clusters");
    assert_eq!(clusters.len(), 1);
    // The fixture has model_number "Azure", so type should be AnqAzure
    assert_eq!(clusters[0]["type"], "AnqAzure");
}

/// Test: help text shows the status command.
#[tokio::test]
async fn test_help_shows_status() {
    let mts = harness::MultiTestServer::start(&["test"]).await;

    mts.command()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("status"));
}

/// Test: JSON output includes per-node network details (connections + NIC stats).
#[tokio::test]
async fn test_status_json_includes_node_network_details() {
    let mts = harness::MultiTestServer::start(&["net_cluster"]).await;
    mts.mount_cluster_fixtures("net_cluster").await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let clusters = json["clusters"].as_array().expect("clusters");
    assert_eq!(clusters.len(), 1);

    let nodes = &clusters[0]["nodes"];
    assert!(nodes["total"].as_u64().unwrap() > 0);

    // Should have per-node details
    let details = nodes["details"].as_array().expect("node details array");
    assert!(!details.is_empty(), "should have node network details");

    // Each node should have connection count and breakdown
    for detail in details {
        assert!(detail.get("node_id").is_some());
        assert!(detail.get("connections").is_some());
        assert!(detail.get("connection_breakdown").is_some());
    }
}

/// Test: network connections 403 doesn't fail the whole cluster.
#[tokio::test]
async fn test_status_network_connections_403_graceful() {
    let mts = harness::MultiTestServer::start(&["partial"]).await;
    // Mount core fixtures but error on network connections
    for fixture in &[
        "cluster_settings",
        "version",
        "cluster_nodes",
        "filesystem",
        "analytics_activity",
        "network_status",
    ] {
        mts.mount_fixture("partial", fixture).await;
    }
    mts.mount_error("partial", "GET", "/v2/network/connections/", 403)
        .await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    // Cluster should still be reachable despite connections 403
    assert_eq!(json["aggregates"]["reachable_count"], 1);
}

/// Test: network status 403 doesn't fail the whole cluster.
#[tokio::test]
async fn test_status_network_status_403_graceful() {
    let mts = harness::MultiTestServer::start(&["partial"]).await;
    for fixture in &[
        "cluster_settings",
        "version",
        "cluster_nodes",
        "filesystem",
        "analytics_activity",
        "network_connections",
    ] {
        mts.mount_fixture("partial", fixture).await;
    }
    mts.mount_error("partial", "GET", "/v3/network/status", 403)
        .await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert_eq!(json["aggregates"]["reachable_count"], 1);
    // Should still have node details (from connections), just no NIC data
    let details = json["clusters"][0]["nodes"]["details"]
        .as_array()
        .expect("details");
    assert!(!details.is_empty());
}

/// Test: on-prem cluster has link speed in node details.
#[tokio::test]
async fn test_status_onprem_has_link_speed() {
    let mts = harness::MultiTestServer::start(&["onprem"]).await;
    // Use gravytrain fixtures (on-prem model_numbers + speed "200000")
    let subdir = "status/gravytrain";
    mts.mount_fixture_from("onprem", "cluster_settings", subdir)
        .await;
    mts.mount_fixture_from("onprem", "version", subdir).await;
    mts.mount_fixture_from("onprem", "cluster_nodes", subdir)
        .await;
    mts.mount_fixture_from("onprem", "network_connections", subdir)
        .await;
    mts.mount_fixture_from("onprem", "network_status", subdir)
        .await;
    // Mount shared fixtures for the ones not in gravytrain subdir
    mts.mount_fixture("onprem", "filesystem").await;
    mts.mount_fixture("onprem", "analytics_activity").await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let details = json["clusters"][0]["nodes"]["details"]
        .as_array()
        .expect("details");
    assert!(!details.is_empty());
    // Fixture has on-prem nodes with speed "200000" = 200 Gbps
    for detail in details {
        let link = detail["nic_link_speed_bps"].as_u64();
        assert!(
            link.is_some(),
            "on-prem nodes should have link speed, got: {:?}",
            detail
        );
        assert_eq!(link.unwrap(), 200_000_000_000);
    }
}
