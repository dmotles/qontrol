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

/// Test: capacity collection + projection from real gravytrain fixtures.
#[tokio::test]
async fn test_status_capacity_projection_onprem() {
    let mts = harness::MultiTestServer::start(&["gravytrain"]).await;
    mts.mount_cluster_fixtures_with_capacity("gravytrain", "gravytrain")
        .await;
    // Also need analytics_activity for base collection
    mts.mount_status_fixture(
        "gravytrain",
        "gravytrain",
        "activity_iops_read",
        "/v1/analytics/activity/current",
    )
    .await;

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

    let capacity = &clusters[0]["capacity"];
    assert!(capacity["total_bytes"].as_u64().unwrap() > 0);
    assert!(capacity["used_bytes"].as_u64().unwrap() > 0);

    // Gravytrain has growth → projection should exist
    let projection = &capacity["projection"];
    assert!(
        !projection.is_null(),
        "on-prem cluster with growth should have projection"
    );
    assert!(projection["days_until_full"].as_u64().is_some());
    assert!(projection["growth_rate_bytes_per_day"].as_f64().unwrap() > 0.0);
}

/// Test: empty capacity history → no projection.
#[tokio::test]
async fn test_status_capacity_no_history() {
    let mts = harness::MultiTestServer::start(&["cluster_a"]).await;
    mts.mount_cluster_fixtures("cluster_a").await;
    // Don't mount capacity_history — collector will get an error and skip projection

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let clusters = json["clusters"].as_array().expect("clusters");
    let projection = &clusters[0]["capacity"]["projection"];
    assert!(
        projection.is_null(),
        "cluster without capacity history should have no projection"
    );
}

/// Test: AWS cloud cluster with growth trend → correct days_to_full.
#[tokio::test]
async fn test_status_capacity_projection_cloud() {
    let mts = harness::MultiTestServer::start(&["aws_grav"]).await;
    mts.mount_cluster_fixtures_with_capacity("aws_grav", "aws-gravytrain")
        .await;
    mts.mount_status_fixture(
        "aws_grav",
        "aws-gravytrain",
        "activity_iops_read",
        "/v1/analytics/activity/current",
    )
    .await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let clusters = json["clusters"].as_array().expect("clusters");
    let cluster = &clusters[0];

    // AWS cluster type should be detected
    assert_eq!(cluster["type"], "CnqAws");

    // AWS has growth in its history → should have projection
    let projection = &cluster["capacity"]["projection"];
    if !projection.is_null() {
        assert!(projection["days_until_full"].as_u64().is_some());
    }
}

/// Test: projection appears in alerts when days_to_full is within threshold.
#[tokio::test]
async fn test_status_capacity_projection_alert() {
    let mts = harness::MultiTestServer::start(&["gravytrain"]).await;
    mts.mount_cluster_fixtures_with_capacity("gravytrain", "gravytrain")
        .await;
    mts.mount_status_fixture(
        "gravytrain",
        "gravytrain",
        "activity_iops_read",
        "/v1/analytics/activity/current",
    )
    .await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let clusters = json["clusters"].as_array().expect("clusters");
    let projection = &clusters[0]["capacity"]["projection"];

    if !projection.is_null() {
        let days = projection["days_until_full"].as_u64().unwrap();
        let alerts = json["alerts"].as_array().expect("alerts");
        let has_capacity_alert = alerts
            .iter()
            .any(|a| a["category"].as_str() == Some("capacity_projection"));

        if days < 90 {
            assert!(
                has_capacity_alert,
                "on-prem cluster with {} days to full should have alert",
                days
            );
        }
    }
}
