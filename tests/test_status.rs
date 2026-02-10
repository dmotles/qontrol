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

// ============================================================
// Wave 2d: Stats + activity collector tests
// ============================================================

/// Test: full status with real gravytrain fixtures → correct file/dir/snapshot counts.
#[tokio::test]
async fn test_status_file_stats_from_fixtures() {
    let mts = harness::MultiTestServer::start(&["gt"]).await;
    mts.mount_full_status_fixtures("gt", "gravytrain").await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let cluster = &json["clusters"][0];
    let files = &cluster["files"];

    // gravytrain: sum of num_files across recursive aggregates = 1,807,976,645
    assert_eq!(files["total_files"].as_u64().unwrap(), 1_807_976_645);
    // gravytrain: sum of num_directories = 219,679,366
    assert_eq!(files["total_directories"].as_u64().unwrap(), 219_679_366);
    // gravytrain: 2147 snapshots
    assert_eq!(files["total_snapshots"].as_u64().unwrap(), 2147);
    // gravytrain: snapshot bytes = 7755127889920
    assert_eq!(files["snapshot_bytes"].as_u64().unwrap(), 7_755_127_889_920);
}

/// Test: aws-gravytrain (idle cloud cluster) → correct stats and idle detection.
#[tokio::test]
async fn test_status_idle_cluster_detection() {
    let mts = harness::MultiTestServer::start(&["aws"]).await;
    mts.mount_full_status_fixtures("aws", "aws-gravytrain")
        .await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let cluster = &json["clusters"][0];

    // AWS cluster has empty activity → should be idle
    assert_eq!(cluster["activity"]["is_idle"], true);
    assert_eq!(cluster["activity"]["iops_read"].as_f64().unwrap(), 0.0);
    assert_eq!(cluster["activity"]["iops_write"].as_f64().unwrap(), 0.0);

    // aws-gravytrain file stats
    let files = &cluster["files"];
    assert_eq!(files["total_files"].as_u64().unwrap(), 150_502_822);
    assert_eq!(files["total_directories"].as_u64().unwrap(), 5_522_888);
    assert_eq!(files["total_snapshots"].as_u64().unwrap(), 43);
    assert_eq!(files["snapshot_bytes"].as_u64().unwrap(), 54_855_823_360);
}

/// Test: active cluster (gravytrain) → correct IOPS/throughput sums, not idle.
#[tokio::test]
async fn test_status_active_cluster_activity() {
    let mts = harness::MultiTestServer::start(&["gt"]).await;
    mts.mount_full_status_fixtures("gt", "gravytrain").await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let activity = &json["clusters"][0]["activity"];

    // Active cluster should NOT be idle
    assert_eq!(activity["is_idle"], false);

    // IOPS and throughput should be non-zero
    assert!(activity["iops_read"].as_f64().unwrap() > 0.0);
    assert!(activity["iops_write"].as_f64().unwrap() > 0.0);
    assert!(activity["throughput_read"].as_f64().unwrap() > 0.0);
    assert!(activity["throughput_write"].as_f64().unwrap() > 0.0);
}

/// Test: empty snapshot list → count = 0.
#[tokio::test]
async fn test_status_empty_snapshots() {
    let mts = harness::MultiTestServer::start(&["cluster_a"]).await;
    mts.mount_cluster_fixtures("cluster_a").await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    // mount_cluster_fixtures mounts empty responses for new endpoints
    assert_eq!(
        json["clusters"][0]["files"]["total_snapshots"]
            .as_u64()
            .unwrap(),
        0
    );
    assert_eq!(
        json["clusters"][0]["files"]["total_files"]
            .as_u64()
            .unwrap(),
        0
    );
}

/// Test: partial failure — activity endpoint returns 403, rest of data still collected.
#[tokio::test]
async fn test_status_partial_activity_failure() {
    let mts = harness::MultiTestServer::start(&["gt"]).await;
    // Mount core fixtures from real gravytrain data
    mts.mount_status_fixture(
        "gt",
        "gravytrain",
        "cluster_settings",
        "GET",
        "/v1/cluster/settings",
    )
    .await;
    mts.mount_status_fixture("gt", "gravytrain", "version", "GET", "/v1/version")
        .await;
    mts.mount_status_fixture(
        "gt",
        "gravytrain",
        "cluster_nodes",
        "GET",
        "/v1/cluster/nodes/",
    )
    .await;
    mts.mount_status_fixture("gt", "gravytrain", "file_system", "GET", "/v1/file-system")
        .await;
    mts.mount_status_fixture(
        "gt",
        "gravytrain",
        "recursive_aggregates",
        "GET",
        "/v1/files/%2F/recursive-aggregates/",
    )
    .await;
    mts.mount_status_fixture(
        "gt",
        "gravytrain",
        "snapshots_list",
        "GET",
        "/v2/snapshots/",
    )
    .await;
    mts.mount_status_fixture(
        "gt",
        "gravytrain",
        "snapshots_total_capacity",
        "GET",
        "/v1/snapshots/total-used-capacity",
    )
    .await;
    // DO NOT mount activity endpoints — they'll return 404

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let cluster = &json["clusters"][0];

    // File stats should still be collected despite activity failure
    assert_eq!(
        cluster["files"]["total_files"].as_u64().unwrap(),
        1_807_976_645
    );
    assert_eq!(cluster["files"]["total_snapshots"].as_u64().unwrap(), 2147);

    // Activity should default to zeros (idle)
    assert_eq!(cluster["activity"]["is_idle"], true);
    assert_eq!(cluster["activity"]["iops_read"].as_f64().unwrap(), 0.0);
}

/// Test: multi-cluster aggregation with real fixtures.
#[tokio::test]
async fn test_status_multi_cluster_aggregates() {
    let mts = harness::MultiTestServer::start(&["gt", "aws"]).await;
    mts.mount_full_status_fixtures("gt", "gravytrain").await;
    mts.mount_full_status_fixtures("aws", "aws-gravytrain")
        .await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let agg = &json["aggregates"];
    assert_eq!(agg["cluster_count"], 2);
    assert_eq!(agg["reachable_count"], 2);

    // Aggregated file stats: gravytrain + aws-gravytrain
    let files = &agg["files"];
    assert_eq!(
        files["total_files"].as_u64().unwrap(),
        1_807_976_645 + 150_502_822
    );
    assert_eq!(
        files["total_directories"].as_u64().unwrap(),
        219_679_366 + 5_522_888
    );
    assert_eq!(files["total_snapshots"].as_u64().unwrap(), 2147 + 43);
}

/// Test: JSON output includes is_idle field.
#[tokio::test]
async fn test_status_json_includes_is_idle() {
    let mts = harness::MultiTestServer::start(&["cluster_a"]).await;
    mts.mount_cluster_fixtures("cluster_a").await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    // is_idle field should exist in activity
    assert!(json["clusters"][0]["activity"].get("is_idle").is_some());
    // With empty fixtures, activity should be idle
    assert_eq!(json["clusters"][0]["activity"]["is_idle"], true);
}
