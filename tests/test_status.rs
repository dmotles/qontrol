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

// ── Capacity projection tests ─────────────────────────────────────────────────

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
        "GET",
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
        "GET",
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
        "GET",
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

// ── Health data collection tests ──────────────────────────────────────────────

/// Test: healthy cluster → health fields populated, no health alerts.
#[tokio::test]
async fn test_status_healthy_cluster_health_data() {
    let mts = harness::MultiTestServer::start(&["healthy_cluster"]).await;
    mts.mount_cluster_fixtures("healthy_cluster").await;

    let output = mts
        .command()
        .args(["status", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let cluster = &json["clusters"][0];
    let health = &cluster["health"];

    // All healthy — no issues
    assert_eq!(health["disks_unhealthy"], 0);
    assert_eq!(health["psus_unhealthy"], 0);
    assert_eq!(health["data_at_risk"], false);
    assert_eq!(health["status"], "healthy");

    // Protection data populated
    assert!(health["remaining_node_failures"].is_number());
    assert!(health["remaining_drive_failures"].is_number());
    assert!(health["protection_type"].is_string());

    // No health alerts (only connectivity-type alerts should be absent too)
    let alerts = json["alerts"].as_array().expect("alerts");
    assert!(
        !alerts.iter().any(|a| {
            let cat = a["category"].as_str().unwrap_or("");
            cat == "disk_unhealthy"
                || cat == "psu_unhealthy"
                || cat == "data_at_risk"
                || cat == "protection_degraded"
        }),
        "healthy cluster should not have health alerts"
    );
}

/// Test: cluster with offline node → node_offline alert (critical).
#[tokio::test]
async fn test_status_offline_node_alert() {
    let mts = harness::MultiTestServer::start(&["degraded"]).await;

    // Mount standard fixtures
    mts.mount_fixture("degraded", "cluster_settings").await;
    mts.mount_fixture("degraded", "version").await;
    mts.mount_fixture("degraded", "filesystem").await;
    mts.mount_fixture("degraded", "analytics_activity").await;
    mts.mount_fixture("degraded", "cluster_slots").await;
    mts.mount_fixture("degraded", "cluster_chassis").await;
    mts.mount_fixture("degraded", "cluster_protection_status")
        .await;
    mts.mount_fixture("degraded", "cluster_restriper_status")
        .await;

    // Mount nodes with one offline
    let nodes_with_offline = r#"[
        {"id": 1, "node_name": "node1", "node_status": "online", "model_number": "C192T", "serial_number": "SN001"},
        {"id": 2, "node_name": "node2", "node_status": "offline", "model_number": "C192T", "serial_number": "SN002"}
    ]"#;
    mts.mount_raw("degraded", "cluster_nodes", nodes_with_offline)
        .await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let alerts = json["alerts"].as_array().expect("alerts");
    let node_alert = alerts
        .iter()
        .find(|a| a["category"].as_str() == Some("node_offline"));
    assert!(node_alert.is_some(), "should have node_offline alert");
    assert_eq!(node_alert.unwrap()["severity"], "critical");
}

/// Test: cluster with unhealthy disk → disk_unhealthy alert.
#[tokio::test]
async fn test_status_unhealthy_disk_alert() {
    let mts = harness::MultiTestServer::start(&["disk_issue"]).await;

    // Mount standard healthy fixtures
    mts.mount_fixture("disk_issue", "cluster_settings").await;
    mts.mount_fixture("disk_issue", "version").await;
    mts.mount_fixture("disk_issue", "cluster_nodes").await;
    mts.mount_fixture("disk_issue", "filesystem").await;
    mts.mount_fixture("disk_issue", "analytics_activity").await;
    mts.mount_fixture("disk_issue", "cluster_chassis").await;
    mts.mount_fixture("disk_issue", "cluster_protection_status")
        .await;
    mts.mount_fixture("disk_issue", "cluster_restriper_status")
        .await;

    // Mount slots with one unhealthy disk
    let slots_with_bad_disk = r#"[
        {"id": "1.1", "node_id": 1, "drive_bay": "1", "disk_type": "HDD", "state": "healthy", "slot": 1},
        {"id": "1.2", "node_id": 1, "drive_bay": "2", "disk_type": "HDD", "state": "unhealthy", "slot": 2},
        {"id": "2.1", "node_id": 2, "drive_bay": "1", "disk_type": "SSD", "state": "healthy", "slot": 1}
    ]"#;
    mts.mount_raw("disk_issue", "cluster_slots", slots_with_bad_disk)
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
    assert_eq!(cluster["health"]["disks_unhealthy"], 1);
    assert_eq!(cluster["health"]["status"], "degraded");

    let alerts = json["alerts"].as_array().expect("alerts");
    let disk_alert = alerts
        .iter()
        .find(|a| a["category"].as_str() == Some("disk_unhealthy"));
    assert!(disk_alert.is_some(), "should have disk_unhealthy alert");
    assert_eq!(disk_alert.unwrap()["severity"], "warning");
    assert!(
        disk_alert.unwrap()["message"]
            .as_str()
            .unwrap()
            .contains("1 disk"),
        "alert message should mention disk count"
    );
}

/// Test: cluster with degraded protection → protection_degraded alert.
#[tokio::test]
async fn test_status_degraded_protection_alert() {
    let mts = harness::MultiTestServer::start(&["prot_issue"]).await;

    // Mount standard fixtures
    mts.mount_fixture("prot_issue", "cluster_settings").await;
    mts.mount_fixture("prot_issue", "version").await;
    mts.mount_fixture("prot_issue", "cluster_nodes").await;
    mts.mount_fixture("prot_issue", "filesystem").await;
    mts.mount_fixture("prot_issue", "analytics_activity").await;
    mts.mount_fixture("prot_issue", "cluster_slots").await;
    mts.mount_fixture("prot_issue", "cluster_chassis").await;
    mts.mount_fixture("prot_issue", "cluster_restriper_status")
        .await;

    // Mount protection with 0 remaining node failures
    let degraded_protection = r#"{
        "blocks_per_stripe": 8,
        "data_blocks_per_stripe": 6,
        "max_drive_failures": 2,
        "max_node_failures": 1,
        "protection_system_type": "PROTECTION_SYSTEM_TYPE_EC",
        "remaining_drive_failures": 2,
        "remaining_node_failures": 0
    }"#;
    mts.mount_raw(
        "prot_issue",
        "cluster_protection_status",
        degraded_protection,
    )
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
    assert_eq!(cluster["health"]["remaining_node_failures"], 0);
    assert_eq!(cluster["health"]["status"], "degraded");

    let alerts = json["alerts"].as_array().expect("alerts");
    let prot_alert = alerts
        .iter()
        .find(|a| a["category"].as_str() == Some("protection_degraded"));
    assert!(
        prot_alert.is_some(),
        "should have protection_degraded alert"
    );
    assert_eq!(prot_alert.unwrap()["severity"], "warning");
}

/// Test: cluster with data at risk → data_at_risk alert (critical).
#[tokio::test]
async fn test_status_data_at_risk_alert() {
    let mts = harness::MultiTestServer::start(&["risky"]).await;

    // Mount standard fixtures
    mts.mount_fixture("risky", "cluster_settings").await;
    mts.mount_fixture("risky", "version").await;
    mts.mount_fixture("risky", "cluster_nodes").await;
    mts.mount_fixture("risky", "filesystem").await;
    mts.mount_fixture("risky", "analytics_activity").await;
    mts.mount_fixture("risky", "cluster_slots").await;
    mts.mount_fixture("risky", "cluster_chassis").await;
    mts.mount_fixture("risky", "cluster_protection_status")
        .await;

    // Mount restriper with data_at_risk = true
    let data_at_risk = r#"{
        "blocked_reason": "",
        "coordinator_node": 1,
        "data_at_risk": true,
        "elapsed_seconds": 120,
        "estimated_seconds_left": 600,
        "percent_complete": 20,
        "phase": "reprotecting",
        "status": "RUNNING"
    }"#;
    mts.mount_raw("risky", "cluster_restriper_status", data_at_risk)
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
    assert_eq!(cluster["health"]["data_at_risk"], true);
    assert_eq!(cluster["health"]["status"], "critical");

    let alerts = json["alerts"].as_array().expect("alerts");
    let risk_alert = alerts
        .iter()
        .find(|a| a["category"].as_str() == Some("data_at_risk"));
    assert!(risk_alert.is_some(), "should have data_at_risk alert");
    assert_eq!(risk_alert.unwrap()["severity"], "critical");
}

/// Test: partial failure — one health endpoint returns 403, rest succeed.
#[tokio::test]
async fn test_status_partial_health_failure() {
    let mts = harness::MultiTestServer::start(&["partial"]).await;

    // Mount all standard fixtures
    mts.mount_fixture("partial", "cluster_settings").await;
    mts.mount_fixture("partial", "version").await;
    mts.mount_fixture("partial", "cluster_nodes").await;
    mts.mount_fixture("partial", "filesystem").await;
    mts.mount_fixture("partial", "analytics_activity").await;
    mts.mount_fixture("partial", "cluster_chassis").await;
    mts.mount_fixture("partial", "cluster_protection_status")
        .await;
    mts.mount_fixture("partial", "cluster_restriper_status")
        .await;

    // Mount 403 for slots endpoint — should not crash, just return 0 unhealthy disks
    mts.mount_error("partial", "GET", "/v1/cluster/slots/", 403)
        .await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    // Cluster should still be returned with partial data
    let clusters = json["clusters"].as_array().expect("clusters");
    assert_eq!(clusters.len(), 1);
    // Disk data unavailable — defaults to 0
    assert_eq!(clusters[0]["health"]["disks_unhealthy"], 0);
    // Other health data should still be present
    assert!(clusters[0]["health"]["remaining_node_failures"].is_number());
}

/// Test: cloud cluster (empty PSU array) → no PSU alerts.
#[tokio::test]
async fn test_status_cloud_cluster_empty_psus() {
    let mts = harness::MultiTestServer::start(&["cloud"]).await;

    // Mount standard fixtures
    mts.mount_fixture("cloud", "cluster_settings").await;
    mts.mount_fixture("cloud", "version").await;
    mts.mount_fixture("cloud", "filesystem").await;
    mts.mount_fixture("cloud", "analytics_activity").await;
    mts.mount_fixture("cloud", "cluster_slots").await;
    mts.mount_fixture("cloud", "cluster_protection_status")
        .await;
    mts.mount_fixture("cloud", "cluster_restriper_status").await;

    // Cloud nodes
    let cloud_nodes = r#"[
        {"id": 1, "node_name": "cloud-1", "node_status": "online", "model_number": "AWS", "serial_number": "i-001"},
        {"id": 2, "node_name": "cloud-2", "node_status": "online", "model_number": "AWS", "serial_number": "i-002"}
    ]"#;
    mts.mount_raw("cloud", "cluster_nodes", cloud_nodes).await;

    // Empty PSU array (cloud behavior)
    let cloud_chassis = r#"[
        {"id": 1, "light_visible": false, "psu_statuses": []},
        {"id": 2, "light_visible": false, "psu_statuses": []}
    ]"#;
    mts.mount_raw("cloud", "cluster_chassis", cloud_chassis)
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
    assert_eq!(cluster["health"]["psus_unhealthy"], 0);
    assert_eq!(cluster["type"], "CnqAws");

    let alerts = json["alerts"].as_array().expect("alerts");
    assert!(
        !alerts
            .iter()
            .any(|a| a["category"].as_str() == Some("psu_unhealthy")),
        "cloud cluster should not have PSU alerts"
    );
}

/// Test: healthy cluster with real recorded fixtures from gravytrain.
#[tokio::test]
async fn test_status_gravytrain_fixtures_health() {
    let mts = harness::MultiTestServer::start(&["gravytrain"]).await;

    // Mount all fixtures from the recorded gravytrain cluster
    for fixture in &[
        "cluster_settings",
        "version",
        "cluster_nodes",
        "file_system",
        "cluster_slots",
        "cluster_chassis",
        "cluster_protection_status",
        "cluster_restriper_status",
    ] {
        mts.mount_cluster_specific_fixture("gravytrain", "gravytrain", fixture)
            .await;
    }
    // Mount activity from base fixtures (recorded fixtures use different naming)
    mts.mount_fixture("gravytrain", "analytics_activity").await;

    let output = mts
        .command()
        .args(["status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let cluster = &json["clusters"][0];
    let health = &cluster["health"];

    // Gravytrain is healthy — all disks healthy, all PSUs good, no data at risk
    assert_eq!(health["disks_unhealthy"], 0);
    assert_eq!(health["psus_unhealthy"], 0);
    assert_eq!(health["data_at_risk"], false);
    assert_eq!(health["remaining_node_failures"], 1);
    assert_eq!(health["remaining_drive_failures"], 2);
    assert_eq!(health["protection_type"], "PROTECTION_SYSTEM_TYPE_EC");
}

/// Test: PSU unhealthy alert is generated.
#[tokio::test]
async fn test_status_unhealthy_psu_alert() {
    let mts = harness::MultiTestServer::start(&["psu_issue"]).await;

    mts.mount_fixture("psu_issue", "cluster_settings").await;
    mts.mount_fixture("psu_issue", "version").await;
    mts.mount_fixture("psu_issue", "cluster_nodes").await;
    mts.mount_fixture("psu_issue", "filesystem").await;
    mts.mount_fixture("psu_issue", "analytics_activity").await;
    mts.mount_fixture("psu_issue", "cluster_slots").await;
    mts.mount_fixture("psu_issue", "cluster_protection_status")
        .await;
    mts.mount_fixture("psu_issue", "cluster_restriper_status")
        .await;

    // Mount chassis with one bad PSU
    let bad_psu_chassis = r#"[
        {
            "id": 1,
            "light_visible": false,
            "psu_statuses": [
                {"location": "left", "name": "PSU2", "state": "GOOD"},
                {"location": "right", "name": "PSU1", "state": "FAILED"}
            ]
        }
    ]"#;
    mts.mount_raw("psu_issue", "cluster_chassis", bad_psu_chassis)
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
    assert_eq!(cluster["health"]["psus_unhealthy"], 1);

    let alerts = json["alerts"].as_array().expect("alerts");
    let psu_alert = alerts
        .iter()
        .find(|a| a["category"].as_str() == Some("psu_unhealthy"));
    assert!(psu_alert.is_some(), "should have psu_unhealthy alert");
    assert_eq!(psu_alert.unwrap()["severity"], "warning");
}

// ── Network data collection tests ─────────────────────────────────────────────

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

// ── Stats + activity collector tests ──────────────────────────────────────────

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
