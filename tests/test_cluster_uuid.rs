mod harness;

/// Test: ensure_cluster_uuids backfills UUID from /v1/node/state and persists to config.toml.
#[tokio::test]
async fn test_backfill_writes_uuid_to_config() {
    let mts = harness::MultiTestServer::start(&["alpha"]).await;
    mts.mount_cluster_fixtures("alpha").await;
    mts.mount_fixture("alpha", "node_state").await;

    let output = mts
        .command()
        .args(["fleet", "status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());

    // Verify UUID was persisted to config.toml on disk
    let config_path = mts.temp_dir.path().join("config.toml");
    let config_str = std::fs::read_to_string(&config_path).expect("read config");
    assert!(
        config_str.contains("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
        "config should contain the UUID from node_state fixture after backfill"
    );

    // Verify UUID also appears in JSON output
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON");
    let clusters = json["clusters"].as_array().expect("clusters array");
    assert_eq!(
        clusters[0]["cluster_uuid"], "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    );
}

/// Test: unreachable cluster is skipped during backfill — no UUID written, no crash.
#[tokio::test]
async fn test_backfill_skips_unreachable_cluster() {
    let mts = harness::MultiTestServer::start(&["reachable", "unreachable"]).await;
    mts.mount_cluster_fixtures("reachable").await;
    mts.mount_fixture("reachable", "node_state").await;
    // "unreachable" has no mocks — all endpoints return 404

    let output = mts
        .command()
        .args(["fleet", "status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());

    // Config should have UUID for reachable but not unreachable
    let config_path = mts.temp_dir.path().join("config.toml");
    let config: toml::Value =
        toml::from_str(&std::fs::read_to_string(&config_path).expect("read config"))
            .expect("parse config");

    let reachable = &config["profiles"]["reachable"];
    assert_eq!(
        reachable.get("cluster_uuid").and_then(|v| v.as_str()),
        Some("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
    );

    let unreachable_profile = &config["profiles"]["unreachable"];
    assert!(
        unreachable_profile.get("cluster_uuid").is_none(),
        "unreachable cluster should not have UUID backfilled"
    );
}

/// Test: profile with UUID already set is not re-fetched during backfill.
/// Verifies that node_state is never called (no mock mounted) and the stored UUID is used.
#[tokio::test]
async fn test_backfill_skips_profile_with_existing_uuid() {
    let mts = harness::MultiTestServer::start(&["alpha"]).await;

    // Overwrite config with pre-set cluster_uuid
    let port = mts.servers[0].1.address().port();
    let config_content = format!(
        r#"default_profile = "alpha"

[profiles.alpha]
host = "127.0.0.1"
port = {port}
token = "test-token-alpha"
insecure = true
base_url = "http://127.0.0.1:{port}"
cluster_uuid = "pre-existing-uuid-1234"
"#
    );
    std::fs::write(mts.temp_dir.path().join("config.toml"), &config_content)
        .expect("write config");

    // Mount cluster fixtures but NOT node_state — if backfill tried to fetch, it'd get 404
    mts.mount_cluster_fixtures("alpha").await;

    let output = mts
        .command()
        .args(["fleet", "status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());

    // UUID in JSON should be the pre-existing one (not from node_state)
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON");
    let clusters = json["clusters"].as_array().expect("clusters array");
    assert_eq!(clusters[0]["cluster_uuid"], "pre-existing-uuid-1234");

    // Config on disk should still have the original UUID unchanged
    let config_str =
        std::fs::read_to_string(mts.temp_dir.path().join("config.toml")).expect("read config");
    assert!(config_str.contains("pre-existing-uuid-1234"));
}

/// Test: collector UUID falls back to empty string when node_state endpoint is unavailable.
#[tokio::test]
async fn test_collector_uuid_empty_when_node_state_unavailable() {
    let mts = harness::MultiTestServer::start(&["alpha"]).await;
    mts.mount_cluster_fixtures("alpha").await;
    // Don't mount node_state — backfill fails, collector fallback also fails

    let output = mts
        .command()
        .args(["fleet", "status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON");
    let clusters = json["clusters"].as_array().expect("clusters array");
    assert_eq!(
        clusters[0]["cluster_uuid"], "",
        "UUID should be empty when node_state is unavailable"
    );
}

/// Test: node_state returns unexpected JSON shape (missing cluster_id) → empty UUID, no crash.
#[tokio::test]
async fn test_uuid_empty_when_node_state_missing_cluster_id() {
    let mts = harness::MultiTestServer::start(&["alpha"]).await;
    mts.mount_cluster_fixtures("alpha").await;
    // Mount node_state with valid JSON but no cluster_id field
    mts.mount_raw(
        "alpha",
        "node_state",
        r#"{"node_id": 1, "state": "ACTIVE"}"#,
    )
    .await;

    let output = mts
        .command()
        .args(["fleet", "status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON");
    let clusters = json["clusters"].as_array().expect("clusters array");
    assert_eq!(
        clusters[0]["cluster_uuid"], "",
        "UUID should be empty when node_state response lacks cluster_id"
    );

    // Config should NOT have cluster_uuid (nothing was backfilled)
    let config_str =
        std::fs::read_to_string(mts.temp_dir.path().join("config.toml")).expect("read config");
    assert!(
        !config_str.contains("cluster_uuid"),
        "config should not have cluster_uuid when backfill had no cluster_id to extract"
    );
}

/// Test: node_state returns 500 error — both backfill and collector degrade gracefully.
#[tokio::test]
async fn test_uuid_graceful_on_node_state_server_error() {
    let mts = harness::MultiTestServer::start(&["alpha"]).await;
    mts.mount_cluster_fixtures("alpha").await;
    mts.mount_error("alpha", "GET", "/v1/node/state", 500).await;

    let output = mts
        .command()
        .args(["fleet", "status", "--json", "--no-cache"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON");
    let clusters = json["clusters"].as_array().expect("clusters array");
    assert_eq!(
        clusters[0]["cluster_uuid"], "",
        "UUID should be empty when node_state returns 500"
    );
}
