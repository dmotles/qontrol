mod harness;

use predicates::prelude::*;

#[tokio::test]
async fn test_snapshot_list() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixtures(&["snapshots_list", "snapshots_capacity"])
        .await;

    ts.command()
        .args(["snapshot", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Test"));
}

#[tokio::test]
async fn test_snapshot_list_json() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixtures(&["snapshots_list", "snapshots_capacity"])
        .await;

    let output = ts
        .command()
        .args(["snapshot", "list", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert!(json.get("entries").is_some());
    let entries = json["entries"].as_array().expect("entries should be array");
    assert!(!entries.is_empty());
    assert_eq!(entries[0]["name"], "Test");
}

#[tokio::test]
async fn test_snapshot_show() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("snapshot_single").await;

    ts.command()
        .args(["snapshot", "show", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Snapshot 1"))
        .stdout(predicate::str::contains("Test"))
        .stdout(predicate::str::contains("Name:"));
}

#[tokio::test]
async fn test_snapshot_show_json() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("snapshot_single").await;

    let output = ts
        .command()
        .args(["snapshot", "show", "1", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert_eq!(json["id"], 1);
    assert_eq!(json["name"], "Test");
}

#[tokio::test]
async fn test_snapshot_policies() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("snapshots_policies").await;

    ts.command()
        .args(["snapshot", "policies"])
        .assert()
        .success()
        .stdout(predicate::str::contains("daily-root"));
}

#[tokio::test]
async fn test_snapshot_policies_json() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("snapshots_policies").await;

    let output = ts
        .command()
        .args(["snapshot", "policies", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert!(json.get("entries").is_some());
    let entries = json["entries"].as_array().expect("entries should be array");
    assert!(!entries.is_empty());
    assert_eq!(entries[0]["policy_name"], "daily-root");
}
