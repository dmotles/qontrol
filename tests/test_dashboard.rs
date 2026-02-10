mod harness;

use predicates::prelude::*;

/// Test: `qontrol dashboard` works as an alias for `qontrol status`.
#[tokio::test]
async fn test_dashboard_alias_produces_status_output() {
    let mts = harness::MultiTestServer::start(&["test_cluster"]).await;
    mts.mount_cluster_fixtures("test_cluster").await;

    mts.command()
        .arg("dashboard")
        .assert()
        .success()
        .stdout(predicate::str::contains("Environment Overview"));
}

/// Test: `qontrol dashboard --json` works as an alias for `qontrol status --json`.
#[tokio::test]
async fn test_dashboard_alias_json_output() {
    let mts = harness::MultiTestServer::start(&["test_cluster"]).await;
    mts.mount_cluster_fixtures("test_cluster").await;

    let output = mts
        .command()
        .args(["dashboard", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert!(json.get("timestamp").is_some());
    assert!(json.get("aggregates").is_some());
    assert!(json.get("clusters").is_some());
    assert!(json.get("alerts").is_some());
}

/// Test: `qontrol dashboard --watch` works as an alias for `qontrol status --watch`.
#[tokio::test]
async fn test_dashboard_alias_watch_mode() {
    let mts = harness::MultiTestServer::start(&["test_cluster"]).await;
    mts.mount_cluster_fixtures("test_cluster").await;

    let output = mts
        .command()
        .args(["dashboard", "--watch", "--interval", "1"])
        .timeout(std::time::Duration::from_secs(3))
        .output()
        .expect("failed to execute");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Refreshing every 1s"),
        "dashboard alias should support --watch"
    );
}
