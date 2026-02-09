mod harness;

use predicates::prelude::*;

#[tokio::test]
async fn test_dashboard_human_output() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixtures(&[
        "cluster_settings",
        "version",
        "cluster_nodes",
        "filesystem",
        "analytics_activity",
    ])
    .await;

    ts.command()
        .arg("dashboard")
        .assert()
        .success()
        .stdout(predicate::str::contains("Cluster:"))
        .stdout(predicate::str::contains("dmotlesai-fs"))
        .stdout(predicate::str::contains("Qumulo Core 7.7.2"))
        .stdout(predicate::str::contains("dmotlesai-fs-1"))
        .stdout(predicate::str::contains("Nodes:"))
        .stdout(predicate::str::contains("Capacity:"));
}

#[tokio::test]
async fn test_dashboard_json_output() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixtures(&[
        "cluster_settings",
        "version",
        "cluster_nodes",
        "filesystem",
        "analytics_activity",
    ])
    .await;

    let output = ts
        .command()
        .args(["dashboard", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert!(json.get("cluster").is_some());
    assert!(json.get("version").is_some());
    assert!(json.get("nodes").is_some());
    assert!(json.get("file_system").is_some());
    assert!(json.get("activity").is_some());

    assert_eq!(json["cluster"]["cluster_name"], "dmotlesai-fs");
    assert_eq!(json["version"]["revision_id"], "Qumulo Core 7.7.2");
}
