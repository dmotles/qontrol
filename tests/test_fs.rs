mod harness;

use predicates::prelude::*;

#[tokio::test]
async fn test_fs_ls_root() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("fs_entries_root").await;

    ts.command()
        .args(["fs", "ls", "/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("home"));
}

#[tokio::test]
async fn test_fs_ls_pagination() {
    let ts = harness::TestServer::start().await;

    // Page 1: only when 'after' param is absent - returns home + etc, paging.next = "etc"
    ts.mount_fixture_without_query(
        "fs_entries_root_page1",
        "GET",
        "/v1/files/%2F/entries/",
        "after",
    )
    .await;
    // Page 2: after=etc - returns var, paging.next = ""
    ts.mount_fixture_with_query(
        "fs_entries_root_page2",
        "GET",
        "/v1/files/%2F/entries/",
        "after",
        "etc",
    )
    .await;

    ts.command()
        .args(["fs", "ls", "/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("etc"))
        .stdout(predicate::str::contains("home"))
        .stdout(predicate::str::contains("var"));
}

#[tokio::test]
async fn test_fs_ls_pagination_json() {
    let ts = harness::TestServer::start().await;

    ts.mount_fixture_without_query(
        "fs_entries_root_page1",
        "GET",
        "/v1/files/%2F/entries/",
        "after",
    )
    .await;
    ts.mount_fixture_with_query(
        "fs_entries_root_page2",
        "GET",
        "/v1/files/%2F/entries/",
        "after",
        "etc",
    )
    .await;

    let output = ts
        .command()
        .args(["fs", "ls", "/", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    let files = json["files"].as_array().expect("files should be array");
    assert_eq!(files.len(), 3, "should have all 3 entries from both pages");

    let names: Vec<&str> = files
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"home"));
    assert!(names.contains(&"etc"));
    assert!(names.contains(&"var"));
}

#[tokio::test]
async fn test_fs_ls_limit() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("fs_entries_root").await;

    ts.command()
        .args(["fs", "ls", "/", "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("home"));
}

#[tokio::test]
async fn test_fs_ls_long() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("fs_entries_root").await;

    ts.command()
        .args(["fs", "ls", "/", "--long"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ID"))
        .stdout(predicate::str::contains("TYPE"))
        .stdout(predicate::str::contains("SIZE"))
        .stdout(predicate::str::contains("NAME"))
        .stdout(predicate::str::contains("home"))
        .stdout(predicate::str::contains("DIR"));
}

#[tokio::test]
async fn test_fs_ls_json() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("fs_entries_root").await;

    let output = ts
        .command()
        .args(["fs", "ls", "/", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert!(json.get("files").is_some());
    let files = json["files"].as_array().expect("files should be array");
    assert!(!files.is_empty());
    assert_eq!(files[0]["name"], "home");
}

#[tokio::test]
async fn test_fs_stat() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("fs_attributes_root").await;

    ts.command()
        .args(["fs", "stat", "/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("File: /"))
        .stdout(predicate::str::contains("Type"))
        .stdout(predicate::str::contains("Size"))
        .stdout(predicate::str::contains("Owner"))
        .stdout(predicate::str::contains("0777"));
}

#[tokio::test]
async fn test_fs_stat_json() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("fs_attributes_root").await;

    let output = ts
        .command()
        .args(["fs", "stat", "/", "--json"])
        .output()
        .expect("failed to execute");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("invalid JSON output");

    assert!(json.get("id").is_some());
    assert!(json.get("type").is_some());
    assert!(json.get("size").is_some());
    assert!(json.get("mode").is_some());
    assert_eq!(json["type"], "FS_FILE_TYPE_DIRECTORY");
}

#[tokio::test]
async fn test_fs_tree() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixtures(&[
        "fs_entries_root",
        "fs_entries_home",
        "fs_recursive_aggregates_root",
    ])
    .await;

    ts.command()
        .args(["fs", "tree", "/", "--max-depth", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("home"));
}

#[tokio::test]
async fn test_fs_tree_multi_page() {
    let ts = harness::TestServer::start().await;

    // Page 1: only when 'after' param is absent - returns home + etc, paging.next = "etc"
    ts.mount_fixture_without_query(
        "fs_entries_root_page1",
        "GET",
        "/v1/files/%2F/entries/",
        "after",
    )
    .await;
    // Page 2: after=etc - returns var, paging.next = ""
    ts.mount_fixture_with_query(
        "fs_entries_root_page2",
        "GET",
        "/v1/files/%2F/entries/",
        "after",
        "etc",
    )
    .await;
    ts.mount_fixture("fs_recursive_aggregates_root").await;

    ts.command()
        .args(["fs", "tree", "/", "--max-depth", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("home"))
        .stdout(predicate::str::contains("etc"))
        .stdout(predicate::str::contains("var"));
}
