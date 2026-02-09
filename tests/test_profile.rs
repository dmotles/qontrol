#![allow(deprecated)]

mod harness;

use assert_cmd::Command;
use predicates::prelude::*;

use harness::TestServer;

#[test]
fn test_profile_add_help() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .args(["profile", "add", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--token"))
        .stdout(predicate::str::contains("--host"))
        .stdout(predicate::str::contains("interactive login"));
}

#[test]
fn test_profile_add_token_requires_host() {
    let temp = tempfile::TempDir::new().unwrap();
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("QONTROL_CONFIG_DIR", temp.path())
        .args(["profile", "add", "test", "--token", "my-token"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--host is required"));
}

#[tokio::test]
async fn test_profile_add_with_token() {
    let ts = TestServer::start().await;

    // Add a new profile using manual --token flow
    ts.command()
        .args([
            "profile",
            "add",
            "manual",
            "--host",
            "10.0.0.1",
            "--token",
            "access-v1:my-manual-token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Profile 'manual' added."));

    // Verify the profile was created by listing profiles
    ts.command()
        .args(["profile", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("manual"));
}

#[tokio::test]
async fn test_profile_add_with_token_sets_default() {
    // Start with empty config
    let temp = tempfile::TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("qontrol").unwrap();
    cmd.env("QONTROL_CONFIG_DIR", temp.path())
        .args([
            "profile",
            "add",
            "first",
            "--host",
            "10.0.0.1",
            "--token",
            "access-v1:tok",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Set as default profile."));
}

#[tokio::test]
async fn test_profile_add_with_token_and_options() {
    let ts = TestServer::start().await;

    ts.command()
        .args([
            "profile",
            "add",
            "secure",
            "--host",
            "cluster.example.com",
            "--port",
            "9000",
            "--token",
            "access-v1:secure-token",
            "--insecure",
            "--default",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Profile 'secure' added."))
        .stdout(predicate::str::contains("Set as default profile."));

    // Verify profile details
    ts.command()
        .args(["profile", "show", "secure"])
        .assert()
        .success()
        .stdout(predicate::str::contains("cluster.example.com:9000"))
        .stdout(predicate::str::contains("Insecure: true"));
}
