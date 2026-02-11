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
    ts.mount_fixture("node_state").await;

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

    // Verify the cluster UUID was stored
    ts.command()
        .args(["profile", "show", "manual", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        ));
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

// --- Interactive login flow tests (non-interactive via --username/--password) ---

#[tokio::test]
async fn test_profile_add_interactive_login() {
    let ts = TestServer::start().await;
    ts.mount_fixtures(&[
        "session_login",
        "session_who_am_i",
        "access_token_create",
        "node_state",
    ])
    .await;

    let port = ts.mock_server.address().port().to_string();
    ts.command()
        .args([
            "profile",
            "add",
            "testcluster",
            "--host",
            "127.0.0.1",
            "--port",
            &port,
            "--insecure",
            "--username",
            "admin",
            "--password",
            "testpass",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Logged in as admin"))
        .stdout(predicate::str::contains("Profile 'testcluster' saved"));

    // Verify the profile was created with the access token from the fixture
    // Token is redacted in show output — last 8 chars of "access-v1:test-long-lived-token" are visible
    ts.command()
        .args(["profile", "show", "testcluster", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ed-token"))
        .stdout(predicate::str::contains(
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        ));
}

#[tokio::test]
async fn test_profile_add_login_bad_password() {
    let ts = TestServer::start().await;
    ts.mount_error("POST", "/v1/session/login", 401).await;

    let port = ts.mock_server.address().port().to_string();
    ts.command()
        .args([
            "profile",
            "add",
            "bad",
            "--host",
            "127.0.0.1",
            "--port",
            &port,
            "--insecure",
            "--username",
            "admin",
            "--password",
            "wrong",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid username or password"));
}

#[tokio::test]
async fn test_profile_add_login_connection_refused() {
    let temp = tempfile::TempDir::new().unwrap();

    // Use a port with nothing listening (port 1 is almost never open)
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("QONTROL_CONFIG_DIR", temp.path())
        .env("QONTROL_BASE_URL", "http://127.0.0.1:1")
        .args([
            "profile",
            "add",
            "bad",
            "--host",
            "127.0.0.1",
            "--port",
            "1",
            "--insecure",
            "--username",
            "admin",
            "--password",
            "test",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Could not connect"));
}

#[tokio::test]
async fn test_profile_add_login_no_permission() {
    let ts = TestServer::start().await;
    ts.mount_fixtures(&["session_login", "session_who_am_i"])
        .await;
    ts.mount_error("POST", "/v1/auth/access-tokens/", 403).await;

    let port = ts.mock_server.address().port().to_string();
    ts.command()
        .args([
            "profile",
            "add",
            "noperm",
            "--host",
            "127.0.0.1",
            "--port",
            &port,
            "--insecure",
            "--username",
            "admin",
            "--password",
            "testpass",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("permission"));
}

#[tokio::test]
async fn test_profile_add_login_expiry_never() {
    let ts = TestServer::start().await;
    ts.mount_fixtures(&["session_login", "session_who_am_i", "access_token_create"])
        .await;

    let port = ts.mock_server.address().port().to_string();
    ts.command()
        .args([
            "profile",
            "add",
            "neverexpire",
            "--host",
            "127.0.0.1",
            "--port",
            &port,
            "--insecure",
            "--username",
            "admin",
            "--password",
            "testpass",
            "--expiry",
            "never",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("never expires"));
}

#[tokio::test]
async fn test_profile_add_login_expiry_6months() {
    let ts = TestServer::start().await;
    ts.mount_fixtures(&["session_login", "session_who_am_i", "access_token_create"])
        .await;

    let port = ts.mock_server.address().port().to_string();
    ts.command()
        .args([
            "profile",
            "add",
            "sixmonths",
            "--host",
            "127.0.0.1",
            "--port",
            &port,
            "--insecure",
            "--username",
            "admin",
            "--password",
            "testpass",
            "--expiry",
            "6months",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("expires"));
}

#[test]
fn test_profile_add_login_requires_host() {
    let temp = tempfile::TempDir::new().unwrap();
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("QONTROL_CONFIG_DIR", temp.path())
        .args([
            "profile",
            "add",
            "test",
            "--username",
            "admin",
            "--password",
            "pass",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--host is required"));
}
