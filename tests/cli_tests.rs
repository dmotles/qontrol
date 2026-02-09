#![allow(deprecated)]

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_help_contains_description() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Qumulo Data Fabric CLI"));
}

#[test]
fn test_version() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("qontrol"));
}

#[test]
fn test_profile_list_no_config() {
    // Use a temp dir to avoid reading the real config
    let temp = std::env::temp_dir().join("qontrol-test-empty");
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("HOME", &temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .args(["profile", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No profiles configured"));
}

#[test]
fn test_api_raw_without_profile_shows_error() {
    let temp = std::env::temp_dir().join("qontrol-test-no-profile");
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("HOME", &temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .args(["api", "raw", "GET", "/v1/version"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no default profile configured"));
}

#[test]
fn test_profile_subcommands_help() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .args(["profile", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("add"))
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("remove"))
        .stdout(predicate::str::contains("show"));
}

#[test]
fn test_dashboard_help() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .args(["dashboard", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Cluster health dashboard"))
        .stdout(predicate::str::contains("--watch"))
        .stdout(predicate::str::contains("--interval"))
        .stdout(predicate::str::contains("--json"));
}

#[test]
fn test_dashboard_without_profile_shows_error() {
    let temp = std::env::temp_dir().join("qontrol-test-dashboard-no-profile");
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("HOME", &temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .args(["dashboard"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no default profile configured"));
}

#[test]
fn test_help_lists_dashboard() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("dashboard"));
}

#[test]
fn test_snapshot_subcommands_help() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .args(["snapshot", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("show"))
        .stdout(predicate::str::contains("policies"))
        .stdout(predicate::str::contains("recommend-delete"))
        .stdout(predicate::str::contains("diff"));
}

#[test]
fn test_snapshot_list_without_profile_shows_error() {
    let temp = std::env::temp_dir().join("qontrol-test-snap-no-profile");
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("HOME", &temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .args(["snapshot", "list"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no default profile configured"));
}

#[test]
fn test_help_shows_snapshot_command() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("snapshot"));
}

#[test]
fn test_fs_subcommands_help() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .args(["fs", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ls"))
        .stdout(predicate::str::contains("tree"))
        .stdout(predicate::str::contains("stat"));
}

#[test]
fn test_fs_ls_without_profile_shows_error() {
    let temp = std::env::temp_dir().join("qontrol-test-fs-no-profile");
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("HOME", &temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .args(["fs", "ls", "/"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no default profile configured"));
}

#[test]
fn test_fs_stat_without_profile_shows_error() {
    let temp = std::env::temp_dir().join("qontrol-test-fs-stat-no-profile");
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("HOME", &temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .args(["fs", "stat", "/"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no default profile configured"));
}

#[test]
fn test_fs_tree_without_profile_shows_error() {
    let temp = std::env::temp_dir().join("qontrol-test-fs-tree-no-profile");
    Command::cargo_bin("qontrol")
        .unwrap()
        .env("HOME", &temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .args(["fs", "tree", "/"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no default profile configured"));
}

#[test]
fn test_help_shows_fs_command() {
    Command::cargo_bin("qontrol")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("fs"));
}
