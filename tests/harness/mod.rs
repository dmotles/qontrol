// Test harness for wiremock-based integration tests.
//
// ## How to add new fixtures
// 1. Record the JSON response from a live Qumulo cluster
// 2. Save it to tests/fixtures/<name>.json
// 3. Add a mapping entry in FIXTURE_ROUTES below
//
// ## How to write new tests
// 1. Create a TestServer: `let ts = TestServer::start().await;`
// 2. Mount needed fixtures: `ts.mount_fixture("cluster_settings").await;`
// 3. Run the binary: `ts.command().args(["dashboard"]).assert().success();`
//
// ## How to refresh fixtures
// Run the relevant API calls against a live cluster and save the JSON responses
// to tests/fixtures/, overwriting the existing files.

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Maps fixture name → (HTTP method, API path)
const FIXTURE_ROUTES: &[(&str, &str, &str)] = &[
    ("cluster_settings", "GET", "/v1/cluster/settings"),
    ("version", "GET", "/v1/version"),
    ("cluster_nodes", "GET", "/v1/cluster/nodes/"),
    ("filesystem", "GET", "/v1/file-system"),
    (
        "analytics_activity",
        "GET",
        "/v1/analytics/activity/current",
    ),
    ("capacity_history", "GET", "/v1/analytics/capacity-history/"),
    ("fs_entries_root", "GET", "/v1/files/%2F/entries/"),
    ("fs_entries_home", "GET", "/v1/files/%2Fhome/entries/"),
    ("fs_attributes_root", "GET", "/v1/files/%2F/info/attributes"),
    (
        "fs_attributes_home",
        "GET",
        "/v1/files/%2Fhome/info/attributes",
    ),
    (
        "fs_recursive_aggregates_root",
        "GET",
        "/v1/files/%2F/recursive-aggregates/",
    ),
    ("snapshots_list", "GET", "/v2/snapshots/"),
    ("snapshots_status", "GET", "/v2/snapshots/status/"),
    (
        "snapshots_capacity",
        "GET",
        "/v1/snapshots/capacity-used-per-snapshot/",
    ),
    ("snapshots_policies", "GET", "/v2/snapshots/policies/"),
    ("snapshot_single", "GET", "/v2/snapshots/1"),
    ("snapshot_status_single", "GET", "/v2/snapshots/status/1"),
    (
        "snapshots_total_capacity",
        "GET",
        "/v1/snapshots/total-used-capacity",
    ),
    ("session_login", "POST", "/v1/session/login"),
    ("session_who_am_i", "GET", "/v1/session/who-am-i"),
    ("access_token_create", "POST", "/v1/auth/access-tokens/"),
];

pub struct TestServer {
    pub mock_server: MockServer,
    pub temp_dir: TempDir,
}

impl TestServer {
    /// Start a new wiremock server and prepare a temp config directory.
    pub async fn start() -> Self {
        let mock_server = MockServer::start().await;
        let temp_dir = TempDir::new().expect("failed to create temp dir");

        // Write config.toml directly in temp dir; QONTROL_CONFIG_DIR points here
        let port = mock_server.address().port();
        let config_content = format!(
            r#"default_profile = "test"

[profiles.test]
host = "127.0.0.1"
port = {port}
token = "test-token"
insecure = true
"#
        );
        std::fs::write(temp_dir.path().join("config.toml"), config_content)
            .expect("failed to write config");

        Self {
            mock_server,
            temp_dir,
        }
    }

    /// Mount a fixture by name on the correct API path.
    pub async fn mount_fixture(&self, name: &str) {
        let (_, http_method, api_path) = FIXTURE_ROUTES
            .iter()
            .find(|(n, _, _)| *n == name)
            .unwrap_or_else(|| panic!("unknown fixture: {}", name));

        let fixture_path = fixtures_dir().join(format!("{}.json", name));
        let body = std::fs::read_to_string(&fixture_path)
            .unwrap_or_else(|_| panic!("failed to read fixture: {}", fixture_path.display()));

        Mock::given(method(*http_method))
            .and(path(*api_path))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&self.mock_server)
            .await;
    }

    /// Mount multiple fixtures at once.
    pub async fn mount_fixtures(&self, names: &[&str]) {
        for name in names {
            self.mount_fixture(name).await;
        }
    }

    /// Mount a fixture on a specific API path, only matching when the given query param is absent.
    pub async fn mount_fixture_without_query(
        &self,
        fixture_name: &str,
        http_method: &str,
        api_path: &str,
        absent_param: &str,
    ) {
        let fixture_path = fixtures_dir().join(format!("{}.json", fixture_name));
        let body = std::fs::read_to_string(&fixture_path)
            .unwrap_or_else(|_| panic!("failed to read fixture: {}", fixture_path.display()));

        Mock::given(method(http_method))
            .and(path(api_path))
            .and(query_param_is_missing(absent_param))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&self.mock_server)
            .await;
    }

    /// Mount a fixture on a specific API path with a required query parameter.
    pub async fn mount_fixture_with_query(
        &self,
        fixture_name: &str,
        http_method: &str,
        api_path: &str,
        query_key: &str,
        query_value: &str,
    ) {
        let fixture_path = fixtures_dir().join(format!("{}.json", fixture_name));
        let body = std::fs::read_to_string(&fixture_path)
            .unwrap_or_else(|_| panic!("failed to read fixture: {}", fixture_path.display()));

        Mock::given(method(http_method))
            .and(path(api_path))
            .and(query_param(query_key, query_value))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&self.mock_server)
            .await;
    }

    /// Mount an error response for a given API path.
    pub async fn mount_error(&self, http_method: &str, api_path: &str, status_code: u16) {
        Mock::given(method(http_method))
            .and(path(api_path))
            .respond_with(ResponseTemplate::new(status_code).set_body_raw(
                serde_json::json!({"description": "error", "module": "test"}).to_string(),
                "application/json",
            ))
            .mount(&self.mock_server)
            .await;
    }

    /// Build an assert_cmd Command pre-configured with the test environment.
    #[allow(deprecated)]
    pub fn command(&self) -> Command {
        let mut cmd = Command::cargo_bin("qontrol").expect("binary not found");
        let port = self.mock_server.address().port();
        cmd.env("QONTROL_CONFIG_DIR", self.temp_dir.path())
            .env("QONTROL_BASE_URL", format!("http://127.0.0.1:{}", port));
        cmd
    }
}

/// Multi-profile test harness: each profile gets its own mock server.
pub struct MultiTestServer {
    pub servers: Vec<(String, MockServer)>,
    pub temp_dir: TempDir,
}

impl MultiTestServer {
    /// Start mock servers for each profile name and write a config with all profiles.
    pub async fn start(profile_names: &[&str]) -> Self {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let mut servers = Vec::new();
        let mut config_sections = Vec::new();

        let default = profile_names.first().copied().unwrap_or("default");
        config_sections.push(format!("default_profile = \"{}\"", default));

        for name in profile_names {
            let mock_server = MockServer::start().await;
            let port = mock_server.address().port();
            config_sections.push(format!(
                r#"
[profiles.{name}]
host = "127.0.0.1"
port = {port}
token = "test-token-{name}"
insecure = true
base_url = "http://127.0.0.1:{port}"
"#
            ));
            servers.push((name.to_string(), mock_server));
        }

        std::fs::write(
            temp_dir.path().join("config.toml"),
            config_sections.join("\n"),
        )
        .expect("failed to write config");

        Self { servers, temp_dir }
    }

    /// Mount a fixture on a specific profile's mock server.
    pub async fn mount_fixture(&self, profile: &str, name: &str) {
        let (_, server) = self
            .servers
            .iter()
            .find(|(n, _)| n == profile)
            .unwrap_or_else(|| panic!("unknown profile: {}", profile));

        let (_, http_method, api_path) = FIXTURE_ROUTES
            .iter()
            .find(|(n, _, _)| *n == name)
            .unwrap_or_else(|| panic!("unknown fixture: {}", name));

        let fixture_path = fixtures_dir().join(format!("{}.json", name));
        let body = std::fs::read_to_string(&fixture_path)
            .unwrap_or_else(|_| panic!("failed to read fixture: {}", fixture_path.display()));

        Mock::given(method(*http_method))
            .and(path(*api_path))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(server)
            .await;
    }

    /// Mount a fixture file from tests/fixtures/status/<cluster>/ on the given API path.
    pub async fn mount_status_fixture(
        &self,
        profile: &str,
        cluster_dir: &str,
        fixture_file: &str,
        api_path: &str,
    ) {
        let (_, server) = self
            .servers
            .iter()
            .find(|(n, _)| n == profile)
            .unwrap_or_else(|| panic!("unknown profile: {}", profile));

        let fixture_path = fixtures_dir()
            .join("status")
            .join(cluster_dir)
            .join(format!("{}.json", fixture_file));
        let body = std::fs::read_to_string(&fixture_path)
            .unwrap_or_else(|_| panic!("failed to read fixture: {}", fixture_path.display()));

        Mock::given(method("GET"))
            .and(path(api_path))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(server)
            .await;
    }

    /// Mount standard cluster fixtures on a profile (settings, version, nodes, fs, activity).
    pub async fn mount_cluster_fixtures(&self, profile: &str) {
        for fixture in &[
            "cluster_settings",
            "version",
            "cluster_nodes",
            "filesystem",
            "analytics_activity",
        ] {
            self.mount_fixture(profile, fixture).await;
        }
    }

    /// Mount cluster fixtures from the status fixture directory with capacity history.
    /// Maps: (fixture_file_name, api_path)
    pub async fn mount_cluster_fixtures_with_capacity(&self, profile: &str, cluster_dir: &str) {
        let mappings: &[(&str, &str)] = &[
            ("cluster_settings", "/v1/cluster/settings"),
            ("version", "/v1/version"),
            ("cluster_nodes", "/v1/cluster/nodes/"),
            ("file_system", "/v1/file-system"),
            ("capacity_history", "/v1/analytics/capacity-history/"),
        ];
        for (file, api) in mappings {
            self.mount_status_fixture(profile, cluster_dir, file, api)
                .await;
        }
    }

    /// Build a command that does NOT set QONTROL_BASE_URL (each profile resolves to its own server).
    #[allow(deprecated)]
    pub fn command(&self) -> Command {
        let mut cmd = Command::cargo_bin("qontrol").expect("binary not found");
        cmd.env("QONTROL_CONFIG_DIR", self.temp_dir.path())
            .env("QONTROL_CACHE_DIR", self.temp_dir.path().join("cache"));
        cmd
    }
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}
