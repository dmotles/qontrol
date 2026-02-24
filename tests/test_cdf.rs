mod harness;

use qontrol::commands::cdf::types::{
    ObjectRelationship, ObjectRelationshipStatus, PortalHub, PortalList, PortalSpoke,
    ReplicationSource, ReplicationSourceStatus, ReplicationTargetStatus,
};

/// Test that portal hubs fixture deserializes correctly through the mock server.
#[tokio::test]
async fn test_portal_hubs_fixture_deserializes() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("portal_hubs").await;

    let port = ts.mock_server.address().port();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/v2/portal/hubs/", port))
        .send()
        .await
        .unwrap();
    let body: PortalList<PortalHub> = resp.json().await.unwrap();
    assert_eq!(body.entries.len(), 2);
    assert_eq!(body.entries[0].id, 1);
    assert_eq!(body.entries[0].portal_type, "PORTAL_READ_WRITE");
    assert_eq!(body.entries[0].state, "ACCEPTED");
    assert_eq!(body.entries[0].status, "ACTIVE");
    assert_eq!(body.entries[1].status, "INACTIVE");
}

/// Test that portal spokes fixture deserializes correctly.
#[tokio::test]
async fn test_portal_spokes_fixture_deserializes() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("portal_spokes").await;

    let port = ts.mock_server.address().port();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/v2/portal/spokes/", port))
        .send()
        .await
        .unwrap();
    let body: PortalList<PortalSpoke> = resp.json().await.unwrap();
    assert_eq!(body.entries.len(), 1);
    assert_eq!(body.entries[0].id, 5);
    assert_eq!(body.entries[0].hub_id, Some(1));
    assert_eq!(body.entries[0].roots.len(), 1);
    assert!(body.entries[0].roots[0].authorized);
}

/// Test that replication sources fixture deserializes correctly (bare array response).
#[tokio::test]
async fn test_replication_sources_fixture_deserializes() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("replication_sources").await;

    let port = ts.mock_server.address().port();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/v2/replication/source-relationships/",
            port
        ))
        .send()
        .await
        .unwrap();
    let body: Vec<ReplicationSource> = resp.json().await.unwrap();
    assert_eq!(body.len(), 1);
    assert_eq!(body[0].id, 10);
    assert!(body[0].replication_enabled);
    assert_eq!(
        body[0].replication_mode.as_deref(),
        Some("REPLICATION_CONTINUOUS")
    );
}

/// Test that replication source statuses fixture deserializes correctly.
#[tokio::test]
async fn test_replication_source_statuses_fixture_deserializes() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("replication_source_statuses").await;

    let port = ts.mock_server.address().port();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/v2/replication/source-relationships/status/",
            port
        ))
        .send()
        .await
        .unwrap();
    let body: Vec<ReplicationSourceStatus> = resp.json().await.unwrap();
    assert_eq!(body.len(), 1);
    assert_eq!(body[0].state.as_deref(), Some("ESTABLISHED"));
    assert_eq!(body[0].job_state.as_deref(), Some("REPLICATION_RUNNING"));
    let job = body[0].replication_job_status.as_ref().unwrap();
    assert_eq!(job.percent_complete.as_deref(), Some("85.2"));
}

/// Test that replication target statuses fixture deserializes correctly.
#[tokio::test]
async fn test_replication_target_statuses_fixture_deserializes() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("replication_target_statuses").await;

    let port = ts.mock_server.address().port();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/v2/replication/target-relationships/status/",
            port
        ))
        .send()
        .await
        .unwrap();
    let body: Vec<ReplicationTargetStatus> = resp.json().await.unwrap();
    assert_eq!(body.len(), 1);
    assert_eq!(body[0].id, 20);
    assert_eq!(body[0].state.as_deref(), Some("ESTABLISHED"));
    assert_eq!(body[0].source_address.as_deref(), Some("10.220.0.10"));
}

/// Test that object relationships fixture deserializes correctly.
#[tokio::test]
async fn test_object_relationships_fixture_deserializes() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("object_relationships").await;

    let port = ts.mock_server.address().port();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/v3/replication/object-relationships/",
            port
        ))
        .send()
        .await
        .unwrap();
    let body: Vec<ObjectRelationship> = resp.json().await.unwrap();
    assert_eq!(body.len(), 2);
    assert_eq!(body[0].direction.as_deref(), Some("COPY_TO_OBJECT"));
    assert_eq!(body[0].bucket.as_deref(), Some("qumulo-backup-prod"));
    assert_eq!(body[1].direction.as_deref(), Some("COPY_FROM_OBJECT"));
}

/// Test that object relationship statuses fixture deserializes correctly.
#[tokio::test]
async fn test_object_relationship_statuses_fixture_deserializes() {
    let ts = harness::TestServer::start().await;
    ts.mount_fixture("object_relationship_statuses").await;

    let port = ts.mock_server.address().port();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/v3/replication/object-relationships/status/",
            port
        ))
        .send()
        .await
        .unwrap();
    let body: Vec<ObjectRelationshipStatus> = resp.json().await.unwrap();
    assert_eq!(body.len(), 1);
    assert_eq!(body[0].state.as_deref(), Some("ESTABLISHED"));
    assert_eq!(body[0].bucket.as_deref(), Some("qumulo-backup-prod"));
}
