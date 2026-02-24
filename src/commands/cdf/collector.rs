use std::collections::HashMap;

use anyhow::Result;
use petgraph::graph::NodeIndex;

use crate::client::QumuloClient;
use crate::config::{Config, ProfileEntry};

use super::types::*;

/// Per-cluster raw CDF data collected from the API.
#[derive(Debug, Clone)]
pub struct ClusterCdfData {
    pub profile: String,
    pub cluster_name: String,
    pub cluster_uuid: String,
    pub address: String,
    pub portal_hubs: Vec<PortalHub>,
    pub portal_spokes: Vec<PortalSpoke>,
    pub replication_sources: Vec<ReplicationSource>,
    pub replication_source_statuses: Vec<ReplicationSourceStatus>,
    pub replication_target_statuses: Vec<ReplicationTargetStatus>,
    pub object_relationships: Vec<ObjectRelationship>,
    pub object_relationship_statuses: Vec<ObjectRelationshipStatus>,
}

/// Errors from a single cluster collection (non-fatal for the overall operation).
#[derive(Debug)]
pub struct ClusterCdfError {
    pub profile: String,
    pub error: String,
}

/// Result of collecting CDF data from all clusters.
pub struct CdfCollectionResult {
    pub graph: CdfGraph,
    pub errors: Vec<ClusterCdfError>,
}

/// Collect CDF data from all configured clusters and build a deduplicated graph.
pub fn collect_all(
    config: &Config,
    profile_filters: &[String],
    timeout_secs: u64,
    cluster_filter: Option<&str>,
) -> Result<CdfCollectionResult> {
    let profiles: Vec<(String, ProfileEntry)> = if profile_filters.is_empty() {
        config
            .profiles
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    } else {
        profile_filters
            .iter()
            .filter_map(|name| {
                config
                    .profiles
                    .get(name)
                    .map(|entry| (name.clone(), entry.clone()))
            })
            .collect()
    };

    if profiles.is_empty() {
        anyhow::bail!("no matching profiles found — add profiles with `qontrol profile add`");
    }

    // Collect from all clusters in parallel
    let results: Vec<Result<ClusterCdfData, ClusterCdfError>> = std::thread::scope(|s| {
        let handles: Vec<_> = profiles
            .iter()
            .map(|(name, entry)| {
                let name = name.clone();
                let entry = entry.clone();
                s.spawn(move || collect_cluster(&name, &entry, timeout_secs))
            })
            .collect();

        handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| {
                    Err(ClusterCdfError {
                        profile: "unknown".to_string(),
                        error: "thread panicked".to_string(),
                    })
                })
            })
            .collect()
    });

    let mut cluster_data = Vec::new();
    let mut errors = Vec::new();

    for result in results {
        match result {
            Ok(data) => cluster_data.push(data),
            Err(e) => {
                tracing::warn!(profile = %e.profile, error = %e.error, "CDF collection failed");
                errors.push(e);
            }
        }
    }

    let graph = build_cdf_graph(&cluster_data, cluster_filter);

    Ok(CdfCollectionResult { graph, errors })
}

/// Collect CDF data from a single cluster.
fn collect_cluster(
    profile: &str,
    entry: &ProfileEntry,
    timeout_secs: u64,
) -> Result<ClusterCdfData, ClusterCdfError> {
    let client = QumuloClient::new(entry, timeout_secs, None).map_err(|e| ClusterCdfError {
        profile: profile.to_string(),
        error: format!("failed to create client: {}", e),
    })?;

    // Get cluster identity
    let cluster_name = client
        .get_cluster_settings()
        .ok()
        .and_then(|s| s["cluster_name"].as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| profile.to_string());

    let cluster_uuid = entry.cluster_uuid.clone().unwrap_or_default();
    let address = entry.host.clone();

    // Query all 7 CDF endpoints — each individually wrapped for error isolation
    let portal_hubs = fetch_portal_hubs(&client);
    let portal_spokes = fetch_portal_spokes(&client);
    let replication_sources = fetch_replication_sources(&client);
    let replication_source_statuses = fetch_replication_source_statuses(&client);
    let replication_target_statuses = fetch_replication_target_statuses(&client);
    let object_relationships = fetch_object_relationships(&client);
    let object_relationship_statuses = fetch_object_relationship_statuses(&client);

    Ok(ClusterCdfData {
        profile: profile.to_string(),
        cluster_name,
        cluster_uuid,
        address,
        portal_hubs,
        portal_spokes,
        replication_sources,
        replication_source_statuses,
        replication_target_statuses,
        object_relationships,
        object_relationship_statuses,
    })
}

fn fetch_portal_hubs(client: &QumuloClient) -> Vec<PortalHub> {
    match client.get_portal_hubs() {
        Ok(v) => {
            let list: Result<PortalList<PortalHub>, _> = serde_json::from_value(v);
            match list {
                Ok(l) => l.entries,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to parse portal hubs");
                    Vec::new()
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch portal hubs");
            Vec::new()
        }
    }
}

fn fetch_portal_spokes(client: &QumuloClient) -> Vec<PortalSpoke> {
    match client.get_portal_spokes() {
        Ok(v) => {
            let list: Result<PortalList<PortalSpoke>, _> = serde_json::from_value(v);
            match list {
                Ok(l) => l.entries,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to parse portal spokes");
                    Vec::new()
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch portal spokes");
            Vec::new()
        }
    }
}

fn fetch_replication_sources(client: &QumuloClient) -> Vec<ReplicationSource> {
    match client.get_replication_sources() {
        Ok(v) => serde_json::from_value(v).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to parse replication sources");
            Vec::new()
        }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch replication sources");
            Vec::new()
        }
    }
}

fn fetch_replication_source_statuses(client: &QumuloClient) -> Vec<ReplicationSourceStatus> {
    match client.get_replication_source_statuses() {
        Ok(v) => serde_json::from_value(v).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to parse replication source statuses");
            Vec::new()
        }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch replication source statuses");
            Vec::new()
        }
    }
}

fn fetch_replication_target_statuses(client: &QumuloClient) -> Vec<ReplicationTargetStatus> {
    match client.get_replication_target_statuses() {
        Ok(v) => serde_json::from_value(v).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to parse replication target statuses");
            Vec::new()
        }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch replication target statuses");
            Vec::new()
        }
    }
}

fn fetch_object_relationships(client: &QumuloClient) -> Vec<ObjectRelationship> {
    match client.get_object_relationships() {
        Ok(v) => serde_json::from_value(v).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to parse object relationships");
            Vec::new()
        }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch object relationships");
            Vec::new()
        }
    }
}

fn fetch_object_relationship_statuses(client: &QumuloClient) -> Vec<ObjectRelationshipStatus> {
    match client.get_object_relationship_statuses() {
        Ok(v) => serde_json::from_value(v).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to parse object relationship statuses");
            Vec::new()
        }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch object relationship statuses");
            Vec::new()
        }
    }
}

// ─── Graph Construction & Deduplication ──────────────────────────────────────

/// Build a CdfGraph from collected cluster data with edge deduplication.
///
/// Deduplication rules:
/// - Portal: hub cluster reports spoke ↔ spoke cluster reports hub → single edge
/// - Replication: source cluster reports target_address matching another cluster → deduplicate
///   with that cluster's target status
/// - Object replication: creates S3Bucket nodes (not inter-cluster, no dedup needed)
pub fn build_cdf_graph(clusters: &[ClusterCdfData], cluster_filter: Option<&str>) -> CdfGraph {
    let mut graph = CdfGraph::new();

    // Map cluster addresses and UUIDs to node indices
    let mut address_to_node: HashMap<String, NodeIndex> = HashMap::new();
    let mut uuid_to_node: HashMap<String, NodeIndex> = HashMap::new();
    let mut profile_to_node: HashMap<String, NodeIndex> = HashMap::new();

    // Step 1: Add all profiled clusters as nodes
    for cluster in clusters {
        let node = graph.add_node(CdfNode::ProfiledCluster {
            name: cluster.cluster_name.clone(),
            uuid: cluster.cluster_uuid.clone(),
            address: cluster.address.clone(),
        });

        address_to_node.insert(cluster.address.clone(), node);
        if !cluster.cluster_uuid.is_empty() {
            uuid_to_node.insert(cluster.cluster_uuid.clone(), node);
        }
        profile_to_node.insert(cluster.profile.clone(), node);
    }

    // Track added edges for deduplication
    // Key: canonical edge identifier, Value: true if already added
    let mut portal_edges: HashMap<PortalEdgeKey, bool> = HashMap::new();
    let mut replication_edges: HashMap<ReplicationEdgeKey, bool> = HashMap::new();

    // Step 2: Process each cluster's CDF data
    for cluster in clusters {
        let source_node = profile_to_node[&cluster.profile];

        // Process portal hubs (this cluster is the hub)
        for hub in &cluster.portal_hubs {
            let target_node = resolve_or_create_cluster_node(
                &mut graph,
                &mut address_to_node,
                &mut uuid_to_node,
                hub.spoke_hosts.first().map(|h| h.address.as_str()),
                hub.spoke_cluster_uuid.as_deref(),
            );

            if let Some(target) = target_node {
                let key = PortalEdgeKey::new(source_node, target, hub.id);
                if !portal_edges.contains_key(&key) {
                    // Find matching spoke from the other side (if that cluster is profiled)
                    let spoke_id = find_matching_spoke(clusters, cluster, hub);
                    graph.add_edge(
                        source_node,
                        target,
                        CdfEdge::Portal {
                            hub_id: hub.id,
                            spoke_id: spoke_id.unwrap_or(0),
                            portal_type: hub.portal_type.clone(),
                            state: hub.state.clone(),
                            status: hub.status.clone(),
                        },
                    );
                    portal_edges.insert(key, true);
                    // Also mark the reverse key to prevent the spoke side from adding a duplicate
                    if let Some(sid) = spoke_id {
                        let reverse_key = PortalEdgeKey::new(target, source_node, sid);
                        portal_edges.insert(reverse_key, true);
                    }
                }
            }
        }

        // Process portal spokes (this cluster is the spoke)
        for spoke in &cluster.portal_spokes {
            let target_node = resolve_or_create_cluster_node(
                &mut graph,
                &mut address_to_node,
                &mut uuid_to_node,
                spoke.hub_hosts.first().map(|h| h.address.as_str()),
                spoke.hub_cluster_uuid.as_deref(),
            );

            if let Some(hub_node) = target_node {
                // Check if this edge was already added from the hub side
                let key = PortalEdgeKey::new(source_node, hub_node, spoke.id);
                if !portal_edges.contains_key(&key) {
                    // Hub side didn't add this; try finding the hub's ID
                    let hub_id = find_matching_hub(clusters, cluster, spoke);
                    // Edge direction: hub → spoke
                    graph.add_edge(
                        hub_node,
                        source_node,
                        CdfEdge::Portal {
                            hub_id: hub_id.unwrap_or(0),
                            spoke_id: spoke.id,
                            portal_type: spoke.portal_type.clone(),
                            state: spoke.state.clone(),
                            status: spoke.status.clone(),
                        },
                    );
                    portal_edges.insert(key, true);
                    if let Some(hid) = hub_id {
                        let reverse_key = PortalEdgeKey::new(hub_node, source_node, hid);
                        portal_edges.insert(reverse_key, true);
                    }
                }
            }
        }

        // Process replication source statuses (outbound from this cluster)
        for status in &cluster.replication_source_statuses {
            let target_node = resolve_or_create_cluster_node(
                &mut graph,
                &mut address_to_node,
                &mut uuid_to_node,
                status.target_address.as_deref(),
                status.target_cluster_uuid.as_deref(),
            );

            if let Some(target) = target_node {
                let key = ReplicationEdgeKey::new(
                    status.source_root_path.as_deref(),
                    status.target_root_path.as_deref(),
                    source_node,
                    target,
                );
                if !replication_edges.contains_key(&key) {
                    graph.add_edge(
                        source_node,
                        target,
                        CdfEdge::Replication {
                            source_path: status.source_root_path.clone(),
                            target_path: status.target_root_path.clone(),
                            mode: status.replication_mode.clone(),
                            enabled: status.replication_enabled,
                            state: status.state.clone(),
                            job_state: status.job_state.clone(),
                            recovery_point: status.recovery_point.clone(),
                            error_from_last_job: status.error_from_last_job.clone(),
                            replication_job_status: status.replication_job_status.clone(),
                        },
                    );
                    replication_edges.insert(key, true);
                }
            }
        }

        // Process replication target statuses (inbound to this cluster)
        for status in &cluster.replication_target_statuses {
            let source = resolve_or_create_cluster_node(
                &mut graph,
                &mut address_to_node,
                &mut uuid_to_node,
                status.source_address.as_deref(),
                status.source_cluster_uuid.as_deref(),
            );

            if let Some(src_node) = source {
                let key = ReplicationEdgeKey::new(
                    status.source_root_path.as_deref(),
                    status.target_root_path.as_deref(),
                    src_node,
                    source_node,
                );
                if !replication_edges.contains_key(&key) {
                    graph.add_edge(
                        src_node,
                        source_node,
                        CdfEdge::Replication {
                            source_path: status.source_root_path.clone(),
                            target_path: status.target_root_path.clone(),
                            mode: None, // target side doesn't have mode
                            enabled: status.replication_enabled,
                            state: status.state.clone(),
                            job_state: status.job_state.clone(),
                            recovery_point: status.recovery_point.clone(),
                            error_from_last_job: status.error_from_last_job.clone(),
                            replication_job_status: status.replication_job_status.clone(),
                        },
                    );
                    replication_edges.insert(key, true);
                }
            }
        }

        // Process object relationships (S3 bucket edges)
        for obj in &cluster.object_relationships {
            if let (Some(addr), Some(bucket)) =
                (&obj.object_store_address, &obj.bucket)
            {
                let s3_node = get_or_create_s3_node(
                    &mut graph,
                    &mut address_to_node,
                    addr,
                    bucket,
                    obj.region.as_deref(),
                );

                // Look up matching status by ID
                let obj_status = cluster
                    .object_relationship_statuses
                    .iter()
                    .find(|s| s.id == obj.id);

                let direction = obj.direction.as_deref().unwrap_or("COPY_TO_OBJECT");
                let (from, to) = if direction == "COPY_FROM_OBJECT" {
                    (s3_node, source_node)
                } else {
                    (source_node, s3_node)
                };

                graph.add_edge(
                    from,
                    to,
                    CdfEdge::ObjectReplication {
                        direction: obj.direction.clone(),
                        bucket: obj.bucket.clone(),
                        folder: obj.object_folder.clone(),
                        state: obj_status.and_then(|s| s.state.clone()),
                    },
                );
            }
        }
    }

    // Step 3: If cluster_filter is set, prune graph to only relevant nodes
    if let Some(filter) = cluster_filter {
        prune_graph(&mut graph, filter);
    }

    graph
}

/// Resolve a cluster reference (by address or UUID) to an existing node, or create an
/// UnknownCluster node if not found.
fn resolve_or_create_cluster_node(
    graph: &mut CdfGraph,
    address_to_node: &mut HashMap<String, NodeIndex>,
    uuid_to_node: &mut HashMap<String, NodeIndex>,
    address: Option<&str>,
    uuid: Option<&str>,
) -> Option<NodeIndex> {
    // Try address first
    if let Some(addr) = address {
        if let Some(&node) = address_to_node.get(addr) {
            return Some(node);
        }
    }

    // Try UUID
    if let Some(uuid) = uuid {
        if !uuid.is_empty() {
            if let Some(&node) = uuid_to_node.get(uuid) {
                return Some(node);
            }
        }
    }

    // Neither matched — create an UnknownCluster node
    if address.is_some() || uuid.is_some() {
        let addr = address.unwrap_or("").to_string();
        let uuid_opt = uuid.filter(|u| !u.is_empty()).map(|u| u.to_string());

        let node = graph.add_node(CdfNode::UnknownCluster {
            address: addr.clone(),
            uuid: uuid_opt.clone(),
        });

        if !addr.is_empty() {
            address_to_node.insert(addr, node);
        }
        if let Some(ref u) = uuid_opt {
            uuid_to_node.insert(u.clone(), node);
        }

        Some(node)
    } else {
        None
    }
}

/// Get or create an S3Bucket node.
fn get_or_create_s3_node(
    graph: &mut CdfGraph,
    address_to_node: &mut HashMap<String, NodeIndex>,
    address: &str,
    bucket: &str,
    region: Option<&str>,
) -> NodeIndex {
    let key = format!("s3://{}:{}", address, bucket);
    if let Some(&node) = address_to_node.get(&key) {
        return node;
    }

    let node = graph.add_node(CdfNode::S3Bucket {
        address: address.to_string(),
        bucket: bucket.to_string(),
        region: region.map(|r| r.to_string()),
    });
    address_to_node.insert(key, node);
    node
}

/// Find the spoke ID on the remote cluster that matches this hub.
fn find_matching_spoke(
    clusters: &[ClusterCdfData],
    hub_cluster: &ClusterCdfData,
    _hub: &PortalHub,
) -> Option<u64> {
    // Look through all other clusters' spokes for one that references our hub
    let hub_uuid = &hub_cluster.cluster_uuid;
    for cluster in clusters {
        if cluster.profile == hub_cluster.profile {
            continue;
        }
        for spoke in &cluster.portal_spokes {
            if spoke.hub_cluster_uuid.as_deref() == Some(hub_uuid.as_str()) {
                return Some(spoke.id);
            }
            // Also match by hub host address
            if spoke
                .hub_hosts
                .iter()
                .any(|h| h.address == hub_cluster.address)
            {
                return Some(spoke.id);
            }
        }
    }
    None
}

/// Find the hub ID on the remote cluster that matches this spoke.
fn find_matching_hub(
    clusters: &[ClusterCdfData],
    spoke_cluster: &ClusterCdfData,
    _spoke: &PortalSpoke,
) -> Option<u64> {
    let spoke_uuid = &spoke_cluster.cluster_uuid;
    for cluster in clusters {
        if cluster.profile == spoke_cluster.profile {
            continue;
        }
        for hub in &cluster.portal_hubs {
            if hub.spoke_cluster_uuid.as_deref() == Some(spoke_uuid.as_str()) {
                return Some(hub.id);
            }
            if hub
                .spoke_hosts
                .iter()
                .any(|h| h.address == spoke_cluster.address)
            {
                return Some(hub.id);
            }
        }
    }
    None
}

/// Prune the graph to only include nodes connected to the named cluster and their direct neighbors.
fn prune_graph(graph: &mut CdfGraph, cluster_name: &str) {
    use petgraph::visit::EdgeRef;

    // Find the matching node
    let matching: Vec<NodeIndex> = graph
        .node_indices()
        .filter(|&n| match &graph[n] {
            CdfNode::ProfiledCluster { name, .. } => {
                name.eq_ignore_ascii_case(cluster_name)
            }
            _ => false,
        })
        .collect();

    if matching.is_empty() {
        return; // No match found, don't prune
    }

    // Collect all nodes reachable from matching nodes (1-hop neighbors)
    let mut keep: std::collections::HashSet<NodeIndex> = std::collections::HashSet::new();
    for &node in &matching {
        keep.insert(node);
        for edge in graph.edges(node) {
            keep.insert(edge.target());
        }
        // Also check incoming edges
        for edge in graph.edges_directed(node, petgraph::Direction::Incoming) {
            keep.insert(edge.source());
        }
    }

    // Remove nodes not in keep set
    let to_remove: Vec<NodeIndex> = graph
        .node_indices()
        .filter(|n| !keep.contains(n))
        .collect();
    // Remove in reverse order to preserve indices
    for node in to_remove.into_iter().rev() {
        graph.remove_node(node);
    }
}

// ─── Deduplication Keys ──────────────────────────────────────────────────────

#[derive(Hash, Eq, PartialEq, Debug)]
struct PortalEdgeKey {
    /// Sorted node pair + portal ID to ensure consistent keys regardless of direction
    low_node: usize,
    high_node: usize,
    portal_id: u64,
}

impl PortalEdgeKey {
    fn new(a: NodeIndex, b: NodeIndex, portal_id: u64) -> Self {
        let (low, high) = if a.index() <= b.index() {
            (a.index(), b.index())
        } else {
            (b.index(), a.index())
        };
        Self {
            low_node: low,
            high_node: high,
            portal_id,
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
struct ReplicationEdgeKey {
    source_path: String,
    target_path: String,
    source_node: usize,
    target_node: usize,
}

impl ReplicationEdgeKey {
    fn new(
        source_path: Option<&str>,
        target_path: Option<&str>,
        source_node: NodeIndex,
        target_node: NodeIndex,
    ) -> Self {
        // Normalize direction: always source→target by node index order for matching
        let (sn, tn, sp, tp) = if source_node.index() <= target_node.index() {
            (
                source_node.index(),
                target_node.index(),
                source_path.unwrap_or("").to_string(),
                target_path.unwrap_or("").to_string(),
            )
        } else {
            (
                target_node.index(),
                source_node.index(),
                target_path.unwrap_or("").to_string(),
                source_path.unwrap_or("").to_string(),
            )
        };
        Self {
            source_path: sp,
            target_path: tp,
            source_node: sn,
            target_node: tn,
        }
    }
}

/// Simple text dump of a CdfGraph for debugging / pre-renderer output.
pub fn dump_graph_text(graph: &CdfGraph) -> String {
    use petgraph::visit::EdgeRef;

    let mut lines = Vec::new();
    lines.push(format!(
        "CDF Graph: {} nodes, {} edges",
        graph.node_count(),
        graph.edge_count()
    ));
    lines.push(String::new());

    // List nodes
    lines.push("Nodes:".to_string());
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let desc = match node {
            CdfNode::ProfiledCluster {
                name,
                uuid,
                address,
            } => format!("  [{}] {} (uuid={}, addr={})", idx.index(), name, uuid, address),
            CdfNode::UnknownCluster { address, uuid } => format!(
                "  [{}] UNKNOWN (addr={}, uuid={})",
                idx.index(),
                address,
                uuid.as_deref().unwrap_or("?")
            ),
            CdfNode::S3Bucket {
                address,
                bucket,
                region,
            } => format!(
                "  [{}] S3 s3://{}/{} (region={})",
                idx.index(),
                address,
                bucket,
                region.as_deref().unwrap_or("?")
            ),
        };
        lines.push(desc);
    }

    lines.push(String::new());
    lines.push("Edges:".to_string());
    for edge in graph.edge_references() {
        let src = &graph[edge.source()];
        let tgt = &graph[edge.target()];
        let src_name = node_short_name(src);
        let tgt_name = node_short_name(tgt);

        let desc = match edge.weight() {
            CdfEdge::Portal {
                portal_type,
                state,
                status,
                ..
            } => format!(
                "  {} → {} [Portal: type={}, state={}, status={}]",
                src_name, tgt_name, portal_type, state, status
            ),
            CdfEdge::Replication {
                source_path,
                target_path,
                mode,
                enabled,
                state,
                job_state,
                ..
            } => format!(
                "  {} → {} [Replication: {}:{} → {}:{}, mode={}, enabled={}, state={}, job_state={}]",
                src_name,
                tgt_name,
                src_name,
                source_path.as_deref().unwrap_or("?"),
                tgt_name,
                target_path.as_deref().unwrap_or("?"),
                mode.as_deref().unwrap_or("?"),
                enabled,
                state.as_deref().unwrap_or("?"),
                job_state.as_deref().unwrap_or("?"),
            ),
            CdfEdge::ObjectReplication {
                direction,
                bucket,
                folder,
                state,
            } => format!(
                "  {} → {} [ObjectReplication: dir={}, bucket={}, folder={}, state={}]",
                src_name,
                tgt_name,
                direction.as_deref().unwrap_or("?"),
                bucket.as_deref().unwrap_or("?"),
                folder.as_deref().unwrap_or("?"),
                state.as_deref().unwrap_or("?"),
            ),
        };
        lines.push(desc);
    }

    lines.join("\n")
}

fn node_short_name(node: &CdfNode) -> String {
    match node {
        CdfNode::ProfiledCluster { name, .. } => name.clone(),
        CdfNode::UnknownCluster { address, .. } => {
            if address.is_empty() {
                "unknown".to_string()
            } else {
                address.clone()
            }
        }
        CdfNode::S3Bucket { bucket, .. } => format!("s3://{}", bucket),
    }
}

/// Serialize the graph to a JSON value for --json output.
pub fn graph_to_json(graph: &CdfGraph) -> serde_json::Value {
    use petgraph::visit::EdgeRef;

    let nodes: Vec<serde_json::Value> = graph
        .node_indices()
        .map(|idx| {
            let node = &graph[idx];
            match node {
                CdfNode::ProfiledCluster {
                    name,
                    uuid,
                    address,
                } => serde_json::json!({
                    "type": "profiled_cluster",
                    "name": name,
                    "uuid": uuid,
                    "address": address,
                }),
                CdfNode::UnknownCluster { address, uuid } => serde_json::json!({
                    "type": "unknown_cluster",
                    "address": address,
                    "uuid": uuid,
                }),
                CdfNode::S3Bucket {
                    address,
                    bucket,
                    region,
                } => serde_json::json!({
                    "type": "s3_bucket",
                    "address": address,
                    "bucket": bucket,
                    "region": region,
                }),
            }
        })
        .collect();

    let edges: Vec<serde_json::Value> = graph
        .edge_references()
        .map(|edge| {
            let src = edge.source().index();
            let tgt = edge.target().index();
            match edge.weight() {
                CdfEdge::Portal {
                    hub_id,
                    spoke_id,
                    portal_type,
                    state,
                    status,
                } => serde_json::json!({
                    "source": src,
                    "target": tgt,
                    "type": "portal",
                    "hub_id": hub_id,
                    "spoke_id": spoke_id,
                    "portal_type": portal_type,
                    "state": state,
                    "status": status,
                }),
                CdfEdge::Replication {
                    source_path,
                    target_path,
                    mode,
                    enabled,
                    state,
                    job_state,
                    recovery_point,
                    error_from_last_job,
                    replication_job_status,
                } => {
                    let mut obj = serde_json::json!({
                        "source": src,
                        "target": tgt,
                        "type": "replication",
                        "source_path": source_path,
                        "target_path": target_path,
                        "mode": mode,
                        "enabled": enabled,
                        "state": state,
                        "job_state": job_state,
                        "recovery_point": recovery_point,
                        "error_from_last_job": error_from_last_job,
                    });
                    if let Some(job) = replication_job_status {
                        obj["replication_job_status"] = serde_json::to_value(job).unwrap_or_default();
                    }
                    obj
                },
                CdfEdge::ObjectReplication {
                    direction,
                    bucket,
                    folder,
                    state,
                } => serde_json::json!({
                    "source": src,
                    "target": tgt,
                    "type": "object_replication",
                    "direction": direction,
                    "bucket": bucket,
                    "folder": folder,
                    "state": state,
                }),
            }
        })
        .collect();

    serde_json::json!({
        "nodes": nodes,
        "edges": edges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cluster(
        profile: &str,
        name: &str,
        uuid: &str,
        address: &str,
    ) -> ClusterCdfData {
        ClusterCdfData {
            profile: profile.to_string(),
            cluster_name: name.to_string(),
            cluster_uuid: uuid.to_string(),
            address: address.to_string(),
            portal_hubs: Vec::new(),
            portal_spokes: Vec::new(),
            replication_sources: Vec::new(),
            replication_source_statuses: Vec::new(),
            replication_target_statuses: Vec::new(),
            object_relationships: Vec::new(),
            object_relationship_statuses: Vec::new(),
        }
    }

    #[test]
    fn test_empty_clusters_produce_empty_graph() {
        let graph = build_cdf_graph(&[], None);
        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_profiled_clusters_become_nodes() {
        let clusters = vec![
            make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1"),
            make_cluster("b", "cluster-b", "uuid-b", "10.0.1.1"),
        ];
        let graph = build_cdf_graph(&clusters, None);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_replication_deduplication() {
        // Cluster A reports outbound replication to B,
        // Cluster B reports inbound replication from A.
        // Should produce a SINGLE edge.
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        cluster_a
            .replication_source_statuses
            .push(ReplicationSourceStatus {
                id: "rel-001".into(),
                state: Some("ESTABLISHED".into()),
                source_cluster_name: Some("cluster-a".into()),
                source_cluster_uuid: Some("uuid-a".into()),
                source_root_path: Some("/data".into()),
                target_cluster_name: Some("cluster-b".into()),
                target_cluster_uuid: Some("uuid-b".into()),
                target_root_path: Some("/replica".into()),
                target_address: Some("10.0.1.1".into()),
                replication_mode: Some("REPLICATION_CONTINUOUS".into()),
                replication_enabled: true,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                duration_of_last_job: None,
                replication_job_status: None,
            });

        let mut cluster_b = make_cluster("b", "cluster-b", "uuid-b", "10.0.1.1");
        cluster_b
            .replication_target_statuses
            .push(ReplicationTargetStatus {
                id: "rel-001".into(),
                state: Some("ESTABLISHED".into()),
                source_cluster_name: Some("cluster-a".into()),
                source_cluster_uuid: Some("uuid-a".into()),
                source_root_path: Some("/data".into()),
                source_address: Some("10.0.0.1".into()),
                source_port: Some(3712),
                target_cluster_name: Some("cluster-b".into()),
                target_cluster_uuid: Some("uuid-b".into()),
                target_root_path: Some("/replica".into()),
                target_root_read_only: Some(true),
                replication_enabled: true,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                duration_of_last_job: None,
                replication_job_status: None,
            });

        let graph = build_cdf_graph(&[cluster_a, cluster_b], None);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1); // Deduplicated!
    }

    #[test]
    fn test_unknown_cluster_resolution() {
        // Cluster A has replication to an address not in our profiles.
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        cluster_a
            .replication_source_statuses
            .push(ReplicationSourceStatus {
                id: "rel-unknown".into(),
                state: Some("ESTABLISHED".into()),
                source_cluster_name: Some("cluster-a".into()),
                source_cluster_uuid: Some("uuid-a".into()),
                source_root_path: Some("/data".into()),
                target_cluster_name: Some("mystery-cluster".into()),
                target_cluster_uuid: Some("uuid-mystery".into()),
                target_root_path: Some("/replica".into()),
                target_address: Some("192.168.1.100".into()),
                replication_mode: Some("REPLICATION_CONTINUOUS".into()),
                replication_enabled: true,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                duration_of_last_job: None,
                replication_job_status: None,
            });

        let graph = build_cdf_graph(&[cluster_a], None);
        assert_eq!(graph.node_count(), 2); // cluster-a + unknown
        assert_eq!(graph.edge_count(), 1);

        // Verify the unknown node exists
        let has_unknown = graph.node_indices().any(|n| {
            matches!(&graph[n], CdfNode::UnknownCluster { address, .. } if address == "192.168.1.100")
        });
        assert!(has_unknown);
    }

    #[test]
    fn test_s3_bucket_node_creation() {
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        cluster_a.object_relationships.push(ObjectRelationship {
            id: "obj-030".into(),
            direction: Some("COPY_TO_OBJECT".into()),
            local_directory_id: Some("100".into()),
            object_store_address: Some("s3.amazonaws.com".into()),
            port: Some(443),
            bucket: Some("my-backup-bucket".into()),
            bucket_style: Some("BUCKET_STYLE_VIRTUAL_HOSTED".into()),
            object_folder: Some("daily/".into()),
            region: Some("us-east-1".into()),
            access_key_id: Some("AKIA...".into()),
        });

        let graph = build_cdf_graph(&[cluster_a], None);
        assert_eq!(graph.node_count(), 2); // cluster-a + s3 bucket
        assert_eq!(graph.edge_count(), 1);

        let has_s3 = graph.node_indices().any(|n| {
            matches!(&graph[n], CdfNode::S3Bucket { bucket, .. } if bucket == "my-backup-bucket")
        });
        assert!(has_s3);
    }

    #[test]
    fn test_portal_deduplication() {
        // Cluster A is hub, reports spoke is cluster B.
        // Cluster B is spoke, reports hub is cluster A.
        // Should produce a single edge.
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        cluster_a.portal_hubs.push(PortalHub {
            id: 1,
            portal_type: "PORTAL_READ_WRITE".into(),
            state: "ACCEPTED".into(),
            status: "ACTIVE".into(),
            spoke_hosts: vec![HostAddress {
                address: "10.0.1.1".into(),
                port: 3712,
            }],
            spoke_cluster_uuid: Some("uuid-b".into()),
            spoke_cluster_name: Some("cluster-b".into()),
            pending_roots: Vec::new(),
            authorized_roots: vec!["/data".into()],
        });

        let mut cluster_b = make_cluster("b", "cluster-b", "uuid-b", "10.0.1.1");
        cluster_b.portal_spokes.push(PortalSpoke {
            id: 5,
            portal_type: "PORTAL_READ_WRITE".into(),
            state: "ACCEPTED".into(),
            status: "ACTIVE".into(),
            hub_hosts: vec![HostAddress {
                address: "10.0.0.1".into(),
                port: 3712,
            }],
            hub_id: Some(1),
            hub_cluster_uuid: Some("uuid-a".into()),
            roots: vec![SpokeRoot {
                local_root: "/remote-data".into(),
                remote_root: "/data".into(),
                authorized: true,
            }],
        });

        let graph = build_cdf_graph(&[cluster_a, cluster_b], None);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1); // Deduplicated!
    }

    #[test]
    fn test_graph_json_serialization() {
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        cluster_a.object_relationships.push(ObjectRelationship {
            id: "obj-001".into(),
            direction: Some("COPY_TO_OBJECT".into()),
            local_directory_id: None,
            object_store_address: Some("s3.amazonaws.com".into()),
            port: None,
            bucket: Some("test-bucket".into()),
            bucket_style: None,
            object_folder: None,
            region: Some("us-east-1".into()),
            access_key_id: None,
        });

        let graph = build_cdf_graph(&[cluster_a], None);
        let json = graph_to_json(&graph);

        assert_eq!(json["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(json["edges"].as_array().unwrap().len(), 1);
        assert_eq!(json["nodes"][0]["type"], "profiled_cluster");
    }

    #[test]
    fn test_cluster_filter_prunes_graph() {
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        let cluster_b = make_cluster("b", "cluster-b", "uuid-b", "10.0.1.1");
        let cluster_c = make_cluster("c", "cluster-c", "uuid-c", "10.0.2.1");

        // A replicates to B, C is standalone
        cluster_a
            .replication_source_statuses
            .push(ReplicationSourceStatus {
                id: "rel-001".into(),
                state: Some("ESTABLISHED".into()),
                source_cluster_name: Some("cluster-a".into()),
                source_cluster_uuid: Some("uuid-a".into()),
                source_root_path: Some("/data".into()),
                target_cluster_name: Some("cluster-b".into()),
                target_cluster_uuid: Some("uuid-b".into()),
                target_root_path: Some("/replica".into()),
                target_address: Some("10.0.1.1".into()),
                replication_mode: Some("REPLICATION_CONTINUOUS".into()),
                replication_enabled: true,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                duration_of_last_job: None,
                replication_job_status: None,
            });

        let graph = build_cdf_graph(
            &[cluster_a, cluster_b, cluster_c],
            Some("cluster-a"),
        );

        // Should have A and B (connected), but not C
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_text_dump_output() {
        let clusters = vec![make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1")];
        let graph = build_cdf_graph(&clusters, None);
        let output = dump_graph_text(&graph);
        assert!(output.contains("CDF Graph: 1 nodes, 0 edges"));
        assert!(output.contains("cluster-a"));
    }

    #[test]
    fn test_multiple_s3_buckets_same_address() {
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        cluster_a.object_relationships.push(ObjectRelationship {
            id: "obj-001".into(),
            direction: Some("COPY_TO_OBJECT".into()),
            local_directory_id: None,
            object_store_address: Some("s3.amazonaws.com".into()),
            port: None,
            bucket: Some("bucket-1".into()),
            bucket_style: None,
            object_folder: None,
            region: Some("us-east-1".into()),
            access_key_id: None,
        });
        cluster_a.object_relationships.push(ObjectRelationship {
            id: "obj-002".into(),
            direction: Some("COPY_TO_OBJECT".into()),
            local_directory_id: None,
            object_store_address: Some("s3.amazonaws.com".into()),
            port: None,
            bucket: Some("bucket-2".into()),
            bucket_style: None,
            object_folder: None,
            region: Some("us-east-1".into()),
            access_key_id: None,
        });

        let graph = build_cdf_graph(&[cluster_a], None);
        // cluster-a + 2 distinct S3 bucket nodes
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 2);
    }

    #[test]
    fn test_copy_from_object_direction() {
        let mut cluster_a = make_cluster("a", "cluster-a", "uuid-a", "10.0.0.1");
        cluster_a.object_relationships.push(ObjectRelationship {
            id: "obj-001".into(),
            direction: Some("COPY_FROM_OBJECT".into()),
            local_directory_id: None,
            object_store_address: Some("s3.amazonaws.com".into()),
            port: None,
            bucket: Some("source-bucket".into()),
            bucket_style: None,
            object_folder: None,
            region: None,
            access_key_id: None,
        });

        let graph = build_cdf_graph(&[cluster_a], None);
        // Edge should go from S3 → cluster (COPY_FROM_OBJECT)
        use petgraph::visit::EdgeRef;
        let edge = graph.edge_references().next().unwrap();
        assert!(matches!(
            &graph[edge.source()],
            CdfNode::S3Bucket { .. }
        ));
        assert!(matches!(
            &graph[edge.target()],
            CdfNode::ProfiledCluster { .. }
        ));
    }
}
