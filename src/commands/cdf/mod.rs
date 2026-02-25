pub mod collector;
pub mod renderer;
pub mod renderer_table;
pub mod types;

use anyhow::Result;
use petgraph::visit::EdgeRef;

use crate::config::Config;
use types::CdfGraph;

/// Filter a CdfGraph to only include edges that represent problems.
fn filter_problems(graph: &CdfGraph) -> CdfGraph {
    let mut filtered = CdfGraph::new();

    // Copy all nodes, keeping a mapping from old to new indices
    let mut node_map = std::collections::HashMap::new();
    for idx in graph.node_indices() {
        let new_idx = filtered.add_node(graph[idx].clone());
        node_map.insert(idx, new_idx);
    }

    // Copy only problem edges
    for edge in graph.edge_references() {
        if edge.weight().is_problem() {
            filtered.add_edge(
                node_map[&edge.source()],
                node_map[&edge.target()],
                edge.weight().clone(),
            );
        }
    }

    // Remove orphaned nodes (no edges after filtering)
    let orphans: Vec<_> = filtered
        .node_indices()
        .filter(|&idx| {
            filtered.edges(idx).next().is_none()
                && filtered
                    .edges_directed(idx, petgraph::Direction::Incoming)
                    .next()
                    .is_none()
        })
        .collect();
    // Remove in reverse order to preserve indices
    for idx in orphans.into_iter().rev() {
        filtered.remove_node(idx);
    }

    filtered
}

/// Run the CDF status command: collect from all clusters and display the graph.
pub fn run(
    config: &Config,
    profiles: &[String],
    json_mode: bool,
    graph_mode: bool,
    cluster_filter: Option<&str>,
    problems_only: bool,
    timeout_secs: u64,
) -> Result<()> {
    let result = collector::collect_all(config, profiles, timeout_secs, cluster_filter)?;

    // Report any collection errors
    if !result.errors.is_empty() {
        for err in &result.errors {
            eprintln!("warning: {}: {}", err.profile, err.error);
        }
    }

    let graph = if problems_only {
        let filtered = filter_problems(&result.graph);
        if filtered.edge_count() == 0 {
            eprintln!("No problems found.");
            return Ok(());
        }
        filtered
    } else {
        result.graph
    };

    if json_mode {
        let json = collector::graph_to_json(&graph);
        println!("{}", serde_json::to_string_pretty(&json)?);
    } else if graph_mode {
        let output = renderer::render(&graph);
        print!("{}", output);
    } else {
        let output = renderer_table::render_table(&graph);
        print!("{}", output);
    }

    Ok(())
}
