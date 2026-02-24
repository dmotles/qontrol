pub mod collector;
pub mod renderer;
pub mod renderer_table;
pub mod types;

use anyhow::Result;

use crate::config::Config;

/// Run the CDF status command: collect from all clusters and display the graph.
pub fn run(
    config: &Config,
    profiles: &[String],
    json_mode: bool,
    detail: bool,
    graph_mode: bool,
    cluster_filter: Option<&str>,
    timeout_secs: u64,
) -> Result<()> {
    let result = collector::collect_all(config, profiles, timeout_secs, cluster_filter)?;

    // Report any collection errors
    if !result.errors.is_empty() {
        for err in &result.errors {
            eprintln!("warning: {}: {}", err.profile, err.error);
        }
    }

    if json_mode {
        let json = collector::graph_to_json(&result.graph);
        println!("{}", serde_json::to_string_pretty(&json)?);
    } else if graph_mode {
        let output = renderer::render(&result.graph, detail);
        print!("{}", output);
    } else {
        let output = renderer_table::render_table(&result.graph, detail);
        print!("{}", output);
    }

    Ok(())
}
