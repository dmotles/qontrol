use anyhow::Result;
use console::Style;
use serde::Serialize;
use serde_json::Value;

use crate::client::QumuloClient;
use crate::config::{Config, ProfileEntry};

/// A single PSU entry with its health status.
#[derive(Debug, Clone, Serialize)]
struct PsuEntry {
    node_id: u64,
    psu_name: String,
    location: String,
    state: String,
}

/// Result of checking PSUs on one cluster.
#[derive(Debug, Clone, Serialize)]
struct ClusterPsuResult {
    cluster: String,
    node_count: usize,
    psu_count: usize,
    healthy_count: usize,
    unhealthy_count: usize,
    psus: Vec<PsuEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Parse all PSUs (healthy and unhealthy) from a chassis JSON array.
fn parse_all_psus(chassis: &Value) -> (usize, Vec<PsuEntry>) {
    let mut psus = Vec::new();
    let mut node_count = 0;
    if let Some(nodes) = chassis.as_array() {
        node_count = nodes.len();
        for node in nodes {
            let node_id = node["id"].as_u64().unwrap_or(0);
            if let Some(psu_statuses) = node["psu_statuses"].as_array() {
                for psu in psu_statuses {
                    psus.push(PsuEntry {
                        node_id,
                        psu_name: psu["name"].as_str().unwrap_or("unknown").to_string(),
                        location: psu["location"].as_str().unwrap_or("unknown").to_string(),
                        state: psu["state"].as_str().unwrap_or("unknown").to_string(),
                    });
                }
            }
        }
    }
    (node_count, psus)
}

/// Check PSU health for a single cluster.
pub fn check(client: &QumuloClient, json_mode: bool) -> Result<()> {
    let chassis = client.get_cluster_chassis()?;
    let (node_count, psus) = parse_all_psus(&chassis);
    let unhealthy_count = psus.iter().filter(|p| !p.state.eq_ignore_ascii_case("GOOD")).count();
    let healthy_count = psus.len() - unhealthy_count;

    if json_mode {
        let result = ClusterPsuResult {
            cluster: String::new(),
            node_count,
            psu_count: psus.len(),
            healthy_count,
            unhealthy_count,
            psus,
            error: None,
        };
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        print_psu_table(&psus);
        println!();
        println!(
            "{} PSUs across {} nodes: {} healthy, {} unhealthy",
            psus.len(),
            node_count,
            healthy_count,
            unhealthy_count,
        );
    }

    if unhealthy_count > 0 {
        std::process::exit(1);
    }
    Ok(())
}

/// Check PSU health across all configured clusters (fleet).
pub fn fleet_check(
    config: &Config,
    profile_filters: &[String],
    timeout_secs: u64,
    json_mode: bool,
    verbose: bool,
) -> Result<()> {
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

    // Collect PSU status from all clusters in parallel
    let results: Vec<ClusterPsuResult> = std::thread::scope(|s| {
        let handles: Vec<_> = profiles
            .iter()
            .map(|(name, entry)| {
                let name = name.clone();
                let entry = entry.clone();
                s.spawn(move || {
                    match QumuloClient::new(&entry, timeout_secs, None) {
                        Ok(client) => match client.get_cluster_chassis() {
                            Ok(chassis) => {
                                let (node_count, psus) = parse_all_psus(&chassis);
                                let unhealthy_count = psus
                                    .iter()
                                    .filter(|p| !p.state.eq_ignore_ascii_case("GOOD"))
                                    .count();
                                ClusterPsuResult {
                                    cluster: name,
                                    node_count,
                                    psu_count: psus.len(),
                                    healthy_count: psus.len() - unhealthy_count,
                                    unhealthy_count,
                                    psus,
                                    error: None,
                                }
                            }
                            Err(e) => ClusterPsuResult {
                                cluster: name,
                                node_count: 0,
                                psu_count: 0,
                                healthy_count: 0,
                                unhealthy_count: 0,
                                psus: Vec::new(),
                                error: Some(format!("{:#}", e)),
                            },
                        },
                        Err(e) => ClusterPsuResult {
                            cluster: name,
                            node_count: 0,
                            psu_count: 0,
                            healthy_count: 0,
                            unhealthy_count: 0,
                            psus: Vec::new(),
                            error: Some(format!("{:#}", e)),
                        },
                    }
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| ClusterPsuResult {
                    cluster: "unknown".to_string(),
                    node_count: 0,
                    psu_count: 0,
                    healthy_count: 0,
                    unhealthy_count: 0,
                    psus: Vec::new(),
                    error: Some("thread panicked".to_string()),
                })
            })
            .collect()
    });

    let any_unhealthy = results.iter().any(|r| r.unhealthy_count > 0);
    let any_errors = results.iter().any(|r| r.error.is_some());

    if json_mode {
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        let green = Style::new().green();
        let red = Style::new().red();
        let yellow = Style::new().yellow();
        let bold = Style::new().bold();

        // Summary table
        println!(
            "{:<20} {:>5} {:>5} {:>7} {:>9}  {}",
            bold.apply_to("CLUSTER"),
            bold.apply_to("NODES"),
            bold.apply_to("PSUS"),
            bold.apply_to("HEALTHY"),
            bold.apply_to("UNHEALTHY"),
            bold.apply_to("STATUS"),
        );
        println!("{}", "-".repeat(72));

        for r in &results {
            if let Some(ref err) = r.error {
                println!(
                    "{:<20} {:>5} {:>5} {:>7} {:>9}  {}",
                    r.cluster,
                    "-",
                    "-",
                    "-",
                    "-",
                    yellow.apply_to(format!("error: {}", truncate(err, 30))),
                );
            } else {
                let status = if r.unhealthy_count == 0 {
                    green.apply_to("✓ healthy".to_string())
                } else {
                    red.apply_to(format!("✗ {} unhealthy", r.unhealthy_count))
                };
                println!(
                    "{:<20} {:>5} {:>5} {:>7} {:>9}  {}",
                    r.cluster,
                    r.node_count,
                    r.psu_count,
                    r.healthy_count,
                    r.unhealthy_count,
                    status,
                );
            }
        }

        // Verbose: show per-node detail for clusters with issues or all clusters
        if verbose {
            println!();
            for r in &results {
                if r.error.is_some() || r.psus.is_empty() {
                    continue;
                }
                println!("{}", bold.apply_to(format!("── {} ──", r.cluster)));
                print_psu_table(&r.psus);
                println!();
            }
        } else if any_unhealthy {
            // Even without --verbose, show unhealthy PSU details
            println!();
            println!("{}", bold.apply_to("Unhealthy PSUs:"));
            for r in &results {
                for psu in &r.psus {
                    if !psu.state.eq_ignore_ascii_case("GOOD") {
                        println!(
                            "  {} node {} {} ({}) — {}",
                            r.cluster,
                            psu.node_id,
                            psu.psu_name,
                            psu.location,
                            red.apply_to(&psu.state),
                        );
                    }
                }
            }
        }
    }

    if any_unhealthy || any_errors {
        std::process::exit(1);
    }
    Ok(())
}

fn print_psu_table(psus: &[PsuEntry]) {
    let green = Style::new().green();
    let red = Style::new().red();
    let bold = Style::new().bold();

    println!(
        "  {:<8} {:<10} {:<10} {}",
        bold.apply_to("NODE"),
        bold.apply_to("PSU"),
        bold.apply_to("LOCATION"),
        bold.apply_to("STATE"),
    );
    println!("  {}", "-".repeat(42));
    for psu in psus {
        let state_styled = if psu.state.eq_ignore_ascii_case("GOOD") {
            green.apply_to(&psu.state)
        } else {
            red.apply_to(&psu.state)
        };
        println!(
            "  {:<8} {:<10} {:<10} {}",
            psu.node_id, psu.psu_name, psu.location, state_styled,
        );
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_all_psus_healthy() {
        let chassis = json!([
            {
                "id": 1,
                "psu_statuses": [
                    {"name": "PSU1", "location": "left", "state": "GOOD"},
                    {"name": "PSU2", "location": "right", "state": "GOOD"}
                ]
            },
            {
                "id": 2,
                "psu_statuses": [
                    {"name": "PSU1", "location": "left", "state": "GOOD"}
                ]
            }
        ]);
        let (node_count, psus) = parse_all_psus(&chassis);
        assert_eq!(node_count, 2);
        assert_eq!(psus.len(), 3);
        assert!(psus.iter().all(|p| p.state == "GOOD"));
    }

    #[test]
    fn test_parse_all_psus_unhealthy() {
        let chassis = json!([
            {
                "id": 1,
                "psu_statuses": [
                    {"name": "PSU1", "location": "left", "state": "GOOD"},
                    {"name": "PSU2", "location": "right", "state": "FAILED"}
                ]
            }
        ]);
        let (node_count, psus) = parse_all_psus(&chassis);
        assert_eq!(node_count, 1);
        assert_eq!(psus.len(), 2);
        assert_eq!(psus[1].state, "FAILED");
        assert_eq!(psus[1].node_id, 1);
    }

    #[test]
    fn test_parse_all_psus_empty() {
        let chassis = json!([]);
        let (node_count, psus) = parse_all_psus(&chassis);
        assert_eq!(node_count, 0);
        assert_eq!(psus.len(), 0);
    }

    #[test]
    fn test_parse_all_psus_no_psu_statuses() {
        let chassis = json!([{"id": 1}]);
        let (node_count, psus) = parse_all_psus(&chassis);
        assert_eq!(node_count, 1);
        assert_eq!(psus.len(), 0);
    }
}
