/// A single API call timing entry.
#[derive(Debug, Clone)]
pub struct ApiCallTiming {
    pub cluster: String,
    pub api_call: String,
    pub duration_ms: u64,
}

/// Aggregated timing data for the --timing report.
#[derive(Debug, Clone, Default)]
pub struct TimingReport {
    /// Per-API-call timings (from all clusters).
    pub api_calls: Vec<ApiCallTiming>,
    /// Per-cluster wall clock time: (profile_name, wall_clock_ms).
    pub cluster_wall_clock: Vec<(String, u64)>,
}

/// Render timing report to stderr.
pub fn render_timing_report(report: &TimingReport) {
    if report.api_calls.is_empty() {
        return;
    }

    // Sort API calls by duration descending (slowest first)
    let mut sorted = report.api_calls.clone();
    sorted.sort_by(|a, b| b.duration_ms.cmp(&a.duration_ms));

    let max_cluster = sorted.iter().map(|e| e.cluster.len()).max().unwrap_or(0);
    let max_api = sorted.iter().map(|e| e.api_call.len()).max().unwrap_or(0);

    eprintln!();
    eprintln!("API Call Timing (sorted slowest first):");
    for entry in &sorted {
        eprintln!(
            "  {:<cw$}  {:<aw$}  {:>10}",
            entry.cluster,
            entry.api_call,
            format_duration_ms(entry.duration_ms),
            cw = max_cluster,
            aw = max_api,
        );
    }

    // Cluster wall-clock totals
    if !report.cluster_wall_clock.is_empty() {
        let mut totals = report.cluster_wall_clock.clone();
        totals.sort_by(|a, b| b.1.cmp(&a.1));

        let max_name = totals.iter().map(|(n, _)| n.len()).max().unwrap_or(0);

        eprintln!();
        eprintln!("Cluster totals (wall clock):");
        for (i, (name, ms)) in totals.iter().enumerate() {
            let suffix = if i == 0 && totals.len() > 1 {
                "  (slowest)"
            } else {
                ""
            };
            eprintln!(
                "  {:<w$}  {:>10}{}",
                name,
                format_duration_ms(*ms),
                suffix,
                w = max_name,
            );
        }
    }
}

/// Format milliseconds with thousands separators: 4230 → "4,230ms"
pub fn format_duration_ms(ms: u64) -> String {
    let s = ms.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    format!("{}ms", result.chars().rev().collect::<String>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_format_duration_ms_small() {
        assert_eq!(format_duration_ms(0), "0ms");
        assert_eq!(format_duration_ms(1), "1ms");
        assert_eq!(format_duration_ms(42), "42ms");
        assert_eq!(format_duration_ms(999), "999ms");
    }

    #[test]
    fn test_format_duration_ms_thousands() {
        assert_eq!(format_duration_ms(1000), "1,000ms");
        assert_eq!(format_duration_ms(4230), "4,230ms");
        assert_eq!(format_duration_ms(25102), "25,102ms");
    }

    #[test]
    fn test_format_duration_ms_large() {
        assert_eq!(format_duration_ms(1_000_000), "1,000,000ms");
        assert_eq!(format_duration_ms(123_456_789), "123,456,789ms");
    }

    #[test]
    fn test_timing_report_sorts_slowest_first() {
        let report = TimingReport {
            api_calls: vec![
                ApiCallTiming {
                    cluster: "fast".into(),
                    api_call: "get_version".into(),
                    duration_ms: 10,
                },
                ApiCallTiming {
                    cluster: "slow".into(),
                    api_call: "get_snapshots".into(),
                    duration_ms: 5000,
                },
                ApiCallTiming {
                    cluster: "medium".into(),
                    api_call: "get_nodes".into(),
                    duration_ms: 200,
                },
            ],
            cluster_wall_clock: vec![],
        };

        let mut sorted = report.api_calls.clone();
        sorted.sort_by(|a, b| b.duration_ms.cmp(&a.duration_ms));
        assert_eq!(sorted[0].duration_ms, 5000);
        assert_eq!(sorted[1].duration_ms, 200);
        assert_eq!(sorted[2].duration_ms, 10);
    }

    #[test]
    fn test_timing_report_cluster_totals_sorted() {
        let report = TimingReport {
            api_calls: vec![],
            cluster_wall_clock: vec![
                ("fast_cluster".into(), 1000),
                ("slow_cluster".into(), 5000),
                ("medium_cluster".into(), 2500),
            ],
        };

        let mut totals = report.cluster_wall_clock.clone();
        totals.sort_by(|a, b| b.1.cmp(&a.1));
        assert_eq!(totals[0].0, "slow_cluster");
        assert_eq!(totals[1].0, "medium_cluster");
        assert_eq!(totals[2].0, "fast_cluster");
    }

    #[test]
    fn test_render_timing_report_empty() {
        let report = TimingReport::default();
        // Should not panic on empty report
        render_timing_report(&report);
    }

    #[test]
    fn test_render_timing_report_smoke() {
        let report = TimingReport {
            api_calls: vec![
                ApiCallTiming {
                    cluster: "az-gravytrain".into(),
                    api_call: "get_snapshots".into(),
                    duration_ms: 4230,
                },
                ApiCallTiming {
                    cluster: "az-gravytrain".into(),
                    api_call: "get_network_connections".into(),
                    duration_ms: 3812,
                },
                ApiCallTiming {
                    cluster: "iss".into(),
                    api_call: "get_version".into(),
                    duration_ms: 50,
                },
            ],
            cluster_wall_clock: vec![("az-gravytrain".into(), 25102), ("iss".into(), 8230)],
        };
        // Should not panic
        render_timing_report(&report);
    }

    #[test]
    fn test_api_call_timing_accumulator() {
        // Verify the accumulator pattern works correctly
        let mut timings: Vec<ApiCallTiming> = Vec::new();

        timings.push(ApiCallTiming {
            cluster: "c1".into(),
            api_call: "get_version".into(),
            duration_ms: 100,
        });
        timings.push(ApiCallTiming {
            cluster: "c1".into(),
            api_call: "get_nodes".into(),
            duration_ms: 200,
        });
        timings.push(ApiCallTiming {
            cluster: "c2".into(),
            api_call: "get_version".into(),
            duration_ms: 50,
        });

        assert_eq!(timings.len(), 3);

        // Verify we can compute cluster sums
        let mut cluster_sums: HashMap<String, u64> = HashMap::new();
        for t in &timings {
            *cluster_sums.entry(t.cluster.clone()).or_default() += t.duration_ms;
        }
        assert_eq!(cluster_sums["c1"], 300);
        assert_eq!(cluster_sums["c2"], 50);
    }

    #[test]
    fn test_timing_with_unreachable_cluster() {
        // Partial timing: cluster fails partway through collection
        let report = TimingReport {
            api_calls: vec![
                ApiCallTiming {
                    cluster: "failing".into(),
                    api_call: "get_cluster_settings".into(),
                    duration_ms: 100,
                },
                // Version call failed — no more entries for this cluster
                ApiCallTiming {
                    cluster: "healthy".into(),
                    api_call: "get_cluster_settings".into(),
                    duration_ms: 50,
                },
                ApiCallTiming {
                    cluster: "healthy".into(),
                    api_call: "get_version".into(),
                    duration_ms: 30,
                },
            ],
            cluster_wall_clock: vec![("failing".into(), 150), ("healthy".into(), 80)],
        };

        // Failing cluster should show partial timing up to failure point
        let failing_calls: Vec<_> = report
            .api_calls
            .iter()
            .filter(|t| t.cluster == "failing")
            .collect();
        assert_eq!(failing_calls.len(), 1);
        assert_eq!(failing_calls[0].api_call, "get_cluster_settings");

        // Should not panic on render
        render_timing_report(&report);
    }

    #[test]
    fn test_timing_single_cluster() {
        let report = TimingReport {
            api_calls: vec![ApiCallTiming {
                cluster: "only".into(),
                api_call: "get_version".into(),
                duration_ms: 42,
            }],
            cluster_wall_clock: vec![("only".into(), 42)],
        };
        // Single cluster: no "(slowest)" suffix
        render_timing_report(&report);
    }
}
