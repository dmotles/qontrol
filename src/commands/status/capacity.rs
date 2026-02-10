use serde_json::Value;

use super::types::{CapacityProjection, ClusterType, ProjectionConfidence};

/// Minimum number of data points required to compute a projection.
const MIN_DATA_POINTS: usize = 7;

/// R-squared threshold below which confidence is marked as "low".
const LOW_CONFIDENCE_R_SQUARED: f64 = 0.5;

/// On-prem: warn if days-to-full < 90.
const ONPREM_WARN_DAYS: u64 = 90;

/// Cloud: warn if days-to-full < 7.
const CLOUD_WARN_DAYS: u64 = 7;

/// Result of a linear regression: y = slope * x + intercept.
#[derive(Debug, Clone)]
pub struct LinearRegressionResult {
    pub slope: f64,
    #[allow(dead_code)] // Used in tests to verify regression correctness
    pub intercept: f64,
    pub r_squared: f64,
}

/// Compute least-squares linear regression on (x, y) pairs.
/// Returns None if fewer than 2 points or all x values are identical.
pub fn linear_regression(points: &[(f64, f64)]) -> Option<LinearRegressionResult> {
    let n = points.len() as f64;
    if points.len() < 2 {
        return None;
    }

    let sum_x: f64 = points.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
    let sum_x2: f64 = points.iter().map(|(x, _)| x * x).sum();

    let denom = n * sum_x2 - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        return None;
    }

    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;

    // Compute R-squared
    let mean_y = sum_y / n;
    let ss_tot: f64 = points.iter().map(|(_, y)| (y - mean_y).powi(2)).sum();
    let ss_res: f64 = points
        .iter()
        .map(|(x, y)| {
            let predicted = slope * x + intercept;
            (y - predicted).powi(2)
        })
        .sum();

    let r_squared = if ss_tot.abs() < f64::EPSILON {
        1.0 // All y values are the same — perfect fit (zero growth)
    } else {
        1.0 - ss_res / ss_tot
    };

    Some(LinearRegressionResult {
        slope,
        intercept,
        r_squared,
    })
}

/// Parse capacity history JSON (array of daily data points) and extract
/// (day_index, capacity_used) pairs.
pub fn parse_capacity_history(history: &Value) -> Vec<(f64, f64)> {
    let entries = match history.as_array() {
        Some(arr) => arr,
        None => return Vec::new(),
    };

    entries
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            let used = parse_byte_str(&entry["capacity_used"])?;
            Some((i as f64, used as f64))
        })
        .collect()
}

/// Extract total_usable from the first entry of capacity history.
#[allow(dead_code)] // Public utility, used in tests
pub fn parse_total_usable(history: &Value) -> Option<u64> {
    history
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|entry| parse_byte_str(&entry["total_usable"]))
}

/// Compute a capacity projection from history data.
///
/// Returns None if:
/// - Fewer than MIN_DATA_POINTS history entries
/// - Slope <= 0 (stable or shrinking)
pub fn compute_projection(
    history: &Value,
    current_used: u64,
    total_capacity: u64,
) -> Option<CapacityProjection> {
    let points = parse_capacity_history(history);
    if points.len() < MIN_DATA_POINTS {
        return None;
    }

    let regression = linear_regression(&points)?;

    // Slope is in bytes per day_index (each index = 1 day)
    let daily_growth = regression.slope;

    if daily_growth <= 0.0 {
        return None;
    }

    let remaining = total_capacity as f64 - current_used as f64;
    let days_to_full = (remaining / daily_growth).ceil() as u64;

    let confidence = if regression.r_squared < LOW_CONFIDENCE_R_SQUARED {
        ProjectionConfidence::Low
    } else {
        ProjectionConfidence::High
    };

    Some(CapacityProjection {
        days_until_full: Some(days_to_full),
        growth_rate_bytes_per_day: daily_growth,
        confidence,
    })
}

/// Check whether a projection should trigger a warning alert.
pub fn should_warn(projection: &CapacityProjection, cluster_type: &ClusterType) -> bool {
    match projection.days_until_full {
        Some(days) => {
            let threshold = match cluster_type {
                ClusterType::OnPrem(_) => ONPREM_WARN_DAYS,
                ClusterType::CnqAws | ClusterType::AnqAzure => CLOUD_WARN_DAYS,
            };
            days < threshold
        }
        None => false,
    }
}

/// Format a projection warning message based on cluster type.
pub fn format_warning(projection: &CapacityProjection, cluster_type: &ClusterType) -> String {
    let days = projection.days_until_full.unwrap_or(0);
    let daily_tb = projection.growth_rate_bytes_per_day / 1_099_511_627_776.0;

    match cluster_type {
        ClusterType::OnPrem(_) => {
            format!(
                "projected to fill in ~{} days (+{:.1} TB/day)",
                days, daily_tb,
            )
        }
        ClusterType::CnqAws | ClusterType::AnqAzure => {
            format!(
                "may run out of space within ~{} days — consider increasing capacity clamp",
                days,
            )
        }
    }
}

fn parse_byte_str(val: &Value) -> Option<u64> {
    match val {
        Value::String(s) => s.parse::<u64>().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Linear regression tests ───────────────────────────────────

    #[test]
    fn test_linear_regression_known_points() {
        // y = 2x + 1: (0,1), (1,3), (2,5), (3,7), (4,9)
        let points: Vec<(f64, f64)> =
            vec![(0.0, 1.0), (1.0, 3.0), (2.0, 5.0), (3.0, 7.0), (4.0, 9.0)];
        let result = linear_regression(&points).unwrap();
        assert!((result.slope - 2.0).abs() < 1e-10);
        assert!((result.intercept - 1.0).abs() < 1e-10);
        assert!((result.r_squared - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_linear_regression_noisy_data() {
        // Roughly y = 1000x + 5000 with noise
        let points: Vec<(f64, f64)> = vec![
            (0.0, 5100.0),
            (1.0, 5900.0),
            (2.0, 7200.0),
            (3.0, 8050.0),
            (4.0, 8900.0),
            (5.0, 10200.0),
            (6.0, 11050.0),
        ];
        let result = linear_regression(&points).unwrap();
        assert!(result.slope > 900.0 && result.slope < 1100.0);
        assert!(result.r_squared > 0.95);
    }

    #[test]
    fn test_linear_regression_flat() {
        let points: Vec<(f64, f64)> = vec![(0.0, 100.0), (1.0, 100.0), (2.0, 100.0), (3.0, 100.0)];
        let result = linear_regression(&points).unwrap();
        assert!(result.slope.abs() < 1e-10);
        assert!((result.r_squared - 1.0).abs() < 1e-10); // Perfect fit on constant
    }

    #[test]
    fn test_linear_regression_too_few_points() {
        assert!(linear_regression(&[(0.0, 1.0)]).is_none());
        assert!(linear_regression(&[]).is_none());
    }

    #[test]
    fn test_linear_regression_two_points() {
        let result = linear_regression(&[(0.0, 0.0), (10.0, 100.0)]).unwrap();
        assert!((result.slope - 10.0).abs() < 1e-10);
        assert!(result.intercept.abs() < 1e-10);
    }

    #[test]
    fn test_linear_regression_negative_slope() {
        let points: Vec<(f64, f64)> = vec![(0.0, 100.0), (1.0, 90.0), (2.0, 80.0), (3.0, 70.0)];
        let result = linear_regression(&points).unwrap();
        assert!((result.slope - (-10.0)).abs() < 1e-10);
    }

    #[test]
    fn test_linear_regression_low_r_squared() {
        // Highly scattered data
        let points: Vec<(f64, f64)> = vec![
            (0.0, 100.0),
            (1.0, 5.0),
            (2.0, 90.0),
            (3.0, 10.0),
            (4.0, 85.0),
            (5.0, 15.0),
            (6.0, 80.0),
        ];
        let result = linear_regression(&points).unwrap();
        assert!(result.r_squared < LOW_CONFIDENCE_R_SQUARED);
    }

    // ─── Projection tests ──────────────────────────────────────────

    #[test]
    fn test_projection_growing_cluster() {
        // 10 days of data, growing ~1 TB/day
        let tb = 1_099_511_627_776_u64;
        let mut entries = Vec::new();
        for i in 0..10 {
            entries.push(serde_json::json!({
                "capacity_used": (50 * tb + i as u64 * tb).to_string(),
                "total_usable": (100 * tb).to_string(),
                "period_start_time": 1768089600 + i * 86400,
            }));
        }
        let history = Value::Array(entries);
        let current_used = 59 * tb;
        let total = 100 * tb;

        let projection = compute_projection(&history, current_used, total).unwrap();
        assert!(projection.days_until_full.is_some());
        let days = projection.days_until_full.unwrap();
        // ~41 TB remaining / ~1 TB/day = ~41 days
        assert!(days > 35 && days < 50, "days_to_full={}", days);
        assert!(projection.growth_rate_bytes_per_day > 0.9 * tb as f64);
        assert_eq!(projection.confidence, ProjectionConfidence::High);
    }

    #[test]
    fn test_projection_stable_cluster_at_95_pct() {
        // 30 days of stable usage — no growth
        let tb = 1_099_511_627_776_u64;
        let mut entries = Vec::new();
        for i in 0..30 {
            entries.push(serde_json::json!({
                "capacity_used": (95 * tb).to_string(),
                "total_usable": (100 * tb).to_string(),
                "period_start_time": 1768089600 + i * 86400,
            }));
        }
        let history = Value::Array(entries);
        let result = compute_projection(&history, 95 * tb, 100 * tb);
        assert!(result.is_none(), "stable cluster should have no projection");
    }

    #[test]
    fn test_projection_shrinking_cluster() {
        let tb = 1_099_511_627_776_u64;
        let mut entries = Vec::new();
        for i in 0..10 {
            entries.push(serde_json::json!({
                "capacity_used": (90 * tb - i as u64 * tb).to_string(),
                "total_usable": (100 * tb).to_string(),
                "period_start_time": 1768089600 + i * 86400,
            }));
        }
        let history = Value::Array(entries);
        let result = compute_projection(&history, 80 * tb, 100 * tb);
        assert!(
            result.is_none(),
            "shrinking cluster should have no projection"
        );
    }

    #[test]
    fn test_projection_fewer_than_7_days() {
        let tb = 1_099_511_627_776_u64;
        let mut entries = Vec::new();
        for i in 0..5 {
            entries.push(serde_json::json!({
                "capacity_used": (50 * tb + i as u64 * tb).to_string(),
                "total_usable": (100 * tb).to_string(),
                "period_start_time": 1768089600 + i * 86400,
            }));
        }
        let history = Value::Array(entries);
        let result = compute_projection(&history, 55 * tb, 100 * tb);
        assert!(result.is_none(), "<7 days should skip projection");
    }

    #[test]
    fn test_projection_low_confidence() {
        // Highly scattered data but with slight upward trend
        let tb = 1_099_511_627_776_u64;
        let used_values: Vec<u64> = vec![50, 80, 45, 85, 40, 90, 42, 88, 44, 92];
        let mut entries = Vec::new();
        for (i, &used) in used_values.iter().enumerate() {
            entries.push(serde_json::json!({
                "capacity_used": (used * tb).to_string(),
                "total_usable": (200 * tb).to_string(),
                "period_start_time": 1768089600 + i as i64 * 86400,
            }));
        }
        let history = Value::Array(entries);
        let proj = compute_projection(&history, 92 * tb, 200 * tb);
        if let Some(p) = proj {
            assert_eq!(p.confidence, ProjectionConfidence::Low);
        }
    }

    // ─── Alert threshold tests ─────────────────────────────────────

    #[test]
    fn test_onprem_warn_at_80_days() {
        let proj = CapacityProjection {
            days_until_full: Some(80),
            growth_rate_bytes_per_day: 1e12,
            confidence: ProjectionConfidence::High,
        };
        assert!(should_warn(
            &proj,
            &ClusterType::OnPrem(vec!["C192T".into()])
        ));
    }

    #[test]
    fn test_onprem_no_warn_at_100_days() {
        let proj = CapacityProjection {
            days_until_full: Some(100),
            growth_rate_bytes_per_day: 1e12,
            confidence: ProjectionConfidence::High,
        };
        assert!(!should_warn(
            &proj,
            &ClusterType::OnPrem(vec!["C192T".into()])
        ));
    }

    #[test]
    fn test_cloud_warn_at_5_days() {
        let proj = CapacityProjection {
            days_until_full: Some(5),
            growth_rate_bytes_per_day: 1e12,
            confidence: ProjectionConfidence::High,
        };
        assert!(should_warn(&proj, &ClusterType::CnqAws));
        assert!(should_warn(&proj, &ClusterType::AnqAzure));
    }

    #[test]
    fn test_cloud_no_warn_at_10_days() {
        let proj = CapacityProjection {
            days_until_full: Some(10),
            growth_rate_bytes_per_day: 1e12,
            confidence: ProjectionConfidence::High,
        };
        assert!(!should_warn(&proj, &ClusterType::CnqAws));
        assert!(!should_warn(&proj, &ClusterType::AnqAzure));
    }

    #[test]
    fn test_no_warn_when_no_days() {
        let proj = CapacityProjection {
            days_until_full: None,
            growth_rate_bytes_per_day: 0.0,
            confidence: ProjectionConfidence::High,
        };
        assert!(!should_warn(&proj, &ClusterType::OnPrem(vec![])));
    }

    // ─── Petabyte-scale tests ──────────────────────────────────────

    #[test]
    fn test_petabyte_scale_values() {
        let pb = 1_125_899_906_842_624_u64; // 1 PB
        let daily_growth = 1_099_511_627_776_u64; // 1 TB/day
        let mut entries = Vec::new();
        for i in 0..30 {
            entries.push(serde_json::json!({
                "capacity_used": (pb + i as u64 * daily_growth).to_string(),
                "total_usable": (2 * pb).to_string(),
                "period_start_time": 1768089600 + i * 86400,
            }));
        }
        let history = Value::Array(entries);
        let current = pb + 29 * daily_growth;
        let total = 2 * pb;

        let proj = compute_projection(&history, current, total).unwrap();
        assert!(proj.days_until_full.is_some());
        // Remaining ~= 1 PB - 29 TB, growth ~1 TB/day → ~995 days
        let days = proj.days_until_full.unwrap();
        assert!(days > 950 && days < 1050, "days_to_full={}", days);
    }

    // ─── Parse tests ───────────────────────────────────────────────

    #[test]
    fn test_parse_capacity_history() {
        let history = serde_json::json!([
            {"capacity_used": "1000", "total_usable": "5000", "period_start_time": 100},
            {"capacity_used": "2000", "total_usable": "5000", "period_start_time": 200},
            {"capacity_used": "3000", "total_usable": "5000", "period_start_time": 300},
        ]);
        let points = parse_capacity_history(&history);
        assert_eq!(points.len(), 3);
        assert!((points[0].0 - 0.0).abs() < f64::EPSILON);
        assert!((points[0].1 - 1000.0).abs() < f64::EPSILON);
        assert!((points[2].1 - 3000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_capacity_history_empty() {
        let history = serde_json::json!([]);
        let points = parse_capacity_history(&history);
        assert!(points.is_empty());
    }

    #[test]
    fn test_parse_capacity_history_not_array() {
        let history = serde_json::json!({"error": "bad"});
        let points = parse_capacity_history(&history);
        assert!(points.is_empty());
    }

    #[test]
    fn test_parse_total_usable() {
        let history = serde_json::json!([
            {"capacity_used": "1000", "total_usable": "99999"},
        ]);
        assert_eq!(parse_total_usable(&history), Some(99999));
    }

    // ─── Warning format tests ──────────────────────────────────────

    #[test]
    fn test_format_warning_onprem() {
        let proj = CapacityProjection {
            days_until_full: Some(62),
            growth_rate_bytes_per_day: 1.2 * 1_099_511_627_776.0,
            confidence: ProjectionConfidence::High,
        };
        let msg = format_warning(&proj, &ClusterType::OnPrem(vec!["C192T".into()]));
        assert!(msg.contains("62 days"));
        assert!(msg.contains("1.2 TB/day"));
    }

    #[test]
    fn test_format_warning_cloud() {
        let proj = CapacityProjection {
            days_until_full: Some(5),
            growth_rate_bytes_per_day: 1e12,
            confidence: ProjectionConfidence::High,
        };
        let msg = format_warning(&proj, &ClusterType::CnqAws);
        assert!(msg.contains("5 days"));
        assert!(msg.contains("capacity clamp"));
    }
}
