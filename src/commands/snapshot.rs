use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate};
use serde_json::Value;

use crate::client::QumuloClient;
use crate::output::{print_table, print_value};

/// Format bytes into human-readable size
fn format_bytes(bytes_str: &str) -> String {
    let bytes: u64 = bytes_str.parse().unwrap_or(0);
    if bytes == 0 {
        return "0 B".to_string();
    }
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < units.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.1} {}", size, units[unit_idx])
    }
}

pub fn list(client: &QumuloClient, json_mode: bool) -> Result<()> {
    let status = client.get_snapshots()?;
    let capacity = client.get_snapshot_capacity_per_snapshot()?;

    // Build capacity lookup: id -> capacity_used_bytes
    let mut cap_map: HashMap<u64, String> = HashMap::new();
    if let Some(entries) = capacity.get("entries").and_then(|v| v.as_array()) {
        for entry in entries {
            if let (Some(id), Some(bytes)) = (
                entry.get("id").and_then(|v| v.as_u64()),
                entry.get("capacity_used_bytes").and_then(|v| v.as_str()),
            ) {
                cap_map.insert(id, bytes.to_string());
            }
        }
    }

    if json_mode {
        // Enrich entries with capacity info
        let mut result = status.clone();
        if let Some(entries) = result.get_mut("entries").and_then(|v| v.as_array_mut()) {
            for entry in entries.iter_mut() {
                if let Some(id) = entry.get("id").and_then(|v| v.as_u64()) {
                    if let Some(bytes) = cap_map.get(&id) {
                        entry.as_object_mut().unwrap().insert(
                            "capacity_used_bytes".to_string(),
                            Value::String(bytes.clone()),
                        );
                    }
                }
            }
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string())
        );
        return Ok(());
    }

    let entries = status
        .get("entries")
        .and_then(|v| v.as_array())
        .context("unexpected response: missing entries")?;

    if entries.is_empty() {
        println!("No snapshots found.");
        return Ok(());
    }

    // Build enriched entries for display
    let enriched: Vec<Value> = entries
        .iter()
        .map(|entry| {
            let mut e = entry.clone();
            let id = entry.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            let bytes = cap_map.get(&id).map(|s| s.as_str()).unwrap_or("0");
            e.as_object_mut()
                .unwrap()
                .insert("capacity".to_string(), Value::String(format_bytes(bytes)));
            e
        })
        .collect();

    let arr = Value::Array(enriched);
    print_value(&arr, false, |val| {
        print_table(
            val,
            &["id", "name", "timestamp", "directory_name", "capacity"],
        );
    });

    Ok(())
}

pub fn show(client: &QumuloClient, id: u64, json_mode: bool) -> Result<()> {
    let snap = client.get_snapshot(id)?;

    if json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(&snap).unwrap_or_else(|_| snap.to_string())
        );
        return Ok(());
    }

    let name = snap.get("name").and_then(|v| v.as_str()).unwrap_or("-");
    let timestamp = snap
        .get("timestamp")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let source = snap
        .get("source_file_id")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let dir_name = snap
        .get("directory_name")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let created_by_policy = snap
        .get("created_by_policy")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let expiration = snap
        .get("expiration")
        .map(|v| {
            if v.is_null() || v.as_str().is_some_and(|s| s.is_empty()) {
                "never".to_string()
            } else {
                v.as_str().unwrap_or("-").to_string()
            }
        })
        .unwrap_or_else(|| "-".to_string());
    let in_delete = snap
        .get("in_delete")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    println!("Snapshot {}", id);
    println!("  Name:        {}", name);
    println!("  Created:     {}", timestamp);
    println!("  Source ID:   {}", source);
    println!("  Directory:   {}", dir_name);
    println!(
        "  Policy:      {}",
        if created_by_policy {
            "policy"
        } else {
            "manual"
        }
    );
    println!("  Expiration:  {}", expiration);
    println!("  Deleting:    {}", in_delete);

    Ok(())
}

pub fn policies(client: &QumuloClient, json_mode: bool) -> Result<()> {
    let policies = client.get_snapshot_policies()?;

    if json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(&policies).unwrap_or_else(|_| policies.to_string())
        );
        return Ok(());
    }

    let entries = policies
        .get("entries")
        .and_then(|v| v.as_array())
        .context("unexpected response: missing entries")?;

    if entries.is_empty() {
        println!("No snapshot policies found.");
        return Ok(());
    }

    // Build display entries with enabled status string
    let display: Vec<Value> = entries
        .iter()
        .map(|entry| {
            let mut e = entry.clone();
            let enabled = entry
                .get("enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            e.as_object_mut().unwrap().insert(
                "status".to_string(),
                Value::String(if enabled { "enabled" } else { "disabled" }.to_string()),
            );
            // Extract schedule TTL for display
            let ttl = entry
                .get("schedule")
                .and_then(|s| s.get("expiration_time_to_live"))
                .and_then(|v| v.as_str())
                .unwrap_or("never");
            e.as_object_mut()
                .unwrap()
                .insert("ttl".to_string(), Value::String(ttl.to_string()));
            e
        })
        .collect();

    let arr = Value::Array(display);
    print_value(&arr, false, |val| {
        print_table(
            val,
            &["id", "policy_name", "status", "source_file_id", "ttl"],
        );
    });

    Ok(())
}

pub fn recommend_delete(
    client: &QumuloClient,
    keep_daily: u32,
    keep_weekly: u32,
    keep_monthly: u32,
    json_mode: bool,
) -> Result<()> {
    let status = client.get_snapshots()?;

    let entries = status
        .get("entries")
        .and_then(|v| v.as_array())
        .context("unexpected response: missing entries")?;

    if entries.is_empty() {
        println!("No snapshots found.");
        return Ok(());
    }

    // Parse snapshot timestamps and sort by date descending
    let mut snapshots: Vec<(u64, String, NaiveDate)> = Vec::new();
    for entry in entries {
        let id = match entry.get("id").and_then(|v| v.as_u64()) {
            Some(id) => id,
            None => continue,
        };
        // Skip snapshots being deleted
        if entry
            .get("in_delete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            continue;
        }
        let ts = entry
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let name = entry
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        // Parse RFC3339 timestamp to NaiveDate
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
            snapshots.push((id, name, dt.date_naive()));
        }
    }

    snapshots.sort_by(|a, b| b.2.cmp(&a.2));

    // GFS retention: keep the most recent snapshot per day/week/month
    let mut keep_set: std::collections::HashSet<u64> = std::collections::HashSet::new();

    // Daily: keep most recent snapshot per calendar day (newest N days)
    let mut daily_days: Vec<NaiveDate> = snapshots.iter().map(|s| s.2).collect();
    daily_days.dedup();
    for day in daily_days.iter().take(keep_daily as usize) {
        if let Some(snap) = snapshots.iter().find(|s| s.2 == *day) {
            keep_set.insert(snap.0);
        }
    }

    // Weekly: keep most recent snapshot per ISO week (newest N weeks)
    let mut weekly_seen: Vec<(i32, u32)> = Vec::new();
    for snap in &snapshots {
        let iso = snap.2.iso_week();
        let key = (iso.year(), iso.week());
        if !weekly_seen.contains(&key) {
            weekly_seen.push(key);
            if weekly_seen.len() <= keep_weekly as usize {
                keep_set.insert(snap.0);
            }
        }
    }

    // Monthly: keep most recent snapshot per year-month (newest N months)
    let mut monthly_seen: Vec<(i32, u32)> = Vec::new();
    for snap in &snapshots {
        let key = (snap.2.year(), snap.2.month());
        if !monthly_seen.contains(&key) {
            monthly_seen.push(key);
            if monthly_seen.len() <= keep_monthly as usize {
                keep_set.insert(snap.0);
            }
        }
    }

    let deletable: Vec<&(u64, String, NaiveDate)> = snapshots
        .iter()
        .filter(|s| !keep_set.contains(&s.0))
        .collect();

    if json_mode {
        let deletable_ids: Vec<u64> = deletable.iter().map(|s| s.0).collect();

        // Calculate space savings
        let savings = if !deletable_ids.is_empty() {
            client.calculate_snapshot_capacity(&deletable_ids)?
        } else {
            serde_json::json!({"bytes": "0"})
        };

        let result = serde_json::json!({
            "keep_daily": keep_daily,
            "keep_weekly": keep_weekly,
            "keep_monthly": keep_monthly,
            "total_snapshots": snapshots.len(),
            "keep_count": keep_set.len(),
            "delete_count": deletable.len(),
            "deletable_ids": deletable_ids,
            "estimated_savings_bytes": savings.get("bytes").and_then(|v| v.as_str()).unwrap_or("0"),
        });

        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string())
        );
        return Ok(());
    }

    println!(
        "GFS retention: keep-daily={}, keep-weekly={}, keep-monthly={}",
        keep_daily, keep_weekly, keep_monthly
    );
    println!(
        "Total snapshots: {}, Keep: {}, Delete: {}",
        snapshots.len(),
        keep_set.len(),
        deletable.len()
    );
    println!();

    if deletable.is_empty() {
        println!("No snapshots recommended for deletion.");
        return Ok(());
    }

    // Calculate space savings
    let deletable_ids: Vec<u64> = deletable.iter().map(|s| s.0).collect();
    let savings = client.calculate_snapshot_capacity(&deletable_ids)?;
    let savings_bytes = savings.get("bytes").and_then(|v| v.as_str()).unwrap_or("0");

    println!("Estimated space savings: {}", format_bytes(savings_bytes));
    println!();

    println!("Deletable snapshots:");
    let deletable_entries: Vec<Value> = deletable
        .iter()
        .map(|s| {
            serde_json::json!({
                "id": s.0,
                "name": s.1,
                "date": s.2.to_string(),
            })
        })
        .collect();
    let arr = Value::Array(deletable_entries);
    print_value(&arr, false, |val| {
        print_table(val, &["id", "name", "date"]);
    });

    Ok(())
}

pub fn diff(client: &QumuloClient, newer: u64, older: u64, json_mode: bool) -> Result<()> {
    let changes = client.get_snapshot_diff(newer, older)?;

    if json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(&changes).unwrap_or_else(|_| changes.to_string())
        );
        return Ok(());
    }

    let entries = changes
        .get("entries")
        .and_then(|v| v.as_array())
        .context("unexpected response: missing entries")?;

    if entries.is_empty() {
        println!("No changes between snapshot {} and {}.", newer, older);
        return Ok(());
    }

    println!(
        "Changes between snapshot {} (newer) and {} (older):",
        newer, older
    );
    println!();

    print_value(&Value::Array(entries.clone()), false, |val| {
        print_table(val, &["op", "path"]);
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes_zero() {
        assert_eq!(format_bytes("0"), "0 B");
    }

    #[test]
    fn test_format_bytes_small() {
        assert_eq!(format_bytes("512"), "512 B");
    }

    #[test]
    fn test_format_bytes_kib() {
        assert_eq!(format_bytes("1024"), "1.0 KiB");
    }

    #[test]
    fn test_format_bytes_gib() {
        assert_eq!(format_bytes("1073741824"), "1.0 GiB");
    }

    #[test]
    fn test_format_bytes_tib() {
        // 1 TiB = 1099511627776
        assert_eq!(format_bytes("1099511627776"), "1.0 TiB");
    }

    #[test]
    fn test_format_bytes_invalid() {
        assert_eq!(format_bytes("not_a_number"), "0 B");
    }
}
