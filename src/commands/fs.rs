use std::io::{self, IsTerminal, Write};

use anyhow::{Context, Result};
use console::Style;
use serde_json::{json, Value};

use crate::client::QumuloClient;
use crate::output::{format_value, print_value};

/// List directory contents with auto-pagination
pub fn ls(
    client: &QumuloClient,
    path: &str,
    long: bool,
    sort: &str,
    limit: Option<u32>,
    json_mode: bool,
) -> Result<()> {
    if json_mode {
        return ls_json(client, path, limit);
    }

    let is_tty = io::stderr().is_terminal();
    let mut total_count: u64 = 0;
    let mut after: Option<String> = None;
    let mut all_entries: Vec<Value> = Vec::new();
    let remaining_limit = limit;

    loop {
        let page_limit = remaining_limit.map(|l| {
            let fetched = total_count as u32;
            l.saturating_sub(fetched)
        });

        // If we've already hit the limit, stop
        if let Some(0) = page_limit {
            break;
        }

        let response = client
            .get_file_entries(path, after.as_deref(), page_limit)
            .with_context(|| format!("failed to list directory: {}", path))?;

        let entries = response
            .get("files")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        total_count += entries.len() as u64;
        all_entries.extend(entries);

        // Show progress on stderr for TTY
        if is_tty && total_count > 0 {
            eprint!("\r\x1b[K(loading... {} entries)", total_count);
            io::stderr().flush().ok();
        }

        // Check if there's a next page
        let has_next = response
            .get("paging")
            .and_then(|p| p.get("next"))
            .and_then(|n| n.as_str())
            .is_some_and(|n| !n.is_empty());

        if !has_next {
            break;
        }

        // Check if we've hit the limit
        if let Some(l) = remaining_limit {
            if total_count >= l as u64 {
                break;
            }
        }

        after = response
            .get("paging")
            .and_then(|p| p.get("next"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string());
    }

    // Clear progress line
    if is_tty && total_count > 0 {
        eprint!("\r\x1b[K");
        io::stderr().flush().ok();
    }

    if all_entries.is_empty() {
        println!("(empty directory)");
        return Ok(());
    }

    sort_entries(&mut all_entries, sort);

    if long {
        print_long_listing(&all_entries);
    } else {
        print_short_listing(&all_entries);
    }

    // Show summary count on stderr
    eprintln!("{} entries", total_count);

    Ok(())
}

/// JSON mode: collect all pages into a single JSON array response
fn ls_json(client: &QumuloClient, path: &str, limit: Option<u32>) -> Result<()> {
    let mut all_files: Vec<Value> = Vec::new();
    let mut after: Option<String> = None;
    let mut last_response: Option<Value> = None;

    loop {
        let page_limit = limit.map(|l| {
            let fetched = all_files.len() as u32;
            l.saturating_sub(fetched)
        });

        if let Some(0) = page_limit {
            break;
        }

        let response = client
            .get_file_entries(path, after.as_deref(), page_limit)
            .with_context(|| format!("failed to list directory: {}", path))?;

        if let Some(files) = response.get("files").and_then(|v| v.as_array()) {
            all_files.extend(files.iter().cloned());
        }

        let has_next = response
            .get("paging")
            .and_then(|p| p.get("next"))
            .and_then(|n| n.as_str())
            .is_some_and(|n| !n.is_empty());

        last_response = Some(response);

        if !has_next {
            break;
        }

        if let Some(l) = limit {
            if all_files.len() >= l as usize {
                break;
            }
        }

        after = last_response
            .as_ref()
            .and_then(|r| r.get("paging"))
            .and_then(|p| p.get("next"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string());
    }

    // Build combined response using structure from last response
    let mut result = last_response.unwrap_or_else(|| json!({}));
    result["files"] = Value::Array(all_files);
    // Clear paging since we've fetched everything
    result["paging"] = json!({"next": ""});

    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string())
    );
    Ok(())
}

/// Show recursive directory tree
pub fn tree(client: &QumuloClient, path: &str, max_depth: u32, json_mode: bool) -> Result<()> {
    if json_mode {
        let mut result = json!({
            "path": path,
            "max_depth": max_depth,
        });

        let tree_data = build_tree_json(client, path, max_depth, 0)?;
        result["tree"] = tree_data;

        // Get aggregates for the root
        if let Ok(aggregates) = client.get_file_recursive_aggregates(path) {
            result["aggregates"] = aggregates;
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string())
        );
        return Ok(());
    }

    // Human-readable tree output
    let dir_style = Style::new().blue().bold();
    println!("{}", dir_style.apply_to(path));

    print_tree_recursive(client, path, max_depth, 0, "")?;

    // Show aggregated sizes at the bottom
    if let Ok(aggregates) = client.get_file_recursive_aggregates(path) {
        println!();
        print_aggregates_summary(&aggregates);
    }

    Ok(())
}

/// Show detailed file/directory attributes
pub fn stat(client: &QumuloClient, path: &str, json_mode: bool) -> Result<()> {
    let attrs = client
        .get_file_attr(path)
        .with_context(|| format!("failed to get attributes: {}", path))?;

    print_value(&attrs, json_mode, |val| {
        print_stat_human(val, path);
    });

    Ok(())
}

// --- Internal helpers ---

fn sort_entries(entries: &mut [Value], sort: &str) {
    match sort {
        "size" => {
            entries.sort_by(|a, b| {
                let size_a = a
                    .get("size")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let size_b = b
                    .get("size")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                size_b.cmp(&size_a) // largest first
            });
        }
        "type" => {
            entries.sort_by(|a, b| {
                let type_a = a.get("type").and_then(|v| v.as_str()).unwrap_or("");
                let type_b = b.get("type").and_then(|v| v.as_str()).unwrap_or("");
                // Directories first, then files
                type_a.cmp(type_b)
            });
        }
        _ => {
            // Default: sort by name
            entries.sort_by(|a, b| {
                let name_a = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let name_b = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                name_a.to_lowercase().cmp(&name_b.to_lowercase())
            });
        }
    }
}

fn print_short_listing(entries: &[Value]) {
    let dir_style = Style::new().blue().bold();
    let file_style = Style::new();
    let symlink_style = Style::new().cyan();

    for entry in entries {
        let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let entry_type = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("FS_FILE_TYPE_FILE");

        let styled_name = match entry_type {
            "FS_FILE_TYPE_DIRECTORY" => dir_style.apply_to(name).to_string(),
            "FS_FILE_TYPE_SYMLINK" => symlink_style.apply_to(name).to_string(),
            _ => file_style.apply_to(name).to_string(),
        };

        println!("{}", styled_name);
    }
}

fn print_long_listing(entries: &[Value]) {
    let dir_style = Style::new().blue().bold();
    let file_style = Style::new();
    let symlink_style = Style::new().cyan();
    let size_style = Style::new().green();

    // Calculate column widths
    let mut max_size_len = 4; // "SIZE"
    let mut max_owner_len = 5; // "OWNER"
    let mut max_id_len = 2; // "ID"

    for entry in entries {
        let size_str = format_size(
            entry
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0),
        );
        max_size_len = max_size_len.max(size_str.len());

        let owner = entry.get("owner").and_then(|v| v.as_str()).unwrap_or("-");
        max_owner_len = max_owner_len.max(owner.len());

        let id = format_value(entry.get("id").unwrap_or(&Value::Null));
        max_id_len = max_id_len.max(id.len());
    }

    // Header
    println!(
        "{:>id_w$}  {:<4}  {:>size_w$}  {:<owner_w$}  {:<19}  NAME",
        "ID",
        "TYPE",
        "SIZE",
        "OWNER",
        "MODIFIED",
        id_w = max_id_len,
        size_w = max_size_len,
        owner_w = max_owner_len,
    );
    println!(
        "{:->id_w$}  {:-<4}  {:->size_w$}  {:-<owner_w$}  {:-<19}  ----",
        "",
        "",
        "",
        "",
        "",
        id_w = max_id_len,
        size_w = max_size_len,
        owner_w = max_owner_len,
    );

    for entry in entries {
        let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let entry_type = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("FS_FILE_TYPE_FILE");
        let size = entry
            .get("size")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let owner = entry.get("owner").and_then(|v| v.as_str()).unwrap_or("-");
        let id = format_value(entry.get("id").unwrap_or(&Value::Null));
        let modified = entry
            .get("modification_time")
            .and_then(|v| v.as_str())
            .map(truncate_timestamp)
            .unwrap_or_else(|| "-".to_string());

        let type_abbrev = match entry_type {
            "FS_FILE_TYPE_DIRECTORY" => "DIR ",
            "FS_FILE_TYPE_SYMLINK" => "LINK",
            _ => "FILE",
        };

        let size_str = format_size(size);

        let styled_name = match entry_type {
            "FS_FILE_TYPE_DIRECTORY" => dir_style.apply_to(name).to_string(),
            "FS_FILE_TYPE_SYMLINK" => symlink_style.apply_to(name).to_string(),
            _ => file_style.apply_to(name).to_string(),
        };

        println!(
            "{:>id_w$}  {}  {}  {:<owner_w$}  {:<19}  {}",
            id,
            type_abbrev,
            size_style.apply_to(format!("{:>size_w$}", size_str, size_w = max_size_len)),
            owner,
            modified,
            styled_name,
            id_w = max_id_len,
            owner_w = max_owner_len,
        );
    }
}

fn print_tree_recursive(
    client: &QumuloClient,
    path: &str,
    max_depth: u32,
    current_depth: u32,
    prefix: &str,
) -> Result<()> {
    if current_depth >= max_depth {
        return Ok(());
    }

    let entries = client
        .get_all_file_entries(path)
        .with_context(|| format!("failed to list directory: {}", path))?;

    let mut sorted_entries = entries;
    sort_entries(&mut sorted_entries, "name");

    let dir_style = Style::new().blue().bold();
    let symlink_style = Style::new().cyan();

    let total = sorted_entries.len();
    for (i, entry) in sorted_entries.iter().enumerate() {
        let is_last = i == total - 1;
        let connector = if is_last {
            "\u{2514}\u{2500}\u{2500} "
        } else {
            "\u{251c}\u{2500}\u{2500} "
        };
        let child_prefix = if is_last { "    " } else { "\u{2502}   " };

        let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let entry_type = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("FS_FILE_TYPE_FILE");

        let styled_name = match entry_type {
            "FS_FILE_TYPE_DIRECTORY" => dir_style.apply_to(name).to_string(),
            "FS_FILE_TYPE_SYMLINK" => symlink_style.apply_to(name).to_string(),
            _ => name.to_string(),
        };

        let size_info = if entry_type != "FS_FILE_TYPE_DIRECTORY" {
            let size = entry
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            format!("  [{}]", format_size(size))
        } else {
            String::new()
        };

        println!("{}{}{}{}", prefix, connector, styled_name, size_info);

        if entry_type == "FS_FILE_TYPE_DIRECTORY" {
            let child_path = if path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", path, name)
            };
            let new_prefix = format!("{}{}", prefix, child_prefix);
            print_tree_recursive(
                client,
                &child_path,
                max_depth,
                current_depth + 1,
                &new_prefix,
            )?;
        }
    }

    Ok(())
}

fn build_tree_json(
    client: &QumuloClient,
    path: &str,
    max_depth: u32,
    current_depth: u32,
) -> Result<Value> {
    let entries = client
        .get_all_file_entries(path)
        .with_context(|| format!("failed to list directory: {}", path))?;

    let mut result = Vec::new();
    for entry in &entries {
        let mut node = entry.clone();
        let entry_type = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("FS_FILE_TYPE_FILE");
        let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("?");

        if entry_type == "FS_FILE_TYPE_DIRECTORY" && current_depth + 1 < max_depth {
            let child_path = if path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", path, name)
            };
            if let Ok(children) = build_tree_json(client, &child_path, max_depth, current_depth + 1)
            {
                node["children"] = children;
            }
        }
        result.push(node);
    }

    Ok(Value::Array(result))
}

fn print_stat_human(attrs: &Value, path: &str) {
    let header_style = Style::new().bold();

    println!("{}", header_style.apply_to(format!("  File: {}", path)));

    if let Some(obj) = attrs.as_object() {
        let fields = [
            ("id", "    ID"),
            ("type", "  Type"),
            ("size", "  Size"),
            ("owner", " Owner"),
            ("group", " Group"),
            ("mode", "  Mode"),
            ("creation_time", "Created"),
            ("modification_time", "Modified"),
            ("change_time", "Changed"),
            ("access_time", "Access"),
            ("child_count", "Children"),
            ("num_links", " Links"),
        ];

        for (key, label) in &fields {
            if let Some(val) = obj.get(*key) {
                let display = if *key == "size" {
                    let raw = format_value(val);
                    if let Ok(bytes) = raw.parse::<u64>() {
                        format!("{} ({})", raw, format_size(bytes))
                    } else {
                        raw
                    }
                } else {
                    format_value(val)
                };
                println!("{}: {}", label, display);
            }
        }

        // Print any remaining fields not in the known list
        let known_keys: Vec<&str> = fields.iter().map(|(k, _)| *k).collect();
        for (key, val) in obj {
            if !known_keys.contains(&key.as_str()) {
                let label = format!("{:>8}", key);
                println!("{}: {}", label, format_value(val));
            }
        }
    }
}

fn print_aggregates_summary(aggregates: &Value) {
    let dim_style = Style::new().dim();

    let total_files = aggregates
        .get("total_files")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let total_directories = aggregates
        .get("total_directories")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let total_data = aggregates
        .get("total_data")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok());
    let total_named_stream_data = aggregates
        .get("total_named_stream_data")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok());

    let size_str = total_data
        .map(format_size)
        .unwrap_or_else(|| "-".to_string());

    println!(
        "{}",
        dim_style.apply_to(format!(
            "{} files, {} directories, {} total",
            total_files, total_directories, size_str
        ))
    );

    if let Some(stream_bytes) = total_named_stream_data {
        if stream_bytes > 0 {
            println!(
                "{}",
                dim_style.apply_to(format!("named stream data: {}", format_size(stream_bytes)))
            );
        }
    }
}

/// Format bytes as human-readable size (e.g. "1.5 GiB")
fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Truncate ISO timestamp to "YYYY-MM-DD HH:MM:SS"
fn truncate_timestamp(ts: &str) -> String {
    // Qumulo timestamps are like "2024-01-15T10:30:45.123456Z"
    ts.replace('T', " ").chars().take(19).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
    }

    #[test]
    fn test_format_size_kib() {
        assert_eq!(format_size(1024), "1.0 KiB");
        assert_eq!(format_size(1536), "1.5 KiB");
    }

    #[test]
    fn test_format_size_mib() {
        assert_eq!(format_size(1048576), "1.0 MiB");
    }

    #[test]
    fn test_format_size_gib() {
        assert_eq!(format_size(1073741824), "1.0 GiB");
    }

    #[test]
    fn test_format_size_tib() {
        assert_eq!(format_size(1099511627776), "1.0 TiB");
    }

    #[test]
    fn test_truncate_timestamp() {
        assert_eq!(
            truncate_timestamp("2024-01-15T10:30:45.123456Z"),
            "2024-01-15 10:30:45"
        );
    }

    #[test]
    fn test_truncate_timestamp_short() {
        assert_eq!(truncate_timestamp("2024-01-15T10:30"), "2024-01-15 10:30");
    }

    #[test]
    fn test_sort_entries_by_name() {
        let mut entries = vec![
            json!({"name": "zebra", "type": "FS_FILE_TYPE_FILE"}),
            json!({"name": "apple", "type": "FS_FILE_TYPE_FILE"}),
            json!({"name": "Mango", "type": "FS_FILE_TYPE_FILE"}),
        ];
        sort_entries(&mut entries, "name");
        assert_eq!(entries[0]["name"], "apple");
        assert_eq!(entries[1]["name"], "Mango");
        assert_eq!(entries[2]["name"], "zebra");
    }

    #[test]
    fn test_sort_entries_by_size() {
        let mut entries = vec![
            json!({"name": "small", "size": "100"}),
            json!({"name": "big", "size": "9999"}),
            json!({"name": "medium", "size": "500"}),
        ];
        sort_entries(&mut entries, "size");
        assert_eq!(entries[0]["name"], "big");
        assert_eq!(entries[1]["name"], "medium");
        assert_eq!(entries[2]["name"], "small");
    }

    #[test]
    fn test_sort_entries_by_type() {
        let mut entries = vec![
            json!({"name": "file1", "type": "FS_FILE_TYPE_FILE"}),
            json!({"name": "dir1", "type": "FS_FILE_TYPE_DIRECTORY"}),
            json!({"name": "link1", "type": "FS_FILE_TYPE_SYMLINK"}),
        ];
        sort_entries(&mut entries, "type");
        assert_eq!(entries[0]["name"], "dir1");
        assert_eq!(entries[1]["name"], "file1");
        assert_eq!(entries[2]["name"], "link1");
    }
}
