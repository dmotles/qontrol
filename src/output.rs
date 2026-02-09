use serde_json::Value;

/// Print a value as JSON or use the provided human formatter
pub fn print_value<F>(value: &Value, json_mode: bool, human_formatter: F)
where
    F: FnOnce(&Value),
{
    if json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        );
    } else {
        human_formatter(value);
    }
}

/// Print a JSON object as an aligned key-value table
#[allow(dead_code)]
pub fn print_kv_table(value: &Value) {
    if let Some(obj) = value.as_object() {
        let max_key_len = obj.keys().map(|k| k.len()).max().unwrap_or(0);
        for (key, val) in obj {
            let display = format_value(val);
            println!("{:width$}  {}", key, display, width = max_key_len);
        }
    }
}

/// Print a JSON array as a simple table with the given column names
pub fn print_table(items: &Value, columns: &[&str]) {
    let Some(arr) = items.as_array() else {
        return;
    };
    if arr.is_empty() {
        return;
    }

    // Calculate column widths (minimum = header length)
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for item in arr {
        for (i, col) in columns.iter().enumerate() {
            let val = format_value(item.get(*col).unwrap_or(&Value::Null));
            widths[i] = widths[i].max(val.len());
        }
    }

    // Print header
    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| format!("{:width$}", col.to_uppercase(), width = widths[i]))
        .collect();
    println!("{}", header.join("  "));

    // Print separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("  "));

    // Print rows
    for item in arr {
        let row: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let val = format_value(item.get(*col).unwrap_or(&Value::Null));
                format!("{:width$}", val, width = widths[i])
            })
            .collect();
        println!("{}", row.join("  "));
    }
}

/// Format a JSON value for human-readable display
fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_format_value_string() {
        assert_eq!(format_value(&json!("hello")), "hello");
    }

    #[test]
    fn test_format_value_number() {
        assert_eq!(format_value(&json!(42)), "42");
    }

    #[test]
    fn test_format_value_bool() {
        assert_eq!(format_value(&json!(true)), "true");
    }

    #[test]
    fn test_format_value_null() {
        assert_eq!(format_value(&json!(null)), "-");
    }

    #[test]
    fn test_print_value_json_mode() {
        // In JSON mode, should not call the human formatter
        let value = json!({"key": "value"});
        let mut called = false;
        // We can't easily capture stdout in a unit test, but we verify the formatter isn't called
        print_value(&value, true, |_| {
            called = true;
        });
        assert!(!called);
    }

    #[test]
    fn test_print_value_human_mode() {
        let value = json!({"key": "value"});
        let mut called = false;
        print_value(&value, false, |_| {
            called = true;
        });
        assert!(called);
    }
}
