use anyhow::Result;

use crate::config::{load_config, save_config, Config, ProfileEntry};

pub fn add(
    name: String,
    host: String,
    port: u16,
    token: String,
    insecure: bool,
    default: bool,
) -> Result<()> {
    let mut config = load_config()?;

    config.profiles.insert(
        name.clone(),
        ProfileEntry {
            host,
            port,
            token,
            insecure,
        },
    );

    if default || config.default_profile.is_none() {
        config.default_profile = Some(name.clone());
    }

    save_config(&config)?;
    println!("Profile '{}' added.", name);
    if config.default_profile.as_deref() == Some(&name) {
        println!("Set as default profile.");
    }
    Ok(())
}

pub fn list() -> Result<()> {
    let config = load_config()?;

    if config.profiles.is_empty() {
        println!("No profiles configured. Use `qontrol profile add` to create one.");
        return Ok(());
    }

    let default = config.default_profile.as_deref().unwrap_or("");

    for name in config.profiles.keys() {
        let marker = if name == default { " (default)" } else { "" };
        println!("  {}{}", name, marker);
    }
    Ok(())
}

pub fn remove(name: String) -> Result<()> {
    let mut config = load_config()?;

    if config.profiles.remove(&name).is_none() {
        anyhow::bail!("profile '{}' not found", name);
    }

    // Clear default if it was the removed profile
    if config.default_profile.as_deref() == Some(&name) {
        config.default_profile = None;
    }

    save_config(&config)?;
    println!("Profile '{}' removed.", name);
    Ok(())
}

pub fn show(name: Option<String>, config: &Config, json_mode: bool) -> Result<()> {
    let profile_name = match &name {
        Some(n) => n.as_str(),
        None => config
            .default_profile
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no profile specified and no default configured"))?,
    };

    let entry = config
        .profiles
        .get(profile_name)
        .ok_or_else(|| anyhow::anyhow!("profile '{}' not found", profile_name))?;

    if json_mode {
        let mut value = serde_json::to_value(entry)?;
        // Redact token in JSON output too
        if let Some(obj) = value.as_object_mut() {
            if let Some(token) = obj.get("token").and_then(|t| t.as_str()) {
                let redacted = redact_token(token);
                obj.insert("token".to_string(), serde_json::Value::String(redacted));
            }
            obj.insert(
                "name".to_string(),
                serde_json::Value::String(profile_name.to_string()),
            );
        }
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        let is_default = config.default_profile.as_deref() == Some(profile_name);
        println!(
            "Profile: {}{}",
            profile_name,
            if is_default { " (default)" } else { "" }
        );
        println!("  Host:     {}:{}", entry.host, entry.port);
        println!("  Token:    {}", redact_token(&entry.token));
        println!("  Insecure: {}", entry.insecure);
    }

    Ok(())
}

fn redact_token(token: &str) -> String {
    if token.len() <= 8 {
        return "****".to_string();
    }
    let visible = &token[token.len() - 8..];
    format!("****{}", visible)
}
