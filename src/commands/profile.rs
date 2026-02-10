use anyhow::Result;
use chrono::Utc;

use crate::client::QumuloClient;
use crate::config::{load_config, save_config, Config, ProfileEntry};
use crate::error::QontrolError;

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

#[allow(clippy::too_many_arguments)]
pub fn add_interactive(
    name: String,
    cli_host: Option<String>,
    cli_port: u16,
    cli_insecure: bool,
    default: bool,
    timeout: u64,
    cli_username: Option<String>,
    cli_password: Option<String>,
    cli_expiry: &str,
) -> Result<()> {
    let non_interactive = cli_username.is_some() && cli_password.is_some();

    let (host, port, insecure) = if let Some(h) = cli_host {
        (h, cli_port, cli_insecure)
    } else {
        if non_interactive {
            anyhow::bail!("--host is required when using --username and --password");
        }
        let h: String = dialoguer::Input::new()
            .with_prompt("Cluster hostname")
            .interact_text()?;
        let p: u16 = dialoguer::Input::new()
            .with_prompt("Port")
            .default(8000u16)
            .interact_text()?;
        let ins = dialoguer::Confirm::new()
            .with_prompt("Skip TLS certificate verification?")
            .default(true)
            .interact()?;
        (h, p, ins)
    };

    let username = if let Some(u) = cli_username {
        u
    } else {
        dialoguer::Input::new()
            .with_prompt("Username")
            .interact_text()?
    };

    let password = if let Some(p) = cli_password {
        p
    } else {
        rpassword::prompt_password("Password: ")?
    };

    let (auth_id, session_token) =
        perform_login(&host, port, insecure, timeout, &username, &password)?;

    println!("Logged in as {} ({})", username, auth_id);

    if !non_interactive {
        println!();
        println!("To avoid storing your password, qontrol will create a long-lived API access");
        println!("token on the cluster. This token will be stored in your local profile for");
        println!("future CLI invocations.");
        println!();
    }

    let expiration_time = if non_interactive {
        parse_expiry(cli_expiry)?
    } else {
        // Prompt for expiry
        let expiry_items = ["6 months", "1 year", "Never (no expiration)"];
        let expiry_selection = dialoguer::Select::new()
            .with_prompt("Access token expiry")
            .items(&expiry_items)
            .default(1)
            .interact()?;

        match expiry_selection {
            0 => Some(Utc::now() + chrono::Duration::days(182)),
            1 => Some(Utc::now() + chrono::Duration::days(365)),
            2 => None,
            _ => unreachable!(),
        }
    };

    let expiration_str = expiration_time.map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string());

    // Create access token using the session token
    let access_token = create_access_token(
        &host,
        port,
        insecure,
        timeout,
        &session_token,
        &auth_id,
        expiration_str.as_deref(),
    )?;

    // Save profile
    let mut config = load_config()?;

    config.profiles.insert(
        name.clone(),
        ProfileEntry {
            host,
            port,
            token: access_token,
            insecure,
        },
    );

    if default || config.default_profile.is_none() {
        config.default_profile = Some(name.clone());
    }

    save_config(&config)?;

    let expiry_msg = match expiration_time {
        Some(dt) => format!("expires {}", dt.format("%Y-%m-%d")),
        None => "never expires".to_string(),
    };
    println!("Profile '{}' saved. Access token {}.", name, expiry_msg);
    if config.default_profile.as_deref() == Some(&name) {
        println!("Set as default profile.");
    }

    Ok(())
}

/// Authenticate with username/password and return (auth_id, session_token).
fn perform_login(
    host: &str,
    port: u16,
    insecure: bool,
    timeout: u64,
    username: &str,
    password: &str,
) -> Result<(String, String)> {
    // Login to get session token
    let client = QumuloClient::from_host(host, port, insecure, timeout, "")?;

    let login_body = serde_json::json!({
        "username": username,
        "password": password
    });

    let login_resp = match client.request_no_auth("POST", "/v1/session/login", Some(&login_body)) {
        Ok(resp) => resp,
        Err(e) => {
            if let Some(QontrolError::ApiError { status, .. }) = e.downcast_ref::<QontrolError>() {
                if *status == 401 {
                    anyhow::bail!("Invalid username or password.");
                }
            }
            return Err(e.context(format!(
                "Could not connect to {}:{}. Check hostname and port.",
                host, port
            )));
        }
    };

    let session_token = login_resp["bearer_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("login response missing bearer_token"))?
        .to_string();

    // Get user identity
    let session_client = QumuloClient::from_host(host, port, insecure, timeout, &session_token)?;
    let who = session_client.request("GET", "/v1/session/who-am-i", None)?;
    let auth_id = who["id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("who-am-i response missing id"))?
        .to_string();

    Ok((auth_id, session_token))
}

/// Create a long-lived access token using the session token.
fn create_access_token(
    host: &str,
    port: u16,
    insecure: bool,
    timeout: u64,
    session_token: &str,
    auth_id: &str,
    expiration_time: Option<&str>,
) -> Result<String> {
    let session_client = QumuloClient::from_host(host, port, insecure, timeout, session_token)?;

    let mut token_body = serde_json::json!({
        "user": {"auth_id": auth_id}
    });
    if let Some(exp) = expiration_time {
        token_body["expiration_time"] = serde_json::Value::String(exp.to_string());
    }

    let token_resp =
        match session_client.request("POST", "/v1/auth/access-tokens/", Some(&token_body)) {
            Ok(resp) => resp,
            Err(e) => {
                if let Some(QontrolError::ApiError { status, .. }) =
                    e.downcast_ref::<QontrolError>()
                {
                    if *status == 403 {
                        anyhow::bail!("User does not have permission to create access tokens.");
                    }
                }
                return Err(e);
            }
        };

    token_resp["bearer_token"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("access token response missing bearer_token"))
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

/// Parse an expiry string ("6months", "1year", "never") into an optional datetime.
fn parse_expiry(expiry: &str) -> Result<Option<chrono::DateTime<Utc>>> {
    match expiry {
        "6months" => Ok(Some(Utc::now() + chrono::Duration::days(182))),
        "1year" => Ok(Some(Utc::now() + chrono::Duration::days(365))),
        "never" => Ok(None),
        other => anyhow::bail!(
            "invalid --expiry value '{}': expected 6months, 1year, or never",
            other
        ),
    }
}

fn redact_token(token: &str) -> String {
    if token.len() <= 8 {
        return "****".to_string();
    }
    let visible = &token[token.len() - 8..];
    format!("****{}", visible)
}
