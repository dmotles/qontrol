pub mod cache;
pub mod capacity;
pub mod collector;
pub mod detection;
pub mod health;
pub mod json;
pub mod renderer;
pub mod types;

use std::thread;
use std::time::Duration;

use anyhow::Result;

use crate::config::Config;

/// Entry point for the `status` command.
pub fn run(
    config: &Config,
    profiles: &[String],
    json_mode: bool,
    watch: bool,
    interval: u64,
    no_cache: bool,
    timeout_secs: u64,
) -> Result<()> {
    loop {
        let status = collector::collect_all(config, profiles, timeout_secs, no_cache)?;

        if json_mode {
            let json_output = json::JsonOutput::from_status(&status);
            println!(
                "{}",
                serde_json::to_string_pretty(&json_output).unwrap_or_else(|_| "{}".to_string())
            );
        } else {
            print!("{}", renderer::render(&status));
        }

        if !watch {
            break;
        }

        thread::sleep(Duration::from_secs(interval));
        if !json_mode {
            print!("\x1B[2J\x1B[H");
        }
    }

    Ok(())
}
