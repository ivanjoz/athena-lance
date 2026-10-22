//! Shared code for the spikes. `compaction` moves into the service in Phase 2.

pub mod compaction;

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use serde::de::DeserializeOwned;

/// Loads the repo's config.toml (`$LANCE_CONFIG`, or the first one found walking up
/// from the cwd) into `T`.
pub fn load_config<T: DeserializeOwned>() -> Result<T> {
    let path = match std::env::var_os("LANCE_CONFIG") {
        Some(p) => PathBuf::from(p),
        None => {
            let cwd = std::env::current_dir()?;
            cwd.ancestors()
                .map(|d| d.join("config.toml"))
                .find(|p| p.is_file())
                .ok_or_else(|| anyhow!("config.toml not found above {}", cwd.display()))?
        }
    };
    let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Exports the AWS profile and region for Lance's default credential chain.
/// Must run before any other thread starts.
pub fn set_aws_env(profile: &str, region: &str) {
    // SAFETY: called at the top of main, while the process is single-threaded.
    unsafe {
        if !profile.is_empty() {
            std::env::set_var("AWS_PROFILE", profile);
        }
        std::env::set_var("AWS_REGION", region);
    }
}
