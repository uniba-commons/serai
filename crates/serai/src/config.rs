use std::{fs, path::PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// "SERAI" in leet: S=5 E=3 R=2 A=4 I=1
pub const BASE_PORT: u16 = 53241;
/// how many consecutive ports the agent probes when the base is busy
pub const PORT_PROBE: u16 = 10;

pub fn env_port() -> Option<u16> {
    std::env::var("SERAI_PORT").ok().and_then(|p| p.parse().ok())
}

pub fn port_file() -> PathBuf {
    serai_dir().join("port")
}

/// The port the CLI should talk to: env override, then the port file
/// written by the running agent, then the base as a last guess.
pub fn discover_port() -> u16 {
    if let Some(p) = env_port() {
        return p;
    }
    if let Ok(s) = std::fs::read_to_string(port_file()) {
        if let Ok(p) = s.trim().parse() {
            return p;
        }
    }
    BASE_PORT
}

pub fn serai_dir() -> PathBuf {
    std::env::var("SERAI_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        dirs::home_dir().expect("home directory not found").join(".serai")
    })
}

/// Local ledger. Note: place *names* are deliberately not stored here —
/// a place's name lives inside the place itself (the `place/name` entry)
/// and is always read live from the synced data.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Config {
    /// namespace ids (hex) of joined places
    #[serde(default)]
    pub places: Vec<String>,
    /// namespace id (hex) of the place used most recently
    #[serde(default)]
    pub last_place: Option<String>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let path = serai_dir().join("config.json");
        if path.exists() {
            Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
        } else {
            Ok(Self::default())
        }
    }

    pub fn save(&self) -> Result<()> {
        fs::create_dir_all(serai_dir())?;
        fs::write(
            serai_dir().join("config.json"),
            serde_json::to_string_pretty(self)?,
        )?;
        Ok(())
    }
}
