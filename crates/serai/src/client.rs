use std::{process::Stdio, time::Duration};

use anyhow::{anyhow, Context, Result};
use serde_json::Value;

use crate::config::{discover_port, serai_dir};

pub fn base_url() -> String {
    format!("http://127.0.0.1:{}", discover_port())
}

/// Makes sure the resident agent is up, spawning it if needed (ssh-agent style).
/// An agent from a different binary version is retired and respawned, so an
/// upgrade takes effect on the first command after it.
pub async fn ensure_agent() -> Result<()> {
    let client = reqwest::Client::new();
    if let Some(info) = ping_info(&client).await {
        if info["version"].as_str() == Some(env!("CARGO_PKG_VERSION")) {
            return Ok(());
        }
        stop(&client, &info).await?;
    }

    std::fs::create_dir_all(serai_dir())?;
    let log = std::fs::File::create(serai_dir().join("agent.log"))?;
    let exe = std::env::current_exe()?;
    std::process::Command::new(exe)
        .arg("agent")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .context("failed to spawn agent")?;

    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if ping_info(&client).await.is_some() {
            return Ok(());
        }
    }
    Err(anyhow!(
        "agent did not come up — check {}",
        serai_dir().join("agent.log").display()
    ))
}

/// The running agent's ping body, without spawning one.
pub async fn agent_info() -> Option<Value> {
    ping_info(&reqwest::Client::new()).await
}

/// Stops the running agent; Ok(false) when none was running.
pub async fn stop_agent() -> Result<bool> {
    let client = reqwest::Client::new();
    match ping_info(&client).await {
        Some(info) => stop(&client, &info).await.map(|_| true),
        None => Ok(false),
    }
}

/// Asks the agent to shut down gracefully, escalating to SIGTERM when it
/// predates /api/shutdown or does not go down in time.
async fn stop(client: &reqwest::Client, info: &Value) -> Result<()> {
    let graceful = client
        .post(format!("{}/api/shutdown", base_url()))
        .timeout(Duration::from_millis(500))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);
    if graceful && wait_gone(client).await {
        return Ok(());
    }
    // pid from ping, or the port owner for agents too old to report one
    let pid = match info["pid"].as_u64() {
        Some(pid) => pid.to_string(),
        None => {
            let out = std::process::Command::new("lsof")
                .args(["-ti", &format!("tcp:{}", discover_port())])
                .output()
                .context("failed to run lsof")?;
            let pid = String::from_utf8_lossy(&out.stdout)
                .lines()
                .next()
                .unwrap_or_default()
                .trim()
                .to_string();
            if pid.is_empty() {
                return Err(anyhow!("agent won't stop and its pid is unknown"));
            }
            pid
        }
    };
    std::process::Command::new("kill").args(["-TERM", &pid]).status()?;
    if wait_gone(client).await {
        return Ok(());
    }
    Err(anyhow!("agent (pid {pid}) did not stop"))
}

/// True when the agent stops answering within ~5s.
async fn wait_gone(client: &reqwest::Client) -> bool {
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if ping_info(client).await.is_none() {
            return true;
        }
    }
    false
}

/// The ping body when a serai agent for our SERAI_DIR answers at the
/// discovered port. Identity is checked so we never mistake an unrelated
/// local server for the agent.
async fn ping_info(client: &reqwest::Client) -> Option<Value> {
    let resp = client
        .get(format!("{}/api/ping", base_url()))
        .timeout(Duration::from_millis(500))
        .send()
        .await
        .ok()?;
    let body: Value = resp.json().await.ok()?;
    (body["serai"].as_bool() == Some(true)
        && body["dir"].as_str() == Some(&serai_dir().display().to_string()))
    .then_some(body)
}

pub async fn post(path: &str, body: Value) -> Result<Value> {
    ensure_agent().await?;
    let resp = reqwest::Client::new()
        .post(format!("{}{path}", base_url()))
        .json(&body)
        .send()
        .await?;
    read_json(resp).await
}

pub async fn get(path: &str) -> Result<Value> {
    ensure_agent().await?;
    let resp = reqwest::Client::new()
        .get(format!("{}{path}", base_url()))
        .send()
        .await?;
    read_json(resp).await
}

async fn read_json(resp: reqwest::Response) -> Result<Value> {
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(anyhow!("{text}"));
    }
    Ok(serde_json::from_str(&text)?)
}
