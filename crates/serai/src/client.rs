use std::{process::Stdio, time::Duration};

use anyhow::{anyhow, Context, Result};
use serde_json::Value;

use crate::config::{discover_port, serai_dir};

pub fn base_url() -> String {
    format!("http://127.0.0.1:{}", discover_port())
}

/// Makes sure the resident agent is up, spawning it if needed (ssh-agent style).
pub async fn ensure_agent() -> Result<()> {
    let client = reqwest::Client::new();
    if ping(&client).await {
        return Ok(());
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
        if ping(&client).await {
            return Ok(());
        }
    }
    Err(anyhow!(
        "agent did not come up — check {}",
        serai_dir().join("agent.log").display()
    ))
}

/// True when a serai agent for our SERAI_DIR answers at the discovered port.
/// Identity is checked so we never mistake an unrelated local server for the agent.
async fn ping(client: &reqwest::Client) -> bool {
    let Ok(resp) = client
        .get(format!("{}/api/ping", base_url()))
        .timeout(Duration::from_millis(500))
        .send()
        .await
    else {
        return false;
    };
    let Ok(body) = resp.json::<serde_json::Value>().await else {
        return false;
    };
    body["serai"].as_bool() == Some(true)
        && body["dir"].as_str() == Some(&serai_dir().display().to_string())
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
