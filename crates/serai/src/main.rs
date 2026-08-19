mod agent;
mod client;
mod config;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "serai",
    about = "an ownerless courtyard where traveling makers spread their artifacts"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Open a new serai (prints its ticket)
    New {
        /// name of the serai (a caravan-flavored one is generated when omitted)
        name: Option<String>,
    },
    /// Spread an artifact in the courtyard
    Spread {
        path: PathBuf,
        /// tag — a one-line note attached to the artifact
        #[arg(short = 'm', long)]
        tag: Option<String>,
        /// serai name, or a ticket (paste a ticket here on first contact)
        serai: Option<String>,
    },
    /// List artifacts in the courtyard you are staying at
    Artifacts,
    /// Move to a serai — or without arguments, see where you are staying
    Stay { serai: Option<String> },
    /// Take an artifact home as a local file
    Take {
        /// filename or artifact id
        target: String,
        /// output path (defaults to the current directory)
        #[arg(short, long)]
        output: Option<PathBuf>,
        serai: Option<String>,
    },
    /// View the courtyard in your browser
    View { serai: Option<String> },
    /// Resident agent (normally spawned automatically)
    Agent {
        #[command(subcommand)]
        cmd: Option<AgentCmd>,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
    /// Show the resident agent's status
    Status,
    /// Stop the resident agent
    Stop,
    /// Restart the resident agent (picks up a freshly installed binary)
    Restart,
}

/// A caravan-flavored name for a serai opened without one.
fn generate_name(taken: &[String]) -> String {
    const ADJ: &[&str] = &[
        "quiet", "amber", "wandering", "distant", "hidden", "golden", "midnight", "dusty",
        "patient", "swift", "silver", "indigo", "dawn", "cool", "spice", "lone",
    ];
    const NOUN: &[&str] = &[
        "oasis", "dune", "mirage", "palm", "well", "moon", "lantern", "compass", "saddle",
        "date", "tent", "star", "wind", "sand", "caravan", "gate",
    ];
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0) as usize;
    for i in 0..ADJ.len() * NOUN.len() {
        let n = seed.wrapping_add(i.wrapping_mul(2654435761));
        let name = format!("{}-{}", ADJ[n % ADJ.len()], NOUN[(n / 251) % NOUN.len()]);
        if !taken.contains(&name) {
            return name;
        }
    }
    // all 256 combinations taken — salute the traveler and disambiguate by number
    format!("caravan-{}", seed % 10000)
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Agent { cmd: None } => agent::run().await,
        Cmd::Agent { cmd: Some(AgentCmd::Status) } => {
            match client::agent_info().await {
                Some(info) => {
                    let version = info["version"].as_str().unwrap_or("pre-0.3.0");
                    let pid = info["pid"]
                        .as_u64()
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "?".into());
                    println!("agent running (version {version})");
                    println!("    pid   {pid}");
                    println!("    url   {}", client::base_url());
                }
                None => println!("no agent running"),
            }
            Ok(())
        }
        Cmd::Agent { cmd: Some(AgentCmd::Stop) } => {
            if client::stop_agent().await? {
                println!("agent stopped");
            } else {
                println!("no agent running");
            }
            Ok(())
        }
        Cmd::Agent { cmd: Some(AgentCmd::Restart) } => {
            client::stop_agent().await?;
            client::ensure_agent().await?;
            let version = client::agent_info()
                .await
                .and_then(|i| i["version"].as_str().map(str::to_string))
                .unwrap_or_else(|| "?".into());
            println!("agent restarted (version {version})");
            Ok(())
        }
        Cmd::New { name } => {
            let name = match name {
                Some(name) => name,
                None => {
                    let res = client::get("/api/places").await?;
                    let taken: Vec<String> = res["places"]
                        .as_array()
                        .map(|ps| {
                            ps.iter()
                                .filter_map(|p| p["name"].as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default();
                    generate_name(&taken)
                }
            };
            let res = client::post("/api/serai", serde_json::json!({ "name": name })).await?;
            println!("opened serai \"{}\"", res["name"].as_str().unwrap_or(""));
            println!("hand this ticket to your companions:\n");
            println!("  {}\n", res["ticket"].as_str().unwrap_or(""));
            Ok(())
        }
        Cmd::Spread { path, tag, serai } => {
            let path = path.canonicalize()?;
            let res = client::post(
                "/api/spread",
                serde_json::json!({ "path": path, "tag": tag, "serai": serai }),
            )
            .await?;
            println!(
                "spread {} at {} ({} bytes)",
                res["filename"].as_str().unwrap_or("?"),
                res["serai"].as_str().unwrap_or("?"),
                res["size"]
            );
            Ok(())
        }
        Cmd::Artifacts => {
            let res = client::get("/api/artifacts").await?;
            println!("── {} ──", res["serai"].as_str().unwrap_or(""));
            let empty = Vec::new();
            for a in res["artifacts"].as_array().unwrap_or(&empty) {
                let state = a["state"].as_str().unwrap_or("pending");
                if state == "pending" {
                    match a["id"].as_u64() {
                        Some(id) => println!("  (pending… · id {id})"),
                        None => println!("  (pending…)"),
                    }
                    continue;
                }
                let state_note = match state {
                    "local" => String::new(),
                    "partial" => match a["progress"].as_u64() {
                        Some(pct) => format!(" · (partial · {pct}%)"),
                        None => " · (partial)".to_string(),
                    },
                    other => format!(" · ({other})"),
                };
                println!(
                    "  {}  {} · {}b · id {}{state_note}{}",
                    a["filename"].as_str().unwrap_or("?"),
                    a["who"].as_str().unwrap_or("?"),
                    a["size"],
                    a["id"],
                    a["tag"]
                        .as_str()
                        .map(|t| format!("\n      \"{t}\""))
                        .unwrap_or_default(),
                );
            }
            Ok(())
        }
        Cmd::Stay { serai: Some(serai) } => {
            let res = client::post("/api/stay", serde_json::json!({ "serai": serai })).await?;
            println!("now staying at {}", res["serai"].as_str().unwrap_or("?"));
            Ok(())
        }
        Cmd::Stay { serai: None } => {
            let res = client::get("/api/places").await?;
            let empty = Vec::new();
            let places = res["places"].as_array().unwrap_or(&empty);
            if places.is_empty() {
                println!("you belong to no serai yet — `serai new <name>` opens one");
                return Ok(());
            }
            let last = res["last_place"].as_str().unwrap_or("");
            for (i, p) in places.iter().enumerate() {
                let id = p["id"].as_str().unwrap_or("");
                let name = p["name"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("(unintroduced serai {}…)", &id[..8]));
                let ticket = p["ticket"].as_str().unwrap_or("");
                let here = if id == last { "   (now staying)" } else { "" };
                if i > 0 {
                    println!();
                }
                println!("{name}{here}");
                println!("    id      {}…", &id[..8]);
                println!("    ticket  {ticket}");
            }
            Ok(())
        }
        Cmd::Take { target, output, serai } => {
            let out = output.unwrap_or_else(|| PathBuf::from("."));
            let out = if out.is_absolute() {
                out
            } else {
                std::env::current_dir()?.join(out)
            };
            let res = client::post(
                "/api/take",
                serde_json::json!({ "target": target, "out": out, "serai": serai }),
            )
            .await?;
            println!(
                "took {} ({} bytes)",
                res["path"].as_str().unwrap_or(""),
                res["size"]
            );
            Ok(())
        }
        Cmd::View { serai } => {
            client::ensure_agent().await?;
            if let Some(s) = serai {
                // joins on first contact (ticket) or switches the current serai (name)
                let _ = client::get(&format!("/api/artifacts?serai={s}")).await?;
            }
            let url = client::base_url();
            println!("→ {url}");
            std::process::Command::new("open").arg(&url).spawn()?;
            Ok(())
        }
    }
}
