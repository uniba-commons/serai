use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{Path as UrlPath, Query as HttpQuery, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_lite::StreamExt;
use iroh::{endpoint::presets, protocol::Router as IrohRouter, Endpoint, SecretKey};
use iroh_blobs::{
    api::blobs::ImportMode, store::fs::FsStore, BlobsProtocol, ALPN as BLOBS_ALPN,
};
use iroh_docs::{
    api::{
        protocol::{AddrInfoOptions, ShareMode},
        Doc,
    },
    protocol::Docs,
    store::Query,
    DocTicket, NamespaceId, ALPN as DOCS_ALPN,
};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::config::{discover_port, env_port, port_file, serai_dir, Config, BASE_PORT, PORT_PROBE};

struct AgentState {
    docs: Docs,
    blobs: iroh_blobs::api::Store,
    downloader: iroh_blobs::api::downloader::Downloader,
    endpoint: Endpoint,
    config: Mutex<Config>,
    open_docs: Mutex<HashMap<NamespaceId, Doc>>,
}

pub async fn run() -> Result<()> {
    let dir = serai_dir();
    std::fs::create_dir_all(&dir)?;

    // refuse to start when an agent for this SERAI_DIR is already up
    if let Some(port) = ping_running_agent().await {
        return Err(anyhow!(
            "a serai agent for {} is already running on port {port}",
            dir.display()
        ));
    }

    // claim a port before touching the stores, so a losing second agent
    // fails fast here instead of hanging on the store file locks below
    let (listener, port) = bind_port().await?;
    std::fs::write(port_file(), port.to_string())?;

    // stable node identity across restarts
    let key_path = dir.join("key");
    let secret_key = if key_path.exists() {
        let hex = std::fs::read_to_string(&key_path)?;
        let bytes: Vec<u8> = (0..64)
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex.trim()[i..i + 2], 16))
            .collect::<Result<_, _>>()
            .context("invalid key file")?;
        SecretKey::from_bytes(bytes.as_slice().try_into().context("invalid key length")?)
    } else {
        let sk = SecretKey::generate();
        let hex: String = sk.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
        std::fs::write(&key_path, hex)?;
        sk
    };

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await?;
    let blobs_store = FsStore::load(dir.join("blobs")).await?;
    let blobs: iroh_blobs::api::Store = (*blobs_store).clone();
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let docs = Docs::persistent(dir.clone())
        .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
        .await?;
    let _router = IrohRouter::builder(endpoint.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs_store, None))
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs.clone())
        .spawn();

    let downloader = blobs_store.downloader(&endpoint);
    let state = Arc::new(AgentState {
        docs,
        blobs,
        downloader,
        endpoint,
        config: Mutex::new(Config::load()?),
        open_docs: Mutex::new(HashMap::new()),
    });

    // heal loop: entries whose content never finished downloading are not
    // re-fetched by sync (reconciliation sees no new entries), so we
    // periodically look for gaps and fetch them from known sync peers
    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                if let Err(err) = heal_missing_content(&state).await {
                    eprintln!("[agent] heal pass failed: {err:#}");
                }
            }
        });
    }

    // re-open all known places so they keep syncing
    {
        let ids = state.config.lock().await.places.clone();
        for ns_hex in ids {
            if let Ok(ns) = NamespaceId::from_str(&ns_hex) {
                if let Err(err) = open_doc(&state, ns).await {
                    eprintln!("[agent] failed to reopen place {ns_hex}: {err:#}");
                }
            }
        }
    }

    let blobs_for_shutdown = state.blobs.clone();
    let app = Router::new()
        .route("/api/ping", get(ping))
        .route("/api/places", get(places))
        .route("/api/serai", post(new_serai))
        .route("/api/stay", post(stay))
        .route("/api/spread", post(spread))
        .route("/api/artifacts", get(list_artifacts))
        .route("/api/take", post(take_artifact))
        .route("/artifacts/{id}/{filename}", get(serve_artifact))
        .fallback(get(courtyard))
        .with_state(state);

    eprintln!("[agent] listening on http://127.0.0.1:{port}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = signal(SignalKind::terminate()).expect("signal handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            eprintln!("[agent] shutting down");
        })
        .await?;
    // flush the blob store so inline (small) payloads survive the restart
    blobs_for_shutdown.shutdown().await.ok();
    let _ = std::fs::remove_file(port_file());
    Ok(())
}

/// Pings the port a running agent would use; Some(port) if it answers
/// as a serai agent serving the same SERAI_DIR.
async fn ping_running_agent() -> Option<u16> {
    let port = discover_port();
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/api/ping"))
        .timeout(Duration::from_millis(500))
        .send()
        .await
        .ok()?;
    let body: serde_json::Value = resp.json().await.ok()?;
    let same_dir = body["dir"].as_str() == Some(&serai_dir().display().to_string());
    (body["serai"].as_bool() == Some(true) && same_dir).then_some(port)
}

/// Binds the agent port: the env override exactly, or the first free port
/// in the probing window starting at BASE_PORT.
async fn bind_port() -> Result<(tokio::net::TcpListener, u16)> {
    if let Some(port) = env_port() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .with_context(|| format!("SERAI_PORT={port} is busy"))?;
        return Ok((listener, port));
    }
    for port in BASE_PORT..BASE_PORT + PORT_PROBE {
        if let Ok(listener) = tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
            return Ok((listener, port));
        }
    }
    Err(anyhow!(
        "no free port in {BASE_PORT}..{} — set SERAI_PORT to override",
        BASE_PORT + PORT_PROBE
    ))
}

/// One pass of the self-heal: for every entry whose blob is incomplete,
/// ask the known sync peers of that serai for the missing content.
async fn heal_missing_content(state: &Arc<AgentState>) -> Result<()> {
    let docs: Vec<Doc> = state.open_docs.lock().await.values().cloned().collect();
    for doc in docs {
        let peers: Vec<iroh::PublicKey> = doc
            .get_sync_peers()
            .await?
            .unwrap_or_default()
            .iter()
            .filter_map(|bytes| iroh::PublicKey::from_bytes(bytes).ok())
            .collect();
        if peers.is_empty() {
            continue;
        }
        let entries = doc.get_many(Query::all()).await?;
        let mut entries = std::pin::pin!(entries);
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            let hash = entry.content_hash();
            let complete = matches!(
                state.blobs.status(hash).await,
                Ok(iroh_blobs::api::proto::BlobStatus::Complete { .. })
            );
            if complete {
                continue;
            }
            eprintln!(
                "[agent] healing {} from {} peer(s)",
                String::from_utf8_lossy(entry.key()),
                peers.len()
            );
            let download = state.downloader.download(hash, peers.clone());
            if let Err(err) =
                tokio::time::timeout(Duration::from_secs(600), download).await
            {
                eprintln!("[agent] heal timed out: {err:#}");
            }
        }
    }
    Ok(())
}

/// Opens a doc (once) and starts its live sync machinery.
async fn open_doc(state: &Arc<AgentState>, ns: NamespaceId) -> Result<Doc> {
    let mut open_docs = state.open_docs.lock().await;
    if let Some(doc) = open_docs.get(&ns) {
        return Ok(doc.clone());
    }
    let doc = state
        .docs
        .api()
        .open(ns)
        .await?
        .ok_or_else(|| anyhow!("place not found in store"))?;
    doc.start_sync(vec![]).await?;
    open_docs.insert(ns, doc.clone());
    Ok(doc)
}

/// Reads a place's self-declared name (the `place/name` entry) live from the doc.
/// Returns None when the entry (or its content) has not arrived yet.
async fn place_name(state: &Arc<AgentState>, doc: &Doc) -> Option<String> {
    let entry = doc.get_one(Query::key_exact("place/name")).await.ok()??;
    let bytes = state.blobs.get_bytes(entry.content_hash()).await.ok()?;
    String::from_utf8(bytes.to_vec()).ok()
}

/// Display name: the self-declared name if it has arrived, otherwise an honest
/// "not yet introduced" marker derived from the id (never stored anywhere).
async fn display_name(state: &Arc<AgentState>, doc: &Doc) -> String {
    match place_name(state, doc).await {
        Some(name) => name,
        None => format!("(unintroduced serai {}…)", &doc.id().to_string()[..8]),
    }
}

/// Waits briefly for a just-joined place to introduce itself.
async fn wait_for_name(state: &Arc<AgentState>, doc: &Doc, timeout: Duration) -> Option<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(name) = place_name(state, doc).await {
            return Some(name);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn remember_membership(state: &Arc<AgentState>, ns_hex: &str) -> Result<()> {
    let mut config = state.config.lock().await;
    if !config.places.contains(&ns_hex.to_string()) {
        config.places.push(ns_hex.to_string());
        config.save()?;
    }
    Ok(())
}

/// Moves the "staying at" pointer — the serai bare commands act on.
async fn set_stay(state: &Arc<AgentState>, ns_hex: &str) -> Result<()> {
    let mut config = state.config.lock().await;
    config.last_place = Some(ns_hex.to_string());
    config.save()
}

/// Resolves a serai spec (name, id prefix, ticket, or None = staying at) to an open Doc.
///
/// `switch` decides whether this resolution moves the "staying at" pointer.
/// Rule: looking around (ls / view / take) never moves you; acting with an
/// explicit destination (spread / stay / new) or arriving through the gate does.
async fn resolve_place(
    state: &Arc<AgentState>,
    spec: Option<&str>,
    switch: bool,
) -> Result<(String, Doc)> {
    match spec {
        // a DocTicket string ("doc...")
        Some(s) if s.starts_with("doc") && s.len() > 60 => {
            let ticket = DocTicket::from_str(s).context("invalid ticket")?;
            let ns = ticket.capability.id();
            let ns_hex = ns.to_string();
            let known = state.config.lock().await.places.contains(&ns_hex);
            let doc = if known {
                // presenting a ticket again is a request to reconnect:
                // re-sync with the peers named in the ticket
                let doc = open_doc(state, ns).await?;
                doc.start_sync(ticket.nodes.clone()).await?;
                doc
            } else {
                // first contact: join, then wait a beat for the place to introduce itself
                let (doc, events) = state.docs.api().import_and_subscribe(ticket).await?;
                tokio::spawn(async move {
                    let mut events = std::pin::pin!(events);
                    while events.next().await.is_some() {}
                });
                state.open_docs.lock().await.insert(ns, doc.clone());
                doc
            };
            remember_membership(state, &ns_hex).await?;
            // walking through the gate means you are now staying there
            set_stay(state, &ns_hex).await?;
            let name = match wait_for_name(state, &doc, Duration::from_secs(3)).await {
                Some(name) => name,
                None => display_name(state, &doc).await,
            };
            Ok((name, doc))
        }
        // a known serai, referred to by its self-declared name or an id prefix
        Some(spec_str) => {
            let ids = state.config.lock().await.places.clone();
            let mut matches = Vec::new();
            for ns_hex in ids {
                let ns = NamespaceId::from_str(&ns_hex)?;
                let doc = open_doc(state, ns).await?;
                let matches_name =
                    place_name(state, &doc).await.as_deref() == Some(spec_str);
                if matches_name || ns_hex.starts_with(spec_str) {
                    matches.push((ns_hex, doc));
                }
            }
            match matches.len() {
                0 => Err(anyhow!("unknown serai: {spec_str}")),
                1 => {
                    let (ns_hex, doc) = matches.remove(0);
                    if switch {
                        set_stay(state, &ns_hex).await?;
                    }
                    Ok((display_name(state, &doc).await, doc))
                }
                n => {
                    let ids: Vec<String> =
                        matches.iter().map(|(h, _)| format!("{}…", &h[..8])).collect();
                    Err(anyhow!(
                        "\"{spec_str}\" is ambiguous — {n} serais answer to it: {}. use an id prefix instead",
                        ids.join(", ")
                    ))
                }
            }
        }
        None => {
            let config = state.config.lock().await;
            let ns_hex = config
                .last_place
                .clone()
                .or_else(|| (config.places.len() == 1).then(|| config.places[0].clone()))
                .ok_or_else(|| anyhow!("no serai specified and none used before"))?;
            drop(config);
            let ns = NamespaceId::from_str(&ns_hex)?;
            let doc = open_doc(state, ns).await?;
            Ok((display_name(state, &doc).await, doc))
        }
    }
}

// ---- handlers ----

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{:#}", self.0)).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

async fn ping(State(state): State<Arc<AgentState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "serai": true,
        "dir": serai_dir().display().to_string(),
        "endpoint": state.endpoint.id().to_string(),
    }))
}

async fn places(State(state): State<Arc<AgentState>>) -> Result<Json<serde_json::Value>, AppError> {
    let (ids, last) = {
        let config = state.config.lock().await;
        (config.places.clone(), config.last_place.clone())
    };
    let mut items = Vec::new();
    for ns_hex in ids {
        let ns = NamespaceId::from_str(&ns_hex)?;
        let doc = open_doc(&state, ns).await?;
        let ticket = doc.share(ShareMode::Write, AddrInfoOptions::Relay).await?;
        items.push(serde_json::json!({
            "id": ns_hex,
            "name": place_name(&state, &doc).await,
            "ticket": ticket.to_string(),
        }));
    }
    Ok(Json(serde_json::json!({ "places": items, "last_place": last })))
}

#[derive(Deserialize)]
struct StayReq {
    serai: String,
}

async fn stay(
    State(state): State<Arc<AgentState>>,
    Json(req): Json<StayReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    let (name, _) = resolve_place(&state, Some(&req.serai), true).await?;
    Ok(Json(serde_json::json!({ "serai": name })))
}

#[derive(Deserialize)]
struct NewSeraiReq {
    name: String,
}

async fn new_serai(
    State(state): State<Arc<AgentState>>,
    Json(req): Json<NewSeraiReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    let api = state.docs.api();
    let doc = api.create().await?;
    let author = api.author_default().await?;
    doc.set_bytes(author, "place/name", req.name.clone()).await?;
    state.blobs.sync_db().await.map_err(|e| anyhow!("{e}"))?;
    // Relay only: direct addresses are a snapshot that goes stale anyway,
    // and they are what makes tickets long. The relay URL + endpoint id is
    // enough to find the peer, with n0 discovery as a further fallback.
    let ticket = doc.share(ShareMode::Write, AddrInfoOptions::Relay).await?;
    doc.start_sync(vec![]).await?;

    let ns = doc.id();
    state.open_docs.lock().await.insert(ns, doc);
    remember_membership(&state, &ns.to_string()).await?;
    set_stay(&state, &ns.to_string()).await?;

    Ok(Json(serde_json::json!({
        "name": req.name,
        "id": ns.to_string(),
        "ticket": ticket.to_string(),
    })))
}

#[derive(Deserialize)]
struct SpreadReq {
    path: PathBuf,
    tag: Option<String>,
    serai: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Artifact {
    id: u64,
    filename: String,
    tag: Option<String>,
    size: u64,
    who: String,
    author: String,
}

async fn spread(
    State(state): State<Arc<AgentState>>,
    Json(req): Json<SpreadReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    // spreading at a named serai is an explicit act: it moves the stay pointer
    let (serai_name, doc) = resolve_place(&state, req.serai.as_deref(), true).await?;
    let path = req.path.canonicalize().context("file not found")?;
    if !path.is_file() {
        return Err(anyhow!("only single files can be spread for now").into());
    }
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("invalid file name"))?
        .to_string();
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let author = state.docs.api().author_default().await?;
    let key = format!("artifacts/{id}/{filename}");
    let progress = doc
        .import_file(
            &state.blobs,
            author,
            key.clone().into(),
            &path,
            ImportMode::Copy,
        )
        .await?;
    let outcome = progress.await?;

    let artifact = Artifact {
        id,
        filename: filename.clone(),
        tag: req.tag,
        size: outcome.size,
        who: std::env::var("USER").unwrap_or_else(|_| "someone".into()),
        author: author.to_string(),
    };
    doc.set_bytes(author, format!("meta/{id}"), serde_json::to_vec(&artifact)?)
        .await?;
    state.blobs.sync_db().await.map_err(|e| anyhow!("{e}"))?;

    Ok(Json(serde_json::json!({
        "serai": serai_name,
        "filename": filename,
        "size": outcome.size,
    })))
}

#[derive(Deserialize)]
struct SeraiQuery {
    serai: Option<String>,
}

async fn list_artifacts(
    State(state): State<Arc<AgentState>>,
    HttpQuery(q): HttpQuery<SeraiQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    // just looking around: never moves the stay pointer
    let (serai_name, doc) = resolve_place(&state, q.serai.as_deref(), false).await?;
    let entries = doc.get_many(Query::key_prefix("meta/")).await?;
    let mut entries = std::pin::pin!(entries);
    let mut items = Vec::new();
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        match state.blobs.get_bytes(entry.content_hash()).await {
            Ok(bytes) => {
                if let Ok(artifact) = serde_json::from_slice::<Artifact>(&bytes) {
                    // where is the payload? local / partial / remote
                    let key = format!("artifacts/{}/{}", artifact.id, artifact.filename);
                    let (state_str, progress) =
                        match doc.get_one(Query::key_exact(&key)).await? {
                            Some(payload) => {
                                match state.blobs.status(payload.content_hash()).await {
                                    Ok(iroh_blobs::api::proto::BlobStatus::Complete { .. }) => {
                                        ("local", None)
                                    }
                                    Ok(iroh_blobs::api::proto::BlobStatus::Partial { size }) => {
                                        let pct = size.map(|got| {
                                            (got as f64 / artifact.size.max(1) as f64 * 100.0)
                                                as u64
                                        });
                                        ("partial", pct)
                                    }
                                    _ => ("remote", None),
                                }
                            }
                            None => ("remote", None),
                        };
                    items.push(serde_json::json!({
                        "id": artifact.id,
                        "filename": artifact.filename,
                        "tag": artifact.tag,
                        "size": artifact.size,
                        "who": artifact.who,
                        "state": state_str,
                        "progress": progress,
                    }));
                }
            }
            Err(_) => {
                // even the metadata blob has not arrived yet — but the id is in the key
                let key = String::from_utf8_lossy(entry.key()).to_string();
                let id = key.strip_prefix("meta/").and_then(|s| s.parse::<u64>().ok());
                items.push(serde_json::json!({
                    "id": id,
                    "state": "pending",
                }));
            }
        }
    }
    items.sort_by_key(|v| std::cmp::Reverse(v.get("id").and_then(|i| i.as_u64()).unwrap_or(0)));
    Ok(Json(serde_json::json!({ "serai": serai_name, "artifacts": items })))
}

fn guess_mime(filename: &str) -> &'static str {
    match filename.rsplit('.').next().unwrap_or("").to_lowercase().as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "pdf" => "application/pdf",
        "txt" | "md" | "rs" | "py" | "js" | "ts" | "json" | "toml" | "yaml" | "yml" => {
            "text/plain; charset=utf-8"
        }
        "html" | "htm" => "text/html; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Serves a single payload to the browser (the clickable door of the courtyard).
async fn serve_artifact(
    State(state): State<Arc<AgentState>>,
    UrlPath((id, filename)): UrlPath<(String, String)>,
    HttpQuery(q): HttpQuery<SeraiQuery>,
) -> Result<Response, AppError> {
    let (_, doc) = resolve_place(&state, q.serai.as_deref(), false).await?;
    let key = format!("artifacts/{id}/{filename}");
    let entry = doc
        .get_one(Query::key_exact(&key))
        .await?
        .ok_or_else(|| anyhow!("artifact not found: {key}"))?;
    let bytes = state
        .blobs
        .get_bytes(entry.content_hash())
        .await
        .map_err(|_| anyhow!("content not here yet — the caravan is on its way"))?;
    Ok((
        [
            (header::CONTENT_TYPE, guess_mime(&filename).to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("inline; filename=\"{filename}\""),
            ),
        ],
        bytes,
    )
        .into_response())
}

#[derive(Deserialize)]
struct TakeReq {
    /// filename or artifact id (prefix)
    target: String,
    /// absolute output path
    out: PathBuf,
    serai: Option<String>,
}

/// Exports a payload to a real file on disk (the CLI door).
async fn take_artifact(
    State(state): State<Arc<AgentState>>,
    Json(req): Json<TakeReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    let (_, doc) = resolve_place(&state, req.serai.as_deref(), false).await?;
    let entries = doc.get_many(Query::key_prefix("artifacts/")).await?;
    let mut entries = std::pin::pin!(entries);
    let mut found = None;
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        let key = String::from_utf8_lossy(entry.key()).to_string();
        // key = artifacts/<id>/<filename>
        let mut parts = key.splitn(3, '/');
        let (_, id, filename) = (parts.next(), parts.next(), parts.next());
        let matches = filename == Some(req.target.as_str())
            || id.map(|i| i.starts_with(&req.target)).unwrap_or(false);
        if matches {
            // entries stream is ordered; keep the newest match
            found = Some((entry, filename.unwrap_or("artifact.bin").to_string()));
        }
    }
    let (entry, filename) =
        found.ok_or_else(|| anyhow!("artifact not found: {}", req.target))?;
    let out = if req.out.is_dir() {
        req.out.join(&filename)
    } else {
        req.out.clone()
    };
    // read-then-write instead of blobs export: works for inline (small) blobs too.
    // TODO: stream via export for very large payloads
    let bytes = state
        .blobs
        .get_bytes(entry.content_hash())
        .await
        .map_err(|_| anyhow!("artifact is not local yet — check `serai artifacts` for its state"))?;
    tokio::fs::write(&out, &bytes)
        .await
        .with_context(|| format!("failed to write {}", out.display()))?;
    let size = bytes.len();
    Ok(Json(serde_json::json!({
        "path": out,
        "filename": filename,
        "size": size,
    })))
}

/// The courtyard SPA, built from courtyard/ and embedded into the binary.
#[derive(rust_embed::Embed)]
#[folder = "../../courtyard/dist"]
struct CourtyardAssets;

async fn courtyard(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    if let Some(file) = CourtyardAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.to_string())], file.data.into_owned())
            .into_response();
    }
    // SPA fallback: unknown paths get the app shell
    match CourtyardAssets::get("index.html") {
        Some(file) => (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8".to_string())],
            file.data.into_owned(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "courtyard assets missing").into_response(),
    }
}
