use std::str::FromStr;

use anyhow::Result;
use futures_lite::StreamExt;
use iroh::{endpoint::presets, protocol::Router, Endpoint};
use iroh_blobs::{store::mem::MemStore, BlobsProtocol, ALPN as BLOBS_ALPN};
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    protocol::Docs,
    DocTicket, ALPN as DOCS_ALPN,
};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let endpoint = Endpoint::bind(presets::N0).await?;
    let blobs = MemStore::default();
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let docs = Docs::memory()
        .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
        .await?;
    let _router = Router::builder(endpoint.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs, None))
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs.clone())
        .spawn();

    let api = docs.api();
    let author = api.author_default().await?;

    match args.get(1).map(|s| s.as_str()) {
        Some("host") => {
            let doc = api.create().await?;
            doc.set_bytes(author, "greeting", "hello from host").await?;
            let ticket = doc
                .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
                .await?;
            println!("TICKET {ticket}");
            let mut events = doc.subscribe().await?;
            eprintln!("[host] waiting for events...");
            while let Some(ev) = events.next().await {
                eprintln!("[host] {:?}", ev?);
            }
        }
        Some("join") => {
            let ticket = DocTicket::from_str(args.get(2).expect("usage: join <ticket>"))?;
            let t0 = std::time::Instant::now();
            let (doc, mut events) = api.import_and_subscribe(ticket).await?;
            eprintln!("[join] imported doc {} in {:?}", doc.id(), t0.elapsed());
            doc.set_bytes(author, "reply", "hello from joiner").await?;
            while let Some(ev) = events.next().await {
                eprintln!("[join] {:?} (t={:?})", ev?, t0.elapsed());
            }
        }
        _ => {
            eprintln!("usage: serai host | serai join <ticket>");
        }
    }
    Ok(())
}
