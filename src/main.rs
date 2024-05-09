mod block_with_tx_hash;
mod click;
mod common;
mod fetcher;

use crate::block_with_tx_hash::*;
use crate::click::*;
use dotenv::dotenv;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "provider";
pub static RUNNING: AtomicBool = AtomicBool::new(true);

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    ctrlc::set_handler(move || {
        RUNNING.store(false, Ordering::SeqCst);
        println!("Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    common::setup_tracing("clickhouse=info,provider=info,fetcher=info");

    tracing::log::info!(target: PROJECT_ID, "Starting Clickhouse Provider");

    let mut db = ClickDB::new(10000);
    db.verify_connection()
        .await
        .expect("Failed to connect to Clickhouse");

    db.fetch_last_block_heights().await;

    let min_block_height = db.min_restart_block();

    tracing::log::info!(target: PROJECT_ID, "Min block height: {}", min_block_height);

    let client = reqwest::Client::new();
    let first_block_height = fetcher::fetch_first_block(&client)
        .await
        .expect("First block doesn't exists")
        .block
        .header
        .height;

    tracing::log::info!(target: PROJECT_ID, "First block: {}", first_block_height);

    let start_block_height = first_block_height.max(min_block_height + 1);
    let (sender, receiver) = mpsc::channel(100);
    tokio::spawn(fetcher::start_fetcher(client, start_block_height, sender));
    listen_blocks(receiver, db).await;
    tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
}

async fn listen_blocks(mut stream: mpsc::Receiver<BlockWithTxHashes>, mut db: ClickDB) {
    while let Some(block) = stream.recv().await {
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block.block.header.height);
        extract_info(&mut db, block).await.unwrap();
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    db.commit().await.unwrap();
}
