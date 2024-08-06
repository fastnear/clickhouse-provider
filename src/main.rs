mod actions;
mod click;
mod common;

mod transactions;
mod types;

use crate::actions::ActionsData;
use crate::click::*;
use crate::transactions::TransactionsData;
use std::sync::Arc;

use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::*;
use fastnear_primitives::types::ChainId;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "provider";

const SAFE_CATCH_UP_OFFSET: u64 = 1000;

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        println!("Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    common::setup_tracing("clickhouse=info,provider=info,neardata-fetcher=info");

    tracing::log::info!(target: PROJECT_ID, "Starting Clickhouse Provider");

    let mut db = ClickDB::new(10000);
    db.verify_connection()
        .await
        .expect("Failed to connect to Clickhouse");

    let client = reqwest::Client::new();
    let chain_id = ChainId::try_from(std::env::var("CHAIN_ID").expect("CHAIN_ID is not set"))
        .expect("Invalid chain id");
    let num_threads = std::env::var("NUM_FETCHING_THREADS")
        .expect("NUM_FETCHING_THREADS is not set")
        .parse::<u64>()
        .expect("Invalid NUM_FETCHING_THREADS");

    let first_block_height = fetcher::fetch_first_block(&client, chain_id)
        .await
        .expect("First block doesn't exists")
        .block
        .header
        .height;

    tracing::log::info!(target: PROJECT_ID, "First block: {}", first_block_height);

    let args: Vec<String> = std::env::args().collect();
    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command");

    match command {
        "actions" => {
            let mut actions_data = ActionsData::new();
            let last_block_height = actions_data.last_block_height(&db).await;
            let start_block_height = first_block_height.max(last_block_height + 1);
            let (sender, receiver) = mpsc::channel(100);
            let config = fetcher::FetcherConfig {
                num_threads,
                start_block_height,
                chain_id,
            };
            tokio::spawn(fetcher::start_fetcher(
                Some(client),
                config,
                sender,
                is_running,
            ));
            listen_blocks_for_actions(receiver, db, actions_data, last_block_height).await;
        }
        "transactions" => {
            let mut transactions_data = TransactionsData::new();
            let last_block_height = transactions_data.last_block_height(&mut db).await;
            let is_cache_ready = transactions_data.is_cache_ready(last_block_height);
            tracing::log::info!(target: PROJECT_ID, "Last block height: {}. Cache is ready: {}", last_block_height, is_cache_ready);

            let start_block_height = if is_cache_ready {
                last_block_height + 1
            } else {
                last_block_height.saturating_sub(SAFE_CATCH_UP_OFFSET)
            };

            let start_block_height = first_block_height.max(start_block_height);
            let (sender, receiver) = mpsc::channel(100);
            let config = fetcher::FetcherConfig {
                num_threads,
                start_block_height,
                chain_id,
            };
            tokio::spawn(fetcher::start_fetcher(
                Some(client),
                config,
                sender,
                is_running,
            ));
            listen_blocks_for_transactions(receiver, db, transactions_data, last_block_height)
                .await;
        }
        _ => {
            panic!("Unknown command");
        }
    };

    tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
}

async fn listen_blocks_for_actions(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    mut db: ClickDB,
    mut actions_data: ActionsData,
    last_block_height: u64,
) {
    while let Some(block) = stream.recv().await {
        let block_height = block.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        actions_data
            .process_block(&mut db, block, last_block_height)
            .await
            .unwrap();
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    actions_data.commit(&mut db).await.unwrap();
    actions_data.flush().await.unwrap();
}

async fn listen_blocks_for_transactions(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    db: ClickDB,
    mut transactions_data: TransactionsData,
    last_block_height: u64,
) {
    while let Some(block) = stream.recv().await {
        let block_height = block.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        transactions_data
            .process_block(&db, block, last_block_height)
            .await
            .unwrap();
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    transactions_data.commit(&db).await.unwrap();
    transactions_data.flush().await.unwrap();
}
