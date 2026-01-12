mod actions;
mod click;
mod common;

mod k2v_tools;
mod transactions;
mod types;

mod k2v_client;

use crate::click::*;
use crate::transactions::TransactionsData;
use std::sync::Arc;

use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::*;
use fastnear_primitives::near_primitives::types::BlockHeight;
use fastnear_primitives::types::ChainId;
use k2v_client::{K2vClient, K2vClientConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio::try_join;

const PROJECT_ID: &str = "provider";

const SAFE_CATCH_UP_OFFSET: u64 = 1000;

#[tokio::main]
async fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();
    let signal_handle = tokio::spawn(async move {
        let mut signals = signal_hook::iterator::Signals::new(&[
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGINT,
        ])
        .unwrap();
        for sig in signals.forever() {
            match sig {
                signal_hook::consts::SIGTERM | signal_hook::consts::SIGINT => {
                    println!("Received signal {}, shutting down...", sig);
                    ctrl_c_running.store(false, Ordering::SeqCst);
                    break;
                }
                _ => unreachable!(),
            }
        }
    });

    common::setup_tracing("garage=info,clickhouse=info,provider=info,neardata-fetcher=info");

    tracing::log::info!(target: PROJECT_ID, "Starting Clickhouse Provider");

    let rayon_threads = std::env::var("RAYON_NUM_THREADS")
        .unwrap_or_else(|_| "8".to_string())
        .parse::<usize>()
        .expect("Invalid RAYON_NUM_THREADS");
    rayon::ThreadPoolBuilder::new()
        .num_threads(rayon_threads) // Use 8 threads for compression
        .build_global()
        .unwrap();

    let db = Arc::new(ClickDB::new(10000));
    db.verify_connection()
        .await
        .expect("Failed to connect to Clickhouse");

    let config = K2vClientConfig {
        endpoint: std::env::var("K2V_ENDPOINT").expect("No K2V_ENDPOINT provided"),
        bucket: std::env::var("K2V_BUCKET").expect("No K2V_BUCKET provided"),
        region: std::env::var("AWS_REGION").expect("No AWS_REGION provided"),
        aws_access_key_id: std::env::var("AWS_ACCESS_KEY_ID")
            .expect("No AWS_ACCESS_KEY_ID provided"),
        aws_secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
            .expect("No AWS_SECRET_ACCESS_KEY provided"),
        user_agent: None,
        default_timeout: Some(std::time::Duration::from_secs(60)),
    };
    let garage = Arc::new(K2vClient::new(config).expect("Failed to create garage k2v client"));
    let _val = garage
        .read_index(k2v_client::Filter {
            start: None,
            end: None,
            prefix: None,
            limit: Some(1),
            reverse: false,
        })
        .await
        .expect("Failed to read index");

    // garage.read_batch(&[
    //     BatchReadOp {
    //         partition_key: "",
    //         filter: Filter {
    //             start: None,
    //             end: None,
    //             prefix: None,
    //             limit: Some(1),
    //             reverse: true,
    //         },
    //         single_item: true,
    //         conflicts_only: false,
    //         tombstones: false,
    //     }
    // ]);

    let client = reqwest::Client::new();
    let chain_id = ChainId::try_from(std::env::var("CHAIN_ID").expect("CHAIN_ID is not set"))
        .expect("Invalid chain id");
    let num_threads = std::env::var("NUM_FETCHING_THREADS")
        .expect("NUM_FETCHING_THREADS is not set")
        .parse::<u64>()
        .expect("Invalid NUM_FETCHING_THREADS");
    let auth_bearer_token = std::env::var("AUTH_BEARER_TOKEN").ok();

    let first_block_height = fetcher::fetch_first_block(&client, chain_id)
        .await
        .expect("First block doesn't exists")
        .block
        .header
        .height;

    tracing::log::info!(target: PROJECT_ID, "First block: {}", first_block_height);

    let args: Vec<String> = std::env::args().collect();
    let backfill_block_height: Option<BlockHeight> = args
        .get(1)
        .map(|v| v.parse().expect("Failed to parse backfill block height"));
    let end_backfill_block_height: Option<BlockHeight> = args.get(2).map(|v| {
        v.parse()
            .expect("Failed to parse end backfill block height")
    });

    let transactions_data =
        TransactionsData::new(end_backfill_block_height.is_some(), garage, db.clone());
    let db_last_block_height = transactions_data
        .last_block_in_range(
            &db,
            backfill_block_height.unwrap_or(0),
            end_backfill_block_height.unwrap_or(10u64.pow(15)),
        )
        .await;
    tracing::log::info!(target: PROJECT_ID, "Last block height in range: {}", db_last_block_height);
    let last_block_height = db_last_block_height;
    let start_block_height = (db_last_block_height + 1)
        .saturating_sub(SAFE_CATCH_UP_OFFSET)
        .max(first_block_height);
    tracing::log::info!(target: PROJECT_ID, "Starting from block height: {}", start_block_height);

    let start_block_height = first_block_height.max(start_block_height);
    let (sender, receiver) = mpsc::channel(100);
    let mut builder = fetcher::FetcherConfigBuilder::new()
        .chain_id(chain_id)
        .num_threads(num_threads)
        .start_block_height(start_block_height);
    if let Some(end_backfill_block_height) = end_backfill_block_height {
        builder = builder.end_block_height(end_backfill_block_height - 1);
    }
    if let Some(auth_bearer_token) = auth_bearer_token {
        builder = builder.auth_bearer_token(auth_bearer_token);
    }
    let fetcher_handle = tokio::spawn(fetcher::start_fetcher(
        builder.build(),
        sender,
        is_running.clone(),
    ));
    let block_listener_handle = tokio::spawn(async move {
        listen_blocks_for_transactions(
            receiver,
            transactions_data,
            last_block_height,
            is_running.clone(),
        )
        .await
    });
    let result = try_join!(block_listener_handle, signal_handle, fetcher_handle);
    if let Err(err) = result {
        tracing::log::error!(target: PROJECT_ID, "Error occurred: {}", err);
        std::process::exit(1);
    }

    tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
}

async fn listen_blocks_for_transactions(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    mut transactions_data: TransactionsData,
    last_block_height: u64,
    is_running: Arc<AtomicBool>,
) {
    let mut prev_block_hash = None;
    while let Some(block) = stream.recv().await {
        if !is_running.load(Ordering::SeqCst) {
            // Graceful shutdown. Consuming remaining blocks without processing.
            continue;
        }
        let block_height = block.block.header.height;
        let block_timestamp = block.block.header.timestamp_nanosec;
        let current_time_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let time_diff_ns = current_time_ns.saturating_sub(block_timestamp);
        tracing::log::info!(target: PROJECT_ID, "Processing block {}\tlatency {:.3} sec", block_height, time_diff_ns as f64 / 1e9f64);
        prev_block_hash = Some(
            transactions_data
                .process_block(block, last_block_height, prev_block_hash)
                .await
                .unwrap(),
        );
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    transactions_data.commit().await.unwrap();
    transactions_data.flush().await.unwrap();
}
