mod actions;
mod click;
mod common;

mod transactions;
mod types;

use crate::click::*;
use crate::transactions::TransactionsData;
use std::sync::Arc;

use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::*;
use fastnear_primitives::near_primitives::types::BlockHeight;
use fastnear_primitives::types::ChainId;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio::try_join;

const PROJECT_ID: &str = "provider";

const SAFE_CATCH_UP_OFFSET: u64 = 1000;

/// How many blocks the fetcher may buffer ahead of us, and by extension the largest
/// bundle a single commit can cover.
const BLOCKS_CHANNEL_SIZE: usize = 100;

/// Number of fetch threads used while at the tip. The crate's own default.
const DEFAULT_NUM_LOOKAHEAD_THREADS: u64 = 4;

#[tokio::main]
async fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();
    let mut signal_handle = tokio::spawn(async move {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Received SIGINT, shutting down...");
            }
            _ = sigterm.recv() => {
                println!("Received SIGTERM, shutting down...");
            }
        }
        ctrl_c_running.store(false, Ordering::SeqCst);
    });

    common::setup_tracing("garage=info,clickhouse=info,provider=info,neardata-fetcher=info");

    tracing::log::info!(target: PROJECT_ID, "Starting Clickhouse Provider");

    let db = Arc::new(ClickDB::new());
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
    // Only `num_threads` applies while backfilling; at the tip the fetcher uses the
    // lookahead pool, which this binary never configured before.
    let num_lookahead_threads = env_u64("NUM_LOOKAHEAD_THREADS", DEFAULT_NUM_LOOKAHEAD_THREADS);
    let max_blocks_per_commit =
        env_u64("MAX_BLOCKS_PER_COMMIT", BLOCKS_CHANNEL_SIZE as u64).max(1) as usize;
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
        TransactionsData::new(chain_id, end_backfill_block_height.is_some(), db.clone());
    let db_last_block_height = transactions_data
        .last_block_in_range(
            &db,
            backfill_block_height.unwrap_or(0),
            end_backfill_block_height.unwrap_or(10u64.pow(15)),
            max_blocks_per_commit,
        )
        .await
        .expect("Failed to query the last indexed block height");
    tracing::log::info!(target: PROJECT_ID, "Last block height in range: {}", db_last_block_height);
    let last_block_height = db_last_block_height;
    let start_block_height = (db_last_block_height + 1)
        .saturating_sub(SAFE_CATCH_UP_OFFSET)
        .max(first_block_height);
    tracing::log::info!(target: PROJECT_ID, "Starting from block height: {}", start_block_height);

    let start_block_height = first_block_height.max(start_block_height);
    let (sender, receiver) = mpsc::channel(BLOCKS_CHANNEL_SIZE);
    let mut builder = fetcher::FetcherConfigBuilder::new()
        .chain_id(chain_id)
        .num_threads(num_threads)
        .num_lookahead_threads(num_lookahead_threads)
        .start_block_height(start_block_height);
    if let Some(end_backfill_block_height) = end_backfill_block_height {
        builder = builder.end_block_height(end_backfill_block_height.saturating_sub(1));
    }
    if let Some(auth_bearer_token) = auth_bearer_token {
        builder = builder.auth_bearer_token(auth_bearer_token);
    }
    let mut fetcher_handle = tokio::spawn(fetcher::start_fetcher(
        builder.build(),
        sender,
        is_running.clone(),
    ));
    let listener_is_running = is_running.clone();
    let mut block_listener_handle = tokio::spawn(async move {
        listen_blocks_for_transactions(
            receiver,
            transactions_data,
            last_block_height,
            listener_is_running,
            max_blocks_per_commit,
        )
        .await
    });

    // Only the listener finishing means "we're done": it is the task that drains the
    // channel and writes the final batch. The other two finish normally in expected
    // situations -- the fetcher when a finite backfill ends or `is_running` goes false,
    // the signal watcher when a signal arrives -- so a clean exit from either is not a
    // reason to stop. Their *failure* is, and must not be mistaken for a graceful
    // shutdown just because the listener then saw the channel close.
    let mut fetcher_done = false;
    let mut signal_done = false;
    let mut aux_error: Option<String> = None;
    let listener_result = loop {
        tokio::select! {
            biased;
            result = &mut block_listener_handle => break result,
            result = &mut fetcher_handle, if !fetcher_done => {
                fetcher_done = true;
                if let Err(err) = result {
                    aux_error.get_or_insert(format!("fetcher task terminated: {}", err));
                    // Wind the listener down: some fetch workers may still hold senders,
                    // so the channel would otherwise never close.
                    is_running.store(false, Ordering::SeqCst);
                }
            }
            result = &mut signal_handle, if !signal_done => {
                signal_done = true;
                if let Err(err) = result {
                    aux_error.get_or_insert(format!("signal task terminated: {}", err));
                    is_running.store(false, Ordering::SeqCst);
                }
            }
        }
    };
    is_running.store(false, Ordering::SeqCst);
    signal_handle.abort();
    fetcher_handle.abort();

    let error = match listener_result {
        Ok(Ok(())) => aux_error,
        Ok(Err(err)) => Some(format!("{:#}", err)),
        Err(err) => Some(format!("block listener terminated: {}", err)),
    };
    match error {
        None => {
            tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
        }
        Some(err) => {
            tracing::log::error!(target: PROJECT_ID, "Indexer failed: {}", err);
            std::process::exit(1);
        }
    }
}

async fn listen_blocks_for_transactions(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    mut transactions_data: TransactionsData,
    last_block_height: u64,
    is_running: Arc<AtomicBool>,
    max_blocks_per_commit: usize,
) -> anyhow::Result<()> {
    let mut prev_block_hash = None;
    // Bundling is adaptive by construction. `recv_many` returns as soon as one block is
    // available, taking whatever else is already buffered with it. At the tip the channel
    // is empty, so a bundle is a single block and we commit per block, as before. When
    // we're behind, the fetcher keeps the channel full, so a bundle is
    // `max_blocks_per_commit` blocks and one commit's fixed cost -- roughly a second of
    // ClickHouse round trips, almost independent of row count -- is amortised across all
    // of them instead of being paid per block.
    let mut bundle = Vec::with_capacity(max_blocks_per_commit);
    loop {
        bundle.clear();
        // `recv_many` appends, and returns 0 only once the channel is closed and drained.
        if stream.recv_many(&mut bundle, max_blocks_per_commit).await == 0 {
            break;
        }
        if !is_running.load(Ordering::SeqCst) {
            // Graceful shutdown. Consuming remaining blocks without processing.
            continue;
        }
        let bundle_size = bundle.len();
        transactions_data.log_each_block = bundle_size == 1;
        let first_bundled_block_height = bundle[0].block.header.height;
        let mut last_bundled_block_height = first_bundled_block_height;
        let mut last_block_timestamp = 0;

        for block in bundle.drain(..) {
            last_bundled_block_height = block.block.header.height;
            last_block_timestamp = block.block.header.timestamp_nanosec;
            if transactions_data.log_each_block {
                tracing::log::info!(target: PROJECT_ID, "Processing block {}\tlatency {:.3} sec",
                    last_bundled_block_height, latency_sec(last_block_timestamp));
            }
            prev_block_hash = Some(
                transactions_data
                    .process_block(block, last_block_height, prev_block_hash)
                    .await?,
            );
        }
        if !transactions_data.log_each_block {
            tracing::log::info!(target: PROJECT_ID, "Processing blocks {}..{} ({} blocks)\tlatency {:.3} sec",
                first_bundled_block_height, last_bundled_block_height, bundle_size,
                latency_sec(last_block_timestamp));
        }

        transactions_data.commit().await?;
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    transactions_data.commit().await?;
    transactions_data.flush().await?;
    Ok(())
}

/// Seconds between a block being produced on chain and now.
fn latency_sec(block_timestamp_nanosec: u64) -> f64 {
    let current_time_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    current_time_ns.saturating_sub(block_timestamp_nanosec) as f64 / 1e9f64
}
