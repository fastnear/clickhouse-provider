use crate::*;
use near_indexer::near_primitives::types::BlockHeight;
use reqwest::Client;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PROJECT_ID: &str = "fetcher";

pub type BlockResult = Result<Option<BlockWithTxHashes>, FetchError>;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum FetchError {
    ReqwestError(reqwest::Error),
}

impl From<reqwest::Error> for FetchError {
    fn from(error: reqwest::Error) -> Self {
        FetchError::ReqwestError(error)
    }
}

pub async fn fetch_block(client: &Client, url: &str, timeout: Duration) -> BlockResult {
    let response = client.get(url).timeout(timeout).send().await?;
    Ok(response.json().await?)
}

pub async fn fetch_block_until_success(
    client: &Client,
    url: &str,
    timeout: Duration,
) -> Option<BlockWithTxHashes> {
    loop {
        match fetch_block(client, url, timeout).await {
            Ok(block) => return block,
            Err(FetchError::ReqwestError(err)) => {
                tracing::log::warn!("Failed to fetch block: {}", err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn target_url(suffix: &str) -> String {
    format!(
        "{}{}",
        env::var("TARGET_URL").expect("Missing env TARGET_URL"),
        suffix
    )
}

pub async fn fetch_first_block(client: &Client) -> Option<BlockWithTxHashes> {
    fetch_block_until_success(client, &target_url("/v0/first_block"), DEFAULT_TIMEOUT).await
}

pub async fn fetch_last_block(client: &Client) -> Option<BlockWithTxHashes> {
    fetch_block_until_success(client, &target_url("/v0/last_block/final"), DEFAULT_TIMEOUT).await
}

pub async fn fetch_block_by_height(
    client: &Client,
    height: BlockHeight,
    timeout: Duration,
) -> Option<BlockWithTxHashes> {
    fetch_block_until_success(
        client,
        &target_url(&format!("/v0/block/{}", height)),
        timeout,
    )
    .await
}

pub async fn start_fetcher(
    client: Client,
    start_block_height: BlockHeight,
    blocks_sink: mpsc::Sender<BlockWithTxHashes>,
) {
    let max_num_threads = env::var("NUM_FETCHING_THREADS")
        .expect("Missing env NUM_FETCHING_THREADS")
        .parse::<u64>()
        .expect("NUM_FETCHING_THREADS should be a number");
    let next_sink_block = Arc::new(AtomicU64::new(start_block_height));
    while RUNNING.load(Ordering::SeqCst) {
        let start_block_height = next_sink_block.load(Ordering::SeqCst);
        let next_fetch_block = Arc::new(AtomicU64::new(start_block_height));
        let last_block_height = fetch_last_block(&client)
            .await
            .expect("Last block doesn't exist")
            .block
            .header
            .height;
        let is_backfill = last_block_height > start_block_height + max_num_threads;
        let num_threads = if is_backfill { max_num_threads } else { 1 };
        tracing::log::info!(
            target: PROJECT_ID,
            "Start fetching from block {} to block {} with {} threads. Backfill: {:?}",
            start_block_height,
            last_block_height,
            num_threads,
            is_backfill
        );
        // starting backfill with multiple threads
        let handles = (0..num_threads)
            .map(|thread_index| {
                let client = client.clone();
                let blocks_sink = blocks_sink.clone();
                let next_fetch_block = next_fetch_block.clone();
                let next_sink_block = next_sink_block.clone();
                tokio::spawn(async move {
                    while RUNNING.load(Ordering::SeqCst) {
                        let block_height = next_fetch_block.fetch_add(1, Ordering::SeqCst);
                        if is_backfill && block_height > last_block_height {
                            break;
                        }
                        tracing::log::debug!(target: PROJECT_ID, "#{}: Fetching block: {}", thread_index, block_height);
                        let block =
                            fetch_block_by_height(&client, block_height, DEFAULT_TIMEOUT).await;
                        while RUNNING.load(Ordering::SeqCst) {
                            let expected_block_height = next_sink_block.load(Ordering::SeqCst);
                            if expected_block_height < block_height {
                                tokio::time::sleep(Duration::from_millis(
                                    block_height - expected_block_height,
                                ))
                                .await;
                            } else {
                                tracing::log::debug!(target: PROJECT_ID, "#{}: Sending block: {}", thread_index, block_height);
                                break;
                            }
                        }
                        if let Some(block) = block {
                            blocks_sink.send(block).await.expect("Failed to send block");
                        } else {
                            tracing::log::debug!(target: PROJECT_ID, "#{}: Skipped block: {}", thread_index, block_height);
                        }
                        next_sink_block.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await.expect("Failed to join fetching thread");
        }
    }
}
