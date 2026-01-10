use aws_sdk_s3::Client;
use futures::{stream, StreamExt};
use rayon::prelude::*;
use std::env;
use std::sync::Arc;
use std::time::Duration;

pub const S3_TARGET: &str = "s3";

/// Inserts transactions into S3 with retry logic.
/// # Arguments
/// * `garage` - An Arc reference to the S3 client.
/// * `transactions` - A vector of tuples containing transaction hash and serialized transaction data.
pub async fn insert_transactions_to_s3(
    client: &Arc<Client>,
    transactions: Vec<(String, String)>,
) -> anyhow::Result<()> {
    if env::var("S3_SKIP_COMMIT") == Ok("true".to_string()) {
        return Ok(());
    }
    let max_workers: usize = env::var("S3_MAX_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);

    let max_retries = 10;
    let s3_bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET is not set");
    let bucket = s3_bucket.as_str();

    // Compressing transactions with zstd
    let transactions = transactions
        .into_par_iter() // ← Par
        .map(|(tx_hash, tx)| {
            let compressed_data = zstd::encode_all(tx.as_bytes(), 3).expect("zstd encode error");
            (tx_hash, compressed_data)
        })
        .collect::<Vec<_>>();

    let results = stream::iter(transactions)
        .map(|(tx_hash, compressed)| {
            let client = client.clone();
            let bucket = bucket;

            async move {
                let mut attempt = 0;
                let mut delay = Duration::from_millis(100);

                loop {
                    let res = client
                        .put_object()
                        .bucket(bucket)
                        .key(tx_hash.clone())
                        .body(aws_sdk_s3::primitives::ByteStream::from(
                            compressed.to_vec(),
                        ))
                        .content_type("application/octet-stream")
                        .content_encoding("zstd")
                        .send()
                        .await;

                    match res {
                        Ok(_) => break Ok(()),
                        Err(e) => {
                            attempt += 1;
                            if attempt >= max_retries {
                                break Err(e);
                            }
                            tracing::log::error!(target: S3_TARGET, "Attempt #{}: Error s3 insert \"{}\": {}", attempt, tx_hash, e);
                            tokio::time::sleep(delay).await;
                            delay *= 2;
                        }
                    }
                }
            }
        })
        .buffer_unordered(max_workers)
        .collect::<Vec<_>>()
        .await;

    for result in results {
        result?;
    }

    Ok(())
}
