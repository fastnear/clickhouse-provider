use crate::k2v_client::{self, K2vClient, K2vValue};
use crate::types::GarageTransaction;
use rayon::prelude::*;
use std::env;
use std::sync::Arc;
use std::time::Duration;

pub const GARAGE_TARGET: &str = "garage";

/// Inserts transactions into Garage with retry logic.
/// # Arguments
/// * `garage` - An Arc reference to the Garage k2v client.
/// * `transactions` - A vector of tuples containing transaction hash and serialized transaction data.
pub async fn insert_transactions_to_garage(
    client: &Arc<K2vClient>,
    transactions: Vec<GarageTransaction>,
) -> anyhow::Result<()> {
    if env::var("GARAGE_SKIP_COMMIT") == Ok("true".to_string()) || transactions.is_empty() {
        return Ok(());
    }

    // Compressing transactions with zstd
    let transactions = transactions
        .into_par_iter() // ← Par
        .map(|mut tx| {
            tx.transaction = zstd::encode_all(&tx.transaction[..], 3).expect("zstd encode error");
            tx
        })
        .collect::<Vec<_>>();

    // Preparing batch insert keys, since they need to be references with lifespan
    let batch_keys: Vec<_> = transactions
        .iter()
        .map(|tx| (tx.tx_hash.clone(), format!("{:012}", tx.last_block_height)))
        .collect();

    let operations: Vec<_> = transactions
        .into_iter()
        .enumerate()
        .map(|(index, tx)| {
            let batch_key = &batch_keys[index];
            k2v_client::BatchInsertOp {
                partition_key: &batch_key.0,
                sort_key: &batch_key.1,
                causality: None,
                value: K2vValue::from(tx.transaction),
            }
        })
        .collect();

    let mut attempt: usize = 0;
    let mut delay = Duration::from_millis(100);
    let max_retries = 10;

    loop {
        let res = client.insert_batch(&operations).await;

        match res {
            Ok(_) => break Ok(()),
            Err(e) => {
                attempt += 1;
                if attempt >= max_retries {
                    break Err(e.into());
                }
                tracing::log::error!(target: GARAGE_TARGET, "Attempt #{}: Error Garage k2v insert (from {:?}): {}", attempt, batch_keys.first(), e);
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
    }
}
