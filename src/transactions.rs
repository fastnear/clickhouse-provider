use crate::*;
use std::env;

use clickhouse::Row;
use near_indexer::near_primitives::borsh::BorshDeserialize;
use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::near_primitives::views::{ReceiptEnumView, SignedTransactionView};
use near_indexer::near_primitives::{borsh, views};
use near_indexer::{IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome};

use serde::{Deserialize, Serialize};

const LAST_BLOCK_HEIGHT_KEY: &str = "last_block_height";
const NUM_TRANSACTIONS_KEY: &str = "num_transactions";
const NUM_RECEIPTS_KEY: &str = "num_receipts";

#[derive(Row, Serialize)]
pub struct TransactionRow {
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
    pub tx_block_hash: String,
    pub tx_block_timestamp: u64,
    pub transaction: String,
    pub last_block_height: u64,
}

#[derive(Row, Serialize)]
pub struct AccountTxRow {
    pub account_id: String,
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
    pub tx_block_timestamp: u64,
}

#[derive(Row, Serialize)]
pub struct BlockTxRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
}

#[derive(Row, Serialize)]
pub struct ReceiptTxRow {
    pub receipt_id: String,
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
    pub tx_block_timestamp: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionView {
    pub transaction: SignedTransactionView,
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipts: Vec<IndexerExecutionOutcomeWithReceipt>,
    pub data_receipts: Vec<views::ReceiptView>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PendingTransaction {
    pub tx_block_height: BlockHeight,
    pub tx_block_hash: CryptoHash,
    pub tx_block_timestamp: u64,
    pub last_block_height: BlockHeight,
    pub transaction: TransactionView,
    pub pending_receipt_ids: Vec<CryptoHash>,
}

impl PendingTransaction {
    pub fn transaction_hash(&self) -> CryptoHash {
        self.transaction.transaction.hash
    }
}

pub struct TransactionsData {
    pub tx_cache: TxCache,
}

impl TransactionsData {
    pub fn new() -> Self {
        let sled_db_path = env::var("SLED_DB_PATH").expect("Missing SLED_DB_PATH env var");
        if !std::path::Path::new(&sled_db_path).exists() {
            std::fs::create_dir_all(&sled_db_path)
                .expect(format!("Failed to create {}", sled_db_path).as_str());
        }
        let sled_db = sled::open(&sled_db_path).expect("Failed to open sled_db_path");
        let tx_cache = TxCache { sled_db };

        Self { tx_cache }
    }

    pub async fn process_block(
        &mut self,
        db: &mut ClickDB,
        block: BlockWithTxHashes,
        skip_missing_receipts: bool,
    ) -> anyhow::Result<()> {
        let block_height = block.block.header.height;
        let block_hash = block.block.header.hash;
        let block_timestamp = block.block.header.timestamp;

        let mut used_receipt_ids = vec![];
        let mut complete_transactions = vec![];
        let mut num_transactions = self.tx_cache.get_u64(NUM_TRANSACTIONS_KEY).unwrap_or(0);
        let mut num_receipts = self.tx_cache.get_u64(NUM_RECEIPTS_KEY).unwrap_or(0);

        for shard in block.shards {
            if let Some(chunk) = shard.chunk {
                for IndexerTransactionWithOutcome {
                    transaction,
                    outcome,
                } in chunk.transactions
                {
                    let pending_receipt_ids = outcome.execution_outcome.outcome.receipt_ids.clone();
                    let pending_transaction = PendingTransaction {
                        tx_block_height: block_height,
                        tx_block_hash: block_hash,
                        tx_block_timestamp: block_timestamp,
                        last_block_height: block_height,
                        transaction: TransactionView {
                            transaction,
                            execution_outcome: outcome.execution_outcome,
                            receipts: vec![],
                            data_receipts: vec![],
                        },
                        pending_receipt_ids,
                    };
                    self.tx_cache.insert_transaction(
                        &pending_transaction,
                        &pending_transaction.pending_receipt_ids,
                    );
                    num_receipts += pending_transaction.pending_receipt_ids.len() as u64;
                    num_transactions += 1;
                }
                for receipt in chunk.receipts {
                    let receipt_id = receipt.receipt_id;
                    match &receipt.receipt {
                        ReceiptEnumView::Action { .. } => {
                            // skipping here, since we'll get one with execution
                        }
                        ReceiptEnumView::Data { .. } => {
                            let tx_hash = match self.tx_cache.peek_receipt_to_tx(&receipt_id) {
                                Some(tx_hash) => tx_hash,
                                None => {
                                    if skip_missing_receipts {
                                        tracing::log::warn!(target: PROJECT_ID, "Missing tx_hash for data receipt_id: {}", receipt_id);
                                        continue;
                                    }
                                    panic!("Missing tx_hash for receipt_id");
                                }
                            };
                            used_receipt_ids.push(receipt_id);
                            let mut pending_transaction = self
                                .tx_cache
                                .get_transaction(&tx_hash)
                                .expect("Missing transaction for data receipt");
                            pending_transaction.transaction.data_receipts.push(receipt);
                            pending_transaction
                                .pending_receipt_ids
                                .retain(|r| r != &receipt_id);
                            pending_transaction.last_block_height = block_height;
                            // Data Receipt can't be the final receipt.
                            self.tx_cache.insert_transaction(&pending_transaction, &[]);
                        }
                    }
                }
            }

            for outcome in shard.receipt_execution_outcomes {
                let receipt = outcome.receipt;
                let execution_outcome = outcome.execution_outcome;
                let receipt_id = receipt.receipt_id;
                let tx_hash = match self.tx_cache.peek_receipt_to_tx(&receipt_id) {
                    Some(tx_hash) => tx_hash,
                    None => {
                        if skip_missing_receipts {
                            tracing::log::warn!(target: PROJECT_ID, "Missing tx_hash for action receipt_id: {}", receipt_id);
                            continue;
                        }
                        panic!("Missing tx_hash for receipt_id");
                    }
                };
                used_receipt_ids.push(receipt_id);
                let mut pending_transaction = self
                    .tx_cache
                    .get_transaction(&tx_hash)
                    .expect("Missing transaction for receipt");
                let pending_receipt_ids = execution_outcome.outcome.receipt_ids.clone();
                pending_transaction
                    .transaction
                    .receipts
                    .push(IndexerExecutionOutcomeWithReceipt {
                        execution_outcome,
                        receipt,
                    });
                pending_transaction.last_block_height = block_height;
                pending_transaction
                    .pending_receipt_ids
                    .retain(|r| r != &receipt_id);
                pending_transaction
                    .pending_receipt_ids
                    .extend(pending_receipt_ids.clone());
                num_receipts += pending_receipt_ids.len() as u64;
                if pending_transaction.pending_receipt_ids.is_empty() {
                    // Received the final receipt.
                    complete_transactions.push(pending_transaction);
                } else {
                    self.tx_cache
                        .insert_transaction(&pending_transaction, &pending_receipt_ids);
                }
            }
        }

        num_transactions -= complete_transactions.len() as u64;
        for transaction in &complete_transactions {
            self.tx_cache
                .remove_transaction(&transaction.transaction_hash());
        }

        num_receipts -= used_receipt_ids.len() as u64;
        for receipt_id in &used_receipt_ids {
            self.tx_cache.remove_receipt_to_tx(receipt_id);
        }

        self.tx_cache
            .set_u64(NUM_TRANSACTIONS_KEY, num_transactions);
        self.tx_cache.set_u64(NUM_RECEIPTS_KEY, num_receipts);
        self.tx_cache.set_u64(LAST_BLOCK_HEIGHT_KEY, block_height);
        self.tx_cache.flush();

        tracing::log::info!(target: PROJECT_ID, "#{}: Complete {} transactions. Pending {} transactions, {} receipts", block_height, complete_transactions.len(), num_transactions, num_receipts);

        for transaction in complete_transactions {
            self.process_transaction(db, transaction).await?;
        }

        // let is_round_block = block_height % crate::actions::SAVE_STEP == 0;
        // if is_round_block {
        //     tracing::log::info!(target: CLICKHOUSE_TARGET, "#{}: Having {} actions, {} events, {} data", block_height, self.rows.actions.len(), self.rows.events.len(), self.rows.data.len());
        // }
        // if self.rows.actions.len() >= db.min_batch || is_round_block {
        //     self.commit_actions(db).await?;
        // }
        // if self.rows.events.len() >= db.min_batch || is_round_block {
        //     self.commit_events(db).await?;
        // }
        // if self.rows.data.len() >= db.min_batch || is_round_block {
        //     self.commit_data(db).await?;
        // }
        Ok(())
    }

    async fn process_transaction(
        &mut self,
        db: &mut ClickDB,
        transaction: PendingTransaction,
    ) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }

    pub async fn commit(&mut self, db: &mut ClickDB) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }
}

pub struct TxCache {
    pub sled_db: sled::Db,
}

impl TxCache {
    pub fn flush(&self) {
        self.sled_db.flush().expect("Failed to flush");
    }

    pub fn peek_receipt_to_tx(&self, receipt_id: &CryptoHash) -> Option<CryptoHash> {
        self.sled_db
            .get(receipt_id)
            .expect("Failed to get")
            .map(into_crypto_hash)
    }

    pub fn store_receipt_to_tx(&self, receipt_id: &CryptoHash, tx_hash: &CryptoHash) -> bool {
        let old_tx_hash = self
            .sled_db
            .insert(receipt_id, tx_hash.0.to_vec())
            .expect("Failed to insert")
            .map(into_crypto_hash);

        if let Some(old_tx_hash) = old_tx_hash {
            assert_eq!(
                &old_tx_hash, tx_hash,
                "Duplicate receipt_id: {} with different TX HASHES!",
                receipt_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate receipt_id: {} old_tx_hash: {} new_tx_hash: {}", receipt_id, old_tx_hash, tx_hash);
            true
        } else {
            false
        }
    }

    fn insert_transaction(
        &mut self,
        pending_transaction: &PendingTransaction,
        pending_receipt_ids: &[CryptoHash],
    ) -> bool {
        let tx_hash = pending_transaction.transaction_hash();
        for receipt_id in pending_receipt_ids {
            self.store_receipt_to_tx(receipt_id, &tx_hash);
        }
        self.sled_db
            .insert(
                &tx_hash,
                serde_json::to_vec(pending_transaction).expect("Failed to serialize"),
            )
            .expect("Failed to set transaction")
            .is_some()
    }

    fn get_transaction(&self, tx_hash: &CryptoHash) -> Option<PendingTransaction> {
        self.sled_db
            .get(tx_hash)
            .expect("Failed to get transaction")
            .map(|v| serde_json::from_slice(&v).expect("Failed to deserialize"))
    }

    fn remove_transaction(&self, tx_hash: &CryptoHash) -> bool {
        self.sled_db
            .remove(tx_hash)
            .expect("Failed to remove transaction")
            .is_some()
    }

    fn remove_receipt_to_tx(&self, receipt_id: &CryptoHash) -> bool {
        self.sled_db
            .remove(receipt_id)
            .expect("Failed to remove receipt_id")
            .is_some()
    }

    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.sled_db
            .get(key)
            .expect("Failed to get")
            .map(|v| u64::try_from_slice(&v).expect("Failed to deserialize"))
    }

    pub fn set_u64(&self, key: &str, value: u64) -> bool {
        self.sled_db
            .insert(key, borsh::to_vec(&value).unwrap())
            .expect("Failed to set")
            .is_some()
    }
}

fn into_crypto_hash(hash: sled::InlineArray) -> CryptoHash {
    let mut result = CryptoHash::default();
    result.0.copy_from_slice(&hash);
    result
}
