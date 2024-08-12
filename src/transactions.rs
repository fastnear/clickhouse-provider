use crate::*;
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;

use clickhouse::Row;
use fastnear_primitives::near_indexer_primitives::IndexerTransactionWithOutcome;
use fastnear_primitives::near_primitives::hash::CryptoHash;
use fastnear_primitives::near_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_primitives::views;
use fastnear_primitives::near_primitives::views::{
    ActionView, ReceiptEnumView, SignedTransactionView,
};

use crate::types::{BlockInfo, ImprovedExecutionOutcome, ImprovedExecutionOutcomeWithReceipt};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

const POTENTIAL_ACCOUNT_ARGS: [&str; 19] = [
    "receiver_id",
    "account_id",
    "sender_id",
    "new_account_id",
    "predecessor_account_id",
    "contract_id",
    "owner_id",
    "token_owner_id",
    "nft_contract_id",
    "token_account_id",
    "creator_id",
    "referral_id",
    "previous_owner_id",
    "seller_id",
    "buyer_id",
    "user_id",
    "beneficiary_id",
    "staking_pool_account_id",
    "owner_account_id",
];

const POTENTIAL_EVENTS_ARGS: [&str; 10] = [
    "account_id",
    "owner_id",
    "old_owner_id",
    "new_owner_id",
    "payer_id",
    "farmer_id",
    "validator_id",
    "liquidation_account_id",
    "contract_id",
    "nft_contract_id",
];

#[allow(dead_code)]
#[derive(Deserialize)]
pub struct EventJson {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Vec<Value>,
}

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

#[derive(Row, Serialize, Deserialize, Clone, Debug)]
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

/// Simplified block view in case there a block with no associated transactions.
/// Also includes some extra metadata.
#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct BlockRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub prev_block_height: Option<u64>,
    pub epoch_id: String,
    pub chunks_included: u64,
    pub prev_block_hash: String,
    pub author_id: String,
    pub signature: String,
    pub protocol_version: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionView {
    pub transaction: SignedTransactionView,
    pub execution_outcome: ImprovedExecutionOutcome,
    pub receipts: Vec<ImprovedExecutionOutcomeWithReceipt>,
    pub data_receipts: Vec<views::ReceiptView>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PendingTransaction {
    pub tx_block_height: BlockHeight,
    pub tx_block_hash: CryptoHash,
    pub tx_block_timestamp: u64,
    pub blocks: Vec<BlockInfo>,
    pub transaction: TransactionView,
    pub pending_receipt_ids: Vec<CryptoHash>,
}

#[derive(Default)]
pub struct TxRows {
    pub transactions: Vec<TransactionRow>,
    pub account_txs: Vec<AccountTxRow>,
    pub block_txs: Vec<BlockTxRow>,
    pub receipt_txs: Vec<ReceiptTxRow>,
    pub blocks: Vec<BlockRow>,
}

impl PendingTransaction {
    pub fn transaction_hash(&self) -> CryptoHash {
        self.transaction.transaction.hash
    }
}

pub struct TransactionsData {
    pub commit_every_block: bool,
    pub tx_cache: TxCache,
    pub rows: TxRows,
    pub commit_handlers: Vec<tokio::task::JoinHandle<Result<(), clickhouse::error::Error>>>,
}

impl TransactionsData {
    pub fn new() -> Self {
        let commit_every_block = env::var("COMMIT_EVERY_BLOCK")
            .map(|v| v == "true")
            .unwrap_or(false);
        let tx_cache = TxCache::new();

        Self {
            commit_every_block,
            tx_cache,
            rows: TxRows::default(),
            commit_handlers: vec![],
        }
    }

    pub async fn process_block(
        &mut self,
        db: &ClickDB,
        block: BlockWithTxHashes,
        last_db_block_height: BlockHeight,
        prev_block_hash: Option<CryptoHash>,
    ) -> anyhow::Result<CryptoHash> {
        let block_height = block.block.header.height;
        let block_hash = block.block.header.hash;
        let block_timestamp = block.block.header.timestamp;
        if let Some(prev_block_hash) = prev_block_hash {
            assert_eq!(
                prev_block_hash, block.block.header.prev_hash,
                "Invalid prev_block_hash for block height {}",
                block_height
            );
        }
        let block_info = BlockInfo {
            block_height,
            block_hash: block_hash.clone(),
            block_timestamp,
        };
        let block_row = BlockRow {
            block_height,
            block_hash: block_hash.to_string(),
            block_timestamp,
            prev_block_height: block.block.header.prev_height,
            epoch_id: block.block.header.epoch_id.to_string(),
            chunks_included: block.block.header.chunks_included,
            prev_block_hash: block.block.header.prev_hash.to_string(),
            author_id: block.block.author.to_string(),
            signature: block.block.header.signature.to_string(),
            protocol_version: block.block.header.latest_protocol_version,
        };

        let skip_missing_receipts = block_height <= last_db_block_height;

        let mut complete_transactions = vec![];

        let mut shards = block.shards;
        for shard in &mut shards {
            if let Some(chunk) = shard.chunk.take() {
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
                        blocks: vec![block_info.clone()],
                        transaction: TransactionView {
                            transaction,
                            execution_outcome: ImprovedExecutionOutcome::from_outcome(
                                outcome.execution_outcome,
                                block_timestamp,
                                block_height,
                            ),
                            receipts: vec![],
                            data_receipts: vec![],
                        },
                        pending_receipt_ids,
                    };
                    let pending_receipt_ids = pending_transaction.pending_receipt_ids.clone();
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                }
                for receipt in chunk.receipts {
                    match receipt.receipt {
                        ReceiptEnumView::Action { .. } => {
                            // skipping here, since we'll get one with execution
                        }
                        ReceiptEnumView::Data { data_id, .. } => {
                            self.tx_cache.insert_data_receipt(&data_id, receipt);
                        }
                    }
                }
            }
        }

        for shard in shards {
            for outcome in shard.receipt_execution_outcomes {
                let receipt = outcome.receipt;
                let execution_outcome = outcome.execution_outcome;
                let receipt_id = receipt.receipt_id;
                let tx_hash = match self.tx_cache.get_and_remove_receipt_to_tx(&receipt_id) {
                    Some(tx_hash) => tx_hash,
                    None => {
                        if skip_missing_receipts {
                            tracing::log::warn!(target: PROJECT_ID, "Missing tx_hash for action receipt_id: {}", receipt_id);
                            continue;
                        }
                        panic!(
                            "Missing tx_hash for receipt_id {} at block {}",
                            receipt_id, block_height
                        );
                    }
                };
                let mut pending_transaction = self
                    .tx_cache
                    .get_and_remove_transaction(&tx_hash)
                    .expect("Missing transaction for receipt");
                pending_transaction
                    .pending_receipt_ids
                    .retain(|r| r != &receipt_id);
                if pending_transaction
                    .blocks
                    .last()
                    .as_ref()
                    .unwrap()
                    .block_height
                    != block_height
                {
                    pending_transaction.blocks.push(block_info.clone());
                }

                // Extracting matching data receipts
                match &receipt.receipt {
                    ReceiptEnumView::Action { input_data_ids, .. } => {
                        let mut ok = true;
                        for data_id in input_data_ids {
                            let data_receipt = match self
                                .tx_cache
                                .get_and_remove_data_receipt(data_id)
                            {
                                Some(data_receipt) => data_receipt,
                                None => {
                                    if skip_missing_receipts {
                                        tracing::log::warn!(target: PROJECT_ID, "Missing data receipt for data_id: {}", data_id);
                                        ok = false;
                                        break;
                                    }
                                    panic!("Missing data receipt for data_id");
                                }
                            };

                            pending_transaction
                                .transaction
                                .data_receipts
                                .push(data_receipt);
                        }
                        if !ok {
                            for receipt_id in &pending_transaction.pending_receipt_ids {
                                self.tx_cache.remove_receipt_to_tx(receipt_id);
                            }
                            continue;
                        }
                    }
                    ReceiptEnumView::Data { .. } => {
                        unreachable!("Data receipt should be processed before")
                    }
                };

                let pending_receipt_ids = execution_outcome.outcome.receipt_ids.clone();
                pending_transaction.transaction.receipts.push(
                    ImprovedExecutionOutcomeWithReceipt {
                        execution_outcome: ImprovedExecutionOutcome::from_outcome(
                            execution_outcome,
                            block_timestamp,
                            block_height,
                        ),
                        receipt,
                    },
                );
                pending_transaction
                    .pending_receipt_ids
                    .extend(pending_receipt_ids.clone());
                if pending_transaction.pending_receipt_ids.is_empty() {
                    // Received the final receipt.
                    complete_transactions.push(pending_transaction);
                } else {
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                }
            }
        }

        self.tx_cache.last_block_height = block_height;

        tracing::log::info!(target: PROJECT_ID, "#{}: Complete {} transactions. Pending {}", block_height, complete_transactions.len(), self.tx_cache.stats());

        if block_height > last_db_block_height {
            self.rows.blocks.push(block_row);
            for transaction in complete_transactions {
                self.process_transaction(transaction).await?;
            }
        }

        self.maybe_commit(db, block_height).await?;

        Ok(block_hash)
    }

    async fn process_transaction(&mut self, transaction: PendingTransaction) -> anyhow::Result<()> {
        let tx_hash = transaction.transaction_hash().to_string();
        let last_block_info = transaction.blocks.last().cloned().unwrap();
        let signer_id = transaction
            .transaction
            .transaction
            .signer_id
            .clone()
            .to_string();

        for block_info in transaction.blocks {
            self.rows.block_txs.push(BlockTxRow {
                block_height: block_info.block_height,
                block_hash: block_info.block_hash.to_string(),
                block_timestamp: block_info.block_timestamp,
                transaction_hash: tx_hash.clone(),
                signer_id: signer_id.clone(),
                tx_block_height: transaction.tx_block_height,
            });
        }

        let mut accounts = HashSet::new();
        accounts.insert(transaction.transaction.transaction.signer_id.clone());
        for receipt in &transaction.transaction.receipts {
            let receipt_id = receipt.receipt.receipt_id.to_string();
            self.rows.receipt_txs.push(ReceiptTxRow {
                receipt_id,
                transaction_hash: tx_hash.clone(),
                signer_id: signer_id.clone(),
                tx_block_height: transaction.tx_block_height,
                tx_block_timestamp: transaction.tx_block_timestamp,
            });
            add_accounts_from_receipt(&mut accounts, &receipt.receipt);
            add_accounts_from_logs(&mut accounts, &receipt.execution_outcome.outcome.logs);
        }
        for data_receipt in &transaction.transaction.data_receipts {
            let receipt_id = data_receipt.receipt_id.to_string();
            self.rows.receipt_txs.push(ReceiptTxRow {
                receipt_id,
                transaction_hash: tx_hash.clone(),
                signer_id: signer_id.clone(),
                tx_block_height: transaction.tx_block_height,
                tx_block_timestamp: transaction.tx_block_timestamp,
            });
        }

        for account_id in accounts {
            self.rows.account_txs.push(AccountTxRow {
                account_id: account_id.to_string(),
                transaction_hash: tx_hash.clone(),
                signer_id: signer_id.clone(),
                tx_block_height: transaction.tx_block_height,
                tx_block_timestamp: transaction.tx_block_timestamp,
            });
        }

        self.rows.transactions.push(TransactionRow {
            transaction_hash: tx_hash.clone(),
            signer_id: signer_id.clone(),
            tx_block_height: transaction.tx_block_height,
            tx_block_hash: transaction.tx_block_hash.to_string(),
            tx_block_timestamp: transaction.tx_block_timestamp,
            transaction: serde_json::to_string(&transaction.transaction).unwrap(),
            last_block_height: last_block_info.block_height,
        });

        // TODO: Save TX to redis

        Ok(())
    }

    pub async fn maybe_commit(
        &mut self,
        db: &ClickDB,
        block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "#{}: Having {} transactions, {} account_txs, {} block_txs, {} receipts_txs, {} blocks",
                block_height,
                self.rows.transactions.len(),
                self.rows.account_txs.len(),
                self.rows.block_txs.len(),
                self.rows.receipt_txs.len(),
                self.rows.blocks.len(),
            );
        }
        if self.rows.transactions.len() >= db.min_batch || is_round_block || self.commit_every_block
        {
            self.commit(db).await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self, db: &ClickDB) -> anyhow::Result<()> {
        let mut rows = TxRows::default();
        std::mem::swap(&mut rows, &mut self.rows);
        while self.commit_handlers.len() >= MAX_COMMIT_HANDLERS {
            self.commit_handlers.remove(0).await??;
        }
        let db = db.clone();
        let handler = tokio::spawn(async move {
            if !rows.transactions.is_empty() {
                insert_rows_with_retry(&db.client, &rows.transactions, "transactions").await?;
            }
            if !rows.account_txs.is_empty() {
                insert_rows_with_retry(&db.client, &rows.account_txs, "account_txs").await?;
            }
            if !rows.block_txs.is_empty() {
                insert_rows_with_retry(&db.client, &rows.block_txs, "block_txs").await?;
            }
            if !rows.receipt_txs.is_empty() {
                insert_rows_with_retry(&db.client, &rows.receipt_txs, "receipt_txs").await?;
            }
            if !rows.blocks.is_empty() {
                insert_rows_with_retry(&db.client, &rows.blocks, "blocks").await?;
            }
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "Committed {} transactions, {} account_txs, {} block_txs, {} receipts_txs, {} blocks",
                rows.transactions.len(),
                rows.account_txs.len(),
                rows.block_txs.len(),
                rows.receipt_txs.len(),
                rows.blocks.len(),
            );
            Ok::<(), clickhouse::error::Error>(())
        });
        self.commit_handlers.push(handler);

        Ok(())
    }

    pub async fn last_block_height(&mut self, db: &ClickDB) -> BlockHeight {
        db.max("block_height", "blocks").await.unwrap_or(0)
    }

    pub fn is_cache_ready(&self, _last_block_height: BlockHeight) -> bool {
        false
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        while let Some(handler) = self.commit_handlers.pop() {
            handler.await??;
        }
        Ok(())
    }
}

fn extract_accounts(accounts: &mut HashSet<AccountId>, value: &Value, keys: &[&str]) {
    for arg in keys {
        if let Some(account_id) = value.get(arg) {
            if let Some(account_id) = account_id.as_str() {
                if let Ok(account_id) = AccountId::from_str(account_id) {
                    accounts.insert(account_id);
                }
            }
        }
    }
}

fn add_accounts_from_logs(accounts: &mut HashSet<AccountId>, logs: &[String]) {
    for log in logs {
        if log.starts_with(EVENT_JSON_PREFIX) {
            let event_json = &log[EVENT_JSON_PREFIX.len()..];
            if let Ok(event) = serde_json::from_str::<EventJson>(event_json) {
                for data in &event.data {
                    extract_accounts(accounts, data, &POTENTIAL_EVENTS_ARGS);
                }
            }
        }
    }
}

fn add_accounts_from_receipt(accounts: &mut HashSet<AccountId>, receipt: &views::ReceiptView) {
    accounts.insert(receipt.receiver_id.clone());
    match &receipt.receipt {
        ReceiptEnumView::Action { actions, .. } => {
            for action in actions {
                match action {
                    ActionView::FunctionCall { args, .. } => {
                        if let Ok(args) = serde_json::from_slice::<Value>(&args) {
                            extract_accounts(accounts, &args, &POTENTIAL_ACCOUNT_ARGS);
                        }
                    }
                    _ => {}
                }
            }
        }
        ReceiptEnumView::Data { .. } => {}
    }
}

#[derive(Default)]
pub struct TxCache {
    pub receipt_to_tx: HashMap<CryptoHash, CryptoHash>,
    pub data_receipts: HashMap<CryptoHash, views::ReceiptView>,
    pub transactions: HashMap<CryptoHash, PendingTransaction>,
    pub last_block_height: BlockHeight,
}

impl TxCache {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn stats(&self) -> String {
        format!(
            "mem: {} tx, {} r, {} dr",
            self.transactions.len(),
            self.receipt_to_tx.len(),
            self.data_receipts.len(),
        )
    }

    pub fn get_and_remove_receipt_to_tx(&mut self, receipt_id: &CryptoHash) -> Option<CryptoHash> {
        self.receipt_to_tx.remove(receipt_id)
    }

    pub fn insert_receipt_to_tx(&mut self, receipt_id: &CryptoHash, tx_hash: CryptoHash) {
        // In-memory insert.
        let old_tx_hash = self.receipt_to_tx.insert(*receipt_id, tx_hash);
        if let Some(old_tx_hash) = old_tx_hash {
            assert_eq!(
                old_tx_hash, tx_hash,
                "Duplicate receipt_id: {} with different TX HASHES!",
                receipt_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate receipt_id: {} old_tx_hash: {} new_tx_hash: {}", receipt_id, old_tx_hash, tx_hash);
        }
    }

    fn remove_receipt_to_tx(&mut self, receipt_id: &CryptoHash) {
        self.receipt_to_tx.remove(receipt_id);
    }

    fn insert_data_receipt(&mut self, data_id: &CryptoHash, receipt: views::ReceiptView) {
        let receipt_id = receipt.receipt_id;
        let old_receipt = self.data_receipts.insert(*data_id, receipt);
        // In-memory insert.
        if let Some(old_receipt) = old_receipt {
            assert_eq!(
                old_receipt.receipt_id, receipt_id,
                "Duplicate data_id: {} with different receipt_id!",
                data_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate data_id: {}", data_id);
        }
    }

    fn get_and_remove_data_receipt(&mut self, data_id: &CryptoHash) -> Option<views::ReceiptView> {
        self.data_receipts.remove(data_id)
    }

    fn insert_transaction(
        &mut self,
        pending_transaction: PendingTransaction,
        pending_receipt_ids: &[CryptoHash],
    ) {
        let tx_hash = pending_transaction.transaction_hash();
        for receipt_id in pending_receipt_ids {
            self.insert_receipt_to_tx(receipt_id, tx_hash);
        }

        self.transactions.insert(tx_hash, pending_transaction);
    }

    fn get_and_remove_transaction(&mut self, tx_hash: &CryptoHash) -> Option<PendingTransaction> {
        self.transactions.remove(tx_hash)
    }
}
