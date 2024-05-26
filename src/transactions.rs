use crate::*;
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::str::FromStr;

use clickhouse::Row;
use near_indexer::near_primitives::borsh::BorshDeserialize;
use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::types::{AccountId, BlockHeight};
use near_indexer::near_primitives::views::{ActionView, ReceiptEnumView, SignedTransactionView};
use near_indexer::near_primitives::{borsh, views};
use near_indexer::{IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome};

use serde::{Deserialize, Serialize};
use serde_json::Value;

const LAST_BLOCK_HEIGHT_KEY: &str = "last_block_height";
const NUM_TRANSACTIONS_KEY: &str = "num_transactions";
const NUM_RECEIPTS_KEY: &str = "num_receipts";
const NUM_DATA_RECEIPTS_KEY: &str = "num_data_receipts";
const NUM_HEADERS_KEY: &str = "num_headers";

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

const BLOCK_HEADER_CLEANUP: u64 = 100000;

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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionView {
    pub transaction: SignedTransactionView,
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipts: Vec<IndexerExecutionOutcomeWithReceipt>,
    pub data_receipts: Vec<views::ReceiptView>,
}

fn trim_execution_outcome(execution_outcome: &mut views::ExecutionOutcomeWithIdView) {
    execution_outcome.proof.clear();
    execution_outcome.outcome.metadata.gas_profile = None;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PendingTransaction {
    pub tx_block_height: BlockHeight,
    pub tx_block_hash: CryptoHash,
    pub tx_block_timestamp: u64,
    pub blocks: Vec<BlockHeight>,
    pub transaction: TransactionView,
    pub pending_receipt_ids: Vec<CryptoHash>,
}

#[derive(Default)]
pub struct TxRows {
    pub transactions: Vec<TransactionRow>,
    pub account_txs: Vec<AccountTxRow>,
    pub block_txs: Vec<BlockTxRow>,
    pub receipt_txs: Vec<ReceiptTxRow>,
}

impl PendingTransaction {
    pub fn transaction_hash(&self) -> CryptoHash {
        self.transaction.transaction.hash
    }
}

pub struct TransactionsData {
    pub tx_cache: TxCache,
    pub rows: TxRows,
}

impl TransactionsData {
    pub fn new() -> Self {
        let sled_db_path = env::var("SLED_DB_PATH").expect("Missing SLED_DB_PATH env var");
        if !std::path::Path::new(&sled_db_path).exists() {
            std::fs::create_dir_all(&sled_db_path)
                .expect(format!("Failed to create {}", sled_db_path).as_str());
        }
        let sled_db = sled::open(&sled_db_path).expect("Failed to open sled_db_path");
        let tx_cache = TxCache::new(sled_db);

        Self {
            tx_cache,
            rows: TxRows::default(),
        }
    }

    pub async fn process_block(
        &mut self,
        db: &mut ClickDB,
        block: BlockWithTxHashes,
        last_db_block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let block_height = block.block.header.height;
        let block_hash = block.block.header.hash;
        let block_timestamp = block.block.header.timestamp;

        let skip_missing_receipts = block_height <= last_db_block_height;

        self.tx_cache.insert_block_header(&block.block.header);

        let mut used_receipt_ids = vec![];
        let mut used_data_receipt_ids = vec![];
        let mut complete_transactions = vec![];

        let mut shards = block.shards;
        for shard in &mut shards {
            if let Some(chunk) = shard.chunk.take() {
                for IndexerTransactionWithOutcome {
                    transaction,
                    mut outcome,
                } in chunk.transactions
                {
                    let pending_receipt_ids = outcome.execution_outcome.outcome.receipt_ids.clone();
                    trim_execution_outcome(&mut outcome.execution_outcome);
                    let pending_transaction = PendingTransaction {
                        tx_block_height: block_height,
                        tx_block_hash: block_hash,
                        tx_block_timestamp: block_timestamp,
                        blocks: vec![block_height],
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
                }
                for receipt in chunk.receipts {
                    match &receipt.receipt {
                        ReceiptEnumView::Action { .. } => {
                            // skipping here, since we'll get one with execution
                        }
                        ReceiptEnumView::Data { data_id, .. } => {
                            self.tx_cache.insert_data_receipt(&data_id, &receipt);
                        }
                    }
                }
            }
        }

        for shard in shards {
            for outcome in shard.receipt_execution_outcomes {
                let receipt = outcome.receipt;
                let mut execution_outcome = outcome.execution_outcome;
                trim_execution_outcome(&mut execution_outcome);
                let receipt_id = receipt.receipt_id;
                let tx_hash = match self.tx_cache.get_receipt_to_tx(&receipt_id) {
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
                pending_transaction
                    .pending_receipt_ids
                    .retain(|r| r != &receipt_id);
                if pending_transaction.blocks.last() != Some(&block_height) {
                    pending_transaction.blocks.push(block_height);
                }

                // Extracting matching data receipts
                match &receipt.receipt {
                    ReceiptEnumView::Action { input_data_ids, .. } => {
                        let mut ok = true;
                        for data_id in input_data_ids {
                            let data_receipt = match self.tx_cache.get_data_receipt(data_id) {
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
                            used_data_receipt_ids.push(*data_id);

                            pending_transaction
                                .transaction
                                .data_receipts
                                .push(data_receipt);
                        }
                        if !ok {
                            for receipt_id in &pending_transaction.pending_receipt_ids {
                                self.tx_cache.remove_receipt_to_tx(receipt_id);
                            }
                            self.tx_cache
                                .remove_transaction(&pending_transaction.transaction_hash());
                            continue;
                        }
                    }
                    ReceiptEnumView::Data { .. } => {
                        unreachable!("Data receipt should be processed before")
                    }
                };

                let pending_receipt_ids = execution_outcome.outcome.receipt_ids.clone();
                pending_transaction
                    .transaction
                    .receipts
                    .push(IndexerExecutionOutcomeWithReceipt {
                        execution_outcome,
                        receipt,
                    });
                pending_transaction
                    .pending_receipt_ids
                    .extend(pending_receipt_ids.clone());
                if pending_transaction.pending_receipt_ids.is_empty() {
                    // Received the final receipt.
                    complete_transactions.push(pending_transaction);
                } else {
                    self.tx_cache
                        .insert_transaction(&pending_transaction, &pending_receipt_ids);
                }
            }
        }

        for transaction in &complete_transactions {
            self.tx_cache
                .remove_transaction(&transaction.transaction_hash());
        }

        for receipt_id in &used_receipt_ids {
            self.tx_cache.remove_receipt_to_tx(receipt_id);
        }

        for data_id in &used_data_receipt_ids {
            self.tx_cache.remove_data_receipt(data_id);
        }

        if let Some(last_block_height) = self.tx_cache.get_u64(LAST_BLOCK_HEIGHT_KEY) {
            let diff = block_height.saturating_sub(last_block_height);
            for i in 0..=diff.min(BLOCK_HEADER_CLEANUP) {
                self.tx_cache.remove_block_header(
                    last_block_height.saturating_sub(BLOCK_HEADER_CLEANUP - i),
                );
            }
        }

        self.tx_cache.save_stats();
        self.tx_cache.set_u64(LAST_BLOCK_HEIGHT_KEY, block_height);
        // self.tx_cache.flush();

        tracing::log::info!(target: PROJECT_ID, "#{}: Complete {} transactions. Pending {}", block_height, complete_transactions.len(), self.tx_cache.stats());

        if block_height > last_db_block_height {
            for transaction in complete_transactions {
                self.process_transaction(transaction).await?;
            }
        }

        self.maybe_commit(db, block_height).await?;

        Ok(())
    }

    async fn process_transaction(&mut self, transaction: PendingTransaction) -> anyhow::Result<()> {
        let tx_hash = transaction.transaction_hash().to_string();
        let last_block_height = *transaction.blocks.last().unwrap();
        let signer_id = transaction
            .transaction
            .transaction
            .signer_id
            .clone()
            .to_string();

        for block_height in transaction.blocks {
            let block_header = self.tx_cache.get_block_header(block_height);
            if let Some(block_header) = block_header {
                self.rows.block_txs.push(BlockTxRow {
                    block_height,
                    block_hash: block_header.hash.to_string(),
                    block_timestamp: block_header.timestamp,
                    transaction_hash: tx_hash.clone(),
                    signer_id: signer_id.clone(),
                    tx_block_height: transaction.tx_block_height,
                });
            } else {
                tracing::log::warn!(target: PROJECT_ID, "Missing block header #{} for a transaction {}", block_height, tx_hash.clone());
                // Append to a file a record about a missing
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open("missing_block_headers.txt")
                    .expect("Failed to open missing_block_headers.txt");
                writeln!(
                    file,
                    "{} {} {} {}",
                    block_height, tx_hash, signer_id, transaction.tx_block_height
                )
                .expect("Failed to write to missing_block_headers.txt");
            }
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
            last_block_height,
        });

        // TODO: Save TX to redis

        Ok(())
    }

    pub async fn maybe_commit(
        &mut self,
        db: &mut ClickDB,
        block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "#{}: Having {} transactions, {} account_txs, {} block_txs, {} receipts_txs",
                block_height,
                self.rows.transactions.len(),
                self.rows.account_txs.len(),
                self.rows.block_txs.len(),
                self.rows.receipt_txs.len(),
            );
        }
        if self.rows.transactions.len() >= db.min_batch || is_round_block {
            self.commit(db).await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self, db: &mut ClickDB) -> anyhow::Result<()> {
        if !self.rows.transactions.is_empty() {
            insert_rows_with_retry(&db.client, &self.rows.transactions, "transactions").await?;
            self.rows.transactions.clear();
        }
        if !self.rows.account_txs.is_empty() {
            insert_rows_with_retry(&db.client, &self.rows.account_txs, "account_txs").await?;
            self.rows.account_txs.clear();
        }
        if !self.rows.block_txs.is_empty() {
            insert_rows_with_retry(&db.client, &self.rows.block_txs, "block_txs").await?;
            self.rows.block_txs.clear();
        }
        if !self.rows.receipt_txs.is_empty() {
            insert_rows_with_retry(&db.client, &self.rows.receipt_txs, "receipt_txs").await?;
            self.rows.receipt_txs.clear();
        }

        Ok(())
    }

    pub async fn last_block_height(&mut self, db: &mut ClickDB) -> BlockHeight {
        let db_block = db.max("last_block_height", "block_txs").await.unwrap_or(0);
        let cache_block = self.tx_cache.get_u64(LAST_BLOCK_HEIGHT_KEY).unwrap_or(0);
        db_block.max(cache_block)
    }

    pub fn is_cache_ready(&self, last_block_height: BlockHeight) -> bool {
        let cache_block = self.tx_cache.get_u64(LAST_BLOCK_HEIGHT_KEY).unwrap_or(0);
        cache_block == last_block_height
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

pub struct TxCache {
    pub sled_db: sled::Db,
    pub num_transactions: u64,
    pub num_receipts: u64,
    pub num_data_receipts: u64,
    pub num_headers: u64,
}

impl TxCache {
    pub fn new(sled: sled::Db) -> Self {
        let mut this = Self {
            sled_db: sled,
            num_transactions: 0,
            num_receipts: 0,
            num_data_receipts: 0,
            num_headers: 0,
        };
        this.num_transactions = this.get_u64(NUM_TRANSACTIONS_KEY).unwrap_or(0);
        this.num_receipts = this.get_u64(NUM_RECEIPTS_KEY).unwrap_or(0);
        this.num_data_receipts = this.get_u64(NUM_DATA_RECEIPTS_KEY).unwrap_or(0);
        this.num_headers = this.get_u64(NUM_HEADERS_KEY).unwrap_or(0);

        this
    }

    pub fn stats(&self) -> String {
        format!(
            "{} transactions, {} receipts, {} data receipts, {} headers",
            self.num_transactions, self.num_receipts, self.num_data_receipts, self.num_headers
        )
    }

    pub fn save_stats(&self) {
        self.set_u64(NUM_TRANSACTIONS_KEY, self.num_transactions);
        self.set_u64(NUM_RECEIPTS_KEY, self.num_receipts);
        self.set_u64(NUM_DATA_RECEIPTS_KEY, self.num_data_receipts);
        self.set_u64(NUM_HEADERS_KEY, self.num_headers);
    }

    pub fn flush(&self) {
        self.sled_db.flush().expect("Failed to flush");
    }

    pub fn insert_block_header(&mut self, block_header: &views::BlockHeaderView) -> bool {
        let serialized = serde_json::to_vec(block_header).expect("Failed to serialize");
        let old_header: Option<_> = self
            .sled_db
            .insert(
                format!("header:{}", block_header.height),
                serialized.clone(),
            )
            .expect("Failed to set data receipt");
        if let Some(old_header) = old_header {
            assert_eq!(
                old_header, serialized,
                "Header mismatch at {}!",
                block_header.height
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate header: {}", block_header.height);
            false
        } else {
            self.num_headers += 1;
            true
        }
    }

    pub fn get_block_header(&self, block_height: BlockHeight) -> Option<views::BlockHeaderView> {
        self.sled_db
            .get(format!("header:{}", block_height))
            .expect("Failed to get block header")
            .map(|v| serde_json::from_slice(&v).expect("Failed to deserialize"))
    }

    pub fn remove_block_header(&mut self, block_height: BlockHeight) -> bool {
        let res = self
            .sled_db
            .remove(format!("header:{}", block_height))
            .expect("Failed to remove block header")
            .is_some();
        if res {
            self.num_headers -= 1;
        }
        res
    }

    pub fn get_receipt_to_tx(&self, receipt_id: &CryptoHash) -> Option<CryptoHash> {
        self.sled_db
            .get(receipt_id)
            .expect("Failed to get")
            .map(into_crypto_hash)
    }

    pub fn store_receipt_to_tx(&mut self, receipt_id: &CryptoHash, tx_hash: &CryptoHash) -> bool {
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
            false
        } else {
            self.num_receipts += 1;
            true
        }
    }

    fn remove_receipt_to_tx(&mut self, receipt_id: &CryptoHash) -> bool {
        let res = self
            .sled_db
            .remove(receipt_id)
            .expect("Failed to remove receipt_id")
            .is_some();
        if res {
            self.num_receipts -= 1;
        }
        res
    }

    fn insert_data_receipt(&mut self, data_id: &CryptoHash, receipt: &views::ReceiptView) -> bool {
        let serialized = serde_json::to_vec(receipt).expect("Failed to serialize");
        let old_receipt: Option<_> = self
            .sled_db
            .insert(data_id, serialized.clone())
            .expect("Failed to set data receipt");
        if let Some(old_receipt) = old_receipt {
            assert_eq!(
                old_receipt, serialized,
                "Duplicate data_id: {} with different receipt!",
                data_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate data_id: {}", data_id);
            false
        } else {
            self.num_data_receipts += 1;
            true
        }
    }

    fn get_data_receipt(&self, data_id: &CryptoHash) -> Option<views::ReceiptView> {
        self.sled_db
            .get(data_id)
            .expect("Failed to get data receipt")
            .map(|v| serde_json::from_slice(&v).expect("Failed to deserialize"))
    }

    fn remove_data_receipt(&mut self, data_id: &CryptoHash) -> bool {
        let res = self
            .sled_db
            .remove(data_id)
            .expect("Failed to remove data receipt")
            .is_some();
        if res {
            self.num_data_receipts -= 1;
        }
        res
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
        let res = self
            .sled_db
            .insert(
                &tx_hash,
                serde_json::to_vec(pending_transaction).expect("Failed to serialize"),
            )
            .expect("Failed to set transaction")
            .is_some();
        if !res {
            self.num_transactions += 1;
        }
        res
    }

    fn get_transaction(&self, tx_hash: &CryptoHash) -> Option<PendingTransaction> {
        self.sled_db
            .get(tx_hash)
            .expect("Failed to get transaction")
            .map(|v| serde_json::from_slice(&v).expect("Failed to deserialize"))
    }

    fn remove_transaction(&mut self, tx_hash: &CryptoHash) -> bool {
        let res = self
            .sled_db
            .remove(tx_hash)
            .expect("Failed to remove transaction")
            .is_some();
        if res {
            self.num_transactions -= 1;
        }
        res
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
