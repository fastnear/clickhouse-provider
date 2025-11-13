use crate::*;
use clickhouse::Row;
use fastnear_primitives::near_indexer_primitives::views::ExecutionStatusView;
use fastnear_primitives::near_indexer_primitives::IndexerTransactionWithOutcome;
use fastnear_primitives::near_primitives::hash::CryptoHash;
use fastnear_primitives::near_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_primitives::views::{
    ActionView, ReceiptEnumView, SignedTransactionView,
};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::str::FromStr;
use std::{env, mem};

use crate::types::{
    BlockInfo, ImprovedExecutionOutcome, ImprovedExecutionOutcomeWithReceipt, ImprovedReceiptView,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";
const SYSTEM_ACCOUNT_ID: &str = "system";

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

/*
   transaction_hash   String COMMENT 'Transaction hash',
   signer_id          String COMMENT 'The account ID of the transaction signer',
   tx_block_height    UInt64 COMMENT 'The block height when the transaction was included',
   tx_index           UInt32 COMMENT 'The index of the transaction in the block',
   tx_block_hash      String COMMENT 'The block hash when the transaction was included',
   tx_block_timestamp DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
   last_block_height  UInt64 COMMENT 'The block height when the last receipt was processed for the transaction',
   is_completed       Bool COMMENT 'Whether the transaction has all the data or still pending some receipts',
   shard_id           Uint64 COMMENT 'The shard ID where the transaction was included',
   receiver_id        String COMMENT 'The account ID of the transaction receiver',
   signer_public_key  String COMMENT 'The public key of the transaction signer',
   priority_fee       UInt64 COMMENT 'The priority fee of the transaction',
   nonce              UInt64 COMMENT 'The nonce of the transaction',
   is_relayed         Bool COMMENT 'Whether the transaction is relayed or not',
   real_signer_id     String COMMENT 'The account ID of the signer of the delegated transaction action, if applicable. Otherwise same as signer_id',
   real_receiver_id   String COMMENT 'The account ID of the receiver of the delegated transaction action, if applicable. Otherwise same as receiver_id',
   is_success         Bool COMMENT 'Whether the transaction execution was successful or not. Pending transactions are considered not successful',
*/
#[derive(Row, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionRow {
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
    pub tx_index: u32,
    pub tx_block_hash: String,
    pub tx_block_timestamp: u64,
    pub last_block_height: u64,
    pub is_completed: bool,
    pub shard_id: u64,
    pub receiver_id: String,
    pub signer_public_key: String,
    pub priority_fee: u64,
    pub nonce: u64,
    pub is_relayed: bool,
    pub real_signer_id: String,
    pub real_receiver_id: String,
    pub is_success: bool,
}

/*
   account_id          String COMMENT 'The account ID',
   transaction_hash    String COMMENT 'The transaction hash',
   tx_block_height     UInt64 COMMENT 'The block height when the transaction was included into the blockchain',
   tx_block_timestamp  DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
   tx_index            UInt32 COMMENT 'The index of the transaction in the block',
   is_signer           Bool COMMENT 'True if the account signed the transaction',
   is_delegated_signer Bool COMMENT 'True if the account was the signer of the delegated transaction action',
   is_real_signer      Bool COMMENT 'True if the account was the real signer of the transaction (either direct or delegated, excluding relayer signer)',
   is_any_signer       Bool COMMENT 'True if the account was the signer of the delegated transaction action or the signer of the transaction',
   is_predecessor      Bool COMMENT 'True if the account was the predecessor of the receipt',
   is_receiver         Bool COMMENT 'True if the account was the receiver of the receipt',
   is_real_receiver    Bool COMMENT 'True if the account was the receiver of the receipt (excluding relayer receiver and gas refunds)',
   is_function_call    Bool COMMENT 'True if the account was the target of a function call action',
   is_action_arg       Bool COMMENT 'True if the account was involved in action arguments',
   is_event_log        Bool COMMENT 'True if the account was involved in JSON event logs',
   is_success          Bool COMMENT 'Whether the transaction execution was successful or not. Pending transactions are considered not successful',
*/
#[derive(Row, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct AccountTxRow {
    pub account_id: String,
    pub transaction_hash: String,
    pub tx_block_height: u64,
    pub tx_block_timestamp: u64,
    pub tx_index: u32,
    pub is_signer: bool,
    pub is_delegated_signer: bool,
    pub is_real_signer: bool,
    pub is_any_signer: bool,
    pub is_predecessor: bool,
    pub is_receiver: bool,
    pub is_real_receiver: bool,
    pub is_function_call: bool,
    pub is_action_arg: bool,
    pub is_event_log: bool,
    pub is_success: bool,
}

impl AccountTxRow {
    pub fn set_signer(&mut self) -> &mut Self {
        self.is_signer = true;
        self
    }

    pub fn set_delegated_signer(&mut self) -> &mut Self {
        self.is_delegated_signer = true;
        self
    }

    pub fn set_real_signer(&mut self) -> &mut Self {
        self.is_real_signer = true;
        self
    }

    pub fn set_any_signer(&mut self) -> &mut Self {
        self.is_any_signer = true;
        self
    }

    pub fn set_predecessor(&mut self) -> &mut Self {
        self.is_predecessor = true;
        self
    }

    pub fn set_receiver(&mut self) -> &mut Self {
        self.is_receiver = true;
        self
    }

    pub fn set_real_receiver(&mut self) -> &mut Self {
        self.is_real_receiver = true;
        self
    }

    pub fn set_function_call(&mut self) -> &mut Self {
        self.is_function_call = true;
        self
    }

    pub fn set_action_arg(&mut self) -> &mut Self {
        self.is_action_arg = true;
        self
    }

    pub fn set_event_log(&mut self) -> &mut Self {
        self.is_event_log = true;
        self
    }
}

/*
   receipt_id           String COMMENT 'The receipt hash',
   receipt_block_height UInt64 COMMENT 'The block height when the receipt was executed',
   receipt_index        UInt32 COMMENT 'Index of the receipt that appears in the block across all shards',
   appear_block_height  UInt64 COMMENT 'The block height when the receipt first appeared (e.g. data receipts appear earlier)',
   appear_receipt_index UInt32 COMMENT 'Index of the receipt that first appeared in the block across all shards',
   transaction_hash     String COMMENT 'The transaction hash',
   tx_block_height      UInt64 COMMENT 'The block height when the transaction was included',
   tx_block_timestamp   DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
   tx_index             UInt32 COMMENT 'The index of the transaction in the block',
   predecessor_id       String COMMENT 'The account ID of the receipt predecessor',
   receiver_id          String COMMENT 'The account ID of where the receipt is executed',
   receipt_type         LowCardinality(String) COMMENT 'The type of the receipt: Action, Data, GlobalContractDistribution',
   priority             Uint64 COMMENT 'The priority of the receipt',
   shard_id           Uint64 COMMENT 'The shard ID where the receipt was included',
*/
#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct ReceiptTxRow {
    pub receipt_id: String,
    pub receipt_block_height: u64,
    pub receipt_index: u32,
    pub appear_block_height: u64,
    pub appear_receipt_index: u32,
    pub transaction_hash: String,
    pub tx_block_height: u64,
    pub tx_block_timestamp: u64,
    pub tx_index: u32,
    pub predecessor_id: String,
    pub receiver_id: String,
    pub receipt_type: String,
    pub priority: u64,
    pub shard_id: u64,
}

impl ReceiptTxRow {
    pub fn new(
        receipt: &ImprovedReceiptView,
        receipt_index: u32,
        pending_transaction: &PendingTransaction,
        block_info: &BlockInfo,
        shard_id: u64,
    ) -> Self {
        let receipt_type = match &receipt.receipt {
            ReceiptEnumView::Action { .. } => "Action",
            ReceiptEnumView::Data { .. } => "Data",
            ReceiptEnumView::GlobalContractDistribution { .. } => "GlobalContractDistribution",
        }
        .to_string();
        Self {
            receipt_id: receipt.receipt_id.to_string(),
            receipt_block_height: block_info.block_height,
            receipt_index,
            appear_block_height: receipt.block_height,
            appear_receipt_index: receipt.receipt_index,
            transaction_hash: pending_transaction.transaction_hash().to_string(),
            tx_block_height: pending_transaction.tx_block_height,
            tx_block_timestamp: pending_transaction.tx_block_timestamp,
            tx_index: pending_transaction.tx_index,
            predecessor_id: receipt.predecessor_id.to_string(),
            receiver_id: receipt.receiver_id.to_string(),
            receipt_type,
            priority: receipt.priority,
            shard_id,
        }
    }
}

/*
   block_height      UInt64 COMMENT 'The block height',
   prev_block_height Nullable(UInt64) COMMENT 'The previous block height',
   block_hash        String COMMENT 'The block hash',
   prev_block_hash   String COMMENT 'The previous block hash',
   block_timestamp   DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC',
   epoch_id          String COMMENT 'The epoch ID',
   next_epoch_id     String COMMENT 'The next epoch ID',
   chunks_included   UInt64 COMMENT 'The number of chunks included in the block',
   author_id         String COMMENT 'The account ID of the block author',
   protocol_version  UInt32 COMMENT 'The protocol version',
   gas_price         UInt128 COMMENT 'The gas price in yoctoNEAR',
   block_ordinal     Nullable(UInt64) COMMENT 'The block ordinal in the chain',
   total_supply      UInt128 COMMENT 'The total supply in yoctoNEAR at this block',
   num_transactions  UInt32 COMMENT 'The number of transactions in the block (executed)',
   num_receipts      UInt32 COMMENT 'The number of receipts in the block (executed or used)',
*/
#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct BlockRow {
    pub block_height: u64,
    pub prev_block_height: Option<u64>,
    pub block_hash: String,
    pub prev_block_hash: String,
    pub block_timestamp: u64,
    pub epoch_id: String,
    pub next_epoch_id: String,
    pub chunks_included: u64,
    pub author_id: String,
    pub protocol_version: u32,
    pub gas_price: u128,
    pub block_ordinal: Option<u64>,
    pub total_supply: u128,
    pub num_transactions: u32,
    pub num_receipts: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionView {
    pub transaction: SignedTransactionView,
    pub execution_outcome: ImprovedExecutionOutcome,
    pub receipts: Vec<ImprovedExecutionOutcomeWithReceipt>,
    pub data_receipts: Vec<ImprovedReceiptView>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Accounts(pub HashMap<String, AccountTxRow>);

impl Accounts {
    pub fn row(&mut self, account_id: &str) -> &mut AccountTxRow {
        self.0.entry(account_id.to_string()).or_default()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PendingTransaction {
    pub tx_block_height: BlockHeight,
    pub tx_block_hash: CryptoHash,
    pub tx_block_timestamp: u64,
    pub tx_index: u32,
    pub shard_id: u64,
    pub last_block_height: BlockHeight,
    pub transaction: TransactionView,
    pub pending_receipt_ids: Vec<CryptoHash>,

    pub committed_tx_row: Option<TransactionRow>,
    pub committed_account_tx_rows: Accounts,
}

#[derive(Default)]
pub struct TxRows {
    pub tx_rows: Vec<TransactionRow>,
    pub account_txs: Vec<AccountTxRow>,
    pub receipt_txs: Vec<ReceiptTxRow>,
    pub blocks: Vec<BlockRow>,
    pub transactions: Vec<(String, String)>,
}

impl PendingTransaction {
    pub fn transaction_hash(&self) -> CryptoHash {
        self.transaction.transaction.hash
    }
}

pub struct TransactionsData {
    pub commit_every_block: bool,
    pub is_backfill: bool,
    pub tx_cache: TxCache,
    pub rows: TxRows,
    pub commit_handlers: Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>,
}

impl TransactionsData {
    pub fn new(is_backfill: bool) -> Self {
        let commit_every_block = env::var("COMMIT_EVERY_BLOCK")
            .map(|v| v == "true")
            .unwrap_or(false);
        let tx_cache = TxCache::new();

        Self {
            commit_every_block,
            is_backfill,
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
        let mut block_row = BlockRow {
            block_height,
            block_hash: block_hash.to_string(),
            block_timestamp,
            prev_block_height: block.block.header.prev_height,
            epoch_id: block.block.header.epoch_id.to_string(),
            next_epoch_id: block.block.header.next_epoch_id.to_string(),
            chunks_included: block.block.header.chunks_included,
            prev_block_hash: block.block.header.prev_hash.to_string(),
            author_id: block.block.author.to_string(),
            protocol_version: block.block.header.latest_protocol_version,
            gas_price: block.block.header.gas_price,
            block_ordinal: block.block.header.block_ordinal,
            total_supply: block.block.header.total_supply,
            num_transactions: 0,
            num_receipts: 0,
        };

        let mut pending_receipt_txs = vec![];

        let catching_up = block_height <= last_db_block_height;

        let mut transactions_to_commit = vec![];
        let mut tx_index = 0u32;
        let mut appear_receipt_index = 0u32;
        let mut receipt_index = 0u32;

        let mut shards = block.shards;
        for shard in &mut shards {
            if let Some(chunk) = shard.chunk.take() {
                let shard_id: u64 = chunk.header.shard_id.into();
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
                        tx_index,
                        shard_id,
                        last_block_height: block_height,
                        transaction: TransactionView {
                            transaction,
                            execution_outcome: ImprovedExecutionOutcome::from_outcome(
                                outcome.execution_outcome,
                                block_timestamp,
                                block_height,
                                tx_index,
                            ),
                            receipts: vec![],
                            data_receipts: vec![],
                        },
                        pending_receipt_ids,
                        committed_tx_row: None,
                        committed_account_tx_rows: Default::default(),
                    };
                    tx_index += 1;
                    let pending_receipt_ids = pending_transaction.pending_receipt_ids.clone();
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                }
                for receipt in chunk.receipts {
                    let receipt = ImprovedReceiptView::from_receipt(
                        receipt,
                        appear_receipt_index,
                        &block_info,
                    );
                    appear_receipt_index += 1;
                    match receipt.receipt {
                        ReceiptEnumView::Action { .. } => {
                            self.tx_cache.insert_action_receipt(receipt);
                        }
                        ReceiptEnumView::Data { data_id, .. } => {
                            self.tx_cache.insert_data_receipt(&data_id, receipt);
                        }
                        ReceiptEnumView::GlobalContractDistribution { .. } => {
                            // Global contract distribution receipts don't have the associated transaction
                            // so we skip them here.
                        }
                    }
                }
            }
        }

        for shard in shards {
            let shard_id: u64 = shard
                .chunk
                .as_ref()
                .map(|c| c.header.shard_id)
                .unwrap_or(shard.shard_id)
                .into();
            for outcome in shard.receipt_execution_outcomes {
                let receipt = outcome.receipt;
                let execution_outcome = outcome.execution_outcome;
                let receipt_id = receipt.receipt_id;
                let tx_hash = match self.tx_cache.get_and_remove_receipt_to_tx(&receipt_id) {
                    Some(tx_hash) => tx_hash,
                    None => {
                        if catching_up {
                            tracing::log::warn!(target: PROJECT_ID, "Missing tx_hash for action receipt_id: {}", receipt_id);
                            // Remove the action receipt from cache to avoid memory leak (if exists).
                            self.tx_cache.remove_action_receipt(&receipt_id);
                            continue;
                        }
                        panic!(
                            "Missing tx_hash for receipt_id {} at block {}",
                            receipt_id, block_height
                        );
                    }
                };
                let action_receipt = self
                    .tx_cache
                    .remove_action_receipt(&receipt_id)
                    .expect("Missing action receipt");
                let mut pending_transaction = self
                    .tx_cache
                    .get_and_remove_transaction(&tx_hash)
                    .expect("Missing transaction for receipt");
                pending_transaction
                    .pending_receipt_ids
                    .retain(|r| r != &receipt_id);

                // Extracting matching data receipts
                match &receipt.receipt {
                    ReceiptEnumView::Action { input_data_ids, .. } => {
                        let mut ok = true;
                        for data_id in input_data_ids {
                            let current_receipt_index = receipt_index;
                            receipt_index += 1;
                            let data_receipt = match self
                                .tx_cache
                                .get_and_remove_data_receipt(data_id)
                            {
                                Some(data_receipt) => data_receipt,
                                None => {
                                    if catching_up {
                                        tracing::log::warn!(target: PROJECT_ID, "Missing data receipt for data_id: {}", data_id);
                                        ok = false;
                                        continue;
                                    }
                                    panic!("Missing data receipt for data_id: {}", data_id);
                                }
                            };
                            if ok {
                                pending_receipt_txs.push(ReceiptTxRow::new(
                                    &data_receipt,
                                    current_receipt_index,
                                    &pending_transaction,
                                    &block_info,
                                    shard_id,
                                ));

                                pending_transaction
                                    .transaction
                                    .data_receipts
                                    .push(data_receipt);
                            }
                        }
                        if !ok {
                            for receipt_id in &pending_transaction.pending_receipt_ids {
                                self.tx_cache.remove_receipt_to_tx(receipt_id);
                                self.tx_cache.remove_action_receipt(receipt_id);
                            }
                            receipt_index += 1;
                            continue;
                        }
                    }
                    ReceiptEnumView::Data { .. } => {
                        unreachable!("Data receipt should be processed before")
                    }
                    ReceiptEnumView::GlobalContractDistribution { .. } => {
                        unreachable!(
                            "GlobalContractDistribution receipt should not have execution outcome"
                        )
                    }
                };

                let current_receipt_index = receipt_index;
                pending_receipt_txs.push(ReceiptTxRow::new(
                    &action_receipt,
                    current_receipt_index,
                    &pending_transaction,
                    &block_info,
                    shard_id,
                ));
                receipt_index += 1;
                let pending_receipt_ids = execution_outcome.outcome.receipt_ids.clone();
                pending_transaction.transaction.receipts.push(
                    ImprovedExecutionOutcomeWithReceipt {
                        execution_outcome: ImprovedExecutionOutcome::from_outcome(
                            execution_outcome,
                            block_timestamp,
                            block_height,
                            current_receipt_index,
                        ),
                        receipt: action_receipt,
                    },
                );
                pending_transaction
                    .pending_receipt_ids
                    .extend(pending_receipt_ids.clone());
                if (self.is_backfill || catching_up)
                    && !pending_transaction.pending_receipt_ids.is_empty()
                {
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                } else {
                    transactions_to_commit.push((pending_transaction, pending_receipt_ids));
                }
            }
        }

        self.tx_cache.last_block_height = block_height;
        block_row.num_transactions = tx_index;
        block_row.num_receipts = receipt_index;

        tracing::log::info!(target: PROJECT_ID, "#{}: {} transactions to commit. Pending {}", block_height, transactions_to_commit.len(), self.tx_cache.stats());

        if !catching_up {
            self.rows.receipt_txs.extend(pending_receipt_txs);
            self.rows.blocks.push(block_row);
            for (mut transaction, pending_receipt_ids) in transactions_to_commit {
                self.process_transaction(&mut transaction);
                if !transaction.pending_receipt_ids.is_empty() {
                    self.tx_cache
                        .insert_transaction(transaction, &pending_receipt_ids);
                }
            }
        }

        self.maybe_commit(db, block_height).await?;

        Ok(block_hash)
    }

    fn process_transaction(&mut self, transaction: &mut PendingTransaction) {
        let tx_hash = transaction.transaction_hash().to_string();
        let signer_id = transaction
            .transaction
            .transaction
            .signer_id
            .clone()
            .to_string();
        let receiver_id = transaction
            .transaction
            .transaction
            .receiver_id
            .clone()
            .to_string();
        let delegate_accounts =
            transaction
                .transaction
                .transaction
                .actions
                .iter()
                .find_map(|action| match action {
                    ActionView::Delegate {
                        delegate_action, ..
                    } => Some((
                        delegate_action.sender_id.to_string(),
                        delegate_action.receiver_id.to_string(),
                    )),
                    _ => None,
                });
        let (delegate_signer_id, delegate_receiver_id) = delegate_accounts
            .map_or((None, None), |(signer_id, receiver_id)| {
                (Some(signer_id), Some(receiver_id))
            });

        let is_completed = transaction.pending_receipt_ids.is_empty();
        let mut tail_receipt_id = None;
        let is_success = loop {
            let status = if let Some(receipt_id) = tail_receipt_id.take() {
                let rs = transaction.transaction.receipts.iter().find_map(|receipt| {
                    if receipt.receipt.receipt_id == receipt_id {
                        Some(&receipt.execution_outcome.outcome.status)
                    } else {
                        None
                    }
                });
                if rs.is_none() {
                    break false;
                }
                rs.unwrap()
            } else {
                &transaction.transaction.execution_outcome.outcome.status
            };
            match status {
                ExecutionStatusView::SuccessValue(_) => {
                    break true;
                }
                ExecutionStatusView::SuccessReceiptId(rh) => {
                    tail_receipt_id = Some(*rh);
                }
                _ => {
                    break false;
                }
            }
        };

        let mut accounts = transaction.committed_account_tx_rows.clone();
        let tx_row = TransactionRow {
            transaction_hash: transaction.transaction_hash().to_string(),
            signer_id: signer_id.clone(),
            tx_block_height: transaction.tx_block_height,
            tx_index: transaction.tx_index,
            tx_block_hash: transaction.tx_block_hash.to_string(),
            tx_block_timestamp: transaction.tx_block_timestamp,
            last_block_height: transaction.last_block_height,
            is_completed,
            shard_id: transaction.shard_id,
            receiver_id: receiver_id.clone(),
            signer_public_key: transaction.transaction.transaction.public_key.to_string(),
            priority_fee: transaction.transaction.transaction.priority_fee,
            nonce: transaction.transaction.transaction.nonce,
            is_relayed: delegate_signer_id.is_some(),
            real_signer_id: delegate_signer_id
                .as_ref()
                .unwrap_or(&signer_id)
                .to_string(),
            real_receiver_id: delegate_receiver_id
                .as_ref()
                .unwrap_or(&receiver_id)
                .to_string(),
            is_success,
        };
        if let Some(delegate_signer_id) = delegate_signer_id {
            accounts
                .row(&delegate_signer_id)
                .set_delegated_signer()
                .set_any_signer();
        }
        accounts.row(&signer_id).set_signer().set_any_signer();
        accounts.row(&tx_row.real_receiver_id).set_real_receiver();
        accounts.row(&tx_row.real_signer_id).set_real_signer();

        if transaction.committed_tx_row.as_ref() != Some(&tx_row) {
            self.rows.tx_rows.push(tx_row.clone());
            transaction.committed_tx_row = Some(tx_row);
        }

        for receipt in &transaction.transaction.receipts {
            add_accounts_from_receipt(&mut accounts, &receipt.receipt);
            add_accounts_from_logs(&mut accounts, &receipt.execution_outcome.outcome.logs);
        }
        if is_success {
            for row in accounts.0.values_mut() {
                row.is_success = true;
            }
        }
        for (account_id, row) in accounts.0.iter_mut() {
            row.account_id = account_id.clone();
            row.tx_block_height = transaction.tx_block_height;
            row.tx_block_timestamp = transaction.tx_block_timestamp;
            row.transaction_hash = tx_hash.clone();
            row.tx_index = transaction.tx_index;
        }

        for row in accounts.0.values() {
            if transaction.committed_account_tx_rows.0.get(&row.account_id) != Some(row) {
                self.rows.account_txs.push(row.clone());
            }
        }
        mem::swap(&mut accounts, &mut transaction.committed_account_tx_rows);

        self.rows.transactions.push((
            transaction.transaction_hash().to_string(),
            serde_json::to_string(&transaction.transaction).unwrap(),
        ));
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
                "#{}: Having {} tx_rows, {} account_txs, {} receipts_txs, {} blocks, {} transactions",
                block_height,
                self.rows.tx_rows.len(),
                self.rows.account_txs.len(),
                self.rows.receipt_txs.len(),
                self.rows.blocks.len(),
                self.rows.transactions.len()
            );
        }
        if self.rows.tx_rows.len() >= db.min_batch || is_round_block || self.commit_every_block {
            self.commit(db).await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self, db: &ClickDB) -> anyhow::Result<()> {
        let mut rows = TxRows::default();
        std::mem::swap(&mut rows, &mut self.rows);
        let max_commit_handlers = if self.is_backfill {
            MAX_COMMIT_HANDLERS
        } else {
            1
        };
        while self.commit_handlers.len() >= max_commit_handlers {
            self.commit_handlers.remove(0).await??;
        }
        let db = db.clone();
        let garage = self.garage_client.clone();
        let handler = tokio::spawn(async move {
            if !rows.transactions.is_empty() {
                // Commit to garage first
                insert_transactions_to_s3(&db.client, &rows.transactions).await?;
            }
            if !rows.tx_rows.is_empty() {
                insert_rows_with_retry(&db.client, &rows.tx_rows, "transactions").await?;
            }
            if !rows.account_txs.is_empty() {
                insert_rows_with_retry(&db.client, &rows.account_txs, "account_txs").await?;
            }
            if !rows.receipt_txs.is_empty() {
                insert_rows_with_retry(&db.client, &rows.receipt_txs, "receipt_txs").await?;
            }
            if !rows.blocks.is_empty() {
                insert_rows_with_retry(&db.client, &rows.blocks, "blocks").await?;
            }
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "Committed {} tx_rows, {} account_txs, {} receipts_txs, {} blocks",
                rows.tx_rows.len(),
                rows.account_txs.len(),
                rows.receipt_txs.len(),
                rows.blocks.len(),
            );
            Ok::<(), anyhow::Error>(())
        });
        self.commit_handlers.push(handler);

        Ok(())
    }

    pub async fn last_block_height(&mut self, db: &ClickDB) -> BlockHeight {
        db.max("block_height", "blocks").await.unwrap_or(0)
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        while let Some(handler) = self.commit_handlers.pop() {
            handler.await??;
        }
        Ok(())
    }
}

fn extract_accounts(
    accounts: &mut Accounts,
    value: &Value,
    keys: &[&str],
    is_function_call_args: bool,
) {
    for arg in keys {
        if let Some(account_id) = value.get(arg) {
            if let Some(account_id) = account_id.as_str() {
                if let Ok(account_id) = AccountId::from_str(account_id) {
                    if is_function_call_args {
                        accounts.row(account_id.as_str()).set_action_arg();
                    } else {
                        accounts.row(account_id.as_str()).set_event_log();
                    }
                }
            }
        }
    }
}

fn add_accounts_from_logs(accounts: &mut Accounts, logs: &[String]) {
    for log in logs {
        if log.starts_with(EVENT_JSON_PREFIX) {
            let event_json = &log[EVENT_JSON_PREFIX.len()..];
            if let Ok(event) = serde_json::from_str::<EventJson>(event_json) {
                for data in &event.data {
                    extract_accounts(accounts, data, &POTENTIAL_EVENTS_ARGS, false);
                }
            }
        }
    }
}

fn add_accounts_from_receipt(accounts: &mut Accounts, receipt: &ImprovedReceiptView) {
    accounts.row(receipt.receiver_id.as_str()).set_receiver();
    accounts
        .row(receipt.predecessor_id.as_str())
        .set_predecessor();
    let mut is_delegate_receipt = false;
    match &receipt.receipt {
        ReceiptEnumView::Action { actions, .. } => {
            for action in actions {
                match action {
                    ActionView::FunctionCall { args, .. } => {
                        accounts
                            .row(receipt.receiver_id.as_str())
                            .set_function_call();
                        if let Ok(args) = serde_json::from_slice::<Value>(&args) {
                            extract_accounts(accounts, &args, &POTENTIAL_ACCOUNT_ARGS, true);
                        }
                    }
                    ActionView::Delegate { .. } => {
                        // Two delegate actions are not an issue
                        is_delegate_receipt = actions.len() == 1;
                    }
                    _ => {}
                }
            }
        }
        ReceiptEnumView::Data { .. } => {}
        ReceiptEnumView::GlobalContractDistribution { .. } => {}
    }
    if !is_delegate_receipt && receipt.predecessor_id.as_str() != SYSTEM_ACCOUNT_ID {
        accounts
            .row(receipt.receiver_id.as_str())
            .set_real_receiver();
    }
}

#[derive(Default)]
pub struct TxCache {
    pub receipt_to_tx: HashMap<CryptoHash, CryptoHash>,
    pub action_receipts: HashMap<CryptoHash, ImprovedReceiptView>,
    pub data_receipts: HashMap<CryptoHash, ImprovedReceiptView>,
    pub transactions: HashMap<CryptoHash, PendingTransaction>,
    pub last_block_height: BlockHeight,
}

impl TxCache {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn stats(&self) -> String {
        format!(
            "mem: {} tr, {} r, {} ar, {} dr",
            self.transactions.len(),
            self.receipt_to_tx.len(),
            self.action_receipts.len(),
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

    fn insert_action_receipt(&mut self, receipt: ImprovedReceiptView) {
        let receipt_id = receipt.receipt_id;
        let old_receipt = self.action_receipts.insert(receipt_id, receipt);
        // In-memory insert.
        if let Some(old_receipt) = old_receipt {
            assert_eq!(
                old_receipt.receipt_id, receipt_id,
                "Duplicate action receipt_id: {} with different receipt_ids!",
                receipt_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate action receipt_id: {} ", receipt_id);
        }
    }

    fn remove_action_receipt(&mut self, receipt_id: &CryptoHash) -> Option<ImprovedReceiptView> {
        self.action_receipts.remove(receipt_id)
    }

    fn insert_data_receipt(&mut self, data_id: &CryptoHash, receipt: ImprovedReceiptView) {
        let receipt_id = receipt.receipt_id;
        let is_promise_resume = match &receipt.receipt {
            ReceiptEnumView::Action { .. } => false,
            ReceiptEnumView::Data {
                is_promise_resume, ..
            } => *is_promise_resume,
            ReceiptEnumView::GlobalContractDistribution { .. } => false,
        };
        let old_receipt = self.data_receipts.insert(*data_id, receipt);
        // In-memory insert.
        if let Some(old_receipt) = old_receipt {
            if old_receipt.receipt_id != receipt_id {
                let old_is_promise_resume = match &old_receipt.receipt {
                    ReceiptEnumView::Action { .. } => false,
                    ReceiptEnumView::Data {
                        is_promise_resume, ..
                    } => *is_promise_resume,
                    ReceiptEnumView::GlobalContractDistribution { .. } => false,
                };
                assert!(
                    is_promise_resume && old_is_promise_resume,
                    "Duplicate data_id: {} with different receipt_ids: new {} and old {} while one of them is not promise_resume {} and {}",
                    data_id,
                    receipt_id,
                    old_receipt.receipt_id,
                    is_promise_resume,
                    old_is_promise_resume
                );
                tracing::log::warn!(target: PROJECT_ID,
                    "Duplicate data_id: {} with different receipt_ids: new {} and old {}. Ignoring new {}",
                    data_id,
                    receipt_id,
                    old_receipt.receipt_id,
                    receipt_id
                );
                // Restoring the old receipt. Ignoring the new one.
                self.data_receipts.insert(*data_id, old_receipt);
            } else {
                tracing::log::warn!(target: PROJECT_ID, "Duplicate data_id: {} with the same receipt id {}", data_id, receipt_id);
            }
        }
    }

    fn get_and_remove_data_receipt(&mut self, data_id: &CryptoHash) -> Option<ImprovedReceiptView> {
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
