use crate::types::*;
use crate::*;
use fastnear_primitives::near_indexer_primitives::views::ExecutionStatusView;
use fastnear_primitives::near_indexer_primitives::IndexerTransactionWithOutcome;
use fastnear_primitives::near_primitives::hash::CryptoHash;
use fastnear_primitives::near_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_primitives::views::{ActionView, ReceiptEnumView};

use crate::actions::extract_rows;
use crate::s3_tools::insert_transactions_to_s3;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::{env, mem};

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

const POTENTIAL_EVENTS_ARGS: [&str; 11] = [
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
    "receiver_id",
];

#[derive(Default)]
pub struct TxRows {
    pub tx_rows: Vec<TransactionRow>,
    pub account_txs: Vec<AccountTxRow>,
    pub receipt_txs: Vec<ReceiptTxRow>,
    pub blocks: Vec<BlockRow>,
    pub transactions: Vec<(String, String)>,
    pub actions: Vec<ActionRow>,
    pub events: Vec<EventRow>,
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
    pub garage: Arc<aws_sdk_s3::Client>,
    pub db: Arc<ClickDB>,
}

impl TransactionsData {
    pub fn new(is_backfill: bool, garage: Arc<aws_sdk_s3::Client>, db: Arc<ClickDB>) -> Self {
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
            garage,
            db,
        }
    }

    pub async fn process_block(
        &mut self,
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
            gas_price: block.block.header.gas_price.as_yoctonear(),
            block_ordinal: block.block.header.block_ordinal,
            total_supply: block.block.header.total_supply.as_yoctonear(),
            num_transactions: 0,
            num_receipts: 0,
            gas_burnt: 0,
            tokens_burnt: 0,
        };

        let mut pending_receipt_txs = vec![];
        let mut pending_action_rows = vec![];
        let mut pending_event_rows = vec![];

        let catching_up = block_height <= last_db_block_height;

        let mut transactions_to_commit = HashSet::new();
        let mut tx_index = 0u32;
        let mut appear_receipt_index = 0u32;
        let mut receipt_index = 0u32;
        let mut block_data_index = 0u32;
        let mut block_action_index = 0u32;

        let mut shards = block.shards;
        for shard in &mut shards {
            if let Some(chunk) = shard.chunk.take() {
                let shard_id: u64 = chunk.header.shard_id.into();
                block_row.gas_burnt += chunk.header.gas_used.as_gas();
                block_row.tokens_burnt += chunk.header.balance_burnt.as_yoctonear();
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
                for receipt in chunk.local_receipts.into_iter().chain(chunk.receipts) {
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
                    .expect("Missing action receipt for an receipt execution outcome");
                let pending_transaction = self.tx_cache.get_and_remove_transaction(&tx_hash);
                if pending_transaction.is_none() {
                    panic!(
                        "Missing pending transaction for receipt_id {} tx_hash {} at block {}",
                        receipt_id, tx_hash, block_height
                    );
                }
                let mut pending_transaction = pending_transaction.unwrap();
                pending_transaction
                    .pending_receipt_ids
                    .retain(|r| r != &receipt_id);
                pending_transaction.last_block_height = block_height;

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
                                    true,
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
                    matches!(
                        execution_outcome.outcome.status,
                        ExecutionStatusView::SuccessValue(_)
                    ) || matches!(
                        execution_outcome.outcome.status,
                        ExecutionStatusView::SuccessReceiptId(_)
                    ),
                ));
                receipt_index += 1;
                let pending_receipt_ids = execution_outcome.outcome.receipt_ids.clone();

                // Actions/Events
                let (action_rows, event_rows) = extract_rows(
                    &action_receipt,
                    current_receipt_index,
                    &execution_outcome.outcome,
                    &pending_transaction,
                    &block_info,
                    &mut block_data_index,
                    &mut block_action_index,
                );
                pending_action_rows.extend(action_rows);
                pending_event_rows.extend(event_rows);

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
                if !(self.is_backfill || catching_up)
                    || pending_transaction.pending_receipt_ids.is_empty()
                {
                    transactions_to_commit.insert(pending_transaction.transaction_hash());
                }
                if !catching_up || !pending_transaction.pending_receipt_ids.is_empty() {
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                }
            }
        }

        self.tx_cache.last_block_height = block_height;
        block_row.num_transactions = tx_index;
        block_row.num_receipts = receipt_index;

        tracing::log::info!(target: PROJECT_ID, "#{}: {} transactions to commit. Pending {}", block_height, transactions_to_commit.len(), self.tx_cache.stats());

        if !catching_up {
            self.rows.receipt_txs.extend(pending_receipt_txs);
            self.rows.actions.extend(pending_action_rows);
            self.rows.events.extend(pending_event_rows);
            self.rows.blocks.push(block_row);
            for tx_hash in transactions_to_commit {
                let mut transaction = self
                    .tx_cache
                    .get_and_remove_transaction(&tx_hash)
                    .expect("Missing pending transaction to commit");
                self.process_transaction(&mut transaction);
                if !transaction.pending_receipt_ids.is_empty() {
                    self.tx_cache.insert_transaction(transaction, &[]);
                }
            }
        }

        self.maybe_commit(block_height).await?;

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
        let mut tx_row = TransactionRow {
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
            gas_burnt: 0,
            tokens_burnt: 0,
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

        for receipt in &transaction.transaction.receipts {
            add_accounts_from_receipt(&mut accounts, &receipt.receipt);
            add_accounts_from_logs(&mut accounts, &receipt.execution_outcome.outcome.logs);
            tx_row.gas_burnt += receipt.execution_outcome.outcome.gas_burnt.as_gas();
            tx_row.tokens_burnt += receipt
                .execution_outcome
                .outcome
                .tokens_burnt
                .as_yoctonear();
        }

        if transaction.committed_tx_row.as_ref() != Some(&tx_row) {
            self.rows.tx_rows.push(tx_row.clone());
            transaction.committed_tx_row = Some(tx_row);
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

    pub async fn maybe_commit(&mut self, block_height: BlockHeight) -> anyhow::Result<()> {
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
        if self.rows.tx_rows.len() >= self.db.min_batch || is_round_block || self.commit_every_block
        {
            self.commit().await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
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
        let db = self.db.clone();
        let garage = self.garage.clone();
        let handler = tokio::spawn(async move {
            if !rows.transactions.is_empty() {
                // Commit to garage first
                insert_transactions_to_s3(&garage, rows.transactions).await?;
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

    pub async fn last_block_height(&self) -> BlockHeight {
        self.db.max("block_height", "blocks").await.unwrap_or(0)
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
        ReceiptEnumView::Action {
            actions, refund_to, ..
        } => {
            if let Some(refund_to) = refund_to {
                accounts.row(refund_to.as_str()).set_explicit_refund_to();
            }
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
