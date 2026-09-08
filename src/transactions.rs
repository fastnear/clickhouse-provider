use crate::types::*;
use crate::*;
use fastnear_primitives::near_indexer_primitives::views::ExecutionStatusView;
use fastnear_primitives::near_indexer_primitives::IndexerTransactionWithOutcome;
use fastnear_primitives::near_primitives::hash::CryptoHash;
use fastnear_primitives::near_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_primitives::views::{ActionView, ReceiptEnumView};

use crate::actions::extract_rows;
use fastnear_primitives::near_primitives::action::delegate::VersionedDelegateActionPayload;
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::{env, mem};
use tokio::sync::{oneshot, Semaphore};

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";
const SYSTEM_ACCOUNT_ID: &str = "system";
const TESTNET_INSTANT_RECEIPT_FIX_BLOCK_HEIGHT: BlockHeight = 243470300;

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

/// A raw transaction that hasn't been compressed yet. Compression is deferred to the
/// commit task so it runs once per surviving transaction, off the block-processing path.
pub struct PendingRawTx {
    pub tx_block_timestamp: u64,
    pub last_block_height: BlockHeight,
    pub json: Vec<u8>,
}

/// The rows accumulated since the last commit.
///
/// In live mode a transaction re-emits its rows on every block it is touched, so a
/// bundle of blocks carries many versions of the same row. The three tables that have a
/// `last_block_height` version column are therefore keyed by their ClickHouse `ORDER BY`
/// and collapsed to the newest version as they are produced -- exactly what
/// `ReplacingMergeTree` would collapse them to on merge, just done before the insert
/// instead of after it.
///
/// `receipt_txs`, `blocks`, `actions` and `events` have no version column and emit
/// exactly one row per block-scoped index, so there is nothing to collapse there.
#[derive(Default)]
pub struct TxRows {
    /// Keyed by `(tx_block_height, tx_index)`.
    pub tx_rows: HashMap<(BlockHeight, u32), TransactionRow>,
    /// Keyed by `(account_id, tx_block_height, tx_index)`.
    pub account_txs: HashMap<(String, BlockHeight, u32), AccountTxRow>,
    /// Keyed by `transaction_hash`.
    pub raw_txs: HashMap<String, PendingRawTx>,
    pub receipt_txs: Vec<ReceiptTxRow>,
    pub blocks: Vec<BlockRow>,
    pub actions: Vec<ActionRow>,
    pub events: Vec<EventRow>,
}

impl TxRows {
    /// `blocks` is the durable watermark, and it is the only table that is written for
    /// every processed block, so an empty `blocks` means there is nothing to commit.
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub fn push_tx_row(&mut self, row: TransactionRow) {
        match self.tx_rows.entry((row.tx_block_height, row.tx_index)) {
            Entry::Occupied(mut e) => {
                if row.last_block_height >= e.get().last_block_height {
                    e.insert(row);
                }
            }
            Entry::Vacant(e) => {
                e.insert(row);
            }
        }
    }

    pub fn push_account_tx(&mut self, row: AccountTxRow) {
        let key = (row.account_id.clone(), row.tx_block_height, row.tx_index);
        match self.account_txs.entry(key) {
            Entry::Occupied(mut e) => {
                if row.last_block_height >= e.get().last_block_height {
                    e.insert(row);
                }
            }
            Entry::Vacant(e) => {
                e.insert(row);
            }
        }
    }

    pub fn push_raw_tx(&mut self, transaction_hash: String, raw: PendingRawTx) {
        match self.raw_txs.entry(transaction_hash) {
            Entry::Occupied(mut e) => {
                if raw.last_block_height >= e.get().last_block_height {
                    e.insert(raw);
                }
            }
            Entry::Vacant(e) => {
                e.insert(raw);
            }
        }
    }
}

impl PendingTransaction {
    pub fn transaction_hash(&self) -> CryptoHash {
        self.transaction.transaction.hash
    }
}

/// An in-flight commit, tagged with its sequence number so errors can be attributed.
pub struct CommitHandler {
    pub seq: u64,
    pub handle: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
}

pub struct TransactionsData {
    pub chain_id: ChainId,
    pub is_backfill: bool,
    /// Whether per-block lines are logged at `info`. Set by the block loop: on when a
    /// bundle is a single block (we're at the tip), off while bundling to keep the log
    /// readable at a hundred blocks per second.
    pub log_each_block: bool,
    pub tx_cache: TxCache,
    pub rows: TxRows,
    pub commit_handlers: VecDeque<CommitHandler>,
    pub commit_semaphore: Arc<Semaphore>,
    /// Completion signal of the previously spawned commit. A commit waits on it before
    /// writing its `blocks` rows, which is what keeps the watermark strictly ordered.
    pub prev_commit_done: Option<oneshot::Receiver<()>>,
    pub commit_seq: u64,
    pub db: Arc<ClickDB>,
}

impl TransactionsData {
    pub fn new(chain_id: ChainId, is_backfill: bool, db: Arc<ClickDB>) -> Self {
        if env::var("COMMIT_EVERY_BLOCK").is_ok() {
            tracing::log::warn!(
                target: CLICKHOUSE_TARGET,
                "COMMIT_EVERY_BLOCK is set but no longer used: commits now happen once per \
                 bundle of blocks, which is at least as often. It can be removed from the env."
            );
        }
        let tx_cache = TxCache::new();

        let max_commit_handlers = env::var("MAX_COMMIT_HANDLERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(MAX_COMMIT_HANDLERS)
            .max(1);
        let commit_semaphore = Arc::new(Semaphore::new(max_commit_handlers));

        Self {
            chain_id,
            is_backfill,
            log_each_block: true,
            tx_cache,
            rows: TxRows::default(),
            commit_handlers: VecDeque::new(),
            commit_semaphore,
            prev_commit_done: None,
            commit_seq: 0,
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
                        committed_account_tx_rows: Accounts::new(),
                    };
                    tx_index += 1;
                    let pending_receipt_ids = pending_transaction.pending_receipt_ids.clone();
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                }
                for receipt in chunk
                    .local_receipts
                    .into_iter()
                    .chain(chunk.receipts)
                    .into_iter()
                    .chain(chunk.instant_receipts)
                {
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
                    .unwrap_or_else(|| {
                        if self.chain_id == ChainId::Mainnet || self.chain_id == ChainId::Testnet && block_height >= TESTNET_INSTANT_RECEIPT_FIX_BLOCK_HEIGHT {
                            panic!(
                                "Missing action receipt for receipt_id {} tx_hash {} at block {}",
                                receipt_id, tx_hash, block_height
                            );
                        }
                        tracing::log::warn!(target: PROJECT_ID,
                            "Missing action receipt for receipt_id {} at block {}. Assuming instant receipt for testnet.",
                            receipt_id, block_height
                        );

                        let action_receipt = ImprovedReceiptView::from_receipt(
                            receipt.clone(),
                            appear_receipt_index,
                            &block_info,
                        );
                        appear_receipt_index += 1;
                        action_receipt
                    });
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

        let mode = if catching_up { "Catching up" } else { "Live" };
        if self.log_each_block {
            tracing::log::info!(target: PROJECT_ID, "#{}: [{}] {} transactions to commit. Pending {}",
                block_height, mode, transactions_to_commit.len(), self.tx_cache.stats());
        } else {
            tracing::log::debug!(target: PROJECT_ID, "#{}: [{}] {} transactions to commit. Pending {}",
                block_height, mode, transactions_to_commit.len(), self.tx_cache.stats());
        }

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
                    ActionView::DelegateV2 {
                        delegate_action, ..
                    } => Some(match delegate_action {
                        VersionedDelegateActionPayload::V2(delegate_payload) => (
                            delegate_payload.sender_id.to_string(),
                            delegate_payload.receiver_id.to_string(),
                        ),
                    }),
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
            priority_fee: 0,
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
            self.rows.push_tx_row(tx_row.clone());
            transaction.committed_tx_row = Some(tx_row);
        }

        if is_success {
            for row in accounts.accounts.values_mut() {
                row.is_success = true;
            }
        }
        for (account_id, row) in accounts.accounts.iter_mut() {
            row.account_id = account_id.clone();
            row.last_block_height = transaction.last_block_height;
            row.tx_block_height = transaction.tx_block_height;
            row.tx_block_timestamp = transaction.tx_block_timestamp;
            row.transaction_hash = tx_hash.clone();
            row.tx_index = transaction.tx_index;
        }

        for row in accounts.accounts.values() {
            if transaction
                .committed_account_tx_rows
                .accounts
                .get(&row.account_id)
                != Some(row)
            {
                self.rows.push_account_tx(row.clone());
            }
        }
        mem::swap(&mut accounts, &mut transaction.committed_account_tx_rows);

        // Serialize here (cheap), compress in the commit task (expensive): within a
        // bundle the same transaction is re-emitted on every block it is touched and only
        // the last version survives, so compressing now would throw the work away.
        self.rows.push_raw_tx(
            transaction.transaction_hash().to_string(),
            PendingRawTx {
                tx_block_timestamp: transaction.tx_block_timestamp,
                last_block_height: transaction.last_block_height,
                json: serde_json::to_vec(&transaction.transaction).unwrap(),
            },
        );
    }

    /// Awaits and removes every commit that has already finished, propagating the first
    /// failure.
    ///
    /// Deliberately not "drain the finished prefix": one slow or wedged commit at the
    /// head must not stop everything behind it from being reaped.
    async fn reap_finished_commits(&mut self) -> anyhow::Result<()> {
        let mut i = 0;
        while i < self.commit_handlers.len() {
            if self.commit_handlers[i].handle.is_finished() {
                let CommitHandler { seq, handle } =
                    self.commit_handlers.remove(i).expect("index is in range");
                handle
                    .await
                    .map_err(|err| anyhow::anyhow!("commit #{} panicked: {}", seq, err))??;
            } else {
                i += 1;
            }
        }
        Ok(())
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let mut rows = TxRows::default();
        mem::swap(&mut rows, &mut self.rows);

        self.reap_finished_commits().await?;

        // The permit is acquired here, on the block-processing task, and therefore always
        // in sequence order. That is what makes the ordering chain below deadlock-free:
        // commit k always holds its permit before commit k+1 acquires one, so the
        // predecessor a commit parks on is either already finished or actively running.
        // Moving this acquire inside the spawned task would break that.
        let permit = self.commit_semaphore.clone().acquire_owned().await?;

        let (done_tx, done_rx) = oneshot::channel::<()>();
        let prev_done = self.prev_commit_done.replace(done_rx);
        let seq = self.commit_seq;
        self.commit_seq += 1;

        let db = self.db.clone();
        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let _permit = permit;

            let blocks = mem::take(&mut rows.blocks);
            let num_receipt_txs = rows.receipt_txs.len();
            let num_actions = rows.actions.len();
            let num_events = rows.events.len();
            let tx_rows = mem::take(&mut rows.tx_rows);
            let account_txs = mem::take(&mut rows.account_txs);
            let raw_txs = mem::take(&mut rows.raw_txs);

            // zstd is the one genuinely CPU-bound step, so it runs on a blocking thread.
            // Sorting by each table's ClickHouse ORDER BY keeps the resulting parts compact.
            let (raw_tx_rows, tx_rows, account_txs) = tokio::task::spawn_blocking(move || {
                let mut raw_tx_rows: Vec<RawTransactionRow> = raw_txs
                    .into_iter()
                    .map(|(transaction_hash, raw)| RawTransactionRow {
                        transaction_hash,
                        tx_block_timestamp: raw.tx_block_timestamp,
                        last_block_height: raw.last_block_height,
                        data: zstd::encode_all(&raw.json[..], 3).expect("zstd encode error"),
                    })
                    .collect();
                raw_tx_rows.sort_unstable_by(|a, b| a.transaction_hash.cmp(&b.transaction_hash));

                let mut tx_rows: Vec<TransactionRow> = tx_rows.into_values().collect();
                tx_rows.sort_unstable_by_key(|row| (row.tx_block_height, row.tx_index));

                let mut account_txs: Vec<AccountTxRow> = account_txs.into_values().collect();
                account_txs.sort_unstable_by(|a, b| {
                    (&a.account_id, a.tx_block_height, a.tx_index).cmp(&(
                        &b.account_id,
                        b.tx_block_height,
                        b.tx_index,
                    ))
                });

                (raw_tx_rows, tx_rows, account_txs)
            })
            .await?;

            try_join!(
                insert_rows_with_retry(&db, &raw_tx_rows, "raw_tx"),
                insert_rows_with_retry(&db, &tx_rows, "transactions"),
                insert_rows_with_retry(&db, &account_txs, "account_txs"),
                insert_rows_with_retry(&db, &rows.receipt_txs, "receipt_txs"),
                insert_rows_with_retry(&db, &rows.actions, "actions"),
                insert_rows_with_retry(&db, &rows.events, "events"),
            )?;
            let data_duration = start.elapsed().as_millis();

            let num_raw_tx_rows = raw_tx_rows.len();
            let num_tx_rows = tx_rows.len();
            let num_account_txs = account_txs.len();
            // Everything but the watermark rows is durable now, so don't keep a whole
            // bundle pinned in memory while parked on the predecessor.
            drop((raw_tx_rows, tx_rows, account_txs, rows));

            // `blocks` is the restart watermark, so it must never become visible before
            // the data of an earlier bundle. A dropped sender means an earlier commit
            // failed or panicked, and that poisons every commit after it.
            if let Some(prev_done) = prev_done {
                prev_done.await.map_err(|_| {
                    anyhow::anyhow!("commit #{}: an earlier commit did not complete", seq)
                })?;
            }
            insert_rows_with_retry(&db, &blocks, "blocks").await?;
            let _ = done_tx.send(());

            let duration = start.elapsed().as_millis();
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let freshness = blocks
                .last()
                .map(|block| now_ns.saturating_sub(block.block_timestamp) as f64 / 1e9f64)
                .unwrap_or_default();
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "({} ms; data {} ms) Committed #{}..{} ({} blocks) freshness {:.3} sec | {} raw_tx_rows, {} tx_rows, {} account_txs, {} receipts_txs, {} actions, {} events",
                duration,
                data_duration,
                blocks.first().map(|block| block.block_height).unwrap_or_default(),
                blocks.last().map(|block| block.block_height).unwrap_or_default(),
                blocks.len(),
                freshness,
                num_raw_tx_rows,
                num_tx_rows,
                num_account_txs,
                num_receipt_txs,
                num_actions,
                num_events,
            );
            Ok::<(), anyhow::Error>(())
        });
        self.commit_handlers.push_back(CommitHandler { seq, handle });

        Ok(())
    }

    /// The height to resume from: the highest block whose bundle is known to be fully
    /// stored.
    ///
    /// `max(block_height)` on its own is not safe. `blocks` is a `Distributed` table
    /// sharded by `cityHash64(block_height)`, so one commit's rows fan out across shards
    /// and the insert is not atomic: a commit that exhausts its retries can leave rows on
    /// some shards and not others. `max()` would then report a height above genuinely
    /// missing lower blocks, and because everything at or below the resume point is
    /// replayed without writing anything, the hole would be permanent.
    ///
    /// So walk the `prev_block_height` chain down from `max()` and stop at the first
    /// break. Only the newest bundle can be partial -- commits are ordered and a failed
    /// one poisons all its successors, so nothing below the previous commit's range can
    /// be affected -- and the window checked here is several bundles deep.
    pub async fn last_block_in_range(
        &self,
        db: &ClickDB,
        start_block: BlockHeight,
        end_block: BlockHeight,
        max_blocks_per_commit: usize,
    ) -> anyhow::Result<BlockHeight> {
        // A query error must not be swallowed into 0 here: that would make the indexer
        // treat the whole range as un-indexed and re-index it from scratch in live mode.
        let max_block = db
            .max_in_range("block_height", "blocks", start_block, end_block)
            .await?;
        if max_block == 0 {
            return Ok(start_block.saturating_sub(1));
        }

        let window = (4 * max_blocks_per_commit as u64).max(SAFE_CATCH_UP_OFFSET);
        let window_start = max_block.saturating_sub(window).max(start_block);
        let chain = db.fetch_block_chain(window_start, max_block).await?;

        let last_contiguous = last_contiguous_block(&chain, max_block);
        if last_contiguous != max_block {
            tracing::log::warn!(
                target: PROJECT_ID,
                "Blocks table has a hole above #{}: max is #{}, but the chain breaks first. \
                 Resuming from #{} and re-indexing the rest.",
                last_contiguous, max_block, last_contiguous
            );
        }
        Ok(last_contiguous)
    }

    /// Awaits every in-flight commit in order, so the first (lowest sequence) real
    /// failure is the one reported, rather than a successor's "an earlier commit did not
    /// complete".
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        let mut first_error = None;
        while let Some(CommitHandler { seq, handle }) = self.commit_handlers.pop_front() {
            let result = handle
                .await
                .map_err(|err| anyhow::anyhow!("commit #{} panicked: {}", seq, err))
                .and_then(|res| res);
            if let Err(err) = result {
                tracing::log::error!(target: CLICKHOUSE_TARGET, "Commit #{} failed: {:#}", seq, err);
                first_error.get_or_insert(err);
            }
        }
        self.prev_commit_done = None;
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// The highest height reachable from the bottom of `chain` without a break in the
/// `prev_block_height` links, or `max_block` if there is none.
///
/// `chain` must be ascending by `block_height`. Heights are not consecutive -- the chain
/// legitimately skips heights with no block -- so only `prev_block_height` distinguishes
/// a missing row from a skipped height.
fn last_contiguous_block(chain: &[BlockChainRow], max_block: BlockHeight) -> BlockHeight {
    for pair in chain.windows(2) {
        // A `None` predecessor can't be checked; treat it as unbroken.
        if let Some(prev_block_height) = pair[1].prev_block_height {
            if prev_block_height != pair[0].block_height {
                return pair[0].block_height;
            }
        }
    }
    max_block
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(pairs: &[(u64, Option<u64>)]) -> Vec<BlockChainRow> {
        pairs
            .iter()
            .map(|(block_height, prev_block_height)| BlockChainRow {
                block_height: *block_height,
                prev_block_height: *prev_block_height,
            })
            .collect()
    }

    #[test]
    fn unbroken_chain_resumes_at_max() {
        // Heights 11 and 13 were skipped on chain, which is not a hole.
        let chain = chain(&[(10, Some(9)), (12, Some(10)), (14, Some(12))]);
        assert_eq!(last_contiguous_block(&chain, 14), 14);
    }

    #[test]
    fn stops_before_a_missing_block() {
        // #12 is missing: #14 says its predecessor was #12, but #12 isn't stored.
        let chain = chain(&[(10, Some(9)), (11, Some(10)), (14, Some(12))]);
        assert_eq!(last_contiguous_block(&chain, 14), 11);
    }

    #[test]
    fn stops_at_the_first_break_not_the_last() {
        let chain = chain(&[(10, Some(9)), (14, Some(12)), (20, Some(18))]);
        assert_eq!(last_contiguous_block(&chain, 20), 10);
    }

    #[test]
    fn unverifiable_link_is_treated_as_unbroken() {
        let chain = chain(&[(10, Some(9)), (11, None), (12, Some(11))]);
        assert_eq!(last_contiguous_block(&chain, 12), 12);
    }

    #[test]
    fn too_short_to_verify_falls_back_to_max() {
        assert_eq!(last_contiguous_block(&[], 42), 42);
        assert_eq!(last_contiguous_block(&chain(&[(42, Some(41))]), 42), 42);
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
                    ActionView::Delegate { .. } | ActionView::DelegateV2 { .. } => {
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
