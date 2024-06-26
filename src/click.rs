use crate::*;

use clickhouse::{Client, Row};
use near_indexer::near_primitives::views::{
    AccessKeyPermissionView, ActionView, ExecutionOutcomeView, ExecutionStatusView,
    ReceiptEnumView, ReceiptView,
};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::env;

use serde::{Deserialize, Serialize};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::types::BlockHeight;
use std::convert::TryFrom;
use std::time::Duration;

const MAX_TOKEN_LENGTH: usize = 64;
const CLICKHOUSE_TARGET: &str = "clickhouse";
const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";
const SAVE_STEP: u64 = 1000;

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum ReceiptStatus {
    Failure = 1,
    Success = 2,
}

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum ActionKind {
    CreateAccount = 1,
    DeployContract = 2,
    FunctionCall = 3,
    Transfer = 4,
    Stake = 5,
    AddKey = 6,
    DeleteKey = 7,
    DeleteAccount = 8,
    Delegate = 9,
}

#[derive(Row, Serialize)]
pub struct FullActionRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub receipt_id: String,
    pub receipt_index: u32,
    pub action_index: u16,
    pub signer_id: String,
    pub signer_public_key: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ReceiptStatus,
    pub action: ActionKind,
    pub action_json: String,
    pub input_data_ids: Vec<String>,

    pub status_success_value: Option<String>,
    pub status_success_receipt: Option<String>,
    pub status_failure: Option<String>,

    pub contract_hash: Option<String>,
    pub public_key: Option<String>,
    pub access_key_contract_id: Option<String>,
    pub deposit: Option<u128>,
    pub gas_price: u128,
    pub attached_gas: Option<u64>,
    pub gas_burnt: u64,
    pub tokens_burnt: u128,
    pub method_name: Option<String>,
    pub args: Option<String>,
}

#[derive(Row, Serialize)]
pub struct FullEventRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub receipt_id: String,
    pub receipt_index: u32,
    pub log_index: u16,
    pub signer_id: String,
    pub signer_public_key: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ReceiptStatus,
    pub log: String,

    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
}

#[derive(Row, Serialize)]
pub struct FullDataRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub receipt_index: u32,
    pub predecessor_id: String,
    pub account_id: String,
    pub data_id: String,
    pub data: Option<String>,
}

#[derive(Default)]
pub struct Rows {
    pub actions: Vec<FullActionRow>,
    pub events: Vec<FullEventRow>,
    pub data: Vec<FullDataRow>,
}

pub struct ClickDB {
    pub client: Client,
    pub rows: Rows,
    pub last_action_block_height: BlockHeight,
    pub last_event_block_height: BlockHeight,
    pub last_data_block_height: BlockHeight,
    pub min_batch: usize,
}

impl ClickDB {
    pub(crate) fn min_restart_block(&self) -> BlockHeight {
        let min_guaranteed_block = std::cmp::max(
            self.last_action_block_height / SAVE_STEP * SAVE_STEP,
            self.last_event_block_height / SAVE_STEP * SAVE_STEP,
        )
        .max(self.last_data_block_height / SAVE_STEP * SAVE_STEP);
        let min_optimistic_block = std::cmp::min(
            self.last_action_block_height,
            self.last_event_block_height
                .min(self.last_data_block_height),
        );
        min_guaranteed_block.max(min_optimistic_block)
    }
}

impl ClickDB {
    pub fn new(min_batch: usize) -> Self {
        Self {
            client: establish_connection(),
            rows: Rows::default(),
            last_action_block_height: 0,
            last_event_block_height: 0,
            last_data_block_height: 0,
            min_batch,
        }
    }

    pub async fn fetch_last_block_heights(&mut self) {
        self.last_action_block_height = self.last_block_height("actions").await.unwrap_or(0);
        self.last_event_block_height = self.last_block_height("events").await.unwrap_or(0);
        self.last_data_block_height = self.last_block_height("data").await.unwrap_or(0);
        tracing::log::info!(target: CLICKHOUSE_TARGET, "Last block heights: actions={}, events={}, data={}", self.last_action_block_height, self.last_event_block_height, self.last_data_block_height);
    }

    pub fn merge(&mut self, rows: Rows, block_height: BlockHeight) {
        if block_height > self.last_action_block_height {
            self.last_action_block_height = block_height;
            self.rows.actions.extend(rows.actions);
        }
        if block_height > self.last_event_block_height {
            self.last_event_block_height = block_height;
            self.rows.events.extend(rows.events);
        }
        if block_height > self.last_data_block_height {
            self.last_data_block_height = block_height;
            self.rows.data.extend(rows.data);
        }
    }

    pub async fn commit(&mut self) -> clickhouse::error::Result<()> {
        self.commit_actions().await?;
        self.commit_events().await?;
        self.commit_data().await?;
        Ok(())
    }

    pub async fn commit_actions(&mut self) -> clickhouse::error::Result<()> {
        if !self.rows.actions.is_empty() {
            insert_rows_with_retry(&self.client, &self.rows.actions, "actions").await?;
            self.rows.actions.clear();
        }
        Ok(())
    }

    pub async fn commit_events(&mut self) -> clickhouse::error::Result<()> {
        if !self.rows.events.is_empty() {
            insert_rows_with_retry(&self.client, &self.rows.events, "events").await?;
            self.rows.events.clear();
        }
        Ok(())
    }

    pub async fn commit_data(&mut self) -> clickhouse::error::Result<()> {
        if !self.rows.data.is_empty() {
            insert_rows_with_retry(&self.client, &self.rows.data, "data").await?;
            self.rows.data.clear();
        }
        Ok(())
    }

    pub async fn last_block_height(&self, table: &str) -> clickhouse::error::Result<BlockHeight> {
        let block_height = self
            .client
            .query(&format!("SELECT max(block_height) FROM {}", table))
            .fetch_one::<u64>()
            .await?;
        Ok(block_height)
    }

    pub async fn verify_connection(&self) -> clickhouse::error::Result<()> {
        self.client.query("SELECT 1").execute().await?;
        Ok(())
    }
}

fn string_from_vec_u8(value: &Vec<u8>) -> String {
    String::from_utf8(value.clone())
        .unwrap_or_else(|_| format!("base64:{}", BASE64_STANDARD.encode(value)))
}

fn establish_connection() -> Client {
    Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap())
}

pub async fn extract_info(db: &mut ClickDB, msg: BlockWithTxHashes) -> anyhow::Result<()> {
    let block_height = msg.block.header.height;
    let rows = extract_rows(msg);
    db.merge(rows, block_height);

    let is_round_block = block_height % SAVE_STEP == 0;
    if is_round_block {
        tracing::log::info!(target: CLICKHOUSE_TARGET, "#{}: Having {} actions, {} events, {} data", block_height, db.rows.actions.len(), db.rows.events.len(), db.rows.data.len());
    }
    if db.rows.actions.len() >= db.min_batch || is_round_block {
        db.commit_actions().await?;
    }
    if db.rows.events.len() >= db.min_batch || is_round_block {
        db.commit_events().await?;
    }
    if db.rows.data.len() >= db.min_batch || is_round_block {
        db.commit_data().await?;
    }
    Ok(())
}

async fn insert_rows_with_retry<T>(
    client: &Client,
    rows: &Vec<T>,
    table: &str,
) -> clickhouse::error::Result<()>
where
    T: Row + Serialize,
{
    let mut delay = Duration::from_millis(100);
    let max_retries = 10;
    let mut i = 0;
    loop {
        let res = || async {
            let mut insert = client.insert(table)?;
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        };
        match res().await {
            Ok(v) => break Ok(v),
            Err(err) => {
                tracing::log::error!(target: CLICKHOUSE_TARGET, "Attempt #{}: Error inserting rows into \"{}\": {}", i, table, err);
                tokio::time::sleep(delay).await;
                delay *= 2;
                if i == max_retries - 1 {
                    break Err(err);
                }
            }
        };
        i += 1;
    }
}

fn limit_length(s: &mut Option<String>) {
    if s.as_ref().map(|s| s.len()).unwrap_or(0) > MAX_TOKEN_LENGTH {
        *s = None;
    }
}

#[derive(Deserialize, Debug, Default)]
pub struct Event {
    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
}

pub fn parse_event(event: &str) -> Option<Event> {
    let mut event: Event = serde_json::from_str(&event).ok()?;
    limit_length(&mut event.version);
    limit_length(&mut event.standard);
    limit_length(&mut event.event);
    Some(event)
}

pub fn extract_rows(msg: BlockWithTxHashes) -> Rows {
    let mut rows = Rows::default();

    let block_height = msg.block.header.height;
    let block_hash = msg.block.header.hash.to_string();
    let block_timestamp = msg.block.header.timestamp_nanosec;

    let mut receipt_index: u32 = 0;
    for shard in msg.shards {
        for outcome in shard.receipt_execution_outcomes {
            let ReceiptView {
                predecessor_id,
                receiver_id: account_id,
                receipt_id,
                receipt,
            } = outcome.receipt;
            let tx_hash = outcome.tx_hash.expect("Tx Hash is not set").to_string();
            let predecessor_id = predecessor_id.to_string();
            let account_id = account_id.to_string();
            let receipt_id = receipt_id.to_string();
            let ExecutionOutcomeView {
                status: execution_status,
                gas_burnt,
                tokens_burnt,
                logs,
                ..
            } = outcome.execution_outcome.outcome;
            let status = match &execution_status {
                ExecutionStatusView::Unknown => ReceiptStatus::Failure,
                ExecutionStatusView::Failure(_) => ReceiptStatus::Failure,
                ExecutionStatusView::SuccessValue(_) => ReceiptStatus::Success,
                ExecutionStatusView::SuccessReceiptId(_) => ReceiptStatus::Success,
            };
            let status_success_value = match &execution_status {
                ExecutionStatusView::SuccessValue(value) => Some(string_from_vec_u8(value)),
                _ => None,
            };
            let status_success_receipt = match &execution_status {
                ExecutionStatusView::SuccessReceiptId(receipt_id) => Some(receipt_id.to_string()),
                _ => None,
            };
            let status_failure = match &execution_status {
                ExecutionStatusView::Failure(failure) => {
                    Some(serde_json::to_string(failure).unwrap())
                }
                _ => None,
            };
            match receipt {
                ReceiptEnumView::Action {
                    signer_id,
                    signer_public_key,
                    output_data_receivers: _,
                    input_data_ids,
                    actions,
                    gas_price,
                } => {
                    for (log_index, log) in logs.into_iter().enumerate() {
                        let log_index = u16::try_from(log_index).expect("Log index overflow");
                        let event = if log.starts_with(EVENT_LOG_PREFIX) {
                            parse_event(&log.as_str()[EVENT_LOG_PREFIX.len()..])
                        } else {
                            None
                        }
                        .unwrap_or_default();
                        rows.events.push(FullEventRow {
                            block_height,
                            block_hash: block_hash.clone(),
                            block_timestamp,
                            transaction_hash: tx_hash.clone(),
                            receipt_id: receipt_id.clone(),
                            receipt_index,
                            log_index,
                            signer_id: signer_id.to_string(),
                            signer_public_key: signer_public_key.to_string(),
                            predecessor_id: predecessor_id.clone(),
                            account_id: account_id.clone(),
                            status,
                            log,

                            version: event.version,
                            standard: event.standard,
                            event: event.event,
                        });
                    }

                    for (action_index, action) in actions.into_iter().enumerate() {
                        let action_index =
                            u16::try_from(action_index).expect("Action index overflow");
                        rows.actions.push(FullActionRow {
                            block_height,
                            block_hash: block_hash.clone(),
                            block_timestamp,
                            transaction_hash: tx_hash.clone(),
                            receipt_id: receipt_id.clone(),
                            receipt_index,
                            action_index,
                            signer_id: signer_id.to_string(),
                            signer_public_key: signer_public_key.to_string(),
                            predecessor_id: predecessor_id.clone(),
                            account_id: account_id.clone(),
                            status,
                            action: match action {
                                ActionView::CreateAccount => ActionKind::CreateAccount,
                                ActionView::DeployContract { .. } => ActionKind::DeployContract,
                                ActionView::FunctionCall { .. } => ActionKind::FunctionCall,
                                ActionView::Transfer { .. } => ActionKind::Transfer,
                                ActionView::Stake { .. } => ActionKind::Stake,
                                ActionView::AddKey { .. } => ActionKind::AddKey,
                                ActionView::DeleteKey { .. } => ActionKind::DeleteKey,
                                ActionView::DeleteAccount { .. } => ActionKind::DeleteAccount,
                                ActionView::Delegate { .. } => ActionKind::Delegate,
                            },
                            action_json: serde_json::to_string(&action).unwrap(),
                            input_data_ids: input_data_ids
                                .iter()
                                .map(|id| id.to_string())
                                .collect(),
                            status_success_value: status_success_value.clone(),
                            status_success_receipt: status_success_receipt.clone(),
                            status_failure: status_failure.clone(),
                            contract_hash: match &action {
                                ActionView::DeployContract { code } => {
                                    Some(CryptoHash::hash_bytes(&code).to_string())
                                }
                                _ => None,
                            },
                            public_key: match &action {
                                ActionView::AddKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                ActionView::DeleteKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                _ => None,
                            },
                            access_key_contract_id: match &action {
                                ActionView::AddKey { access_key, .. } => {
                                    match &access_key.permission {
                                        AccessKeyPermissionView::FunctionCall {
                                            receiver_id,
                                            ..
                                        } => Some(receiver_id.to_string()),
                                        _ => None,
                                    }
                                }
                                _ => None,
                            },
                            deposit: match &action {
                                ActionView::Transfer { deposit, .. } => Some(*deposit),
                                ActionView::Stake { stake, .. } => Some(*stake),
                                ActionView::FunctionCall { deposit, .. } => Some(*deposit),
                                _ => None,
                            },
                            gas_price,
                            attached_gas: match &action {
                                ActionView::FunctionCall { gas, .. } => Some(*gas),
                                _ => None,
                            },
                            gas_burnt,
                            tokens_burnt,
                            method_name: match &action {
                                ActionView::FunctionCall { method_name, .. } => {
                                    Some(method_name.to_string())
                                }
                                _ => None,
                            },
                            args: match &action {
                                ActionView::FunctionCall { args, .. } => {
                                    Some(string_from_vec_u8(args))
                                }
                                _ => None,
                            },
                        });
                    }

                    // Increasing receipt index only for action receipts
                    receipt_index = receipt_index
                        .checked_add(1)
                        .expect("Receipt index overflow");
                }
                ReceiptEnumView::Data { .. } => {
                    unreachable!("Data receipts don't have execution outcomes");
                }
            }
        }
        // Extracting data receipts
        if let Some(chunk) = shard.chunk {
            for receipt_view in chunk.receipts {
                let ReceiptView {
                    predecessor_id,
                    receiver_id: account_id,
                    receipt_id,
                    receipt,
                } = receipt_view;
                match receipt {
                    ReceiptEnumView::Action { .. } => {
                        // Ignoring. Processed with the execution outcomes.
                    }
                    ReceiptEnumView::Data { data_id, data } => {
                        rows.data.push(FullDataRow {
                            block_height,
                            block_hash: block_hash.clone(),
                            block_timestamp,
                            receipt_id: receipt_id.to_string(),
                            receipt_index,
                            predecessor_id: predecessor_id.to_string(),
                            account_id: account_id.to_string(),
                            data_id: data_id.to_string(),
                            data: data.as_ref().map(string_from_vec_u8),
                        });
                        receipt_index = receipt_index
                            .checked_add(1)
                            .expect("Receipt index overflow");
                    }
                }
            }
        }
    }
    rows
}
