use crate::*;
use base64::Engine;
use std::env;

use base64::prelude::BASE64_STANDARD;
use clickhouse::Row;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_primitives::hash::CryptoHash;

use fastnear_primitives::near_primitives::types::BlockHeight;
use fastnear_primitives::near_primitives::views::{
    AccessKeyPermissionView, ActionView, ExecutionOutcomeView, ExecutionStatusView,
    ReceiptEnumView, ReceiptView,
};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

const MAX_TOKEN_LENGTH: usize = 64;
const MAX_TOKEN_IDS_LENGTH: usize = 4;
const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";

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
    NonrefundableStorageTransfer = 10,
    DeployGlobalContract = 11,
    DeployGlobalContractByAccountId = 12,
    UseGlobalContract = 13,
    UseGlobalContractByAccountId = 14,
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

    pub args_account_id: Option<String>,
    pub args_new_account_id: Option<String>,
    pub args_owner_id: Option<String>,
    pub args_receiver_id: Option<String>,
    pub args_sender_id: Option<String>,
    pub args_token_id: Option<String>,
    pub args_amount: Option<u128>,
    pub args_balance: Option<u128>,
    pub args_nft_contract_id: Option<String>,
    pub args_nft_token_id: Option<String>,
    pub return_value_int: Option<u128>,
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

    pub data_account_id: Option<String>,
    pub data_owner_id: Option<String>,
    pub data_old_owner_id: Option<String>,
    pub data_new_owner_id: Option<String>,
    pub data_liquidation_account_id: Option<String>,
    pub data_authorized_id: Option<String>,
    pub data_token_ids: Vec<String>,
    pub data_token_id: Option<String>,
    pub data_position: Option<String>,
    pub data_amount: Option<u128>,
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

pub struct ActionsData {
    pub commit_every_block: bool,
    pub rows: Rows,
    pub commit_handlers: Vec<tokio::task::JoinHandle<Result<(), clickhouse::error::Error>>>,
}

impl ActionsData {
    pub fn new() -> Self {
        let commit_every_block = env::var("COMMIT_EVERY_BLOCK")
            .map(|v| v == "true")
            .unwrap_or(false);
        Self {
            commit_every_block,
            rows: Rows::default(),
            commit_handlers: vec![],
        }
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
                "#{}: Having {} actions, {} events, {} data",
                block_height,
                self.rows.actions.len(),
                self.rows.events.len(),
                self.rows.data.len()
            );
        }
        if self.rows.actions.len() >= db.min_batch || is_round_block || self.commit_every_block {
            self.commit(db).await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self, db: &ClickDB) -> anyhow::Result<()> {
        let mut rows = Rows::default();
        std::mem::swap(&mut rows, &mut self.rows);
        while self.commit_handlers.len() >= MAX_COMMIT_HANDLERS {
            self.commit_handlers.remove(0).await??;
        }
        let db = db.clone();
        let handler = tokio::spawn(async move {
            if !rows.actions.is_empty() {
                insert_rows_with_retry(&db.client, &rows.actions, "actions").await?;
            }
            if !rows.events.is_empty() {
                insert_rows_with_retry(&db.client, &rows.events, "events").await?;
            }
            if !rows.data.is_empty() {
                insert_rows_with_retry(&db.client, &rows.data, "data").await?;
            }
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "Committed {} actions, {} events, {} data",
                rows.actions.len(),
                rows.events.len(),
                rows.data.len()
            );
            Ok::<(), clickhouse::error::Error>(())
        });
        self.commit_handlers.push(handler);

        Ok(())
    }

    pub async fn process_block(
        &mut self,
        db: &mut ClickDB,
        block: BlockWithTxHashes,
        last_db_block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let block_height = block.block.header.height;
        let rows = extract_rows(block);
        if block_height > last_db_block_height {
            self.rows.actions.extend(rows.actions);
            self.rows.events.extend(rows.events);
            self.rows.data.extend(rows.data);
        }

        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(target: CLICKHOUSE_TARGET, "#{}: Having {} actions, {} events, {} data", block_height, self.rows.actions.len(), self.rows.events.len(), self.rows.data.len());
        }

        self.maybe_commit(db, block_height).await?;
        Ok(())
    }

    pub async fn last_block_height(&mut self, db: &ClickDB) -> BlockHeight {
        db.max("block_height", "actions").await.unwrap_or(0)
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        while let Some(handler) = self.commit_handlers.pop() {
            handler.await??;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct ArgsData {
    pub account_id: Option<AccountId>,
    pub args_new_account_id: Option<AccountId>,
    pub args_owner_id: Option<AccountId>,
    pub receiver_id: Option<AccountId>,
    pub sender_id: Option<AccountId>,
    pub token_id: Option<String>,
    pub nft_contract_id: Option<AccountId>,
    pub nft_token_id: Option<String>,
    pub amount: Option<String>,
    pub balance: Option<String>,
}

pub fn extract_args_data(action: &ActionView) -> Option<ArgsData> {
    match action {
        ActionView::FunctionCall { args, .. } => {
            let mut args_data: ArgsData = serde_json::from_slice(&args).ok()?;
            // If token length is larger than 64 bytes, we remove it.
            limit_length(&mut args_data.token_id);
            limit_length(&mut args_data.nft_token_id);
            Some(args_data)
        }
        _ => None,
    }
}

fn limit_length(s: &mut Option<String>) {
    if s.as_ref().map(|s| s.len()).unwrap_or(0) > MAX_TOKEN_LENGTH {
        *s = None;
    }
}

fn string_from_vec_u8(value: &Vec<u8>) -> String {
    String::from_utf8(value.clone())
        .unwrap_or_else(|_| format!("base64:{}", BASE64_STANDARD.encode(value)))
}

fn extract_return_value_int(execution_status: &ExecutionStatusView) -> Option<u128> {
    if let ExecutionStatusView::SuccessValue(value) = execution_status {
        let str_value = serde_json::from_slice::<String>(&value).ok()?;
        str_value.parse::<u128>().ok()
    } else {
        None
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct EventData {
    pub account_id: Option<AccountId>,
    pub owner_id: Option<AccountId>,
    pub old_owner_id: Option<AccountId>,
    pub new_owner_id: Option<AccountId>,
    pub liquidation_account_id: Option<AccountId>,
    pub authorized_id: Option<AccountId>,
    pub token_ids: Option<Vec<String>>,
    pub token_id: Option<String>,
    pub position: Option<String>,
    pub amount: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct Event {
    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
    pub data: Option<Vec<EventData>>,
}

pub fn parse_event(event: &str) -> Option<Event> {
    let mut event: Event = serde_json::from_str(&event).ok()?;
    limit_length(&mut event.version);
    limit_length(&mut event.standard);
    limit_length(&mut event.event);
    if let Some(data) = event.data.as_mut().and_then(|data| data.get_mut(0)) {
        if let Some(token_ids) = data.token_ids.as_mut() {
            token_ids.retain(|s| s.len() <= MAX_TOKEN_LENGTH);
            if token_ids.len() > MAX_TOKEN_IDS_LENGTH {
                token_ids.resize(MAX_TOKEN_IDS_LENGTH, "".to_string());
            }
        }
        limit_length(&mut data.token_id);
    } else {
        event.data = None;
    }
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
                priority: _priority,
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
            let return_value_int = extract_return_value_int(&execution_status);
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
                    is_promise_yield: _is_promise_yield,
                } => {
                    for (log_index, log) in logs.into_iter().enumerate() {
                        let log_index = u16::try_from(log_index).expect("Log index overflow");
                        let mut event = if log.starts_with(EVENT_LOG_PREFIX) {
                            parse_event(&log.as_str()[EVENT_LOG_PREFIX.len()..])
                        } else {
                            None
                        }
                        .unwrap_or_default();
                        let data = event
                            .data
                            .take()
                            .map(|mut data| data.remove(0))
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

                            data_account_id: data
                                .account_id
                                .as_ref()
                                .map(|account_id| account_id.to_string()),
                            data_owner_id: data
                                .owner_id
                                .as_ref()
                                .map(|owner_id| owner_id.to_string()),
                            data_old_owner_id: data
                                .old_owner_id
                                .as_ref()
                                .map(|old_owner_id| old_owner_id.to_string()),
                            data_new_owner_id: data
                                .new_owner_id
                                .as_ref()
                                .map(|new_owner_id| new_owner_id.to_string()),
                            data_liquidation_account_id: data
                                .liquidation_account_id
                                .as_ref()
                                .map(|liquidation_account_id| liquidation_account_id.to_string()),
                            data_authorized_id: data
                                .authorized_id
                                .as_ref()
                                .map(|authorized_id| authorized_id.to_string()),
                            data_token_ids: data.token_ids.clone().unwrap_or_default(),
                            data_token_id: data.token_id,
                            data_position: data.position,
                            data_amount: data
                                .amount
                                .as_ref()
                                .and_then(|amount| amount.parse().ok()),
                        });
                    }

                    for (action_index, action) in actions.into_iter().enumerate() {
                        let action_index =
                            u16::try_from(action_index).expect("Action index overflow");
                        let args_data = extract_args_data(&action);
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
                                ActionView::DeployGlobalContract { .. } => {
                                    ActionKind::DeployGlobalContract
                                }
                                ActionView::DeployGlobalContractByAccountId { .. } => {
                                    ActionKind::DeployGlobalContractByAccountId
                                }
                                ActionView::UseGlobalContract { .. } => {
                                    ActionKind::UseGlobalContract
                                }
                                ActionView::UseGlobalContractByAccountId { .. } => {
                                    ActionKind::UseGlobalContractByAccountId
                                }
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
                                // ActionView::NonrefundableStorageTransfer { deposit } => {
                                //     Some(*deposit)
                                // }
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
                            args_account_id: args_data.as_ref().and_then(|args| {
                                args.account_id
                                    .as_ref()
                                    .map(|account_id| account_id.to_string())
                            }),
                            args_new_account_id: args_data.as_ref().and_then(|args| {
                                args.args_new_account_id
                                    .as_ref()
                                    .map(|new_account_id| new_account_id.to_string())
                            }),
                            args_owner_id: args_data.as_ref().and_then(|args| {
                                args.args_owner_id
                                    .as_ref()
                                    .map(|owner_id| owner_id.to_string())
                            }),
                            args_receiver_id: args_data.as_ref().and_then(|args| {
                                args.receiver_id
                                    .as_ref()
                                    .map(|receiver_id| receiver_id.to_string())
                            }),
                            args_sender_id: args_data.as_ref().and_then(|args| {
                                args.sender_id
                                    .as_ref()
                                    .map(|sender_id| sender_id.to_string())
                            }),
                            args_token_id: args_data
                                .as_ref()
                                .and_then(|args| args.token_id.clone()),
                            args_amount: args_data.as_ref().and_then(|args| {
                                args.amount.as_ref().and_then(|amount| amount.parse().ok())
                            }),
                            args_balance: args_data.as_ref().and_then(|args| {
                                args.balance
                                    .as_ref()
                                    .and_then(|balance| balance.parse().ok())
                            }),
                            args_nft_contract_id: args_data.as_ref().and_then(|args| {
                                args.nft_contract_id
                                    .as_ref()
                                    .map(|nft_contract_id| nft_contract_id.to_string())
                            }),
                            args_nft_token_id: args_data.as_ref().and_then(|args| {
                                args.nft_token_id
                                    .as_ref()
                                    .map(|nft_token_id| nft_token_id.to_string())
                            }),
                            return_value_int,
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
                ReceiptEnumView::GlobalContractDistribution { .. } => {}
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
                    priority: _priority,
                } = receipt_view;
                match receipt {
                    ReceiptEnumView::Action { .. } => {
                        // Ignoring. Processed with the execution outcomes.
                    }
                    ReceiptEnumView::Data {
                        data_id,
                        data,
                        is_promise_resume: _is_promise_resume,
                    } => {
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
                    ReceiptEnumView::GlobalContractDistribution { .. } => {}
                }
            }
        }
    }
    rows
}
