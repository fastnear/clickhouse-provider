use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::views::GlobalContractIdentifierView;
use fastnear_primitives::near_primitives::hash::CryptoHash;

use crate::types::{ActionRow, BlockInfo, EventRow, ImprovedReceiptView, PendingTransaction};
use fastnear_primitives::near_primitives::views::{
    AccessKeyPermissionView, ActionView, ExecutionOutcomeView, ExecutionStatusView, ReceiptEnumView,
};
use serde::Deserialize;

const MAX_EVENT_FIELD_LENGTH: usize = 64;
const MAX_EVENT_TOKEN_LENGTH: usize = 160;
const MAX_ARGS_LENGTH: usize = 512;
const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";

#[derive(Deserialize)]
pub struct ArgsData {
    pub account_id: Option<AccountId>,
    pub args_new_account_id: Option<AccountId>,
    pub args_owner_id: Option<AccountId>,
    pub receiver_id: Option<AccountId>,
    pub sender_id: Option<AccountId>,
    #[serde(default)]
    pub token_ids: Vec<String>,
    pub token_id: Option<String>,
    #[serde(default)]
    pub tokens: Vec<String>,
    pub nft_contract_id: Option<AccountId>,
    pub nft_token_id: Option<String>,
    pub amount: Option<String>,
    pub balance: Option<String>,
    #[serde(default)]
    pub amounts: Vec<String>,

    #[serde(skip)]
    pub parsed_amount: Option<u128>,
    #[serde(skip)]
    pub parsed_token_id: Option<String>,
}

pub fn extract_args_data(action: &ActionView) -> Option<ArgsData> {
    match action {
        ActionView::FunctionCall { args, .. } => {
            let mut data: ArgsData = serde_json::from_slice(&args).ok()?;
            for token_id in data.token_ids.iter_mut() {
                limit_length(token_id, MAX_EVENT_TOKEN_LENGTH);
            }
            limit_length_opt(&mut data.token_id, MAX_EVENT_TOKEN_LENGTH);
            for token in data.tokens.iter_mut() {
                limit_length(token, MAX_EVENT_TOKEN_LENGTH);
            }
            // Select token_id
            if let Some(token_id) = &data.token_id {
                data.parsed_token_id = Some(token_id.clone());
            } else if let Some(token_id) = data.token_ids.get(0) {
                data.parsed_token_id = Some(token_id.clone());
            } else if let Some(token) = data.tokens.get(0) {
                data.parsed_token_id = Some(token.clone());
            }

            limit_length_opt(&mut data.token_id, MAX_EVENT_TOKEN_LENGTH);

            // Parse amount
            if let Some(amount) = data.amount.take() {
                data.parsed_amount = amount.parse().ok();
            } else if let Some(amount) = data.amounts.get(0) {
                data.parsed_amount = amount.parse().ok();
            }

            Some(data)
        }
        _ => None,
    }
}

fn limit_length_inline(s: &str, max_len: usize) -> String {
    let index = s.floor_char_boundary(max_len);
    s[..index].to_string()
}

fn limit_length(s: &mut String, max_len: usize) {
    if s.len() > max_len {
        let index = s.floor_char_boundary(max_len);
        s.truncate(index);
    }
}

fn limit_length_opt(s: &mut Option<String>, max_len: usize) {
    s.as_mut().map(|s| limit_length(s, max_len));
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
    pub receiver_id: Option<AccountId>,
    pub owner_id: Option<AccountId>,
    pub old_owner_id: Option<AccountId>,
    pub new_owner_id: Option<AccountId>,
    pub liquidation_account_id: Option<AccountId>,
    #[serde(default)]
    pub token_ids: Vec<String>,
    pub token_id: Option<String>,
    #[serde(default)]
    pub tokens: Vec<String>,
    pub amount: Option<String>,
    #[serde(default)]
    pub amounts: Vec<String>,

    #[serde(skip)]
    pub parsed_amount: Option<u128>,
    #[serde(skip)]
    pub parsed_token_id: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct Event {
    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
    #[serde(default)]
    pub data: Vec<EventData>,
}

pub fn parse_event(event: &str) -> Option<Event> {
    let mut event: Event = serde_json::from_str(&event).ok()?;
    limit_length_opt(&mut event.version, MAX_EVENT_FIELD_LENGTH);
    limit_length_opt(&mut event.standard, MAX_EVENT_FIELD_LENGTH);
    limit_length_opt(&mut event.event, MAX_EVENT_FIELD_LENGTH);
    for data in event.data.iter_mut() {
        for token_id in data.token_ids.iter_mut() {
            limit_length(token_id, MAX_EVENT_TOKEN_LENGTH);
        }
        limit_length_opt(&mut data.token_id, MAX_EVENT_TOKEN_LENGTH);
        for token in data.tokens.iter_mut() {
            limit_length(token, MAX_EVENT_TOKEN_LENGTH);
        }
        // Select token_id
        if let Some(token_id) = &data.token_id {
            data.parsed_token_id = Some(token_id.clone());
        } else if let Some(token_id) = data.token_ids.get(0) {
            data.parsed_token_id = Some(token_id.clone());
        } else if let Some(token) = data.tokens.get(0) {
            data.parsed_token_id = Some(token.clone());
        }

        // Parse amount
        if let Some(amount) = data.amount.take() {
            data.parsed_amount = amount.parse().ok();
        } else if let Some(amount) = data.amounts.get(0) {
            data.parsed_amount = amount.parse().ok();
        }
    }
    Some(event)
}

pub fn extract_rows(
    receipt: &ImprovedReceiptView,
    receipt_index: u32,
    outcome: &ExecutionOutcomeView,
    pending_transaction: &PendingTransaction,
    block_info: &BlockInfo,
    block_data_index: &mut u32,
    block_action_index: &mut u32,
) -> (Vec<ActionRow>, Vec<EventRow>) {
    let mut action_rows = vec![];
    let mut event_rows = vec![];
    let tx_hash = pending_transaction.transaction_hash();

    let ImprovedReceiptView {
        block_height: _,
        block_hash: _,
        block_timestamp: _,
        receipt_index: _,
        predecessor_id,
        receiver_id,
        receipt_id,
        receipt,
        priority: _priority,
    } = receipt;
    let BlockInfo {
        block_height,
        block_hash: _,
        block_timestamp,
    } = block_info;
    let predecessor_id = predecessor_id.to_string();
    let receiver_id = receiver_id.to_string();
    let receipt_id = receipt_id.to_string();
    let ExecutionOutcomeView {
        status: execution_status,
        gas_burnt,
        tokens_burnt,
        logs,
        ..
    } = outcome;
    let is_success = match &execution_status {
        ExecutionStatusView::Unknown | ExecutionStatusView::Failure(_) => false,
        ExecutionStatusView::SuccessValue(_) | ExecutionStatusView::SuccessReceiptId(_) => true,
    };
    let return_value_int = extract_return_value_int(&execution_status);
    let status_success_value = match &execution_status {
        ExecutionStatusView::SuccessValue(value) => {
            Some(value[..MAX_EVENT_TOKEN_LENGTH.min(value.len())].to_vec())
        }
        _ => None,
    };
    let status_success_receipt = match &execution_status {
        ExecutionStatusView::SuccessReceiptId(receipt_id) => Some(receipt_id.to_string()),
        _ => None,
    };
    match receipt {
        ReceiptEnumView::Action {
            signer_id,
            signer_public_key: _,
            output_data_receivers: _,
            input_data_ids: _,
            actions,
            gas_price,
            is_promise_yield: _is_promise_yield,
            refund_to,
        } => {
            let num_actions = actions.len() as u16;
            let num_logs = logs.len() as u16;
            for (log_index, log) in logs.into_iter().enumerate() {
                let log_index = u16::try_from(log_index).expect("Log index overflow");
                let event = if log.starts_with(EVENT_LOG_PREFIX) {
                    parse_event(&log.as_str()[EVENT_LOG_PREFIX.len()..])
                } else {
                    None
                }
                .unwrap_or_default();
                // Data rows

                let num_data = event.data.len() as u16;
                for (data_index, data) in event.data.into_iter().enumerate() {
                    let current_block_data_index = *block_data_index;
                    *block_data_index += 1;
                    event_rows.push(EventRow {
                        receipt_id: receipt_id.clone(),
                        block_height: *block_height,
                        block_timestamp: *block_timestamp,
                        receipt_index,
                        log_index,
                        data_index: u16::try_from(data_index).expect("Data index overflow"),
                        block_data_index: current_block_data_index,
                        transaction_hash: tx_hash.to_string(),
                        signer_id: signer_id.to_string(),
                        predecessor_id: predecessor_id.clone(),
                        receiver_id: receiver_id.clone(),
                        is_success,
                        num_actions,
                        num_logs,
                        num_data,

                        version: event.version.clone(),
                        standard: event.standard.clone(),
                        event: event.event.clone(),

                        data_account_id: data
                            .account_id
                            .as_ref()
                            .map(|account_id| account_id.to_string()),
                        data_receiver_id: data
                            .receiver_id
                            .as_ref()
                            .map(|receiver_id| receiver_id.to_string()),
                        data_owner_id: data.owner_id.as_ref().map(|owner_id| owner_id.to_string()),
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
                        data_token_id: data.parsed_token_id,
                        data_amount: data.parsed_amount,
                    });
                }
            }

            for (action_index, action) in actions.into_iter().enumerate() {
                let action_index = u16::try_from(action_index).expect("Action index overflow");
                let args_data = extract_args_data(&action);
                let current_block_action_index = *block_action_index;
                *block_action_index += 1;
                action_rows.push(ActionRow {
                    receipt_id: receipt_id.clone(),
                    block_height: *block_height,
                    block_timestamp: *block_timestamp,
                    receipt_index,
                    action_index,
                    block_action_index: current_block_action_index,
                    transaction_hash: tx_hash.to_string(),
                    signer_id: signer_id.to_string(),
                    predecessor_id: predecessor_id.clone(),
                    receiver_id: receiver_id.clone(),
                    refund_to_id: refund_to.as_ref().map(|id| id.to_string()),
                    action_type: action_type(action),

                    is_success,
                    num_actions,
                    gas_burnt: gas_burnt.as_gas(),
                    tokens_burnt: tokens_burnt.as_yoctonear(),

                    success_value: status_success_value.clone(),
                    success_receipt: status_success_receipt.clone(),
                    success_value_int: return_value_int,

                    contract_hash: contract_hash_from_action(action),
                    public_key: match &action {
                        ActionView::AddKey { public_key, .. } => Some(public_key.to_string()),
                        ActionView::DeleteKey { public_key, .. } => Some(public_key.to_string()),
                        ActionView::Stake { public_key, .. } => Some(public_key.to_string()),
                        _ => None,
                    },
                    access_key_contract_id: match &action {
                        ActionView::AddKey { access_key, .. } => match &access_key.permission {
                            AccessKeyPermissionView::FunctionCall { receiver_id, .. } => {
                                Some(receiver_id.to_string())
                            }
                            _ => None,
                        },
                        _ => None,
                    },
                    deposit: match &action {
                        ActionView::Transfer { deposit, .. } => Some(*deposit),
                        ActionView::Stake { stake, .. } => Some(*stake),
                        ActionView::FunctionCall { deposit, .. } => Some(*deposit),
                        ActionView::DeterministicStateInit { deposit, .. } => Some(*deposit),
                        _ => None,
                    }
                    .map(|d| d.as_yoctonear()),
                    gas_price: gas_price.as_yoctonear(),
                    attached_gas: match &action {
                        ActionView::FunctionCall { gas, .. } => Some(gas.as_gas()),
                        _ => None,
                    },
                    method_name: match &action {
                        ActionView::FunctionCall { method_name, .. } => {
                            Some(limit_length_inline(&method_name, MAX_EVENT_FIELD_LENGTH))
                        }
                        _ => None,
                    },
                    args: match &action {
                        ActionView::FunctionCall { args, .. } => {
                            Some(args[..MAX_ARGS_LENGTH.min(args.len())].to_vec())
                        }
                        _ => None,
                    },
                    delegate_receiver_id: match &action {
                        ActionView::Delegate {
                            delegate_action, ..
                        } => Some(delegate_action.receiver_id.to_string()),
                        _ => None,
                    },
                    global_account_id: match &action {
                        ActionView::UseGlobalContractByAccountId { account_id, .. } => {
                            Some(account_id.to_string())
                        }
                        ActionView::DeterministicStateInit { code, .. } => match code {
                            GlobalContractIdentifierView::AccountId(account_id) => {
                                Some(account_id.to_string())
                            }
                            _ => None,
                        },
                        _ => None,
                    },
                    beneficiary_id: match &action {
                        ActionView::DeleteAccount { beneficiary_id } => {
                            Some(beneficiary_id.to_string())
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
                        .and_then(|args| args.parsed_token_id.clone()),
                    args_amount: args_data.as_ref().and_then(|args| args.parsed_amount),
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
                });
            }
        }
        ReceiptEnumView::Data { .. } => {
            unreachable!("Data receipts don't have execution outcomes");
        }
        ReceiptEnumView::GlobalContractDistribution { .. } => {
            unreachable!("GlobalContractDistribution receipts don't have execution outcomes");
        }
    }
    (action_rows, event_rows)
}

fn contract_hash_from_action(action: &ActionView) -> Option<String> {
    let v = match action {
        ActionView::DeployContract { code } => CryptoHash::try_from(&code[..]).ok(),
        ActionView::DeployGlobalContract { code } => CryptoHash::try_from(&code[..]).ok(),
        ActionView::DeployGlobalContractByAccountId { code } => {
            CryptoHash::try_from(&code[..]).ok()
        }
        ActionView::UseGlobalContract { code_hash } => Some(*code_hash),
        ActionView::DeterministicStateInit { code, .. } => match code {
            GlobalContractIdentifierView::CodeHash(code_hash) => Some(*code_hash),
            GlobalContractIdentifierView::AccountId(_) => None,
        },
        _ => None,
    };
    v.map(|hash| hash.to_string())
}

fn action_type(action: &ActionView) -> String {
    match action {
        ActionView::CreateAccount => "CreateAccount",
        ActionView::DeployContract { .. } => "DeployContract",
        ActionView::FunctionCall { .. } => "FunctionCall",
        ActionView::Transfer { .. } => "Transfer",
        ActionView::Stake { .. } => "Stake",
        ActionView::AddKey { .. } => "AddKey",
        ActionView::DeleteKey { .. } => "DeleteKey",
        ActionView::DeleteAccount { .. } => "DeleteAccount",
        ActionView::Delegate { .. } => "Delegate",
        ActionView::DeployGlobalContract { .. } => "DeployGlobalContract",
        ActionView::DeployGlobalContractByAccountId { .. } => "DeployGlobalContractByAccountId",
        ActionView::UseGlobalContract { .. } => "UseGlobalContract",
        ActionView::UseGlobalContractByAccountId { .. } => "UseGlobalContractByAccountId",
        ActionView::DeterministicStateInit { .. } => "DeterministicStateInit",
    }
    .to_string()
}
