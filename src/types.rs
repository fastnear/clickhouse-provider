use clickhouse::Row;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::views::{ReceiptEnumView, SignedTransactionView};
use fastnear_primitives::near_indexer_primitives::{views, CryptoHash};
use fastnear_primitives::near_primitives::types::BlockHeight;
use fastnear_primitives::near_primitives::views::{
    ExecutionOutcomeView, ExecutionOutcomeWithIdView,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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
   shard_id           UInt64 COMMENT 'The shard ID where the transaction was included',
   receiver_id        String COMMENT 'The account ID of the transaction receiver',
   signer_public_key  String COMMENT 'The public key of the transaction signer',
   priority_fee       UInt64 COMMENT 'The priority fee of the transaction',
   nonce              UInt64 COMMENT 'The nonce of the transaction',
   is_relayed         Bool COMMENT 'Whether the transaction is relayed or not',
   real_signer_id     String COMMENT 'The account ID of the signer of the delegated transaction action, if applicable. Otherwise same as signer_id',
   real_receiver_id   String COMMENT 'The account ID of the receiver of the delegated transaction action, if applicable. Otherwise same as receiver_id',
   is_success         Bool COMMENT 'Whether the transaction execution was successful or not. Pending transactions are considered not successful',
   gas_burnt          UInt64 COMMENT 'The amount of burnt gas for the execution of the whole transaction',
   tokens_burnt       UInt128 COMMENT 'The amount of tokens in yoctoNEAR burnt for the execution of the whole transaction',
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
    pub gas_burnt: u64,
    pub tokens_burnt: u128,
}

/*
    account_id            String COMMENT 'The account ID',
    transaction_hash      String COMMENT 'The transaction hash',
    tx_block_height       UInt64 COMMENT 'The block height when the transaction was included into the blockchain',
    tx_block_timestamp    DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
    tx_index              UInt32 COMMENT 'The index of the transaction in the block',
    is_signer             Bool COMMENT 'True if the account signed the transaction',
    is_delegated_signer   Bool COMMENT 'True if the account was the signer of the delegated transaction action',
    is_real_signer        Bool COMMENT 'True if the account was the real signer of the transaction (either direct or delegated, excluding relayer signer)',
    is_any_signer         Bool COMMENT 'True if the account was the signer of the delegated transaction action or the signer of the transaction',
    is_predecessor        Bool COMMENT 'True if the account was the predecessor of a receipt',
    is_explicit_refund_to Bool COMMENT 'True if the account was the explicitly set as a refund_to account of an action receipt',
    is_receiver           Bool COMMENT 'True if the account was the receiver of a receipt',
    is_real_receiver      Bool COMMENT 'True if the account was the receiver of a receipt (excluding relayer receiver and gas refunds)',
    is_function_call      Bool COMMENT 'True if the account was the target of a function call action',
    is_action_arg         Bool COMMENT 'True if the account was involved in action arguments',
    is_event_log          Bool COMMENT 'True if the account was involved in JSON event logs',
    is_success            Bool COMMENT 'Whether the transaction execution was successful or not. Pending transactions are considered not successful',
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
    pub is_explicit_refund_to: bool,
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

    pub fn set_explicit_refund_to(&mut self) -> &mut Self {
        self.is_explicit_refund_to = true;
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
   block_height         UInt64 COMMENT 'The block height when the receipt was executed',
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
   priority             UInt64 COMMENT 'The priority of the receipt',
   shard_id             UInt64 COMMENT 'The shard ID where the receipt was included',
   is_success           Bool COMMENT 'Whether the receipt execution was successful or not, true for Data and GlobalContractDistribution receipts',
*/
#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct ReceiptTxRow {
    pub receipt_id: String,
    pub block_height: u64,
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
    pub is_success: bool,
}

impl ReceiptTxRow {
    pub fn new(
        receipt: &ImprovedReceiptView,
        receipt_index: u32,
        pending_transaction: &PendingTransaction,
        block_info: &BlockInfo,
        shard_id: u64,
        is_success: bool,
    ) -> Self {
        let receipt_type = match &receipt.receipt {
            ReceiptEnumView::Action { .. } => "Action",
            ReceiptEnumView::Data { .. } => "Data",
            ReceiptEnumView::GlobalContractDistribution { .. } => "GlobalContractDistribution",
        }
        .to_string();
        Self {
            receipt_id: receipt.receipt_id.to_string(),
            block_height: block_info.block_height,
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
            is_success,
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
   gas_burnt         UInt64 COMMENT 'The total gas burnt in the block',
   tokens_burnt      UInt128 COMMENT 'The total tokens burnt in yoctoNEAR in the block',
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
    pub gas_burnt: u64,
    pub tokens_burnt: u128,
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BlockInfo {
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
    pub block_timestamp: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ImprovedExecutionOutcome {
    pub block_hash: CryptoHash,
    pub block_timestamp: u64,
    pub block_height: u64,
    // Either tx_index or receipt_index.
    pub index: u32,
    pub id: CryptoHash,
    pub outcome: ExecutionOutcomeView,
}

impl ImprovedExecutionOutcome {
    pub fn from_outcome(
        mut outcome: ExecutionOutcomeWithIdView,
        block_timestamp: u64,
        block_height: BlockHeight,
        index: u32,
    ) -> Self {
        outcome.outcome.metadata.gas_profile = None;
        Self {
            block_hash: outcome.block_hash,
            block_timestamp,
            block_height,
            index,
            id: outcome.id,
            outcome: outcome.outcome,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ImprovedReceiptView {
    /// The block in which this receipt was included into a chunk.
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
    pub block_timestamp: u64,
    /// The index of the receipt in the block's chunk receipts vector.
    pub receipt_index: u32,

    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHash,

    pub receipt: ReceiptEnumView,
    // Default value used when deserializing ReceiptView which are missing the `priority` field.
    // Data which is missing this field was serialized before the introduction of priority.
    // For ReceiptV0 ReceiptPriority::NoPriority => 0
    #[serde(default)]
    pub priority: u64,
}

impl ImprovedReceiptView {
    pub fn from_receipt(
        receipt: views::ReceiptView,
        receipt_index: u32,
        block_info: &BlockInfo,
    ) -> Self {
        Self {
            block_height: block_info.block_height,
            block_hash: block_info.block_hash,
            block_timestamp: block_info.block_timestamp,
            receipt_index,
            predecessor_id: receipt.predecessor_id,
            receiver_id: receipt.receiver_id,
            receipt_id: receipt.receipt_id,
            receipt: receipt.receipt,
            priority: receipt.priority,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ImprovedExecutionOutcomeWithReceipt {
    pub execution_outcome: ImprovedExecutionOutcome,
    pub receipt: ImprovedReceiptView,
}

/*
   receipt_id             String COMMENT 'The receipt hash',
   block_height           UInt64 COMMENT 'The block height when the receipt was executed',
   block_timestamp        DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the receipt was executed',
   receipt_index          UInt32 COMMENT 'Index of the receipt that was executed in the block across all shards',
   action_index           UInt16 COMMENT 'Index of the actions within the receipt',
   block_action_index     UInt32 COMMENT 'Index of the action within the block across all shards',
   transaction_hash       String COMMENT 'Transaction hash',
   signer_id              String COMMENT 'The account ID of the transaction signer',
   predecessor_id         String COMMENT 'The account ID of the receipt predecessor',
   receiver_id            String COMMENT 'The account ID of where the receipt is executed',
   refund_to_id           Nullable(String) COMMENT 'The account ID where the attached deposit refund is sent to, if any',
   action_type            LowCardinality(String) COMMENT 'The type of the action',

   is_success             bool COMMENT 'True, if the receipt execution was successful',
   num_actions            UInt16 COMMENT 'The total number of actions in the receipt',
   gas_burnt              UInt64 COMMENT 'The amount of burnt gas for the execution of the whole receipt (not just this action)',
   tokens_burnt           UInt128 COMMENT 'The amount of tokens in yoctoNEAR burnt for the execution of the whole receipt (not just this action)',

   success_value          Nullable(String) COMMENT 'Value, if the status is SuccessValue (it may be a binary string)',
   success_receipt        Nullable(String) COMMENT 'The receipt ID, if the status is SuccessReceipt',
   success_value_int      Nullable(UInt128) COMMENT 'The parsed integer string from the returned value of the FunctionCall action',

   contract_hash          Nullable(String) COMMENT 'The hash of the contract if the action is DeployContract, DeployGlobalContract, DeployGlobalContractByAccountId, UseGlobalContract',
   public_key             Nullable(String) COMMENT 'The public key used in the action if the action is AddKey or DeleteKey',
   access_key_contract_id Nullable(String) COMMENT 'The contract ID of the limited access key if the action is AddKey and not a full access key',
   deposit                Nullable(UInt128) COMMENT 'The amount of attached deposit in yoctoNEAR if the action is FunctionCall, Stake or Transfer',
   gas_price              UInt128 COMMENT 'The gas price in yoctoNEAR for the receipt',
   attached_gas           Nullable(UInt64) COMMENT 'The amount of attached gas if the action is FunctionCall',
   method_name            Nullable(String) COMMENT 'The method name if the action is FunctionCall (truncated to 64 characters)',
   args                   Nullable(String) COMMENT 'The arguments if the action is FunctionCall (truncated to 512 characters)',
   delegate_receiver_id   Nullable(String) COMMENT 'The delegate receiver ID if the action is DelegateAction (valid account ID)',
   global_account_id      Nullable(String) COMMENT 'The global contract account ID if the action is UseGlobalContractByAccountId (valid account ID)',
   beneficiary_id         Nullable(String) COMMENT 'The beneficiary account ID if the action is DeleteAccount (valid account ID)',

   args_account_id        Nullable(String) COMMENT '`account_id` argument from the JSON arguments if the action is FunctionCall (valid account ID)',
   args_new_account_id    Nullable(String) COMMENT '`new_account_id` argument from the JSON arguments if the action is FunctionCall  (valid account ID)',
   args_owner_id          Nullable(String) COMMENT '`owner_id` argument from the JSON arguments if the action is FunctionCall  (valid account ID)',
   args_receiver_id       Nullable(String) COMMENT '`receiver_id` argument from the JSON arguments if the action is FunctionCall (valid account ID)',
   args_sender_id         Nullable(String) COMMENT '`sender_id` argument from the JSON arguments if the action is FunctionCall (valid account ID)',
   args_token_id          Nullable(String) COMMENT '`token_id` argument from the JSON arguments if the action is FunctionCall (truncated to 160 characters). For MT standard, the first `token_id` from `token_ids` array, when `token_id` does not exist',
   args_amount            Nullable(UInt128) COMMENT '`amount` argument from the JSON arguments if the action is FunctionCall. For MT standard, the first `amount` from `amounts` array, when `amount` does not exist',
   args_balance           Nullable(UInt128) COMMENT '`balance` argument from the JSON arguments if the action is FunctionCall',
   args_nft_contract_id   Nullable(String) COMMENT '`nft_contract_id` argument from the JSON arguments if the action is FunctionCall (valid account ID)',
   args_nft_token_id      Nullable(String) COMMENT '`nft_token_id` argument from the JSON arguments if the action is FunctionCall (truncated to 160 characters)',
*/
#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct ActionRow {
    pub receipt_id: String,
    pub block_height: u64,
    pub block_timestamp: u64,
    pub receipt_index: u32,
    pub action_index: u16,
    pub block_action_index: u32,
    pub transaction_hash: String,
    pub signer_id: String,
    pub predecessor_id: String,
    pub receiver_id: String,
    pub refund_to_id: Option<String>,
    pub action_type: String,

    pub is_success: bool,
    pub num_actions: u16,
    pub gas_burnt: u64,
    pub tokens_burnt: u128,

    pub success_value: Option<Vec<u8>>,
    pub success_receipt: Option<String>,
    pub success_value_int: Option<u128>,

    pub contract_hash: Option<String>,
    pub public_key: Option<String>,
    pub access_key_contract_id: Option<String>,
    pub deposit: Option<u128>,
    pub gas_price: u128,
    pub attached_gas: Option<u64>,
    pub method_name: Option<String>,
    pub args: Option<Vec<u8>>,
    pub delegate_receiver_id: Option<String>,
    pub global_account_id: Option<String>,
    pub beneficiary_id: Option<String>,

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
}

/*
   receipt_id                  String COMMENT 'The receipt hash',
   block_height                UInt64 COMMENT 'The block height when the receipt was executed',
   block_timestamp             DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the receipt was executed',
   receipt_index               UInt32 COMMENT 'Index of the receipt that was executed in the block across all shards',
   log_index                   UInt16 COMMENT 'Index of the log within the receipt',
   data_index                  UInt16 COMMENT 'Index of the data entry within the JSON event',
   block_data_index            UInt32 COMMENT 'Index of the log within the block across all shards',
   transaction_hash            String COMMENT 'Transaction hash',
   signer_id                   String COMMENT 'The account ID of the transaction signer',
   predecessor_id              String COMMENT 'The account ID of the receipt predecessor',
   receiver_id                 String COMMENT 'The account ID of where the receipt is executed',

   is_success                  bool COMMENT 'True, if the receipt execution was successful',
   num_actions                 UInt16 COMMENT 'The total number of actions in the receipt',
   num_logs                    UInt16 COMMENT 'The total number of logs in the receipt',
   num_data                    UInt16 COMMENT 'The number of data entries in this JSON event',

   version                     Nullable(String) COMMENT '`version` field from the JSON event (if exists, truncated to 64 characters)',
   standard                    Nullable(String) COMMENT '`standard` field from the JSON event (if exists, truncated to 64 characters)',
   event                       Nullable(String) COMMENT '`event` field from the JSON event (if exists, truncated to 64 characters)',

   data_account_id             Nullable(String) COMMENT '`account_id` field from the data object in the JSON event (valid account ID)',
   data_receiver_id            Nullable(String) COMMENT '`receiver_id` field from the data object in the JSON event (valid account ID)',
   data_owner_id               Nullable(String) COMMENT '`owner_id` field from the data object in the JSON event (valid account ID)',
   data_old_owner_id           Nullable(String) COMMENT '`old_owner_id` field from first data object in the JSON event (valid account ID)',
   data_new_owner_id           Nullable(String) COMMENT '`new_owner_id` field from first data object in the JSON event (valid account ID)',
   data_liquidation_account_id Nullable(String) COMMENT '`liquidation_account_id` field from the data object in the JSON event (valid account ID)',
   data_token_id               Nullable(String) COMMENT '`token_id` field from the first data object in the JSON event (truncated to 160 characters). For MT standard, the first `token_id` from `token_ids` array, when `token_id` does not exist. For DIP-4 standard, the first key from `tokens` map, or `token` if only single token transfer',
   data_amount                 Nullable(UInt128) COMMENT '`amount` field from the first data object in the JSON event. For MT standard, the first `amount` from `amounts` array, when `amount` does not exist. For DIP-4 standard, the first value from `tokens` map',
*/
#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct EventRow {
    pub receipt_id: String,
    pub block_height: u64,
    pub block_timestamp: u64,
    pub receipt_index: u32,
    pub log_index: u16,
    pub data_index: u16,
    pub block_data_index: u32,
    pub transaction_hash: String,
    pub signer_id: String,
    pub predecessor_id: String,
    pub receiver_id: String,

    pub is_success: bool,
    pub num_actions: u16,
    pub num_logs: u16,
    pub num_data: u16,

    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,

    pub data_account_id: Option<String>,
    pub data_receiver_id: Option<String>,
    pub data_owner_id: Option<String>,
    pub data_old_owner_id: Option<String>,
    pub data_new_owner_id: Option<String>,
    pub data_liquidation_account_id: Option<String>,
    pub data_token_id: Option<String>,
    pub data_amount: Option<u128>,
}
