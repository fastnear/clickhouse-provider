use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::views::ReceiptEnumView;
use fastnear_primitives::near_indexer_primitives::{views, CryptoHash};
use fastnear_primitives::near_primitives::types::BlockHeight;
use fastnear_primitives::near_primitives::views::{
    ExecutionOutcomeView, ExecutionOutcomeWithIdView,
};

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
