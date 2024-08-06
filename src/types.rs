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
    pub id: CryptoHash,
    pub outcome: ExecutionOutcomeView,
}

impl ImprovedExecutionOutcome {
    pub fn from_outcome(
        mut outcome: ExecutionOutcomeWithIdView,
        block_timestamp: u64,
        block_height: BlockHeight,
    ) -> Self {
        outcome.outcome.metadata.gas_profile = None;
        Self {
            block_hash: outcome.block_hash,
            block_timestamp,
            block_height,
            id: outcome.id,
            outcome: outcome.outcome,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ImprovedExecutionOutcomeWithReceipt {
    pub execution_outcome: ImprovedExecutionOutcome,
    pub receipt: views::ReceiptView,
}
