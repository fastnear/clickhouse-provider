### Clickhouse explorer tables

The explorer is transaction focused. Everything is bundled around transactions.

```clickhouse
-- This is a ClickHouse table.
CREATE TABLE local_raw_tx ON CLUSTER '{cluster}'
(
    transaction_hash   String COMMENT 'Transaction hash',
    tx_block_timestamp DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
    last_block_height  UInt64 COMMENT 'The block height when the last receipt was processed for the transaction',
    data               String COMMENT 'The zstd compressed raw transaction data' CODEC (NONE),

) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default/local_raw_tx', '{replica}',
                                        last_block_height)
      PARTITION BY toYYYYMM(tx_block_timestamp)
      PRIMARY KEY (transaction_hash)
      ORDER BY (transaction_hash)
      SETTINGS
          index_granularity = 128,
          index_granularity_bytes = 0, -- Disable adaptive granularity, use fixed 128 rows
          min_bytes_for_wide_part = 0, -- Force wide parts format
          min_rows_for_wide_part = 0 -- Force wide parts format

CREATE TABLE local_transactions ON CLUSTER '{cluster}'
(
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

    INDEX transaction_hash_bloom_index transaction_hash TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX signer_id_bloom_index signer_id TYPE bloom_filter() GRANULARITY 1,
    INDEX tx_block_height_minmax_idx tx_block_height TYPE minmax GRANULARITY 1,
    INDEX tx_block_timestamp_minmax_idx tx_block_timestamp TYPE minmax GRANULARITY 1,
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default/local_transactions', '{replica}',
                                        last_block_height)
      PARTITION BY toYYYYMM(tx_block_timestamp)
      PRIMARY KEY (tx_block_height)
      ORDER BY (tx_block_height, tx_index)

CREATE TABLE local_account_txs ON CLUSTER '{cluster}'
(
    account_id            String COMMENT 'The account ID',
    transaction_hash      String COMMENT 'The transaction hash',
    last_block_height     UInt64 COMMENT 'The block height when the account was last updated',
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

    INDEX tx_block_timestamp_minmax_idx tx_block_timestamp TYPE minmax GRANULARITY 1,
    INDEX tx_block_height_minmax_idx tx_block_height TYPE minmax GRANULARITY 1,

) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default/local_account_txs', '{replica}',
                                        last_block_height)
      PARTITION BY toYYYYMM(tx_block_timestamp)
      PRIMARY KEY (account_id, tx_block_height)
      ORDER BY (account_id, tx_block_height, tx_index)

CREATE TABLE local_receipt_txs ON CLUSTER '{cluster}'
(
    receipt_id           String COMMENT 'The receipt hash',
    block_height         UInt64 COMMENT 'The block height when the receipt was executed',
    block_timestamp      DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the receipt was executed',
    receipt_index        UInt32 COMMENT 'Index of the receipt that was executed in the block across all shards',
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
    shard_id             UInt64 COMMENT 'The shard ID where the receipt was executed',
    is_success           Bool COMMENT 'Whether the receipt execution was successful or not',

    INDEX receipt_id_bloom_index receipt_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX tx_block_timestamp_minmax_idx tx_block_height TYPE minmax GRANULARITY 1,
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default/local_receipt_txs', '{replica}')
      PARTITION BY toYYYYMM(block_timestamp)
      PRIMARY KEY (block_height, receipt_index)
      ORDER BY (block_height, receipt_index, receipt_id)

CREATE TABLE local_blocks ON CLUSTER '{cluster}'
(
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

    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX author_id_bloom_index author_id TYPE bloom_filter() GRANULARITY 1,
    INDEX epoch_id_bloom_index epoch_id TYPE bloom_filter() GRANULARITY 1,
    INDEX block_hash_bloom_index block_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX protocol_version_minmax_idx protocol_version TYPE minmax GRANULARITY 1,
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default/local_blocks', '{replica}')
      PARTITION BY toYYYYMM(block_timestamp)
      PRIMARY KEY (block_height)
      ORDER BY (block_height)

-- This is a ClickHouse table.
CREATE TABLE local_actions ON CLUSTER '{cluster}'
(
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

    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX receiver_id_bloom_index receiver_id TYPE bloom_filter() GRANULARITY 1,
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default/local_actions', '{replica}')
      PARTITION BY toYYYYMM(block_timestamp)
      PRIMARY KEY (block_height, block_action_index)
      ORDER BY (block_height, block_action_index)

CREATE TABLE local_events ON CLUSTER '{cluster}'
(
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
    data_amount                 Nullable(UInt128) COMMENT '`amount` field from the first data object in the JSON event. For MT standard, the first `amount` from `amounts` array, when `amount` does not exist. For DIP-4 standard, the first value from `amounts` map',

    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX receiver_id_bloom_index receiver_id TYPE bloom_filter() GRANULARITY 1,
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default/local_events', '{replica}')
      PARTITION BY toYYYYMM(block_timestamp)
      PRIMARY KEY (block_height, block_data_index)
      ORDER BY (block_height, block_data_index)
```

# Distributed Tables

```clickhouse
CREATE TABLE default.raw_tx ON CLUSTER '{cluster}'
    AS default.local_raw_tx
        ENGINE = Distributed('{cluster}', default, local_raw_tx, cityHash64(transaction_hash));

CREATE TABLE default.transactions ON CLUSTER '{cluster}'
    AS default.local_transactions
        ENGINE = Distributed('{cluster}', default, local_transactions, cityHash64(tx_block_height));

CREATE TABLE default.account_txs ON CLUSTER '{cluster}'
    AS default.local_account_txs
        ENGINE = Distributed('{cluster}', default, local_account_txs, cityHash64(account_id));

CREATE TABLE default.receipt_txs ON CLUSTER '{cluster}'
    AS default.local_receipt_txs
        ENGINE = Distributed('{cluster}', default, local_receipt_txs, cityHash64(block_height));

CREATE TABLE default.blocks ON CLUSTER '{cluster}'
    AS default.local_blocks
        ENGINE = Distributed('{cluster}', default, local_blocks, cityHash64(block_height));

CREATE TABLE default.actions ON CLUSTER '{cluster}'
    AS default.local_actions
        ENGINE = Distributed('{cluster}', default, local_actions, cityHash64(block_height));

CREATE TABLE default.events ON CLUSTER '{cluster}'
    AS default.local_events
        ENGINE = Distributed('{cluster}', default, local_events, cityHash64(block_height));
```
