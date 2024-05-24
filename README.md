## Clickhouse Provider based on FASTNEAR's indexed neardata xyz

### Create clickhouse table

For generic action view:

```sql
-- This is a ClickHouse table.
CREATE TABLE actions
(
    block_height           UInt64 COMMENT 'Block height',
    block_hash             String COMMENT 'Block hash',
    block_timestamp        DateTime64(9, 'UTC') COMMENT 'Block timestamp in UTC',
    transaction_hash       String COMMENT 'Transaction hash',
    receipt_id             String COMMENT 'Receipt hash',
    receipt_index          UInt32 COMMENT 'Index of the receipt that appears in the block across all shards',
    action_index           UInt16 COMMENT 'Index of the actions within the receipt',
    signer_id              String COMMENT 'The account ID of the transaction signer',
    signer_public_key      String COMMENT 'The public key of the transaction signer',
    predecessor_id         String COMMENT 'The account ID of the receipt predecessor',
    account_id             String COMMENT 'The account ID of where the receipt is executed',
    status                 Enum('FAILURE', 'SUCCESS') COMMENT 'The status of the receipt execution, either SUCCESS or FAILURE',
    action                 Enum('CREATE_ACCOUNT', 'DEPLOY_CONTRACT', 'FUNCTION_CALL', 'TRANSFER', 'STAKE', 'ADD_KEY', 'DELETE_KEY', 'DELETE_ACCOUNT', 'DELEGATE') COMMENT 'The action type',
    action_json            String COMMENT 'The JSON serialization of the ActionView',
    input_data_ids         Array(String) COMMENT 'The input data IDs for the receipt data dependencies of the action',

    status_success_value   Nullable(String) COMMENT 'Value, if the status is SuccessValue (either UTF8 string or a base64:)',
    status_success_receipt Nullable(String) COMMENT 'The receipt ID, if the status is SuccessReceipt',
    status_failure         Nullable(String) COMMENT 'The json serialized error message, if the status is Failure',

    contract_hash          Nullable(String) COMMENT 'The hash of the contract if the action is DEPLOY_CONTRACT',
    public_key             Nullable(String) COMMENT 'The public key used in the action if the action is ADD_KEY or DELETE_KEY',
    access_key_contract_id Nullable(String) COMMENT 'The contract ID of the limited access key if the action is ADD_KEY and not a full access key',
    deposit                Nullable(UInt128) COMMENT 'The amount of attached deposit in yoctoNEAR if the action is FUNCTION_CALL, STAKE or TRANSFER',
    gas_price              UInt128 COMMENT 'The gas price in yoctoNEAR for the receipt',
    attached_gas           Nullable(UInt64) COMMENT 'The amount of attached gas if the action is FUNCTION_CALL',
    gas_burnt              UInt64 COMMENT 'The amount of burnt gas for the execution of the whole receipt',
    tokens_burnt           UInt128 COMMENT 'The amount of tokens in yoctoNEAR burnt for the execution of the whole receipt',
    method_name            Nullable(String) COMMENT 'The method name if the action is FUNCTION_CALL',
    args                   Nullable(String) COMMENT 'The arguments if the action is FUNCTION_CALL (either UTF8 string or base64:)',

    INDEX                  block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX                  account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, account_id)
ORDER BY (block_height, account_id, receipt_index, action_index)

CREATE TABLE events
(
    block_height      UInt64 COMMENT 'Block height',
    block_hash        String COMMENT 'Block hash',
    block_timestamp   DateTime64(9, 'UTC') COMMENT 'Block timestamp in UTC',
    transaction_hash  String COMMENT 'Transaction hash',
    receipt_id        String COMMENT 'Receipt hash',
    receipt_index     UInt32 COMMENT 'Index of the receipt that appears in the block across all shards',
    log_index         UInt16 COMMENT 'Index of the log within the receipt',
    signer_id         String COMMENT 'The account ID of the transaction signer',
    signer_public_key String COMMENT 'The public key of the transaction signer',
    predecessor_id    String COMMENT 'The account ID of the receipt predecessor',
    account_id        String COMMENT 'The account ID of where the receipt is executed',
    status            Enum('FAILURE', 'SUCCESS') COMMENT 'The status of the receipt execution, either SUCCESS or FAILURE',
    log               String COMMENT 'The LogEntry',

    version           Nullable(String) COMMENT '`version` field from the JSON event (if exists)',
    standard          Nullable(String) COMMENT '`standard` field from the JSON event (if exists)',
    event             Nullable(String) COMMENT '`event` field from the JSON event (if exists)',

    INDEX             block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX             account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, account_id)
ORDER BY (block_height, account_id, receipt_index, log_index)

CREATE TABLE data
(
    block_height    UInt64 COMMENT 'Block height',
    block_hash      String COMMENT 'Block hash',
    block_timestamp DateTime64(9, 'UTC') COMMENT 'Block timestamp in UTC',
    receipt_id      String COMMENT 'Receipt hash',
    receipt_index   UInt32 COMMENT 'Index of the receipt that appears in the block across all shards',
    predecessor_id  String COMMENT 'The account ID of the receipt predecessor',
    account_id      String COMMENT 'The account ID of where the receipt is executed',
    data_id         String COMMENT 'The Data ID',
    data            Nullable(String) COMMENT 'The Data (either UTF8 string or base64:)',

    INDEX           block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX           account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, account_id)
ORDER BY (block_height, account_id, receipt_index)

```

### Clickhouse explorer tables

The explorer is transaction focused. Everything is bundled around transactions.

```sql
-- This is a ClickHouse table.
CREATE TABLE transactions
(
    transaction_hash   String COMMENT 'Transaction hash',
    signer_id          String COMMENT 'The account ID of the transaction signer',
    tx_block_height    UInt64 COMMENT 'The block height when the transaction was included',
    tx_block_hash      String COMMENT 'The block hash when the transaction was included',
    tx_block_timestamp DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
    transaction        String COMMENT 'The JSON serialization of the transaction view without profiling and proofs',
    last_block_height  UInt64 COMMENT 'The block height when the last receipt was processed for the transaction',

    INDEX              signer_id_bloom_index signer_id TYPE bloom_filter() GRANULARITY 1,
    INDEX              tx_block_height_minmax_idx tx_block_height TYPE minmax GRANULARITY 1,
    INDEX              tx_block_timestamp_minmax_idx tx_block_timestamp TYPE minmax GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (transaction_hash)
ORDER BY (transaction_hash)

CREATE TABLE account_txs
(
    account_id         String COMMENT 'The account ID',
    transaction_hash   String COMMENT 'The transaction hash',
    signer_id          String COMMENT 'The account ID of the transaction signer',
    tx_block_height    UInt64 COMMENT 'The block height when the transaction was included',
    tx_block_timestamp DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',

    INDEX              tx_block_timestamp_minmax_idx tx_block_timestamp TYPE minmax GRANULARITY 1,

) ENGINE = ReplacingMergeTree
PRIMARY KEY (account_id, tx_block_height)
ORDER BY (account_id, tx_block_height, transaction_hash)

CREATE TABLE block_txs
(
    block_height     UInt64 COMMENT 'The block height',
    block_hash       String COMMENT 'The block hash',
    block_timestamp  DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC',
    transaction_hash String COMMENT 'The transaction hash',
    signer_id        String COMMENT 'The account ID of the transaction signer',
    tx_block_height  UInt64 COMMENT 'The block height when the transaction was included',

    INDEX            block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height)
ORDER BY (block_height, transaction_hash)

CREATE TABLE receipt_txs
(
    receipt_id         String COMMENT 'The receipt hash',
    transaction_hash   String COMMENT 'The transaction hash',
    signer_id          String COMMENT 'The account ID of the transaction signer',
    tx_block_height    UInt64 COMMENT 'The block height when the transaction was included',
    tx_block_timestamp DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',

    INDEX              receipt_id_bloom_index receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX              tx_block_timestamp_minmax_idx tx_block_height TYPE minmax GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (tx_block_height)
ORDER BY (tx_block_height, receipt_id)

```
