## Alternative Implementation for ScyllaDB

### Example .env file

```
SCYLLADB_IP=127.0.0.1
SCYLLADB_PORT=9142
SCYLLADB_USER=default
SCYLLADB_PASSWORD=password
SCYLLADB_KEYSPACE=mainnet_tx
SCYLLADB_SSL_CERTFILE=./certs/ca-cert.pem
SCYLLADB_SSL_USERKEY=./certs/client.key
SCYLLADB_SSL_USERCERT=./certs/client.crt
SCYLLADB_PREFERRED_DC=dc1
SCYLLADB_MAX_RETRY = "${RPC_MAX_RETRY}"
SCYLLADB_STRICT_MODE = "${RPC_STRICT_MODE}"
SCYLLADB_KEEPALIVE_INTERVAL = 1

NUM_FETCHING_THREADS=8
CLICKHOUSE_SKIP_COMMIT=false
COMMIT_EVERY_BLOCK=false
CHAIN_ID=testnet
```

### ScyllaDB tables

```cql
CREATE TABLE mainnet_tx.transactions (
    transaction_hash text,
    tx_block_height bigint,
    last_block_height bigint,
    signer_id text,
    transaction text,
    tx_block_hash text,
    tx_block_timestamp bigint,
    tx_shard_id int,
    tx_order_in_block int,
    status int,
    PRIMARY KEY ((transaction_hash), tx_block_height, tx_shard_id, tx_order_in_block)
) WITH CLUSTERING ORDER BY (tx_block_height ASC, tx_shard_id ASC, tx_order_in_block ASC);

CREATE TABLE mainnet_tx.account_txs (
    account_id text,
    transaction_hash text,
    signer_id text,
    tx_block_height bigint,
    tx_block_timestamp bigint,
    tx_shard_id int,
    tx_order_in_block int,
    PRIMARY KEY ((account_id), tx_block_height, tx_shard_id, tx_order_in_block)
) WITH CLUSTERING ORDER BY (tx_block_height ASC, tx_shard_id ASC, tx_order_in_block ASC);

CREATE TABLE mainnet_tx.block_txs (
    block_height bigint,
    block_hash text,
    block_timestamp bigint,
    transaction_hash text,
    signer_id text,
    tx_block_height bigint,
    tx_block_timestamp bigint,
    tx_shard_id int,
    tx_order_in_block int,
    PRIMARY KEY ((block_height), tx_block_height, tx_shard_id, tx_order_in_block)
) WITH CLUSTERING ORDER BY (tx_block_height ASC, tx_shard_id ASC, tx_order_in_block ASC);

CREATE TABLE mainnet_tx.receipt_txs (
    receipt_id text,
    transaction_hash text,
    signer_id text,
    tx_block_height bigint,
    tx_block_timestamp bigint,
    tx_shard_id int,
    tx_order_in_block int,
    PRIMARY KEY ((receipt_id), tx_block_height, tx_shard_id, tx_order_in_block)
) WITH CLUSTERING ORDER BY (tx_block_height ASC, tx_shard_id ASC, tx_order_in_block ASC);

CREATE TABLE mainnet_tx.blocks (
    block_height bigint,
    block_hash text,
    block_timestamp bigint,
    prev_block_height bigint,
    epoch_id text,
    chunks_included bigint,
    prev_block_hash text,
    author_id text,
    signature text,
    protocol_version int,
    num_transactions int,
    num_receipts int,
    PRIMARY KEY ((block_hash), block_height)
) WITH CLUSTERING ORDER BY (block_height ASC);
```

## Clickhouse Provider based on FASTNEAR's indexed neardata xyz

### Example .env

```

DATABASE_URL=http://localhost:8123
DATABASE_USER=default
DATABASE_PASSWORD=password
DATABASE_DATABASE=default
NUM_FETCHING_THREADS=8
CLICKHOUSE_SKIP_COMMIT=false
COMMIT_EVERY_BLOCK=false
CHAIN_ID=testnet

```

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
    action                 Enum('CREATE_ACCOUNT', 'DEPLOY_CONTRACT', 'FUNCTION_CALL', 'TRANSFER', 'STAKE', 'ADD_KEY', 'DELETE_KEY', 'DELETE_ACCOUNT', 'DELEGATE', 'NON_REFUNDABLE_STORAGE_TRANSFER') COMMENT 'The action type',
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

    args_account_id        Nullable(String) COMMENT '`account_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_new_account_id    Nullable(String) COMMENT '`new_account_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_owner_id          Nullable(String) COMMENT '`owner_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_receiver_id       Nullable(String) COMMENT '`receiver_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_sender_id         Nullable(String) COMMENT '`sender_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_token_id          Nullable(String) COMMENT '`token_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_amount            Nullable(UInt128) COMMENT '`amount` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_balance           Nullable(UInt128) COMMENT '`balance` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_nft_contract_id   Nullable(String) COMMENT '`nft_contract_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_nft_token_id      Nullable(String) COMMENT '`nft_token_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    return_value_int       Nullable(UInt128) COMMENT 'The parsed integer string from the returned value of the FUNCTION_CALL action',

    INDEX                  block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX                  account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  signer_id_bloom_index signer_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  block_hash_bloom_index block_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX                  transaction_hash_bloom_index transaction_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX                  receipt_id_bloom_index receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  precise_public_key_bloom_index public_key TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX                  predecessor_id_bloom_index predecessor_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  method_name_index method_name TYPE set(0) GRANULARITY 1,
    INDEX                  args_account_id_bloom_index args_account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  args_new_account_id_bloom_index args_new_account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  args_owner_id_bloom_index args_owner_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  args_receiver_id_bloom_index args_receiver_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                  args_sender_id_bloom_index args_sender_id TYPE bloom_filter() GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, account_id)
ORDER BY (block_height, account_id, receipt_index, action_index)

CREATE TABLE events
(
    block_height                UInt64 COMMENT 'Block height',
    block_hash                  String COMMENT 'Block hash',
    block_timestamp             DateTime64(9, 'UTC') COMMENT 'Block timestamp in UTC',
    transaction_hash            String COMMENT 'Transaction hash',
    receipt_id                  String COMMENT 'Receipt hash',
    receipt_index               UInt32 COMMENT 'Index of the receipt that appears in the block across all shards',
    log_index                   UInt16 COMMENT 'Index of the log within the receipt',
    signer_id                   String COMMENT 'The account ID of the transaction signer',
    signer_public_key           String COMMENT 'The public key of the transaction signer',
    predecessor_id              String COMMENT 'The account ID of the receipt predecessor',
    account_id                  String COMMENT 'The account ID of where the receipt is executed',
    status                      Enum('FAILURE', 'SUCCESS') COMMENT 'The status of the receipt execution, either SUCCESS or FAILURE',
    log                         String COMMENT 'The LogEntry',

    version                     Nullable(String) COMMENT '`version` field from the JSON event (if exists)',
    standard                    Nullable(String) COMMENT '`standard` field from the JSON event (if exists)',
    event                       Nullable(String) COMMENT '`event` field from the JSON event (if exists)',

    data_account_id             Nullable(String) COMMENT '`account_id` field from the first data object in the JSON event',
    data_owner_id               Nullable(String) COMMENT '`owner_id` field from the first data object in the JSON event',
    data_old_owner_id           Nullable(String) COMMENT '`old_owner_id` field from the first data object in the JSON event',
    data_new_owner_id           Nullable(String) COMMENT '`new_owner_id` field from the first data object in the JSON event',
    data_liquidation_account_id Nullable(String) COMMENT '`liquidation_account_id` field from the first data object in the JSON event',
    data_authorized_id          Nullable(String) COMMENT '`authorized_id` field from the first data object in the JSON event',
    data_token_ids              Array(String) COMMENT '`token_ids` field from the first data object in the JSON event',
    data_token_id               Nullable(String) COMMENT '`token_id` field from the first data object in the JSON event',
    data_position               Nullable(String) COMMENT '`position` field from the first data object in the JSON event',
    data_amount                 Nullable(UInt128) COMMENT '`amount` field from the first data object in the JSON event',

    INDEX                       block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX                       account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                       event_set_index event TYPE set(0) GRANULARITY 1,
    INDEX                       data_account_id_bloom_index data_account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                       data_owner_id_bloom_index data_owner_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                       data_old_owner_id_bloom_index data_old_owner_id TYPE bloom_filter() GRANULARITY 1,
    INDEX                       data_new_owner_id_bloom_index data_new_owner_id TYPE bloom_filter() GRANULARITY 1,
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
    INDEX           data_id_bloom_index data_id TYPE bloom_filter() GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, account_id)
ORDER BY (block_height, account_id, receipt_index)

--- Modify the table to add new action
alter table actions modify column action Enum('CREATE_ACCOUNT', 'DEPLOY_CONTRACT', 'FUNCTION_CALL', 'TRANSFER', 'STAKE', 'ADD_KEY', 'DELETE_KEY', 'DELETE_ACCOUNT', 'DELEGATE', 'NON_REFUNDABLE_STORAGE_TRANSFER', 'DEPLOY_GLOBAL_CONTRACT', 'DEPLOY_GLOBAL_CONTRACT_BY_ACCOUNT_ID', 'USE_GLOBAL_CONTRACT', 'USE_GLOBAL_CONTRACT_BY_ACCOUNT_ID') COMMENT 'The action type';

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

CREATE TABLE blocks
(
    block_height      UInt64 COMMENT 'The block height',
    block_hash        String COMMENT 'The block hash',
    block_timestamp   DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC',
    prev_block_height Nullable(UInt64) COMMENT 'The previous block height',
    epoch_id          String COMMENT 'The epoch ID',
    chunks_included   UInt64 COMMENT 'The number of chunks included in the block',
    prev_block_hash   String COMMENT 'The previous block hash',
    author_id         String COMMENT 'The account ID of the block author',
    signature         String COMMENT 'The block signature',
    protocol_version  UInt32 COMMENT 'The protocol version',

    INDEX             block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX             author_id_bloom_index author_id TYPE bloom_filter() GRANULARITY 1,
    INDEX             epoch_id_bloom_index epoch_id TYPE bloom_filter() GRANULARITY 1,
    INDEX             block_hash_bloom_index block_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX             protocol_version_minmax_idx protocol_version TYPE minmax GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height)
ORDER BY (block_height)
```
