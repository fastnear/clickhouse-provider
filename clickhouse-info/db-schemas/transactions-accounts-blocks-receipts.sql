-- More ClickHouse tables.
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
ORDER BY (transaction_hash);

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
ORDER BY (account_id, tx_block_height, transaction_hash);

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
ORDER BY (block_height, transaction_hash);

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
ORDER BY (tx_block_height, receipt_id);

CREATE TABLE blocks
(
    block_height     UInt64 COMMENT 'The block height',
    block_hash       String COMMENT 'The block hash',
    block_timestamp  DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC',
    prev_block_height Nullable(UInt64) COMMENT 'The previous block height',
    epoch_id         String COMMENT 'The epoch ID',
    chunks_included  UInt64 COMMENT 'The number of chunks included in the block',
    prev_block_hash  String COMMENT 'The previous block hash',
    author_id        String COMMENT 'The account ID of the block author',
    signature        String COMMENT 'The block signature',
    protocol_version UInt32 COMMENT 'The protocol version',

    INDEX            block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX            author_id_bloom_index author_id TYPE bloom_filter() GRANULARITY 1,
    INDEX            epoch_id_bloom_index epoch_id TYPE bloom_filter() GRANULARITY 1,
    INDEX            block_hash_bloom_index block_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX            protocol_version_minmax_idx protocol_version TYPE minmax GRANULARITY 1,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height)
ORDER BY (block_height)
