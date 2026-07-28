CREATE DATABASE IF NOT EXISTS evm;

CREATE TABLE IF NOT EXISTS evm.blocks
(
    event_id String,
    chain_id UInt32,
    block_number UInt64,
    block_hash String,
    parent_hash String,
    transactions_count UInt32,
    canonical UInt8 DEFAULT 1,
    version UInt64 DEFAULT 1,
    received_at DateTime64(3,'UTC') DEFAULT now64(3),
    raw_payload String
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY intDiv(block_number,1000000)
ORDER BY(
    chain_id,
    block_number,
    block_hash
);

CREATE TABLE IF NOT EXISTS evm.transactions
(
    event_id String,
    chain_id UInt32,
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    transaction_index UInt32,
    from_address String,
    to_address Nullable(String),
    value String,
    gas UInt64,
    gas_price String,
    input String,
    canonical UInt8 DEFAULT 1,
    version UInt64 DEFAULT 1,
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),
    raw_payload String
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (
    chain_id,
    block_hash,
    transaction_hash
);


CREATE TABLE IF NOT EXISTS evm.logs
(
    event_id String,
    chain_id UInt32,
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    contract_address String,
    topics Array(String),
    data String,
    removed UInt8 DEFAULT 0,
    canonical UInt8 DEFAULT 1,
    version UInt64 DEFAULT 1,
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),
    raw_payload String
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (
    chain_id,
    block_hash,
    transaction_hash,
    log_index
);