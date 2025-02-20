-------------------------------------------------
-- Meta tables to store Substreams information --
-------------------------------------------------

CREATE TABLE IF NOT EXISTS cursors
(
    id        String,
    cursor    String,
    block_num Int64,
    block_id  String,
    version   UInt64  -- Added version column for ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (id);

-------------------------------------------------
-- Table for all balance changes event --
-------------------------------------------------

CREATE TABLE IF NOT EXISTS balance_changes (
    -- block --
    timestamp           DateTime(0, 'UTC'),
    block_num           UInt32,
    date                Date,

    -- transaction --
    transaction_id      FixedString(64),
    call_index          UInt32,
    `index`             UInt32,
    version             UInt64,

    -- balance change --
    contract            FixedString(40),
    owner               FixedString(40),
    amount              UInt256,
    old_balance         UInt256,
    new_balance         UInt256
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (transaction_id, version);

-------------------------------------------------
-- Table for latest balances --
-------------------------------------------------

CREATE TABLE IF NOT EXISTS latest_balances
(
    contract                    FixedString(40),
    owner                       FixedString(40),
    balance                     UInt256,

    last_transaction_id         FixedString(64),
    last_block_num              UInt32,
    last_call_index             UInt32,
    last_timestamp              DateTime,
    last_date                   Date,
    version                     UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (contract, owner);

CREATE MATERIALIZED VIEW IF NOT EXISTS latest_balances_mv
TO latest_balances
AS SELECT
    contract,
    owner,
    argMax(new_balance, version) AS balance,
    argMax(block_num, version) AS last_block_num,
    argMax(timestamp, version) AS last_timestamp,
    argMax(date, version) AS last_date,
    max(version) AS last_version,  -- Use max() directly instead of argMax(version, version)
    argMax(transaction_id, version) AS last_transaction_id
FROM balance_changes
GROUP BY contract, owner;


-------------------------------------------------
-- Table for daily balances --
-------------------------------------------------

CREATE TABLE IF NOT EXISTS daily_balances
(
    date         Date,
    contract     FixedString(40),
    owner        FixedString(40),
    balance      UInt256,
    version      UInt32
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (owner, contract, date);

-------------------------------------------------
-- Table for all token information --
-------------------------------------------------

CREATE TABLE IF NOT EXISTS contracts (
    contract    FixedString(40),
    name        String,
    symbol      String,
    decimals    UInt64,
    block_num   UInt32,
    timestamp   DateTime(0, 'UTC')  -- Removed extra comma
)
ENGINE = ReplacingMergeTree()
ORDER BY (contract);

-------------------------------------------------
-- Table for token supply --
-------------------------------------------------

CREATE TABLE IF NOT EXISTS supply (
    contract    FixedString(40),
    supply      UInt256,
    block_num   UInt32,
    timestamp   DateTime(0, 'UTC')
)
ENGINE = ReplacingMergeTree()
ORDER BY (contract, supply);

-------------------------------------------------
-- Table for transfers events --
-------------------------------------------------

CREATE TABLE IF NOT EXISTS transfers (
    id          String,
    contract    FixedString(40),
    `from`      FixedString(40),
    `to`        FixedString(40),
    value       UInt256,  -- Changed from String to UInt256 for consistency
    tx_id       FixedString(64),
    action_index UInt32,
    block_num   UInt32,
    timestamp   DateTime(0, 'UTC')
)
ENGINE = ReplacingMergeTree()
ORDER BY (id, tx_id, block_num, timestamp);
