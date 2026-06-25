CREATE DATABASE IF NOT EXISTS turbofan;
USE turbofan;

-- raw_data is written by an at-least-once Kafka consumer.
-- If ClickHouse insert succeeds but Kafka offset commit is retried, duplicate
-- raw rows may appear. event_id makes duplicates detectable; exact
-- deduplication can be added later with ReplacingMergeTree or materialized views.
CREATE TABLE IF NOT EXISTS raw_data (
    event_id String,
    engine_id String,
    timestamp DateTime,
    cycle UInt16,
    altitude Float32,
    mach_number Float32,
    throttle Float32,
    T2 Float32,
    T50 Float32,
    P2 Float32,
    P15 Float32,
    Nf Float32,
    Nc Float32,
    Ps30 Float32,
    phi Float32,
    NRf Float32,
    NRc Float32,
    BPR Float32,
    anomaly Bool,
    RUL UInt16
) ENGINE = MergeTree()
ORDER BY (engine_id, timestamp, event_id);

CREATE TABLE IF NOT EXISTS main_stats (
    window_start DateTime,
    window_end DateTime,
    engine_id String,
    avg_T2 Float32,
    avg_T50 Float32,
    avg_Nf Float32,
    avg_Nc Float32,
    avg_P2 Float32,
    avg_Ps30 Float32,
    anomaly_count UInt16,
    min_RUL UInt16
) ENGINE = MergeTree()
ORDER BY (window_start, window_end, engine_id);
