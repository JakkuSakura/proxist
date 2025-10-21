DROP TABLE IF EXISTS ticks;
CREATE TABLE ticks
(
    tenant String,
    shard_id String,
    symbol String,
    ts_micros Int64,
    payload_base64 String,
    seq UInt64
) ENGINE = MergeTree
ORDER BY (tenant, symbol, ts_micros);

SELECT 'ready' FORMAT TabSeparated;
