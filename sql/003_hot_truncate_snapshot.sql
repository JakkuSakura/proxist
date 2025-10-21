-- Reset tables
DROP TABLE IF EXISTS proxist.hot_ticks;
DROP TABLE IF EXISTS proxist.ticks_persist;

CREATE TABLE proxist.hot_ticks
(
    tenant String,
    symbol String,
    ts DateTime64(6),
    px Float64
) ENGINE = Memory;

CREATE TABLE proxist.ticks_persist
(
    tenant String,
    symbol String,
    ts DateTime64(6),
    px Float64
) ENGINE = MergeTree
ORDER BY (tenant, symbol, ts);

-- Seed and persist
INSERT INTO proxist.hot_ticks VALUES ('alpha', 'AAPL', toDateTime64('2024-01-01 09:30:00', 6), 100.00);
INSERT INTO proxist.hot_ticks VALUES ('alpha', 'AAPL', toDateTime64('2024-01-01 09:30:01', 6), 100.50);
INSERT INTO proxist.hot_ticks VALUES ('alpha', 'MSFT', toDateTime64('2024-01-01 09:30:02', 6), 99.75);

INSERT INTO proxist.ticks_persist SELECT * FROM proxist.hot_ticks;

-- Clear the hot cache to simulate snapshot rollover
TRUNCATE TABLE proxist.hot_ticks;

SELECT 'hot' AS layer, count() AS rows
FROM proxist.hot_ticks
UNION ALL
SELECT 'persisted' AS layer, count() AS rows
FROM proxist.ticks_persist
ORDER BY layer
FORMAT TSVWithNames;
