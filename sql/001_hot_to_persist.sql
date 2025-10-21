-- Setup: drop any existing tables to start clean
DROP TABLE IF EXISTS proxist.hot_ticks;
DROP TABLE IF EXISTS proxist.ticks_persist;

-- Create an in-memory table to simulate the hot engine
CREATE TABLE proxist.hot_ticks
(
    tenant String,
    symbol String,
    ts DateTime64(6),
    px Float64
) ENGINE = Memory;

-- Create a persistent table for cold storage
CREATE TABLE proxist.ticks_persist
(
    tenant String,
    symbol String,
    ts DateTime64(6),
    px Float64
) ENGINE = MergeTree
ORDER BY (tenant, symbol, ts);

-- Seed hot data
INSERT INTO proxist.hot_ticks VALUES
('alpha', 'AAPL', toDateTime64('2024-01-01 12:00:00', 6), 100.75),
('alpha', 'AAPL', toDateTime64('2024-01-01 12:00:01', 6), 100.50),
('alpha', 'MSFT', toDateTime64('2024-01-01 12:00:02', 6), 99.50);

-- Persist the in-memory rows into the durable table
INSERT INTO proxist.ticks_persist
SELECT tenant, symbol, ts, px
FROM proxist.hot_ticks;

-- Report results for verification
SELECT tenant, symbol, count() AS rows, round(sum(px), 2) AS total_px
FROM proxist.ticks_persist
GROUP BY tenant, symbol
ORDER BY tenant, symbol
FORMAT TSVWithNames;

