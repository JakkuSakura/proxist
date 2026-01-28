DROP TABLE IF EXISTS ticks_036;
CREATE TABLE ticks_036
(
    symbol String,
    ts DateTime64(6),
    value UInt32
) ENGINE = MergeTree
ORDER BY (symbol, ts);

INSERT INTO ticks_036 (symbol, ts, value) VALUES
('AAPL', toDateTime64('2024-01-01 10:00:00', 6), 10),
('AAPL', toDateTime64('2024-01-01 10:00:02', 6), 20),
('MSFT', toDateTime64('2024-01-01 10:00:01', 6), 5);

SELECT symbol, max(ts) AS last_ts
FROM ticks_036
GROUP BY symbol
ORDER BY symbol
FORMAT TSVWithNames;
