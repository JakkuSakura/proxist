DROP TABLE IF EXISTS ticks_010;
CREATE TABLE ticks_010
(
    tenant String,
    symbol String,
    ts DateTime64(6),
    price Float64,
    seq UInt64
) ENGINE = MergeTree
ORDER BY (tenant, symbol, ts);

INSERT INTO ticks_010 (tenant, symbol, ts, price, seq) VALUES
('alpha', 'AAPL', toDateTime64('2024-01-01 10:00:00', 6), 188.12, 1),
('alpha', 'AAPL', toDateTime64('2024-01-01 10:00:01', 6), 188.25, 2),
('alpha', 'MSFT', toDateTime64('2024-01-01 10:00:02', 6), 342.50, 1),
('beta', 'GOOG', toDateTime64('2024-01-01 10:00:03', 6), 140.01, 1);

SELECT tenant, symbol, count() AS rows, min(price) AS min_price, max(price) AS max_price
FROM ticks_010
GROUP BY tenant, symbol
ORDER BY tenant, symbol
FORMAT TSVWithNames;
