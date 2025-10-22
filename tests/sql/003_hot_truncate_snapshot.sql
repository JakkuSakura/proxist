-- Append a few more ticks through proxist to exercise the hot seam.
INSERT INTO ticks (tenant, shard_id, symbol, ts, seq) VALUES
('alpha', 'alpha-shard', 'AAPL', toDateTime64('2024-01-01 10:00:02', 6), 3),
('alpha', 'alpha-shard', 'AAPL', toDateTime64('2024-01-01 10:00:03', 6), 4),
('alpha', 'alpha-shard', 'MSFT', toDateTime64('2024-01-01 10:00:04', 6), 5),
('beta', 'beta-shard', 'GOOG', toDateTime64('2024-01-01 10:00:05', 6), 2);

-- Show the merged ticks view proxist serves back.
SELECT tenant, symbol, count() AS total_rows, min(ts_micros) AS first_ts_micros, max(ts_micros) AS last_ts_micros
FROM ticks
GROUP BY tenant, symbol
ORDER BY tenant, symbol
FORMAT TSVWithNames;
