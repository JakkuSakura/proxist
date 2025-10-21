INSERT INTO ticks (tenant, shard_id, symbol, ts, seq) VALUES
('alpha', 'alpha-shard', 'AAPL', toDateTime64('2024-01-01 10:00:00', 6), 1),
('alpha', 'alpha-shard', 'MSFT', toDateTime64('2024-01-01 10:00:01', 6), 2);

SELECT tenant, symbol, count() AS rows
FROM ticks
GROUP BY tenant, symbol
ORDER BY tenant, symbol
FORMAT TSVWithNames;
