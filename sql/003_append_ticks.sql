INSERT INTO ticks (tenant, shard_id, symbol, ts, seq) VALUES
('alpha', 'alpha-shard', 'AAPL', toDateTime64('2024-01-01 10:00:02', 6), 3),
('beta', 'beta-shard', 'GOOG', toDateTime64('2024-01-01 10:00:03', 6), 1);

SELECT tenant, count() AS rows
FROM ticks
GROUP BY tenant
ORDER BY tenant
FORMAT TSVWithNames;
