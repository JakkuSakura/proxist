INSERT INTO ticks (tenant, symbol, ts, seq) VALUES
('alpha', 'AAPL', toDateTime64('2024-01-01 10:00:02', 6), 3),
('beta', 'GOOG', toDateTime64('2024-01-01 10:00:03', 6), 1);

SELECT tenant, count() AS rows
FROM ticks
GROUP BY tenant
ORDER BY tenant
FORMAT TSVWithNames;
