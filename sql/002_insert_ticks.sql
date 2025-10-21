INSERT INTO ticks (tenant, symbol, ts, seq) VALUES
('alpha', 'AAPL', toDateTime64('2024-01-01 10:00:00', 6), 1),
('alpha', 'MSFT', toDateTime64('2024-01-01 10:00:01', 6), 2);

SELECT tenant, symbol, count() AS rows
FROM ticks
GROUP BY tenant, symbol
ORDER BY tenant, symbol
FORMAT TSVWithNames;
