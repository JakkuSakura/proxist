DROP TABLE IF EXISTS ticks_021;
CREATE TABLE ticks_021
(
    tenant String,
    symbol String,
    ts DateTime,
    price Float64
) ENGINE = MergeTree
PRIMARY KEY (tenant, symbol)
ORDER BY (tenant, symbol, ts);

INSERT INTO ticks_021 (tenant, symbol, ts, price) VALUES
('alpha', 'AAPL', now(), 100.0),
('alpha', 'MSFT', now(), 200.0),
('beta', 'GOOG', now(), 300.0);

SELECT tenant, count() AS rows
FROM ticks_021
GROUP BY tenant
ORDER BY tenant
FORMAT TSVWithNames;
