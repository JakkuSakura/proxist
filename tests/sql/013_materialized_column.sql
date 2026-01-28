DROP TABLE IF EXISTS ticks_013;
CREATE TABLE ticks_013
(
    id UInt64,
    price Float64,
    qty UInt32,
    total Float64 MATERIALIZED price * qty
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_013 (id, price, qty) VALUES
(1, 10.5, 2),
(2, 3.25, 4);

SELECT id, price, qty, total
FROM ticks_013
ORDER BY id
FORMAT TSVWithNames;
