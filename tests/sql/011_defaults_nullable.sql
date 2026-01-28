DROP TABLE IF EXISTS ticks_011;
CREATE TABLE ticks_011
(
    id UInt64,
    name String,
    note Nullable(String),
    created_at DateTime DEFAULT now(),
    qty UInt32 DEFAULT 1
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_011 (id, name, note, qty) VALUES
(1, 'alpha', NULL, 3),
(2, 'beta', 'first batch', 5);

INSERT INTO ticks_011 (id, name) VALUES
(3, 'gamma');

SELECT id, name, note, qty
FROM ticks_011
ORDER BY id
FORMAT TSVWithNames;
