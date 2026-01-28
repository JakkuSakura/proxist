DROP TABLE IF EXISTS ticks_024_a;
DROP TABLE IF EXISTS ticks_024_b;

CREATE TABLE ticks_024_a
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE ticks_024_b
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_024_a (id, value) VALUES
(1, 'a1'),
(2, 'a2');

INSERT INTO ticks_024_b (id, value) VALUES
(3, 'b3'),
(4, 'b4');

SELECT id, value
FROM ticks_024_a
UNION ALL
SELECT id, value
FROM ticks_024_b
ORDER BY id
FORMAT TSVWithNames;
