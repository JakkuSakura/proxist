DROP TABLE IF EXISTS ticks_030;
CREATE TABLE ticks_030
(
    id UInt64,
    status Enum8('new' = 1, 'open' = 2, 'done' = 3),
    owner LowCardinality(String)
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_030 (id, status, owner) VALUES
(1, 'new', 'alice'),
(2, 'open', 'bob'),
(3, 'done', 'alice');

SELECT owner, count() AS rows
FROM ticks_030
GROUP BY owner
ORDER BY owner
FORMAT TSVWithNames;
