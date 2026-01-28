DROP TABLE IF EXISTS ticks_037;
CREATE TABLE ticks_037
(
    id UInt64,
    tag String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_037 (id, tag) VALUES
(1, 'x'),
(2, 'x'),
(3, 'y'),
(4, 'z');

SELECT DISTINCT tag
FROM ticks_037
ORDER BY tag
LIMIT 2 OFFSET 0
FORMAT TSVWithNames;
