DROP TABLE IF EXISTS ticks_035_src;
DROP TABLE IF EXISTS ticks_035_dst;

CREATE TABLE ticks_035_src
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE ticks_035_dst
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_035_src (id, value) VALUES
(1, 'a'),
(2, 'b');

INSERT INTO ticks_035_dst
SELECT id, value FROM ticks_035_src;

SELECT id, value
FROM ticks_035_dst
ORDER BY id
FORMAT TSVWithNames;
