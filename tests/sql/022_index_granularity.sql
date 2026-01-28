DROP TABLE IF EXISTS ticks_022;
CREATE TABLE ticks_022
(
    id UInt64,
    ts DateTime,
    value String
) ENGINE = MergeTree
ORDER BY (id, ts)
SETTINGS index_granularity = 1024;

INSERT INTO ticks_022 (id, ts, value) VALUES
(1, now(), 'a'),
(2, now(), 'b');

SELECT count() AS rows
FROM ticks_022
FORMAT TSVWithNames;
