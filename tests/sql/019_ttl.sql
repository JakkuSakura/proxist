DROP TABLE IF EXISTS ticks_019;
CREATE TABLE ticks_019
(
    id UInt64,
    ts DateTime,
    value String
) ENGINE = MergeTree
ORDER BY (id, ts)
TTL ts + INTERVAL 30 DAY;

INSERT INTO ticks_019 (id, ts, value) VALUES
(1, now() - INTERVAL 10 DAY, 'fresh'),
(2, now() - INTERVAL 40 DAY, 'old');

SELECT count() AS rows
FROM ticks_019
FORMAT TSVWithNames;
