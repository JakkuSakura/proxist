DROP TABLE IF EXISTS ticks_020;
CREATE TABLE ticks_020
(
    id UInt64,
    ts DateTime64(6),
    value String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (id, ts);

INSERT INTO ticks_020 (id, ts, value) VALUES
(1, toDateTime64('2024-01-15 10:00:00', 6), 'jan'),
(2, toDateTime64('2024-02-01 10:00:00', 6), 'feb');

SELECT toYYYYMM(ts) AS part, count() AS rows
FROM ticks_020
GROUP BY part
ORDER BY part
FORMAT TSVWithNames;
