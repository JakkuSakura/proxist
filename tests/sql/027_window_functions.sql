DROP TABLE IF EXISTS ticks_027;
CREATE TABLE ticks_027
(
    grp String,
    ts DateTime64(6),
    value UInt32
) ENGINE = MergeTree
ORDER BY (grp, ts);

INSERT INTO ticks_027 (grp, ts, value) VALUES
('A', toDateTime64('2024-01-01 10:00:00', 6), 10),
('A', toDateTime64('2024-01-01 10:00:01', 6), 20),
('B', toDateTime64('2024-01-01 10:00:00', 6), 7);

SELECT grp, ts, value,
       sum(value) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
FROM ticks_027
ORDER BY grp, ts
FORMAT TSVWithNames;
