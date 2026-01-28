DROP TABLE IF EXISTS ticks_014;
CREATE TABLE ticks_014
(
    id UInt64,
    value String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY id;

INSERT INTO ticks_014 (id, value, version) VALUES
(1, 'v1', 1),
(1, 'v2', 2),
(2, 'a', 1);

SELECT id, argMax(value, version) AS latest_value, max(version) AS latest_version
FROM ticks_014
GROUP BY id
ORDER BY id
FORMAT TSVWithNames;
