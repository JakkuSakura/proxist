DROP TABLE IF EXISTS ticks_023;
CREATE TABLE ticks_023
(
    id UInt64,
    name String,
    value UInt32
) ENGINE = MergeTree
ORDER BY id
SAMPLE BY cityHash64(id);

INSERT INTO ticks_023 (id, name, value) VALUES
(1, 'one', 10),
(2, 'two', 20),
(3, 'three', 30),
(4, 'four', 40);

SELECT count() AS sampled_rows
FROM ticks_023
SAMPLE 0.5
FORMAT TSVWithNames;
