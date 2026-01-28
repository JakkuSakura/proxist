DROP TABLE IF EXISTS ticks_026;
CREATE TABLE ticks_026
(
    id UInt64,
    tag String,
    value UInt32
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_026 (id, tag, value) VALUES
(1, 'keep', 10),
(2, 'drop', 20),
(3, 'keep', 30);

SELECT id, tag, value
FROM ticks_026
WHERE id IN (SELECT id FROM ticks_026 WHERE tag = 'keep')
ORDER BY id
FORMAT TSVWithNames;
