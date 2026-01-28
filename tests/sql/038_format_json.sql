DROP TABLE IF EXISTS ticks_038;
CREATE TABLE ticks_038
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_038 (id, value) VALUES
(1, 'alpha'),
(2, 'beta');

SELECT id, value
FROM ticks_038
ORDER BY id
FORMAT JSONEachRow;
