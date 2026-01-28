DROP TABLE IF EXISTS ticks_034;
CREATE TABLE ticks_034
(
    id UInt64,
    value String,
    updated UInt8 DEFAULT 0
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_034 (id, value) VALUES
(1, 'alpha'),
(2, 'beta'),
(3, 'gamma');

ALTER TABLE ticks_034 UPDATE updated = 1 WHERE id = 2;
ALTER TABLE ticks_034 DELETE WHERE id = 3;

SELECT id, value, updated
FROM ticks_034
ORDER BY id
FORMAT TSVWithNames;
