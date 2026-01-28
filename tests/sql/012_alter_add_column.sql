DROP TABLE IF EXISTS ticks_012;
CREATE TABLE ticks_012
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_012 (id, value) VALUES
(1, 'one'),
(2, 'two');

ALTER TABLE ticks_012
ADD COLUMN extra String DEFAULT 'n/a';

SELECT id, value, extra
FROM ticks_012
ORDER BY id
FORMAT TSVWithNames;
