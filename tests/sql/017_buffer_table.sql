DROP TABLE IF EXISTS ticks_017;
DROP TABLE IF EXISTS ticks_017_buffer;

CREATE TABLE ticks_017
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE ticks_017_buffer
AS ticks_017
ENGINE = Buffer(currentDatabase(), 'ticks_017', 16, 10, 100, 10000, 100000, 1000000, 10000000);

INSERT INTO ticks_017_buffer (id, value) VALUES
(1, 'alpha'),
(2, 'beta');

SELECT id, value
FROM ticks_017_buffer
ORDER BY id
FORMAT TSVWithNames;
