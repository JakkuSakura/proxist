DROP TABLE IF EXISTS ticks_039;
CREATE TABLE ticks_039
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_039 (id, value) VALUES
(1, 'alpha');

SELECT currentDatabase() AS db, version() AS ch_version
FORMAT TSVWithNames;

SELECT id, value
FROM ticks_039
ORDER BY id
FORMAT TSVWithNames;
