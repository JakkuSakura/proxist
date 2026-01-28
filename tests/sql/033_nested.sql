DROP TABLE IF EXISTS ticks_033;
CREATE TABLE ticks_033
(
    id UInt64,
    items Nested(name String, qty UInt32)
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_033 (id, items.name, items.qty) VALUES
(1, ['a', 'b'], [10, 20]),
(2, ['c'], [5]);

SELECT id, arrayJoin(items.name) AS name, arrayJoin(items.qty) AS qty
FROM ticks_033
ORDER BY id, name
FORMAT TSVWithNames;
