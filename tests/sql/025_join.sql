DROP TABLE IF EXISTS ticks_025_left;
DROP TABLE IF EXISTS ticks_025_right;

CREATE TABLE ticks_025_left
(
    id UInt64,
    name String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE ticks_025_right
(
    id UInt64,
    score UInt32
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_025_left (id, name) VALUES
(1, 'alpha'),
(2, 'beta');

INSERT INTO ticks_025_right (id, score) VALUES
(1, 10),
(3, 30);

SELECT l.id, l.name, r.score
FROM ticks_025_left AS l
LEFT JOIN ticks_025_right AS r
ON l.id = r.id
ORDER BY l.id
FORMAT TSVWithNames;
