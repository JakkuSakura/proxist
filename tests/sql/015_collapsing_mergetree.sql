DROP TABLE IF EXISTS ticks_015;
CREATE TABLE ticks_015
(
    id UInt64,
    sign Int8,
    value String
) ENGINE = CollapsingMergeTree(sign)
ORDER BY id;

INSERT INTO ticks_015 (id, sign, value) VALUES
(1, 1, 'open'),
(1, -1, 'open'),
(2, 1, 'keep');

SELECT id, sum(sign) AS balance
FROM ticks_015
GROUP BY id
ORDER BY id
FORMAT TSVWithNames;
