DROP TABLE IF EXISTS ticks_031;
CREATE TABLE ticks_031
(
    id UInt64,
    amount Decimal(18, 4)
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_031 (id, amount) VALUES
(1, 12.3456),
(2, 98.7654);

SELECT id, amount, round(amount, 2) AS rounded
FROM ticks_031
ORDER BY id
FORMAT TSVWithNames;
