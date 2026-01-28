DROP TABLE IF EXISTS ticks_018;
DROP VIEW IF EXISTS view_018;

CREATE TABLE ticks_018
(
    id UInt64,
    category String,
    amount UInt32
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_018 (id, category, amount) VALUES
(1, 'A', 10),
(2, 'A', 5),
(3, 'B', 7);

CREATE VIEW view_018 AS
SELECT category, sum(amount) AS total
FROM ticks_018
GROUP BY category;

SELECT category, total
FROM view_018
ORDER BY category
FORMAT TSVWithNames;
