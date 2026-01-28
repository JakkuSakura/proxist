DROP TABLE IF EXISTS ticks_028;
CREATE TABLE ticks_028
(
    id UInt64,
    nums Array(Int32),
    meta Tuple(String, UInt32)
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_028 (id, nums, meta) VALUES
(1, [1, 2, 3], ('alpha', 10)),
(2, [4, 5], ('beta', 20));

SELECT id, arrayJoin(nums) AS num, tupleElement(meta, 1) AS label
FROM ticks_028
ORDER BY id, num
FORMAT TSVWithNames;
