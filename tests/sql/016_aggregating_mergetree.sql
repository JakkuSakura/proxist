DROP TABLE IF EXISTS ticks_016;
CREATE TABLE ticks_016
(
    key String,
    count_state AggregateFunction(count),
    sum_state AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO ticks_016 (key, count_state, sum_state) VALUES
('alpha', countState(), sumState(10.0)),
('alpha', countState(), sumState(5.5)),
('beta', countState(), sumState(2.0));

SELECT key, countMerge(count_state) AS cnt, sumMerge(sum_state) AS total
FROM ticks_016
GROUP BY key
ORDER BY key
FORMAT TSVWithNames;
