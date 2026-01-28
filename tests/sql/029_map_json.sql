DROP TABLE IF EXISTS ticks_029;
CREATE TABLE ticks_029
(
    id UInt64,
    attrs Map(String, String),
    payload String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_029 (id, attrs, payload) VALUES
(1, map('env', 'prod', 'region', 'us'), '{"key":"value"}'),
(2, map('env', 'stage'), '{"key":"other"}');

SELECT id, mapKeys(attrs) AS keys, JSONExtractString(payload, 'key') AS value
FROM ticks_029
ORDER BY id
FORMAT TSVWithNames;
