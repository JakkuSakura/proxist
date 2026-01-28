DROP TABLE IF EXISTS ticks_032;
CREATE TABLE ticks_032
(
    id UInt64,
    ip IPv4,
    session UUID
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO ticks_032 (id, ip, session) VALUES
(1, '192.168.1.1', generateUUIDv4()),
(2, '10.0.0.1', generateUUIDv4());

SELECT id, ip
FROM ticks_032
ORDER BY id
FORMAT TSVWithNames;
