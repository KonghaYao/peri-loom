DELETE FROM perf_kv;
DELETE FROM perf_events;
WITH RECURSIVE seq(i) AS (
  SELECT 1
  UNION ALL
  SELECT i + 1 FROM seq WHERE i < 100000
)
INSERT INTO perf_kv(id, value, payload)
SELECT i, 0, printf('r%08d:', i) || substr(hex(zeroblob(200)), 1, 246)
FROM seq;
