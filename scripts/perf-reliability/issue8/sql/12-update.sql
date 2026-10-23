UPDATE kv
SET revision = revision + 1, value = value + :delta, payload = :payload
WHERE id = :id;
