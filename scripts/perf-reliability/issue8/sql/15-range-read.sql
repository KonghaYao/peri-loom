SELECT id, revision, value, payload FROM kv
WHERE tenant_id = :tenant_id AND id > :after_id
ORDER BY id LIMIT 100;
