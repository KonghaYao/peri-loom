ALTER TABLE api_tokens ADD COLUMN database_id TEXT REFERENCES databases(id);

UPDATE api_tokens
SET database_id = json_extract(record, '$.database_id')
WHERE json_type(record, '$.database_id') = 'text';

WITH ranked AS (
    SELECT id,
           row_number() OVER (PARTITION BY database_id ORDER BY created_at DESC, id DESC) AS rank
    FROM api_tokens
    WHERE database_id IS NOT NULL AND revoked_at IS NULL
)
UPDATE api_tokens
SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    record = json_set(
        record,
        '$.revoked_at',
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
WHERE id IN (SELECT id FROM ranked WHERE rank > 1);

CREATE UNIQUE INDEX api_tokens_one_active_per_database_idx
    ON api_tokens(database_id)
    WHERE database_id IS NOT NULL AND revoked_at IS NULL;
