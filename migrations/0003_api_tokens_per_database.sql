-- Keep one active API token per database. Preserve historical rows and their hashes.
WITH ranked AS (
    SELECT id,
           row_number() OVER (PARTITION BY database_id ORDER BY created_at DESC, id DESC) AS rank
    FROM api_tokens
    WHERE database_id IS NOT NULL AND revoked_at IS NULL
)
UPDATE api_tokens AS token
SET revoked_at = now()
FROM ranked
WHERE token.id = ranked.id AND ranked.rank > 1;

CREATE UNIQUE INDEX api_tokens_one_active_per_database_idx
    ON api_tokens (database_id)
    WHERE database_id IS NOT NULL AND revoked_at IS NULL;
