-- Run outside latency measurement. The driver compares FULL returned content
-- with its reference model. Aggregate equality alone is not a correctness proof.
SELECT id, payload FROM bench ORDER BY id;
SELECT id, tenant_id, revision, value, payload FROM kv ORDER BY id;
SELECT id, tenant_id, revision, payload FROM events ORDER BY id;
SELECT id, balance FROM accounts ORDER BY id;
SELECT op_id, source_id, target_id, amount FROM transfer_receipts ORDER BY op_id;
