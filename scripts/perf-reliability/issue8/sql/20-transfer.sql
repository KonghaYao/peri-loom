-- Separate statements on the SAME session, in this order.
-- Only COMMIT ACK is counted as a durable transaction ACK.
-- source_id != target_id; both must already exist; amount is a positive integer.
BEGIN IMMEDIATE;
UPDATE accounts SET balance = balance - :amount WHERE id = :source_id;
UPDATE accounts SET balance = balance + :amount WHERE id = :target_id;
INSERT INTO transfer_receipts(op_id, source_id, target_id, amount)
VALUES (:op_id, :source_id, :target_id, :amount);
COMMIT;
