-- 0002_owner_epoch_monotonic.sql
--
-- owner_epoch 只增不减（架构 §11.3）。
--
-- 背景：Remote WAL 是 owner epoch 的**权威**，它对同一 DB 的 SetOwnerEpoch / Append
-- 只接受**严格递增**的 epoch。Catalog 一旦把某个 DB 的 owner_epoch 改到低位
-- （手工 SQL、从备份恢复、开发期清理数据），启动路径会稳定被 Storage-level Fencing
-- 拒绝（"owner epoch 必须严格递增：已记录 N，请求 M"），而且重试永远不会成功 ——
-- 该 DB 从此卡在 STARTING，只能人工干预。
--
-- 因此把「只增不减」下沉到 **schema 层**：这不是某一条代码路径的约定，而是
-- databases 表的不变量。任何 UPDATE（含绕过 Catalog API 的裸 SQL）只要降低
-- owner_epoch 就会被拒绝。
--
-- 唯一的例外是**显式的重置**：`Catalog::reset_owner_epoch_with_reason`
-- （crates/catalog/src/databases.rs）在同一事务里先设置下面两个会话级 GUC，
-- 触发器据此放行，并且仍然要求
--   1) 带上非空理由（审计），且
--   2) 目标 epoch 不低于存储层已记录的 epoch（`dbplatform.owner_epoch_floor`），
--      否则重置只会把 DB 变成永久起不来的状态。
-- GUC 用 `set_config(..., is_local => true)` 设置，事务结束即失效，不会泄漏到
-- 连接池里的下一条语句。

CREATE OR REPLACE FUNCTION databases_owner_epoch_monotonic()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    reset_reason text := current_setting('dbplatform.owner_epoch_reset_reason', true);
    floor_text   text := current_setting('dbplatform.owner_epoch_floor', true);
    floor_value  bigint;
BEGIN
    -- 只增不减：不小于旧值直接放行（最热路径，先判它）
    IF NEW.owner_epoch >= OLD.owner_epoch THEN
        RETURN NEW;
    END IF;

    IF reset_reason IS NULL OR btrim(reset_reason) = '' THEN
        RAISE EXCEPTION
            'owner_epoch 只增不减（架构 §11.3）：database % 的 owner_epoch 不能从 % 降到 %；如确需重置，请走 Catalog::reset_owner_epoch_with_reason 并给出理由',
            OLD.id, OLD.owner_epoch, NEW.owner_epoch
            USING ERRCODE = '23514';
    END IF;

    BEGIN
        floor_value := floor_text::bigint;
    EXCEPTION
        WHEN invalid_text_representation THEN
            floor_value := NULL;
        WHEN numeric_value_out_of_range THEN
            floor_value := NULL;
    END;

    IF floor_value IS NULL OR NEW.owner_epoch < floor_value THEN
        RAISE EXCEPTION
            'owner_epoch 重置被拒绝：目标 % 低于存储层已记录值 %（Remote WAL 是 epoch 权威，低于它会被 Storage-level Fencing 永久拒绝）',
            NEW.owner_epoch, COALESCE(floor_text, '<未提供>')
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

-- OF owner_epoch：只在本列被 UPDATE 时触发，不给普通状态/租约更新增加开销。
CREATE TRIGGER databases_owner_epoch_monotonic
    BEFORE UPDATE OF owner_epoch ON databases
    FOR EACH ROW
    EXECUTE FUNCTION databases_owner_epoch_monotonic();
