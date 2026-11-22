-- lock-lab 数据库初始化脚本
-- 由 postgres 官方镜像在首次启动时自动执行（挂载到 /docker-entrypoint-initdb.d）

CREATE TABLE IF NOT EXISTS inventory (
    location_id  TEXT PRIMARY KEY,
    product_code TEXT NOT NULL,
    qty_on_hand  INTEGER NOT NULL CHECK (qty_on_hand >= 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO inventory (location_id, product_code, qty_on_hand)
VALUES ('A-01', 'WIDGET-X', 100),
       ('A-02', 'WIDGET-X', 100)
ON CONFLICT (location_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 实际等待关系观测视图：一条记录 = “blocked_app 正被 blocking_app 阻塞”
--
-- 关键点：PostgreSQL 15 中，行级锁(FOR UPDATE)的等待者在 pg_locks 里只留下
-- 未授予的 transactionid/ShareLock，不会留下 tuple 条目，因此仅靠 pg_locks
-- 自连接无法稳定识别行锁等待、也反查不到具体库位。这里以数据库内置的
-- pg_blocking_pids() 作为等待关系的地面真值，再 LATERAL 补一条等待者当前
-- 未授予锁的明细（若存在 tuple 条目则反查出库位）。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_lock_waits AS
SELECT
    blocked.pid               AS blocked_pid,
    blocked.application_name  AS blocked_app,
    blocked.state             AS blocked_state,
    blocked.query             AS blocked_query,
    blocking.pid              AS blocking_pid,
    blocking.application_name AS blocking_app,
    blocking.state            AS blocking_state,
    blocking.query            AS blocking_query,
    COALESCE(bl.locktype, 'transactionid')              AS lock_type,
    COALESCE(bl.mode, 'ShareLock')                      AS lock_mode,
    CASE WHEN bl.relation IS NULL THEN ''
         ELSE bl.relation::regclass::text END          AS relation_name,
    bl.page                                             AS page_no,
    bl.tuple                                            AS tuple_no,
    -- 若未授予锁恰好带 tuple 信息（部分 PG 版本/场景会出现），直接反查库位
    CASE
        WHEN bl.locktype = 'tuple'
         AND bl.relation = 'inventory'::regclass
        THEN (
            SELECT location_id
            FROM inventory
            WHERE ctid = format('(%s,%s)', bl.page, bl.tuple)::tid
        )
        ELSE NULL
    END                                                 AS waited_location,
    now() - blocked.state_change                        AS wait_duration
FROM pg_stat_activity blocked
CROSS JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS bp(blocking_pid)
JOIN pg_stat_activity blocking
  ON blocking.pid = bp.blocking_pid
LEFT JOIN LATERAL (
    SELECT l.locktype, l.mode, l.relation, l.page, l.tuple
    FROM pg_locks l
    WHERE l.pid = blocked.pid
      AND NOT l.granted
    ORDER BY CASE WHEN l.locktype = 'tuple' THEN 0 ELSE 1 END
    LIMIT 1
) bl ON TRUE
WHERE blocked.datname = current_database();
