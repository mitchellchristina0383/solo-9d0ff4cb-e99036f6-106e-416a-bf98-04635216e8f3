"""对 v_lock_waits 视图与 inventory 表的只读查询封装。"""


WAIT_SQL = """
SELECT
    blocked_pid,
    blocked_app,
    blocked_state,
    blocked_query,
    blocking_pid,
    blocking_app,
    blocking_state,
    blocking_query,
    lock_type,
    lock_mode,
    COALESCE(relation_name, ''),
    COALESCE(waited_location, ''),
    COALESCE(EXTRACT(EPOCH FROM wait_duration), 0)
FROM v_lock_waits
ORDER BY blocked_app, lock_type
"""


def fetch_lock_waits(conn) -> list[dict]:
    """返回当前数据库里真实存在的等待边（空列表 = 当前无人等待）。"""
    with conn.cursor() as cur:
        cur.execute(WAIT_SQL)
        rows = cur.fetchall()
    edges = []
    for r in rows:
        edges.append({
            "blocked_pid": r[0],
            "blocked_app": r[1],
            "blocked_state": r[2],
            "blocked_query": r[3],
            "blocking_pid": r[4],
            "blocking_app": r[5],
            "blocking_state": r[6],
            "blocking_query": r[7],
            "lock_type": r[8],
            "lock_mode": r[9],
            "relation": r[10],
            "waited_location": r[11],
            "wait_seconds": round(float(r[12]), 3),
            "location_source": "pg_locks",
        })
    return edges


def fetch_inventory(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT location_id, product_code, qty_on_hand, updated_at "
            "FROM inventory ORDER BY location_id"
        )
        rows = cur.fetchall()
    return [
        {
            "location_id": r[0],
            "product_code": r[1],
            "qty_on_hand": r[2],
            "updated_at": r[3].isoformat() if r[3] else None,
        }
        for r in rows
    ]
