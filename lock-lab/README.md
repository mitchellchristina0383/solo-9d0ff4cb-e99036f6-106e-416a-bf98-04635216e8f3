# lock-lab —— 库位调拨行锁互斥实验（Flask + PostgreSQL）

复盘两笔库位调拨互相卡住的场景。两笔样例调拨在各自独立的数据库连接/事务里
对**同两条库存记录**执行 `SELECT ... FOR UPDATE`，页面按调拨单展示 PostgreSQL
中**实际观测到**的等待关系；只有数据库自己报死锁（SQLSTATE `40P01`）才判定为死锁。

## 业务场景

| 调拨单 | 方向 | 数量 |
|---|---|---|
| TR-1001（T1） | A-01 → A-02 | 10 |
| TR-1002（T2） | A-02 → A-01 | 10 |

初始两库位各 100 件。每笔事务：`BEGIN` → 按顺序对两条记录 `FOR UPDATE` 加行锁
→ 扣减/增加库存 → `COMMIT`。

页面可切换两种加锁次序：

- **相反次序（opposite）**：T1 先 A-01 后 A-02，T2 先 A-02 后 A-01
  → 互相等待，PostgreSQL 达到 `deadlock_timeout` 后检测出等待环，回滚其中一笔，另一笔提交。
- **相同次序（same）**：两笔都按 A-01 → A-02 加锁
  → 只排队、不成环，不会死锁，两笔先后提交。

## 快速启动（Docker Compose）

```bash
cd lock-lab
docker compose up --build
# 打开 http://localhost:8000
```

- `db`：`postgres:15-alpine`，首次启动自动执行 `db/init.sql` 建表建视图，
  带健康检查；`deadlock_timeout=500ms`、`log_lock_waits=on`。
- `web`：Python 3.12 + gunicorn（1 进程 8 线程，每笔调拨独占一条 psycopg2 连接）。
- 停止并清库：`docker compose down -v`。

页面上可调参数：`lock_timeout`、`deadlock_timeout`、首锁后停顿 `stagger`
（保证两笔先各持一把锁）、硬性超时秒数。

## 实现要点

### 1. 等待关系只认数据库地面真值

`db/init.sql` 中的 `v_lock_waits` 视图以 `pg_blocking_pids()` 为等待关系来源，
关联 `pg_stat_activity` 取双方应用名、语句与状态。

> 注意一个 PG15 的实际行为：行级锁等待者在 `pg_locks` 里只留下未授予的
> `transactionid / ShareLock`，**没有 tuple 条目**，因此经典的 pg_locks
> 自连接写法能看到等待边但反查不到具体库位。本项目用 `pg_blocking_pids()`
> 保证等待边不丢；库位信息优先取视图中的 tuple 反查，缺失时由应用侧
> 根据“该连接当前正在请求的库位”补注（边数据上带 `location_source` 字段区分来源）。

### 2. 并发编排与采样

- 每轮由 supervisor 线程启动 T1、T2 两个工作线程 + 一个监控线程，
  三者在首锁请求前通过 `threading.Barrier(3)` 同时起跑，保证从第一刻起就在采样。
- 监控线程每 **100ms** 查询一次 `v_lock_waits`，全部快照随轮次保留，
  页面展示每条等待边（谁等谁、等哪个库位、锁类型、等待时长、等待中的 SQL）。
- 等待环只用于页面高亮：把所有采样到的边构图后 DFS 找环；
  **死锁判定唯一依据是数据库返回的 40P01**，应用不自行推断。

### 3. 结局与超时

- 工作线程捕获异常后按 SQLSTATE 分类：`40P01` 死锁回滚 / `55P03` lock_timeout 回滚 / 其他回滚，
  成功则提交；最终每笔调拨单显示实际结局与说明。
- **lock_timeout**：会话级 `SET lock_timeout`，超时该笔回滚（55P03），页面显示为“回滚但非死锁”。
- **硬性超时**：监控线程到点设置取消信号并 `pg_terminate_backend()` 本轮两个后端，
  连接在 worker 的 `finally` 中也会 `close()`，确保本轮连接全部释放，之后可立刻重新试跑。
- **手动取消**：`POST /api/runs/<id>/cancel`，对阻塞中的后端调用 `pg_cancel_backend()`。
- 每轮开始前把库存重置为 100/100；轮次历史保留最近 50 轮，解锁/结束后仍可回看
  当轮等待环、被回滚的一笔、另一笔的提交结果和库存实况。

### HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 实验页面 |
| POST | `/api/runs` | 发起一轮（body：`lock_order` 等参数） |
| GET | `/api/runs/<id>` | 轮次详情（快照、结局、库存、等待环） |
| POST | `/api/runs/<id>/cancel` | 手动取消并释放连接 |
| GET | `/api/runs` | 历史轮次列表 |
| GET | `/api/health` | 数据库连通性 |

## 实测记录（PostgreSQL 15.20）

以下为本地真实试跑结果（每 100ms 采样）：

### 相反次序 —— 数据库判定死锁

- `db_declared_deadlock=true`，其中一笔收到 `40P01` 被数据库回滚，另一笔正常提交。
  （当轮记录里牺牲品是 T1；具体回滚哪一笔由 PostgreSQL 死锁检测器选择，多轮试跑中也观察到过 T2 被回滚，页面按实际结果展示。）
- 采样到完整等待环（连续约 1s 的双向边，与 1s 的 `deadlock_timeout` 吻合）：
  - `locklab-T1 → locklab-T2`，等待库位 **A-02**（T1 已持 A-01）
  - `locklab-T2 → locklab-T1`，等待库位 **A-01**（T2 已持 A-02）
- 库存只体现 T2 的调拨：A-01=110、A-02=90（T1 回滚不留痕迹）。

### 相同次序 —— 普通等待，无死锁

- `db_declared_deadlock=false`，两笔均 `committed`，无等待环。
- 采样到单边排队：`locklab-T1 → locklab-T2 @ A-01`，持续约 1s 后 T2 提交放行，T1 继续。
- 两笔对冲后库存回到 100/100。

### 异常路径

- **lock_timeout 路径**：`lock_timeout=800ms` 且把 `deadlock_timeout` 调到 30s，
  两笔均收到 `55P03` 回滚，页面明确显示“回滚但数据库未判定为死锁”，库存不变。
- **硬性超时**：`hard_timeout_s=5` 到点后轮次置为 `cancelled`，
  `pg_terminate_backend` 生效，`pg_stat_activity` 中 `locklab-*` 连接数为 0，
  随后可立即发起新一轮并正常完成。
- **手动取消**：取消接口返回后两笔回滚、连接数归 0。

## 目录结构

```
lock-lab/
├── docker-compose.yml        # db(healthcheck) + web
├── README.md
├── db/
│   └── init.sql              # inventory 表 + v_lock_waits 观测视图
└── app/
    ├── Dockerfile
    ├── requirements.txt
    ├── app.py                # 编排、工作线程、监控、超时、HTTP API
    ├── wait_snapshot.py      # 视图只读查询
    └── templates/index.html  # 单页前端
```

## 余项 / 已知边界

1. 交付环境未安装 Docker，实测是在本地用 micromamba 部署的 PostgreSQL 15.20 +
   原生 Flask/gunicorn 完成；`docker-compose.yml` 仅做 YAML 与结构校验，
   未在本机实际 `docker compose up`（逻辑与本地完全一致，镜像为官方 postgres/python）。
2. 等待关系采样为 100ms 周期快照，极短于 100ms 的等待窗口可能采不到；
   默认 `deadlock_timeout=1000ms` 正是为了让等待环稳定落入采样窗口。
3. 为便于复盘，全局同一时间只允许一轮实验在跑（第二个请求返回 409）；
   历史轮次保存在进程内存（最近 50 轮），重启进程后清空，未做持久化。
4. 库存表仅两条样例记录、调拨数量写死，聚焦锁等待演示，未做通用库存建模。
