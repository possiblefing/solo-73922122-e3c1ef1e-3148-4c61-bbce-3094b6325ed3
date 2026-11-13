# 遥测恢复演练台（FastAPI + PostgreSQL 16）

数仓工程师误删遥测记录后的恢复演练环境：在**原库持续入仓不受影响**的前提下，
从**物理基线备份 + WAL 归档**真实重放，精确停在所选**命名恢复点**，拉起一份
**独立核查库**，用于核对误删前数据。

- 恢复 = 复制 `pg_basebackup` 物理基线 + `restore_command` 从 WAL 归档重放 +
  `recovery_target_name` 精确停止 + `recovery_target_action=promote`，**不是 SQL 重建样例**。
- 只有三重证据齐备才开放结果（仅凭数据库可连接不算成功）：
  1. 日志出现 `recovery stopping at restore point "<所选恢复点>"`
  2. 日志出现 `archive recovery complete`
  3. `pg_is_in_recovery() = false`（已 promote）
- 全站同时只允许一份恢复；归档缺段 / 未到达目标 → 失败，恢复进程被停止回收，
  失败残库不开放查询，必须“回收残库”后才允许重试。

## 架构

```
┌────────────┐   WAL 归档    ┌──────────────┐
│  primary   │ ───────────▶ │  wal_archive │（命名卷）
│ 遥测源(原库)│              └──────┬───────┘
│  :5432     │   pg_basebackup     │ restore_command
└─────▲──────┘ ───────────▶ ┌─────┴──────────────┐
      │ 持续写入             │  basebackup/base   │
┌─────┴──────┐              └─────┬──────────────┘
│  writer    │                    │ 复制副本 + recovery.signal
│ 心跳遥测    │              ┌─────▼──────────────┐
└────────────┘              │  app (FastAPI)     │
                            │  · 恢复编排/状态机  │
                            │  · 内嵌 PG16 恢复实例│
                            │    127.0.0.1:5433  │
                            │  · 浏览器 :8080     │
                            └────────────────────┘
```

## 快速开始

```bash
docker compose up --build -d
docker compose logs -f setup     # 等待出现 “[setup] 完成。”
# 浏览器打开 http://localhost:8080
```

可选：直接核查原库 `psql postgresql://postgres:postgres@localhost:5432/telemetry`

## 演练流程

1. **恢复到误删前**：选择 `rp_before_delete` → 启动恢复 → 观察 WAL 逐段重放 →
   READY 后核对：总行数 168、误删集（WX-03/temperature）7 行、
   最新 recorded_at `2026-11-12 11:00:00+08`。可逐行查询恢复库记录。
2. **回收**当前恢复库 → 选择 `rp_after_delete` → 核对：总行数 185、
   误删集 0 行、含误删后批次2（最新 recorded_at `2026-11-13 09:00:00+08`）。
   两个恢复点的差异即“误删事件”的影响范围。
3. **故障演示**：勾选“注入归档缺段”→ 启动恢复 → 恢复在缺失段处停下，
   postgres 报 `recovery ended before configured recovery target was reached` →
   页面显示失败，进程已回收 → 点击“回收残库并重置”后才可重试。
4. 全程观察左上角原库面板：行数持续增长，证明原库入仓不受影响；
   恢复点之后产生的 `WX-LIVE` 心跳不会出现在恢复库。

## 虚构遥测数据与两个恢复点的差异

| 批次 | 内容 | 行数 |
|---|---|---|
| 基线（备份前） | 6 设备 × 4 指标 × 5 时点 | 120 |
| 批次1（备份后、rp1 前） | 6 设备 × 4 指标 × 2 时点 | +48 |
| **误删**（rp1 后） | `DELETE WHERE device_id='WX-03' AND metric='temperature'` | −7 |
| 批次2（误删后、rp2 前） | 6 设备 × 4 指标 × 1 时点 | +24 |
| 心跳（rp2 后持续） | writer 每 2 秒 1 行 `WX-LIVE/heartbeat` | 持续增长 |

| 核对项 | rp_before_delete | rp_after_delete |
|---|---|---|
| 遥测总行数 | 168 | 185 |
| 误删集行数（WX-03/temperature） | 7 | 0 |
| 最新 recorded_at | 2026-11-12 11:00:00+08 | 2026-11-13 09:00:00+08 |

清单 `/basebackup/manifest.json` 由 setup 以实际 SQL 计数生成，页面“预期”列以此为准。

## 目录

```
docker-compose.yml        # 四服务编排：primary / setup / writer / app
primary/initdb/00_init.sh # 复制账号 + pg_hba
setup/setup.sh            # 造数、pg_basebackup、两个命名恢复点、归档完备性校验、清单
writer/writer.sh          # 持续入仓心跳
app/Dockerfile            # postgres:16 + python3 + fastapi
app/main.py               # 恢复编排状态机（单恢复锁、目标确认、失败回收、查询门禁）
app/restore_command.sh    # restore_command（支持注入“归档缺段”故障）
app/static/index.html     # 演练台页面
```

## 重置

```bash
docker compose down -v && docker compose up --build -d
```

## 说明

- 恢复实例只监听 app 容器内 `127.0.0.1:5433`，不对外暴露；结果查询一律经过
  应用层 READY 门禁（失败残库不可查）。
- 演示环境使用弱口令（postgres/postgres、replicator/replicator），仅限本地演练。
- setup 幂等：已存在基线备份与清单时直接跳过；如需重新生成请先 `down -v`。
