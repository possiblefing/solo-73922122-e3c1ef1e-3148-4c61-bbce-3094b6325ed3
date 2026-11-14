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
4. **正常关闭收尾演示**：在“关闭演示停点”下拉框中选择
   **复制中 / 启动交接处 / 重放中**，启动恢复后任务会停在所选阶段（默认 60s；
   可在页面显式触发）。此时另开终端执行关闭：

   ```bash
   docker compose stop app        # SIGTERM → 应用进入关闭收尾
   docker compose logs app | grep -E '\[shutdown\]|关闭收尾|不启动数据库|不开放结果'
   ```

   预期（每种停点都必须成立）：
   - 关闭开始后 `/api/recover` 一律拒绝（不再受理恢复）；
   - “准备中”的任务**不会继续启动数据库**；与启动命令交错时**迟到创建的实例也会被停止**；
   - 恢复线程与本次启动命令全部结束、所属恢复数据库确认停止后，**才**删除副本；
   - 迟到完成的实例不得开放结果，也不得把状态写回 `READY`；
   - 全部完成后才输出 `关闭完成`；若有未结束项则明确报“未能干净收尾”并保留副本，
     由下次启动清理，**绝不伪造完成**。
5. 全程观察左上角原库面板：行数持续增长，证明原库入仓不受影响；
   恢复点之后产生的 `WX-LIVE` 心跳不会出现在恢复库。

> “重放中”停点会为每段 WAL 增加 `REPLAY_DEMO_DELAY_MS`（默认 800ms）的
> restore_command 延迟，仅让重放窗口足够触发关闭；不选该停点时无任何延迟。

### 关闭收尾的可验证日志

```text
[shutdown] 开始关闭收尾：不再受理恢复，在途任务 N 个
[recovery] 关闭已开始（复制完成），放弃启动数据库，仅做收尾
[recovery] 关闭已开始，停止重放轮询，等待恢复进程收尾
[shutdown] 恢复线程与启动命令已结束、恢复实例已确认停止；已删除副本 [...]，关闭完成
```

### 单元回归（无需 Docker）

```bash
python3 -I -m unittest discover -s app/tests -t app -v
```

覆盖：关闭后拒绝新恢复、准备中不启动数据库、启动交接处取消、迟到完成不写 READY/
不抓取结果、线程或启动命令未结束时不删除副本、干净收尾删除副本、迟到实例被发现并停止、
启动命令执行期间可被关闭流程看到、启动与关闭在同一把锁下判定、交错实例先停止再删除、复制中止。

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
app/main.py               # 恢复编排状态机（单恢复锁、目标确认、失败回收、查询门禁、关闭收尾）
app/restore_command.sh    # restore_command（支持注入“归档缺段”故障与重放延迟演示）
app/static/index.html     # 演练台页面（恢复点选择、进度与记录、回收、关闭演示停点）
app/tests/test_shutdown.py# 关闭收尾回归测试（无需 Docker/PostgreSQL）
```

## 关闭语义（为什么可以放心 stop）

| 阶段 | 关闭时的行为 |
|---|---|
| 复制中 | 分块复制立即中止；**不会**启动数据库，半成品在收尾时删除 |
| 启动交接处 | 启动前最后复核 `closing`；`start_postgres` 与关闭在同一把锁下判定，不会“关闭后冒出新实例” |
| 重放中 | 轮询立即退出；停止恢复实例并确认 postmaster 结束；迟到 promote **不开放结果、不写 READY** |

收尾顺序严格为：**拒绝新任务 → 等在途恢复线程 → 等本次 `pg_ctl start/stop` 命令结束
→ 停止并确认所有恢复数据库（含扫描发现的迟到实例）→ 才删除副本**。
任一项未完成则输出“未能干净收尾”并保留副本，由下次启动的 lifespan 清理；
绝不把“未完成”伪装成“已完成”。

## 重置

```bash
docker compose down -v && docker compose up --build -d
```

## 说明

- 恢复实例只监听 app 容器内 `127.0.0.1:5433`，不对外暴露；结果查询一律经过
  应用层 READY 门禁（失败残库不可查）。
- 演示环境使用弱口令（postgres/postgres、replicator/replicator），仅限本地演练。
- setup 幂等：已存在基线备份与清单时直接跳过；如需重新生成请先 `down -v`。
