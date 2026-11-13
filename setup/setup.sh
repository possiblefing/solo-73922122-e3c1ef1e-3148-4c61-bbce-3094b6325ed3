#!/usr/bin/env bash
# =============================================================================
# 演练数据与备份初始化（幂等）：
#   1) 重建遥测表并写入基线批次（120 行，备份前已入仓）
#   2) pg_basebackup 物理基线备份 -> /basebackup/base
#   3) 批次1（48 行）-> 命名恢复点 rp_before_delete
#   4) 模拟误删：DELETE WX-03 / temperature（7 行）
#   5) 批次2（24 行，误删后新入仓）-> 命名恢复点 rp_after_delete
#   6) 切换 WAL 并等待归档完备，写出 /basebackup/manifest.json
# =============================================================================
set -euo pipefail

PGHOST=primary
PGUSER=postgres
PGDATABASE=telemetry
export PGPASSWORD=postgres
PSQL=(psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -X -q -A -t)

if [[ -f /basebackup/manifest.json && -d /basebackup/base ]]; then
  echo "[setup] 已存在基线备份与清单，跳过（幂等）。如需重置演练：docker compose down -v && docker compose up --build"
  exit 0
fi

echo "[setup] 等待 primary 就绪 ..."
until pg_isready -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do sleep 1; done

echo "[setup] 重建 telemetry_events 并写入基线遥测（6 设备 × 4 指标 × 5 时点 = 120 行）..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -X -q <<'SQL'
DROP TABLE IF EXISTS telemetry_events;
CREATE TABLE telemetry_events (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  device_id   text NOT NULL,
  site        text NOT NULL,
  metric      text NOT NULL,
  value       double precision NOT NULL,
  recorded_at timestamptz NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now()
);

-- 基线批次：备份前已入仓的历史遥测
INSERT INTO telemetry_events (device_id, site, metric, value, recorded_at)
SELECT d.device_id, d.site, m.metric,
       CASE m.metric
         WHEN 'temperature' THEN (12 + 8*sin(g*1.7 + ascii(right(d.device_id,1))))::numeric(10,2)
         WHEN 'humidity'    THEN (55 + 20*cos(g*1.3 + ascii(right(d.device_id,1))))::numeric(10,2)
         WHEN 'wind_speed'  THEN (3 + 2.5*sin(g*2.1 + ascii(right(d.device_id,1))))::numeric(10,2)
         ELSE                    (98 - g*1.5)::numeric(10,2)
       END::float8,
       timestamptz '2026-11-10 00:00:00+08' + make_interval(hours => g*6)
FROM (VALUES ('WX-01','青山站'),('WX-02','白鹭站'),('WX-03','临湖站'),
             ('WX-04','风口站'),('WX-05','城北站'),('WX-06','湿地站')) AS d(device_id, site)
CROSS JOIN (VALUES ('temperature'),('humidity'),('wind_speed'),('battery')) AS m(metric)
CROSS JOIN generate_series(0,4) AS g;
CHECKPOINT;
SQL

BASELINE=$("${PSQL[@]}" -c "SELECT count(*) FROM telemetry_events")
echo "[setup] 基线行数: $BASELINE"

echo "[setup] 执行物理基线备份 pg_basebackup -> /basebackup/base ..."
rm -rf /basebackup/base
PGPASSWORD=replicator pg_basebackup -h "$PGHOST" -U replicator -D /basebackup/base -Fp -Xs -c fast
FIRST_WAL=$("${PSQL[@]}" -c "SELECT pg_current_wal_file_name()")
echo "[setup] 基线备份完成；备份后首个 WAL 段: $FIRST_WAL（故障注入演示将“丢失”该段）"

echo "[setup] 写入批次1（48 行，备份后新入仓）..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -X -q <<'SQL'
INSERT INTO telemetry_events (device_id, site, metric, value, recorded_at)
SELECT d.device_id, d.site, m.metric,
       CASE m.metric
         WHEN 'temperature' THEN (14 + 7*sin(g*2.3 + ascii(right(d.device_id,1))))::numeric(10,2)
         WHEN 'humidity'    THEN (58 + 18*cos(g*1.9 + ascii(right(d.device_id,1))))::numeric(10,2)
         WHEN 'wind_speed'  THEN (4 + 2.0*sin(g*1.1 + ascii(right(d.device_id,1))))::numeric(10,2)
         ELSE                    (96 - g*1.2)::numeric(10,2)
       END::float8,
       timestamptz '2026-11-12 08:00:00+08' + make_interval(hours => g*3)
FROM (VALUES ('WX-01','青山站'),('WX-02','白鹭站'),('WX-03','临湖站'),
             ('WX-04','风口站'),('WX-05','城北站'),('WX-06','湿地站')) AS d(device_id, site)
CROSS JOIN (VALUES ('temperature'),('humidity'),('wind_speed'),('battery')) AS m(metric)
CROSS JOIN generate_series(0,1) AS g;
SQL

RP1_LSN=$("${PSQL[@]}" -c "SELECT pg_create_restore_point('rp_before_delete')")
RP1_TOTAL=$("${PSQL[@]}" -c "SELECT count(*) FROM telemetry_events")
RP1_DS=$("${PSQL[@]}" -c "SELECT count(*) FROM telemetry_events WHERE device_id='WX-03' AND metric='temperature'")
RP1_MAX=$("${PSQL[@]}" -c "SELECT max(recorded_at) FROM telemetry_events")
RP1_AT=$("${PSQL[@]}" -c "SELECT now()")
echo "[setup] 命名恢复点 rp_before_delete @ $RP1_LSN（总行数 $RP1_TOTAL，WX-03/temperature $RP1_DS 行）"

echo "[setup] 模拟误删：DELETE FROM telemetry_events WHERE device_id='WX-03' AND metric='temperature' ..."
DELETED=$("${PSQL[@]}" -c "WITH d AS (DELETE FROM telemetry_events WHERE device_id='WX-03' AND metric='temperature' RETURNING 1) SELECT count(*) FROM d")
echo "[setup] 误删 $DELETED 行（临湖站 WX-03 温度遥测）"

echo "[setup] 误删后原库继续入仓：批次2（24 行）..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -X -q <<'SQL'
INSERT INTO telemetry_events (device_id, site, metric, value, recorded_at)
SELECT d.device_id, d.site, m.metric,
       CASE m.metric
         WHEN 'temperature' THEN (15 + 6*sin(ascii(right(d.device_id,1))))::numeric(10,2)
         WHEN 'humidity'    THEN (60 + 15*cos(ascii(right(d.device_id,1))))::numeric(10,2)
         WHEN 'wind_speed'  THEN (5 + 1.5*sin(ascii(right(d.device_id,1))))::numeric(10,2)
         ELSE                    (95)::numeric(10,2)
       END::float8,
       timestamptz '2026-11-13 09:00:00+08'
FROM (VALUES ('WX-01','青山站'),('WX-02','白鹭站'),('WX-03','临湖站'),
             ('WX-04','风口站'),('WX-05','城北站'),('WX-06','湿地站')) AS d(device_id, site)
CROSS JOIN (VALUES ('temperature'),('humidity'),('wind_speed'),('battery')) AS m(metric);
SQL

RP2_LSN=$("${PSQL[@]}" -c "SELECT pg_create_restore_point('rp_after_delete')")
RP2_TOTAL=$("${PSQL[@]}" -c "SELECT count(*) FROM telemetry_events")
RP2_DS=$("${PSQL[@]}" -c "SELECT count(*) FROM telemetry_events WHERE device_id='WX-03' AND metric='temperature'")
RP2_MAX=$("${PSQL[@]}" -c "SELECT max(recorded_at) FROM telemetry_events")
RP2_AT=$("${PSQL[@]}" -c "SELECT now()")
echo "[setup] 命名恢复点 rp_after_delete @ $RP2_LSN（总行数 $RP2_TOTAL，WX-03/temperature $RP2_DS 行）"

# 切换 WAL，确保两个恢复点记录都进入已归档段，然后等待归档器追平
TARGET_WAL=$("${PSQL[@]}" -c "SELECT pg_current_wal_file_name()")
"${PSQL[@]}" -c "SELECT pg_switch_wal()" >/dev/null
echo "[setup] 等待 WAL 归档覆盖到 $TARGET_WAL ..."
ok=0
for _ in $(seq 1 90); do
  LAST=$("${PSQL[@]}" -c "SELECT COALESCE(last_archived_wal,'') FROM pg_stat_archiver")
  FAILED_CNT=$("${PSQL[@]}" -c "SELECT failed_count FROM pg_stat_archiver")
  if [[ "$FAILED_CNT" != "0" ]]; then
    echo "[setup] 归档器出现失败（failed_count=$FAILED_CNT），请检查 archive_command" >&2
    exit 1
  fi
  if [[ -n "$LAST" && ! "$LAST" < "$TARGET_WAL" ]]; then ok=1; break; fi
  sleep 2
done
[[ "$ok" == "1" ]] || { echo "[setup] 等待归档超时" >&2; exit 1; }
echo "[setup] WAL 归档完备（last_archived_wal=$LAST）"

NOW=$("${PSQL[@]}" -c "SELECT now()")
cat > /basebackup/manifest.json <<EOF
{
  "created_at": "$NOW",
  "db": "telemetry",
  "table": "public.telemetry_events",
  "baseline_rows": $BASELINE,
  "batch1_rows": 48,
  "deleted_rows": $DELETED,
  "batch2_rows": 24,
  "deleted_set": {
    "device_id": "WX-03",
    "metric": "temperature",
    "description": "被误删的临湖站（WX-03）温度遥测"
  },
  "first_wal_after_backup": "$FIRST_WAL",
  "restore_points": [
    {
      "name": "rp_before_delete",
      "lsn": "$RP1_LSN",
      "created_at": "$RP1_AT",
      "expected_total": $RP1_TOTAL,
      "deleted_set_rows": $RP1_DS,
      "max_recorded_at": "$RP1_MAX",
      "note": "误删前：包含全部 WX-03 温度遥测，不含误删后入仓的批次2"
    },
    {
      "name": "rp_after_delete",
      "lsn": "$RP2_LSN",
      "created_at": "$RP2_AT",
      "expected_total": $RP2_TOTAL,
      "deleted_set_rows": $RP2_DS,
      "max_recorded_at": "$RP2_MAX",
      "note": "误删后：WX-03 温度遥测已缺失，包含误删后新入仓的批次2（24 行）"
    }
  ]
}
EOF
echo "[setup] 清单已写入 /basebackup/manifest.json"
echo "[setup] 完成。两个恢复点差异：rp_before_delete=$RP1_TOTAL 行(含误删集 $RP1_DS) / rp_after_delete=$RP2_TOTAL 行(误删集 $RP2_DS)"
