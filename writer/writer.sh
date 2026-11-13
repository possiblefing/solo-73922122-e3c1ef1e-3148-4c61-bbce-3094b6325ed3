#!/usr/bin/env bash
# 持续写入器：模拟原库在恢复演练期间继续入仓（恢复点之后的新遥测）
set -u
export PGPASSWORD=postgres
until pg_isready -h primary -U postgres -d telemetry >/dev/null 2>&1; do sleep 1; done
echo "[writer] 开始持续入仓（每 2 秒 1 行心跳遥测，device_id=WX-LIVE）"
while true; do
  psql -h primary -U postgres -d telemetry -X -q -c \
    "INSERT INTO telemetry_events(device_id, site, metric, value, recorded_at)
     VALUES ('WX-LIVE','在途入仓站','heartbeat', round(random()::numeric,4), now());" \
    || echo "[writer] 写入失败，2s 后重试"
  sleep 2
done
