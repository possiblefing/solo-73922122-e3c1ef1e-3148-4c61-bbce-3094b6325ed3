#!/bin/bash
# restore_command：由恢复实例的 postmaster 调用
#   $1 = %f（WAL 段文件名）
#   $2 = %p（目标路径）
#   $3 = 可选：模拟缺失的段名（故障注入，"-" 表示无）
#   $4 = 可选：每段重放延迟毫秒（仅用于“重放中关闭”演示，0 表示不延迟）
set -u
f="$1"
dest="$2"
missing="${3:-}"
slew="${4:-0}"

# 演示：拉长重放时间，给关闭演示留出可控窗口；不影响正常演练（默认 0）
if [ -n "$slew" ] && [ "$slew" != "0" ]; then
  sleep "$(awk -v ms="$slew" 'BEGIN{ printf "%.3f", ms/1000 }')"
fi

if [ -n "$missing" ] && [ "$missing" != "-" ] && [ "$f" = "$missing" ]; then
  echo "restore_command: 段 $f 在归档中缺失（模拟归档缺段）" >&2
  exit 1
fi
if [ ! -f "/wal_archive/$f" ]; then
  echo "restore_command: 段 $f 不存在于 /wal_archive" >&2
  exit 1
fi
cp "/wal_archive/$f" "$dest"
