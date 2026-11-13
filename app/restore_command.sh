#!/bin/bash
# restore_command：由恢复实例的 postmaster 调用
#   $1 = %f（WAL 段文件名）  $2 = %p（目标路径）  $3 = 可选：模拟缺失的段名（故障注入）
set -u
f="$1"
dest="$2"
missing="${3:-}"
if [ -n "$missing" ] && [ "$f" = "$missing" ]; then
  echo "restore_command: 段 $f 在归档中缺失（模拟归档缺段）" >&2
  exit 1
fi
if [ ! -f "/wal_archive/$f" ]; then
  echo "restore_command: 段 $f 不存在于 /wal_archive" >&2
  exit 1
fi
cp "/wal_archive/$f" "$dest"
