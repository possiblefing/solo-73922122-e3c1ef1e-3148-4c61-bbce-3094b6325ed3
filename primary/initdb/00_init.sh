#!/bin/bash
# 原库初始化：创建物理复制账号并放行 replication 连接（供 pg_basebackup 使用）
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD 'replicator';
SQL
echo "host replication replicator 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
