#!/usr/bin/env bash
# 触发一次 nginx 日志轮转（rename 为唯一归档名 + 重开同名日志）。
# 用法: ./scripts/rotate.sh [自定义归档后缀]
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -ge 1 ]; then
  exec docker compose exec -e ROTATE_NAME="$1" nginx \
       /usr/local/bin/rotate-nginx-log.sh
fi
exec docker compose exec nginx /usr/local/bin/rotate-nginx-log.sh
