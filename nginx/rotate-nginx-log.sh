#!/bin/sh
# 可控日志轮转样例（容器内执行）。
#
# 语义与 logrotate 的 copytruncate 不同 —— 这里采用 nginx 官方推荐的
# “重命名 + 重开信号”模式，严格满足题目要求：
#   1. 把当前 access.log 重命名为带时间戳的唯一归档名（不截断、不删除）；
#   2. nginx 在旧 inode 上可能仍有短暂写入，旧文件继续保留并继续被采集；
#   3. 发送 USR1 让 nginx 重新打开同名 access.log（一个全新 inode）；
#   4. 所有归档永久保留在共享目录中。
#
# 用法:
#   docker compose exec nginx /usr/local/bin/rotate-nginx-log.sh
#   docker compose exec nginx ROTATE_NAME=manual-1 /usr/local/bin/rotate-nginx-log.sh
set -eu

LOG_DIR=/var/log/nginx/json
LOG_FILE="$LOG_DIR/access.log"

if [ ! -e "$LOG_FILE" ]; then
    echo "no $LOG_FILE yet — nothing to rotate" >&2
    exit 0
fi

if [ -n "${ROTATE_NAME:-}" ]; then
    SUFFIX="${ROTATE_NAME}"
else
    SUFFIX="$(date -u +%Y%m%dT%H%M%SZ)"
fi
ARCHIVE="$LOG_DIR/access.log.${SUFFIX}.log"

# 极端情况下同一秒内轮转两次：保证归档名唯一，绝不覆盖旧归档。
i=0
while [ -e "$ARCHIVE" ]; do
    i=$((i + 1))
    ARCHIVE="$LOG_DIR/access.log.${SUFFIX}.${i}.log"
done

mv "$LOG_FILE" "$ARCHIVE"
echo "rotated: $LOG_FILE -> $ARCHIVE"

# nginx 主进程重新打开日志文件；旧 worker 已在飞行中的请求可能先写进
# 旧 inode（即归档文件），USR1 之后的请求写入新建的 access.log。
nginx -s reopen
echo "nginx log reopened; archives now in $LOG_DIR:"
ls -1 "$LOG_DIR"
