#!/bin/sh
# nginx 容器入口：启动 nginx，并可选地在后台周期性轮转日志。
# AUTO_ROTATE_INTERVAL 留空（默认）= 只手动轮转；
# 设为秒数（如 15）= 每隔该秒自动轮转一次，用于演示“轮转与采集交错”。
set -eu

mkdir -p /var/log/nginx/json

if [ -n "${AUTO_ROTATE_INTERVAL:-}" ]; then
    (
        # 先等 nginx 起来并产生首批日志
        sleep "$AUTO_ROTATE_INTERVAL"
        while true; do
            /usr/local/bin/rotate-nginx-log.sh || true
            sleep "$AUTO_ROTATE_INTERVAL"
        done
    ) &
    echo "auto-rotation enabled every ${AUTO_ROTATE_INTERVAL}s"
fi

# 官方镜像的默认入口会 exec nginx
exec /docker-entrypoint.sh nginx -g 'daemon off;'
