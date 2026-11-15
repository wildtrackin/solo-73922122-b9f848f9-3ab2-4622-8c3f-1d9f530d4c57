#!/bin/sh
# 持续请求生成器：周期性向 nginx 发请求，制造正常流量与故障请求。
# 关闭方式：docker compose stop traffic
set -eu

TARGET="${TARGET:-http://nginx:80}"
INTERVAL="${INTERVAL:-0.2}"

PATHS="/ /api/orders /api/payments /api/orders /static/app.js /healthz /flaky /fail/404 /fail/500 /fail/502 /fail/503 /missing/page"

echo "traffic generator starting -> $TARGET (interval ${INTERVAL}s)"
i=0
while true; do
    i=$((i + 1))
    for p in $PATHS; do
        # -s 静默，-o /dev/null 丢弃响应体，-w 输出状态码；失败不退出循环
        code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$TARGET$p" || echo "000")
        # 控制日志量：仅每轮打印一次摘要
        if [ "$p" = "/fail/500" ] || [ "$p" = "/fail/503" ]; then
            echo "request #$i $p -> $code"
        fi
        sleep "$INTERVAL"
    done
done
