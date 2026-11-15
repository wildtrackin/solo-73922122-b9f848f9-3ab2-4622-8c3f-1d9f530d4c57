FROM nginx:1.27-alpine

# json 日志目录放到独立卷，和采集器共享
RUN mkdir -p /var/log/nginx/json

COPY default.conf /etc/nginx/conf.d/default.conf
COPY rotate-nginx-log.sh /usr/local/bin/rotate-nginx-log.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/rotate-nginx-log.sh /usr/local/bin/entrypoint.sh

# 默认 error/access 日志仍走标准输出，不干扰 JSON 目录
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
