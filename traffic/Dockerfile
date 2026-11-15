FROM alpine:3.20

RUN apk add --no-cache curl bash

COPY traffic.sh /usr/local/bin/traffic.sh
RUN chmod +x /usr/local/bin/traffic.sh

CMD ["/usr/local/bin/traffic.sh"]
