FROM golang:1.12-alpine
RUN echo "extra"
RUN apk update
RUN echo "extra"
RUN apk add git
RUN apk add tzdata
RUN cp /usr/share/zoneinfo/America/Denver /etc/localtime
ADD root /var/spool/cron/crontabs/root
RUN mkdir -p /go/src/rabbitmq-scanner
ADD rabbitmq-scanner.go  /go/src/rabbitmq-scanner/rabbitmq-scanner.go
ADD build.sh /build.sh
RUN chmod +x /build.sh
RUN /build.sh
CMD ["crond", "-f"]




