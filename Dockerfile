FROM golang:1.16.5 AS build-env
ADD . /mini-redis
WORKDIR /mini-redis
RUN make build-linux

FROM alpine
RUN apk add -U tzdata
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai  /etc/localtime
RUN mkdir -p /mini-redis
COPY --from=build-env /mini-redis/mini-redis /mini-redis
EXPOSE 6379
CMD [ "/mini-redis/mini-redis" ]
