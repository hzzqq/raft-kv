# deploy/Dockerfile —— raft-kv 全部可执行文件的单镜像构建（I4 真部署化）。
#
# 一个镜像内含三个二进制，compose 里靠 command 区分角色：
#   kvnode   单节点进程（-config cluster.json -name m0|g0-0 -http :9100）
#   gateway  HTTP 网关（-connect cluster.json -addr :8080，纯客户端、不持节点）
#   kvcli    压测/读写客户端（bench，用于给看板打流量）
#
# 构建（在仓库根目录执行，上下文必须是仓库根，因为要 COPY src/）：
#   docker build -f deploy/Dockerfile -t raftkv:local .
#
# 说明：本仓库零第三方依赖（go.mod 无 require），故不需要 go.sum、也无需 go mod download。
# CGO_ENABLED=0 静态链接，运行镜像可以只带 busybox。

FROM golang:1.22-alpine AS build
WORKDIR /src
# 先拷 go.mod 让依赖层可缓存（即使目前无外部依赖，保持惯例便于将来加依赖）
COPY go.mod ./
COPY src ./src
ENV CGO_ENABLED=0 GOFLAGS=-mod=mod
RUN go build -trimpath -ldflags="-s -w" -o /out/kvnode  ./src/kvnode  && \
    go build -trimpath -ldflags="-s -w" -o /out/gateway ./src/gateway && \
    go build -trimpath -ldflags="-s -w" -o /out/kvcli   ./src/kvcli

FROM alpine:3.20
# wget 来自 busybox，用于 compose healthcheck 探 /healthz；tzdata 让日志时间可读
RUN apk add --no-cache tzdata && adduser -D -u 10001 raftkv && mkdir -p /data && chown raftkv /data
COPY --from=build /out/kvnode /out/gateway /out/kvcli /usr/local/bin/
# 配置由 compose 以只读卷挂到 /etc/raftkv/cluster.json
VOLUME ["/data"]
USER raftkv
WORKDIR /
# 默认给出用法而不是静默退出，误起容器时日志里能直接看到怎么用
CMD ["kvnode"]
