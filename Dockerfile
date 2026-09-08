FROM golang:1.21-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /pulsekv ./cmd/pulsekv

FROM alpine:3.20
RUN addgroup -S pulsekv && adduser -S -G pulsekv pulsekv && mkdir -p /data && chown pulsekv:pulsekv /data
USER pulsekv
WORKDIR /data
COPY --from=build /pulsekv /usr/local/bin/pulsekv
EXPOSE 6379
ENTRYPOINT ["/usr/local/bin/pulsekv"]
CMD ["-port", "6379", "-dir", "/data", "-dbfilename", "dump.rdb", "-appendonly", "yes"]
