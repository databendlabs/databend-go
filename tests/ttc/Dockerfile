FROM golang:latest AS builder

WORKDIR /usr/src

COPY . .

RUN CGO_ENABLED=0 go build -tags rows_hack ./tests/ttc

FROM debian:bullseye-slim

COPY --from=builder  /usr/src/ttc /usr/local/bin/ttc-server

# docker run --net host -e TTC_PORT=9093 -e DATABEND_DSN=databend://default:@127.0.0.1:8000 ghcr.io/databendlabs/ttc-go:latest
CMD ["ttc-server"]
