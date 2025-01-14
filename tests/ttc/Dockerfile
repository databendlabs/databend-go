FROM golang:latest AS builder

WORKDIR /usr/src

COPY . .

RUN go build .

FROM debian:bullseye-slim
COPY --from=builder  /usr/src/ttc-go /usr/local/bin/ttc-server

# docker run --net host datafuselabs/ttc-go -e TTC_PORT=9093 -e DATABEND_DSN=databend://default:@127.0.0.1:8000
CMD ["ttc-server"]
