FROM golang:1.21-bullseye AS builder
WORKDIR /tmp/src
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
ARG VERSION=unknown
RUN go build -mod=readonly -ldflags "-X main.version=$VERSION" -o /tmp/coroot-cluster-agent .


FROM debian:bullseye
RUN apt update && apt install -y ca-certificates && apt clean

COPY --from=builder /tmp/coroot-cluster-agent /coroot-cluster-agent

ENTRYPOINT ["/coroot-cluster-agent", "config.yaml"]
