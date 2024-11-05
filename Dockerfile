FROM golang:1.21-bullseye AS builder
WORKDIR /tmp/src
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
ARG VERSION=unknown
RUN go build -mod=readonly -ldflags "-X main.version=$VERSION" -o coroot-cluster-agent .


FROM registry.access.redhat.com/ubi9/ubi

ARG VERSION=unknown
LABEL name="coroot-cluster-agent" \
      vendor="Coroot, Inc." \
      version=${VERSION} \
      summary="Coroot Cluster Agent."

COPY LICENSE /licenses/LICENSE

COPY --from=builder /tmp/src/coroot-cluster-agent /usr/bin/coroot-cluster-agent

ENTRYPOINT ["coroot-cluster-agent"]
