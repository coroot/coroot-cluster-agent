FROM golang:1.23-bullseye AS builder
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
      maintainer="Coroot, Inc." \
      version=${VERSION} \
      release="1" \
      summary="Coroot Cluster Agent." \
      description="Coroot Cluster Agent container image."

COPY LICENSE /licenses/LICENSE

COPY --from=builder /tmp/src/coroot-cluster-agent /usr/bin/coroot-cluster-agent
RUN mkdir /data && chown 65534:65534 /data

USER 65534:65534
VOLUME /data
ENTRYPOINT ["coroot-cluster-agent"]
