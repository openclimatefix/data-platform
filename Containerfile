FROM golang:1.24 AS build

ENV DEBIAN_FRONTEND=noninteractive
ENV GOPATH=/go
RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip \
    && rm -rf /var/lib/apt/lists/*

ENV PB_REL="https://github.com/protocolbuffers/protobuf/releases"
RUN curl -LO $PB_REL/download/v30.2/protoc-30.2-linux-x86_64.zip \
    && unzip protoc-30.2-linux-x86_64.zip -d /go \
    && rm protoc-30.2-linux-x86_64.zip

WORKDIR /go/src/app
COPY go.mod go.sum Makefile ./
COPY cmd/ cmd/
COPY internal/ internal/
COPY proto/ proto/

RUN \
    --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    go mod download
RUN go mod verify
RUN \
    --mount=type=cache,target=/go/bin,sharing=locked \
    --mount=type=cache,target=/go/include,sharing=locked \
    make gen
RUN make CGO_ENABLED=0 GOOS=linux GOARCH=amd64 build

FROM gcr.io/distroless/static-debian11 AS app

COPY --from=build bin/dp-server /
CMD ["/dp-server"]

