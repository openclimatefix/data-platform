FROM golang:1.24 AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip \
    && rm -rf /var/lib/apt/lists/*

ENV PB_REL="https://github.com/protocolbuffers/protobuf/releases"
RUN curl -LO $PB_REL/download/v30.2/protoc-30.2-linux-x86_64.zip \
    && unzip protoc-30.2-linux-x86_64.zip -d /usr/local

WORKDIR /go/src/app
COPY go.mod go.sum Makefile ./
COPY cmd/ cmd/
COPY internal/ internal/
COPY proto/ proto/

RUN go mod download
RUN go mod verify
RUN go install tool
RUN make gen
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /go/bin/app cmd/main.go

FROM gcr.io/distroless/static-debian11 AS app

COPY --from=build /go/bin/app /
CMD ["/app"]

