# --- CONFIGURATION ----------------------------------------------------------------------- #
# Use GOPATH/bin or GOBIN, default to ~/go/bin
GOPATH ?= $(shell go env GOPATH)
GOBIN  := $(shell go env GOBIN)
ifeq ($(GOBIN),)
	GOBIN := $(GOPATH)/bin
endif
VERSION := $(shell git describe --tags --always --long --dirty)
OUT := bin/dp-server

# --- Binaries and Tools ---
PROTOC  := $(GOBIN)/protoc
SQLC    := $(GOBIN)/sqlc
PROTOC_GEN_GO    := $(GOBIN)/protoc-gen-go
PROTOC_GEN_GRPC := $(GOBIN)/protoc-gen-go-grpc
TOOLS := $(SQLC) $(PROTOC) $(PROTOC_GEN_GO) $(PROTOC_GEN_GRPC)

# --- Sources ---
GO_SOURCES := $(shell find . -name '*.go' -not -path "./internal/gen/*" -not -path "./vendor/*")
PROTO_SOURCES := $(shell find proto/ocf/dp -name '*.proto')
PROTO_STAMP_FILE := internal/gen/.protoc.stamp
SQLC_SOURCES := $(shell find internal/server/postgres/sql -name '*.sql')
SQLC_SOURCE_CONFIG := internal/server/postgres/.sqlc.yaml
SQLC_STAMP_FILE := internal/server/postgres/.sqlc.stamp

# --- MAIN TARGETS ------------------------------------------------------------------- #

# Build the binary
${OUT}: ${GO_SOURCES} ${PROTO_STAMP_FILE} ${SQLC_STAMP_FILE}
	@go build -v -o=${OUT} -ldflags="-X 'main.version=${VERSION}'" cmd/main.go

.PHONY: build
build: ${OUT}

.PHONY: run
run: ${OUT}
	@./${OUT}

.PHONY: init
init: tools gen
	@go mod tidy
	@git config --local core.hooksPath .github/hooks

.PHONY: test
test: gen
	@go run gotest.tools/gotestsum@latest --format=testname --junitfile unit-tests.xml

.PHONY: lint
lint:
	@go mod tidy
	@go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0 run \
		--show-stats=false --fix
	@gofmt -l . # Lists files that are likely to be changed by the next command
	@go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0 fmt
	@uvx -q sqlfluff fix -q \
		--disable-progress-bar \
		--config=internal/server/postgres/sql/.sqlfluff.toml \
		internal/server/postgres/sql/queries

.PHONY: bench
bench: gen
	@go test ./...  -bench=. -run=^a -timeout=30m

.PHONY: clean
clean:
	@echo "Cleaning up..."
	@rm -f ${OUT}
	@rm -f ${SQLC_STAMP_FILE}
	@rm -f ${PROTO_STAMP_FILE}
	@rm -rf internal/gen
	@rm -rf internal/server/postgres/gen
	@rm -rf gen/python
	@rm -f unit-tests.xml

.PHONY: doctor
doctor: ${PROTOC} ${SQLC}
	@go version
	@${PROTOC} --version || echo "protoc not installed"
	@echo "sqlc $$(${SQLC} version || echo "not installed")"

# --- Code Generation Targets ------------------------------------------------------------- #

.PHONY: gen
gen: ${PROTO_STAMP_FILE} ${SQLC_STAMP_FILE}

.PHONY: gen.db
gen.db: ${SQLC_STAMP_FILE}

.PHONY: gen.proto.go
gen.proto.go: ${PROTO_STAMP_FILE}

# Depends on the sqlc binary, the config, and all .sql files.
${SQLC_STAMP_FILE}: ${SQLC} ${SQLC_SOURCES} ${SQLC_SOURCE_CONFIG}
	@echo "Generating internal database code..."
	@${SQLC} generate --file ${SQLC_SOURCE_CONFIG}
	@touch ${SQLC_STAMP_FILE}
	@echo " * Success."

# Depends on the protoc binary, plugins, and all .proto source files.
${PROTO_STAMP_FILE}: ${PROTOC} ${PROTOC_GEN_GO} ${PROTOC_GEN_GRPC} ${PROTO_SOURCES}
	@echo "Generating internal protobuf code..."
	@rm -rf internal/gen && mkdir -p internal/gen
	@${PROTOC} \
		${PROTO_SOURCES} \
		-I=proto \
		--go_out=internal/gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=require_unimplemented_servers=false:internal/gen \
		--go-grpc_opt=paths=source_relative
	@touch ${PROTO_STAMP_FILE}
	@echo " * Success."

# --- SUPPLEMENTARY TARGETS ---------------------------------------------------------------------- #

.PHONY: tools
tools: ${TOOLS}

${PROTOC}:
	@echo "Installing protoc..."
	@PB_REL="https://github.com/protocolbuffers/protobuf/releases" ;\
	PB_VER="32.1" ;\
	if [ "$$(uname -s)" = "Darwin" ]; then \
		curl -L "$${PB_REL}/download/v$${PB_VER}/protoc-$${PB_VER}-osx-aarch_64.zip" -o /tmp/protoc.zip; \
	elif [ "$$(uname -s)" = "Linux" ]; then \
		curl -L "$${PB_REL}/download/v$${PB_VER}/protoc-$${PB_VER}-linux-x86_64.zip" -o /tmp/protoc.zip; \
	else \
		echo "Unsupported OS: $$(uname -s)"; exit 1; \
	fi && unzip /tmp/protoc.zip -x readme.txt -d "$(GOPATH)" && rm /tmp/protoc.zip; \

${SQLC}:
	@echo "Installing sqlc..."
	@go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0

${LINTER}:
	@echo "Installing golangci-lint..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0

${TESTSUM}:
	@echo "Installing gotestsum..."
	@go install gotest.tools/gotestsum@latest

${PROTOC_GEN_GO}:
	@echo "Installing protoc-gen-go..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.9

${PROTOC_GEN_GRPC}:
	@echo "Installing protoc-gen-go-grpc..."
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

# --- EXTERNAL GENERATION TARGETS --------------------------------------------------------------- #

define pyproj =
[build-system]
requires = ["setuptools>=67", "wheel", "setuptools-git-versioning>=2.0,<3"]
build-backend = "setuptools.build_meta"
[project]
name = "dp-sdk"
dynamic = ["version"]
description = "Python client for OCF Data Platform API"
dependencies = ["betterproto==2.0.0b7", "grpcio"]
[tool.setuptools.packages.find]
where = ["src"]
[tool.setuptools-git-versioning]
enabled = true
endef

.PHONY: gen.proto.python
gen.proto.python: ${PROTOC}
	@echo "Generating Python client bindings..."
	@rm -rf gen/python && mkdir -p gen/python/src/dp_sdk
	@uvx --from 'betterproto[compiler]==2.0.0b7' ${PROTOC} \
		proto/ocf/dp/*.proto \
		-I=proto \
		--python_betterproto_opt=typing.310 \
		--python_betterproto_out=gen/python/src/dp_sdk
	@echo "$$pyproj" > gen/python/pyproject.toml
	@echo "Building wheel..."
	@cd gen/python && uv build && cd ../..

# --- LOCAL RUNNING TARGETS --------------------------------------------------------------------- #

.PHONY: run.db # Run an instance of Postgres with the required extensions
run.db:
	docker build -f internal/server/postgres/infra/Containerfile internal/server/postgres/infra -t data-platform-pgdb:local
	docker run --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p "5400:5432" data-platform-pgdb:local

.PHONY: run.notebook # Run a python notebook to inspect the API
run.notebook: gen.proto.python
	uvx marimo edit --headless --sandbox examples/python-notebook/example.py

