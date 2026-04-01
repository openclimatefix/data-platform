GO_TOOL := go tool -modfile=go.tool.mod
SQLC    := $(GO_TOOL) sqlc
LINT    := $(GO_TOOL) golangci-lint
TESTSUM := $(GO_TOOL) gotestsum
VULN    := $(GO_TOOL) govulncheck

# --- Local Binaries (PATH dependent) ---
LOCAL_BIN := bin
OUT := $(LOCAL_BIN)/dp-server
export PATH := $(LOCAL_BIN):$(PATH)
PROTOC           := $(LOCAL_BIN)/protoc
PROTOC_INCLUDE   := $(LOCAL_BIN)/include
PROTOC_GEN_GO    := $(LOCAL_BIN)/protoc-gen-go
PROTOC_GEN_GRPC  := $(LOCAL_BIN)/protoc-gen-go-grpc

# --- Sources & Stamps ---
GO_SOURCES          := $(shell find . -name '*.go' -not -path "./internal/gen/*" -not -path "./vendor/*")
PROTO_SOURCES       := $(shell find proto/ocf/dp -name '*.proto')
PROTO_GO_STAMP_FILE := internal/gen/.protoc.stamp
SQLC_SOURCES        := $(shell find internal/server/postgres/sql -name '*.sql')
SQLC_SOURCE_CONFIG  := internal/server/postgres/.sqlc.yaml
SQLC_STAMP_FILE     := internal/server/postgres/.sqlc.stamp

# --- MAIN TARGETS ------------------------------------------------------------------- #

.PHONY: build
build: ${OUT}

${OUT}: ${GO_SOURCES} ${PROTO_GO_STAMP_FILE} ${SQLC_STAMP_FILE}
	@echo "Compiling to ${OUT}..."
	@mkdir -p $(dir $@)
	go build -v -o=${OUT} -ldflags="-X 'main.version=${VERSION}'" cmd/main.go

.PHONY: run
run: ${OUT}
	@./${OUT}

.PHONY: init
init: gen
	@echo "Tidying main and tool modules..."
	@go mod tidy
	@go -modfile=go.tool.mod mod tidy
	@git config --local core.hooksPath .github/hooks

.PHONY: test
test: gen
	@$(TESTSUM) --format=testname --junitfile unit-tests.xml

.PHONY: lint
lint:
	@$(LINT) run --show-stats=false --fix
	@gofmt -l . 
	@$(LINT) fmt
	@uvx -q sqlfluff fix -q \
		--disable-progress-bar \
		--config=internal/server/postgres/sql/.sqlfluff.toml \
		internal/server/postgres/sql/queries

.PHONY: bench
bench: gen
	@go test ./... -bench=. -run=^a -timeout=20m

.PHONY: clean
clean:
	@echo "Cleaning up..."
	@rm -rf bin/
	@rm -f ${SQLC_STAMP_FILE} ${PROTO_GO_STAMP_FILE} unit-tests.xml
	@rm -rf internal/gen internal/server/postgres/gen gen/python

.PHONY: doctor
doctor: $(PROTOC)
	@go version
	@$(LINT) --version
	@$(SQLC) version
	@$(PROTOC) --version

# --- Code Generation Targets ------------------------------------------------------------- #

.PHONY: gen
gen: gen.proto.go gen.db

.PHONY: gen.db
gen.db: ${SQLC_STAMP_FILE}

.PHONY: gen.proto.go
gen.proto.go: ${PROTO_GO_STAMP_FILE}

${SQLC_STAMP_FILE}: ${SQLC_SOURCES} ${SQLC_SOURCE_CONFIG}
	@echo "Generating internal database code..."
	@$(SQLC) generate --file ${SQLC_SOURCE_CONFIG}
	@touch ${SQLC_STAMP_FILE}
	@echo " * Success."

${PROTO_GO_STAMP_FILE}: ${PROTOC} ${PROTOC_GEN_GO} ${PROTOC_GEN_GRPC} ${PROTO_SOURCES}
	@echo "Generating internal protobuf code..."
	@rm -rf internal/gen && mkdir -p internal/gen
	@${PROTOC} \
		${PROTO_SOURCES} \
		-I=proto \
		-I=$(PROTOC_INCLUDE) \
		--go_out=internal/gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=require_unimplemented_servers=false:internal/gen \
		--go-grpc_opt=paths=source_relative
	@touch ${PROTO_GO_STAMP_FILE}
	@echo " * Success."

# --- LOCAL TOOL INSTALLATION ----------------------------------------------------------------- #

# protoc (C++ binary) must still be downloaded manually, including standard types.
${PROTOC}:
	@echo "Installing protoc to $(LOCAL_BIN)..."
	@mkdir -p $(LOCAL_BIN)
	@PB_REL="https://github.com/protocolbuffers/protobuf/releases" ;\
	PB_VER="27.2" ;\
	if [ "$$(uname -s)" = "Darwin" ]; then \
		curl -sSL "$${PB_REL}/download/v$${PB_VER}/protoc-$${PB_VER}-osx-aarch_64.zip" -o /tmp/protoc.zip; \
	elif [ "$$(uname -s)" = "Linux" ]; then \
		curl -sSL "$${PB_REL}/download/v$${PB_VER}/protoc-$${PB_VER}-linux-x86_64.zip" -o /tmp/protoc.zip; \
	else \
		echo "Unsupported OS: $$(uname -s)"; exit 1; \
	fi && \
	unzip -o -j /tmp/protoc.zip bin/protoc -d $(LOCAL_BIN) && \
	unzip -o /tmp/protoc.zip "include/*" -d $(LOCAL_BIN) && \
	rm /tmp/protoc.zip; \
	chmod +x $(LOCAL_BIN)/protoc

# Build the plugins directly from the versions locked in go.tool.mod
${PROTOC_GEN_GO}: go.tool.mod
	@echo "Building protoc-gen-go from go.tool.mod..."
	@mkdir -p $(LOCAL_BIN)
	@go build -modfile=go.tool.mod -o $@ google.golang.org/protobuf/cmd/protoc-gen-go

${PROTOC_GEN_GRPC}: go.tool.mod
	@echo "Building protoc-gen-go-grpc from go.tool.mod..."
	@mkdir -p $(LOCAL_BIN)
	@go build -modfile=go.tool.mod -o $@ google.golang.org/grpc/cmd/protoc-gen-go-grpc

# --- EXTERNAL GENERATION TARGETS --------------------------------------------------------------- #

define GEN_PYPROJ
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
export GEN_PYPROJ

.PHONY: gen.proto.python
gen.proto.python: ${PROTOC}
	@echo "Generating Python client bindings..."
	@rm -rf gen/python && mkdir -p gen/python/src/dp_sdk
	@uvx --from 'grpcio-tools==1.80.0' --with 'betterproto[compiler]==2.0.0b7' python-grpc-tools-protoc \
		$$(find proto -iname "*.proto") \
		-I=proto \
		-I=$(PROTOC_INCLUDE) \
		--python_out=gen/python/src/dp_sdk \
		--pyi_out=gen/python/src/dp_sdk \
		--grpc_python_out=gen/python/src/dp_sdk
		--python_betterproto_opt=typing.310 \
		--python_betterproto_out=gen/python/src/dp_sdk
	@touch gen/python/src/dp_sdk/py.typed
	@echo "$$GEN_PYPROJ" > gen/python/pyproject.toml
	@echo "Building wheel..."
	@cd gen/python && echo $$(uv run python -m setuptools_git_versioning) && uv build

# --- LOCAL RUNNING TARGETS --------------------------------------------------------------------- #

.PHONY: run.db
run.db:
	docker build -f internal/server/postgres/infra/Containerfile internal/server/postgres/infra -t data-platform-pgdb:local
	docker run --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p "5400:5432" data-platform-pgdb:local postgres -c 'shared_preload_libraries=pg_cron' -c 'cron.database_name=postgres'

.PHONY: run.notebook
run.notebook: gen.proto.python
	uvx marimo edit --headless --sandbox examples/python-notebook/example.py
