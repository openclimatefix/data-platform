# --- DEVELOPMENT TARGETS ------------------------------------------------------------------- #

.PHONY: init
init:
	@GO_BIN="$${GOPATH:-$${HOME}/go}/bin"; \
	if ! echo "$${PATH}" | grep -q "$${GO_BIN}"; then \
		export PATH="$${PATH}:$${GO_BIN}"; \
	fi
	@echo "Generating code..."
	@make gen
	@echo "Installing Go dependencies..."
	@go get ./...
	@echo " * Success."
	@echo "Adding git hooks..."
	@git config --local core.hooksPath .github/hooks

.PHONY: test
test:
	go run gotest.tools/gotestsum@latest --format=testname --junitfile unit-tests.xml

.PHONY: lint
lint:
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0
	@go mod tidy
	@golangci-lint run \
	   --show-stats=false --fix
	@gofmt -l . # Lists files that are likely to be changed by the next command
	@golangci-lint fmt
	@uvx -q sqlfluff fix -q \
		--disable-progress-bar \
		--config=internal/server/postgres/sql/.sqlfluff.toml \
		internal/server/postgres/sql/queries

.PHONY: bench
bench:
	@go test ./...  -bench=. -run=^a -timeout=30m

.PHONY: gen
gen: gen.db gen.proto.go

.PHONY: doctor
doctor:
	@go version
	@protoc --version || echo "protoc not installed"
	@echo "sqlc $$(sqlc version || echo "not installed")"

# --- SUPPLEMENTARY TARGETS ---------------------------------------------------------------------- #

.PHONY: gen.db
gen.db:
	@go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0
	@echo "Generating internal database code..."
	@sqlc generate --file internal/server/postgres/.sqlc.yaml
	@echo " * Success."

.PHONY: gen.proto.go
gen.proto.go: install.protoc
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.9
	@echo "Generating internal protobuf code..."
	@rm -rf internal/gen && mkdir -p internal/gen
	@protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--go_out=internal/gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=require_unimplemented_servers=false:internal/gen \
		--go-grpc_opt=paths=source_relative
	@echo " * Success."

.PHONY: install.protoc
# Apologies, I'm installing it to gobin since it's already on the path, and I'm assuming (I know)
# all devs have ARM macs...
install.protoc:
	@GO_PATH="$${GOPATH:-$${HOME}/go}"; \
	if [ ! -f "$${GO_PATH}/bin/protoc" ]; then \
		echo "Installing protoc..."; \
		PB_REL="https://github.com/protocolbuffers/protobuf/releases" ;\
		PB_VER="32.1" ;\
		if [ "$$(uname -s)" = "Darwin" ]; then \
			curl -L "$${PB_REL}/download/v$${PB_VER}/protoc-$${PB_VER}-osx-aarch_64.zip" -o /tmp/protoc.zip; \
		elif [ "$$(uname -s)" = "Linux" ]; then \
			curl -L "$${PB_REL}/download/v$${PB_VER}/protoc-$${PB_VER}-linux-x86_64.zip" -o /tmp/protoc.zip; \
		else \
			echo "Unsupported OS: $$(uname -s)"; exit 1; \
		fi && unzip /tmp/protoc.zip -x readme.txt -d "$${GO_PATH}" && rm /tmp/protoc.zip; \
	fi;

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
gen.proto.python: install.protoc
	@echo "Generating Python client bindings..."
	@rm -rf gen/python && mkdir -p gen/python/src/dp_sdk
	@uvx --from 'betterproto[compiler]==2.0.0b7' protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--python_betterproto_opt=typing.310 \
		--python_betterproto_out=gen/python/src/dp_sdk
	@echo "$$pyproj" > gen/python/pyproject.toml
	@echo "Building wheel..."
	@cd gen/python && uv build && cd ../..

.PHONY: gen.proto.typescript
gen.proto.typescript: install.protoc
	@test -s protoc-gen-ts || npm install -g @protobuf-ts/plugin
	@rm -rf gen/typescript && mkdir -p gen/typescript
	@protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--ts_out=gen/typescript \

.PHONY: gen.proto.openapi
gen.proto.openapi: install.protoc
	@rm -rf gen/openapi && mkdir -p gen/openapi
	@test -s protoc-gen-openapi || go install github.com/googleapis/gnostic/apps/protoc-gen-openapi@latest
	@protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--openapi_out=gen/openapi
	@npx redocly build-docs gen/openapi.yaml --output gen/index.html

# --- LOCAL RUNNING TARGETS --------------------------------------------------------------------- #

.PHONY: run # Run the Data Platform GRPC API.
# Set DATABASE_URL="postgresql://postgres:postgres@localhost:5400/postgres" in env to connect to instance spawned with `make run.db`
run:
	DATABASE_URL=${DATABASE_URL} LOGLEVEL=DEBUG go run cmd/main.go

.PHONY: run.db # Run an instance of Postgres with the required extensions
run.db:
	docker build -f internal/server/postgres/infra/Containerfile internal/server/postgres/infra -t data-platform-pgdb:local
	docker run --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p "5400:5432" data-platform-pgdb:local

.PHONY: run.client # Run a GRPC client to inspect the API
run.client:
	grpcui -plaintext localhost:50051

.PHONY: run.notebook # Run a python notebook to inspect the API
run.notebook: gen.proto.python
	uvx marimo edit --headless --sandbox examples/python-notebook/example.py
