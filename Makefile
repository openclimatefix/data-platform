.PHONY: init
init:
	@echo "Installing Go dependencies..."
	go get ./...
	go install tool
	@echo "Adding git hooks..."
	@git config --local core.hooksPath .github/hooks
	@echo "Generating boilerplate code..."
	@make gen

.PHONY: test
test:
	go run gotest.tools/gotestsum@latest --format=testname --junitfile unit-tests.xml

.PHONY: lint
lint:
	@go mod tidy
	@go tool gofumpt -l -w .

.PHONY: bench
bench:
	@go test ./...  -bench=. -run=^a -timeout=15m

.PHONY: gen
gen: gen-db gen-proto

.PHONY: gen-db
gen-db:
	@echo "Generating internal database code..."
	@go tool sqlc generate --file internal/database/postgres/.sqlc.yaml
	@echo " * Success."

.PHONY: gen-proto
gen-proto:
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

# --- EXTERNAL GENERATION TARGETS --------------------------------------------------------------- #

.PHONY: gen-ext
gen-ext: gen-proto-python gen-proto-openapi gen-proto-typescript

.PHONY: gen-proto-python
gen-proto-python:
	rm -rf gen/python && mkdir -p gen/python
	uvx --from 'betterproto[compiler]==2.0.0b7' protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--python_betterproto_opt=typing.310 \
		--python_betterproto_out=gen/python

.PHONY: gen-proto-typescript
gen-proto-typescript:
	@test -s protoc-gen-ts || npm install -g @protobuf-ts/plugin
	rm -rf gen/typescript && mkdir -p gen/typescript
	protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--ts_out=gen/typescript \

.PHONY: gen-proto-openapi
gen-proto-openapi:
	rm -rf gen/openapi && mkdir -p gen/openapi
	@test -s protoc-gen-openapi || go install github.com/googleapis/gnostic/apps/protoc-gen-openapi@latest
	protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--openapi_out=gen/openapi
	npx redocly build-docs gen/openapi.yaml --output gen/index.html

# --- LOCAL RUNNING TARGETS --------------------------------------------------------------------- #

.PHONY: run-db
run-db:
	docker build -f internal/database/postgres/infra/Containerfile internal/database/postgres/infra -t fcfs-pgdb:local && docker run --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p "5400:5432" fcfs-pgdb:local

.PHONY: migrate-db
migrate-db:
	goose postgres "postgresql://postgres:postgres@localhost:5400/postgres" -dir ./internal/database/postgres/sql/migrations up

.PHONY: run-api
run-api:
	DATABASE_URL=postgres://postgres:postgres@localhost:5400/postgres DATABASE_TYPE=postgres LOGLEVEL=DEBUG go run cmd/main.go

.PHONY: run-grpc-client
run-grpc-client:
	grpcui -plaintext localhost:50051

