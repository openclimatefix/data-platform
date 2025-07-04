.PHONY: test bench gen gen-reqs gen-go gen-ext gen-db-go gen-proto-go gen-proto-python gen-proto-openapi run-db run-api run-grpc-client

test:
	go run gotest.tools/gotestsum@latest --format=testname --junitfile unit-tests.xml

bench:
	go test ./...  -bench=BenchmarkPostgresClient -run=BenchmarkPostgresClient -timeout=50m

gen-reqs:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	npm install -g @protobuf-ts/plugin @redocly/cli
	uv tool install betterproto

gen: gen-go gen-ext

gen-go: gen-db-go gen-proto-go

gen-ext: gen-proto-python gen-proto-openapi

gen-db-go:
	go generate ./...

gen-proto-go:
	protoc \
		src/proto/fcfsapi/*.proto \
		-I=src/proto \
		--go_out=src/internal/models \
		--go_opt=paths=source_relative \
		--go-grpc_out=require_unimplemented_servers=false:src/internal/models \
		--go-grpc_opt=paths=source_relative \

gen-proto-python:
	protoc \
		src/proto/fcfsapi/*.proto \
		-I=src/proto \
		--python_betterproto_out=src/sdk/python

gen-proto-typescript:
	mkdir -p src/sdk/typescript
	protoc \
		src/proto/fcfsapi/*.proto \
		-I=src/proto \
		--ts_out=src/sdk/typescript \

gen-proto-openapi:
	protoc \
		src/proto/fcfsapi/*.proto \
		-I=src/proto \
		--openapi_out=src/sdk/html
	redocly build-docs src/sdk/html/openapi.yaml --output src/sdk/index.html

run-db:
	docker build -f src/internal/repository/postgres/infra/Containerfile src/internal/repository/postgres/infra -t fcfs-pgdb:local && docker run --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p "5400:5432" fcfs-pgdb:local

migrate-db:
	goose postgres "postgresql://postgres:postgres@localhost:5400/postgres" -dir ./src/internal/repository/postgres/sql/migrations up

run-api:
	DATABASE_URL=postgres://postgres:postgres@localhost:5400/postgres DATABASE_TYPE=postgres go run src/cmd/main.go

run-grpc-client:
	grpcui -plaintext localhost:50051

