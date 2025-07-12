.PHONY: all test bench gen-int gen-ext gen-db-internal gen-proto-internal gen-proto-python gen-proto-typescript gen-proto-openapi run-db migrate-db run-api run-grpc-client gen-reqs

REF_NAME ?= $(shell git symbolic-ref HEAD --short | tr / - 2>/dev/null)

test:
	go run gotest.tools/gotestsum@latest --format=testname --junitfile unit-tests.xml

bench:
	go test ./...  -bench=. -run=^a -timeout=15m > bench-$(REF_NAME).txt

bench-stat:
	@test -s benchstat || go install golang.org/x/perf/cmd/benchstat@latest
	@test -e bench-main.txt && benchstat bench-main.txt bench-$(REF_NAME).txt || benchstat bench-$(REF_NAME).txt

lint:
	@go mod tidy
	@gofmt -l -w .

gen-int: gen-db-internal gen-proto-internal

gen-ext: gen-proto-python gen-proto-openapi gen-proto-typescript

gen-db-internal:
	go generate ./...

gen-proto-internal:
	protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--go_out=internal/protogen \
		--go_opt=paths=source_relative \
		--go-grpc_out=require_unimplemented_servers=false:internal/protogen \
		--go-grpc_opt=paths=source_relative \

gen-proto-python:
	rm -rf protogen/python
	mkdir -p protogen/python
	uvx --from 'betterproto[compiler]==2.0.0b7' protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--python_betterproto_opt=typing.310 \
		--python_betterproto_out=protogen/python

gen-proto-typescript:
	rm -rf protogen/typescript
	mkdir -p protogen/typescript
	protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--ts_out=protogen/typescript \

gen-proto-openapi:
	rm -rf protogen/openapi
	mkdir -p protogen/openapi
	protoc \
		proto/ocf/dp/*.proto \
		-I=proto \
		--openapi_out=protogen/openapi
	redocly build-docs protogen/openapi.yaml --output protogen/index.html

run-db:
	docker build -f internal/database/postgres/infra/Containerfile internal/database/postgres/infra -t fcfs-pgdb:local && docker run --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p "5400:5432" fcfs-pgdb:local

migrate-db:
	goose postgres "postgresql://postgres:postgres@localhost:5400/postgres" -dir ./internal/database/postgres/sql/migrations up

run-api:
	DATABASE_URL=postgres://postgres:postgres@localhost:5400/postgres DATABASE_TYPE=postgres LOGLEVEL=DEBUG go run cmd/main.go

run-grpc-client:
	grpcui -plaintext localhost:50051

gen-reqs:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	npm install -g @protobuf-ts/plugin @redocly/cli
	uv tool install betterproto

