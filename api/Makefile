.PHONY: run-docker-db run-api

gen:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	npm install -g @protobuf-ts/plugin @redocly/cli
	uv pip install betterproto
	uv run protoc \
		src/proto/fcfsapi/*.proto \
		-I=src/proto \
		--go_out=src/internal/models \
		--go_opt=paths=source_relative \
		--go-grpc_out=require_unimplemented_servers=false:src/internal/models \
		--go-grpc_opt=paths=source_relative \
		--python_betterproto_out=src/sdk/python \
		--openapi_out=src/sdk/html
	redocly build-docs src/sdk/html/openapi.yaml --output src/sdk/index.html
	go generate ./...

run-db:
	docker build -f src/internal/repository/postgres/infra/Containerfile src/internal/repository/postgres/infra -t fcfs-pgdb:local && docker run --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p "5400:5432" fcfs-pgdb:local

run-api:
	DATABASE_URL=postgres://postgres:postgres@localhost:5400/postgres DATABASE_TYPE=postgres go run src/cmd/main.go

run-grpc-client:
	grpcui -plaintext localhost:50051
