/// This file is used to generate SDK files from proto definitions.
/// Requires protoc tool installed: https://grpc.io/docs/protoc-installation/
// See also: https://grpc.io/docs/languages/go/quickstart/#prerequisites
package proto

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
//go:generate npm install -g @protobuf-ts/plugin
//go:generate protoc --version

//go:generate protoc --go_out=../gen --go_opt=paths=source_relative api.proto
//go:generate protoc --go-grpc_out=../gen --go-grpc_opt=paths=source_relative api.proto
//go:generate protoc --ts_out=../gen --ts_opt=output_javascript api.proto
