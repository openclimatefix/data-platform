# FCFS API

**GRPC API serving predicted renewable generation values for the FCFS project.**

## Installation

Install using docker

```bash
$ docker build . --tag api:local
```

or via Go

```
$ go install github.com/devsjc/fcfs/api
```

## Documentation

The API is defined using `.proto` files in `src/proto`.
These represent the contract between the client and any servers invoking the API.

Relevant models and interfaces for both client and server implementations are then generated
using the [protoc compiler](https://protobuf.dev/installation/), invoked through `go generate`:

```bash
$ go generate ./...
```

This will populate the `src/gen` directory with language-specific bindings for the API.

These bindings are then used to implement the server code in `src/internal/service/server.go`.
They should also be imported or copied into external codebases to create type-safe and contract-bound clients.

The server then implements custom logic to serve the generated interface
via the use of an abstraction, representing a data repository.
This interface - or boundary layer - between the API logic and the data layer
is defined in `src/internal/models.go`, along with other models relevant to the internal logic of the API.

By separating the layers like this,
the API can be easily tested and extended without needing to change the underlying data layer;
similarly, data layers can be swapped out or modified without breaking the API contract.

```
+----------+                     +----------+                             + - - - - - - - - +
|  Client  |  <--- GRPC API -->  |  Server  |  <-- Database Interface --> : Data Repository :
+----------+                     +----------+                             + - - - - - - - - +

|<--Uses generated bindings-->|  |<------------Logic in the the API repository------------->|          
```

Currently [postgres](https://www.postgresql.org/) is the only concrete data repository implementation,
but there is also a dummy repository for testing purposes. These are found in `src/internal/repository`.

## Example usage

```bash
$ go run src/cmd/main/go
```

