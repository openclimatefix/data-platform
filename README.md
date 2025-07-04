# FCFS Data Platform

**Reimagining OCF's Data Platform for Performance and Useability**


## Documentation

### External schema (replacing `pydantic` in the old `datamodel`)

The Data Platform defines a strongly typed _data contract_ as its external interface. This is the
API that any external clients have to use to interact with the platform. The schema for this is
defined via Protocol Buffers in `proto/ocf/dp`.

Boilerplate code for client and server implementations is generated in the required language from
these `.proto` files using the `protoc` compiler.

Changes to the schema modifies the data contract, and will require client and server
implementations to regenerate their bindings and update their code. As such they should be made
with purpose and care.

**Generating bindings**

```
make gen-ext
```

This will populate the `protogen` directory with language-specific bindings for implementations
of server and client code.


## Example usage

```bash
$ go run src/cmd/main/go
```

## Installation

Install using docker

```bash
$ docker build . --tag api:local
```

or via Go

```
$ go install github.com/devsjc/fcfs
```

