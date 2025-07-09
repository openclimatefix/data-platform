# FCFS Data Platform

**Reimagining OCF's Data Platform for Performance and Useability**

- 2 orders of magnitude faster than current dataplatform (milliseconds vs seconds)
- Able to scale to fit OCF's ambition for increased size and scope
- Fully typed implementations in Python and Typescript
- Simple to understand due to codegen of boilerplate
- Costly metrics and blend apps obsoleted with on-the-fly calculation capability
- Safer data platform with single, considered source of entry to database

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


## Examples

### Python notebook

There is an example python notebook outlining using the data platform as a data analysis tool.
It shows an how an analysis workflow would use the generated python library code. To run it,
ensure the DataPlatform API is running on `localhost:50051`, and that colocated with the script
are the latest generated python bindings; then use uvx to run the notebook:

```bash
$ make gen-proto-python && cp -r protogen/python/* examples/python-notebook/
$ cd examples/python-notebook && uvx --with="marimo" marimo edit --headless example.py 
```


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

