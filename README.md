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

## Development

This project requires the [Go Toolchain](https://go.dev/doc/install) to be installed.
Clone the repository, then run

```bash
$ make init
```

This will fetch the dependencies, and install the git hooks required for development.

> [!Note]
> Since this project is uses lots of generated code, these hooks are vital to keep this generated
> code up to date, and as such running `make init` is a vital step towards a smooth development
> experience.

### Running tests

Unit tests can be run using `make test`. Benchmarks can be run using `make bench`.
Both of these utilise [TestContainers](https://github.com/testcontainers/testcontainers-go),
so ensure you meet their 
[general system requirements](https://golang.testcontainers.org/system_requirements/).

### Generating code

If you make changes to the SQL migrations or queries, or to the Protocol Buffers schema,
you will need to regenrate the Go library code to reflect these changes. Again there is a make
target for this, `make gen`.
