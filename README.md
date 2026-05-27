# Data Platform

**GRPC API and and database handler for storing and serving energy forecast data.**

<p align="center">
  <picture align="center">
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/f1b3a35f-4fc5-4a62-b6f3-ed41ddcfb6ed">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/16e95933-8978-4517-b3d4-57b8e669526b">
    <img width="762" height="285" alt="Shows a bar chart with benchmark results." src="https://github.com/user-attachments/assets/16e95933-8978-4517-b3d4-57b8e669526b">
  </picture>
</p>

The Data Platform is a gRPC API server that provides efficient access to, and storage of, renewable
energy forecast data. It has been architected to be performant under the specific workflows and
data access patterns required by OCF's applications, in order to enable scaling, and to improve the
developer experience when integrating with OCF's stack. With this in mind, there is a focus on not
just the quality of the code, but also of the tooling surrounding the codebase. This replaces the
old SQLAlchemy `datamodel` repositories and databases.


## Quickstart

### Running the server

The Data Platform gRPC API server is packaged for portability as a container. This can be run using
a container orchestration tool, e.g. with Docker:

```bash
$ docker run -p 50051:50051 ghcr.io/openclimatefix/data-platform
```

Alternatively, it can be run locally using Go. See
[Local Running](#local-running) in the [Development](#development) section.

Once running, the server RPCs can be investigated using a gRPC client tool.

### Configuration

To connect to a backend database and have retention in the platform data, the server must be
appropriately configured via environment variables. All available options are defined via the
configuration file in `cmd/server.conf`.

> [!Important]
> Whilst the configuration is held in a file, this is NOT intended to be overwritten or modified in
> order to configure the Data Platform. Configuration should always be handled via environment
> variables; the config file is simply provided as a version-controlled single point of reference
> for what those variables might be.

The available configuration may differ between versions of the Data Platform. Ensure you check the
correct version of the configuration file for your deployment.

### Connecting a client

There is an example Python script demonstrating how to use the Python bindings in a client to a
Data Platform server. The example runs through a data analysis workflow. To run it, ensure first
that the Data Platform Server is running on `localhost:50051`
(see [Getting Started](#getting-started)); and that the python bindings have been generated (see
[Generating Code](#generating-code)). Then use 
[uvx](https://docs.astral.sh/uv/reference/cli/#uv-tool-run) to run the notebook:

```bash
$ make gen.proto.python
$ uvx marimo edit --headless --sandbox examples/python-notebook/example.py 
```

For ease, the above process is wrapped in a Makefile target:

```bash
$ make run.notebook
```


## Architecture

The Data Platform has clear separation boundaries between its components:

```
                +-------------------------------------------------------------+
                |                     Data Platform Server                    |
                +-------------------+                     +-------------------+
--- Clients --> | External Schema   | <-- Server Impl --- | Database Schema   | <-- Database
                +-------------------+                     +-------------------+
                |                                                             |
                +-------------------------------------------------------------+
```

### gRPC API schema

The Data Platform defines a strongly typed _data contract_ as its external interface, served via
gRPC. This is the API that any external clients have to use to interact with the platform. The
schema for this is defined in Protocol Buffers, located at `proto/ocf/dp`.

Boilerplate code for client and server implementations is generated in the required language from
these `.proto` files using the `protoc` compiler.

> [!Important]
> Changes to the schema modifies the data contract, and may require client and server
> implementations to regenerate their bindings and update their code. As such they should be made
> with purpose and care, and aim to be backwards compatible whenever the affect the hot path.

### Database schema

The Data Platform can be configured to use different database backends. Each backend has a server
implementation that inherits the External Schema. The currently supported backends are:

- PostgreSQL
- Dummy (a memoryless backend for quick testing)

and are selected according to the relevant environment variables (see the
[Configration](#configuration) section). 

The schema for the PostgreSQL backend is defined using PostgreSQL's native SQL dialect in the
`internal/server/postgres/sql/migrations` directory, and access functions to the data are defined
in `internal/server/postgres/sql/queries`.

Boilerplate code for using these queries is generated using the `sqlc` tool. This generated code
provides a strongly typed interface to the database.

> [!Note]
> These changes can be made without having to update the data contract, and so will not require
> updates to clients using the Data Platform.

Having the queries defined in SQL allows for more efficient interaction with the database,
as they can be written to take advantage of the design of the database's features and be written
to be optimal with regards to its indexes.

> [!Important]
> If using PostgreSQL as a backend, it is recommended that you tune your database instance
> according to the specifications of said instance (available CPU and RAM etc). This will ensure
> optimal performance for the Data Platform server.

### Server

The Database Schema is mapped to the External Schema by implementing the server interface generated
from the Data Contract. This is done in `internal/server/<database>/serverimpl.go`. It isn't much
more than a conversion layer, with the business logic shared between the implemented functions and
the SQL queries.


## Development

### Getting Started

This project requires the [Go Toolchain](https://go.dev/doc/install) to be installed.

> [!Note]
> This project uses Go modules for dependency management. Ensure that your `PATH` environment
> variable has been updated to include the Go binary installation location, as per the instructions
> linked above, otherwise you may see errors.

Clone the repository, then run

```bash
$ make init
```

This will fetch the dependencies, and install the git hooks required for development.

> [!Important]
> Since this project is uses lots of generated code, these hooks are vital to keep this generated
> code up to date, and as such running `make init` is a necessary step towards a smooth development
> experience.

### Local running

The server can be run locally with no database connection via a fake database implementation via
a Make target. This is recommended as it will ensure that code generation is up to date and that
the running version has been embedded into the built binary.

```bash
$ make run
```

This will start the Data Platform API GRPC's server on `localhost:50051`. The RPCs can then be
investigated using a tool such as [grpcurl](https://github.com/fullstorydev/grpcurl) or 
[grpcui](https://fullstorydev/grpcui). In this testing mode, the data returned by the server is
entirely generated and has little bearing on the request objects themselves.

There is also an example Docker compose file in `examples/docker-compose.yml`, which runs the Data
Platform API server in a container, backed by Postgres, and which also includes a GRPC UI for
testing.

### Testing

Unit tests can be run using `make test`. Benchmarks can be run using `make bench`.
Both of these utilise [TestContainers](https://github.com/testcontainers/testcontainers-go),
so ensure you meet their 
[general system requirements](https://golang.testcontainers.org/system_requirements/).

### Generating Code
 
In order to make changes to the *SQL queries*, or add a new *Database migration*, you will need to
add or modify the relevant `.sql` files in the `sql` directory. Then, regenerate the Go library
code to reflect these changes. This can be done using

```
$ make gen
```

This will populate the `internal/server/postgres/gen` directory with language-specific bindings
for implementations of server and client code. Next, update the `serverimpl.go` file for the given
database to use the newly generated code, and ensure the test suite passes. Since the Data Platform
container automatically migrates the database on startup, simply re-deploying the container will
propagate the changes to your deployment environment.

In order to change the *Data Contract*, you will need to modify the `.proto` files in the `proto`
directory, and regenerate the code. GRPC client/server interfaces - and boilerplate code - gets
generated from these Protocol Buffer definitions. The `make gen` target already handles generating
the go code used internall in the application, placing generated code in `internal/gen`.

Language-specific client/server bindings for external applications are generated as part of the CI
pipeline, but can also be generated manually, e.g. for python

```bash
$ make gen.proto.python
```

This places the generated code in `gen/python`. See the `Makefile` for more external targets.

## API Documentation
