# FCFS Data Platform

**Reimagining OCF's Data Platform for Performance and Useability**

- 2 orders of magnitude faster than current dataplatform (milliseconds vs seconds)
- Able to scale to fit OCF's ambition for increased size and scope
- Fully typed implementations in Python and Typescript
- Simple to understand due to codegen of boilerplate
- Costly metrics and blend apps obsoleted with on-the-fly calculation capability
- Safer data platform with single, considered source of entry to database
- Unlocks greater depth of analysis with geometries, capacity limits, history and more


## Usage

### Python Notebook Client

There is an example Python notebook outlining using the Data Platform as a data analysis tool.
It shows an how an analysis workflow would use the generated python library code. To run it,
ensure first that the Data Platform Server is running on `localhost:50051`
(see [Getting Started](#getting-started)); and that colocated with the script are the latest
generated Python bindings (see [Generating Code](#generating-code)). Then use
[uvx](https://docs.astral.sh/uv/reference/cli/#uv-tool-run) to run the notebook:

```bash
$ make gen-proto-python && cp -r gen/python/* examples/python-notebook/
$ cd examples/python-notebook && uvx --with="marimo" marimo edit --headless example.py 
```

### Typescript Frontend Client

TODO!


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

### External schema

The Data Platform defines a strongly typed _data contract_ as its external interface. This is the
API that any external clients have to use to interact with the platform. The schema for this is
defined via Protocol Buffers in `proto/ocf/dp`.

Boilerplate code for client and server implementations is generated in the required language from
these `.proto` files using the `protoc` compiler.

> [!Note]
> This is a direct analogue to the Pydantic models used in the old `datamodel` project.

Changes to the schema modifies the data contract, and will require client and server
implementations to regenerate their bindings and update their code. As such they should be made
with purpose and care.


### Database schema

The Data Platform uses a PostgreSQL database to store its data. The schema for this database is
defined in PostgreSQL's native SQL dialect in the `internal/database/postgres/sql/migrations`
directory, and access functions to the data are defined in
`internal/database/postgres/sql/queries`.

Boilerplate code for using these queries is generated using the `sqlc` tool. This generated code
provides a strongly typed interface to the database.

> [!Note]
> This is a direct analogue to the SQLAlchemy models used in the old `datamodel` project.

Having the queries defined in SQL allows for more efficient interaction with the database,
as they can be written to take advantage of the design of the database's features and be written
to be optimal with regards to its indexes.

These changes can be made without having to update the data contract, and so will not require
updates to clients using the Data Platform.


### Server

The Database Schema is mapped to the External Schema by implementing the server interface generated
from the Data Contract. This is done in `internal/database/serverimpl.go`. It isn't much more than a
conversion layer, with the business logic shared between the implemented functions and the SQL
queries.



## Development

### Getting Started

This project requires the [Go Toolchain](https://go.dev/doc/install) to be installed.
Clone the repository, then run

```bash
$ make init
```

This will fetch the dependencies, and install the git hooks required for development.

> [!Important]
> Since this project is uses lots of generated code, these hooks are vital to keep this generated
> code up to date, and as such running `make init` is a vital step towards a smooth development
> experience.

The server can then be run locally using

```bash
$ go run cmd/main.go
```

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

This will populate the `internal/database/postgres/gen` directory with language-specific bindings
for implementations of server and client code. Next, update the `serverimpl.go` file for the given
database to use the newly generated code, and ensure the test suite passes. Since the Data Platform
container automatically migrates the database on startup, simply re-deploying the container will
propagate the changes to your deployment environment.


In order to change the *Data Contract*, you will need to modify the `.proto` files in the `proto`
directory. Language specific bindigs are generated as part of the CI pipeline, but can be generated
manually

```bash
$ make gen-ext
``` 

The resultant server/client code can be copied from the `gen` directory to the relevant client
projects for development/testing.


## Further Comparisons

<details><summary>Complexity analysis of Data Platform vs old datamodels & metrics (scc)</summary>

```
Data Platform:
───────────────────────────────────────────────────────────────────────────────
Language                 Files     Lines   Blanks  Comments     Code Complexity
───────────────────────────────────────────────────────────────────────────────
SQL                          7      1288       75       566      647          6
Go                           5      2353      222       191     1940        253
Shell                        4       108       11        17       80         11
YAML                         4       224       34         2      188          0
Protocol Buffers             2       418       82        79      257          0
Makefile                     1        88       20         3       65          7
Markdown                     1       143       33         0      110          0
───────────────────────────────────────────────────────────────────────────────
Total                       24      4622      477       858     3287        277
───────────────────────────────────────────────────────────────────────────────
Estimated Cost to Develop (organic) $94,239
Estimated Schedule Effort (organic) 5.61 months
Estimated People Required (organic) 1.49
───────────────────────────────────────────────────────────────────────────────
Processed 11480397 bytes, 11.480 megabytes (SI)
───────────────────────────────────────────────────────────────────────────────

Datamodels & Metrics:
───────────────────────────────────────────────────────────────────────────────
Language                 Files     Lines   Blanks  Comments     Code Complexity
───────────────────────────────────────────────────────────────────────────────
Python                     190     23776     3213      3119    17444        508
YAML                         9       294       30        10      254          0
Markdown                     6       825      222         0      603          0
CSV                          3       978        0         0      978          0
Mako                         3        74       21         0       53          0
TOML                         3       196       28        20      148          2
Dockerfile                   2        56       18        12       26          2
INI                          2       213       46        99       68          0
Plain Text                   2        12        0         0       12          0
Autoconf                     1         1        0         0        1          0
License                      1        21        4         0       17          0
Makefile                     1        23        4         5       14          0
───────────────────────────────────────────────────────────────────────────────
Total                      223     26469     3586      3265    19618        512
───────────────────────────────────────────────────────────────────────────────
Estimated Cost to Develop (organic) $615,010
Estimated Schedule Effort (organic) 11.43 months
Estimated People Required (organic) 4.78
───────────────────────────────────────────────────────────────────────────────
Processed 909596 bytes, 0.910 megabytes (SI)
───────────────────────────────────────────────────────────────────────────────
```

(Produced via `$ scc --exclude-dir=".git,examples,proto/buf,proto/google"`)

</details>

