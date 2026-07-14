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

<!-- DOCS START -->

## GRPC API Documentation
<details><summary>Messages (ocf/dp/dp-data.messages.proto)</summary>

<a name="ocf-dp-CreateForecastRequest"></a>
<details><summary>CreateForecastRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  || location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || init_time_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || values | [CreateForecastRequest.ForecastValue](#ocf-dp-CreateForecastRequest-ForecastValue) | repeated |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) | optional |  || created_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The UTC time to set as the created_timestamp for the forecast. Leave empty to use current time. This is useful for backfilling historical forecasts with accurate created timestamps, but should generally be left empty for new forecasts. |
</details><a name="ocf-dp-CreateForecastRequest-ForecastValue"></a>
<details><summary>CreateForecastRequest.ForecastValue</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| horizon_mins | [uint32](#uint32) |  |  || p50_fraction | [float](#float) |  |  || other_statistics_fractions | [CreateForecastRequest.ForecastValue.OtherStatisticsFractionsEntry](#ocf-dp-CreateForecastRequest-ForecastValue-OtherStatisticsFractionsEntry) | repeated | Struct for storing additional statistics like p10, p90, mean etc. |
</details><a name="ocf-dp-CreateForecastRequest-ForecastValue-OtherStatisticsFractionsEntry"></a>
<details><summary>CreateForecastRequest.ForecastValue.OtherStatisticsFractionsEntry</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  || value | [float](#float) |  |  |
</details><a name="ocf-dp-CreateForecastResponse"></a>
<details><summary>CreateForecastResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecast_uuid | [string](#string) |  |  |
</details><a name="ocf-dp-CreateForecasterRequest"></a>
<details><summary>CreateForecasterRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  || version | [string](#string) |  |  |
</details><a name="ocf-dp-CreateForecasterResponse"></a>
<details><summary>CreateForecasterResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  |
</details><a name="ocf-dp-CreateLocationRequest"></a>
<details><summary>CreateLocationRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_name | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || geometry_wkt | [string](#string) |  | A geometry string in Well-Known-Text (WKT) format. Geometry type must be POINT, POLYGON, or MULTIPOLYGON, must have 2 dimensions, and must be in the EPSG:4326 coordinate system (longitude/latitude). || effective_capacity_watts | [uint64](#uint64) |  | The effective capacity of the location in watts. This refers to the useable capacity for generation, not the installed capacity. If tracking of installed capacity is required, this should be stored in metadata. || location_type | [LocationType](#ocf-dp-LocationType) |  |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) | optional |  || valid_from_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The UTC time from which this location is considered valid. Leave empty to use current time. || associated_latlng | [LatLng](#ocf-dp-LatLng) | optional | Optional latitude/longitude to associate with the location. Defaults to the centroid of the geometry if not provided. Not required for Point geometries. |
</details><a name="ocf-dp-CreateLocationResponse"></a>
<details><summary>CreateLocationResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || location_name | [string](#string) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  |
</details><a name="ocf-dp-CreateObservationsRequest"></a>
<details><summary>CreateObservationsRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || observer_name | [string](#string) |  |  || values | [CreateObservationsRequest.Value](#ocf-dp-CreateObservationsRequest-Value) | repeated |  |
</details><a name="ocf-dp-CreateObservationsRequest-Value"></a>
<details><summary>CreateObservationsRequest.Value</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || value_watts | [uint64](#uint64) |  |  |
</details><a name="ocf-dp-CreateObservationsResponse"></a>
<details><summary>CreateObservationsResponse</summary>



</details><a name="ocf-dp-CreateObserverRequest"></a>
<details><summary>CreateObserverRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
</details><a name="ocf-dp-CreateObserverResponse"></a>
<details><summary>CreateObserverResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| observer_uuid | [string](#string) |  |  || observer_name | [string](#string) |  |  |
</details><a name="ocf-dp-DeleteForecastRequest"></a>
<details><summary>DeleteForecastRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  || init_time_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
</details><a name="ocf-dp-DeleteForecastResponse"></a>
<details><summary>DeleteForecastResponse</summary>



</details><a name="ocf-dp-ForecastDatum"></a>
<details><summary>ForecastDatum</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| init_timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || location_uuid | [string](#string) |  |  || forecaster_fullname | [string](#string) |  |  || horizon_mins | [uint32](#uint32) |  |  || p50_fraction | [float](#float) |  |  || other_statistics_fractions | [ForecastDatum.OtherStatisticsFractionsEntry](#ocf-dp-ForecastDatum-OtherStatisticsFractionsEntry) | repeated |  || created_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  || metadata | [ForecastDatum.MetadataEntry](#ocf-dp-ForecastDatum-MetadataEntry) | repeated |  || target_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
</details><a name="ocf-dp-ForecastDatum-MetadataEntry"></a>
<details><summary>ForecastDatum.MetadataEntry</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  || value | [string](#string) |  |  |
</details><a name="ocf-dp-ForecastDatum-OtherStatisticsFractionsEntry"></a>
<details><summary>ForecastDatum.OtherStatisticsFractionsEntry</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  || value | [float](#float) |  |  |
</details><a name="ocf-dp-Forecaster"></a>
<details><summary>Forecaster</summary>
Forecaster represents a generative source of predicted values.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecaster_name | [string](#string) |  |  || forecaster_version | [string](#string) |  | The version of the forecaster to use. If not specified, the latest version will be used. |
</details><a name="ocf-dp-GetForecastAsTimeseriesRequest"></a>
<details><summary>GetForecastAsTimeseriesRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || horizon_mins | [uint32](#uint32) |  |  || time_window | [TimeWindow](#ocf-dp-TimeWindow) |  |  || forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  || pivot_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The time to search backwards from to find forecasts. If not specified, the current time will be used. Forecasts created after this time are not included. || initialization_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | An individual init time to filter forecasts by. If specified, only forecasts with this init time will be returned. This enables fetching data from a single, specific forecast run. |
</details><a name="ocf-dp-GetForecastAsTimeseriesResponse"></a>
<details><summary>GetForecastAsTimeseriesResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || location_name | [string](#string) |  |  || values | [GetForecastAsTimeseriesResponse.Value](#ocf-dp-GetForecastAsTimeseriesResponse-Value) | repeated |  |
</details><a name="ocf-dp-GetForecastAsTimeseriesResponse-Value"></a>
<details><summary>GetForecastAsTimeseriesResponse.Value</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || p50_value_fraction | [float](#float) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  || initialization_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || created_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || other_statistics_fractions | [GetForecastAsTimeseriesResponse.Value.OtherStatisticsFractionsEntry](#ocf-dp-GetForecastAsTimeseriesResponse-Value-OtherStatisticsFractionsEntry) | repeated |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
</details><a name="ocf-dp-GetForecastAsTimeseriesResponse-Value-OtherStatisticsFractionsEntry"></a>
<details><summary>GetForecastAsTimeseriesResponse.Value.OtherStatisticsFractionsEntry</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  || value | [float](#float) |  |  |
</details><a name="ocf-dp-GetForecastAtTimestampRequest"></a>
<details><summary>GetForecastAtTimestampRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuids | [string](#string) | repeated |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The time to fetch predicted yields for. If not specified, the current time will be used. || forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  |
</details><a name="ocf-dp-GetForecastAtTimestampResponse"></a>
<details><summary>GetForecastAtTimestampResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || values | [GetForecastAtTimestampResponse.Value](#ocf-dp-GetForecastAtTimestampResponse-Value) | repeated |  |
</details><a name="ocf-dp-GetForecastAtTimestampResponse-Value"></a>
<details><summary>GetForecastAtTimestampResponse.Value</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || location_name | [string](#string) |  |  || value_fraction | [float](#float) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  || latlng | [LatLng](#ocf-dp-LatLng) |  |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  || initialization_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || created_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || other_statistics_fractions | [GetForecastAtTimestampResponse.Value.OtherStatisticsFractionsEntry](#ocf-dp-GetForecastAtTimestampResponse-Value-OtherStatisticsFractionsEntry) | repeated |  |
</details><a name="ocf-dp-GetForecastAtTimestampResponse-Value-OtherStatisticsFractionsEntry"></a>
<details><summary>GetForecastAtTimestampResponse.Value.OtherStatisticsFractionsEntry</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  || value | [float](#float) |  |  |
</details><a name="ocf-dp-GetLatestForecastsRequest"></a>
<details><summary>GetLatestForecastsRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || pivot_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The time to search backwards from to find the 'latest' forecast. If not specified, the current time will be used. |
</details><a name="ocf-dp-GetLatestForecastsResponse"></a>
<details><summary>GetLatestForecastsResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecasts | [GetLatestForecastsResponse.Forecast](#ocf-dp-GetLatestForecastsResponse-Forecast) | repeated |  |
</details><a name="ocf-dp-GetLatestForecastsResponse-Forecast"></a>
<details><summary>GetLatestForecastsResponse.Forecast</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| initialization_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || created_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  || location_uuid | [string](#string) |  |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
</details><a name="ocf-dp-GetLatestObservationsRequest"></a>
<details><summary>GetLatestObservationsRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuids | [string](#string) | repeated |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || observer_name | [string](#string) |  |  || pivot_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The time to search backwards from to find the 'latest' observation. If not specified, the current time will be used. |
</details><a name="ocf-dp-GetLatestObservationsResponse"></a>
<details><summary>GetLatestObservationsResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| observations | [GetLatestObservationsResponse.Observation](#ocf-dp-GetLatestObservationsResponse-Observation) | repeated |  |
</details><a name="ocf-dp-GetLatestObservationsResponse-Observation"></a>
<details><summary>GetLatestObservationsResponse.Observation</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || value_fraction | [float](#float) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  |
</details><a name="ocf-dp-GetLocationAsTimeseriesRequest"></a>
<details><summary>GetLocationAsTimeseriesRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || time_window | [TimeWindow](#ocf-dp-TimeWindow) |  |  |
</details><a name="ocf-dp-GetLocationAsTimeseriesResponse"></a>
<details><summary>GetLocationAsTimeseriesResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | [GetLocationAsTimeseriesResponse.LocationSnapshot](#ocf-dp-GetLocationAsTimeseriesResponse-LocationSnapshot) | repeated |  |
</details><a name="ocf-dp-GetLocationAsTimeseriesResponse-LocationSnapshot"></a>
<details><summary>GetLocationAsTimeseriesResponse.LocationSnapshot</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
</details><a name="ocf-dp-GetLocationRequest"></a>
<details><summary>GetLocationRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || include_geometry | [bool](#bool) |  | If true, the geometry_wkb field will be included in the response. This may be very big, so only include if necessary. || pivot_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The UTC time the data should be valid for. Leave empty to use current time. |
</details><a name="ocf-dp-GetLocationResponse"></a>
<details><summary>GetLocationResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || location_name | [string](#string) |  |  || latlng | [LatLng](#ocf-dp-LatLng) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  || geometry_wkb | [bytes](#bytes) | optional |  |
</details><a name="ocf-dp-GetLocationsAsGeoJSONRequest"></a>
<details><summary>GetLocationsAsGeoJSONRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuids | [string](#string) | repeated |  || unsimplified | [bool](#bool) |  | If true, the GeoJSON will not be simplified. Defaults to false if not set to reduce response size. |
</details><a name="ocf-dp-GetLocationsAsGeoJSONResponse"></a>
<details><summary>GetLocationsAsGeoJSONResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| geojson | [string](#string) |  |  |
</details><a name="ocf-dp-GetObservationsAsTimeseriesRequest"></a>
<details><summary>GetObservationsAsTimeseriesRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || observer_name | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || time_window | [TimeWindow](#ocf-dp-TimeWindow) |  |  |
</details><a name="ocf-dp-GetObservationsAsTimeseriesResponse"></a>
<details><summary>GetObservationsAsTimeseriesResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || values | [GetObservationsAsTimeseriesResponse.Value](#ocf-dp-GetObservationsAsTimeseriesResponse-Value) | repeated |  |
</details><a name="ocf-dp-GetObservationsAsTimeseriesResponse-Value"></a>
<details><summary>GetObservationsAsTimeseriesResponse.Value</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || value_fraction | [float](#float) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  |
</details><a name="ocf-dp-GetObservationsAtTimestampRequest"></a>
<details><summary>GetObservationsAtTimestampRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuids | [string](#string) | repeated |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || observer_name | [string](#string) |  |  || timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The time to fetch observations for. If not specified, the current time will be used. |
</details><a name="ocf-dp-GetObservationsAtTimestampResponse"></a>
<details><summary>GetObservationsAtTimestampResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  || values | [GetObservationsAtTimestampResponse.Value](#ocf-dp-GetObservationsAtTimestampResponse-Value) | repeated |  |
</details><a name="ocf-dp-GetObservationsAtTimestampResponse-Value"></a>
<details><summary>GetObservationsAtTimestampResponse.Value</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || value_fraction | [float](#float) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  || latlng | [LatLng](#ocf-dp-LatLng) |  |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
</details><a name="ocf-dp-GetWeekAverageDeltasRequest"></a>
<details><summary>GetWeekAverageDeltasRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || pivot_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | The characteristic time to detrmine averages for. The time component specifies the initialization time, and the date component is to define the end of the seven-day period over which to average. || forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  || observer_name | [string](#string) |  |  |
</details><a name="ocf-dp-GetWeekAverageDeltasResponse"></a>
<details><summary>GetWeekAverageDeltasResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deltas | [GetWeekAverageDeltasResponse.AverageDelta](#ocf-dp-GetWeekAverageDeltasResponse-AverageDelta) | repeated |  || init_time_of_day | [string](#string) |  | The initialisation time that was compared across the week. Formatted as HH:MM, e.g. "12:00" |
</details><a name="ocf-dp-GetWeekAverageDeltasResponse-AverageDelta"></a>
<details><summary>GetWeekAverageDeltasResponse.AverageDelta</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| horizon_mins | [uint32](#uint32) |  |  || delta_fraction | [float](#float) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  |
</details><a name="ocf-dp-ListForecastersRequest"></a>
<details><summary>ListForecastersRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecaster_names_filter | [string](#string) | repeated | Optional filter to only return forecasters from a given set. If empty, all forecasters will be returned. || latest_versions_only | [bool](#bool) |  | If true, only the latest version of each forecaster will be returned. |
</details><a name="ocf-dp-ListForecastersResponse"></a>
<details><summary>ListForecastersResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecasters | [Forecaster](#ocf-dp-Forecaster) | repeated |  |
</details><a name="ocf-dp-ListLocationsRequest"></a>
<details><summary>ListLocationsRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| energy_source_filter | [EnergySource](#ocf-dp-EnergySource) | optional | Optional filter to only return locations of a specific energy source. || location_type_filter | [LocationType](#ocf-dp-LocationType) | optional | Optional filter to only return locations of a specific location type. || location_uuids_filter | [string](#string) | repeated | Optional filter to only return locations from a given set. || organisation_id_filter | [string](#string) | optional | Optional filter to only return locations belonging to a specific organisation. || enclosing_location_uuid_filter | [string](#string) | optional | Optional filter to only return locations enclosed within a specific location. || enclosed_location_uuid_filter | [string](#string) | optional | Optional filter to only return locations that enclose a specific location. || location_names_filter | [string](#string) | repeated | Optional filter to only return locations with a specific name. |
</details><a name="ocf-dp-ListLocationsResponse"></a>
<details><summary>ListLocationsResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| locations | [ListLocationsResponse.LocationSummary](#ocf-dp-ListLocationsResponse-LocationSummary) | repeated |  |
</details><a name="ocf-dp-ListLocationsResponse-LocationSummary"></a>
<details><summary>ListLocationsResponse.LocationSummary</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || location_name | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || location_type | [LocationType](#ocf-dp-LocationType) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  || latlng | [LatLng](#ocf-dp-LatLng) |  |  || metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
</details><a name="ocf-dp-ListObserversRequest"></a>
<details><summary>ListObserversRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| observer_names_filter | [string](#string) | repeated | Optional filter to only return observers from a given set. If empty, all observers will be returned. |
</details><a name="ocf-dp-ListObserversResponse"></a>
<details><summary>ListObserversResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| observers | [ListObserversResponse.ObserverSummary](#ocf-dp-ListObserversResponse-ObserverSummary) | repeated |  |
</details><a name="ocf-dp-ListObserversResponse-ObserverSummary"></a>
<details><summary>ListObserversResponse.ObserverSummary</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| observer_uuid | [string](#string) |  |  || observer_name | [string](#string) |  |  |
</details><a name="ocf-dp-StreamCreateForecastsResponse"></a>
<details><summary>StreamCreateForecastsResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecast_uuids | [string](#string) | repeated | A list of the UUIDs generated for the successfully created forecasts. |
</details><a name="ocf-dp-StreamForecastDataRequest"></a>
<details><summary>StreamForecastDataRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuids | [string](#string) | repeated |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || time_window | [TimeWindow](#ocf-dp-TimeWindow) |  |  || forecasters | [Forecaster](#ocf-dp-Forecaster) | repeated |  || include_metadata | [bool](#bool) |  |  |
</details><a name="ocf-dp-StreamForecastDataResponse"></a>
<details><summary>StreamForecastDataResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | [ForecastDatum](#ocf-dp-ForecastDatum) | repeated |  |
</details><a name="ocf-dp-TimeWindow"></a>
<details><summary>TimeWindow</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | The start of the time window, inclusive. Cannot be more than 7 days before end_timestamp_utc, nor more than 1 month in the future. || end_timestamp_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | The end of the time window, inclusive. Cannot be more than 7 days after start_timestamp_utc. |
</details><a name="ocf-dp-UpdateForecasterRequest"></a>
<details><summary>UpdateForecasterRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  || new_version | [string](#string) |  |  |
</details><a name="ocf-dp-UpdateForecasterResponse"></a>
<details><summary>UpdateForecasterResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| forecaster | [Forecaster](#ocf-dp-Forecaster) |  |  |
</details><a name="ocf-dp-UpdateLocationOwnerRequest"></a>
<details><summary>UpdateLocationOwnerRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || new_organisation_id | [string](#string) |  |  |
</details><a name="ocf-dp-UpdateLocationOwnerResponse"></a>
<details><summary>UpdateLocationOwnerResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || organisation_id | [string](#string) |  |  |
</details><a name="ocf-dp-UpdateLocationRequest"></a>
<details><summary>UpdateLocationRequest</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || energy_source | [EnergySource](#ocf-dp-EnergySource) |  |  || new_location_name | [string](#string) | optional | The new name for the location. || new_effective_capacity_watts | [uint64](#uint64) | optional |  || new_metadata | [google.protobuf.Struct](#google-protobuf-Struct) | optional | The new metadata object to set for the location. Note that this will replace any existing metadata, so be sure to include existing fields where needed. || valid_from_utc | [google.protobuf.Timestamp](#google-protobuf-Timestamp) | optional | The UTC time from which this name is considered valid. Leave empty to use current time. |
</details><a name="ocf-dp-UpdateLocationResponse"></a>
<details><summary>UpdateLocationResponse</summary>


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_uuid | [string](#string) |  |  || location_name | [string](#string) |  |  || effective_capacity_watts | [uint64](#uint64) |  |  |
</details>
</details>



<a name="ocf-dp-DataPlatformDataService"></a>

### DataPlatformDataService (ocf/dp/dp-data.service.proto)


<a name="GetForecastAsTimeseries"></a>

#### GetForecastAsTimeseries

GetForecastTimeseries fetches a 1-D horizontal slice of predicted data.
These values can either come from a sample of many forecasts; or from one specific forecast. 
In the case of the sample, values whose timestamps are shared across overlapping forecasts
are cherry-picked based on the lowest allowable lead time (horizon).

_[GetForecastAsTimeseriesRequest](#ocf-dp-GetForecastAsTimeseriesRequest) / [GetForecastAsTimeseriesResponse](#ocf-dp-GetForecastAsTimeseriesResponse)_

<a name="GetForecastAtTimestamp"></a>

#### GetForecastAtTimestamp

GetForecastAtTimestamp fetches a 1-D vertical slice of predicted data.
Useful for spatial snapshots at a given time, for instance to display on a map.

_[GetForecastAtTimestampRequest](#ocf-dp-GetForecastAtTimestampRequest) / [GetForecastAtTimestampResponse](#ocf-dp-GetForecastAtTimestampResponse)_

<a name="GetObservationsAsTimeseries"></a>

#### GetObservationsAsTimeseries

GetObservationsAsTimeseries fetches a 1-D horizontal slice of observed data.
It is the observations analogue of GetForecastAsTimeseries.

_[GetObservationsAsTimeseriesRequest](#ocf-dp-GetObservationsAsTimeseriesRequest) / [GetObservationsAsTimeseriesResponse](#ocf-dp-GetObservationsAsTimeseriesResponse)_

<a name="GetObservationsAtTimestamp"></a>

#### GetObservationsAtTimestamp

GetObservationAtTimestamp fetches a 1-D vertical slice of observation data.
It is the observations analogue of GetForecastsAtTimestamp.

_[GetObservationsAtTimestampRequest](#ocf-dp-GetObservationsAtTimestampRequest) / [GetObservationsAtTimestampResponse](#ocf-dp-GetObservationsAtTimestampResponse)_

<a name="GetLocation"></a>

#### GetLocation

GetLocation fetches a snapshot of information about a specific location at a point in time. 
It can also optionally return the geometry of the location.

_[GetLocationRequest](#ocf-dp-GetLocationRequest) / [GetLocationResponse](#ocf-dp-GetLocationResponse)_

<a name="GetLocationAsTimeseries"></a>

#### GetLocationAsTimeseries

GetLocationAsTimeseries fetches the history of a location across a given time window.

_[GetLocationAsTimeseriesRequest](#ocf-dp-GetLocationAsTimeseriesRequest) / [GetLocationAsTimeseriesResponse](#ocf-dp-GetLocationAsTimeseriesResponse)_

<a name="CreateLocation"></a>

#### CreateLocation

CreateLocation registers a new location in which to log or forecast generation.

_[CreateLocationRequest](#ocf-dp-CreateLocationRequest) / [CreateLocationResponse](#ocf-dp-CreateLocationResponse)_

<a name="UpdateLocation"></a>

#### UpdateLocation

UpdateLocation modifies various attributes associated with a given location.

_[UpdateLocationRequest](#ocf-dp-UpdateLocationRequest) / [UpdateLocationResponse](#ocf-dp-UpdateLocationResponse)_

<a name="UpdateLocationOwner"></a>

#### UpdateLocationOwner

UpdateLocationOwner changes the ownership of a location.

_[UpdateLocationOwnerRequest](#ocf-dp-UpdateLocationOwnerRequest) / [UpdateLocationOwnerResponse](#ocf-dp-UpdateLocationOwnerResponse)_

<a name="ListLocations"></a>

#### ListLocations

ListLocations fetches a list of registered locations that match the supplied filters.

_[ListLocationsRequest](#ocf-dp-ListLocationsRequest) / [ListLocationsResponse](#ocf-dp-ListLocationsResponse)_

<a name="CreateForecaster"></a>

#### CreateForecaster

CreateForecaster registers a new forecaster. 
A forecaster is a producer of predicted values. Forecasters are differentiated by their name and version.

_[CreateForecasterRequest](#ocf-dp-CreateForecasterRequest) / [CreateForecasterResponse](#ocf-dp-CreateForecasterResponse)_

<a name="UpdateForecaster"></a>

#### UpdateForecaster

UpdateForecaster modifies the version of an existing forecaster.

_[UpdateForecasterRequest](#ocf-dp-UpdateForecasterRequest) / [UpdateForecasterResponse](#ocf-dp-UpdateForecasterResponse)_

<a name="ListForecasters"></a>

#### ListForecasters

ListForecasters fetches a list of registered forecasters that match the supplied filters.

_[ListForecastersRequest](#ocf-dp-ListForecastersRequest) / [ListForecastersResponse](#ocf-dp-ListForecastersResponse)_

<a name="CreateForecast"></a>

#### CreateForecast

CreateForecast saves a timeseries of predicted values from a given forecaster.

_[CreateForecastRequest](#ocf-dp-CreateForecastRequest) / [CreateForecastResponse](#ocf-dp-CreateForecastResponse)_

<a name="GetLatestForecasts"></a>

#### GetLatestForecasts

GetLatestForecasts fetches metadata for the most recently produced forecasts.

_[GetLatestForecastsRequest](#ocf-dp-GetLatestForecastsRequest) / [GetLatestForecastsResponse](#ocf-dp-GetLatestForecastsResponse)_

<a name="DeleteForecast"></a>

#### DeleteForecast

DeleteForecast removes a series of forecast values from the database.

_[DeleteForecastRequest](#ocf-dp-DeleteForecastRequest) / [DeleteForecastResponse](#ocf-dp-DeleteForecastResponse)_

<a name="CreateObserver"></a>

#### CreateObserver

CreateObserver registers a new observer. 
An observer is a producer of observed, or measured, values.

_[CreateObserverRequest](#ocf-dp-CreateObserverRequest) / [CreateObserverResponse](#ocf-dp-CreateObserverResponse)_

<a name="ListObservers"></a>

#### ListObservers

ListObservers fetches a list of registered observers that match the supplied filters.

_[ListObserversRequest](#ocf-dp-ListObserversRequest) / [ListObserversResponse](#ocf-dp-ListObserversResponse)_

<a name="CreateObservations"></a>

#### CreateObservations

CreateObservations saves a timeseries of observed values from a given observer.

_[CreateObservationsRequest](#ocf-dp-CreateObservationsRequest) / [CreateObservationsResponse](#ocf-dp-CreateObservationsResponse)_

<a name="GetLatestObservations"></a>

#### GetLatestObservations

GetLatestObservation fetches the most recent observation for a given location and observer.

_[GetLatestObservationsRequest](#ocf-dp-GetLatestObservationsRequest) / [GetLatestObservationsResponse](#ocf-dp-GetLatestObservationsResponse)_

<a name="GetLocationsAsGeoJSON"></a>

#### GetLocationsAsGeoJSON

GetLocationsAsGeoJSON fetches a given set of locations as GeoJSON, suitable for display on a
map or for integration with GIS software.

_[GetLocationsAsGeoJSONRequest](#ocf-dp-GetLocationsAsGeoJSONRequest) / [GetLocationsAsGeoJSONResponse](#ocf-dp-GetLocationsAsGeoJSONResponse)_

<a name="GetWeekAverageDeltas"></a>

#### GetWeekAverageDeltas

GetWeekAverageDeltas fetches the average delta at the given init time over the past week.
This is useful for making adjustments based on recent performance.

_[GetWeekAverageDeltasRequest](#ocf-dp-GetWeekAverageDeltasRequest) / [GetWeekAverageDeltasResponse](#ocf-dp-GetWeekAverageDeltasResponse)_

<a name="StreamForecastData"></a>

#### StreamForecastData

StreamForecastData streams forecast data for a given location, forecasters, and time range.
Useful for analytics and performance monitoring.

_[StreamForecastDataRequest](#ocf-dp-StreamForecastDataRequest) / [StreamForecastDataResponse](#ocf-dp-StreamForecastDataResponse) stream_

<a name="StreamCreateForecasts"></a>

#### StreamCreateForecasts

StreamCreateForecasts allows for efficient batch creation of multiple forecasts and their values.
Note: This method is executed in a single transaction. To prevent resource exhaustion, a maximum of 5000 forecasts can be sent per stream. Exceeding this limit will abort the stream and roll back all inserts.

_[CreateForecastRequest](#ocf-dp-CreateForecastRequest) stream / [StreamCreateForecastsResponse](#ocf-dp-StreamCreateForecastsResponse)_




<details><summary>Messages (ocf/dp/dp.common.proto)</summary>

<a name="ocf-dp-LatLng"></a>
<details><summary>LatLng</summary>
LatLng represents a WSG84 coordinate pair.
Float precision enables a resolution of about 1cm,
which is more precise than we'll ever have data for.

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| latitude | [float](#float) |  |  || longitude | [float](#float) |  |  |
</details>
</details>

<details><summary>Enums (ocf/dp/dp.common.proto)</summary>

<a name="ocf-dp-EnergySource"></a>

<details><summary>EnergySource</summary>
EnergySource indicates the type of energy generation.
NOTE: These enum numbers are used to find the corresponding entry in the postgres database.
Do not change without considering this first!

| Name | Number | Description |
| ---- | ------ | ----------- |
| ENERGY_SOURCE_UNSPECIFIED | 0 |  || ENERGY_SOURCE_SOLAR | 1 |  || ENERGY_SOURCE_WIND | 2 |  |
</details><a name="ocf-dp-LocationType"></a>

<details><summary>LocationType</summary>
LocationType indicates the type of location.
NOTE: These enum numbers are used to find the corresponding entry in the postgres database.
Do not change without considering this first!
The values are spaced apart in order to allow for future expansion.

| Name | Number | Description |
| ---- | ------ | ----------- |
| LOCATION_TYPE_UNSPECIFIED | 0 |  || LOCATION_TYPE_SITE | 1 |  || LOCATION_TYPE_GSP | 2 |  || LOCATION_TYPE_DNO | 3 |  || LOCATION_TYPE_NATION | 4 |  || LOCATION_TYPE_STATE | 5 |  || LOCATION_TYPE_COUNTY | 6 |  || LOCATION_TYPE_CITY | 7 |  || LOCATION_TYPE_PRIMARY_SUBSTATION | 8 |  |
</details><a name="ocf-dp-Permission"></a>

<details><summary>Permission</summary>
Permission indicates the level of access a user has to a resource.
NOTE: These enum numbers are used to find the corresponding entry in the postgres database.
Do not change without considering this first!

| Name | Number | Description |
| ---- | ------ | ----------- |
| PERMISSION_UNSPECIFIED | 0 |  || PERMISSION_READ | 1 |  || PERMISSION_WRITE | 2 |  |
</details>
</details>





<!-- DOCS END -->
