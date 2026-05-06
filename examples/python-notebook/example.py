# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "dp_sdk",
# #   "dp_sdk @ ${PROJECT_ROOT}/gen/python", # for local testing
#     "grpclib==0.4.8",
#     "xarray==2025.7.1",
# ]
# [tool.uv.sources]
# dp-sdk = { url = "https://github.com/openclimatefix/data-platform/releases/download/v0.30.0/dp_sdk-0.30.0-py3-none-any.whl" }
# ///
"""Example script for pulling data from the data platform.

Run with `$ uv run example.py`
"""

import asyncio
import grpc.aio
from google.protobuf.json_format import MessageToDict
from ocf.dp.dp import common_pb2
from ocf.dp.dp_data import messages_pb2, service_pb2_grpc
import pandas as pd
import xarray as xr
import datetime as dt


async def main() -> None:
    # Wrap in a try/finally block so the channel is cleaned up even if something goes wrong
    try:
        print(":: Creating a client")
        # Ensure you have a connection to a data platform instance on port 50051 before starting the script
        channel = grpc.aio.insecure_channel("localhost:50051")
        dpc = service_pb2_grpc.DataPlatformDataServiceStub(channel)
        
        print(":: Getting a UI-style forecast for the UK")
        print("\tThis is a composite timeseries that can be made up of values from many different forecasts.")

        time_window = messages_pb2.TimeWindow(
            start_timestamp_utc=dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=7),
            end_timestamp_utc=dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=5),
        )

        print(":: -> Getting the UK national location")
        glreq = messages_pb2.ListLocationsRequest(
            location_names_filter=["uk"],
            energy_source_filter=common_pb2.EnergySource.ENERGY_SOURCE_SOLAR,
        )
        glresp = await dpc.ListLocations(glreq)
        print(f":: -> {len(glresp.locations)} available locations")
        uk_location = glresp.locations[0]
        print(f"\t{uk_location.effective_capacity_watts=}")

        print(":: -> Getting GSP locations")
        glreq = messages_pb2.ListLocationsRequest(
            location_type_filter=common_pb2.LocationType.LOCATION_TYPE_GSP,
            energy_source_filter=common_pb2.EnergySource.ENERGY_SOURCE_SOLAR,
        )
        glresp = await dpc.ListLocations(glreq)
        gsp_locations = glresp.locations
        print(f"\t{len(gsp_locations)} available GSP locations")

        print(":: -> Listing available forecasters called 'blend'")
        lfresp = await dpc.ListForecasters(messages_pb2.ListForecastersRequest(latest_versions_only=True))
        blend_forecaster = next(f for f in lfresp.forecasters if "blend" in f.forecaster_name)
        print(f"\t{blend_forecaster}")

        print(":: -> Getting a forecast for the UK national location")
        gfreq = messages_pb2.GetForecastAsTimeseriesRequest(
            location_uuid=uk_location.location_uuid,
            energy_source=common_pb2.EnergySource.ENERGY_SOURCE_SOLAR,
            forecaster=blend_forecaster,
            time_window=time_window,
            horizon_mins=0,
        )
        gfreq_response = await dpc.GetForecastAsTimeseries(gfreq)
        start_time = gfreq_response.values[0].target_timestamp_utc.ToDatetime(tzinfo=dt.UTC)
        end_time = gfreq_response.values[-1].target_timestamp_utc.ToDatetime(tzinfo=dt.UTC)
        print(f"\tReceived {len(gfreq_response.values)} forecast points from {start_time} to {end_time}")

        print(f":: -> Converting response to a dataframe")
        # preserving_proto_field_name prevents conversion to lowerCamelCase.
        # always_print_fields_with_no_presence ensures all fields are present in the dict, even if they have no value in the protobuf.
        df = pd.DataFrame.from_dict([
            MessageToDict(v, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)
            for v in gfreq_response.values
        ])
        df = df.pipe(
            lambda df: df.join(pd.json_normalize(df['other_statistics_fractions']))
        ).drop("other_statistics_fractions", axis=1).set_index(
            "target_timestamp_utc"
        ).assign(
            p50_watts=lambda df: (df['p50_value_fraction'] * uk_location.effective_capacity_watts).astype(int),
            p10_watts=lambda df: (df['p10'] * uk_location.effective_capacity_watts).astype(int),
            p90_watts=lambda df: (df['p90'] * uk_location.effective_capacity_watts).astype(int),
        ).drop(["p50_value_fraction", "p10", "p90"], axis=1)
        print(df.head())

        print(f":: Getting 'ground truths' for the same location and time period")

        print(f":: -> Getting an observer")
        loresp = await dpc.ListObservers(messages_pb2.ListObserversRequest())
        observer = next(o for o in loresp.observers if "pvlive" in o.observer_name)
        print(f"\t{observer.observer_name=}")

        print(f":: -> Getting the ground truth for the UK national location")
        gtreq = messages_pb2.GetObservationsAsTimeseriesRequest(
            location_uuid=uk_location.location_uuid,
            energy_source=common_pb2.EnergySource.ENERGY_SOURCE_SOLAR,
            observer_name=observer.observer_name,
            time_window=time_window,
        )
        gtresp = await dpc.GetObservationsAsTimeseries(gtreq)
        start_time = gtresp.values[0].timestamp_utc.ToDatetime(tzinfo=dt.UTC)
        end_time = gtresp.values[-1].timestamp_utc.ToDatetime(tzinfo=dt.UTC)
        print(f"\tReceived {len(gtresp.values)} ground truth points from {start_time} to {end_time}")

        print(":: Getting ALL forecasts for a given location for multiple forecasters and time periods")
        print("   These forecasts are not composite, so will overlap in time.")

        print(":: -> Streaming forecast values")
        sdreq = messages_pb2.StreamForecastDataRequest(
            location_uuids=[uk_location.location_uuid],
            energy_source=common_pb2.EnergySource.ENERGY_SOURCE_SOLAR,
            forecasters=[f for f in lfresp.forecasters if "blend" in f.forecaster_name][:2],
            time_window=messages_pb2.StreamForecastDataRequest.TimeWindow(
                start_timestamp_utc=time_window.start_timestamp_utc,
                end_timestamp_utc=time_window.end_timestamp_utc,
            ),
            include_metadata=True,
        )
        forecast_values = []
        async for chunk in dpc.StreamForecastData(sdreq):
            forecast_values.extend(chunk.values)
        print(f"\tReceived {len(forecast_values)} forecast points for {len(sdreq.forecasters)} forecasters")

        print(":: -> Converting values to a dataframe")
        df = pd.DataFrame.from_dict([
            MessageToDict(f, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)
            for f in forecast_values
        ]).pipe(
            lambda df: df.join(pd.json_normalize(df['other_statistics_fractions']))
        ).drop("other_statistics_fractions", axis=1).set_index(
            ["location_uuid", "forecaster_fullname", "init_timestamp", "horizon_mins"]
        )
        print(df.head())

        print(":: That's it!")
        print("   Remeber: When using protobuf's `MessageToDict()` function,") 
        print("    ALWAYS set `always_print_fields_with_no_presence=True`")


    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
