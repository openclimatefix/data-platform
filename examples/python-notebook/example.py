# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "dp_sdk",
# #   "dp_sdk @ ${PROJECT_ROOT}/gen/python", # for local testing
#     "grpclib==0.4.8",
#     "xarray==2025.7.1",
# ]
# [tool.uv.sources]
# dp-sdk = { url = "https://github.com/openclimatefix/data-platform/releases/download/v0.29.0/dp_sdk-0.29.0-py3-none-any.whl" }
# ///
"""Example script for pulling data from the data platform.

Run with `$ uv run example.py`
"""

from grpclib.client import Channel
import betterproto
import asyncio
from ocf import dp
import pandas as pd
import xarray as xr
import datetime as dt


async def main() -> None:
    # Wrap in a try/finally block so the channel is cleaned up even if something goes wrong
    try:
        print(":: Creating a client")
        # Ensure you have a connection to a data platform instance on port 50051 before starting the script
        channel = Channel(host="localhost", port=50051)
        dpc = dp.DataPlatformDataServiceStub(channel)
        
        print(":: Getting a UI-style forecast for the UK")
        print("\tThis is a composite timeseries that can be made up of values from many different forecasts.")

        time_window = dp.TimeWindow(
            start_timestamp_utc=dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=7),
            end_timestamp_utc=dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=5),
        )

        print(":: -> Getting the UK national location")
        glreq = dp.ListLocationsRequest(
            location_names_filter=["uk"],
            energy_source_filter=dp.EnergySource.SOLAR,
        )
        glresp = await dpc.list_locations(glreq)
        print(f":: -> {len(glresp)} available locations")
        uk_location = glresp.locations[0]
        print(f"\t{uk_location.effective_capacity_watts=}")

        print(":: -> Getting GSP locations")
        glreq = dp.ListLocationsRequest(
            location_type_filter=dp.LocationType.GSP,
            energy_source_filter=dp.EnergySource.SOLAR,
        )
        glresp = await dpc.list_locations(glreq)
        gsp_locations = glresp.locations
        print(f"\t{len(gsp_locations)} available GSP locations")

        print(":: -> Listing available forecasters called 'blend'")
        lfresp = await dpc.list_forecasters(dp.ListForecastersRequest(latest_versions_only=True))
        blend_forecaster = next(f for f in lfresp.forecasters if "blend" in f.forecaster_name)
        print(f"\t{blend_forecaster}")

        print(":: -> Getting a forecast for the UK national location")
        gfreq = dp.GetForecastAsTimeseriesRequest(
            location_uuid=uk_location.location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            forecaster=blend_forecaster,
            time_window=time_window,
            horizon_mins=0,
        )
        gfreq_response = await dpc.get_forecast_as_timeseries(gfreq)
        start_time = gfreq_response.values[0].target_timestamp_utc
        end_time = gfreq_response.values[-1].target_timestamp_utc
        print(f"\tReceived {len(gfreq_response.values)} forecast points from {start_time} to {end_time}")

        print(f":: -> Converting response to a dataframe")
        df = pd.DataFrame.from_dict([
            v.to_dict(include_default_values=True, casing=betterproto.Casing.SNAKE)
            for v in gfreq_response.values
        ]).pipe(
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
        loresp = await dpc.list_observers(dp.ListObserversRequest())
        observer = next(o for o in loresp.observers if "pvlive" in o.observer_name)
        print(f"\t{observer.observer_name=}")

        print(f":: -> Getting the ground truth for the UK national location")
        gtreq = dp.GetObservationsAsTimeseriesRequest(
            location_uuid=uk_location.location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            observer_name=observer.observer_name,
            time_window=time_window,
        )
        gtresp = await dpc.get_observations_as_timeseries(gtreq)
        start_time = gtresp.values[0].timestamp_utc
        end_time = gtresp.values[-1].timestamp_utc
        print(f"\tReceived {len(gtresp.values)} ground truth points from {start_time} to {end_time}")

        print(":: Getting ALL forecasts for a given location for multiple forecasters and time periods")
        print("   These forecasts are not composite, so will overlap in time.")

        print(":: -> Streaming forecast values")
        sdreq = dp.StreamForecastDataRequest(
            location_uuids=[uk_location.location_uuid],
            energy_source=dp.EnergySource.SOLAR,
            forecasters=[f for f in lfresp.forecasters if "blend" in f.forecaster_name][:2],
            time_window=dp.StreamForecastDataRequestTimeWindow(
                start_timestamp_utc=time_window.start_timestamp_utc,
                end_timestamp_utc=time_window.end_timestamp_utc,
            ),
            include_metadata=True,
        )
        forecast_values = []
        async for chunk in dpc.stream_forecast_data(sdreq):
            forecast_values.append(chunk)
        print(f"\tReceived {len(forecast_values)} forecast points for {len(sdreq.forecasters)} forecasters")

        print(":: -> Converting values to a dataframe")
        df = pd.DataFrame.from_dict([
            f.to_dict(include_default_values=True, casing=betterproto.Casing.SNAKE)
            for f in forecast_values
        ]).pipe(
            lambda df: df.join(pd.json_normalize(df['other_statistics_fractions']))
        ).drop("other_statistics_fractions", axis=1).set_index(
            ["location_uuid", "forecaster_fullname", "init_timestamp", "horizon_mins"]
        )
        print(df.head())

        print(":: That's it!")
        print("   Remeber: When using betterproto's `to_dict()` function,") 
        print("    ALWAYS set `include_default_values=True`")


    finally:
        channel.close()


if __name__ == "__main__":
    asyncio.run(main())
