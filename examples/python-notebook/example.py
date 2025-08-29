# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "betterproto==2.0.0b7",
#     "marimo",
#     "grpclib==0.4.8",
#     "pandas==2.3.1",
#     "matplotlib==3.10.3",
#     "vega-datasets==0.9.0",
#     "xarray==2025.7.1",
# ]
# ///

import marimo

__generated_with = "0.15.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import betterproto.grpc
    from grpclib.client import Channel
    import datetime as dt
    from ocf import dp
    import xarray as xr
    import json
    import pandas as pd
    import matplotlib.pyplot as plt
    import marimo as mo
    import altair as alt
    from vega_datasets import data
    import uuid
    return Channel, alt, betterproto, data, dp, dt, mo, pd, uuid


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Connecting to the data platform

    The data platform's generated code includes a stub that is used to initialize a client.
    """
    )
    return


@app.cell
def _(Channel, dp):
    channel = Channel(host="localhost", port=50051)
    client = dp.DataPlatformServiceStub(channel)
    return channel, client


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Get predicted generation data for a given time window and horizon

    This corresponds to the main graph that is shown on the frontend when a location is selected.
    """
    )
    return


@app.cell
def _(mo):
    horizon_slider = mo.ui.slider(start=0, stop=300, step=30, label="Horizon (mins)")
    horizon_slider
    return (horizon_slider,)


@app.cell
def _(mo):
    month_slider = mo.ui.slider(start=0, stop=9, step=1, label="Lookback (months)")
    month_slider
    return (month_slider,)


@app.cell
def _(mo):
    mo.md(r"""Note that timestamps should be converted to UTC before calling any RPCs!""")
    return


@app.cell
def _(dt, month_slider):
    pivot_timestamp = (
        dt.datetime.now().replace(minute=0, second=0, microsecond=0)
        - dt.timedelta(weeks=month_slider.value * 4)
    ).astimezone(dt.UTC)
    pivot_timestamp
    return (pivot_timestamp,)


@app.cell
def _(mo):
    mo.md(r"""First, create the request object with the dataclasses improted from the library code.""")
    return


@app.cell
async def _(client, dp, dt, horizon_slider, pivot_timestamp, uuid):
    gf_request = dp.GetForecastAsTimeseriesRequest(
        energy_source=dp.EnergySource.SOLAR,
        forecaster=dp.Forecaster(forecaster_name="test_forecaster_1", forecaster_version="v1"),
        horizon_mins=horizon_slider.value,
        location_uuid=str(uuid.uuid4()),
        time_window=dp.TimeWindow(
            start_timestamp_utc=pivot_timestamp - dt.timedelta(hours=48),
            end_timestamp_utc=pivot_timestamp + dt.timedelta(hours=36),
        ),
    )
    gf_response = await client.get_forecast_as_timeseries(gf_request)
    gf_response
    return (gf_response,)


@app.cell
def _(mo):
    mo.md(r"""The response can be easily transformed into a dataframe:""")
    return


@app.cell
def _(betterproto, gf_response, pd):
    gfdf = pd.DataFrame.from_dict(
        gf_response.to_dict(include_default_values=True, casing=betterproto.Casing.SNAKE)["values"]
    )
    gfdf
    return (gfdf,)


@app.cell
def _(mo):
    mo.md(
        r"""
    And the dataframe is trivial to plot.

    Try changing the horizon and lookback and see the effect on the graph.
    """
    )
    return


@app.cell
def _(alt, gfdf):
    gf_p50_line = (
        alt.Chart(gfdf)
        .mark_line()
        .encode(
            y="p50_value_percent",
            x="timestamp_utc:T",
        )
    )
    gf_band = (
        alt.Chart(gfdf)
        .mark_errorband(extent="ci")
        .encode(
            y="p10_value_percent",
            y2="p90_value_percent",
            x="timestamp_utc:T",
        )
    )
    gf_chart = gf_band + gf_p50_line
    gf_chart
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Get values for adjuster

    The adjuster requires the average delta for each time horizon over the last week, at a given time of day. There is a dedicated RPC function for this.
    """
    )
    return


@app.cell
async def _(client, dp, pivot_timestamp, uuid):
    gd_request = dp.GetWeekAverageDeltasRequest(
        location_uuid=str(uuid.uuid4()),
        energy_source=dp.EnergySource.SOLAR,
        pivot_time=pivot_timestamp,
        forecaster=dp.Forecaster(
            forecaster_name="test_forecaster_1",
            forecaster_version="v1",
        ),
        observer_name="test_observer",
    )
    gd_response = await client.get_week_average_deltas(gd_request)
    gd_response
    return (gd_response,)


@app.cell
def _(alt, betterproto, gd_response, pd):
    gddf = pd.DataFrame.from_dict(
        gd_response.to_dict(include_default_values=True, casing=betterproto.Casing.SNAKE)["deltas"],
    )
    gd_chart = (
        alt.Chart(gddf)
        .mark_point()
        .encode(
            y="delta_percent",
            x="horizon_mins",
        )
    )
    gd_chart
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Plot locations on a map

    A useful workflow for visualising the predictions of several locations at a given time.
    """
    )
    return


@app.cell
async def _(client, dp, pivot_timestamp, uuid):
    gm_request = dp.GetForecastAtTimestampRequest(
        location_uuids=[str(uuid.uuid4()) for i in range(10)],
        energy_source=dp.EnergySource.SOLAR,
        forecaster=dp.Forecaster(
            forecaster_name="test_forecaster_1",
            forecaster_version="v1",
        ),
        timestamp_utc=pivot_timestamp,
    )
    gm_response = await client.get_forecast_at_timestamp(gm_request)
    gm_response
    return (gm_response,)


@app.cell
def _(betterproto, gm_response, pd):
    gmdf = (
        pd.DataFrame.from_dict(
            gm_response.to_pydict(include_default_values=True, casing=betterproto.Casing.SNAKE)[
                "values"
            ],
        )
        .assign(
            latitude=lambda d: pd.json_normalize(d["latlng"])["latitude"],
            longitude=lambda d: pd.json_normalize(d["latlng"])["longitude"],
        )
        .drop(columns="latlng")
    )
    gmdf
    return (gmdf,)


@app.cell
def _(alt, data, gmdf):
    countries = alt.topo_feature(data.world_110m.url, "countries")

    projection = (
        alt.Chart(countries)
        .mark_geoshape(fill="lightgray", stroke="white")
        .project("equirectangular")
        .properties(width=500, height=300)
    )
    points = (
        alt.Chart(gmdf)
        .mark_point()
        .encode(
            size="value_percent",
            longitude="longitude:Q",
            latitude="latitude:Q",
            tooltip=["location_uuid", "location_name", "effective_capacity_watts"],
        )
    )
    gm_chart = projection + points
    gm_chart
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Get data dump for analysis

    This pulls all the forecast data for the given location and time window via a server stream. It is easy to turn this into an xarray dataset for further investigation.
    """
    )
    return


@app.cell
async def _(betterproto, client, dp, dt, pivot_timestamp, uuid):
    sd_request = dp.StreamForecastDataRequest(
        location_uuid=str(uuid.uuid4()),
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_utc=pivot_timestamp - dt.timedelta(days=7),
            end_timestamp_utc=pivot_timestamp,
        ),
        forecasters=[
            dp.Forecaster(
                forecaster_name="test_model_1",
                forecaster_version="v1",
            ),
            dp.Forecaster(
                forecaster_name="test_model_2",
                forecaster_version="v1",
            ),
        ],
    )

    forecasts = []
    async for chunk in client.stream_forecast_data(sd_request):
        forecasts.append(chunk.to_dict(casing=betterproto.Casing.SNAKE))
    return (forecasts,)


@app.cell
def _(forecasts, pd):
    pd.DataFrame.from_dict(forecasts).set_index(
        ["init_timestamp", "location_uuid", "forecaster_fullname", "horizon_mins"]
    ).to_xarray()
    return


@app.cell
def _(mo):
    mo.md(r"""## Close the channel""")
    return


@app.cell
def _(channel):
    channel.close()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
