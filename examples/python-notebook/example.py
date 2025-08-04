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

__generated_with = "0.14.16"
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
    return Channel, alt, betterproto, data, dp, dt, mo, pd


@app.cell
def _(mo):
    mo.md(r"""## Connecting to the data platform""")
    return


@app.cell
def _(Channel, dp, dt):
    channel = Channel(host="localhost", port=50051)
    client = dp.DataPlatformServiceStub(channel)
    pivot_timestamp = dt.datetime.now().replace(
        minute=0, hour=0, second=0, microsecond=0, tzinfo=dt.UTC
    )
    return channel, client, pivot_timestamp


@app.cell
def _(mo):
    mo.md(r"""## Get the latest forecast values for a location and model""")
    return


@app.cell
async def _(client, dp, pivot_timestamp):
    request = dp.GetLatestPredictionsRequest(
        location_id=1,
        energy_source=dp.EnergySource.SOLAR,
        pivot_timestamp_unix=pivot_timestamp,
        model=dp.Model(
            model_name="test_model_1",
            model_version="v1",
        ),
    )
    response = await client.get_latest_predictions(request)
    response
    return (response,)


@app.cell
def _(betterproto, pd, response):
    response_df = pd.DataFrame.from_dict(
        response.to_dict(include_default_values=True, casing=betterproto.Casing.SNAKE)["yields"]
    )
    response_df
    return (response_df,)


@app.cell
def _(alt, response_df):
    line = (
        alt.Chart(response_df)
        .mark_line()
        .encode(
            y="yield_percent",
            x="timestamp_unix:T",
        )
    )
    band = (
        alt.Chart(response_df)
        .mark_errorband(extent="ci")
        .encode(
            y="yield_p10_percent",
            y2="yield_p90_percent",
            x="timestamp_unix:T",
        )
    )

    chart1 = line + band
    chart1
    return


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
    horizon_slider = mo.ui.slider(start=0, stop=120, step=30, label="Horizon (mins)")
    horizon_slider
    return (horizon_slider,)


@app.cell
async def _(client, dp, dt, horizon_slider, pivot_timestamp):
    request2 = dp.GetPredictedTimeseriesRequest(
        location_id=1,
        energy_source=dp.EnergySource.SOLAR,
        horizon_mins=horizon_slider.value,
        time_window=dp.TimeWindow(
            start_timestamp_unix=pivot_timestamp - dt.timedelta(hours=48),
            end_timestamp_unix=pivot_timestamp,
        ),
        model=dp.Model(
            model_name="test_model_1",
            model_version="v1",
        ),
    )
    response2 = await client.get_predicted_timeseries(request2)
    response2
    return (response2,)


@app.cell
def _(betterproto, pd, response2):
    response2_df = pd.DataFrame.from_dict(
        response2.to_dict(include_default_values=True, casing=betterproto.Casing.SNAKE)["yields"]
    )
    response2_df
    return (response2_df,)


@app.cell
def _(alt, response2_df):
    line2 = (
        alt.Chart(response2_df)
        .mark_line()
        .encode(
            y="yield_percent",
            x="timestamp_unix:T",
        )
    )
    band2 = (
        alt.Chart(response2_df)
        .mark_errorband(extent="ci")
        .encode(
            y="yield_p10_percent",
            y2="yield_p90_percent",
            x="timestamp_unix:T",
        )
    )
    chart2 = line2 + band2
    chart2
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
async def _(client, dp, pivot_timestamp):
    request3 = dp.GetWeekAverageDeltasRequest(
        location_id=1,
        energy_source=dp.EnergySource.SOLAR,
        pivot_time=pivot_timestamp,
        model=dp.Model(
            model_name="test_model_1",
            model_version="v1",
        ),
        observer_name="test_observer",
    )
    response3 = await client.get_week_average_deltas(request3)
    response3
    return (response3,)


@app.cell
def _(alt, betterproto, pd, response3):
    response3_df = pd.DataFrame.from_dict(
        response3.to_dict(include_default_values=True, casing=betterproto.Casing.SNAKE)["deltas"],
    )
    chart3 = (
        alt.Chart(response3_df)
        .mark_point()
        .encode(
            y="delta_percent",
            x="horizon_mins",
        )
    )
    chart3
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
async def _(client, dp, pivot_timestamp):
    request4 = dp.GetPredictedCrossSectionRequest(
        location_ids=list(range(11)),
        energy_source=dp.EnergySource.SOLAR,
        model=dp.Model(
            model_name="test_model_1",
            model_version="v1",
        ),
        timestamp_unix=pivot_timestamp,
    )
    response4 = await client.get_predicted_cross_section(request4)
    response4
    return (response4,)


@app.cell
def _(betterproto, pd, response4):
    response4_df = (
        pd.DataFrame.from_dict(
            response4.to_pydict(include_default_values=True, casing=betterproto.Casing.SNAKE)["yields"],
        )
        .assign(
            latitude=lambda d: pd.json_normalize(d["latlng"])["latitude"],
            longitude=lambda d: pd.json_normalize(d["latlng"])["longitude"],
        )
        .drop(columns="latlng")
    )
    response4_df
    return (response4_df,)


@app.cell
def _(alt, data, response4_df):
    countries = alt.topo_feature(data.world_110m.url, "countries")

    projection = (
        alt.Chart(countries)
        .mark_geoshape(fill="lightgray", stroke="white")
        .project("equirectangular")
        .properties(width=500, height=300)
    )
    points = (
        alt.Chart(response4_df)
        .mark_point()
        .encode(
            size="capacity_watts",
            longitude="longitude:Q",
            latitude="latitude:Q",
            tooltip=["location_id", "location_name", "capacity_watts"],
        )
    )
    chart4 = projection + points
    chart4
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
async def _(betterproto, client, dp, dt, pivot_timestamp):
    request5 = dp.StreamForecastDataRequest(
        location_id=1,
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_unix=pivot_timestamp - dt.timedelta(days=7),
            end_timestamp_unix=pivot_timestamp,
        ),
        models=[
            dp.Model(
                model_name="test_model_1",
                model_version="v1",
            ),
            dp.Model(
                model_name="test_model_2",
                model_version="v1",
            ),
        ],
    )

    forecasts = []
    async for chunk in client.stream_forecast_data(request5):
        forecasts.append(chunk.to_dict(casing=betterproto.Casing.SNAKE))
    return (forecasts,)


@app.cell
def _(forecasts, pd):
    pd.DataFrame.from_dict(forecasts).set_index(
        ["init_timestamp", "location_id", "model_fullname", "horizon_mins"]
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
