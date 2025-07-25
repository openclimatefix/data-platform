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
# ]
# ///

import marimo

__generated_with = "0.14.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import betterproto.grpc
    from grpclib.client import Channel
    import datetime as dt
    from ocf import dp
    import json
    import pandas as pd
    import matplotlib.pyplot as plt
    import marimo as mo
    import altair as alt
    from vega_datasets import data
    return Channel, alt, data, dp, dt, mo, pd, plt


@app.cell
def _(Channel, dp):
    channel = Channel(host="localhost", port=50051)
    client = dp.DataPlatformServiceStub(channel)
    return channel, client


@app.cell
async def _(client, dp, dt):
    request = dp.GetLatestPredictionsRequest(
        location_id=1,
        energy_source=dp.EnergySource.SOLAR,
        pivot_timestamp_unix=dt.datetime.now().replace(
            minute=0, second=0, microsecond=0, tzinfo=dt.UTC
        ),
        model=dp.Model(
            model_name="test_model_1",
            model_version="v1",
        ),
    )
    response = await client.get_latest_predictions(request)
    type(response)
    return (response,)


@app.cell
def _(plt, response):
    xs = [p.yield_percent for p in response.yields]
    ys = [p.timestamp_unix for p in response.yields]
    plt.plot(ys, xs)
    plt.show()
    return


@app.cell
def _(mo):
    horizon_slider = mo.ui.slider(start=0, stop=120, step=30, label="Horizon (mins)")
    horizon_slider
    return (horizon_slider,)


@app.cell
async def _(client, dp, dt, horizon_slider):
    request2 = dp.GetPredictedTimeseriesRequest(
        location_id=1,
        energy_source=dp.EnergySource.SOLAR,
        horizon_mins=horizon_slider.value,
        time_window=dp.TimeWindow(
            start_timestamp_unix=dt.datetime.now().replace(tzinfo=dt.UTC) - dt.timedelta(hours=48),
            end_timestamp_unix=dt.datetime.now().replace(tzinfo=dt.UTC),
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
def _(pd, response2):
    response2df = pd.DataFrame.from_dict(response2.to_dict()["yields"])
    response2df
    return (response2df,)


@app.cell
def _(alt, response2df):
    chart = (
        alt.Chart(response2df)
        .mark_line()
        .encode(
            y="yieldPercent",
            x="timestampUnix:T",
        )
    )
    chart
    return


@app.cell
async def _(client, dp, dt):
    request3 = dp.GetWeekAverageDeltasRequest(
        location_id=1,
        energy_source=dp.EnergySource.SOLAR,
        pivot_time=dt.datetime(2025, 7, 17, 10, tzinfo=dt.UTC),
        model=dp.Model(
            model_name="test_model_1",
            model_version="v1",
        ),
        observer_name="test_observer",
    )
    response3 = await client.get_week_average_deltas(request3)
    response3.deltas
    return (response3,)


@app.cell
def _(alt, pd, response3):
    chart3 = (
        alt.Chart(pd.DataFrame.from_dict(response3.to_pydict()["deltas"]))
        .mark_point()
        .encode(
            y="deltaPercent",
            x="horizonMins",
        )
    )
    chart3
    return


@app.cell
def _(alt, data):
    countries = alt.topo_feature(data.world_110m.url, "countries")

    alt.Chart(countries).mark_geoshape(fill="lightgray", stroke="white").project(
        "equirectangular"
    ).properties(width=500, height=300)
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
