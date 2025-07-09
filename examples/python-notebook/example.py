# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "betterproto==2.0.0b7",
#     "marimo",
#     "grpclib==0.4.8",
#     "pandas==2.3.1",
#     "matplotlib==3.10.3",
# ]
# ///

import marimo

__generated_with = "0.14.10"
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
    return Channel, dp, dt, mo, plt


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
            model_name="test_model",
            model_version="v10",
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
            model_name="test_model",
            model_version="v10",
        ),
    )
    response2 = await client.get_predicted_timeseries(request2)
    response2
    return (response2,)


@app.cell
def _(plt, response2):
    xs2 = [p.yield_percent for p in response2.yields]
    ys2 = [p.timestamp_unix for p in response2.yields]
    plt.plot(ys2, xs2)
    plt.show()
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
