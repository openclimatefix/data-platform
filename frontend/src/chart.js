function generateData() {
    var res = [];
    var time = new Date(Date.UTC(2018, 0, 1, 0, 0, 0, 0));
    for (var i = 0; i < 500; ++i) {
        res.push({
            time: time.getTime() / 1000,
            value: i,
        });

        time.setUTCDate(time.getUTCDate() + 1);
    }

    return res;
}

var chart = LightweightCharts.createChart(document.getElementById('container'));

var mainSeries = chart.addLineSeries({
    priceFormat: {
        minMove: 1,
        precision: 0,
    },
});

mainSeries.setData(generateData());
