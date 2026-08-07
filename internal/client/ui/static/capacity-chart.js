(function() {
    window.mountCapacityChart = function(chartElId, configElId) {
        var chartDom = document.getElementById(chartElId);
        var configEl = document.getElementById(configElId);
        if (!chartDom || !configEl) return;

        var config = JSON.parse(configEl.textContent);
        var theme = window.chartTheme();

        var option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'line', lineStyle: { type: 'dashed' } },
                formatter: function(params) {
                    return params[0].axisValue + '<br/><strong>' + window.formatEnergyValue(params[0].value) + '</strong>';
                }
            },
            xAxis: {
                type: 'category',
                data: config.labels,
                axisLabel: {
                    color: theme.text,
                    hideOverlap: true,
                    showMinLabel: true,
                    showMaxLabel: true
                },
                axisLine: { show: false },
                axisTick: { show: false }
            },
            yAxis: {
                type: 'value',
                scale: true,
                axisLabel: {
                    formatter: window.formatEnergyValue,
                    color: theme.text
                },
                splitLine: { lineStyle: { type: 'dashed', color: theme.grid } }
            },
            grid: { left: 60, right: 20, top: 20, bottom: 20, containLabel: false },
            series: [{
                name: 'Capacity',
                type: 'line',
                step: 'end',
                data: config.values,
                itemStyle: { color: theme.colors[0] },
                areaStyle: { opacity: 0.1, color: theme.colors[0] },
                lineStyle: { width: 3 }
            }]
        };

        window.mountChart(chartDom, function() { return option; });
    };
})();
