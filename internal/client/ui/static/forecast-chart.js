(function() {
    function buildMarkLine(index, color) {
        return {
            data: [
                {
                    xAxis: index,
                    label: { show: false },
                    lineStyle: { color: color, type: 'dashed', width: 2, opacity: 0.7 }
                }
            ],
            symbol: ['none', 'none'],
            animation: false
        };
    }

    window.mountForecastChart = function(chartElId, configElId) {
        var chartDom = document.getElementById(chartElId);
        var configEl = document.getElementById(configElId);
        if (!chartDom || !configEl) return;

        var config = JSON.parse(configEl.textContent);
        var labels = config.labels;
        var seriesConfig = config.series;
        var capacityWatts = config.capacityWatts;
        var timestamps = config.timestamps;

        var theme = window.chartTheme();
        var colors = theme.colors;

        var seriesData = [];

        seriesConfig.forEach(function(s, i) {
            var color = colors[i % colors.length];

            var dataObjects = s.data.map(function(val, idx) {
                var obj = { value: val };
                if (s.hasBands && s.bandMap && s.bandMap[idx]) {
                    obj.bandMap = s.bandMap[idx];
                }
                return obj;
            });

            var item = {
                id: (i === 0) ? 'primaryLine' : s.name + '_Main',
                name: s.name,
                type: 'line',
                data: dataObjects,
                smooth: 0.2,
                showSymbol: false,
                lineStyle: { color: color, width: 2 },
                itemStyle: { color: color }
            };

            if (s.isObservation) {
                item.lineStyle.type = 'dashed';
            }
            seriesData.push(item);

            if (s.hasBands && s.bands) {
                var baseOpacity = 0.15;
                s.bands.forEach(function(band, bIndex) {
                    var opacity = baseOpacity + (bIndex * 0.1);

                    seriesData.push({
                        id: s.name + "_Band_" + band.lowerName,
                        name: s.name,
                        type: 'line',
                        data: band.lower,
                        lineStyle: { opacity: 0 },
                        stack: "band_" + i + "_" + bIndex,
                        showSymbol: false,
                        tooltip: { show: false }
                    });

                    seriesData.push({
                        id: s.name + "_Band_" + band.upperName,
                        name: s.name,
                        type: 'line',
                        data: band.diff,
                        lineStyle: { opacity: 0 },
                        areaStyle: { color: color, opacity: opacity },
                        stack: "band_" + i + "_" + bIndex,
                        showSymbol: false,
                        tooltip: { show: false },
                        z: 0 - bIndex // Ensure wider bands are drawn underneath narrower bands
                    });
                });
            }
        });

        var nowMs = Date.now();
        var closestIndex = 0;
        var minDiff = Infinity;

        timestamps.forEach(function(ts, idx) {
            var diff = Math.abs(nowMs - ts * 1000);
            if (diff < minDiff) {
                minDiff = diff;
                closestIndex = idx;
            }
        });

        if (seriesData.length > 0) {
            seriesData[0].markLine = buildMarkLine(closestIndex, theme.text);
        }

        var option = {
            backgroundColor: 'transparent',
            legend: {
                data: seriesConfig.map(function(s) { return s.name; }),
                icon: 'circle',
                itemWidth: 10,
                itemHeight: 10,
                textStyle: { color: theme.textPrimary, fontSize: 13 },
                left: 0,
                top: 0
            },
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'line', lineStyle: { type: 'dotted', color: theme.text, opacity: 0.5 } },
                formatter: function(params) {
                    if (!params || params.length === 0) return '';
                    var tooltipStr = '<strong>' + params[0].axisValue + '</strong><br/>';

                    var mainSeriesParams = params.filter(function(p) {
                        return !p.seriesId || !p.seriesId.includes('_Band');
                    });

                    mainSeriesParams.forEach(function(param) {
                        var valStr = window.formatEnergyValue(param.value);
                        var name = param.seriesName;

                        var extraStr = '';
                        if (param.data && param.data.bandMap) {
                            var bm = param.data.bandMap;

                            var levels = [];
                            var vals = [];

                            Object.keys(bm).sort(function(a, b) {
                                return parseInt(a.substring(1)) - parseInt(b.substring(1));
                            }).forEach(function(k) {
                                levels.push(k.substring(1)); // strip the 'p'
                                var vStr = window.formatEnergyValue(bm[k]);
                                vals.push(vStr);
                            });

                            if (levels.length > 0) {
                                var pUnit = vals[vals.length - 1].replace(/[\d\.\s]/g, '');
                                var cleanVals = vals.map(function(v, idx) {
                                    if (idx < vals.length - 1 && pUnit && v.endsWith(pUnit)) {
                                        return v.replace(' ' + pUnit, '');
                                    }
                                    return v;
                                });
                                extraStr = ' (P' + levels.join('/') + ': ' + cleanVals.join('/') + ')';
                            }
                        }

                        tooltipStr += '<div style="margin-top: 6px;">';
                        tooltipStr += param.marker;
                        tooltipStr += '<span>' + name + ': </span>';
                        tooltipStr += '<strong>' + valStr + '</strong>';
                        if (extraStr) {
                            tooltipStr += '<span style="font-size: 0.9em; opacity: 0.8; margin-left: 8px;">' + extraStr + '</span>';
                        }
                        tooltipStr += '</div>';
                    });
                    return tooltipStr;
                }
            },
            grid: {
                left: 20, right: 40, top: 40, bottom: 40, containLabel: true
            },
            dataZoom: [
                { type: 'inside', xAxisIndex: 0, filterMode: 'none', minValueSpan: 12 },
                {
                    type: 'slider',
                    xAxisIndex: 0,
                    filterMode: 'none',
                    bottom: 5,
                    height: 20,
                    minValueSpan: 12,
                    borderColor: 'transparent',
                    backgroundColor: 'transparent',
                    fillerColor: window.getCssVar('--chart-slider-fill', 'rgba(28, 28, 30, 0.1)'),
                    handleStyle: { color: theme.panelBg, borderColor: theme.textPrimary, borderWidth: 1 },
                    moveHandleStyle: { color: theme.textPrimary, opacity: 0.5 },
                    dataBackground: {
                        lineStyle: { color: theme.grid, opacity: 0.8 },
                        areaStyle: { color: theme.grid, opacity: 0.5 }
                    },
                    selectedDataBackground: {
                        lineStyle: { color: theme.text, opacity: 0.8 },
                        areaStyle: { color: theme.text, opacity: 0.2 }
                    }
                }
            ],
            xAxis: {
                type: 'category',
                data: labels,
                axisLabel: {
                    color: theme.text,
                    hideOverlap: true,
                    showMinLabel: true,
                    showMaxLabel: true
                },
                axisTick: { show: false },
                axisLine: { show: false },
                splitLine: { show: false }
            },
            yAxis: {
                type: 'value',
                splitLine: {
                    lineStyle: { type: 'dashed', color: theme.grid }
                },
                axisLabel: { color: theme.text, formatter: window.formatEnergyValue }
            },
            series: seriesData
        };

        var myChart = window.mountChart(chartDom, function() { return option; });

        myChart.getZr().on('click', function(params) {
            var pointInPixel = [params.offsetX, params.offsetY];
            if (myChart.containPixel('grid', pointInPixel)) {
                var pointInGrid = myChart.convertFromPixel({ seriesIndex: 0 }, pointInPixel);
                var xIndex = pointInGrid[0];

                myChart.setOption({
                    series: [{
                        id: 'primaryLine',
                        markLine: buildMarkLine(xIndex, theme.text)
                    }]
                });

                var capacity = capacityWatts;
                if (capacity <= 0) return;

                var sum = 0;
                var count = 0;

                seriesData.forEach(function(s) {
                    if (s.type === 'line' && (!s.id || !s.id.includes('_Band'))) {
                        var d = s.data[xIndex];
                        var val = (d && typeof d === 'object') ? d.value : d;
                        if (val !== null && val !== undefined && val !== 'null' && !isNaN(val)) {
                            sum += (val / capacity);
                            count++;
                        }
                    }
                });

                var timestamp = timestamps[xIndex];
                var label = labels[xIndex];

                var fraction = 0.15;
                if (count > 0) {
                    fraction = sum / count;
                    if (fraction < 0.15) fraction = 0.15;
                    if (fraction > 1.0) fraction = 1.0;
                }

                document.dispatchEvent(new CustomEvent('timeSelected', {
                    detail: {
                        timestamp: timestamp,
                        label: label,
                        fraction: fraction
                    }
                }));
            }
        });
    };
})();
