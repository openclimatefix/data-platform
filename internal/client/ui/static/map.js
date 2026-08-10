(function() {
    function panelColors() {
        var theme = window.chartTheme();
        return {
            panelBg: theme.panelBg,
            empty: window.getCssVar('--nc-bg-3', '#f0f1f3'),
            hover: window.getCssVar('--chart-color-1', '#1095c1'),
            visualMapHigh: window.getCssVar('--chart-color-2', '#f2a900'),
            borderColor: theme.textPrimary
        };
    }

    // The map panel is replaced wholesale on every swap (hx-swap-oob="innerHTML"), so a
    // listener stashed on the map element itself is unreachable on the next mount - the old
    // element is detached and a fresh one takes its id. Track the single active listener at
    // module scope instead, so mounting always tears down whatever came before it.
    var activeTimeSelectedListener = null;

    function setTimeSelectedListener(fn) {
        if (activeTimeSelectedListener) {
            document.removeEventListener('timeSelected', activeTimeSelectedListener);
        }
        activeTimeSelectedListener = fn;
        if (fn) {
            document.addEventListener('timeSelected', fn);
        }
    }

    window.mountChoroplethMap = function(mapElId, configElId) {
        setTimeSelectedListener(null);

        var mapDom = document.getElementById(mapElId);
        var configEl = document.getElementById(configElId);
        if (!mapDom || !configEl) return;

        var config = JSON.parse(configEl.textContent);
        var geojsonData = config.geojson;
        var labelElId = mapElId + '-time-label';

        if (!geojsonData || !geojsonData.features) {
            mapDom.innerHTML = '<div class="empty-state">No GSP boundary data found.</div>';
            return;
        }

        var colors = panelColors();

        var uuidToName = {};
        geojsonData.features.forEach(function(f) {
            if (!f.properties) f.properties = {};
            var uuid = f.id || f.properties.geometry_uuid || f.properties.uuid;
            if (!uuid && f.properties.name) uuid = f.properties.name;
            var name = f.properties.geometry_name || f.properties.location_name || uuid || 'Unknown GSP';
            if (uuid) {
                uuidToName[uuid] = name;
                f.properties.name = uuid;
            }
        });

        var mapName = 'gspMap_' + mapElId;
        echarts.registerMap(mapName, geojsonData);

        var option = {
            tooltip: {
                trigger: 'item',
                showDelay: 0,
                transitionDuration: 0.2,
                formatter: function(params) {
                    var title = (params.name && uuidToName[params.name]) || params.name || 'Unknown GSP';
                    if (!params.data) return '<strong>' + title + '</strong><br/>No Data';
                    var val = params.data.value;
                    if (isNaN(val) || val === null) return '<strong>' + title + '</strong><br/>No Data';

                    var cap = params.data.capacity || 0;
                    var gen = val * cap;

                    return '<strong>' + title + '</strong><br/>' +
                        'Generation: <strong>' + window.formatEnergyValue(gen) + '</strong> (' + (val * 100).toFixed(1) + '%)<br/>' +
                        'Capacity: <strong>' + window.formatEnergyValue(cap) + '</strong>';
                }
            },
            visualMap: {
                left: 'right',
                bottom: 'bottom',
                min: 0, max: 1.0,
                text: ['Max (100%)', 'Min (0%)'],
                realtime: true, calculable: true,
                inRange: { color: [colors.empty, colors.visualMapHigh] },
                formatter: function(value) { return (value * 100).toFixed(0) + '%'; }
            },
            series: [{
                id: 'gspSeries',
                name: 'GSP Generation',
                type: 'map', map: mapName,
                roam: true, layoutCenter: ['50%', '50%'], layoutSize: '100%',
                scaleLimit: { min: 1, max: 10 },
                selectedMode: 'single',
                label: { show: false },
                select: {
                    label: { show: false },
                    itemStyle: { borderColor: colors.borderColor, borderWidth: 2, shadowColor: 'rgba(0,0,0,0.5)', shadowBlur: 10 }
                },
                itemStyle: { areaColor: colors.empty, borderColor: colors.panelBg, borderWidth: 0.5 },
                emphasis: {
                    label: { show: false },
                    itemStyle: { areaColor: colors.hover, borderColor: colors.borderColor, borderWidth: 1 }
                },
                data: []
            }]
        };

        var myMap = window.mountChart(mapDom, function() { return option; });

        var currentSelectedGsp = null;
        var resetTimeout = null;
        var isResettingMap = false;

        myMap.on('georoam', function(params) {
            if (isResettingMap) return;

            var opt = myMap.getOption();
            if (opt && opt.series && opt.series.length > 0) {
                var s = opt.series[0];
                if (s.zoom <= 1.001) {
                    var needsReset = false;
                    if (s.center) {
                        needsReset = true;
                    } else if (params.dx || params.dy) {
                        needsReset = true;
                    }

                    if (needsReset) {
                        if (resetTimeout) clearTimeout(resetTimeout);
                        resetTimeout = setTimeout(function() {
                            isResettingMap = true;
                            myMap.setOption({
                                series: [{
                                    id: 'gspSeries',
                                    zoom: 1,
                                    center: null,
                                    animationDurationUpdate: 300,
                                    animationEasingUpdate: 'cubicOut'
                                }]
                            });
                            setTimeout(function() { isResettingMap = false; }, 350);
                        }, 100);
                    }
                }
            }
        });

        myMap.on('click', function(params) {
            if (!params.name) return;

            var clickedUuid = params.name;
            var targetUuid = clickedUuid;

            if (currentSelectedGsp === clickedUuid) {
                targetUuid = config.locationUuid;
                currentSelectedGsp = null;
            } else {
                currentSelectedGsp = clickedUuid;
            }

            // location_uuid/energy_source/time_window are pinned to the config that produced
            // this map (not read from the form), in case the user has an unconfirmed edit
            // pending in the query form; only forecaster/observer are read live from it.
            var params = new URLSearchParams();
            params.set('location_uuid', targetUuid);
            params.set('skip_map', 'true');
            params.set('energy_source', config.energySource);
            params.set('time_window', config.timeWindow);

            var qf = document.getElementById('query-form');
            if (qf) {
                new FormData(qf).forEach(function(value, key) {
                    if (key === 'forecaster' || key === 'observer') {
                        params.append(key, value);
                    }
                });
            }

            htmx.ajax('GET', '/components/forecast?' + params.toString(), '#chart-panel')
                .catch(function(err) { console.error(err); });
        });

        var updateMapData = function(timestamp) {
            if (!timestamp) return;
            fetch('/api/dashboard/map-snapshot?timestamp=' + timestamp +
                '&nation_uuid=' + config.locationUuid +
                '&energy_source=' + config.energySource +
                '&forecaster=' + encodeURIComponent(config.firstForecaster))
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var mOpt = myMap.getOption();
                    if (mOpt && mOpt.series && mOpt.series.length > 0) {
                        mOpt.series[0].data = data;
                        myMap.setOption(mOpt);
                    }
                }).catch(function(err) { console.error(err); });
        };

        setTimeSelectedListener(function(e) {
            var labelEl = document.getElementById(labelElId);
            if (labelEl) labelEl.innerHTML = e.detail.label;
            updateMapData(e.detail.timestamp);
        });

        if (config.timestamps && config.timestamps.length > 0) {
            updateMapData(config.timestamps[0]);
        }
    };

    window.mountLocationMap = function(mapElId, configElId) {
        setTimeSelectedListener(null);

        var mapDom = document.getElementById(mapElId);
        var configEl = document.getElementById(configElId);
        if (!mapDom || !configEl) return;

        var config = JSON.parse(configEl.textContent);
        var geojsonData = config.geojson;
        var labelElId = mapElId + '-time-label';
        var colors = panelColors();

        var lat = config.lat || 0;
        var lng = config.lng || 0;
        var fillOpacity = config.avgFraction || 1.0;
        if (fillOpacity < 0.15) fillOpacity = 0.15;

        var option = {};

        if (geojsonData) {
            // A single-location response always has exactly one feature; its geometry type
            // decides whether to render an area fill (geo) or a point (scatter).
            var geomType = geojsonData.features && geojsonData.features[0] && geojsonData.features[0].geometry
                ? geojsonData.features[0].geometry.type
                : '';
            var hasArea = geomType === 'Polygon' || geomType === 'MultiPolygon';
            var locMapColor = window.getCssVar('--chart-color-0', '#7a9374');

            if (hasArea) {
                var mapName = 'locMap_' + mapElId;
                echarts.registerMap(mapName, geojsonData);
                option = {
                    geo: {
                        map: mapName, roam: false,
                        itemStyle: { areaColor: locMapColor, opacity: fillOpacity, borderColor: colors.borderColor, borderWidth: 2 },
                        emphasis: { itemStyle: { areaColor: locMapColor, opacity: Math.min(fillOpacity + 0.2, 1) }, label: { show: false } },
                        layoutCenter: ['50%', '50%'], layoutSize: '80%'
                    }
                };
            } else {
                option = {
                    xAxis: { type: 'value', show: false, min: lng - 0.1, max: lng + 0.1 },
                    yAxis: { type: 'value', show: false, min: lat - 0.1, max: lat + 0.1 },
                    grid: { left: 0, right: 0, top: 0, bottom: 0 },
                    series: [{
                        type: 'scatter', symbolSize: 14,
                        itemStyle: { color: locMapColor, opacity: fillOpacity, borderColor: 'white', borderWidth: 3, shadowColor: 'rgba(0,0,0,0.3)', shadowBlur: 4, shadowOffsetY: 2 },
                        data: [[lng, lat]]
                    }]
                };
            }
        }

        var myMap = window.mountChart(mapDom, function() { return option; });

        setTimeSelectedListener(function(e) {
            var labelEl = document.getElementById(labelElId);
            if (labelEl && config.hasLatlng) {
                labelEl.innerHTML = config.latlngLabel + ' | ' + e.detail.label;
            }

            var mapOpt = myMap.getOption();
            if (mapOpt.geo && mapOpt.geo.length > 0) {
                mapOpt.geo[0].itemStyle.opacity = e.detail.fraction;
                mapOpt.geo[0].emphasis.itemStyle.opacity = Math.min(e.detail.fraction + 0.2, 1);
                myMap.setOption(mapOpt);
            } else if (mapOpt.series && mapOpt.series.length > 0) {
                mapOpt.series[0].itemStyle.opacity = e.detail.fraction;
                myMap.setOption(mapOpt);
            }
        });
    };
})();
