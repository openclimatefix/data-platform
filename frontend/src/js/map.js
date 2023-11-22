import { createGraph } from './graph.js';
import { getCrossSection, getNationalTimeSeries } from './api.js';

const map = L.map('map', {
    zoomControl: false,
    attributionControl: false,
    minZoom: 6,
    maxZoom: 9,
    maxBounds: L.latLngBounds(L.latLng(49.5, -11.5), L.latLng(60.5, 2.5)),
});

function getColor(d) {
    return d > 80 ? '#ffffd4' :
        d > 60 ? '#fed98e' :
        d > 40 ? '#fe9929' :
        d > 20 ? '#d95f0e' :
        '#993404';
}

class GSPSymboliser {
    constructor(crossSection) {
        this.yields = crossSection;
    }

    draw(context, geom, z, feature) {

        // Fill the GSPs with a colour based on their yield
        var fill = '#2D2D2D';
        let y = this.yields.find((y) => y.id == feature.props.GSPs);
        fill = getColor(y.value);
        context.strokeStyle = '#2F2F2F';
        context.beginPath()
        for (var poly of geom) {
            for (var p = 0; p < poly.length-1; p++) {
                let pt = poly[p]
                if (p == 0) context.moveTo(pt.x, pt.y)
                else context.lineTo(pt.x, pt.y)
            }
        }
        context.fillStyle = fill
        context.fill()
    }
}

// Paint the layer initially with a uniform grey,
// then add the GSPs yield data on top when loaded.
var gsplayer = protomapsL.leafletLayer({
    url: '/uk-gsp.pmtiles',
    edgeBufferTiles: 1,
    paint_rules: [
        {
            dataLayer: "gspregions",
            symbolizer: new protomapsL.PolygonSymbolizer({fill: "dimgrey"})
        },
        {
            dataLayer: "gspregions",
            symbolizer: new GSPSymboliser(getCrossSection())
        }
    ]
});

var featurelayer = new L.GeoJSON.AJAX("/gsp-regions-lowpoly.json", {
    style: function(feature) {
        return {
            color: "#FFFFFF",
            weight: 0,
            fillOpacity: 0
        };
    },
    onEachFeature: function(feature, layer) {
        layer.on({
            click: async function(e) {
                createGraph(await getNationalTimeSeries(), "#graph");
                zoomToFeature(e);
            },
            mouseover: highlightFeature,
            mouseout: resetHighlight
        })
    }
});

var info = L.control();

info.onAdd = function (map) {
    this._div = L.DomUtil.create('div', 'info'); // create a div with a class "info"
    this.update();
    return this._div;
};

info.update = function (props) {
    this._div.innerHTML = (props ?
        '<b>' + props.GSPs + '</b><br />'
        : 'Hover over a region');
};

var legend = L.control({position: 'bottomright'});

legend.onAdd = function (map) {

    var div = L.DomUtil.create('div', 'info legend'),
        grades = [0, 20, 40, 60, 80],
        labels = [];

    // loop through our density intervals and generate a label with a colored square for each interval
    for (var i = 0; i < grades.length; i++) {
        div.innerHTML +=
            '<i style="background:' + getColor(grades[i] + 1) + '"></i> ' +
            grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
    }

    return div;
};

function zoomToFeature(e) {
    map.fitBounds(e.target.getBounds());
}

function resetHighlight(e) {
    featurelayer.resetStyle(e.target);
    info.update();
}

function highlightFeature(e) {
    var layer = e.target;

    layer.setStyle({
        weight: 8,
        color: '#666',
        dashArray: '',
        fillOpacity: 0.7
    });

    info.update(layer.feature.properties);
    layer.bringToFront();
}

featurelayer.addTo(map);
gsplayer.addTo(map);
info.addTo(map);
legend.addTo(map);

map.setView([54.8, -4.3], 6);

function updateRegionPaint(data) {
    gsplayer.paint_rules = [
        {
            dataLayer: "gspregions",
            symbolizer: new protomapsL.PolygonSymbolizer({fill: "dimgrey"})
        },
        {
            dataLayer: "gspregions",
            symbolizer: new GSPSymboliser(data)
        }
    ];
    gsplayer.rerenderTiles();
}

export { updateRegionPaint };
