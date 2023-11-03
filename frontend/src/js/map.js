import { createGraph, generateData } from './graph.js';

const map = L.map('map', {
    zoomControl: false,
    minZoom: 6,
    maxZoom: 8,
    maxBounds: L.latLngBounds(L.latLng(49.5, -11.5), L.latLng(60.5, 2.5)),
});

class GSPSymboliser {
    draw(context, geom, z, feature) {
        var fill = '#2D2D2D' 
        if (feature.props.GSPGroup == "_N") fill = 'darkslategrey'
        context.strokeStyle = '#2F2F2F'
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

var gsplayer = protomapsL.leafletLayer({
    url: '/uk-gsp.pmtiles',
        paint_rules: [
        {
            datalayer: "gspregions",
            symbolizer: new protomapsL.PolygonSymbolizer({fill: "dimgrey"})
        },
        {
            dataLayer:"gspregions",
            symbolizer:new GSPSymboliser()
        },
        ],
        attribution: 'opennetzero.org',
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
            click: function(e) {
                console.log(feature.properties);
                createGraph(generateData());
            }
        })
    }
});

featurelayer.addTo(map);
gsplayer.addTo(map);
  
map.setView([54.8, -4.3], 6);
