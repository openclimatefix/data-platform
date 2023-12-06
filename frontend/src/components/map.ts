import { 
    Map, MapOptions, 
    Control, ControlOptions, 
    DomUtil, 
    LatLng, LatLngBounds,
    GeoJSON as GeoJSONLayer } from 'leaflet';
import type { Layer, Point } from 'leaflet';
import 'leaflet-edgebuffer';
import 'leaflet/dist/leaflet.css';
import { leafletLayer, PolygonSymbolizer } from 'protomaps-leaflet';
import type { PaintSymbolizer, Feature as PLFeature } from 'protomaps-leaflet';
import type { GeoJsonProperties, Feature } from 'geojson';

import { TypedEmitter } from 'tiny-typed-emitter';
import { AppEvents } from './events';
import { UKGSPs } from './geojsons';
import { GetPredictedCrossSectionResponse } from '../proto/api';


function getColor(d: number): string {
    let col = d > 80 ? '#ffffd4' :
        d > 60 ? '#fed98e' :
        d > 40 ? '#fe9929' :
        d > 20 ? '#d95f0e' :
        '#2D2D2D';
    return col;
}

/**
 * A leaflet symboliser for the GSP regions
 */
class GSPSymboliser implements PaintSymbolizer {
    
    _csr: GetPredictedCrossSectionResponse;

    constructor(cs: GetPredictedCrossSectionResponse) {
        this._csr = cs;
    }

    /**
     * Overriden method from PaintSymbolizer to draw the GSP regions
     * @param ctx The canvas context to draw on
     * @param geom The geometry of the feature
     * @param z The zoom level
     * @param feature The feature to draw
     * @returns void
     */
    draw(ctx: CanvasRenderingContext2D, geom: Point[][], _: number, feature: PLFeature): void {
        // Get the yield for the region defined by the feature
        let csr = this._csr;
        let locID = feature.props ? feature.props.GSPs : null;
        let y = csr.yields.find((y) => y.locationID == locID);

        // Define a function to draw the region with colour based on the yield
        let fill = '#2D2D2D';
        let drawPath = () => {
            fill = y ? getColor(y.yieldKw) : fill;
            ctx.strokeStyle = '#000000';
            ctx.fillStyle = fill
            ctx.stroke();
            ctx.fill();
        };

        // Draw the region as a path
        var vertices_in_path = 0;
        ctx.beginPath();
        for (var poly of geom) {
            for (var p = 0; p < poly.length; p++) {
                let pt = poly[p];
                p == 0 ? ctx.moveTo(pt.x, pt.y) : ctx.lineTo(pt.x, pt.y);
            }
            vertices_in_path += poly.length;
        }
        if (vertices_in_path > 0)
            drawPath();
    }
}


/**
 * A leaflet map control to display information about a region
 */
class Info extends Control {
    _div: HTMLElement;
    
    declare options: ControlOptions
    constructor(options: ControlOptions) {
        super(options);
        this._div = HTMLElement.prototype;
    }

    /**
     * Overriden method from Control to create the info view
     * @param map The map to add the info view to
     * @returns The container for the info view
     */
    onAdd(_: Map): HTMLElement {
        this._div = DomUtil.create('div', 'info'); // create a div with a class "info"
        this._div.innerHTML = 'Hover over a region';
        return this._div;
    }
    /* 
     * New method that we will use to update the control based on feature properties passed
     * @param props The properties of the feature
     * @returns void
     */
    update(properties?: GeoJsonProperties): void {
        this._div.innerHTML = (properties ?
            '<b>' + properties.GSPs + '</b><br />'
            : 'Hover over a region');
    }
}

/**
 * A leaflet map control to display a legend
 */
class Legend extends Control {
    _div: HTMLElement;
    
    declare options: ControlOptions
    constructor(options: ControlOptions) {
        super(options);
        this._div = HTMLElement.prototype;
    }

    /**
     * Overriden method from Control to create the legend
     * @param map The map to add the legend to
     * @returns The container for the legend
     */
    onAdd(_: Map): HTMLElement {
        this._div = DomUtil.create('div', 'legend'); 
        var grades = [0, 20, 40, 60, 80];

        // loop through density intervals and generate a label with a colored square for each interval
        for (var i = 0; i < grades.length; i++) {
            this._div.innerHTML +=
                '<i style="background:' + getColor(grades[i] + 1) + '"></i> ' +
                grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
        }
        return this._div;
    }
}

/**
 * A map of the UK, with GSP regions highlighted
 * @param divID The ID of the div to put the map in
 */
class UKMap extends Map {
    declare options: MapOptions 
    declare element: string
    
    private infoControl: Info
    private pmtilesLayer: any
    private emitter: TypedEmitter<AppEvents>

    constructor(element: string | HTMLElement, emitter: TypedEmitter<AppEvents>, options?: MapOptions) {
        // Set the default options
        if (options == null) {
            options = {
                zoomSnap: 0.5,
                wheelPxPerZoomLevel: 1,
                zoomControl: false,
                attributionControl: false,
                minZoom: 6,
                maxZoom: 9,
                maxBounds: new LatLngBounds(new LatLng(49.5, -11.5), new LatLng(60.5, 2.5))
            }
        }
        // Create the map with the passed in options
        super(element, options);

        // Set the emitter
        this.emitter = emitter;

        // Add the info and legend controls
        this.infoControl = new Info({ position: 'topright' });
        this.infoControl.addTo(this);
        new Legend({ position: 'bottomright' }).addTo(this);

        // Add the PMTiles layer
        this.pmtilesLayer = leafletLayer({
            url: '/uk-gsp.pmtiles',
            edgeBufferTiles: 2,
            paint_rules: [
                {
                    dataLayer: "gspregions",
                    symbolizer: new PolygonSymbolizer({fill: "dimgrey"})
                },
            ]
        });
        this.pmtilesLayer.addTo(this);
        this.updateRegionPaint = this.updateRegionPaint.bind(this);

        // Add the interactive layer
        new GeoJSONLayer(UKGSPs, {
            style: function() {
                return {
                    color: "#FFFFFF",
                    weight: 0,
                    fillOpacity: 0
                };
            },
            onEachFeature: this.onEachFeature.bind(this),
        }).addTo(this);

        this.setView([54.8, -4], 6);
    }

    private onEachFeature(_: Feature, layer: Layer) {
         layer.on({
            click: (e) => {
                // Emit the event
                this.emitter.emit("region:selected", e.target.feature.properties.GSPs);
                // Zoom to the region
                this.fitBounds(e.target.getBounds());
            },
            mouseover: (e) => {
                // Highlight a GeoJSON feature in the feature layer
                var layer = e.target;
                layer.setStyle({
                    weight: 8,
                    color: '#666',
                    dashArray: '',
                    fillOpacity: 0.7
                });
                this.infoControl.update(layer.feature.properties);
                layer.bringToFront();
            },
            mouseout: (e) => {
                // Unhighlight the GeoJSON feature in the feature layer
                var layer = e.target;
                layer.setStyle({
                    weight: 0,
                    fillOpacity: 0
                });
                this.infoControl.update(e.target.feature.properties);
            }
        })
    }

    updateRegionPaint(data: GetPredictedCrossSectionResponse) {
        this.pmtilesLayer.paint_rules = [
            {
                dataLayer: "gspregions",
                symbolizer: new GSPSymboliser(data)
            }
        ];
        this.pmtilesLayer.rerenderTiles();
    }
}

export { UKMap, UKGSPs, GSPSymboliser };
