Architecture choices
===========================================================================

*The old frontend is slow, and falls over with more than 4 simultaneous 
visitors*

The resolution to this is partly an API issue, but that doesn't mean we 
can't optimise it in the frontend too.

The old frontend is not efficient in terms of network requests on load.
That is to say, it makes many requests, some repeating, and some completely
unecessary. I also bundles many large files which are also slow to load.
Two such issues are resolved with this frontend:

**Large files: GeoJSON**

Instead of a large GeoJSON file defining the GSP regions, the JSON is first
reduced dramatically to 3% of its original resolution. This does not affect
how the map looks, because painting is now done with a vector tile map
(more on that shortly). The feature GeoJSON layer is only used for mouse
clicks so it does not have to be a very precise map. This reduces the
size of the website and so increases loading speeds.

**Unecesssary network calls: Map providers**

The frontend here also reduce the overhead of extraneous networks calls by
bundling the mapdata in with the website, in an efficient vector tile
format called `.pmtiles`. This encodes the gsp region data in a package
only a few hundred kilobytes large. The map is then loaded from this which
is a) fast and b) prevents network overhead. Why load an entire map of the
UK, with roads and so on, when that is irrelevant to the data we are
showing?



PMTiles
-------

- PMTiles viewer https://protomaps.github.io/PMTiles/
- Protomaps Leaflet Paint rules https://docs.protomaps.com/pmtiles/leaflet
- Using tippecanoe to create vector tiles https://bertt.wordpress.com/2023/01/06/creating-vector-pmtiles-with-tippecanoe/

GeoJSON
-------

- Convert GeoJSON between projections https://datawanderings.com/2018/08/23/changing-dataset-projection-with-ogr2ogr/
- Reduce size of GEOJson filees https://github.com/mapbox/tippecanoe#installation
- Cloropleth leaflet.js with GeoJSON https://leafletjs.com/examples/choropleth/

Resources
---------

- GIS (GSP) regions for UK: https://opennetzero.org/dataset/gis-boundaries-for-gb-grid-supply-points
- Colorscheme generator for maps: https://colorbrewer2.org/#type=sequential&scheme=YlOrBr&n=5
