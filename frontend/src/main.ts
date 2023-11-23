import './init';
import { Map } from './components/map.js';
import { createGraph, generateData } from './components/graph.js';
import { getCrossSection } from './components/api';

let map = new Map("map-container");
map.updateRegionPaint(getCrossSection());
map.addInteractiveLayer(() => {
    createGraph("regional-graph", generateData())
    map.updateRegionPaint(getCrossSection());
});

createGraph("national-graph", generateData())

