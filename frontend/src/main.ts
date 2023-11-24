import './init';
import { UKMap, GSPSymboliser } from './components/map';
import { createGraph } from './components/graph';
import { getCrossSection, getSingleTimeSeries } from './components/api';

let map = new UKMap("map-container");
let initialCrossSection = await getCrossSection(new Date());
map.updateRegionPaint(initialCrossSection);


let data = await getSingleTimeSeries("national");
createGraph("national-graph", data)

