import './init';
import { UKMap } from './components/map';
import { TimeseriesGraph } from './components/graph';
import { getCrossSection, getSingleTimeSeries } from './components/api';
import { AppEvents } from './components/events';
import { TypedEmitter } from "tiny-typed-emitter";

let nationalGraph = new TimeseriesGraph("national-graph");
let regionalGraph = new TimeseriesGraph("regional-graph");

const AppEmitter = new TypedEmitter<AppEvents>();
AppEmitter.on('region:selected', async (region) => {
    console.log("region:selected - " + region);
    let regionData = await getSingleTimeSeries(region);
    regionalGraph.update(regionData.yields);
})

let map = new UKMap("map-container", AppEmitter);

let initialCrossSection = await getCrossSection(new Date());
map.updateRegionPaint(initialCrossSection);

let data = await getSingleTimeSeries("national");
nationalGraph.update(data.yields);



