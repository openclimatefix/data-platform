import './init';
import { UKMap } from './components/map';
import { createGraph } from './components/graph';
import { getCrossSection, getSingleTimeSeries } from './components/api';
import { AppEvents } from './components/events';
import { TypedEmitter } from "tiny-typed-emitter";

const AppEmitter = new TypedEmitter<AppEvents>();
AppEmitter.on('region:selected', async (region) => {
    console.log("region:selected - " + region);
    let regionData = await getSingleTimeSeries(region);
    createGraph("regional-graph", regionData);
})

let map = new UKMap("map-container", AppEmitter);

let initialCrossSection = await getCrossSection(new Date());
map.updateRegionPaint(initialCrossSection);

let data = await getSingleTimeSeries("national");
createGraph("national-graph", data)



