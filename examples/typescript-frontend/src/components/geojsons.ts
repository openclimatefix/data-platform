import { FeatureCollection } from 'geojson';

const _data = await fetch('/gsp-regions-lowpoly.json');
const UKGSPs: FeatureCollection = await _data.json();

export { UKGSPs };
