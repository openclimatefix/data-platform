import { QuartzAPIClient } from "../proto/api.client";
import { GrpcWebFetchTransport } from "@protobuf-ts/grpcweb-transport";

let transport = new GrpcWebFetchTransport({baseUrl: "http://localhost:8088"});
let client = new QuartzAPIClient(transport);

async function getNationalTimeSeries() {
    let locationValues = [];
    let streamingCall = client.getPredictedTimeseries({
        locationIDs: ["national"]
    });
    for await (let response of streamingCall.responses) {
        for (let y of response.yields) {
            locationValues.push({
                time: new Date(Number(y.timestampUnix) * 1000),
                value: Number(y.yieldKw),
            });
        }
    }
    console.log(locationValues);
    return locationValues;
};

function getCrossSection() {
    var values = [];
    fetch('/gsp-regions-lowpoly.json')
        .then(response => response.json())
        .then(data => {
            for (let i = 0; i < data.features.length; i++) {
                let region = data.features[i].properties.GSPs
                values.push({
                    id: region,
                    value: Math.random() * 100
                })
            };
        })
    return values;
};

export { getCrossSection, getNationalTimeSeries };
