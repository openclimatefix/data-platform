import { QuartzAPIClient } from "../proto/api.client";
import { GrpcWebFetchTransport } from "@protobuf-ts/grpcweb-transport";

let transport = new GrpcWebFetchTransport({baseUrl: "http://localhost:8088"});
let client = new QuartzAPIClient(transport);

async function getTimeSeries() {
    var values = [];
    let streamingCall = client.getPredictedTimeseries({
        locationIDs: ["test-id", "test-id-2"]
    });
    for await (let response of streamingCall.responses) {
        values.push(response);
    }
    console.log(values);
    return values;
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

export { getCrossSection, getTimeSeries };
