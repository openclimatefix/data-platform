import { QuartzAPIClient } from "../proto/api.client";
import { GrpcWebFetchTransport } from "@protobuf-ts/grpcweb-transport";
import {
    GetPredictedTimeseriesResponse,
    GetPredictedCrossSectionResponse } from "../proto/api";

import { UKGSPs } from "./geojsons";

// Create a client using the GrpcWebFetchTransport
let transport = new GrpcWebFetchTransport({baseUrl: "http://localhost:8088"});
let client = new QuartzAPIClient(transport);

/**
 * getSingleTimeSeries calls the quartz API to get a predicted generation
 * timeseries for the given location.
 * @param locationID - the location ID to get the timeseries for
 * @returns a promise that resolves to a GetPredictedTimeseriesResponse
 */
async function getSingleTimeSeries(locationID: string): Promise<GetPredictedTimeseriesResponse> {
    let responses: GetPredictedTimeseriesResponse[] = [];
    let streamingCall = client.getPredictedTimeseries({
        locationIDs: [locationID]
    });
    console.log("GRPC Request: " + streamingCall.method.name, streamingCall.request)
    for await (let response of streamingCall.responses) {
        responses.push(response);
        console.log("GRPC Response: " + streamingCall.method.name, response)
    }
    return responses[0];
};

/**
 * getCrossSection calls the quartz API to get a predicted generation
 * cross section for the given date.
 * @param d - the date to get the cross section for
 * @returns a promise that resolves to a GetPredictedCrossSectionResponse
 */
async function getCrossSection(d: Date): Promise<GetPredictedCrossSectionResponse> {
    let ids: string[] = UKGSPs.features.map((f) => f.properties!.GSPs);
    let call = await client.getPredictedCrossSection({
        timestampUnix: BigInt(Math.floor(d.getTime() / 1000)),
        locationIDs: ids
    });
    console.log("GRPC Request: " + call.method.name, call.request)
    let response = call.response;
    console.log("GRPC Response: " + call.method.name, response)
    return response;
};

// Export the functions to be available to other modules
export { getCrossSection, getSingleTimeSeries };
