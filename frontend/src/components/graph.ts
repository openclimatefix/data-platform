import { select  } from "d3-selection";
import { scaleLinear, scaleTime } from "d3-scale";
import { timeHour } from "d3-time";
import { axisBottom, axisLeft } from "d3-axis";
import { line, curveMonotoneX } from "d3-shape";

import type { GetPredictedTimeseriesResponse, PredictedYield } from "../proto/api";

/**
 * Generate random data for testing purposes
 * @param scale Scale of the data (default: 100)
 * @returns Randomly generated data
 */
function generateData(scale?: number): GetPredictedTimeseriesResponse {

    // Set default scale
    let maxValue: number = 100;
    if (scale !== undefined) {
        maxValue = scale;
    }

    var now = new Date();
    // The x values are a time window with the following definition:
    // * Start: 2 days ago
    // * End: 2 days in the future
    // * Interval: 1 hour
    let allHours: Date[]  = timeHour
        .range(new Date(now.getTime() - 2 * 86400000), new Date(now.getTime() + 2 * 86400000));

    // The y values are random numbers between 0 and the scale
    var data: PredictedYield[] = allHours
        .map(function(time) {
            return {
                timestampUnix: BigInt(time.getTime() / 1000),
                yieldKw: Math.random() * maxValue,
            };
        });

    return {
        yields: data,
        locationID: "generated-location",
    };
}

/**
 * Create a graph in the given DOM element
 * @param target Target DOM element id
 * @param data Data to be displayed in the graph
 */
function createGraph(target: string, data: GetPredictedTimeseriesResponse): void {

    target = "#" + target;

    // Set the dimensions and margins of the graph
    var margin = {top: 30, right: 30, bottom: 30, left: 60},
        width = 600 - margin.left - margin.right,
        height = 300 - margin.top - margin.bottom;

    // Remove previous graph
    select(target).selectAll("svg").remove();

    // Create canvas
    var canvas = select(target)
        .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // Add x axis in date format
    let msUnixTimes: number[] = data.yields.map(y => Number(y.timestampUnix) * 1000);
    var x = scaleTime()
        .domain([new Date(Math.min(...msUnixTimes)), new Date(Math.max(...msUnixTimes))])
        .range([0, width]);
    canvas.append("g")
        .attr("transform", "translate(0," + height + ")")
        .attr("class", "axis")
        .call(axisBottom(x));

    // Add y axis in number format
    let yields: number[] = data.yields.map(y => y.yieldKw);
    var y = scaleLinear()
        .domain([Math.min(...yields), Math.max(...yields)])
        .range([height, 0]);
    canvas.append("g")
        .attr("class", "axis")
        .call(axisLeft(y));

    // Add the line
    const graphline = line<PredictedYield>()
        .x(d => x(Number(d.timestampUnix) * 1000))
        .y(d => y(d.yieldKw))
        .curve(curveMonotoneX);
    canvas.append("path")
        .datum(data.yields)
        .attr("fill", "none")
        .attr("stroke", "#fe9929")
        .attr("stroke-width", 1.5)
        .attr("d", graphline);
}

// Export functions to be used in other modules
export { createGraph, generateData };

