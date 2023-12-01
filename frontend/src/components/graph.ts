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
    var margin = {top: 30, right: 12, bottom: 30, left: 60};

    // Remove previous graph
    select(target).selectAll("svg").remove();

    // Create svg canvas (container)
    var canvas = select(target)
        .append("svg")
            .attr("width", '100%')
            .attr("height", '100%')
            .attr("viewbox", [0, 0, "100%", "100%"])
            .attr("preserveAspectRatio", "xMinYMin meet")
    
    // Get the width and height of the canvas
    var width = 600;
    var height = 300;
    var node = canvas.node();
    if (node) {
        width = node.getBoundingClientRect().width;
        height = node.getBoundingClientRect().height;
    }

    // Declate the x axis
    let msUnixTimes: number[] = data.yields.map(y => Number(y.timestampUnix) * 1000);
    const x = scaleTime()
        .domain([new Date(Math.min(...msUnixTimes)), new Date(Math.max(...msUnixTimes))])
        .range([margin.left, width-margin.right]);
    // Decalre the y axis
    let yields: number[] = data.yields.map(y => y.yieldKw);
    const y = scaleLinear()
        .domain([Math.min(...yields), Math.max(...yields)])
        .range([height - margin.bottom, margin.top]);
    // Add x axis
    canvas.append("g")
        .attr("transform", `translate(0,${height - margin.bottom})`)
        .attr("class", "axis")
        .call(axisBottom(x).tickSizeOuter(0));

    // Add y axis
    canvas.append("g")
        .attr("transform", `translate(${margin.left},0)`)
        .attr("class", "axis")
        .call(axisLeft(y))
        .call(g => g.select(".domain").remove())
        .call(g => g.selectAll(".tick line").clone()
            .attr("x2", width - margin.left - margin.right)
            .attr("stroke-opacity", 0.3))
        .call(g => g.append("text")
            .attr("text-anchor", "start")
            .attr("x", -margin.left)
            .attr("y", 10)
            .attr("fill", "currentColor")
            .text("↑ Yield (kW)"));

    // Create the line generator
    const graphline = line<PredictedYield>()
        .x(d => x(Number(d.timestampUnix) * 1000))
        .y(d => y(d.yieldKw))
        .curve(curveMonotoneX);

    // Add the line
    canvas.append("path")
        .datum(data.yields)
        .attr("fill", "none")
        .attr("stroke", "#ccc")
        .attr("stroke-width", 1.5)
        .attr("d", graphline);
}

// Export functions to be used in other modules
export { createGraph, generateData };

