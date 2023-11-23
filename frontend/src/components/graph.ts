import { select  } from "d3-selection";
import { scaleLinear, scaleTime } from "d3-scale";
import { timeHour } from "d3-time";
import { axisBottom, axisLeft } from "d3-axis";
import { line, curveMonotoneX } from "d3-shape";

import type { GetPredictedTimeseriesResponse, PredictedYield } from "../proto/api";

function generateData(): GetPredictedTimeseriesResponse {
    // Generate data with random y value, and an x value of every hour
    // between two days ago and two days ahead
    var now = new Date();
    let allHours: Date[]  = timeHour
        .range(new Date(now.getTime() - 2 * 86400000), new Date(now.getTime() + 2 * 86400000));
    var data: PredictedYield[] = allHours
        .map(function(time) {
            return {
                timestampUnix: time.getTime() / 1000,
                yieldKw: Math.random() * 100
            };
        });
        return {
            yields: data,
            locationID: "generated-location",
        };
}


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
    let msUnixTimes: number[] = data.yields.map(y => y.timestampUnix * 1000);
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
        .x(d => x(d.timestampUnix * 1000))
        .y(d => y(d.yieldKw))
        .curve(curveMonotoneX);
    canvas.append("path")
        .datum(data.yields)
        .attr("fill", "none")
        .attr("stroke", "#fe9929")
        .attr("stroke-width", 1.5)
        .attr("d", graphline);
}

export { createGraph, generateData };
