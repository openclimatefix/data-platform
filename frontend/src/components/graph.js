import { select  } from "d3-selection";
import { scaleLinear, scaleTime } from "d3-scale";
import { timeHour } from "d3-time";
import { extent } from "d3-array";
import { axisBottom, axisLeft } from "d3-axis";
import { line } from "d3-shape";

function generateData() {
    // Generate data with random y value, and an x value of every hour
    // between two days ago and two days ahead
    var now = new Date();
    var data = timeHour
        .every(1)
        .range(new Date(now.getTime() - 2 * 86400000), new Date(now.getTime() + 2 * 86400000))
        .map(function(time) {
            return {
                time: time,
                value: Math.random() * 100
            };
        });
    return data;
}


function createGraph(target, data) {

    target = "#" + target;
    
    // set the dimensions and margins of the graph
    var margin = {top: 30, right: 30, bottom: 30, left: 60},
        width = 600 - margin.left - margin.right,
        height = 300 - margin.top - margin.bottom;

    // Remove previous graph
    select(target).selectAll("svg").remove();

    // Add svg
    var svg = select(target)
        .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // Add x axis in date format
    var x = scaleTime()
        .domain(extent(data, function(d) { return d.time; }))
        .range([0, width]);
    svg.append("g")
        .attr("transform", "translate(0," + height + ")")
        .attr("class", "axis")
        .call(axisBottom(x));
    // Add y axis in number format
    var y = scaleLinear()
        .domain(extent(data, function(d) { return d.value; }))
        .range([height, 0]);
    svg.append("g")
        .attr("class", "axis")
        .call(axisLeft(y));

    // Add the line
    svg.append("path")
        .datum(data)
        .attr("fill", "none")
        .attr("stroke", "#fe9929")
        .attr("stroke-width", 1.5)
        .attr("d", line()
        .x(function(d) { return x(d.time) })
        .y(function(d) { return y(d.value) })
    );
}

export { createGraph, generateData };
