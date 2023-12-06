import { select } from "d3-selection";
import type { Selection } from "d3-selection";
import { transition } from "d3-transition";
import { scaleLinear, scaleTime } from "d3-scale";
import type { ScaleLinear, ScaleTime } from "d3-scale";
import { timeHour } from "d3-time";
import { axisBottom, axisLeft } from "d3-axis";
import { line, curveMonotoneX } from "d3-shape";

import type { GetPredictedTimeseriesResponse, PredictedYield } from "../proto/api";

/**
 * Generate random data for testing purposes
 * @param scale Scale of the data (default: 100)
 * @returns Randomly generated data
 */
export function generateData(scale?: number): GetPredictedTimeseriesResponse {

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
 * Class for creating a timeseries graph
 * The graph is created in the given DOM element
 * @param target Target DOM element id
 */
export class TimeseriesGraph {

    private svg: Selection<SVGSVGElement, unknown, HTMLElement, any>; 
    private x: ScaleTime<number, number, never>;
    private y: ScaleLinear<number, number, never>;
    private yAxis: Selection<SVGGElement, unknown, HTMLElement, any>;
    
    margin = {top: 30, right: 12, bottom: 30, left: 60};
    size = {width: 600, height: 300};

    constructor(target: string) {

        select(target).selectAll("svg").remove();
        
        this._createSvg(target);
        this._initAxes();

        this.update = this.update.bind(this);
    }

    /**
     * Create the SVG canvas for the graph
     * @param target Target DOM element id
     */
    private _createSvg(target: string): void {
        
        if (target[0] !== "#") {
            target = "#" + target;
        }

        // Append SVG canvas to the target element
        this.svg = select(target)
            .append("svg")
                .attr("width", '100%')
                .attr("height", '100%')
                .attr("viewbox", [0, 0, "100%", "100%"])
                .attr("preserveAspectRatio", "xMinYMin meet")
            
        this.svg.append("g")
                .attr("transform", `translate(${this.margin.left},${this.margin.top})`);

        // Get the width and height of the canvas
        var node = this.svg.node();
        if (node) {
            this.size.width = node.getBoundingClientRect().width;
            this.size.height = node.getBoundingClientRect().height;
        }
    }

    private _initAxes(): void {
        // Add the x axis
        this.x = scaleTime()
            .domain(this._getWindow())
            .range([this.margin.left, this.size.width-this.margin.right]);
        this.svg.append("g")
            .attr("transform", `translate(0,${this.size.height - this.margin.bottom})`)
            .attr("class", "x-axis")
            .call(axisBottom(this.x).tickSizeOuter(0).scale(this.x));

        // Add the y axis with a default range
        this.y = scaleLinear()
            .domain([0, 12000*Math.pow(10, 3)])
            .range([this.size.height - this.margin.bottom, this.margin.top]);
        this.yAxis = this.svg.append("g")
            .attr("transform", `translate(${this.margin.left},0)`)
            .attr("class", "y-axis")
            .call(g => g.select(".domain").remove())
            .call(g => g.selectAll(".tick line").clone()
                .attr("x2", this.size.width - this.margin.left - this.margin.right)
                .attr("stroke-opacity", 0.3))
            .call(g => g.append("text")
                .attr("text-anchor", "start")
                .attr("x", -this.margin.left)
                .attr("y", 10)
                .attr("fill", "currentColor")
            .text("↑ Yield (kW)"))
            .call(axisLeft(this.y).scale(this.y));
    }


    /**
     * Get the window of the timeseries funciton to be displayed in the graph
     * The window goes from the start of the day 2 days ago
     * to the end of the day 2 days in the future
     * @returns Start and end of the window
     */
    _getWindow(): [Date, Date] {
        var now = new Date();
        const start = new Date(new Date(now.getTime() - 2 * 86400000).toDateString()); 
        const end = new Date(new Date(now.getTime() + 2 * 86400000).toDateString());
        return [start, end];
    }

    update(data: PredictedYield[]) {
    
        // Update the y axis
        this.y.domain([0, Math.max(...data.map(d => d.yieldKw))]);
        var t = transition().duration(350);
        this.yAxis.transition(t).call(axisLeft(this.y).scale(this.y));


        // Create the line generator
        const lineGenerator = line<PredictedYield>()
            .x(d => this.x(Number(d.timestampUnix) * 1000))
            .y(d => this.y(d.yieldKw))
            .curve(curveMonotoneX);

        // Create update selection
        // var u = this.svg.selectAll("path").data([data]).enter().append("path");
        // Update the line
        this.svg.append("path")
            .datum(data)
            .attr("fill", "none")
            .attr("stroke", "#FFAC5F")
            .attr("stroke-width", 1.5)
            .attr("d", lineGenerator);
    }
}

