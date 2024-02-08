import { select } from "d3-selection";
import type { Selection } from "d3-selection";
import { transition } from "d3-transition";
import { scaleLinear, scaleTime } from "d3-scale";
import type { ScaleLinear, ScaleTime } from "d3-scale";
import { bisector } from "d3-array";
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
    let maxValue = 100;
    if (scale !== undefined) {
        maxValue = scale;
    }

    const now = new Date();
    // The x values are a time window with the following definition:
    // * Start: 2 days ago
    // * End: 2 days in the future
    // * Interval: 1 hour
    const allHours: Date[]  = timeHour
        .range(new Date(now.getTime() - 2 * 86400000), new Date(now.getTime() + 2 * 86400000));

    // The y values are random numbers between 0 and the scale
    const data: PredictedYield[] = allHours
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

    private data: PredictedYield[] = [];
    private svg: Selection<SVGSVGElement, unknown, HTMLElement, any> = select("svg");
    private x: ScaleTime<number, number, never> = scaleTime();
    private y: ScaleLinear<number, number, never> = scaleLinear();
    private yAxis: Selection<SVGGElement, unknown, HTMLElement, any> = select("g.y-axis");
    private line: Selection<SVGPathElement, unknown, HTMLElement, any> = select("path");

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
        const node = this.svg.node();
        if (node) {
            this.size.width = node.getBoundingClientRect().width;
            this.size.height = node.getBoundingClientRect().height;
        }
    }

    /**
     * Initialize the axes of the graph
     * @returns void
     */
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

        // Append an as yet undrawn path for the data
        this.line = this.svg.append("path")
            .attr("class", "line")
            .attr("fill", "none")
            .attr("stroke", "#FFAC5F")
            .attr("stroke-width", 1.5);

    }


    /**
     * Get the window of the timeseries funciton to be displayed in the graph
     * The window goes from the start of the day 2 days ago
     * to the end of the day 2 days in the future
     * @returns Start and end of the window
     */
    _getWindow(): [Date, Date] {
        const now = new Date();
        const startOfDay = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));
        const start = new Date(startOfDay.getTime().valueOf() - 2 * 86400 * 1000);
        const end = new Date(startOfDay.getTime().valueOf() + 2 * 86400 * 1000);
        return [start, end];
    }

    update(data: PredictedYield[]) {

        const t = transition().duration(350);

        // Update the y axis
        this.y.domain([0, Math.max(...data.map(d => d.yieldKw))]);
        this.yAxis.transition(t).call(axisLeft(this.y).scale(this.y));

        // Create the line generator
        const lineGenerator = line<PredictedYield>()
            .x(d => this.x(new Date(Number(d.timestampUnix) * 1000)))
            .y(d => this.y(d.yieldKw))
            .curve(curveMonotoneX);

        this.line.attr("d", lineGenerator(data)).transition(t);
    }
}

