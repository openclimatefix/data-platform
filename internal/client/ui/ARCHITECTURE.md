# Data Platform UI Specification

This specification outlines the architecture, layout, design, and functionality of the internal Data Platform overview dashboard.

## 1. Core Architecture & Philosophy
- **Embedded Deployment:** The UI is served directly from the core Data Platform Go binary using `//go:embed` and `html/template`.
- **Zero-Build Frontend:** No npm, Webpack, React, or TypeScript. The frontend relies purely on HTML, standard CSS, and vanilla JS.
- **Server-Side Rendering (SSR):** Interactivity is powered by **HTMX**. Form submissions request compiled HTML partials from the backend, which are swapped into the DOM seamlessly.
- **Browser-Native Primacy:** Use native HTML5 components (e.g., `<datalist>`, `<input type="number">`) wherever possible before falling back to custom JavaScript.

## 2. Layout Structure
The UI utilizes a Single Page Application (SPA) feel powered by HTMX (`hx-boost`). The structure is split across two primary modes: **Analysis** and **Dashboard**, both utilizing the same core CSS grid.

**Global Layout (`base.html`):**
- **Header Navigation:** A persistent top-level navigation bar linking to the Analysis and Dashboard modes.
- **Grid:** A 2-column layout (`grid-template-columns: 2fr 1fr`) constrained to a maximum width of `1400px`.
- **Panels:** All distinct UI sections are housed in white "cards" (`border-radius: 16px`, faint drop shadow, `1px` soft border).

**Panel 1: Query Configuration (Top Left)**
Spans the full width of the left column. Houses the form elements to filter data.

**Panel 2: Forecast Results Chart (Bottom Left)**
Spans the full width of the left column below Panel 1. Displays the generated timeseries chart.

**Panel 3: Location Map (Right Column)**
Spans the entire height of the right column, sitting adjacent to Panels 1 and 2. Displays a dynamically responsive geographical map of the selected location, focused primarily on the data shapes.

## 3. Modes & Functionality

### 3.1. Analysis Mode
Designed for data science teams to inspect timeseries overlap, compare forecasters against observers, and validate raw predictive accuracy.

**Configuration:**
- **Location Selector:** Uses fuzzy search across all Location Types.
- **Sources (Multi-Select):** Allows unlimited Forecaster and Observer selection to overlay alongside each other on the chart.
- **Chart:** Plots all selected timeseries, incorporating probabilistic bounds (P10/P90) where available.
- **Map:** Displays a single boundary polygon representing the requested Location. Map opacity updates based on the aggregate data fraction when scrubbing the chart.

### 3.2. Dashboard Mode
Designed for grid operators, this mode focuses on the hierarchical relationship between large areas (Nations) and their constituent distribution components (GSPs).

**Configuration:**
- **Location Selector:** Strictly filters to `LOCATION_TYPE_NATION`.
- **Source:** Limited to a single Forecaster model.
- **Chart Drill-Down:** The chart initially loads displaying the national aggregate timeseries. Clicking a specific GSP directly on the interactive map triggers an HTMX partial-swap, instantly replacing the Nation chart with that specific GSP's localized timeseries.

**Map Interaction:**
- **Asynchronous Snapshot Loading:** To avoid massive data payloads, the dashboard fetches all GSP boundaries asynchronously using a crisp simplification level (`0.005` degrees). 
- **Time Scrubbing:** Clicking the timeseries chart triggers a lightweight JSON `fetch()` to `/api/dashboard/map-snapshot`, which instantly re-colorizes all GSPs to reflect their specific generation percentage at that given timestamp.
- **Choropleth VisualMap:** Employs an interactive ECharts `visualMap` component. Users can drag the High/Low filters on the legend to dynamically hide/show GSPs in real-time based on their generation.

## 4. Design & Styling (CSS)
- **Framework:** `@exampledev/new.css` (Classless CSS framework for minimal overhead).
- **Theme Support:** Dark mode is supported natively via `@media (prefers-color-scheme: dark)`, matching background variables securely. The ECharts elements inherit `transparent` backgrounds.
- **Palette:** Soft off-white backgrounds (`#f4f5f7`), with custom deep-green (`#7a9374`) accents for chart lines and map geometry fills. Form inputs use native styling modified with soft borders to look distinct but subdued.
