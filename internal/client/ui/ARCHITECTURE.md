# Data Platform UI Specification

This specification outlines the architecture, layout, design, and functionality of the internal Data Platform overview dashboard.

## 1. Core Architecture & Philosophy
- **Embedded Deployment:** The UI is served directly from the core Data Platform Go binary using `//go:embed` and `html/template`.
- **Zero-Build Frontend:** No npm, Webpack, React, or TypeScript. The frontend relies purely on HTML, standard CSS, and vanilla JS.
- **Server-Side Rendering (SSR):** Interactivity is powered by **HTMX**. Form submissions request compiled HTML partials from the backend, which are swapped into the DOM seamlessly.
- **Browser-Native Primacy:** Use native HTML5 components (e.g., `<datalist>`, `<input type="number">`) wherever possible before falling back to custom JavaScript.

## 2. Layout Structure
The UI follows a desktop-optimized, responsive grid layout split into three distinct panels.

**Global Layout:**
- **Header:** Full-width title ("Data Platform Overview").
- **Grid:** A 2-column layout (`grid-template-columns: 2fr 1fr`) constrained to a maximum width of `1400px`.
- **Panels:** All distinct UI sections are housed in white "cards" (`border-radius: 16px`, faint drop shadow, `1px` soft border).

**Panel 1: Query Configuration (Top Left)**
Spans the full width of the left column. Houses the form elements to filter data.

**Panel 2: Forecast Results Chart (Bottom Left)**
Spans the full width of the left column below Panel 1. Displays the generated timeseries chart.

**Panel 3: Location Map (Right Column)**
Spans the entire height of the right column, sitting adjacent to Panels 1 and 2. Displays a static geographical map of the selected location.

## 3. Component Details & Functionality

### 3.1. Query Configuration (Form)
The form submits via an HTMX `hx-get` request to the backend. It consists of two rows of inputs:

**Row 1 (50/50 Split):**
- **Location Selector:** 
  - `text` input mapped to an HTML5 `<datalist>`.
  - Options display: `Location Name (Type, Capacity)`.
  - **Behavior:** Standard browser fuzzy search. Selecting an option fires vanilla JS that maps the selected text to its underlying `UUID` and stores it in a hidden input for submission.
- **Forecaster Selector (Multi-Select):** 
  - `text` input mapped to an HTML5 `<datalist>`.
  - **Behavior:** Selecting an option creates a visual "chip" and a hidden input. Crucially, the selected option is *removed* from the datalist to prevent duplicates.
  - **Selected Badge:** An inline button displaying "X Selected". Clicking it toggles a popover (`z-index: 50`) listing the selected chips. Clicking the "x" on a chip destroys the hidden input and re-injects the option back into the `<datalist>`.

**Row 2 (25/25/50 Split):**
- **Energy Source:** A standard HTML `<select>` dropdown (Options: "Solar", "Wind").
- **Horizon Minutes:** An HTML5 `<input type="number">` constrained with `min="0"`, `max="2160"`, and `step="5"`.
- **Time Window:** A single text input progressively enhanced by `flatpickr`. Opens a calendar popover allowing the user to select a contiguous Date/Time range.

### 3.2. Forecast Results (Chart)
Powered by **Chartist.js** (chosen because its pure SVG rendering avoids canvas memory-leak issues during HTMX DOM swaps).

- **Header:** Displays the Location Name and UUID.
- **Legend:** A flexbox row mapping model names to colored circles corresponding to the chart lines.
- **Graph:** 
  - **Layout:** Capped at `320px` height.
  - **Lines:** Plotted using smooth cardinal interpolation (`tension: 0.2`). No data points/vertices are rendered (`showPoint: false`).
  - **Axes:** Vertical grid lines are hidden via CSS. The X-axis dynamically spaces ticks to ensure only ~8 labels are visible at any time. The Y-axis enforces integer scaling and appends " W" (Watts) to the labels.
- **Data Handling:** P50 fractional data is multiplied by the location capacity on the backend (via Go template functions) to plot raw Watts. Multiple forecasters are supported; timestamps are unified and gaps are padded with `"null"` to ensure lines plot concurrently on the same X-axis.

### 3.3. Location Map
Powered by **Leaflet.js** using CARTO "Light No Labels" basemaps to provide a clean, minimalistic aesthetic.

- **Header:** Displays the explicit Latitude and Longitude coordinates.
- **Interactivity:** Completely locked down. No panning, scrolling, or zooming allowed.
- **Rendering:**
  - If the backend provides GeoJSON for the location, the map draws the polygon, colors it with a dynamic opacity tied to the average forecast generation, and automatically fits the map bounds to the geometry.
  - If no GeoJSON is provided, the map falls back to a custom CSS-styled circular dot marker at the target coordinates.

## 4. Design & Styling (CSS)
- **Framework:** `@exampledev/new.css` (Classless CSS framework for minimal overhead).
- **Theme Support:** Dark mode is supported natively via `@media (prefers-color-scheme: dark)`, which dynamically adjusts the Chartist labels and axis colors to remain legible.
- **Palette:** Soft off-white backgrounds (`#f4f5f7`), with custom deep-green (`#7a9374`) accents for chart lines and map geometry fills. Form inputs use `#fafafb` with soft `#e0e0e4` borders to look distinct but subdued.
