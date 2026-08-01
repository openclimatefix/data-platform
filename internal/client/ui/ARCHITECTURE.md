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
Spans the entire height of the right column, sitting adjacent to Panels 1 and 2. Displays a dynamically responsive geographical map of the selected location, focused primarily on the data shapes.

## 3. Component Details & Functionality

### 3.1. Query Configuration (Form)
The form submits via an HTMX `hx-get` request to the backend. It consists of two rows of inputs:

**Row 1 (33/33/33 Split):**
- **Location Selector:** 
  - `text` input mapped to an HTML5 `<datalist>`.
  - Options display: `Location Name (Type, Capacity)`.
  - **Behavior:** Standard browser fuzzy search. Selecting an option fires vanilla JS that maps the selected text to its underlying `UUID` and stores it in a hidden input for submission.
- **Forecaster Selector (Multi-Select):** 
  - `text` input mapped to an HTML5 `<datalist>`.
  - **Behavior:** Selecting an option creates a visual "chip" and a hidden input. Crucially, the selected option is *removed* from the datalist to prevent duplicates.
  - **Selected Badge:** An inline button displaying "X Selected". Clicking it toggles a popover (`z-index: 50`) listing the selected chips. Clicking the "x" on a chip destroys the hidden input and re-injects the option back into the `<datalist>`.
- **Observer Selector (Multi-Select):**
  - Follows the identical custom `<datalist>` behavior as the Forecaster selector, enabling timeseries observations to be overlaid alongside forecasts.

**Row 2 (25/25/50 Split):**
- **Energy Source:** A standard HTML `<select>` dropdown (Options: "Solar", "Wind").
- **Horizon Minutes:** An HTML5 `<input type="number">` constrained with `min="0"`, `max="2160"`, and `step="5"`.
- **Time Window:** A single text input progressively enhanced by `flatpickr`. Opens a calendar popover allowing the user to select a contiguous Date/Time range.

### 3.2. Forecast Results (Chart)
Powered by **ECharts** running from a locally hosted minified script, allowing for robust timeseries functionality, interactive tooltips, and rendering of probabilistic data bounds without external charting dependencies.

- **Header:** Displays the Location Name and UUID.
- **Legend:** A flexbox row mapping model names to colored circles corresponding to the chart lines.
- **Graph:** 
  - **Layout:** Flexes to fill the container height, with a minimum height of `320px`. Automatically resizes via `ResizeObserver`.
  - **Lines:** P50 predictions are plotted as smooth lines. Observation data lines are automatically rendered with a `dashed` stroke style to distinguish them visually.
  - **Probabilistic Bands:** If P10 and P90 data is returned by the backend, it is rendered as a semi-transparent layered area behind the main predictive lines (`opacity: 0.2`) to visually denote certainty ranges.
  - **Axes:** Grid lines are styled to match light/dark mode seamlessly. The X-axis dynamically spaces labels (Jan 02 15:00) cleanly, avoiding overlap.
  - **Tooltips:** Hovering over the chart renders dynamic tooltips showing formatted watts (W, kW, MW, GW) for the P50 line, explicitly appending `(P10: ... - P90: ...)` if probabilistic limits exist.
- **Interactivity:** Clicking anywhere on the chart grid captures the timestamp beneath the cursor, updating the map panel instantly.

### 3.3. Location Map
Powered by **ECharts** native geo bindings. Eschews topographic tile-layers (like Leaflet) completely in favor of high-contrast data visualization.

- **Header:** Displays the explicit Latitude and Longitude coordinates. Updates dynamically to indicate `"Snapshot: [Time]"` when the timeseries chart is clicked.
- **Interactivity:** Pan/Zoom disabled. Click interactions in the chart update the map opacity directly via ECharts' fast `setOption` merges, reflecting the snapshot generation fraction at that specific instant.
- **Rendering:**
  - If the backend provides GeoJSON for the location, the map draws the geometry natively as an ECharts `geo` layer and sizes it optimally in the center of the panel.
  - If no GeoJSON is provided (or if it is a single point), the map renders a custom coordinate `scatter` marker centered on a blank hidden Cartesian grid.

## 4. Design & Styling (CSS)
- **Framework:** `@exampledev/new.css` (Classless CSS framework for minimal overhead).
- **Theme Support:** Dark mode is supported natively via `@media (prefers-color-scheme: dark)`, matching background variables securely. The ECharts elements inherit `transparent` backgrounds.
- **Palette:** Soft off-white backgrounds (`#f4f5f7`), with custom deep-green (`#7a9374`) accents for chart lines and map geometry fills. Form inputs use native styling modified with soft borders to look distinct but subdued.
