# Interactive pricing lab

Implements the agreed interactive presentation: select a zone/hour, explore archived observations, adjust price and elasticity, compare two scenarios, inspect curves, and share/export the current scenario.

## Design and behavior

- A map-first, compact analytical workspace inspired by the parameter → simulation → feedback interaction of LoadSketch. NYC yellow, warm white, charcoal type; blue for historical observations, amber for scenarios. No decorative imagery or external chart/runtime dependencies.
- Desktop: controls beside a geographical map, indicators and curves. Mobile: stacked panels; dropdown alternatives to map selection, labeled native sliders, visible focus states.
- Historical mode shows observed zone-hour prices and volume profiles. Scenario mode defaults to an explicitly assumed elasticity of -1, bounded [-3, 0], and a price change in the notebook's original [-10%, +15%] range.
- The notebook's linear response formula is retained: Q/Q0 = max(0, 1 + elasticity × price_change); revenue index = 100 × (1 + price_change) × Q/Q0. Both indices start at 100. These are conditional scenarios, not identified causal forecasts or real-world pricing recommendations.
- Positive/missing historical association coefficients are displayed as context and are never silently imputed or used as causal elasticities. Missing zone-hour observations produce an explicit empty state.
- Archived counts average weekday-category totals over the 2024 input window; they are not hourly arrival rates. Dollar revenue would also require a consistent joint fare/distance aggregation. This app therefore uses indices, not claimed dollar revenue.
- All 6,259 observations are retained. Prices above $100/mile receive a visible source-quality warning; the interface does not silently remove or fix outliers.
- Source and contributors: original team project; Jeremy's original focus is price sensitivity. Notebook authorship is retained. TLC sources are linked in the interface.

## Source and commands

`web/dist/` is a buildless static website. `web/dist/data/` is a deterministic export of the committed parquet plus simplified official TLC polygons and zone labels. No raw trip downloads, API keys, cookies, analytics, or account creation.

```bash
cd web
npm test
npm run dev
# open http://localhost:4173
```

To refresh exported data, from the repo root:

```bash
pip install pandas pyarrow pyshp shapely
python scripts/export_web_data.py
```

The exporter records source SHA-256 and geometry source. Geometry is simplified in its original projected coordinate system for display, not for geographic measurement. Zones without geometry remain available in the selector. No weekday filter is offered: that dimension is not present in the committed master table.

## Acceptance checks

- Scenario baseline and unit-elasticity arithmetic verified by Node's native test runner, including invalid URL/input boundaries.
- Every exported row matches the committed source; no duplicate zone/hour keys or non-standard JSON NaN tokens.
- Browser checks: zone/time selection, sliders, historical switch, comparison, reset, share state, download, empty/error handling and responsive layouts.
- Static files work on ordinary static hosting; Sites publication is owner-private unless separately shared.
