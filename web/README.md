# NYC Taxi · Pricing Lab

Buildless interactive presentation of the project's archived 2024 zone-hour data.

```bash
cd web
npm test
npm run dev
# http://localhost:4173
```

Node.js 20+ runs the dependency-free tests; Python 3 serves the static files.
Deploy `dist/` to any static host. There is no JavaScript installation or build step.

- Click a map zone or use the accessible dropdown; select an hour.
- Explore historical fare / volume profiles or switch to conditional scenarios.
- Adjust fares (−10% to +15%) and assumed elasticity (−3 to 0).
- Save scenario A, change the controls, and compare against B.
- Copy URL state or export scenario curves / historical zone observations as CSV.
- A shared URL restores controls, not the session's saved comparison A.

[Method, source provenance, and refresh instructions](../docs/interactive-lab.md).
All indices are context-specific, baseline 100. Historical associations are not used as causal elasticities. No unsupported weather, supply, or event effects are included.
