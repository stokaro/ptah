# Ptah Docs Site

This directory contains the Astro + Starlight documentation site.

```bash
npm ci
ASTRO_TELEMETRY_DISABLED=1 npm run build
npm run versions:selftest
npm audit --audit-level=low
```

For local development:

```bash
ASTRO_TELEMETRY_DISABLED=1 npm run dev
```

The site is versioned by `DOCS_VERSION`; `edge` tracks `master`.
