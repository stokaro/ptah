# Schema UI screenshot source

`schema-document.png`, `schema-serve-matches.png`, and
`schema-serve-drift.png` are screenshots of Ptah's generated HTML, not authored
diagrams.

The schema-document screenshots and downloadable HTML use the canonical static
fixture at `docs/site/fixtures/source-equivalence/schema.sql`. This keeps the
source-neutral `schema export` feature from looking dependent on Go annotations.

The schema-serve screenshots use `docs/site/fixtures/schema-ui/`, a necessary
Go-specific product UI fixture because `schema serve` currently accepts Go
annotations only. Its matching and drift variants share the same conceptual
shop schema on purpose.

Generator: `docs/site/scripts/generate-schema-ui-assets.mjs`

Viewport: 1200 by 760 CSS pixels, Chromium from the version pinned in
`docs/site/package-lock.json`.

Regenerate from the repository root after building `bin/ptah`:

```bash
cd docs/site
PTAH_BIN=../../bin/ptah node scripts/generate-schema-ui-assets.mjs
```

The generator creates one matching SQLite database and one database built from
the base model without `orders.placed_at`. The latter produces the documented
warning drift state. It replaces the comparison time and temporary database
path in the rendered DOM before capture, and pins the version in the exported
document's footer, so the committed pixels and samples contain no
machine-specific path, volatile timestamp, or per-build version.

Review owner: documentation maintainers. Re-run the generator when
`internal/schemadoc`, `cmd/internal/schemaserve`, or either named fixture
changes.
