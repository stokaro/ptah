# Schema UI screenshot source

`schema-document.png`, `schema-serve-matches.png`, and
`schema-serve-drift.png` are screenshots of Ptah's generated HTML, not authored
diagrams.

Source fixture: `docs/site/fixtures/schema-ui/`

Generator: `docs/site/scripts/generate-schema-ui-assets.mjs`

Viewport: 1440 by 900 CSS pixels, Chromium from the version pinned in
`docs/site/package-lock.json`.

Regenerate from the repository root after building `bin/ptah`:

```bash
cd docs/site
PTAH_BIN=../../bin/ptah node scripts/generate-schema-ui-assets.mjs
```

The generator creates one matching SQLite database and one database built from
the base model without `orders.placed_at`. The latter produces the documented
warning drift state. It replaces the comparison time and temporary database
path in the rendered DOM before capture, so the committed pixels contain no
machine-specific path or volatile timestamp.

Review owner: documentation maintainers. Re-run the generator when
`internal/schemadoc`, `cmd/internal/schemaserve`, or the fixture changes.
