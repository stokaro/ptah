# Ptah Documentation

The human-facing documentation site source lives in [`site`](site).

Start with:

- [Install](site/src/content/docs/start/install.mdx)
- [Quick start](site/src/content/docs/start/quick-start.mdx)
- [Quick start: versioned migrations](site/src/content/docs/start/quick-start-migrations.mdx)
- [Continue direct schema changes](site/src/content/docs/start/quick-start-direct.mdx)
- [Choose a workflow](site/src/content/docs/start/choose-a-workflow.md)
- [Troubleshooting](site/src/content/docs/operate/troubleshooting.md)

The other Markdown files in this directory remain only where they provide
contributor or implementation detail beyond the site.

## Documentation layers

| Layer | Purpose |
| --- | --- |
| `docs/site` | Human-facing documentation site and task-oriented guides. |
| `docs/*.md` | Detailed source references for commands, config, dialects, and design. |
| `docs/adr` | Architecture decision records: the alternatives weighed and the boundary chosen, kept so a later reader can tell a decision from an accident. |
| `docs/architecture_boundaries.md` | The measured boundary inventory and the executable invariant set, with the baseline the gate ratchets against. |
| `docs/canonical_pipeline_prototype.md` | What the ADR 0001 prototype measured, and what it changed about the record. |
| `docs/feature-inventory.json` | The derived feature register: every native verb, ledger package, released program and dialect, with the page that claims it. Generated. |
| `docs/source-support.json` | The generated command-by-source audit: exact invocation, implementation owner, evidence, composability, opt-in, limitation, and verification status for every audited cell. |
| `examples/*` | Runnable local examples and generated artifacts. |
| `ptah-atlas-conformance` | External Atlas compatibility evidence and gap reports. |

When a topic has both a site page and a source reference, use the site for the
reader workflow and the source reference for implementation detail. Do not keep
a repository-level copy that merely repeats the site.

## Maintenance rule

When Ptah behavior changes, update every relevant layer:

- the task page in `docs/site/src/content/docs/`;
- any implementation reference in `docs/*.md`, `examples/*`, package docs, or
  conformance reports that owns additional detail.

Do not update only the nearest README when a command path, flag, config key,
generated SQL shape, public API, or Atlas parity claim changes.

### The feature inventory

`docs/feature-inventory.json` is generated and carries no authored column.
Adding a verb, a ledger package, a released binary or a dialect is not an edit
here at all: the register is derived from the command tree, `docs/public_api.md`,
`.goreleaser.yaml` and `renderer.SupportedDialects`, so the row appears when the
declaration does.

The one thing a person writes is a page claiming what it documents:

```yaml
---
title: "Apply directly"
owns:
  - cli-ptah-schema-apply
---
```

Then regenerate:

```bash
scripts/check-feature-inventory.sh --write
```

The identifier is compared to the derived one by string equality, so a claim
naming nothing is refused with the page and the identifier. What the gate cannot
do is read. The column is called `claimed_by` for that reason: it proves the
claim resolves to a derived feature and that no second page makes it, and never
that the page explains the feature. No column says canonical, because nothing
here can check that.

Two floors sit under the register, and neither one lives in it. The claimed-row
count may not fall below `featureinventory.ClaimedFloor`, a constant in
`internal/featureinventory`, so raising coverage is a reviewed source edit and
lowering it is a red gate -- a ratchet read out of the file it guards is not a
ratchet. And a page under `runnable_examples` has to publish a step: the
`quickstart: true` marking is deliberate, but a deliberate marking is still a
claim.

The quick-start runner executes `bash` and `powershell` fences in their matching
tabs. A `console` fence outside those tabs is one shell-neutral command block;
the runner executes it in both programs. Use that form when the command is
byte-identical in Bash and PowerShell instead of publishing duplicate tabs.

The two words are deliberate. A page writes `owns:`, which is an author saying
what their page is for; the register answers `claimed_by`, `claimed` and
`claimed_floor`, which is all a machine can confirm about that sentence.

The file states those limits in its own `notice` field, with the rest of what it
does not claim. The count is not repeated here: `notice` is generated from
`featureinventory.Notice`, so a number written beside it is one more thing to
keep in step.

### The source-support manifest

`docs/source-support.json` is generated from the audited declarations in
`docs/site/scripts/build-source-support.mjs`. It distinguishes verified cells,
supported cells that still lack a command-level test, conditional uses,
deliberate exclusions, and product gaps. Do not infer one command's source
support from another command merely because they share a loader.

Regenerate and check the manifest against the built command tree:

```bash
node docs/site/scripts/build-source-support.mjs --write --binary ./bin/ptah
node docs/site/scripts/build-source-support.mjs --binary ./bin/ptah
scripts/check-source-equivalence.sh
scripts/check-source-workflows.sh
```

The equivalence gate renders every canonical source through the built CLI,
materializes the result on a fresh SQLite database, and compares the resulting
inspection JSON. Its fixture lives under
`docs/site/fixtures/source-equivalence/`.

The workflow gate takes the same fixtures through migration plan/generate,
shadow-verified brownfield baseline, a first subsequent change, and a static
Protobuf export. It also exercises OCI plan/generate through the focused Go
command test named in the manifest.

### The visual-output fixtures

`docs/site/visual-output-inventory.json` names each browser UI, report, diagram,
lineage, metrics, and contract output. `docs/site/visual-assets.json` connects
the reader-facing artifact to its fixture, generator, full-size result, and
manifest-backed browser proof.

Build Ptah first, then regenerate every product artifact from the committed
fixtures:

```bash
go build -o bin/ptah ./cmd/ptah
cd docs/site
PTAH_BIN=../../bin/ptah npm run assets:write
npm run check:visual-assets
```

The generated PNGs are review artifacts, not cross-platform pixel baselines.
Review their diff and the built pages at mobile and desktop widths; the browser
gate checks identity, placement, dimensions, actions, variants, theme behavior,
keyboard focus, and overflow without pretending that two rasterizers produce
byte-identical pixels.

## Contributing to the documentation

- [Style guide](STYLE_GUIDE.md) — authoritative rules for page types,
  templates, voice, terminology, examples, links, and the review checklist.
- [Generated content inventory](site/content-inventory.json) — the current
  page map, metadata, navigation placement, link graph, source size, and word
  counts. Regenerate it with `npm run inventory:write` in `docs/site`.
- [Inventory decisions](site/CONTENT_INVENTORY.md) — reader journeys and
  editorial decisions that cannot be derived mechanically.
