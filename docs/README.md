# Ptah Documentation

The human-facing documentation site source lives in [`site`](site).

Start with:

- [Install](site/src/content/docs/start/install.mdx)
- [Quick start: versioned migrations](site/src/content/docs/start/quick-start-migrations.mdx)
- [Quick start: direct schema changes](site/src/content/docs/start/quick-start-direct.mdx)
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
do is read: `owns:` proves the link is mutual, unique and resolves to a real
file, and never that the page explains the feature. The file states that limit
in its own `notice` field, along with the eight other things it does not claim.

## Contributing to the documentation

- [Style guide](STYLE_GUIDE.md) — authoritative rules for page types,
  templates, voice, terminology, examples, links, and the review checklist.
- [Content inventory](site/CONTENT_INVENTORY.md) — per-page audit, reader
  journeys, target navigation, and the inventory maintenance rule. Update it
  in the same PR whenever a reader-facing page is added, moved, merged, split,
  or retired.
