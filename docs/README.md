# Ptah Documentation

The human-facing documentation site source lives in [`site`](site).

Start with:

- [Quick start](site/src/content/docs/start/quick-start.md)
- [Comparison](site/src/content/docs/atlas/comparison.md)
- [Declarative database testing](testing.md)
- [Troubleshooting](site/src/content/docs/operate/troubleshooting.md)

The other Markdown files in this directory remain only where they provide
contributor or implementation detail beyond the site.

## Documentation layers

| Layer | Purpose |
| --- | --- |
| `docs/site` | Human-facing documentation site and task-oriented guides. |
| `docs/*.md` | Detailed source references for commands, config, dialects, and design. |
| `docs/adr` | Architecture decision records: the alternatives weighed and the boundary chosen, kept so a later reader can tell a decision from an accident. |
| `docs/canonical_pipeline_prototype.md` | What the ADR 0001 prototype measured, and what it changed about the record. |
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

## Contributing to the documentation

- [Style guide](STYLE_GUIDE.md) — authoritative rules for page types,
  templates, voice, terminology, examples, links, and the review checklist.
- [Content inventory](site/CONTENT_INVENTORY.md) — per-page audit, reader
  journeys, target navigation, and the inventory maintenance rule. Update it
  in the same PR whenever a reader-facing page is added, moved, merged, split,
  or retired.
