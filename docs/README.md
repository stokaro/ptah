# Ptah Documentation

The human-facing documentation site source lives in [`site`](site).

Start with:

- [Quick start](site/src/content/docs/start/quick-start.md)
- [Comparison](site/src/content/docs/atlas/comparison.md)
- [Declarative database testing](testing.md)
- [Troubleshooting](site/src/content/docs/operate/troubleshooting.md)

The other markdown files in this directory remain detailed source references for
commands, configuration, dialects, conformance, and design.

## Documentation layers

| Layer | Purpose |
| --- | --- |
| `docs/site` | Human-facing documentation site and task-oriented guides. |
| `docs/*.md` | Detailed source references for commands, config, dialects, and design. |
| `examples/*` | Runnable local examples and generated artifacts. |
| `ptah-atlas-conformance` | External Atlas compatibility evidence and gap reports. |

When a task is covered by both the site and a source reference, use the site
for the workflow and the source reference for exact flags, schema shapes, or
implementation details.

## Maintenance rule

When Ptah behavior changes, update both layers that readers will hit:

- the task page in `docs/site/src/content/docs/`;
- the exact source reference in `docs/*.md`, `examples/*`, package docs, or
  conformance reports.

Do not update only the nearest README when a command path, flag, config key,
generated SQL shape, public API, or Atlas parity claim changes.

## Contributing to the documentation

- [Style guide](STYLE_GUIDE.md) — authoritative rules for page types,
  templates, voice, terminology, examples, links, and the review checklist.
- [Content inventory](site/CONTENT_INVENTORY.md) — per-page audit, reader
  journeys, target navigation, and the inventory maintenance rule. Update it
  in the same PR whenever a reader-facing page is added, moved, merged, split,
  or retired.
