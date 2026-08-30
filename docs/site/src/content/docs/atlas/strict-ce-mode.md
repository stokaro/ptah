---
title: Strict Community Edition mode
description: Run ptah-compat against the pinned Atlas Community Edition surface without silently losing Ptah capabilities.
type: status
audience:
  - "atlas-migrator"
  - "evaluator"
readerQuestion: "What changes when PTAH_ATLAS_STRICT_COMPAT is enabled?"
goal: "Run and interpret a strict Atlas CE conformance invocation."
sourceOfTruth:
  - "internal/atlascompatpolicy"
  - "internal/envbool"
  - "cmd/atlas"
generated: false
lastVerified: "2026-08-30"
evidence:
  - "internal/atlascompatpolicy"
  - "cmd/atlas/strict_compat_policy_test.go"
  - "stokaro/ptah-atlas-conformance"
searchAliases:
  - PTAH_ATLAS_STRICT_COMPAT
  - Atlas CE mode
overlaps:
  - "/atlas/overview/"
  - "/atlas/conformance/"
  - "/reference/extension-variables/"
disposition: keep
---

Strict mode is a conformance profile for comparison with the pinned Atlas
Community Edition binary. It is not the recommended profile for ordinary Atlas
migrations and it does not affect native `ptah`.

Enable it for one process:

```bash
PTAH_ATLAS_STRICT_COMPAT=1 ptah-compat schema inspect --help
```

The selector is an environment variable rather than a flag because the strict
command surface is measured against Atlas CE. Adding a Ptah-only flag would
change the very surface being compared.

## What changes before dispatch

Ptah constructs the CE command and flag tree before Cobra parses the
invocation. Gated verbs use a Ptah-owned diagnostic that tells the operator to
unset the selector for the complete compatibility surface. Known Ptah flag
environment bindings and gated feature toggles are validated before help,
version, arguments, project files, or database access.

Strict mode does not reserve every `PTAH_*` name. An `atlas.hcl` file may use
`getenv` to read ordinary project inputs, so unrelated environment values remain
available. The classified extension variables and their strict behavior are in
[Extension environment variables](../../reference/extension-variables/).

## Content strict mode refuses

Strict mode rejects content Atlas CE cannot represent safely instead of
discarding it:

- Pro-only authored or live schema objects;
- YAML desired-schema sources;
- an authored `schema apply` lint policy the CE execution path cannot enforce;
- extended `atlas.hcl` evaluation outside the supported CE subset;
- Atlas txtar, Ptah directives, and SQL templates for commands that execute,
  convert, or replay migration bodies.

Checksum-only migration reads preserve those bytes because they do not execute
or reinterpret the body.

Live schema inspect, apply, diff, and clean inventory catalog-only object kinds
before output or mutation. A Pro-only object stops the operation; the result
cannot silently describe less than the database contains. Cleanup validates the
writer's full destruction inventory, including dependent objects.

## Capabilities retained deliberately

Strict mode preserves safety and correctness changes where copying CE would
lose data, hide state, or fail for an unrelated reason. Those measured
differences are [Retained divergences](../retained-divergences/), not parity
claims.

With the selector absent, `ptah-compat` retains every implemented Atlas
Pro-like and best-effort capability. PostgreSQL extensions, sequences, and
row-level security policies remain reachable on the compatibility surface; a
Pro migration should not have to be rewritten against native command names to
keep them.

## Verify both profiles

Run CE differential and CLI-surface tests with the selector enabled. Run
Pro-retention and ordinary compatibility tests with it absent. Passing only the
strict profile proves nothing about the default migration surface, and command
registration alone proves neither runtime behavior.

Use [Conformance](../conformance/) for the current measured contours and
[Feature matrix](../feature-matrix/) for capability-level status.
