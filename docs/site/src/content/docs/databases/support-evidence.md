---
title: Database support evidence
description: How Ptah measures release-line support, attributes observations, and evaluates engines without a dialect.
type: status
audience:
  - "database-engineer"
  - "evaluator"
readerQuestion: "What evidence supports Ptah's database support claims?"
goal: "Trace support claims to release declarations, CI execution, and measured limitations."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - ".github/workflows/capability-matrix.yml"
  - ".github/workflows/go-integration-tests.yml"
generated: false
lastVerified: "2026-08-30"
evidence:
  - "internal/capabilityprobe/cells.go"
  - "internal/capabilityprobe/cells_test.go"
  - "stokaro/ptah#1879"
searchAliases:
  - database support measurement
  - unsupported database engines
overlaps:
  - "/databases/support-matrix/"
  - "/databases/support-policy/"
disposition: split
---

Ptah treats a support claim as a statement about executable evidence. A vendor
page can explain a release policy, but it cannot prove that Ptah parses,
renders, introspects, or migrates that engine correctly.

## Evidence chain

Each release line is declared in `internal/capabilityprobe/cells.go` with its
support level, capability preset, reproducible container when one exists, and
the source used to interpret the vendor's lifecycle. The capability-matrix
workflow reads that declaration instead of maintaining a second release list.

Three kinds of execution can support a line:

- the capability probe starts the declared cell and measures its statement set;
- the integration workflow starts that server and runs Ptah behavior against it;
- SQLite is compiled into the binary and exercised by the ordinary Go test
  suite.

A census test derives both sides of the certification rule from the declaration
and workflows. A non-emulated line that CI exercises must be `certified` or
`legacy-tested`; a line nothing exercises must be `best-effort`. The generated
[support matrix](../support-matrix/) derives all counts, release enumerations,
non-probed reasons, and container-tag notes from the same declaration.

## Attribution and shared presets

Measurement and attribution are separate. A server may answer a probe while the
resolver still maps several releases to one shared preset. A line refined by a
parsed version is attributable on its own. A line selected only from an engine
banner is attributable only where that exact line was measured. When every
release receives the same preset, a result on one release is not evidence that
all siblings produced it.

`ptah db capabilities` reports the narrower server-side fact as `Preset source`:
which rule selected the preset for that server. It does not claim that every
value in the preset was measured on that exact line.

Future releases without a dedicated preset fall back at runtime, but the probe
pipeline reports the unmatched line as a failure. [Issue 916](https://github.com/stokaro/ptah/issues/916)
tracks remaining version-specific refinement.

## Emulator evidence

An emulator can prove compatibility with itself, not with a managed service.
Spanner is the explicit example: the matrix can start the vendor emulator behind
PGAdapter, but the declaration records that the evidence is emulated. The
reference service and emulator already differ on the database option required
before creating a serial column; [issue 942](https://github.com/stokaro/ptah/issues/942)
records that measurement.

## Engines with no Ptah dialect

Snowflake, Amazon Redshift, and Databricks have no dialect entry. Naming one is
an error instead of a fallback:

```text
ptah schema render --dialect snowflake
error: error rendering snowflake schema: unsupported database dialect: snowflake
```

The 2026-08-30 review did not find a reproducible path that a pull request from
a fork could start without a human-held credential and that measured the actual
engine rather than an imitation:

- **Snowflake:** trials reach the real service, but depend on an account.
  DuckDB-backed emulators measure their own reimplementation, not Snowflake.
  The SQL API also reports statement failures inside an HTTP 200 response, so a
  status-code-only probe produces false support.
- **Amazon Redshift:** commonly cited container images expose PostgreSQL, not a
  Redshift catalog. Control queries found ordinary PostgreSQL catalogs while
  Redshift catalog views and Redshift-specific DDL were absent.
- **Databricks:** Free Edition reaches a real SQL warehouse but depends on a
  human-created workspace token; it cannot provision an isolated workspace for
  each forked pull request. No engine emulator was available.

The commands, observations, external sources, and decision record are preserved
in [issue 1879](https://github.com/stokaro/ptah/issues/1879). A faithful emulator,
a credential-free per-PR service, or an explicit change to Ptah's evidence
standard would justify reopening the decision.

## Refreshing the evidence

Continuous-execution evidence is checked on every relevant change. Vendor
lifecycle evidence is time-sensitive and must be reread from the sources beside
each block in `cells.go`. Some vendors publish a duration or moving window
instead of a fixed date, so the support level can change even when no date in
the repository passes.

Refreshing an upstream lifecycle never rewrites runtime behavior by itself. It
may change an exercised line from `certified` to `legacy-tested`; capability and
removal decisions remain separate.
