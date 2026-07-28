---
title: Conformance
description: Where Atlas compatibility evidence lives and how to interpret it.
---

Atlas compatibility evidence is maintained in [`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance).

The conformance repository keeps Atlas Apache-2.0 fixtures outside Ptah's MIT source tree and imports Ptah as the system under test:

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

## Current summary

The authoritative current numbers live in the conformance repository reports:

- [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md)
- [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md)
- [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
- [`cli-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md)
- [`PARITY.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md)

Green conformance reports mean that the current measured corpus has no red
results. They do not, by themselves, prove every Atlas OSS command, flag,
dialect feature, and output mode. Use the comparison gap register for product
and coverage gaps that are outside the current measured corpus.

## How to read green and red checks

The conformance repository separates regression budgets from full parity:

| Gate type | Meaning |
| --- | --- |
| Regression budget | No new gaps beyond the accepted budget for that contour. Should stay green. |
| Full conformance | Every checked case in that contour passes. May stay red while the measured corpus still has non-OK results. |

A green regression-budget check does not mean Ptah has full Atlas OSS parity.
A red full-conformance check is expected while the report still lists measured
non-OK results.

Even when both regression-budget and full-conformance checks are green, the
claim is limited to the corpus represented by the generated reports. Expanding
live and differential coverage is tracked in
[`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167).

## Workflow parity

Workflow capabilities that do not introduce schema objects sit outside the
round-trip corpus:

| Workflow | Native Ptah | Atlas-compatible Ptah surface | Atlas CE | Evidence |
| --- | --- | --- | --- | --- |
| Declarative migration and schema tests | `ptah migrations test` and `ptah schema test` | `ptah atlas migrate test` and `ptah atlas schema test` forward to the native runners with Atlas-shaped flags and exit codes | Cannot run either command; the framework is outside the open-source core | Unit coverage (including the Atlas-compatible forwards) plus integration-tagged live PostgreSQL runner tests; not counted as a schema-object fixture |
| Migration directory maintenance | `ptah migrations edit`, `rebase`, and `rm` | `ptah atlas migrate edit`, `rebase`, and `rm` forward to the native commands with Atlas-shaped flags and `{name \| version}` positionals; the `--edit` flags on `migrate new`, `migrate diff`, and `schema apply` open the operator's editor | Cannot run any of the three verbs; they abort with the community-version boundary | Unit coverage with hermetic editor scripts, including `ptah migrations validate` passing on the mutated directory; not counted as a schema-object fixture |
| Verified and reported rollback | `ptah migrations down --shadow-db` replays the rollback plan on a disposable shadow database before the target is touched | `ptah atlas migrate down --dev-url` maps to the shadow verification, and `--format` renders an Atlas Go-template down report (`.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, `.Error`); the forward defaults to Atlas revision bookkeeping (`--revision-format atlas`) with the native `--revision-format ptah` pass-through as the escape hatch; `--to-tag`, `--skip-checks`, and `--plan` are recorded registry-bound waivers that fail loudly | `migrate down` does not exist in the community binary; the CE notice lists down migrations among excluded features | Unit coverage over live SQLite: verification success and pre-target abort on both paths, report rendering including partial-failure reports, waiver rejections, a byte-identity regression pinning the default forward output to the native command, and revision-format regressions proving a bare `atlas migrate down` reverts revisions written by `atlas migrate apply` |
| Pre-approved declarative plans | Ptah plans and applies declarative schema changes through the same engine that powers `schema apply` | `ptah atlas schema plan` saves the computed plan to a fingerprinted local JSON plan file; `ptah atlas schema apply --plan file://<path>` executes the saved statements only after the live database matches the plan's source fingerprint, refusing drifted targets; registry planning flags are recorded waivers and the plan registry sub-verbs stay CE boundary stubs | `schema plan` aborts with the community-version boundary; the plan/approval flow is bound to the Atlas Pro registry | Unit coverage over live SQLite: plan computation and save, plan execution with schema assertions, stale-plan refusal after target drift, dry-run, dialect mismatch, malformed documents, and waiver rejections; not counted as a schema-object fixture |

This is a workflow-parity record, not a claim of full Atlas Pro compatibility.
For the code-by-code audit of the analyzer checks Atlas marks as Pro, see
[Comparison: Atlas Pro analyzer coverage](../../reference/comparison/#atlas-pro-analyzer-coverage).
The upstream `atlas migrate ls`, `migrate show`, `schema stats`, and
`schema validate` verbs are absent from the pinned Atlas CE v1.2.0 binary and
are triaged in the comparison gap register rather than measured here.

## Local commands

From the Ptah repository:

```bash
make conformance
```

From `ptah-atlas-conformance`:

```bash
make probe
make budget
make gate
make probe-live
make budget-live
make probe-diff
make budget-diff
make probe-cli-surface
make budget-cli-surface
make gate-cli-surface
```

Live and differential probes require real database URLs. Differential probes also require an Atlas CE binary built from the pinned Atlas version in the conformance repository. CLI surface probes use the same pinned Atlas CE binary to compare command paths, help boundaries, flags, and runtime classifications.

## When to update reports

Update conformance after Ptah changes that affect Atlas command behavior,
schema parsing/rendering, migration directory semantics, live database
round-trips, or public compatibility APIs. Bump the Ptah module version in the
conformance repository, run `go mod tidy`, regenerate the relevant reports, and
let both regression and full-conformance checks show the expected state.
