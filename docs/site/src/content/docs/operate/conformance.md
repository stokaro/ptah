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
- [`PARITY.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md)

Ptah documentation must not claim full Atlas OSS parity until the full conformance gates are green.

## How to read green and red checks

The conformance repository separates regression budgets from full parity:

| Gate type | Meaning |
| --- | --- |
| Regression budget | No new gaps beyond the accepted budget for that contour. Should stay green. |
| Full conformance | Every checked case in that contour passes. May stay red while known gaps remain. |

A green regression-budget check does not mean Ptah has full Atlas OSS parity.
A red full-conformance check is expected while the report still lists known
gaps.

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
```

Live and differential probes require real database URLs. Differential probes also require an Atlas CE binary built from the pinned Atlas version in the conformance repository.

## When to update reports

Update conformance after Ptah changes that affect Atlas command behavior,
schema parsing/rendering, migration directory semantics, live database
round-trips, or public compatibility APIs. Bump the Ptah module version in the
conformance repository, run `go mod tidy`, regenerate the relevant reports, and
let both regression and full-conformance checks show the expected state.
