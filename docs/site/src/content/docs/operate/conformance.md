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
