---
title: License boundary
description: Ptah's independent implementation boundary around Atlas compatibility work.
---

Ptah does not use Atlas source code.

Ptah is an independent implementation that studies Atlas's public command surface, observable behavior, and test assets. Atlas-derived Apache-2.0 fixture material is kept in the separate `ptah-atlas-conformance` repository so the Ptah source tree remains implementation-clean and MIT-licensed.

## Repository boundary

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

Ptah can be tested by the conformance repo, but Ptah does not import or vendor that repository.

## Documentation rule

When documenting Atlas compatibility:

- Say `Atlas-compatible` for implemented command paths and behavior.
- Link to conformance reports for current evidence.
- Do not say `full parity`, `drop-in replacement`, or equivalent claims until the full conformance gates prove it.
