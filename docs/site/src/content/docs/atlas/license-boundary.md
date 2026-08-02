---
title: License boundary
description: Ptah's independent implementation boundary around Atlas compatibility work.
---

Ptah's implementation does not use Atlas source code.

Ptah is an independent project, not affiliated with or endorsed by Ariga.

Ptah is an independent implementation that studies Atlas's public command surface, observable behavior, and test assets. Atlas-derived Apache-2.0 fixture material is kept in the separate `ptah-atlas-conformance` repository so the Ptah source tree remains implementation-clean and MIT-licensed.

## Repository boundary

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

Ptah can be tested by the conformance repo, but Ptah does not import or vendor that repository.

## What is allowed

Ptah compatibility work may use:

- Atlas public command names, flags, file formats, and documented behavior;
- observable behavior from running Atlas OSS;
- Apache-2.0 test assets kept in the separate conformance repository;
- independently written Ptah code, tests, and documentation.

Ptah CI may build an Apache-2.0 Atlas CE release into a disposable executable
and invoke it as an external black-box test oracle. The build verifies the
release tag's locked commit, downloads that immutable commit archive, checks
its committed SHA-256 digest, and validates the exact version string before any
comparison. The source archive and executable are never imported, vendored,
linked, or shipped with Ptah.

Ptah must not copy, vendor, or port Atlas source code into this repository.

## Documentation rule

When documenting Atlas compatibility:

- Say `Atlas-compatible` for implemented command paths and behavior.
- Link to conformance reports for current evidence.
- Do not say `full parity`, `drop-in replacement`, or equivalent claims until
  conformance reports and the documented gap register both support that claim.
