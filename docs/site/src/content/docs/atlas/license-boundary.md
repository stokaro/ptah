---
title: License boundary
description: Ptah's independent implementation boundary around Atlas compatibility work.
---

Ptah's implementation does not use Atlas source code.

Ptah is an independent project, not affiliated with or endorsed by Ariga.

Ptah is an independent implementation that studies Atlas's public command surface, observable behavior, and test assets. Atlas-derived Apache-2.0 fixture material is kept in the separate `ptah-atlas-conformance` repository so the Ptah source tree remains implementation-clean and MIT-licensed.

Ptah does not reverse engineer proprietary implementations. It does not decompile or disassemble proprietary binaries, access proprietary source code, or copy protected implementation expression from Atlas or from any other product. The Ptah implementation contains no third-party product code, Atlas code included. External implementation code enters the project only through dependencies declared in the repository manifests.

Compatibility work is limited to public interfaces, documentation, properly licensed assets, and external behavior lawfully observed while using software under a valid right to use it. Subject to applicable law, and without copying protected expression, Ptah reserves the right to independently reimplement the interface of any application in order to provide a free and open-source alternative.

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

## Development jurisdiction and legal basis

Ptah is developed in the Czech Republic under Czech and European Union law, including [Directive 2009/24/EC](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32009L0024) on the legal protection of computer programs.

The position above rests in particular on:

- Articles 1(2), 5(3), and 8 of Directive 2009/24/EC. They distinguish protected expression from the ideas and principles underlying a program and its interfaces, permit an authorized user to observe, study, and test program behavior, and make contrary contractual terms null and void.
- [Sections 65 and 66 of Czech Act No. 121/2000 Coll.](https://e-sbirka.gov.cz/sb/2000/121), including Section 66(1)(d), which implements the right to study and test the functionality of a computer program.
- [*SAS Institute Inc. v World Programming Ltd.*, C-406/10](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:62010CJ0406), in which the Court of Justice of the European Union held that program functionality, programming languages, and data file formats are not protected forms of program expression, and confirmed the licensed user's right to observe, study, and test program behavior.

Legal questions about Ptah's development and compatibility work are addressed under Czech and European Union law. This page records the project's development and provenance policy. It is not legal advice.

## Documentation rule

When documenting Atlas compatibility:

- Say `Atlas-compatible` for implemented command paths and behavior.
- Link to conformance reports for current evidence.
- Do not say `full parity`, `drop-in replacement`, or equivalent claims until
  conformance reports and the documented gap register both support that claim.
