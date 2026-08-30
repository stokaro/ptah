---
title: Atlas compatibility overview
description: Choose native Ptah, the default Atlas-compatible surface, or strict Community Edition conformance mode.
type: landing
audience:
  - "atlas-migrator"
  - "evaluator"
readerQuestion: "Which Ptah surface should an Atlas user choose?"
goal: "Choose the correct Ptah or ptah-compat surface for an Atlas workflow."
sourceOfTruth:
  - "cmd/atlas"
  - "internal/atlascompatpolicy"
generated: false
searchAliases:
  - replace Atlas CLI
  - ptah compat
overlaps:
  - "/atlas/adoption/"
  - "/atlas/strict-ce-mode/"
  - "/reference/atlas-commands/"
disposition: split
---

Ptah serves two migration paths. New projects use the native `ptah` command
tree. Existing Atlas scripts use the separate `ptah-compat` binary, which keeps
Atlas-shaped commands and configuration while executing Ptah behavior.

## Choose a surface

| Situation | Surface | What it promises |
| --- | --- | --- |
| New Ptah-authored workflow | `ptah` | Ptah-native commands, configuration, output, and every implemented capability. |
| Existing Atlas CE or Pro pipeline | `ptah-compat` | Atlas-shaped commands plus every compatible and Pro-like capability Ptah implements. This is the default compatibility profile. |
| Differential test against the pinned Atlas CE binary | `PTAH_ATLAS_STRICT_COMPAT=1 ptah-compat` | A separately constructed CE-only command and flag tree with explicit refusal of content CE cannot represent. |

Do not enable strict mode merely because a pipeline originally used Atlas. The
default compatibility profile is also the migration path for Atlas Pro users;
removing their reachable capabilities would not be compatibility.

## What is shared

The binaries share generally useful schema and migration capabilities, not
command lines. For example:

| Task | Native Ptah | Atlas-compatible surface |
| --- | --- | --- |
| Apply migrations | `ptah migrations up` | `ptah-compat migrate apply` |
| Roll back migrations | `ptah migrations down` | `ptah-compat migrate down` |
| Inspect a database | `ptah db read` | `ptah-compat schema inspect` |
| Compare schemas | `ptah schema compare` | `ptah-compat schema diff` |

Atlas-specific codecs, `atlas://` resolution, project configuration, revision
metadata, and output contracts stay on `ptah-compat` because they have no
independent native meaning. Native `ptah` accepts no Atlas command aliases.

The generated [Atlas-compatible command index](../../reference/atlas-commands/)
is the complete verb inventory. [Atlas migrate commands](../migrate-commands/)
and [Atlas schema commands](../schema-commands/) provide task-oriented usage.

## What compatibility does not mean

Registration is not implementation. Some command paths exist so a migrated
script fails in the correct namespace with an explicit boundary. The
[feature matrix](../feature-matrix/) distinguishes implemented, partial,
unsupported, waived, and externally measured behavior per capability.

Ptah also refuses to copy defects. Strict CE mode never silently drops authored
schema objects, hides a live object, or corrupts migration state merely because
the pinned community binary does. Deliberate safety and correctness differences
are listed in [Retained divergences](../retained-divergences/).

Atlas Cloud registry operations have no account-independent protocol Ptah can
implement. Compatibility stubs such as `migrate push` and `schema push` refuse
with that reason. Native Ptah publishes schema and migration artifacts through
[OCI registries](../../operate/oci-registry/).

## Adopt an existing project

[Adopt an Atlas project](../adoption/) classifies project settings and migration
history before changing either. It separates exact support, compatible rewrites,
and unsupported constructs, then gives you a native Ptah path or a retained
`ptah-compat` path.

If an executable must literally be named `atlas`, build or link `ptah-compat`
under that name. The binary adopts its invoked name, so help and diagnostics use
`atlas ...` without changing the behavior being run.

## Evaluate the claim

- [Strict Community Edition mode](../strict-ce-mode/) explains what the selector
  removes and why ordinary migrated pipelines should leave it unset.
- [Conformance](../conformance/) records current external measurements rather
  than inferring parity from the command tree.
- [Feature matrix](../feature-matrix/) tracks product, test, and documentation
  status per capability.
- [Atlas docs coverage](../docs-coverage/) maps official Atlas documentation to
  the Ptah pages that own each covered behavior.
