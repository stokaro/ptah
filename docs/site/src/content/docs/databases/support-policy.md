---
title: Database support policy
description: What Ptah support levels promise, how undeclared releases behave, and why capability gates remain separate.
type: concept
audience:
  - "database-engineer"
readerQuestion: "What does a Ptah database support level promise?"
goal: "Interpret support levels without confusing testing coverage with runtime capability."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/serverprofile"
generated: false
searchAliases:
  - certified database release
  - best-effort database support
overlaps:
  - "/databases/support-matrix/"
  - "/concepts/dialects-and-capabilities/"
disposition: split
---

A support level says how much Ptah testing stands behind one database release
line. It does not enable or disable database operations. Ptah decides whether an
operation is valid from the capability profile resolved for the target server.

## The four levels

| Level | Ptah's testing promise |
| --- | --- |
| `certified` | Ptah exercises the release line in continuous integration and commits to the tested feature surface. |
| `legacy-tested` | Ptah still exercises an upstream end-of-life line as a regression sentinel. Runtime behavior is unchanged; the maintenance promise is weaker. |
| `best-effort` | Ptah does not regularly exercise the line. The connection is not rejected, and resolved capabilities still govern each operation. |
| `known-incompatible` | Ptah has measured and named a concrete technical incompatibility. Upstream end of life alone does not earn this label. |

The current assignment for every declared line is generated on the
[support matrix](../support-matrix/). Do not copy its counts or release-line
lists into authored prose.

## How Ptah assigns a level

Two questions determine the ordinary case:

1. Does Ptah continuous integration exercise the release line?
2. Does the vendor still support that release line?

Two yes answers produce `certified`. An exercised line past upstream end of
life becomes `legacy-tested`. A line that Ptah does not exercise is
`best-effort`, regardless of the vendor's own support statement.

An emulator is a deliberate exception. Exercising an emulator proves that the
capability preset still matches that interface; it does not certify the managed
service. The release-line declaration records whether evidence came from an
emulator so the certification check cannot silently treat the two as equal.

## A version outside the declared set

A server whose version matches no declared release line resolves to
`best-effort`. Ptah does not refuse the connection. The dialect resolver selects
the closest applicable preset: a version ladder where one exists, or the
dialect default or banner match otherwise.

Ask the server in front of you what Ptah resolved:

```bash
ptah db capabilities --db-url "$DATABASE_URL"
```

The text report names the support level, release line, server version, preset,
preset source, behavior values, and supported or unsupported capability keys.
The JSON form contains the same facts plus each capability key's documentation:

```bash
ptah db capabilities --db-url "$DATABASE_URL" --format json
```

For an undeclared version, the report explicitly says that the preset is a
fallback rather than a measured match. The capability-probe pipeline is stricter
than runtime behavior: a result it cannot attribute to a declared line is not
accepted as evidence for that line.

## Support and capability answer different questions

- **Support level:** how often and where Ptah tests this release line.
- **Capability:** whether this concrete target accepts a schema construct or
  operation.
- **Dialect:** which parser, renderer, and database family Ptah uses.

A certified line may deliberately lack a capability. A best-effort line may
still support an operation because its resolved preset enables it. See
[Dialects and capabilities](../../concepts/dialects-and-capabilities/) for the
runtime model and [Capabilities](../../reference/capabilities/) for the complete
key reference.

## Upstream end of life

An upstream end-of-life date never removes a line or blocks a connection by
itself. It moves an exercised line from `certified` to `legacy-tested`. Removing
a release line, refusing an operation, and changing a capability preset are
separate product decisions that need their own evidence.
