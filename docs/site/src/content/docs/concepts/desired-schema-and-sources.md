---
title: Desired schema and schema sources
description: The declarative idea behind Ptah - you declare the schema you want, from any mix of sources, and Ptah computes how to get there.
---

Everything in Ptah starts from one declarative idea: you describe the schema a
database *should* have — the **desired schema** — and Ptah computes what
separates any concrete database from it. Migration files, direct applies,
drift checks, and tests are all different ways of consuming that same
computed difference.

## How Ptah models it

A **schema source** is anything that can declare part of the desired schema:

- annotated Go structs (`--root-dir`),
- YAML, HCL, or SQL schema files (`--schema-file`),
- an external loader — any program that emits SQL, HCL, or YAML, such as an
  ORM exporter (`--schema-cmd`),
- a live database, turned into annotated Go models by `ptah introspect`.

Every source parses into the same internal schema representation before any
planning or rendering happens. The source format is therefore independent of
both the target dialect and the workflow: a YAML file can produce PostgreSQL
migrations, and the same Go annotations can drive versioned files or a direct
apply without remodeling.

Sources compose. `--root-dir` and `--schema-file` repeat and mix freely, and
the merged result is the **composite desired schema**. Objects are matched by
their database identity, identical definitions are deduplicated, and
conflicting definitions stop the command before it renders SQL or connects to
a database — the merge rules live on
[Composite desired schema](../../schema/composite/).

## Consequences

- **The desired schema is the review surface.** Changing a database starts
  with changing a source file that lives in version control, whichever
  workflow applies the change.
- **Sources converge on one IR.** `ptah schema export` converts Go annotations
  to HCL, and a brownfield database can be introspected into source at any time.
  Every valid Go annotation semantic has an HCL representation. Export reports
  opaque SQL bodies and byte-level normalization before destructive cleanup;
  HCL may additionally express semantics with no Go annotation spelling.
- **Declaring is not supporting.** What a concrete target accepts is decided
  later, by capability-aware planning — see
  [Dialects and capabilities](../dialects-and-capabilities/).

## Where it appears

- Each source format has its own page under [Model your schema](../../schema/go-annotations/).
- The versioned/direct decision that consumes the desired schema: [Choose a workflow](../../start/choose-a-workflow/).
- Turning an existing database into sources: [Adopt an existing database](../../start/adopt-an-existing-database/).
