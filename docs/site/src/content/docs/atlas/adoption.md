---
title: Adopting an Atlas project
description: The path from Atlas through `ptah-compat` to native Ptah, one reversible step at a time.
owns:
  - cli-ptah-project-adopt
  - cli-ptah-project-inspect
---

Adoption is a path, not a switch. Each step below leaves a project that works,
and each one can be taken on its own:

```text
Atlas
  -> ptah-compat
  -> native-readable Ptah project
  -> optionally normalized Ptah HCL
  -> native ptah
```

The two halves of adoption are separate concerns and are treated separately
here: the **project configuration**, which is a file, and the **database
history**, which is persisted state. A project file can be perfectly adoptable
in front of a database that is not, and the reverse. Nothing on this page
converts one on the strength of the other.

## Step 1 — run what you have through `ptah-compat`

`ptah-compat` is a drop-in for the Atlas CLI. Point it at the `atlas.hcl` you
already have and the commands you already run keep working. See the
[Atlas compatibility overview](../overview/) for what the drop-in covers.

Nothing has to change to take this step, and nothing later on this page removes
it. `ptah-compat` is not a temporary staging area to be escaped from.

## Step 2 — point native `ptah` at the same file

`atlas.hcl` is a project language native Ptah reads. The `ptah` binary reads it
directly, without `ptah-compat` in front:

```console
$ ptah migrations lint --env local
No lint findings.
```

There is no `--migrations` flag in that command: the directory came from the
`migration { dir }` block in `atlas.hcl`. Native verbs that read project
configuration take `--env` to select an env block from `ptah.yaml` **or**
`atlas.hcl`.

This is the step most projects can stop at. HCL is a supported project language
in its own right, not an import format, and the supported Atlas HCL subset and
native Ptah HCL are meant to describe one project model wherever their concepts
overlap. Extensions Ptah adds are extensions to a native language; a project
using one is no longer a project Atlas reads.

### Both files, if you want both

`ptah.yaml` and `atlas.hcl` are both read when both are present, and merged.
Where the two name the same setting, **`atlas.hcl` wins**. That makes an
incremental move possible in either direction: settings Ptah alone needs can go
in `ptah.yaml` while `atlas.hcl` stays authoritative for everything it declares.
[Configuration](../../reference/configuration/) has the full precedence table,
flags and environment variables included.

## Step 3 — ask what adoption would take

```console
$ ptah project adopt --check
```

Every construct the project file declares is put in one of three classes:

| Class | Meaning |
| --- | --- |
| `exact` | Ptah acts on it and it means the same in a native Ptah project. Nothing to do. |
| `compat-only` | Ptah acts on it, but the spelling is Atlas's and a native equivalent exists. |
| `unsupported` | Ptah read the name and acts on nothing. |

A project with nothing in the last two classes is **native-ready**: it can be
operated by native Ptah as it stands, without its file being rewritten. That is
the answer normalization is optional against — a file that already means what it
should does not need to be rewritten to prove it.

An `unsupported` construct is why a project is not native-ready. It is named in
the report rather than dropped: a conversion that silently omitted it would
produce a file that looks adopted while the behavior that name asked for is
missing.

`--check` writes nothing. `--format json` answers the same analysis as a
document.

## Step 4 — the database half

Configuration compatibility says nothing about the migration history a database
already holds. Native Ptah records revisions in its own table by default, and a
database whose history was written in the Atlas-compatible layout reads as
*never migrated* through that default — which means every applied migration runs
again.

```console
$ ptah project adopt --check --preflight
```

The preflight reads the revision history in the database the project points at
and reports, per dimension, whether native Ptah may take it over: which layout
holds the rows and where, whether the recorded revisions and the migration
directory name the same migrations, whether the recorded checksums are ones that
directory accounts for, whether anything is unfinished, whether any row is
recorded as applied without having run, and who wrote the rows.

```text
Database adoption:
  ~ the history is in the Atlas-compatible layout, in atlas_schema_revisions.atlas_schema_revisions
    native Ptah reads this layout only when the project names it: set the migration revision
    format to atlas in the native project file, or Ptah writes its own layout, reads this
    database as never migrated, and applies every recorded migration again
  ✓ every recorded revision finished
  ✓ every recorded revision names a migration this directory contains
```

It takes no URL: the project file is where the answer to "which database"
already lives, and a flag disagreeing with it would report confidently about a
history the project does not target.

**The preflight writes nothing** — not the revision table it looks for, and not
the layout of one it finds. Some states it refuses rather than works around: a
database holding both a native and an Atlas revision table is two histories, and
choosing between them would be guessing which one describes the schema.

The section is printed whether or not `--preflight` was passed. Without it, the
report says the database was not inspected, because a clean verdict about a file
is not a verdict about a database.

### Keeping the history where it is

A history in the Atlas-compatible layout does not have to move. Naming that
layout in the project's migration revision format is enough for native Ptah to
continue it in place — no rows are rewritten, and no migration re-runs. Moving
the rows is a choice, not a requirement.

## Step 5 — normalize the file, if you want to

```console
$ ptah project adopt
```

Without `--check`, the compat-only spellings are rewritten in place and
everything else in the file is left byte for byte as it was. A project declaring
anything `unsupported` is refused rather than half-converted.

One compat-only spelling today is the `atlas://` migration-directory reference,
which becomes an `oci://` reference to the same artifact. It needs somewhere to
point: set `PTAH_ATLAS_REGISTRY` to the registry namespace holding your
directories, and the rewrite names it. Without it the analysis reports the gap
instead of inventing a repository.

## What adoption does not require

* **Converting HCL to YAML.** HCL stays. `ptah.yaml` is an alternative, not a
  destination.
* **Rewriting migration SQL.** Existing migration files are kept as they are.
* **Re-generating plans.** Atlas-format migration files are not regenerated from
  schema state to make them "native".
* **Renaming the directory layout.** A supported migration directory layout is
  not rewritten for the sake of canonicalization.
* **Giving up `ptah-compat`.** It remains supported, and a project can keep
  using it indefinitely.

## Two verbs, and which one to reach for

| Question | Verb |
| --- | --- |
| Which of my settings does Ptah act on, and which does it ignore? | `ptah project inspect` |
| What would adoption take, and is this project native-ready? | `ptah project adopt --check` |
| May native Ptah take over this database's history? | `ptah project adopt --check --preflight` |
| Rewrite the compat-only spellings. | `ptah project adopt` |
