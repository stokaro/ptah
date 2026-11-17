---
title: Strict Community Edition mode
description: Run ptah-compat against the pinned Atlas Community Edition surface without silently losing Ptah capabilities.
type: status
audience:
  - "atlas-migrator"
  - "evaluator"
readerQuestion: "What changes when PTAH_ATLAS_STRICT_COMPAT is enabled?"
goal: "Run and interpret a strict Atlas CE conformance invocation."
sourceOfTruth:
  - "internal/atlascompatpolicy"
  - "internal/envbool"
  - "internal/cli/atlas"
generated: false
lastVerified: "2026-08-30"
evidence:
  - "internal/atlascompatpolicy"
  - "internal/cli/atlas/strict_compat_policy_test.go"
  - "github:stokaro/ptah-atlas-conformance"
searchAliases:
  - PTAH_ATLAS_STRICT_COMPAT
  - Atlas CE mode
overlaps:
  - "/atlas/overview/"
  - "/atlas/conformance/"
  - "/reference/extension-variables/"
disposition: keep
---

Strict mode is a conformance profile for comparison with the pinned Atlas
Community Edition binary. It is not the recommended profile for ordinary Atlas
migrations and it does not affect native `ptah`.

Enable it for one process:

```bash
PTAH_ATLAS_STRICT_COMPAT=1 ptah-compat schema inspect --help
```

The selector is an environment variable rather than a flag because a flag
would change the surface it selects. Neither profile registers a flag Atlas
lacks. The full compatibility surface registers each command's CE flags and the
flags Atlas Pro adds to the same command, such as `--include` on
`schema inspect`. The strict surface registers the CE flags alone. A capability
that no Atlas edition offers is selected with a `PTAH_*` variable, never with a
flag. The conformance CLI surface probe compares the full surface with the
pinned CE binary and a closed list of those Pro flags, and reports any other
flag.

## What changes before dispatch

Ptah constructs the CE command and flag tree before Cobra parses the
invocation. Gated verbs use a Ptah-owned diagnostic that tells the operator to
unset the selector for the complete compatibility surface. Known Ptah flag
environment bindings and gated feature toggles are validated before help,
version, arguments, project files, or database access.

Strict mode does not reserve every `PTAH_*` name. An `atlas.hcl` file may use
`getenv` to read ordinary project inputs, so unrelated environment values remain
available. The classified extension variables and their strict behavior are in
[Extension environment variables](../../reference/extension-variables/).

`PTAH_PROVIDER_CONFIG` names a provider manifest for engines the pinned binary
has no driver for. It is not a boolean, so it is refused by presence and by
name, and a strict run starts no provider. See
[Executable providers](../../extend/executable-providers/#use-a-provider-from-ptah-compat).

`PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE` names the objects `migrate diff` leaves out
of its comparison in full compatibility mode, where no Atlas edition has an
`--exclude` flag on that command. It is refused by presence and by name too.
The selected env's `exclude` list selects the same objects and stays available
in strict mode. `PTAH_ATLAS_MIGRATE_DIFF_INCLUDE`, the positive selection for
the same command, is refused the same way; no Atlas spelling selects those
objects there.

An engine no Atlas edition has a driver for is refused in the pinned binary's
own words: a URL with `sql/sqlclient: unknown driver "<scheme>"`, and a
`docker://` dev URL with `unsupported docker image "<engine>"`. That covers
YDB and every official engine only an executable provider serves, such as
DuckDB.

A MySQL or MariaDB URL carrying `ssl-ca` is refused with `Atlas Community
Edition strict compatibility does not support the ssl-ca URL parameter`. The
pinned binary does not apply the parameter, measured against a server whose
certificate a private CA signed. Without `tls`, it sends the parameter to the
server as a session variable, and the server answers `Error 1064`. With
`tls=true`, the handshake fails with `x509: certificate signed by unknown
authority` whichever file `ssl-ca` names, the signing CA included. The default
surface verifies the server against the named bundle, as
[MySQL and MariaDB](../../databases/mysql/#tls-with-a-private-certificate-authority)
describes.

## Content strict mode refuses

Strict mode rejects content Atlas CE cannot represent safely instead of
discarding it:

- Pro-only authored or live schema objects;
- YAML desired-schema sources;
- an authored `schema apply` lint policy the CE execution path cannot enforce;
- extended `atlas.hcl` evaluation outside the supported CE subset, including a
  `data "composite_schema"` block, declared or referenced, and a reference to a
  `docker` block's `url`, which Atlas CE refuses with `Unsupported attribute`;
- Atlas txtar, Ptah directives, and SQL templates for commands that execute,
  convert, or replay migration bodies.

Checksum-only migration reads preserve those bytes because they do not execute
or reinterpret the body.

Live schema inspect, apply, diff, and clean inventory catalog-only object kinds
before output or mutation. A Pro-only object stops the operation; the result
cannot silently describe less than the database contains. Cleanup validates the
writer's full destruction inventory, including dependent objects.

## Plans strict mode writes as Atlas CE does

Strict mode also changes what a plan writes where the default surface writes
more than Atlas CE. On PostgreSQL, a column made `NOT NULL` that declares a
default is one such case. The default surface fills the column's `NULL` rows
from that default before `SET NOT NULL`; see
[Making a column NOT NULL](../../databases/postgresql/#making-a-column-not-null).
Atlas CE writes `SET NOT NULL` alone, so under strict mode `migrate diff`,
`schema apply` and `schema diff` write no fill, and the statement fails on a
`NULL` row as it does there. A layout that writes a rollback file leaves the
fill out of both halves. Strict mode also leaves out the `setval` that moves a new
identity's or `SERIAL` column's sequence past the rows the table holds, so the next
insert can collide with one, as it does there.

## Lint summaries strict mode counts as Atlas CE does

The `migrate lint` summary and its `.TotalChanges` count schema changes. The
default surface counts a change to a view, a trigger, a routine, a sequence, a
policy, a privilege or row-level security as one, because Ptah models each of
them. Atlas CE reads none of them off the dev database and counts none, so
strict mode counts only tables with their columns, constraints, indexes and
comments, schemas, and enum types. A PostgreSQL `DROP SEQUENCE ... CASCADE`
counts there through the tables it changes: the cascade removes each column
default that calls `nextval` on the sequence, and each table that loses one
counts as a change. See
[The schema-change count](../migrate-commands/#the-schema-change-count).

## Capabilities retained deliberately

Strict mode preserves safety and correctness changes where copying CE would
lose data, hide state, or fail for an unrelated reason. Those measured
differences are [Compatibility differences](../retained-divergences/), not parity
claims. One of them is a file strict mode writes: `migrate diff` into the
Liquibase layout writes rollback lines Liquibase runs, where Atlas CE writes a
spelling Liquibase reads as a comment.

With the selector absent, `ptah-compat` retains every implemented Atlas
Pro-like and best-effort capability. PostgreSQL extensions, sequences, and
row-level security policies remain reachable on the compatibility surface; a
Pro migration should not have to be rewritten against native command names to
keep them.

## Verify both profiles

Run CE differential and CLI-surface tests with the selector enabled. Run
Pro-retention and ordinary compatibility tests with it absent. Passing only the
strict profile proves nothing about the default migration surface, and command
registration alone proves neither runtime behavior.

Use [Conformance](../conformance/) for the current measured contours and
[Feature matrix](../feature-matrix/) for capability-level status.
