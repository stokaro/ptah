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

Strict mode refuses a PostgreSQL `search_path` that the pinned binary and the
server read as different schemas. The pinned binary reads the whole value as
one schema name, byte for byte, so a space around it, a capital letter and a
comma stay part of the name, and `$user` names a schema called `$user`. The
server, and Ptah with it, reads the value with the `search_path` grammar,
which trims an unquoted name, folds it to lower case, reads a comma as a list
and reads `$user` as the schema named after the session's user. The refusal
covers the value `$user` and a value that holds the ASCII white space the
server trims, a double quote, a comma, a plus sign or an upper-case letter, or
is longer than 63 bytes. It applies to `-u`, `--from`, `--to`, `--dev-url` and
the URLs an `atlas.hcl` env sets, arrives before Ptah connects, and names the
source:

```text
Error: Atlas Community Edition strict compatibility refuses search_path=A in --url: the community binary reads the value as one schema name, byte for byte, and the server's search_path grammar reads another; name a schema other than $user, in lower case, at most 63 bytes, without white space, quotes, commas or a plus sign, or unset PTAH_ATLAS_STRICT_COMPAT to read the value as the server does
```

A lower-case name passes, and a `docker://` dev URL is not checked, because its
`search_path` names the schema the dev container's baseline creates.

The refusal is stricter than Atlas CE on purpose. Following the pinned binary
would make a schema with white space or a capital letter the current schema of
a connection whose readers take that name from the server, and following the
server would select a schema the pinned binary does not. Measured on Atlas CE
v1.4.0 against PostgreSQL 18, on a database holding the schemas `a`, ` a`,
`a `, `A` and `a,b`, each with one table:

| `search_path` | Atlas CE, `-u` and an env `url` | Atlas CE, `--to` | Ptah, strict mode | Ptah, default |
| --- | --- | --- | --- | --- |
| `%20a` | selects ` a` | selects ` a` | refused | selects `a` |
| `a%20` | selects `a ` | selects `a ` | refused | selects `a` |
| `A` | selects `A` | selects `A` | refused | selects `a` |
| `a,b` | selects `a,b` | refused: `missing scheme` | refused | reads every schema |
| `$user` | selects `$user` | selects `$user` | refused | selects the session user's schema |

On `--to`, Atlas CE splits the flag's value on the comma, so `a,b` leaves a URL
fragment without a scheme. Through `-u` and an env `url`, it selects `a,b` only
because the database holds a schema of that name; without one it refuses the
run with `postgres: schema "a,b" was not found`. The same holds for `$user`:
without a schema of that name, Atlas CE refuses the run with
`postgres: schema "$user" was not found`, where the server reads the schema
named after the session's user, and a run that applies a plan through it
changes that schema. A value the server refuses,
such as `a%20b`, fails on Atlas CE as
`invalid value for parameter "search_path": "a b"`. Atlas CE reads `a+b` as
`a b` and fails the same way.

On a `--dev-url`, Atlas CE replays a document where the server's grammar puts
it and reads the literal schema back. Measured on a dev database that also
holds `a`, a document inspected through `search_path=%20a`, `a%20` or `A`
comes out empty, exit 0, and its tables stay behind in the dev database's
schema `a`. `migrate apply` through `search_path=%20a` creates
`atlas_schema_revisions` in `a` and then fails to read it from ` a`, with
`relation " a.atlas_schema_revisions" does not exist`. Strict mode refuses the
URL and replays and applies nothing.

The default surface reads the value with the server's grammar. A list scopes
nothing, so a run reads every schema. To select a schema whose name holds white
space, an upper-case letter or a comma, write it as one quoted identifier:
`search_path=%22%20a%22` selects ` a`, and a document inspected through it
applies back with no changes.

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
  convert, or replay migration bodies;
- the pre-apply drift check an `atlas.hcl` env declares in
  `check "migrate_apply"`, which Atlas CE ignores; see
  [Pre-apply drift check](../project-config/#pre-apply-drift-check).
- a PostgreSQL temporal key, `without_overlaps` or `period`, in a desired or an
  inspected schema. Atlas CE accepts both attributes and plans a plain key,
  refuses `type = GIST` on a key, and fails to inspect a database whose foreign
  key uses `PERIOD`; see
  [Temporal keys](../../databases/postgresql/#temporal-keys).

Checksum-only migration reads preserve those bytes because they do not execute
or reinterpret the body.

Live schema inspect, apply, diff, and clean inventory catalog-only object kinds
before output or mutation. A Pro-only object stops the operation; the result
cannot silently describe less than the database contains. Cleanup validates the
writer's full destruction inventory, including dependent objects.

A PostgreSQL publication is the exception. Atlas CE reads a database
that holds one, such as a Supabase or Debezium database, and describes none. So
strict mode leaves an inspected publication out of `schema inspect` output and
plans none for a database or a migration directory on the desired side. A
publication declared in a desired schema is still refused.

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
