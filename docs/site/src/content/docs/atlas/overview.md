---
title: Atlas compatibility overview
description: How the ptah-compat drop-in binary replaces the Atlas CLI, and how Atlas-style flags translate to native Ptah concepts.
---

You run scripts, CI jobs, or habits built around the Atlas CLI and want to know
how Ptah fits them. Ptah ships a separate drop-in binary, `ptah-compat`, that
presents the Atlas-compatible command surface. This page explains how to
install it, how Atlas-style flags translate to Ptah concepts, and where the
compatibility boundary is — before you pick up the per-command usage pages.

## The drop-in binary

The main `ptah` binary is a purely native CLI — it has no Atlas-style command
paths. The separate `ptah-compat` binary is a binary-level drop-in replacement
for the Atlas CLI, built for scripts that need Atlas-style root commands:

```bash
go install go.5x5.cz/ptah/cmd/ptah-compat@latest

ptah-compat migrate apply --url "$DATABASE_URL" --dir ./migrations
```

Command examples on the Atlas compatibility pages are written as
`ptah-compat <command> ...` — the name the binary ships under.

Every Atlas-compatible command has a native `ptah` twin — for example
`ptah-compat migrate apply` and `ptah migrations up`, or `ptah-compat schema inspect` and
`ptah schema inspect --db-url ...`. Use the native tree for new Ptah-authored
work and the compat binary for existing Atlas scripts; the per-verb mapping is
listed in the [Atlas-compatible commands reference](../../reference/atlas-commands/).

### Installing under the name `atlas`

For a byte-level drop-in with existing scripts that call an executable named
`atlas`, install the binary under that name:

```bash
# Build it under the name your scripts expect:
go build -o atlas ./cmd/ptah-compat

# Or install it and link the atlas name:
go install go.5x5.cz/ptah/cmd/ptah-compat@latest
install_dir="$(go env GOPATH)/bin"
ln -sf "$install_dir/ptah-compat" "$install_dir/atlas"
```

The binary adopts the name it is invoked as, so usage, help, and error output
read `atlas migrate apply ...` when the executable is named `atlas`.

## Translation model

Implemented Atlas-compatible commands either execute dedicated Atlas-shaped
behavior or translate Atlas-style flags into the closest native Ptah command
model. Unsupported flags fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR for supported `ptah-compat ... --env` defaults |
| `--config`, `-c` | Local Atlas project config path for `schema` and `migrate` commands |
| `--var name=value` | Atlas HCL variable override for supported local expressions |
| Atlas revision table mode | Ptah revision format and table settings |

Atlas project flags are persistent on the Atlas-compatible `schema` and
`migrate` command groups, so both of these forms are valid:

```bash
ptah-compat migrate --config project.hcl --env local hash
ptah-compat migrate hash --config project.hcl --env local
```

The supported `atlas.hcl` subset those flags read is documented in
[Atlas project config](../project-config/).

Atlas OSS shorthand aliases are part of the compatibility surface. Ptah accepts
`-u` for `--url`, `-c` for `--config`, `-s` for `--schema` on Atlas commands
that register schema selection, and `-f` for `schema diff --from`. `schema apply`
also accepts Atlas's hidden `--file/-f` input alias for local HCL or SQL paths;
prefer the native `ptah schema apply` verb in new Ptah-authored scripts.

## Utility commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah-compat version` | Prints Ptah build information. |
| `ptah-compat license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |
| `ptah-compat completion <shell>` | Generates Cobra completion output for the Atlas-compatible command tree. |

## Format reports and redaction

Atlas-compatible `--format` reports use the Atlas data shape. URL fields render
as redacted URL strings in Go templates such as `{{ .Env.URL }}`, but
`{{ json . }}` emits an Atlas-like URL object with `Scheme`, `User`, `Host`,
`Path`, `RawQuery`, `Fragment`, `RawPath`, `RawFragment`, `ForceQuery`,
`OmitHost`, and, for SQLite URLs, `Schema`. Query keys that look like
passwords, tokens, secrets, or API keys are replaced with `xxxxx`; URL userinfo
passwords are removed.

The per-command template fields are listed on
[Atlas migrate commands](../migrate-commands/#format-template-fields) and
[Atlas schema commands](../schema-commands/#format-template-fields).

## Compatibility boundaries

Some Atlas Pro commands and flags are bound to Atlas Cloud services Ptah
intentionally has no counterpart for. Those paths are recorded waivers or Atlas
CE boundary stubs: they are registered so scripts fail in the right namespace,
and they reject execution loudly with their rationale instead of being silently
ignored. The largest examples are `migrate push` and `schema push` (the Atlas
Registry protocol is proprietary and account-bound; the native
`ptah migrations push` and `ptah schema push` verbs publish to any
[OCI registry](../../operate/oci-registry/) instead) and the `schema plan`
registry sub-verbs. The per-command pages name each waiver where it appears.

## Compatibility never costs you a capability

Ptah models things the Atlas community CLI does not. PostgreSQL extensions,
sequences and row-level security policies are the clearest examples: that CLI
answers `postgres: extensions are not supported by this version` and refuses a
schema file that declares any of them, while Ptah reads, diffs and applies all
three.

Being a drop-in for that CLI never means giving those up.

The compatibility surface **defaults** to what the community CLI accepts, so
output you hand back to it stays readable. What that default leaves out is
reported rather than dropped in silence — you are told what was omitted and
why, so a compatibility-shaped inspect never describes less of your database
than it found without saying so.

The fuller behavior stays available on the same `ptah-compat` surface through a
`PTAH_*` environment variable. It is an environment variable rather than a flag
on purpose: the compatibility binary's flags are held to parity with the pinned
community CLI, so a flag Atlas does not have would break the very drop-in
promise it was added to serve. Native `ptah` verbs always emit everything Ptah
models, with no switch to set.

This matters most if you are coming from Atlas **Pro** rather than CE. The
compatibility surface is the migration path for Pro scripts and configuration
too, not only CE ones — a capability you could only reach by rewriting your
pipeline against native `ptah` verbs would not be a migration path at all.

### The variables

**`PTAH_ATLAS_INSPECT_ALL_BLOCKS`** — by default, `ptah-compat schema inspect`
leaves an `extension`, `sequence` or `policy` block out of PostgreSQL HCL
output when nothing else in the document depends on it, and reports each
omission on standard error. For an extension, "depends on" is measured against
what the catalog says the extension supplies — `isn` supplies the type `isbn` —
rather than against its name. Set it to `1` and every block Ptah models is
emitted: the output describes the database in full, and the community CLI
refuses it.

**`PTAH_POSTGRES_INSPECT_ALL_ROLES`** — by default, a PostgreSQL read describes
only the roles the inspected schemas use, because roles are cluster-wide and a
description of one database is not a place to list another tenant's roles. Each
read reports on standard error how many managed roles it left out. Set it to
`1` and every role Ptah manages on the server is described again, which is what
you need to reproduce one cluster's roles in another. It widens the description
only: comparison already treats undescribed roles as present, so the planned
statements are identical either way. Reserved `pg_` names and the bootstrap
`postgres` superuser are outside it in both directions.

**`PTAH_ALLOW_EXTERNAL_SCHEMA`** — by default, `atlas.hcl`
`data "external_schema"` is not evaluated, because it runs a
repository-controlled program. Set it to `1` and the data source is evaluated,
matching the native `--allow-external-schema` flag.

### One shape has no Atlas-readable form at all

Suppression can only leave out a block nothing else names. A **sequence behind a
column default** is named, so the block stays and the document is not readable
by the community CLI:

```sql
CREATE SEQUENCE order_seq;
CREATE TABLE orders (id integer NOT NULL DEFAULT nextval('order_seq'::regclass));
```

This is not a gap Ptah can close. Measured on PostgreSQL 17, the community CLI's
own inspect of that database emits
`default = sql("nextval('order_seq'::regclass)")` with no `sequence` block, and
then cannot read its own output back: `pq: relation "order_seq" does not exist`.
There is no faithful description of that database the CLI can read — not Ptah's
and not its own. Ptah keeps the sequence, so the document is at least readable
by Ptah and true about the database, and says so on standard error. Dropping the
column's default to make the file readable would describe a database you do not
have, which is the one outcome worse than a refusal.

So `ptah-compat schema inspect` is not a promise that every PostgreSQL database
produces community-CLI-readable HCL. It is a promise that the output is always
self-consistent, that nothing disappears without being reported, and that the
full description is one environment variable away.

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../conformance/) for current evidence and
[Comparison](../comparison/) for tracked product, coverage, and
documentation gaps.

## Next steps

- Running migration directories with Atlas verbs:
  [Atlas migrate commands](../migrate-commands/).
- Inspecting, diffing, and applying schemas with Atlas verbs:
  [Atlas schema commands](../schema-commands/).
- Evaluating the compatibility claims first:
  [Conformance](../conformance/).
