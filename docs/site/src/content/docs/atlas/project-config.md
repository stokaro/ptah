---
title: Atlas project config
description: Supported `atlas.hcl` project configuration subset.
---

Ptah can read a strict subset of Atlas project configuration from `atlas.hcl`
and translate it into Ptah's project config IR. This is command configuration,
not schema HCL input. For schema HCL, see [HCL schema](../../reference/hcl-schema/).
`ptah-compat <command> ...` invocations on this page run the separate
`ptah-compat` drop-in binary; see the
[Atlas compatibility overview](../overview/).

## Supported blocks

Ptah accepts these local configuration blocks:

- top-level `variable`
- top-level `locals`
- `data "hcl_schema"` for local schema file data
- `data "external_schema"` for program-generated desired state
- `env` blocks, with either one label or no label
- top-level and env-local `lint`
- env-local `schema`, `migration`, `format`, and `diff`

Atlas Cloud, registry, remote directory, and unsupported data-source constructs
fail explicitly.

## Example

```hcl
lint {
  git {
    base = "origin/master"
    dir  = "."
  }
}

env "local" {
  url     = "postgres://user:pass@localhost:5432/app?sslmode=disable"
  dev     = "postgres://user:pass@localhost:5432/app_shadow?sslmode=disable"
  src     = ["file://schema.hcl"]
  exclude = ["tmp_*"]

  schema {
    src = ["file://schema.hcl"]
    mode {
      funcs       = false
      permissions = false
      roles       = false
      triggers    = false
    }
  }

  migration {
    dir              = "file://migrations"
    format           = "atlas"
    revisions_schema = "atlas"
    lock_timeout     = "3s"
    exec_order       = "linear"
    tx_mode          = "file"
  }

  lint {
    latest = 5
    destructive {
      error = false
    }
  }

  format {
    schema {
      inspect = "json"
      apply   = "{{ sql . \"  \" }}"
      diff    = "{{ sql . \"\" }}"
    }
    migrate {
      apply = "{{ json . }}"
      diff  = "{{ sql . \"\" }}"
    }
  }

  diff {
    skip {
      drop_table = true
    }
    concurrent_index {
      create = true
    }
  }
}
```

## Mapping to Ptah behavior

| Atlas setting | Ptah behavior |
| --- | --- |
| `env.url` | Default database URL for compatible schema and migration commands. |
| `env.dev` | Default disposable replay database URL. Rollback verification resets it and requires it to identify a different database from `env.url`. |
| `env.src` | Default desired schema source for `schema apply`. |
| `env.schema.src` | Default desired schema source for `schema apply`, `schema diff`, and `migrate diff`. |
| `env.schema.mode.<object>` | Default object-kind exclusions for supported schema object kinds. |
| `env.exclude` | Default Atlas-style resource exclusion filters. |
| `migration.dir` | Default migration directory. |
| `migration.format` | Default migration directory format where supported; safety gate for `migrate apply`. |
| `migration.revisions_schema` | Default revision metadata schema. |
| `migration.lock_timeout` | Default migration lock timeout. |
| `migration.exec_order` | Default migration execution order. |
| `migration.tx_mode` | Default transaction mode for compatible apply paths. |
| `lint.latest` | Latest-N migration lint selection. |
| `lint.git.base` | Git base for migration lint selection. |
| `lint.git.dir` | Git working directory for migration lint selection. |
| `lint.<analyzer>.error` | Severity mapping for supported Ptah lint rule families. |
| `lint.log` | Atlas Go-template that renders `migrate lint` output; shares the `format.migrate.lint` IR and precedence. |
| `format.schema.inspect` | Default `schema inspect --format`. |
| `format.schema.apply` | Default `schema apply --format`. |
| `format.schema.diff` | Default `schema diff --format`. |
| `format.migrate.apply` | Default `migrate apply --format`. |
| `format.migrate.diff` | Default `migrate diff --format`. |
| `format.migrate.lint` | Default `migrate lint --format`. |
| `format.migrate.status` | Default `migrate status --format`. |
| `diff.skip.drop_table` | Suppresses table drops in supported local diff/apply plans. |
| `diff.concurrent_index.create` | Requests PostgreSQL concurrent index creation where transaction mode allows it. |
| `diff.concurrent_index.drop` | Requests PostgreSQL `DROP INDEX CONCURRENTLY` for standalone index removals. |

`migration.tx_mode` accepts `file`, `all`, or `none`. A migration's leading
`atlas:txmode file` or `atlas:txmode none` header overrides global `file` or
`none`; global `all` rejects every explicit file mode before the selected batch
starts. An explicit file mode under global `none` restores a per-file
transaction and permits migration timeouts.

Project config precedence is explicit CLI flags, environment variables,
`atlas.hcl`, `ptah.yaml`, then built-in defaults. Project-file merging
preserves source presence. For a supported field, an explicitly present value
replaces the lower-precedence value instead of being treated as absent. This
includes an empty string, zero, `false`, or an empty list when the field
accepts that type.

After project sources are merged, a command applies its built-in default only
when a field is absent. An explicitly present empty or zero value instead
reaches normal validation. Fields that do not accept empty values, including
Atlas format templates, fail during parsing or command validation.

When a forwarded native implementation also consumes project configuration,
the adapter passes this merged snapshot instead of reopening either project
file. This currently applies to `migrate down`; other adapters map evaluated
Atlas project values to explicit native command arguments.

`env.exclude` and disabled `env.schema.mode` values compose with the
`schema apply`/`schema diff` positive selection flags in a fixed order:
`--schema` names define the schema universe, `--include` selectors pick
resources inside it, and the configured exclusions subtract from that
selection last, exactly like CLI `--exclude`. See
[Scope the comparison with `--schema` and `--include`](../schema-commands/#scope-the-comparison-with---schema-and---include).

`env.schema.mode.sensitive` accepts Atlas's `DENY` and `ALLOW` values. Both are
no-ops because Ptah does not emit sensitive values through the supported local
workflows. Ptah records either spelling as an ignored compatibility construct
and warns that it has no effect.

Ptah accepts Atlas's `atlas`, `golang-migrate`, `goose`, `flyway`,
`liquibase`, and `dbmate` values while evaluating `atlas.hcl`, and
`ptah-compat migrate apply` executes all of them.

The native `atlas` format is read from disk unchanged (preserving `atlas.sum`
verification and down migrations); every other format is converted in memory
to Atlas single-file, up-only migrations, so apply runs only the source tool's
forward (up) SQL. Unknown formats and, currently, Flyway repeatable migrations
still fail before the target database is opened.

An explicit `?format=` query on the effective directory URL, whether declared
by `migration.dir` or passed with CLI `--dir`, overrides this project default,
matching Atlas's URL precedence. An empty query value selects the native
`atlas` format.

```bash
# Apply a Goose directory directly.
ptah-compat migrate apply --env local \
  --dir "file://migrations?format=goose"
```

Apply and `ptah-compat migrate import` share one format-loading implementation,
so they agree on every format's up/down semantics. See
[`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742).

`env.src` and `env.schema.src` provide local schema-file defaults for
`schema apply` and `schema diff`. `migrate diff` resolves the same defaults
through its typed desired-state resolver, so they can contain local schema
files or one directly connectable database URL. Plain local schema paths and
relative `file://` schema URLs declared in `atlas.hcl` resolve relative to the
directory containing that `atlas.hcl` file, not the process working directory.
Explicit CLI `--to` and `--from` values keep CLI semantics and resolve relative
to the process working directory unless they are absolute.

The Atlas-compatible schema commands and `migrate diff` also accept explicit
`env://` references. `env://src` and `env://schema.src` expand the selected
environment's schema sources through the typed desired-schema resolver, so the
expanded value can be a supported local file, database URL, or declared
external schema program. `env://url` and `env://dev` resolve the corresponding
database URL; `env://migration.dir` resolves the configured local migration
directory. Nested `env://` references fail explicitly.

## Reading files with file() and fileset()

`file("path")` inlines a file's contents into a config value, and
`fileset("glob")` expands to a list of paths. Both resolve relative to the
directory holding `atlas.hcl`, and both are confined to it. These are refused,
with the reason named:

| Argument | Refused because |
| --- | --- |
| `file("/run/secrets/db")` | absolute path |
| `file("../secrets/db")` | parent traversal |
| `file("link.txt")` where `link.txt` points outside the directory | symbolic link leaving the directory |
| `fileset("*.hcl")` where one match points outside the directory | the whole call fails; escaping entries are never dropped silently |

A symbolic link that stays inside the directory is read normally, including one
that walks up and back down inside it. The rule is about where the path goes,
not about links.

Pass a value that genuinely lives elsewhere through the environment instead:

```hcl
env "local" {
  url = getenv("DATABASE_URL")
}
```

This is stricter than the community binary, deliberately. Measured against the
pinned community v1.3.0 build: an `atlas.hcl` calling `file("/etc/passwd")` or
`file("../../../../etc/passwd")` exits 0 there with the file read, and the
contents reach an observable place — a database URL, an error message on
standard error. An `atlas.hcl` is repository-controlled and evaluated before
anything is applied, so matching that would hand any config author an
arbitrary-file read on the machine running the migration. The exit code is 1
either way today, so no working configuration changes; what changes is that the
refusal now names its reason. See
[`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042).

The confinement covers what `file()` and `fileset()` read. It is not a claim
that `atlas.hcl` cannot name a path outside its directory at all: a schema
source such as `src = "../shared/schema.hcl"` is a path the author points the
tool at deliberately, and it still resolves.

## Local schema data source

`data "hcl_schema"` names local schema files and exposes them as
`data.hcl_schema.<name>.url` — one `file://` URL for `path`, a list of them for
`paths`.

Its paths resolve relative to the directory holding `atlas.hcl` but are not
confined to it: `path = "../shared/schema.hcl"` is a file the author points the
tool at deliberately, and it resolves. Two kinds of value are refused, and each
names its own rule rather than blaming the `path` key, which is supported:

| Value | Refusal |
| --- | --- |
| `path = "/etc/absolute.hcl"` | `atlas.hcl "path" at atlas.hcl:2: absolute paths are not supported: /etc/absolute.hcl: give a path relative to the directory holding atlas.hcl` |
| `path = "s3://bucket/x.hcl"` | `atlas.hcl "path" at atlas.hcl:2: unsupported URL scheme: s3://bucket/x.hcl` |

`paths` is refused the same way, naming `paths`. An attribute the data source
does not have — Atlas's `vars`, for one — is a different failure and keeps the
construct wording: `unsupported atlas.hcl construct "vars"`.

## External schema data source

`data "external_schema"` declares a program whose standard output is the
desired schema. Selecting its `.url` as an env's desired-state source makes
that program the source of truth for the environment:

```hcl
data "external_schema" "app" {
  program = ["python3", "export.py"]
  format  = "sql"
}

env "dev" {
  url = "sqlite://app.db"
  src = data.external_schema.app.url
}
```

The program runs directly with an explicit argument vector — never through a
shell — and must print the complete desired schema to standard output. This is
the `atlas.hcl` spelling of the native `ptah.yaml` `external_schema` block and
the `--schema-cmd` flag; all three share one execution path. See
[External and ORM schema sources](../../schema/orm-and-external/).

| Attribute | Meaning |
| --- | --- |
| `program` | Required argv list. `program[0]` is the executable; no shell runs. |
| `format` | Stdout format: `sql` (default), `hcl`, or `yaml`. Ptah extension. |
| `working_dir` | Program working directory. Relative values resolve against the `atlas.hcl` directory. Ptah extension. |
| `env` | Extra `KEY=VALUE` entries for the program. `PATH` and `PWD` cannot be overridden. Ptah extension. |

The data source follows strict placement rules:

- Its `.url` value is only valid as the selected env's desired-state source
  (`env.src` or `env.schema.src`) and must be that source's only value.
  Referencing it from `url`, `dev`, `migration.dir`, or `exclude` fails
  explicitly.
- A declared-but-unreferenced data source is ignored and never executed.
- When the selected env's desired state is an external schema source, it
  replaces a `ptah.yaml` `external_schema` block wholesale, so the two config
  files never mix into one hybrid program configuration.

Executing repository-controlled code from an auto-discovered config file
requires an explicit opt-in, in both binaries:

- Native `ptah` commands that consume project config (for example
  `ptah schema render --env dev`) require `--allow-external-schema` or its
  `PTAH_ALLOW_EXTERNAL_SCHEMA` environment twin. Without it, the command fails
  with `atlas.hcl data.external_schema is disabled by default; pass
  --allow-external-schema to execute it`.
- `ptah-compat` keeps the Atlas-identical flag surface, so the opt-in is the
  `PTAH_ALLOW_EXTERNAL_SCHEMA=1` environment variable. Without it, commands
  fail during source classification, before the program could run.

`ptah-compat schema diff`, `schema apply`, `schema inspect` (spelled
`--url env://src`), and `migrate diff` consume the source. `schema plan` and
`schema test` do not support it yet and fail explicitly.

Community Atlas rejects this data source entirely: measured 2026-08-01 with a
logged-out Atlas CE v1.2.0 binary, an `atlas.hcl` declaring
`data "external_schema"` fails with exit 1 and
`Error: data.external_schema is not supported by the community version of
Atlas.` Ptah evaluates it in the open build, behind the opt-in described
above.

When an `atlas.hcl` `migration` block is present, Ptah defaults
`revision-format` to `atlas`, so migration commands use
`atlas_schema_revisions` unless an explicit CLI flag overrides it. Relative
`migration.dir` values declared in `atlas.hcl` resolve relative to the
directory containing that `atlas.hcl` file.

Explicit CLI `--dir` values keep CLI semantics and resolve relative to the
process working directory unless they are absolute. Apply, down, status, lint,
set, and native repair commands open the resolved directory through a rooted
handle and capture an immutable snapshot before database work. Relative CLI
traversal and symlink escapes are rejected; explicit absolute paths remain
supported.

Intentional project-relative paths such as `../shared-migrations` retain their
config-relative meaning and are captured at the resolved location. Non-local
URI schemes in `migration.dir` and `schema.src` fail explicitly when a command
needs that configured value; an explicit CLI path flag still wins before URI
validation.

## Environment selection

Use Atlas project flags on commands under `ptah-compat schema ...` and
`ptah-compat migrate ...`:

```bash
ptah-compat schema inspect --config project.hcl --env local
ptah-compat migrate apply -c project.hcl --env local
ptah-compat migrate hash --env local --var dir=migrations
```

`--config` and `-c` select a local project config path. `file://` config URLs
are accepted; other URL schemes fail explicitly. `--var name=value` can be
repeated. Repeating the same variable name produces a string list for supported
Atlas HCL expressions.

A `--var` carrying no `=` is refused where it is written, on every verb, in the
community CLI's own words
([#1231](https://github.com/stokaro/ptah/issues/1231)):

```bash
ptah-compat migrate status --dir file://migrations --url "$DATABASE_URL" --var novalue
# Error: invalid argument "novalue" for "--var" flag: variables must be format as key=value, got: "novalue"
```

The refusal precedes everything the verb itself requires — the missing `--url`,
the missing `--dir`, the arity check — and it fires on every verb, including the
ones that never read the flag. A value is checked field by field as CSV, so
`--var a=1,b` is refused naming `b`. Only the separator is required within a
field: an empty name and an empty value are both accepted, because both are
accepted there.

`PTAH_VAR` carries the same rule and the same sentence, since the value reaches
the same check:

```text
Error: invalid argument "novalue" for "--var" flag: variables must be format as key=value, got: "novalue"
```

Variable blocks accept the `type` constraints `string`, `number`, `bool`,
`list(string)`, and `map(string)`. `--var` overrides convert to scalar types,
and repeated flags fill a `list(string)` variable. `map(string)` values come
from defaults or HCL expressions because the string/list flag syntax does not
encode maps. Overrides of the wrong shape, defaults that do not match the
declared type, and other constraints such as `object(...)` fail with named errors.
`sensitive = true` is accepted; parse-time conversion errors print
`(sensitive value)` instead of the variable's value, though a sensitive value
interpolated into a URL or path can still appear in downstream errors that
print that URL or path. `validation` blocks remain unsupported and fail
explicitly.

If an `atlas.hcl` file has exactly one `env` block, named or unnamed, Ptah can
use it as the default. Atlas-compatible `migrate apply` does not need to select
an environment when both `--url` and `--dir` are explicit. Other ambiguous or
unsupported environment layouts fail instead of guessing.

### Expand one environment into several targets

Use `for_each` when one Atlas environment applies the same migration directory
to several databases:

```hcl
env {
  for_each = toset([
    "sqlite://bar.db?_fk=1",
    "sqlite://foo.db?_fk=1",
  ])
  name = atlas.env
  url  = each.value

  migration {
    dir = "file://migrations"
  }
}
```

`atlas.env` is the requested `--env` value. `each.key` and `each.value` expose
the current collection entry. For an unlabeled block,
`name` must depend on `atlas.env`; a static name or a name based only on
`each.key` does not define the requested environment. A labeled block uses its
label as the initial candidate when `name` is absent. Every expanded instance
of an admitted block is evaluated before its resulting name is filtered, so an
invalid nonmatching instance still fails. Tuples and lists keep source order;
objects, maps, and sets use stable key order. As a Ptah extension, typed list
and map values are accepted for env `for_each` in addition to tuple, object,
and set values.

`ptah-compat migrate apply --env local` runs every selected target sequentially
and stops at the first failure. A formatted run emits one document per attempted
target with one newline between adjacent documents. Commands that require one
project instance reject a multi-instance selection instead of choosing one.

## Structural validation and ignored names

Ptah gives each project-config name one of three outcomes:

| Outcome | Result |
| --- | --- |
| Supported | Parsed into project config; expressions are evaluated for the selected environment. |
| Structurally unsupported | Fails with `unsupported atlas.hcl construct ...`, including in an unselected environment. |
| Ignored by Atlas CE | Accepted for compatibility and reported on stderr as having no effect. |

The ignored category contains only names that Atlas CE itself accepts without
acting on. Ptah does not silently discard them. A successful command reports
each ignored source location once:

```text
warning: atlas.hcl attribute "project" at atlas.hcl:2 is ignored for Atlas compatibility and has no effect
```

Structural validation covers every `env` block, including environments that
are not selected for the current command. An unsupported attribute, nested
block, label, or duplicate therefore fails even when it appears in another
environment. Expressions inside `env` blocks, including ignored attributes and
block bodies, are evaluated only in the selected environment. An unselected
environment may therefore refer to variables, files, or environment values
unavailable in the current invocation. Global `variable`, `locals`, and `data`
blocks are evaluated separately to build the shared context before environment
selection.
