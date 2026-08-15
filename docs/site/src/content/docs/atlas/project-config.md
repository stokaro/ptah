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
      inspect = "{{ json . }}"
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
| `env.schemas` | Restricts the schema universe that compatible `schema inspect`, `schema apply`, `schema diff`, `schema plan`, and `migrate diff` operate over. |
| `migration.baseline` | Migration version `migrate apply` marks applied before running the pending ones; the config spelling of its `--baseline` flag, which still wins when passed. |
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

### `migration.baseline`

`baseline` names the migration version `migrate apply` marks as already applied
before it runs the pending ones — the `atlas.hcl` spelling of the command's
`--baseline` flag. The flag still wins when it is passed, so a project file sets
the default and a caller can override it for one run.

The value is a version, resolved against the migration directory: one that the
directory does not hold fails with
`baseline version "20200101000000" not found`. `baseline = null` and
`baseline = ""` are both read as "no baseline", which leaves every migration
pending. A value that is not a string is refused with
`atlas.hcl "baseline" at atlas.hcl:5 must be a string`.

### `env.schemas`

`schemas` names the schemas the environment operates over. It must be a list of
strings; a bare string, an object, or a list holding anything but strings is
refused with its source location, which is what Atlas CE does with the same
file.

The list restricts the schema universe rather than filtering the output, so a
schema it does not name is not read at all:

```hcl
env "local" {
  url     = getenv("DATABASE_URL")
  schemas = ["app", "audit"]
}
```

Semantics, all measured against a PostgreSQL database holding schemas `one`,
`two`, and `public`:

| Value | Schemas described |
| --- | --- |
| `schemas = ["one"]` | `one` |
| `schemas = ["one", "two"]` | `one` and `two` |
| `schemas = ["nosuchschema"]` | none; the command still exits 0 |
| `schemas = []` | all of them — an empty list is not a selection |
| attribute absent | all of them |

`--schema` outranks the attribute outright and does not intersect with it: with
`schemas = ["one"]` in the file, `--schema two` describes `two` alone.

Restricting the universe means Ptah describes less than it did before the
attribute was honored. Set `PTAH_ATLAS_IGNORE_ENV_SCHEMAS=1` to keep the
realm-wide description; the attribute is then reported as having no effect, as
any other tolerated name is. The variable governs the selection only — a value
the field cannot hold is refused with it set exactly as it is without it,
because Atlas CE refuses that file and compatibility never exits 0 where the
community binary exits 1.

A present value the variable cannot hold is refused on every Atlas
project-config load or parse, before the document is read: whether the file is
absent, whether it parses at all, and whether the selected environment spells
`schemas` make no difference to the diagnostic. The variable is a property of
the environment, so the answer may not depend on the file under it.
`PTAH_ATLAS_STRICT_COMPAT=1` rejects an enabled opt-out because restoring the
realm-wide Ptah view is deliberately outside the CE-only profile.

`format.schema.inspect` follows the Atlas-compatible command's template
semantics. The exact bare values `"hcl"`, `"sql"`, and `"json"` write those
literal bytes with no line feed. Surrounding whitespace is also preserved; for
example, `" sql "` writes hex `20 73 71 6c 20`. Use `"{{ hcl . }}"`,
`"{{ sql . }}"`, or `"{{ json . }}"` to render the inspected schema. Native
`ptah schema inspect --format hcl|sql|json` keeps its rendered shorthands.

`migration.tx_mode` accepts `file`, `all`, or `none`. A migration's leading
`atlas:txmode file` or `atlas:txmode none` header overrides global `file` or
`none`; global `all` rejects every explicit file mode before the selected batch
starts. An explicit file mode under global `none` restores a per-file
transaction and permits migration timeouts. The header is significant only in
the unbroken run of line comments that begins on line 1; a directive outside
that block is ignored, as it is on Atlas CE, and reported at `WARN` rather than
dropped in silence.

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
`--schema` names define the universe for schema-owned resources, `--include`
selectors pick resources, and the configured exclusions subtract last, exactly
like CLI `--exclude`. Database-wide extensions skip the schema ownership
restriction. An extension-only include filters their identities; a matching
non-extension resource carries all extensions as support even beside extension
selectors without treating an omitted desired extension as a removal.
Schema-only and extension-only scopes remain authoritative. Exclusions still
subtract afterward. See
[Scope the comparison with `--schema` and `--include`](../schema-commands/#scope-the-comparison-with---schema-and---include).

`env.schema.mode.sensitive` accepts Atlas's `DENY` and `ALLOW` values. Both are
no-ops because Ptah does not emit sensitive values through the supported local
workflows. Ptah records either spelling as an ignored compatibility construct
and warns that it has no effect.

Ptah accepts Atlas's `atlas`, `golang-migrate`, `goose`, `flyway`,
`liquibase`, and `dbmate` values while evaluating `atlas.hcl`, and
`ptah-compat migrate apply` executes all of them.

The native `atlas` format is read from disk unchanged (preserving `atlas.sum`
verification, down migrations, and Atlas `R`/`<number>R` repeatable migration
tokens); every other format is converted in memory to Atlas single-file,
up-only migrations, so apply runs only the source tool's forward (up) SQL.
Flyway repeatables in converted directories are treated as one-time migrations.
Unknown formats still fail before the target database is opened.

An explicit `?format=` query on the effective directory URL, whether declared
by `migration.dir` or passed with CLI `--dir`, overrides this project default,
matching Atlas's URL precedence. An empty query value selects the native
`atlas` format.

```bash
# Apply a Goose directory directly.
ptah-compat migrate apply --env local \
  --dir "file://migrations?format=goose"
```

Apply and `ptah-compat migrate import` share the format parsers and up/down
semantics. Conventional Liquibase import adds a persistence adapter that emits
one numeric Atlas file per changeset; direct apply retains its numbered-file
requirement and source-file boundary. See
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
does not have is a different failure and keeps the construct wording:
`unsupported atlas.hcl construct "frobnicate"`.

### `vars` is scoped to the files the data source selects

`vars` supplies values for the `variable` blocks of the schema files this data
source names, and only those files:

```hcl
data "hcl_schema" "app" {
  paths = ["schema.hcl"]
  vars = {
    tenant = "acme"
  }
}

env "local" {
  url = "sqlite://app.db"
  src = data.hcl_schema.app.url
}
```

The scoping is the whole point, and it runs in both directions. Another data
source's `vars` never reach these files, and the run's global `--var` does not
cross the boundary either — a data source that declares no `vars` at all still
closes it, so a schema file behind one with a required variable and no value
fails rather than picking the flag up. A file named directly, as
`src = "file://schema.hcl"`, is outside every data source and does take `--var`.

The map takes strings, numbers and bools; each is carried as the text of the
literal, so `tenant = 42` reaches the file as `"42"`. A name the file does not
declare is ignored. `vars = null` and `vars = {}` are both read as "no values
given", and a value that is not a map — `vars = "acme"`, `vars = [1, 2]` — is
refused with `atlas.hcl "vars" at atlas.hcl:3 must be a map of values`.

Two data sources may not both select the same file with different `vars` and
both be referenced by one `src`: the parse refuses and names both blocks, rather
than picking one and making the desired state depend on map order.

A `null` reaching a name Ptah acts on is refused, and the refusal names the type
the setting wants. With `variable "s" { type = string, default = null }`, both
`dev = null` and `dev = var.s` produce
`atlas.hcl "dev" at atlas.hcl:8 must be a string`; the declared type of the
variable makes no difference, and the `bool`, `number` and `list(string)`
settings behave the same way. This is stricter than the community binary, which
reads a null as an unset field and exits 0. It is the same standing divergence
as the label-arity and duplicate refusals, and it can only reject a file that
binary reads, never accept one it rejects. Names Ptah merely reports as ignored
are the other case and do accept `null` — see
[Settings Ptah does not act on are still type-checked](#settings-ptah-does-not-act-on-are-still-type-checked).

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
`atlas_schema_revisions` unless an explicit CLI flag overrides it.
`migration.dir` values declared in `atlas.hcl` resolve relative to the directory
containing that `atlas.hcl` file and must remain inside that project root after
symbolic-link resolution. The same confinement applies when the project file
uses an absolute value.

Explicit CLI `--dir` values keep CLI semantics and resolve relative to the
process working directory unless they are absolute. Apply, down, status, lint,
set, and native repair commands open the resolved directory through a rooted
handle and capture an immutable snapshot before database work. Relative CLI
traversal and symlink escapes are rejected; explicit absolute paths remain
supported.

Parent-relative paths that resolve outside the project root, absolute paths
outside it, and symbolic links that leave it fail as `outside allowed root`.
Non-local URI schemes in `migration.dir` and `schema.src` fail explicitly when
a command needs that configured value; an explicit CLI path flag still wins
before URI validation.

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

### Structured settings must be written as blocks

A setting Atlas CE decodes into a structure takes a **block** body. The same
name written as an **attribute** with an object value is refused:

```text
Error: atlas.hcl "lint" at atlas.hcl:3 must be a block, or an empty object
```

This is not a Ptah restriction. Atlas CE routes both spellings to the same
field, and its object decoder refuses every member name it finds there —
including the members the block spelling accepts, so `lint = { latest = 1 }`
and `lint = { anything = 1 }` both fail on the community binary. The attribute
spelling carries no configuration on either binary. The two values that are
accepted are an empty object and `null`, for the same reason: they carry
nothing.

The affected names were measured one at a time and the set is neither "every
block name" nor scope-independent:

| Scope | Must be a block | Tolerated as an attribute |
| --- | --- | --- |
| top level | `diff`, `lint`, `test` | `atlas`, `data`, `format`, `locals`, `migration`, `schema`, `variable` |
| `env` | `diff`, `format`, `lint`, `migration`, `schema`, `test` | — |
| `diff` and `env.diff` | `skip` | `concurrent_index` |
| `lint` and `env.lint` | `git` | `concurrent_index`, `condrop`, `data_depend`, `destructive`, `incompatible`, `nestedtx` |
| `env.format` | `migrate`, `schema` | — |
| `env.migration` | `repo` | `skip_report` |
| `env.schema` | `repo` | `mode` |
| `test` and `env.test` | `migrate`, `schema` | everything else |

`diff` and `lint` are the two blocks that may sit at the top level as well as
inside `env`, and their nested names behave the same in both places: top-level
`diff { skip = { k = "v" } }` and `lint { git = { k = "v" } }` are refused, the
same as their `env` spellings.

The `format`, `migration` and `schema` rows stay `env`-only, and that is measured
rather than assumed. A top-level `format`, `migration` or `schema` block is not
decoded into those structures by the community binary, so top-level
`format { schema = { k = "v" } }`, `migration { repo = { k = "v" } }` and
`schema { repo = { k = "v" } }` all exit 0 — Ptah drops the whole block with an
ignored-block warning and exits 0 too.

The `test` row is the one that applies inside a block with no effect at all.
Neither binary implements `test`: it is dropped whole and reported as ignored.
The community binary still runs its object decoder on `migrate` and `schema`
within it, so `test { schema = { q = "v" } }` fails on both while
`test { schema = {} }` and `test { schema "s" { src = ["file://t.hcl"] } }` do
not.

Writing any of these as a block is unaffected.

This refusal is a value rule, not a structural one, so it follows the same
selection boundary as every other value: it applies to the environment the
command selects. An `env "prod"` carrying `lint = { k = "v" }` does not fail a
command run with `--env dev`, because Atlas CE does not decode an unselected
environment either. The value is read after `var`, `local` and `data` are
available, so `lint = local.nothing` resolves normally.

### Settings Ptah does not act on are still type-checked

A handful of names are decoded by Atlas CE into a plain string, bool or
string-list field that Ptah has no equivalent for. Ptah accepts the name and
reports it as having no effect, but the **value** still has to be the kind the
community binary requires, because that binary refuses a wrong-typed value
before any command runs:

```text
Error: atlas.hcl "drop_column" at atlas.hcl:5 must be a bool
```

| Scope | Name | Required value |
| --- | --- | --- |
| `diff.skip` and `env.diff.skip` | `add_schema`, `modify_schema`, `add_table`, `modify_table`, `add_column`, `modify_column`, `drop_column`, `add_index`, `modify_index`, `drop_index`, `add_foreign_key`, `modify_foreign_key`, `drop_foreign_key` | a bool |
| `lint` and `env.lint` | `review` | a string |
| `env` | `include` | a list of strings |
| `env.migration` | `exclude` | a list of strings |
| top level | `env` written as an attribute | a block; only `null` is accepted as a value |

`null` is accepted for every one of them, as it is on the community binary.
`drop_schema` and `drop_table` are absent from the table because Ptah acts on
them, so they are ordinary supported names rather than ignored ones.

This is a value rule too, with the same selection boundary and the same reading
order as the block rule above, so `drop_column = var.flag` resolves normally.

A list-valued name takes a tuple, a list or a set of strings, so
`include = toset(["a", "b"])` is accepted. A null *element* inside one is not,
which is the one place `null` stops being accepted here — the community binary
answers `null value is not allowed` for `["public.t1", null]`, and it makes no
difference whether the list was written literally or came from a
`variable "tables" { type = list(string) }`. One bare string is not a
one-element list, and an object is refused even when it is empty —
`include = {}` fails where `repo = {}` succeeds. That empty object is the shape that separates a
list-valued name from a struct-valued one, and the top-level `env` row takes it
one step further: Atlas CE fills that field from `env` blocks and decodes no
value spelling for it at all, so `env = {}` is refused as well. The `env` block
spelling is untouched.

`env.migration.baseline` is no longer one of them. It is a supported name now:
`migrate apply` reads it as the config spelling of `--baseline`, and the flag
still wins when it is passed. See
[`migration.baseline`](#migrationbaseline). `env.include` and
`env.migration.exclude` are still type-checked and not acted on — neither has a
Ptah setting behind it, and `env.exclude` is the separate, supported name.

The scope matters throughout — a `baseline` written at `env` level, inside
`lint`, or in a top-level `migration` block is not decoded by the community
binary, and neither is an `include` outside `env` or an `exclude` outside
`env.migration`, so any value is accepted in those places.

The membership is measured name by name and is not "every change kind":
`add_view`, `drop_func`, `modify_trigger`, `add_type`, `drop_sequence`,
`add_check`, `drop_role`, `add_policy`, `add_extension` and `drop_domain` are
among the names the community binary does not decode inside `skip`, so any value
is accepted for them.

### An unknown block is tolerated where an unknown attribute is

A body that tolerates a name it does not implement tolerates it in either
spelling. `diff`, `env.diff`, `lint`, `env.lint`, `env.format.migrate` and
`env.format.schema` accept an unknown attribute **and** an unknown nested block,
so a format extension such as `format { migrate { custom { } } }` is accepted
and reported as having no effect rather than refused, exactly as the community
binary reads it.

The bodies that refuse a nested block are the leaves, and they are the whole
list:

| Scope | Leaf bodies that refuse a nested block |
| --- | --- |
| `diff` and `env.diff` | `concurrent_index`, `skip` |
| `lint` and `env.lint` | `concurrent_index`, `condrop`, `data_depend`, `destructive`, `git`, `incompatible`, `nestedtx` |
| `env.schema` | `mode`, `repo` |
| `env.migration` | `repo` |

```text
Error: unsupported atlas.hcl construct "anything" at atlas.hcl:5
```

All 21 scope-and-leaf pairs were measured one at a time and the community binary
exits 0 on every one of them, so this is a known remaining divergence in the
loud direction — the same standing policy as the label-arity and duplicate
refusals. It never accepts a project file that binary rejects.

Structural validation covers every `env` block, including environments that
are not selected for the current command. A structurally unsupported construct
therefore fails even when it appears in another environment: a data-source
field, a label on a supported block that takes none, a duplicate supported
block, or a nested block inside one of the leaf bodies above. An unknown
attribute and an unknown nested block in a tolerant body are not in that
category and are accepted in a selected and an unselected environment alike.

Expressions inside `env` blocks, including ignored attributes and
block bodies, are evaluated only in the selected environment. An unselected
environment may therefore refer to variables, files, or environment values
unavailable in the current invocation. Global `variable`, `locals`, and `data`
blocks are evaluated separately to build the shared context before environment
selection.
