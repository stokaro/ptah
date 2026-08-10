# Atlas project config

Ptah can read a limited Atlas project config subset from `atlas.hcl` and
translate it into Ptah's project config IR. This is project configuration for
commands, not schema HCL input. Schema HCL input is documented separately in
[HCL Schema Input](atlas_hcl_schema.md). `ptah-compat <command> ...`
invocations in this document run the separate `ptah-compat` drop-in binary.

## Supported subset

Ptah accepts top-level `variable`, `locals`, `data "hcl_schema"`,
`data "external_schema"`, `env`, `lint`, and `diff` blocks. `env` blocks may have either one label or no label:

```hcl
lint {
  git {
    base = "origin/master"
    dir  = "."
  }
}

env "local" {
  url = "postgres://user:pass@localhost:5432/app?sslmode=disable"
  dev = "postgres://user:pass@localhost:5432/app_shadow?sslmode=disable"
  src = ["file://schema.hcl", "schema.sql"]
  exclude = ["tmp_*"]

  schema {
    src = ["file://schema.hcl", "schema.sql"]
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
    concurrent_index {
      error = true
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

The supported attributes map to Ptah settings as follows:

| Atlas setting | Ptah setting |
| --- | --- |
| `env.url` | `--db-url`, `ptah-compat schema inspect --url`, `ptah-compat schema apply --url`, `ptah-compat migrate apply --url`, or `ptah-compat migrate status --url` default |
| `env.dev` | Disposable replay database default for `migrations generate --shadow-db`, `migrations down --shadow-db`, and compatible `--dev-url` workflows; rollback verification resets it and requires it to identify a different database from `env.url` |
| `env.src` | `ptah-compat schema apply --to` default |
| `env.schema.src` | `ptah-compat schema apply --to`, `ptah-compat schema diff --to`, or `ptah-compat migrate diff --to` default |
| `env.schema.mode.<object>` | Atlas-style exclusion defaults for supported object kinds |
| `env.exclude` | `ptah-compat schema inspect --exclude`, `ptah-compat schema apply --exclude`, or `ptah-compat schema diff --exclude` default |
| `migration.dir` | `--migrations-dir` or `--dir` default |
| `migration.format` | `--dir-format` default where the command exposes that flag; safety gate for `ptah-compat migrate apply` |
| `migration.revisions_schema` | `--migrations-schema` default |
| `migration.lock_timeout` | `--lock-timeout` default |
| `migration.exec_order` | `--exec-order` default |
| `migration.tx_mode` | `migrations up --tx-mode` default |
| `lint.latest` | `migrations lint --latest` default |
| `lint.git.base` | `migrations lint --git-base` default |
| `lint.git.dir` | `migrations lint --git-dir` default |
| `lint.destructive.error` | `DS` lint rule-family severity |
| `lint.concurrent_index.error` | `PG101` and `PG103` lint rule severity |
| `lint.condrop.error` | `CD` lint rule-family and `DS105` lint rule severity |
| `lint.data_depend.error` | `DD` lint rule-family severity |
| `lint.incompatible.error` | `BC` lint rule-family severity |
| `lint.nestedtx.error` | `TX201` lint rule severity |
| `lint.log` | `ptah-compat migrate lint` output template; shares the `format.migrate.lint` default |
| `format.schema.inspect` | `ptah-compat schema inspect --format` default |
| `format.schema.apply` | `ptah-compat schema apply --format` default |
| `format.schema.clean` | `ptah-compat schema clean --format` default |
| `format.schema.diff` | `ptah-compat schema diff --format` default |
| `format.migrate.apply` | `ptah-compat migrate apply --format` default |
| `format.migrate.diff` | `ptah-compat migrate diff --format` default |
| `format.migrate.lint` | `ptah-compat migrate lint --format` default |
| `format.migrate.status` | `ptah-compat migrate status --format` default |
| `diff.skip.drop_table` | Drop-table suppression for local schema diff/apply planning |
| `diff.skip.drop_schema` | Accepted and type-checked; Ptah's planner emits no schema drop for it to suppress |
| `diff.concurrent_index.create` | PostgreSQL concurrent index creation where the command can execute without a surrounding transaction |
| `diff.concurrent_index.drop` | PostgreSQL concurrent index removal for standalone index drops, capability-gated |
| `env.schema.repo.name` | Accepted and type-checked; no local behavior, matching the community binary |

`env.src` and `env.schema.src` accept either one string or a list of strings.
The nested `schema.src` form matches Atlas project config syntax. Ptah
currently uses these values as local schema-file defaults for `schema apply`
and `schema diff`.

`migrate diff` resolves the same defaults through its typed desired-state
resolver, so they can contain local schema files or one directly connectable
database URL. Plain local schema paths and relative `file://` schema URLs
declared in `atlas.hcl` resolve relative to the directory containing that
`atlas.hcl` file, not the process working directory.

Explicit CLI `--to` and `--from` values keep CLI semantics and resolve
relative to the process working directory unless they are absolute.

`ptah-compat schema apply`, `ptah-compat schema diff`, and
`ptah-compat migrate diff` also accept explicit `env://` references.
`env://src` and `env://schema.src` expand the selected environment's schema
sources through the typed desired-schema resolver, so the expanded value can be
a supported local file or database URL. `env://url` and `env://dev` resolve the
corresponding database URL; `env://migration.dir` resolves the configured
local migration directory. Nested `env://` references fail explicitly.

Relative `migration.dir` values resolve from the directory containing
`atlas.hcl`. Apply, down, status, lint, set, and native repair commands open the
resolved directory through a rooted handle and capture an immutable snapshot
before database work. Relative CLI `--dir` paths remain rooted at the process
working directory, symlink escapes are rejected, and explicit absolute paths
remain supported. Intentional config-relative paths such as
`../shared-migrations` retain their meaning and are captured at the resolved
location.

Ptah's `ptah.yaml external_schema` block is a separate native configuration
surface. It supplies an explicit external-program argument list and SQL, HCL,
or YAML stdout to `ptah schema render`, `ptah schema compare`,
`ptah schema drift`, `ptah migrations plan`, and
`ptah migrations generate`; executing that config-sourced program requires
`--allow-external-schema`. Atlas HCL `data "external_schema"` is part of the
supported subset: a block declares `program` (argv, no shell) plus the Ptah
extensions `format`, `working_dir`, and `env`, and its `.url` value is
consumed when an env `src` selects it. Execution stays gated: native commands
require `--allow-external-schema`, and `ptah-compat` requires
`PTAH_ALLOW_EXTERNAL_SCHEMA=1` because the compat flag surface mirrors Atlas.

Ptah parses Atlas's `atlas`, `golang-migrate`, `goose`, `flyway`, `liquibase`,
and `dbmate` migration format values so the project file can be evaluated
without changing Atlas syntax. `ptah-compat migrate apply` executes all of
them. The native `atlas` format is read from disk unchanged, preserving
`atlas.sum` verification, down migrations, and Atlas `R`/`<number>R`
repeatable migration tokens.

Every other format is read and converted in memory to Atlas single-file,
up-only migrations, so applying it runs only the source tool's forward (up)
SQL and never its rollback, undo, or metadata section. Flyway repeatables in
converted directories are represented as one-time versioned migrations. Unknown
formats still fail before the target database is opened.

An explicit `?format=` query on the effective directory URL, whether it comes
from `migration.dir` or CLI `--dir`, takes precedence over the
`migration.format` default, matching Atlas. An empty query value selects the
native `atlas` format.

```bash
# Apply a Goose directory directly, no conversion step required.
ptah-compat migrate apply --env local \
  --dir "file://migrations?format=goose"
```

The reusable format-loading layer is shared with `ptah-compat migrate import`, so
apply and import agree on every format's up/down semantics. See
[`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742).

`env.exclude` accepts either one string or a list of strings. `ptah-compat schema
apply --env <name>` uses it as the default resource exclusion filter unless an
explicit `--exclude` flag is provided.

`env.schema.mode` accepts `funcs`, `objects`, `permissions`, `roles`, `tables`,
`triggers`, `types`, and `views` booleans. Ptah maps disabled values to the
matching Atlas-style resource exclusions for object kinds represented in Ptah's
schema IR. `sensitive = DENY` is accepted as a no-op because Ptah does not emit
sensitive values through the supported local workflows. `sensitive = ALLOW` is
also accepted as a no-op, matching Atlas CE's project-config decoding. Both
values produce the ignored-construct warning because Ptah has no
sensitive-value behavior to enable or disable.

`env.schema.repo` names a schema repository in a hosted registry. Its `name` is
accepted and type-checked as a string, and nothing reads it afterwards — the
same as the community binary, whose `schema inspect`, `schema apply --dry-run`
and `schema apply --auto-approve` output is byte-identical with and without the
block. Registry access itself stays refused on both: an `atlas://` URL fails,
and `ptah-compat schema plan --repo` reports that Ptah plans are local files.

`format` blocks configure the same Atlas Go-template output strings accepted by
the matching commands. Ptah supports `schema.inspect`, `schema.apply`,
`schema.diff`, `migrate.apply`, `migrate.diff`, `migrate.lint`, and
`migrate.status` for the command-specific output contracts documented in the
Atlas-compatible command reference.

`diff.skip.drop_table = true` removes table drops from supported local
declarative diff/apply plans and also removes index or constraint drops owned by
those dropped tables. `diff.skip.drop_schema` is accepted and type-checked but
changes no plan: Ptah's schema diff has no removed-schema list and no code path
renders `DROP SCHEMA`, so the suppression has nothing to omit. Because the
setting only ever removes a statement, honoring it vacuously can never make Ptah
emit a schema drop Atlas would have withheld. A non-boolean value is refused,
matching the community binary, which reports `value of attr "drop_schema" cannot
be read as bool` even on commands that never plan a diff.

`diff.concurrent_index.create = true` maps to PostgreSQL concurrent index
creation in schema diff planning. For non-dry-run PostgreSQL `schema apply`
plans that actually emit `CREATE INDEX CONCURRENTLY`, Ptah requires
`--tx-mode none` because PostgreSQL does not allow concurrent index creation
inside a transaction. `ptah-compat migrate diff` splits mixed plans and tags
concurrent-index files with `-- atlas:txmode none`, so replay executes those
files outside a transaction.

`diff.concurrent_index.drop = true` maps to `DROP INDEX CONCURRENTLY` for
standalone index removals, gated on the target's `drop_index_concurrently`
capability. An index that is dropped and recreated under the same identity is a
redefinition, not a standalone removal, and keeps the blocking drop the planner
pairs with the rebuild. The setting governs the up direction only: the rollback
of a concurrent index build is always emitted concurrently where the target
supports it, because a blocking drop there would take the very write lock the
build avoided. A non-dry-run PostgreSQL `schema apply` plan that emits
`DROP INDEX CONCURRENTLY` requires `--tx-mode none` for the same reason the
create side does.

Either setting fails `migrate diff` before it writes anything when the index it
names belongs to a PostgreSQL declaratively partitioned parent: PostgreSQL has
no concurrent form of either statement for `relkind = 'p'` and answers with
SQLSTATE `0A000` at execution time. The error names the index and the table.

`lint.latest` and `lint.git` configure the migration changeset selected by
`migrations lint` and `ptah-compat migrate lint`. These selectors are mutually
exclusive. On Atlas-format directories they select repeatable migrations by
their revision token (`R` or `<number>R`), and bare `R` sorts after numeric
migration files. `lint.git.dir` matches Atlas's working-directory option for
Git changeset detection and defaults to the current directory when omitted.

The supported lint policy analyzer blocks map the Atlas `error` boolean to
Ptah lint severity only where the analyzer has a matching Ptah rule family.
`error = true` sets the mapped findings to error severity; `error = false`
sets them to warning severity.

The supported mappings are `destructive` to the `DS` family, `data_depend` to
the `DD` family, `incompatible` to the `BC` family, `concurrent_index` to
`PG101` and `PG103`, `nestedtx` to `TX201`, and `condrop` to the `CD` family
plus `DS105`. Atlas `check` blocks are rejected for now because Atlas check IDs
and Ptah rule IDs are not a stable one-to-one namespace.

`condrop` is a separate analyzer from `destructive` and from `data_depend`, not
an alias for either. On the community binary, a migration dropping a foreign key
is reported as a warning by default; `condrop { error = true }` turns that run
into an error while `destructive { error = true }` leaves it a warning. Ptah's
`CD` family is the same constraint-deletion family, and `DS105` is included
because it is Ptah's untyped fallback for the ANSI `ALTER TABLE ... DROP
CONSTRAINT <name>` form — the exact statement the community binary attributes to
`condrop`.

Analyzer `force` options, allow-list blocks such as `allow_table` /
`allow_column`, custom `rule` blocks, and policy families without a matching
Ptah lint engine fail explicitly.

`lint.log` is an Atlas Go-template string that renders the `ptah-compat migrate
lint` output. It is parsed into the same format IR as `format.migrate.lint`, so
the two share one precedence chain: an explicit CLI `--format` overrides the
project template, and a selected `--env` `lint.log` overrides a global one. When
neither a CLI `--format` nor a project template is set, `ptah-compat migrate lint`
prints Ptah's compatibility report with per-version diagnostics, mapped rule
IDs, and a summary. Diagnostic prose is Ptah-owned. The native `ptah migrations
lint` output is unchanged.

`migration.tx_mode` accepts `file`, `all`, and `none`, matching
`ptah-compat migrate apply --tx-mode`. A leading `-- atlas:txmode file` or
`-- atlas:txmode none` header overrides global `file` or `none` for that file.
`all` is limited to dialects where Ptah can safely wrap data definition
language (DDL) in a single transaction and rejects every explicit file mode,
per-migration timeout, and pre-migration check in the selected batch. Under
global `none`, an explicit file mode restores a per-file transaction and may
use migration timeouts.

The Atlas header must be in the initial line-comment block. A blank line after
the header is accepted but not required. Unknown, duplicate, and file-level
`all` values fail before the affected migration body or revision row changes.
Validation applies only to the migrations selected after amount and baseline
processing.

When an `atlas.hcl` `migration` block is present, Ptah also defaults
`revision-format` to `atlas`, so migration commands use
`atlas_schema_revisions` unless an explicit CLI flag overrides it. `file://`
migration directories are normalized to local paths. Relative migration
directories declared in `atlas.hcl` resolve relative to the directory containing
that `atlas.hcl` file, not the process working directory. Explicit CLI `--dir`
values keep CLI semantics and resolve relative to the process working directory
unless they are absolute. Other URI schemes are rejected.

## Expression evaluation

Ptah evaluates a scoped Atlas-compatible expression subset for local project
config workflows:

- `variable` blocks with `default` values and optional `description` metadata.
- `locals` blocks referenced as `local.<name>`.
- `getenv("NAME")` for environment-provided URLs and settings.
- `file("path")` for local file contents, relative to the `atlas.hcl` file.
- `fileset("glob")` for local file lists, relative to the `atlas.hcl` file.
- `toset(value)` for deterministic environment-instance expansion.
- `atlas.env`, plus `each.key` and `each.value` inside an env expanded with
  `for_each`.
- `data "hcl_schema" "name"` blocks with either `path` or `paths`, exposed as
  `data.hcl_schema.<name>.url`.
- `format(format_string, values...)` and `jsonencode(value)` for Atlas-style
  local project-config string construction.

Example:

```hcl
variable "database_url" {
  default = getenv("DATABASE_URL")
}

data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = var.database_url
  src = data.hcl_schema.app.url
  dev = getenv("DEV_DATABASE_URL")
}
```

`data.hcl_schema.<name>.url` returns one `file://...` URL when `path` is used
and a list of `file://...` URLs when `paths` is used. `fileset` returns stable
slash-separated relative paths sorted lexicographically and supports recursive
`**` path segments.

`path` and `paths` resolve relative to the directory holding `atlas.hcl` but are
not confined to it: `path = "../shared/schema.hcl"` resolves. An absolute path
and a scheme other than `file://` are refused, and each names its own rule
rather than blaming the supported `path` key:

```text
# path = "/etc/absolute.hcl"
error: atlas.hcl "path" at atlas.hcl:2: absolute paths are not supported: /etc/absolute.hcl: give a path relative to the directory holding atlas.hcl

# path = "s3://bucket/x.hcl"
error: atlas.hcl "path" at atlas.hcl:2: unsupported URL scheme: s3://bucket/x.hcl
```

An attribute the data source does not have keeps the construct wording:
`unsupported atlas.hcl construct "vars"`.

### The file() and fileset() sandbox

Both functions are confined to the directory holding `atlas.hcl`. An absolute
path, a parent-traversal path, and a symbolic link whose target leaves the
directory are refused, and the error names the rule and points at `getenv()`
for a value that lives elsewhere. A `fileset` glob whose match leaves the
directory fails the whole call rather than dropping the entry, because the
returned paths become schema-source URLs that another reader opens. A symbolic
link that stays inside the directory is read normally.

The refusal is enforced twice on purpose. The loader resolves what it can
itself, so the message names the offending link instead of leaving an
`openat: path escapes from parent` to interpret; the directory is also opened
through a rooted handle, which is what catches a link chain the resolver stops
following and a path swapped for a link between the check and the read.
`ParseAtlasFSWithOptions` takes the filesystem from its caller, so a caller that
supplies one without that protection has chosen a weaker boundary; the loaders
in this package and in `cmd/atlas` supply a rooted one.

This is deliberately stricter than Atlas. The pinned community v1.3.0 binary
reads all three shapes and exits 0, and the contents land somewhere observable,
so matching it would give any author of a repository-controlled `atlas.hcl` an
arbitrary-file read wherever the tool runs. The divergence is measured by the
`TestOracle*` runs in `config/projectconfig` under the Atlas CE Oracle
workflow. See [`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042).

Atlas-compatible commands under `ptah-compat schema ...` and
`ptah-compat migrate ...` accept Atlas project flags:

- `--config path/to/atlas.hcl` or `-c path/to/atlas.hcl` selects the project
  config file. Ptah also accepts local `file://` config URLs. Other URL schemes
  fail explicitly.
- `--env <name>` selects a named Atlas environment.
- `--var name=value` provides a variable override. The flag can be repeated;
  repeated values for the same variable become a string list, matching Atlas's
  local project-variable behavior.

`-c` and `--env` **select** a project file, so naming either one and having no
`atlas.hcl` is an error on both binaries. `--var` only **supplies values** to a
project file, so it does not require one: with no `atlas.hcl` present the
command runs with no project at all. This holds on every verb. It previously
held only on the verbs whose flags Cobra parses, and `migrate hash`,
`migrate validate`, `migrate new`, `migrate down`, `migrate checkpoint`,
`migrate edit`, `migrate rebase`, `migrate rm`, `migrate test` and
`schema test` answered `failed to read atlas config atlas.hcl: …` at exit 1
where the pinned Atlas community binary v1.3.0 exits 0
([#1241](https://github.com/stokaro/ptah/issues/1241) item 12).

Not requiring a project file is not the same as not reading the flag. The
**syntax** of `--var` is checked wherever it is spelled, with or without an
`atlas.hcl`, because the pinned binary parses it while parsing flags and refuses
a malformed value before it looks for a project file:

```console
$ ptah-compat migrate validate --dir file://migrations --var foo
Error: invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"
$ echo $?
1
```

The value is comma-separated, and each field is checked, so `--var a=1,b` is
refused naming `b`. An empty name is accepted here — `--var =v` passes, as it
does on the pinned binary — and refused later by the project loader, where a
file is actually being evaluated.

"Wherever it is spelled" includes the commands that never look at the value.
`--var` is registered once, on the `schema` and `migrate` groups, so every
descendant accepts it — including the group commands themselves, `schema fmt`,
and `migrate import`, none of which read a project file. A check that lived only
where a consumer asked for the values left those unrefused, which mattered most
on the one that writes:

```console
$ ptah-compat migrate import --from file://src --to file://dst --dir-format flyway --var foo
Error: invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"
$ echo $?
1
$ ls dst
ls: dst: No such file or directory
```

The refusal therefore runs for every command under both groups, before the
command does any work — so `ptah-compat migrate new nm --var foo` creates no
migration directory, no migration file and no `atlas.sum`, and
`ptah-compat schema fmt --var foo` reformats nothing.

A `variable` block without a `default` is valid when the invocation provides a
matching `--var name=value`. Variable blocks accept the `type` constraints
`string`, `number`, `bool`, `list(string)`, and `map(string)`:

- `--var` overrides convert to the declared type: `--var latest=3` becomes a
  number and `--var concurrent=true` becomes a bool. Bool conversion follows
  cty's rules: `1` and `0` convert, while `True` and `yes` fail with the
  wrong-shape error. Repeated `--var name=value` flags build a `list(string)`
  value.
- `map(string)` is available to defaults and HCL expressions. The string/list
  `--var` flag syntax does not encode map values.
- A `default` that does not convert to the declared type, an override of the
  wrong shape, and any other type constraint (for example `object(...)`,
  `set(...)`, or `tuple(...)`) fail with named errors.
- `sensitive = true` is accepted. Parse-time conversion errors print
  `(sensitive value)` instead of the variable's value; a sensitive value
  interpolated into a URL or path can still appear in downstream errors that
  print that URL or path.
- `validation` blocks are not accepted until Ptah implements their semantics.

Unsupported dynamic data sources such as SQL data sources, registry-backed
sources, and Cloud-specific sources still fail explicitly.

## Env selection

Use `--env <name>` when a command needs values from one of multiple
`atlas.hcl` `env` blocks. When the file contains exactly one `env` block, Ptah
selects it automatically. Atlas-compatible `migrate apply` does not need to
select an environment when both `--url` and `--dir` are explicit. If project
values are required and multiple envs remain ambiguous, Ptah returns:

```text
atlas.hcl contains multiple env blocks; pass --env
```

An env block can expand into several instances with `for_each`. Labeled and
unlabeled blocks both support the meta-attributes:

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

For an unlabeled block selected with `--env`, `name` must depend on `atlas.env`;
a static name or a name based only on `each.key` does not define that selected
environment. Labeled blocks use their label as the initial candidate. Every
expanded instance of an admitted block is evaluated before its resulting name
is filtered, so an invalid nonmatching instance still fails the command. Tuple
and list instances keep source order; object, map, and set instances use stable
key order. As a Ptah extension, typed list and map values are accepted for env
`for_each` in addition to tuple, object, and set values.

`ptah-compat migrate apply --env local` runs every selected instance in that
order and stops at the first failure. With `--format '{{ json . }}'`, each
attempt writes one JSON document and adjacent documents are separated by one
newline. Other commands and the singular project-config Go APIs reject a
selection that produces more than one instance instead of choosing one.

## Precedence

Ptah merges configuration in this order:

1. Explicit CLI flags
2. `PTAH_*` environment variables
3. `atlas.hcl`
4. `ptah.yaml`
5. Built-in command defaults

Project-file merging preserves source presence. For a supported field, an
explicitly present value replaces the lower-precedence value instead of being
treated as absent. This includes an empty string, zero, `false`, or an empty
list when the field accepts that type. Thus `atlas.hcl` wins over `ptah.yaml`,
while `PTAH_*` environment variables and explicit CLI flags still win.

After project sources are merged, a command applies its built-in default only
when a field is absent. An explicitly present empty or zero value instead
reaches the command's normal validation. Fields that do not accept empty
values, including Atlas format templates, fail during parsing or command
validation.

When a forwarded native implementation also consumes project configuration,
the adapter passes this merged snapshot instead of reopening either project
file. This currently applies to `migrate down`; other adapters map evaluated
Atlas project values to explicit native command arguments.

This means a repo can keep an Atlas-shaped migration setup while still letting
one-off CLI invocations override any value:

```bash
ptah migrations status --env local --json
ptah migrations up --env local
ptah migrations up --env local --db-url postgres://override/db
```

## Commands

The project config is currently consumed by commands that need the mapped
settings:

- `migrations up`
- `migrations down`
- `migrations status`
- `migrations lint`
- `migrations generate`
- `ptah-compat schema inspect`
- `ptah-compat schema apply`
- `ptah-compat schema diff`
- `ptah-compat migrate apply`
- `ptah-compat migrate diff`
- `ptah-compat migrate lint`

Atlas command paths under `ptah-compat <command> ...` inherit this behavior
when they forward to one of these native commands. Dedicated Atlas-compatible
commands document their own project-config support. The separate `ptah-compat`
binary exposes the same Atlas-compatible command tree at process root for
drop-in script migration.

`ptah-compat schema inspect` reads `env.url`, `env.dev`, `env.exclude`,
`env.schema.mode`, and `format.schema.inspect`.

`ptah-compat schema apply` reads `env.url`, `env.src`, `env.schema.src`,
`env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and
supported `diff` policy.

`ptah-compat schema diff` reads `env.schema.src`, `env.dev`, `env.exclude`,
`env.schema.mode`, `format.schema.diff`, and supported `diff` policy.

`ptah-compat schema clean` reads `env.url` and `format.schema.clean`.

`ptah-compat migrate apply` reads `env.url`, `migration`, and
`format.migrate.apply`.

`ptah-compat migrate diff` reads `env.schema.src`, `env.dev`, `migration.dir`,
`format.migrate.diff`, and supported `diff` policy.

## Structural contract and ignored names

Ptah classifies every `atlas.hcl` name into one of three outcomes:

| Outcome | Behavior |
| --- | --- |
| Supported | Ptah parses the value into project config and evaluates its expression when the containing environment is selected. |
| Structurally unsupported | Ptah rejects the file with a location-aware error, even when the construct is in an unselected environment. |
| Ignored by Atlas CE | Ptah accepts the name for compatibility, records it in `Config.IgnoredConstructs`, and the CLI warns that it has no effect. |

Structurally unsupported attributes, data sources, lint policy shapes, format
fields, diff policy fields, labels, and duplicate supported blocks fail with a
location-aware error:

```text
unsupported atlas.hcl construct "src" at atlas.hcl:2
```

Ptah does not turn a construct that Atlas CE decodes or enforces into a no-op.
The ignored category is limited to names that Atlas CE itself accepts without
acting on. The CLI writes one warning per ignored source location:

```text
warning: atlas.hcl attribute "project" at atlas.hcl:2 is ignored for Atlas compatibility and has no effect
```

This distinction preserves Atlas CE's unknown-name behavior without hiding a
likely typo or a policy that does nothing. The warning goes to stderr; stdout
and the success exit code remain unchanged.

Structural validation covers every `env` block, including environments that
are not selected for the current command. An unsupported attribute, nested
block, label, or duplicate therefore fails even when it appears in another
environment. Expressions inside `env` blocks, including bodies and values of
ignored names, are evaluated only in the selected environment. An unselected
environment may therefore refer to variables, files, or environment values
unavailable in the current invocation. Global `variable`, `locals`, and `data`
blocks are evaluated separately to build the shared context before environment
selection.

Non-local URI schemes in `migration.dir` and `schema.src` fail explicitly when
a command needs that configured value. An explicit CLI path flag still wins over
the matching `atlas.hcl` value before URI validation.
