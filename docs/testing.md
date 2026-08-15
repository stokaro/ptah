# Declarative Database Testing

Ptah provides two native commands for repeatable migration and desired-schema
tests:

- `ptah migrations test` runs migration steps and assertions.
- `ptah schema test` applies a desired schema before running assertions.

Both commands use the exported `migration/dbtest` engine, require no account or
cloud service, and support text, JSON, and HTML reports. Atlas keeps the
corresponding testing framework outside its open-source core. On the
Atlas-compatible surface, the `ptah-compat` binary's `migrate test` and
`schema test` verbs forward to these
native runners: `--dir` / `-u --url` select the migration directory or desired
schema, `--dev-url` the throwaway database, and `--run` the case filter, with
Ptah-native YAML test files as the executable payload.

## Test Case Format

Each `*.yaml` or `*.yml` file under `--dir` contains a top-level `cases` list.
Files are loaded in lexical order. A case has a name and an ordered list of
steps. Every step must set exactly one action:

| Action | Behavior |
| --- | --- |
| `migrate_to` | Migrate to a non-negative integer version, `latest`, or `0`. Migration tests only. |
| `apply_schema` | Apply the Go-annotation desired schema from `--root-dir`. Write `apply_schema: true`. |
| `seed` | Apply matching seed files through `migration/seeder`. Requires `env`; `dir` overrides the run-level seed directory. |
| `exec` | Execute raw SQL. |
| `assert` | Execute a query and check one assertion. |

Schema tests converge the desired schema before each ephemeral case, or once for
a shared explicit database. An `apply_schema: true` step repeats the live
introspection, diff, and apply operation. It reports `desired schema already
applied` when nothing changed and repairs supported drift otherwise.

`apply_schema` owns only objects declared by the desired schema. It does not
drop unrelated objects created by migration steps. Planning uses the live
server's dialect capabilities and identifier semantics. Roles and grants are
rejected because they can mutate cluster-scoped security state outside the
throwaway database lifecycle.

```yaml
cases:
  - name: users migration
    steps:
      - name: migrate history
        migrate_to: latest
      - name: apply desired audit schema
        apply_schema: true
      - name: load test fixtures
        seed:
          dir: ./seeds
          env: test
      - name: insert user
        exec: INSERT INTO users (name) VALUES ('ada')
      - name: one user exists
        assert:
          query: SELECT id FROM users
          row_count: 1
```

Unknown YAML fields are rejected. Multi-document YAML files are supported, and
all documents contribute cases.

Case names must be unique across everything one run loads: a name repeated in
two files, in two documents of one file, or twice in one `cases` list fails the
load. A collision between two files names both of them; a collision inside one
file names that one.

Comparison removes surrounding whitespace but does not fold case, because that
is the line `--run` already draws. `--run` is an unanchored regular expression,
so `--run dup` selects both `dup` and `dup ` — write it expecting one case and
you silently run two. It selects only the first of `dup` and `DUP`, so those
stay two distinct cases.

Both commands also read Atlas-format `*.test.hcl` files from `--dir` alongside
the YAML above. Each `test` block there is labeled with the kind it belongs to,
and a run loads only blocks of its own kind, so uniqueness is checked over what
that run actually loads rather than over everything on disk. A directory pairing
`dup` in `a.yaml` with a `test "migrate" "dup"` block therefore loads clean
under `ptah schema test`, which never sees the migrate case, and is rejected by
`ptah migrations test`, which loads both.

## Assertions

An `assert` step requires `query` and exactly one condition:

| Condition | Behavior |
| --- | --- |
| `row_count` | Count returned rows and compare with the non-negative integer value. |
| `scalar` | Compare the first column of the first row as text. |
| `error_contains` | Require the query to fail with an error containing the configured text. |

Scalar values are normalized deterministically. Text and byte values compare as
strings, timestamps use RFC 3339, SQL `NULL` compares as `<nil>`, and other
driver values use their standard string representation.

The runner stops a case after its first failed step because later steps usually
depend on state the failed step should have created. Other cases continue.

## Migration Tests

```bash
ptah migrations test \
  --dir ./tests \
  --migrations-dir ./migrations \
  --root-dir ./models \
  --seed-dir ./seeds \
  --dir-format ptah \
  --run '^users' \
  --report text
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `--dir` | `./tests` | Directory containing test-case YAML files. |
| `--migrations-dir` | `./migrations` | Migration directory used by `migrate_to`. |
| `--root-dir` | `./models` | Go annotation root used by `apply_schema`. |
| `--seed-dir` | Empty | Default directory for seed steps that omit `dir`. |
| `--dir-format` | `ptah` | Migration format: `auto`, `ptah`, or `atlas`. |
| `--db-url` | Empty | Explicit throwaway database URL. |
| `--run` | Empty | Run only case names matching a Go regular expression. |
| `--report` | `text` | Report format: `text`, `json`, or `html`. |

`--migrations-dir` is required only when a case uses `migrate_to`.
`--root-dir` is required only when a case uses `apply_schema`. A seed step must
set `dir` unless `--seed-dir` supplies the run-level default.

The migration test command captures `--migrations-dir` once, verifies its
integrity file when present, and supplies that immutable filesystem to every
`migrate_to` step. A pathname change during the test cannot switch later steps
onto bytes that were not checked at command entry.

A `--migrations-dir` that does not exist is an error rather than an empty
history, so a `migrate_to` step cannot report success having executed nothing.
The directory is required only when a selected case carries a `migrate_to` step;
a suite of `apply_schema`, `exec`, `seed` and `assert` cases never reads it.

## Schema Tests

```bash
ptah schema test \
  --dir ./tests \
  --root-dir ./models \
  --seed-dir ./seeds \
  --run '^users' \
  --report json
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `--dir` | `./tests` | Directory containing test-case YAML files. |
| `--root-dir` | `./models` | Go annotations, a SQL or HCL schema file, or a live database applied before test steps. |
| `--seed-dir` | Empty | Default directory for seed steps that omit `dir`. |
| `--db-url` | Empty | Explicit throwaway database URL. |
| `--var` | Empty | Repeatable `name=value` override for an HCL desired-schema file. |
| `--run` | Empty | Run only case names matching a Go regular expression. |
| `--report` | `text` | Report format: `text`, `json`, or `html`. |

A `migrate_to` step fails in a schema test because that surface has no migration
history.

The Atlas-compatible `schema test` adapter forwards an explicit source's
`--var` values. A project source selected through `data.hcl_schema` uses that
block's `vars` instead, including an explicitly empty scope.

## Database Isolation

When `--db-url` is omitted, each case receives its own ephemeral SQLite
database. Ptah provisions and cleans it through the same disposable-database
lifecycle used by migration generator shadow verification. State cannot leak
between cases.

When `--db-url` is set, all cases share that explicit database. Ptah does not
delete the caller-owned database after the run, so the caller must create,
isolate, and clean it. Reusing the same URL is supported for idempotent cases,
but Ptah does not serialize concurrent test processes or roll back arbitrary
SQL between cases. Never point a test command at production or another database
whose schema or data must be preserved. Migration, schema, seed, and raw SQL
steps all mutate the target; seed steps deliberately bypass protected
environment checks because the target is required to be disposable.

## Reports And Exit Codes

Text reports are intended for terminals. JSON reports include the test kind,
summary counts, cases, steps, pass status, and failure details. HTML reports are
self-contained.

The native CLI exits:

- `0` when every case passes;
- `1` when the runner completes and any case fails;
- `2` for invalid flags, invalid test cases, unreadable inputs, connection
  failures, interrupted runs, schema setup failures, or report errors.

## Embedding

Import `go.5x5.cz/ptah/migration/dbtest` and construct `Case`, `Step`,
and `Assertion` values directly, or load YAML with `ParseCases` / `LoadCases`.
Use `FilterCases` for the same regular-expression selection as `--run`. Call
`RunMigrationTest` or `RunSchemaTest`; `Options.SeedDir` and
`SchemaOptions.SeedDir` provide the same run-level seed default as `--seed-dir`.
`dbtest.Options.MigrationsFS` lets an embedder pass an immutable, already
authorized history for every `migrate_to` step; when it is nil, the engine opens
`Options.MigrationsDir` for compatibility with existing callers.
Render the returned `Report` and check `Report.Failed`.

See [Public Go API](public_api.md) for the compatibility contract and the
[testing workflow](site/src/content/docs/testing/migrations-and-schema.md) for CI-oriented
guidance.
