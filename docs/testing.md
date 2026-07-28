# Declarative Database Testing

Ptah provides two native commands for repeatable migration and desired-schema
tests:

- `ptah migrations test` runs migration steps and assertions.
- `ptah schema test` applies a desired schema before running assertions.

Both commands use the exported `migration/dbtest` engine, require no account or
cloud service, and support text, JSON, and HTML reports. Atlas keeps the
corresponding testing framework outside its open-source core. On the
Atlas-compatible surface, `ptah atlas migrate test` and `ptah atlas schema
test` (and the same verbs under the `ptah-compat` binary) forward to these
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
| `--root-dir` | `./models` | Go annotation root applied before test steps. |
| `--seed-dir` | Empty | Default directory for seed steps that omit `dir`. |
| `--db-url` | Empty | Explicit throwaway database URL. |
| `--run` | Empty | Run only case names matching a Go regular expression. |
| `--report` | `text` | Report format: `text`, `json`, or `html`. |

A `migrate_to` step fails in a schema test because that surface has no migration
history.

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

Import `github.com/stokaro/ptah/migration/dbtest` and construct `Case`, `Step`,
and `Assertion` values directly, or load YAML with `ParseCases` / `LoadCases`.
Use `FilterCases` for the same regular-expression selection as `--run`. Call
`RunMigrationTest` or `RunSchemaTest`; `Options.SeedDir` and
`SchemaOptions.SeedDir` provide the same run-level seed default as `--seed-dir`.
Render the returned `Report` and check `Report.Failed`.

See [Public Go API](public_api.md) for the compatibility contract and the
[testing workflow](site/src/content/docs/workflows/testing.md) for CI-oriented
guidance.
