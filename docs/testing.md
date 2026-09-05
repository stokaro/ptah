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
schema, `--dev-url` the throwaway database, and `--run` the case filter. The
executable payload is either a Ptah-native YAML file or an Atlas `.test.hcl`
file; the two live side by side in one directory and the section below says
what each `.test.hcl` may contain.

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

## Atlas `.test.hcl` Files

Both commands also read Atlas-format `*.test.hcl` files from `--dir` alongside
the YAML above. A file is a sequence of `test` blocks, each labeled with its
kind and its name, whose body is an ordered list of step blocks:

```hcl
test "schema" "users_insert_select" {
  exec {
    sql = "INSERT INTO users (id, name) VALUES (1, 'ada')"
  }
  exec {
    sql    = "SELECT name FROM users WHERE id = 1"
    output = "ada"
  }
}
```

Three kinds exist, and each is loaded by one command:

| Kind | Loaded by |
| --- | --- |
| `test "migrate" "..."` | `ptah migrations test` |
| `test "schema" "..."` | `ptah schema test` |
| `test "plan" "..."` | `ptah-compat schema plan test` |

| Step | Attributes | Behavior |
| --- | --- | --- |
| `exec` | `sql`, optional `output` or `match`, optional `format` | Runs the statement, and compares its result when either assertion attribute is present. See below. |
| `catch` | `sql`, optional `error` | Expects the statement to fail. Without `error` the expectation is only that something failed; with it, the message must match that unanchored regular expression. A statement that succeeds fails the case either way. |
| `assert` | `sql`, optional `error_message` | Expects the statement to answer a single true value. A false answer is a failing test rather than an invalid case, and `error_message` is appended to the failure so the report says what the question meant. |
| `log` | `message` | Records the message where it stands among the steps. It reaches no database and cannot fail, so it never decides whether a case passed. |
| `cleanup` | `sql` | Runs after the case body rather than in it. See below. |
| `external` | `program`, optional `working_dir`, optional `output` or `match` | Runs a program. Refused unless the run authorizes it. See below. |
| `migrate` | `to` | Migrates to that version, as `migrate_to` does in YAML. |
| `schema` | `url` | Establishes the starting state. Plan cases only. |
| `apply` | `url` | Applies the saved plan file that URL names. Plan cases only. |

Everything above is available in a `test "plan"` case too, and `for_each` earns
its place there: an instance can select the plan file it applies, so one case
covers several plans.

```hcl
test "plan" "each_plan" {
  for_each = { email = "add_email", nickname = "add_nickname" }
  parallel = true

  schema { url = "file://snapshots/v1.sql" }
  catch  { sql = "SELECT ${each.key} FROM users" }
  apply  { url = "file://plans/${each.value}.plan.hcl" }
  exec   { sql = "SELECT ${each.key} FROM users" }
}
```

The `catch` before the `apply` is what makes the case measure anything: it
establishes that the column is absent, so the `exec` after the plan can only
pass because the plan added it.

### What a case takes

A `test` block carries two attributes of its own, beside the step blocks in its
body:

| Attribute | Behavior |
| --- | --- |
| `for_each` | Expands the case into one instance per element, named `<case>/1`, `<case>/2` and so on. |
| `skip` | An expression. When it is true the instance is reported as skipped and none of its steps run. |
| `parallel` | An expression. When it is true the case may run at the same time as other parallel cases. |

A file may also declare top-level `variable` blocks. Each needs a `default`, and
a variable without one is refused rather than resolved to nothing.

```hcl
variable "prefix" {
  default = "acct"
}

test "schema" "accounts" {
  for_each = { small = 1, large = 1000 }
  skip     = each.key == "large"

  exec {
    sql = "INSERT INTO accounts (name, size) VALUES ('${var.prefix}-${each.key}', ${each.value})"
  }
}
```

Four values resolve inside a case, and `file()` reads beside the test:

| Reference | Resolves to |
| --- | --- |
| `each.key` | The element's position over a collection, and the key itself over a mapping. |
| `each.value` | The element. |
| `self.name` | The instance's own name, `accounts/1` rather than `accounts`. |
| `self.dev_url` | The disposable database's address. |
| `file("payload.sql")` | The file's contents, read from the directory holding the test. |

Two of those repay a second reading. `each.key` differs by what is iterated, so
a case written against a mapping and later pointed at a list changes what its
key means. And a mapping iterates in **sorted key order** rather than the order
its keys were written, which is what makes a report reproducible.

`file()` reads only inside the directory that holds the test file, which is the
directory `--dir` names rather than wherever the command was run from. An
absolute path, a parent traversal, and a symbolic link pointing outward are each
refused, because a test file is repository-controlled and evaluated before
anything runs.

### When a case may run in parallel

`parallel` is honored only where each case gets a database of its own, which is
what `ptah schema test` and `ptah migrations test` do when no database URL is
given: a case is provisioned a throwaway database, runs against it, and it is
removed afterwards.

Naming a database URL does not take that away. A file that asks for `parallel`
opts into per-case isolation, so each case gets a database of its own **on the
server that URL names**, created for it and dropped afterwards -- which is what
lets `ptah-compat schema test` and `ptah-compat migrate test` run a parallel
file at all, since an Atlas-shaped invocation always supplies `--dev-url`.

A file that never says `parallel` is unaffected: an explicit URL keeps its
documented behavior of one shared database for every case.

Where a dialect has no way to give a case its own database, a parallel case is
**refused** before anything runs rather than quietly sharing one.

Two properties hold whatever the schedule was. The report lists cases in
document order, so two runs of one file are comparable; and every case
contributes a result: a suite that reported success while some of its cases
never ran would be green, shorter by however many went missing, and silent about
which ones.

`parallel` is per case, so a file may mix parallel and ordinary cases, and a
skipped case stays skipped either way.

### When a `cleanup` runs

A `cleanup` block leaves the case body and joins the case's teardown, wherever
it is written among the steps. Four things decide its behavior, and each is a
promise rather than an implementation detail:

- **It runs in reverse written order.** A cleanup written beside the setup it
  undoes is therefore correct without the author arranging anything: the last
  thing created is the first thing removed.
- **It runs whatever the body did** — after it passes, after a step fails, after
  a `catch` handles an expected failure, and after the run is canceled. A
  teardown that only ran on success would release nothing on exactly the runs
  that left something behind.
- **It does not stop at its own first failure.** A cleanup exists to release
  something, and stopping would leave everything after the failure held.
- **It runs against the case's own database**, the one the body used.

A cleanup failure fails the case without displacing the body's failure. Both
appear as steps, in the order they happened, and each report marks which is
which: the text report writes `cleanup step`, the JSON document carries
`"kind": "cleanup"` on the step and `cleanup_failed` on the case, and the HTML
page labels the step. A reader sees the check that failed and the teardown that
failed as the two separate problems they are: a
failed check is a defect in what the case asserts, and a failed teardown is a
database left dirty. The JSON document carries `cleanup_failed` on the case for
a consumer that needs to tell them apart without reading the steps.

A skipped case runs no cleanup, because nothing was set up.

### When an `external` step is allowed to run

An `external` step runs a program on the machine executing the suite, which is a
larger authority than the rest of a test file has. Ptah therefore refuses one by
default and the refusal is a **load** failure: it happens before any database is
provisioned, so a directory whose last case runs a program does not first create
a database and apply a schema. Set `PTAH_ALLOW_EXTERNAL_TEST_COMMAND` to
authorize it, and the refusal names that variable so an operator reading a
report knows what the decision is.

`program` is a list, and that is the security property rather than a spelling
convention. Its elements become an argument vector, so the program is executed
directly and nothing a test file writes is interpreted as shell syntax; a value
holding spaces, quotes or semicolons is one argument. A `program` written as a
string is refused rather than split, because splitting it is how an argument
vector silently becomes a command line.

The step passes when the program exits zero and whichever expectation it carries
holds: `output` compares the combined output after surrounding whitespace is
removed, and `match` tests it against an unanchored regular expression. Writing
both is refused. A step with neither only asserts a successful exit, which is
what a fixture whose job is a side effect wants.

There is no `timeout` attribute. The bound belongs to the runner rather than the
document, so a test file can neither raise nor remove it.

### What an `exec` compares

`output` compares the **whole** result rather than its first value, so a query
answering more rows or columns than the author expected fails instead of passing
on the first one it finds. `format` selects the rendering: `csv`, the default,
puts one row per line with values separated by commas; `table` draws a header, a
rule beneath it, and one padded cell per value.

`csv` and `table` are the whole set, and a `format` naming anything else is
refused with its file and line rather than rendered as the default. The two
renderings genuinely differ, so an author who wrote `tabel` and was given CSV
would have their assertion checked against a layout they did not ask for and be
told it held.

`match` tests the first value against an unanchored regular expression. Writing
`output` and `match` together is refused rather than resolved, so a typo in one
cannot leave the other unchecked, and `format` without `output` is refused for
the same reason: it would be an instruction nothing reads. An empty
`format = ""` is refused too — naming no `format` selects the default, but
writing the attribute says the author expected it to decide something.

### What a report marks

A report tells three passing outcomes apart, because `passed` alone describes
two of them wrongly. An ordinary step carries no marker; a `log` is marked
`LOG`, since it reached no database and checked nothing; and a `catch` whose
statement failed as expected is marked `CAUGHT`, since it passed *because*
something went wrong. A cleanup step keeps the ordinary `PASS` or `FAIL` word -- whether it worked is
the same question as for any other step -- and says which half of the case it
belongs to beside its name, as `cleanup step`.

The marker appears in the text report's status column, as
`kind` in the JSON document -- absent for an ordinary step, so a report of
nothing else is byte-identical to one from before the field existed -- and as
the label and CSS class in the HTML page.

A **case** has a fourth state beside passed and failed. A skipped case is marked
`SKIP`, carries no steps because none ran, counts in its own column of the
summary line, and is neither a pass nor a failure: folding it into the passes
would tell a reader that a check they rely on ran, and dropping it from the
report would make a skipped case indistinguishable from one nobody wrote. The
summary line therefore always states four numbers:

```text
4 cases, 2 passed, 1 failed, 1 skipped
```

An unknown step, an unknown attribute, a `test` block without exactly two
labels, and a `schema` or `apply` step outside a plan case are each refused by
name rather than ignored. Step order is significant and is preserved, so an
`exec` that writes a row runs before the `exec` that reads it back.

A run loads only blocks of its own kind, so uniqueness is checked over what
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
block's `vars` instead, including an explicitly empty scope. `PTAH_VAR` is
inside that scope rather than around it: the adapter reads the variable itself
and forwards only what the scope decided, so an empty scope refuses an
environment value exactly as it refuses a flag.

## Database Isolation

When `--db-url` is omitted, each case receives its own ephemeral SQLite
database. Ptah provisions and cleans it through the same disposable-database
lifecycle used by shadow verification. State cannot leak between cases.

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
