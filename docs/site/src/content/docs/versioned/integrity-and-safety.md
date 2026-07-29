---
title: Integrity and safety
description: Hash and validate the migration directory, replay it on a dev database, lint for unsafe SQL, and gate destructive changes.
---

You are about to let migrations touch a database other people depend on. This
page shows the gates Ptah puts between a migration directory and that
database: the integrity file, replay validation, the lint rules, the
destructive-change gate, and pre-migration assertion checks — and what each
one's failure looks like.

Prerequisites: a migration directory (see
[Generate migrations](../generate/)). The examples use fixed-version manual
migrations and a local SQLite file.

## The integrity file

`ptah migrations hash` writes `ptah.sum`: a hash of every migration file,
committed alongside them.

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

Expected output includes:

```text
Wrote ./migrations/ptah.sum
4 migration file(s) hashed
OK: migrations directory matches ptah.sum
```

The file lists a directory-level hash and one line per migration file:

```text
h1:tIMjgi/Ua3SltHhtGO6BksGhtMonFkeLmHl0+fM8p/o=
0000000001_init.down.sql h1:iwBWserNbFegc3M+AZUbJlv00afJyT9smM0ezgVpk4o=
0000000001_init.up.sql h1:5Lxcm8LHBcS4SgvVBg/frPdzEflRbvdjWh5UOSDATiM=
```

Atlas-format directories use `atlas.sum` the same way.

## Detect out-of-band edits

Any change to a hashed file — a hotfix applied in place, a merge gone wrong —
makes validation fail and name the file (exit `1`):

```bash
ptah migrations validate --dir ./migrations
```

```text
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

The same drift blocks an apply when `--verify-sum` is set (exit `2`):

```text
error: migration sum verification failed:
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

Recovery is a decision, not a command: if the change is intentional, review
it and re-run `ptah migrations hash`; if it is not, restore the file from
version control. Use `git diff` on the migration directory to tell the two
apart. Run `validate` in CI and `up --verify-sum` everywhere so drift is
caught at review time, not at deploy time.

## Replay on a dev database

Hashes prove the files are unchanged, not that the SQL executes. Add
`--dev-url` to also clean a disposable
**[dev database](../../concepts/database-urls-and-dev-databases/)** and replay
the whole directory on it:

```bash
ptah migrations validate \
  --dir ./migrations \
  --dev-url "sqlite://replay.db"
```

Expected output includes:

```text
OK: migrations directory matches ptah.sum
OK: migration SQL validated on dev database
```

A migration that no longer executes — here one that alters a table dropped by
an earlier edit — fails the replay (exit `2`):

```text
error: error validating migration SQL on dev database: replay migration 3 on dev database: failed to execute migration SQL: sqlite: SQL execution failed: SQL logic error: no such table: missing (1)
SQL: ALTER TABLE missing ADD COLUMN nickname TEXT
```

The dev database is dropped clean on every run — point it at a scratch
database of the target engine, never at a real environment.

## Lint for production-unsafe SQL

`ptah migrations lint` analyzes migration files for patterns that are legal
SQL but dangerous in production — dropped tables and columns, lock-heavy
DDL, and dialect-specific hazards:

```bash
ptah migrations lint --dir ./migrations --dialect sqlite
```

A finding names the file, line, rule, and remediation (exit `1`):

```text
migrations/0000000003_drop_users.up.sql:1 [error] DS101: DROP TABLE permanently deletes the table and every row in it; take a verified backup first and consider a rename-and-retire window instead (table dropped)

1 finding(s).
```

A clean run prints `No lint findings.` and exits `0`.

Useful controls, all designed for CI:

- `--latest N` lints only the newest N versions — the changeset of a pull
  request rather than all of history. `--git-base <branch>` selects the
  changeset from Git instead.
- `--fail-on error` (default) fails only on error-severity findings;
  `--fail-on any` fails on warnings too; `--fail-on none` always exits `0`.
- `--format json`, `--format sarif`, and `--format github-actions` feed code
  scanners and PR annotations. The SARIF output is a SARIF 2.1.0 document
  that GitHub code scanning ingests — [CI](../../testing/ci/) shows the
  upload step.
- `--dialect` gates dialect-specific rules; `--dev-url` infers the dialect
  and additionally replays the directory on the dev database.
- `--disable DS101` (or a family such as `MY`) skips rules ad hoc; a
  committed `.ptah-lint.yaml` does it persistently and adds per-rule
  severity and path scoping — see below.

For OCI-distributed directories, `lint --dir oci://...` lints the published
artifact, and `--attach` stores the canonical report next to it — see
[OCI registry artifacts](../../operate/oci-registry/).

### Configure rules with `.ptah-lint.yaml`

Commit a `.ptah-lint.yaml` next to the migration files to make lint policy
part of the reviewed migration directory. Without an explicit `--config`
path, Ptah loads `<dir>/.ptah-lint.yaml` when present:

```yaml
dialect: postgres
disabled-rules:
  - MF103
  - MY
rules:
  DS103:
    severity: warning
  DS102:
    severity: error
    exclude:
      - legacy/**
```

- `dialect` sets the default lint dialect; `--dialect` overrides it.
- `disabled-rules` lists rule codes (`DS101`) or family prefixes (`MY`) to
  skip entirely; entries merge with `--disable` flags.
- `rules` keys name an exact code or a family prefix; the most specific key
  wins, so a `DS102` entry beats a `DS` entry.
- `severity` accepts `warning` or `error` and replaces the rule's default
  severity on its findings — the level that `--fail-on` and the apply-time
  destructive gate below evaluate. Any other value fails config parsing
  (exit `2`).
- `exclude` lists slash-separated path globs (`**` crosses directory
  levels) where the rule is skipped, matched against each migration file's
  path — "DS102 is acceptable under `legacy/`" without disabling the rule
  everywhere.

Precedence: a rule listed in `disabled-rules` never runs, regardless of its
`rules` entry; then `exclude` skips the matching files; then `severity`
relabels the findings that remain. Per-rule entries apply to custom
analyzer codes the same way as to built-in ones — see
[Reusable components](../../extend/components/) for registering custom
rules from Go.

### Suppress a single statement inline

When one reviewed statement is acceptable but the rule should stay active
everywhere else, put a `ptah:nolint` comment directly above it:

```sql
-- ptah:nolint DS102
ALTER TABLE users DROP COLUMN archived_note;
```

The directive suppresses only the named rules, and only for the next
statement. List several codes separated by spaces or commas, name a family
(`DS`), or write a bare `-- ptah:nolint` to silence every rule for that one
statement. `-- atlas:nolint DS102` is accepted as an alias with the same
statement-scoped behavior, so a directory shared with Atlas tooling keeps
one set of directives. Atlas analyzer-name selectors (such as
`destructive`) and whole-file `atlas:nolint` headers take effect only on
the Atlas-compatible surface — see
[Atlas migrate commands](../../atlas/migrate-commands/).

## The destructive-change gate

Destructive statements require explicit policy at two points:

- **At generation**, `plan`/`generate --check-destructive` refuse to write
  destructive SQL without `--allow-destructive` — see
  [Generate migrations](../generate/).
- **At apply**, `ptah migrations up` refuses pending migrations that contain
  destructive statements (exit `2`):

```text
error: error running migrations: pending migrations contain destructive statements; rerun with --allow-destructive after review:
- 0000000003_drop_users.up.sql:1 DS101 error: DROP TABLE permanently deletes the table and every row in it; take a verified backup first and consider a rename-and-retire window instead
```

Use `--allow-destructive` only after the plan has been reviewed and the
rollback path is understood.

The apply-time gate loads the same `.ptah-lint.yaml` as
`ptah migrations lint` and blocks on error-severity `DS` data-safety
findings. That makes the escape hatch proportional: a rule downgraded with
`severity: warning`, listed under `disabled-rules`, or excluded for a path
stops blocking exactly that reviewed pattern — a widening
`ALTER COLUMN ... TYPE` under `rules: {DS103: {severity: warning}}` applies
without `--allow-destructive` — while a `DROP TABLE` in another pending
file of the same batch still aborts the apply. `--allow-destructive`
remains the all-or-nothing per-run override rather than the only way past
a single warning-grade finding.

Know the limits of this gate: the policy file is not tamper-evident.
`ptah.sum` hashes only the migration `*.sql` files, so
`ptah migrations validate` and `up --verify-sum` still pass when
`.ptah-lint.yaml` is added or edited out-of-band, and the apply prints no
notice when the config suppresses a destructive finding — a one-line
`disabled-rules: [DS]` dropped next to the migrations at deploy time
disables this gate silently. Loosening the gate is a visible committed
change only if your process makes it one: commit `.ptah-lint.yaml`, treat
any edit to it as an edit to the gate in review, and restrict writes to
the deployed migration directory, because the integrity file does not
protect the policy the way it protects the SQL.

`ptah migrations up` captures the migration directory before checksum
verification, provider registration, and destructive linting. The safety gate
therefore checks the same immutable SQL bytes that the migrator will execute,
even if files on disk change while the command is running.

## Pre-migration checks

Guard a migration on a data-state precondition with a `-- +ptah check`
directive, which runs before the migration's statements and aborts if the
assertion fails:

```sql
-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
DROP TABLE users;
```

Each check is a separate read against committed state that runs before the
migration's statements, so a failing assertion leaves nothing applied and
exits non-zero. Checks are rejected under `--tx-mode all` (a pooled read
cannot see the batch's uncommitted state), and
`ptah migrations up --skip-checks` is an emergency bypass. This is the open,
local half of Atlas Pro's pre-migration checks; the Cloud approval-policy
half is intentionally out of scope.

## Exit codes are the contract

Every gate above communicates through the CLI exit code — `0` clean, `1` for
an expected negative result (drift found, findings found), `2` for errors
and refused operations — so CI wiring is a matter of checking `$?`. The full
per-command table is in [Exit codes](../../reference/exit-codes/).

## Next steps

- Wiring these gates into pull requests? [CI](../../testing/ci/).
- A gate fired and the migration needs rework?
  [Maintain migration history](../maintain-history/).
- Asserting behavior rather than safety?
  [Test migrations and schemas](../../testing/migrations-and-schema/).
