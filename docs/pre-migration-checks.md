# Pre-migration assertion checks

Pre-migration checks are SQL predicates that run **before** a migration's
statements and abort the migration if the precondition is not met. The
motivating case is guarding a destructive migration on a data-state
precondition — for example, refusing to `DROP TABLE users` unless the table is
already empty.

This is a **local, offline** capability: no network, no external service, no
account. It is the open, MIT, embeddable half of Atlas's Pro "pre-migration
checks" feature. The reviewer-approval-policy half (gated on Atlas Cloud) is
intentionally **not** implemented.

## Directive

Declare a check in the migration file with a `-- +ptah check` directive line,
part of Ptah's existing `-- +ptah` directive family:

```sql
-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
DROP TABLE users;
```

Keys:

| Key | Required | Meaning |
| --- | --- | --- |
| `assert` | yes | One top-level `SELECT` that returns exactly one column and one row containing a truthy scalar. |
| `name` | no | A label for the check, shown in error output. |
| `on_fail` | no | What to do when the assertion is not satisfied. Only `abort` is supported; it is the default. |

- `assert` is a single top-level `SELECT` returning exactly one column and one
  row. A boolean result uses its value; a number passes when non-zero; a
  text/`bytea` result accepts the common
  truthy spellings (`t`/`true`/`1`/`y`/`yes`, case-insensitive) and otherwise
  parses as a number. A `NULL` or unrecognized result **fails** the check — a
  precondition that cannot be shown to hold blocks the migration.
- The `assert` value is double-quoted so it can contain spaces and `=`. A literal
  double quote inside the value is escaped by doubling it (`""`).
- Multiple `-- +ptah check` lines per migration are allowed and run in file
  order, before the first migration statement.
- A malformed check directive (missing `assert`, unknown key, unsupported
  `on_fail`, unterminated quote, multi-statement `assert`) aborts the migration
  with nothing applied.

## Execution semantics

A check's `assert` runs as a **separate read against the database's committed
state, before the migration's statements** — not inside the migration
transaction. (The migration transaction has no query path, and reading on the
pool while the transaction held its connection would starve a single-connection
pool.) Because the check runs before any body statement, committed state is
exactly the pre-migration state the migration is about to change.

- **Default (per-file transaction) and `no_transaction` migrations**: the check
  runs, then the migration body runs. A failing or erroring assertion aborts
  before any statement or transaction, so nothing is applied.
- **`--tx-mode all`**: pre-migration checks are **not supported** and are
  rejected before anything is applied. Under one shared transaction a check
  reading committed state cannot see earlier batched migrations' uncommitted
  changes, so it would silently evaluate a precondition against stale state. Run
  such migrations with the default per-file mode.

Checks are evaluated for the **up** direction only (they guard forward,
typically destructive, migrations); a `-- +ptah check` in a down migration is
ignored. A failing assertion produces a `CheckFailedError` that names the
migration version and assertion. An Atlas `oneof` file in which every assertion
is falsy produces a `CheckGroupFailedError` that names the file and assertion
count. In both cases, `ptah migrations up` exits non-zero.

A failed check writes **no revision row**. Checks run before the migration's
bookkeeping row is created, so a blocked migration is recorded as never
started rather than as dirty: the revision table is left byte-identical, and
`migrations status` keeps reporting the previous version with the blocked
migration merely pending. Once the data the check guarded is corrected, the
next run applies it with no bypass flag and no `migrations repair`. It matches
Atlas, which also records nothing when its own checks fail.

This matters most on `ptah-compat migrate apply`, which registers no
`--skip-checks` flag (neither does Atlas). It does register `--allow-dirty`, but
that flag cannot currently clear a dirty row: the retry fails on the revision
re-insert with a `UNIQUE constraint failed` error
([`stokaro/ptah#966`](https://github.com/stokaro/ptah/issues/966)). A recorded
check failure would therefore have left no working in-band recovery on that
surface. The `PTAH_SKIP_CHECKS` bypass is an emergency override, not that
recovery path: correcting the guarded data is, and it needs no bypass at all.

## Assertion result shape

The `assert` predicate is contracted to return one row with one column.

- **More than one column** fails the check closed: the result cannot be
  interpreted, and a check that cannot be evaluated blocks the migration
  rather than passing it.
- **More than one row** fails the check closed. Use an aggregate
  (`SELECT count(*) = 0 FROM ...`) or an `EXISTS` form when the underlying query
  can match multiple rows.

Checks use a dedicated physical session that Ptah discards afterward (in-memory
SQLite retains its sole connection after rollback because that connection owns
the database lifetime). On transaction-capable drivers they execute in a
transaction that is always rolled back; ClickHouse executes directly on the
disposable session because its driver does not implement transactions. Each
assertion must be a top-level `SELECT` returning exactly one column and one row.
PostgreSQL-family and MySQL-family drivers additionally enforce database-level
read-only mode. SQL Server assertions cannot use `NEXT VALUE FOR`, whose
sequence advance survives rollback.

The check still precedes the migration body and is not atomic with it: a
concurrent session can commit between the check and the body. Keep checks as
guards against pre-existing state, not as serialization primitives. Keep each
`assert` cheap because it is bounded only by the caller's context, not the
migration's `statement_timeout`.

## Atlas txtar check files

An Atlas txtar migration can carry the default `checks.sql` section and ordered
named sections under `checks/*.sql`. Ptah enforces every check file through the
same machinery as `-- +ptah check`, matching Atlas's
enforcement point (measured). Each statement is a
top-level `SELECT` that must return exactly one column and one row containing a
truthy scalar. Statements are split using the target database dialect and files
run in archive order before any `migration.sql` statement.

Every assertion in a check file must pass by default. A file-level
`-- atlas:assert oneof` directive changes that file so at least one assertion
must pass. Query errors and invalid result shapes still fail closed inside a
`oneof` group; this directive does not turn invalid SQL into a passing check.
An empty `oneof` file also fails because none of its zero assertions passed.
A failure names the embedded file and assertion position, such as
`checks/users.sql#2`. A `oneof` group where every assertion is falsy names the
group itself.

```sql
-- atlas:txtar

-- checks.sql --
SELECT NOT EXISTS (SELECT * FROM users);

-- checks/roles.sql --
-- atlas:assert oneof
SELECT NOT EXISTS (SELECT * FROM roles);
SELECT NOT EXISTS (SELECT * FROM user_roles);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
```

Txtar checks follow every rule on this page: truthiness interpretation,
up-direction only, the `--tx-mode all` refusal, and the `--skip-checks`
bypass on native `ptah migrations up`. `ptah-compat migrate apply` has no
`--skip-checks` flag (parity with Atlas, which has none either); it reads the
`PTAH_SKIP_CHECKS` environment variable instead — see
[Bypassing checks](#bypassing-checks).

The `#N` suffix counts only non-empty statements, so comment-only spans and
stray separators (`;;`) do not consume a number: the third assertion you can
read in `checks/users.sql` is always `checks/users.sql#3`.

Ptah preserves each assertion's SQL text when executing it. Dialect-aware
lexing recognizes PostgreSQL `E'...'` escape strings, MySQL/MariaDB's required
whitespace after a `--` comment marker, and MySQL/MariaDB executable comments
and optimizer hints. These constructs therefore cannot hide a later assertion
or be stripped into a query with different semantics.

Before execution, Ptah expands executable-comment bodies into an effective SQL
form and applies version guards against the connected server version. A numeric
prefix is a version guard only when it contains at least five digits; shorter
prefixes remain part of the executable SQL body. The effective form must still
contain exactly one top-level `SELECT`, so an internal statement delimiter or a
non-`SELECT` body fails closed before the query runs.

## Bypassing checks

Checks are an additive, finer-grained safety gate that composes with the coarse
`--check-destructive` / `--allow-destructive` gate. For an emergency override,
`ptah migrations up --skip-checks` skips all pre-migration checks — both
`-- +ptah check` directives and all Atlas txtar check files — mirroring the
`--allow-destructive` bypass. Use it only after review.

On the Atlas-compatible surface the same bypass is spelled as an environment
variable, `PTAH_SKIP_CHECKS`:

```bash
PTAH_SKIP_CHECKS=1 ptah-compat migrate apply --url "$DB" --dir file://migrations
```

The name is not a second convention. Ptah binds every native flag to a
`PTAH_<FLAG>` environment twin, so `ptah migrations up --skip-checks` already
answers to `PTAH_SKIP_CHECKS`; `ptah-compat migrate apply` reads the same
variable. Values are parsed as booleans (`1`, `true`, `t`, and their negations),
and an unset or empty value enforces checks. A run with the bypass active prints
a warning on stderr, because unlike a flag it leaves no trace in the command
line.

The two binaries agree on the name and on every valid boolean. They differ on an
**invalid** one: `ptah-compat migrate apply` refuses it outright, before opening
the database, so a typo in a CI environment file cannot read as a bypass that
silently was not one, while native `ptah migrations up` discards the parse error
and enforces checks. Both fail safe — a value neither of them understands never
bypasses anything — but only the compat surface says so. Values with surrounding
whitespace (`" 1"`) are not booleans and follow the same split.

It is an environment variable rather than a flag because Atlas registers no
`--skip-checks` on `migrate apply` and `ptah-compat` does not add flags Atlas
does not have. Measured, not assumed: Atlas CE v1.2.0 answers
`migrate apply --skip-checks` with `unknown flag: --skip-checks`, identically to
a nonsense flag, and Atlas's own help surface registers `--skip-checks`
only on `migrate down`. This is the same mechanism `PTAH_ALLOW_EXTERNAL_SCHEMA`
uses for `atlas.hcl` `data "external_schema"`.

`PTAH_SKIP_CHECKS` bypasses pre-migration checks and nothing else. In particular
`atlas.sum` verification still refuses a tampered or unhashed migration
directory, and revision bookkeeping is unchanged.

`ptah-compat migrate down` accepts an Atlas `--skip-checks` flag it does not
implement and refuses it loudly. That one refusal is explicit-only:
`PTAH_SKIP_CHECKS` does not trigger it, so exporting the variable for an apply
does not break rollbacks in the same shell, and `migrate down --help` shows no
`[env: ...]` suffix for it.

That exception stops there. `migrate down`'s other waivers — `--to-tag` and
`--plan` — keep their environment twins, because those names mean nothing else:
setting `PTAH_TO_TAG` *is* a request for a capability Ptah lacks, and refusing
it is the right answer. Ignoring it would not be a harmless no-op — the target
would parse as version `0` and the rollback would revert the entire history.

## Integrity

Check directives live in the migration file, so they are covered by the existing
`ptah.sum` integrity verification with no new checksum surface — tampering with a
check changes the file hash and fails verification.

## Relationship to Atlas

Atlas keeps pre-migration checks in its proprietary Pro build, requiring the
closed-source binary, and not embeddable. Ptah offers the local assertion half as an open,
no-account, in-process capability. The Atlas Cloud approval-policy half is out of
scope.
