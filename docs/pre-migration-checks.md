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
| `assert` | yes | A single SQL predicate that must evaluate to a truthy scalar. |
| `name` | no | A label for the check, shown in error output. |
| `on_fail` | no | What to do when the assertion is not satisfied. Only `abort` is supported; it is the default. |

- `assert` is a single statement returning one scalar. A boolean result uses its
  value; a number passes when non-zero; a text/`bytea` result accepts the common
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
ignored. A failing check produces a `CheckFailedError` that names the migration
version and check, and `ptah migrations up` exits non-zero.

A failed check writes **no revision row**. Checks run before the migration's
bookkeeping row is created, so a blocked migration is recorded as never
started rather than as dirty: the revision table is left byte-identical, and
`migrations status` keeps reporting the previous version with the blocked
migration merely pending. Once the data the check guarded is corrected, the
next run applies it with no bypass flag and no `migrations repair`. This
matters most for `ptah-compat migrate apply`, which by design carries neither
`--skip-checks` nor `--allow-dirty`, and it matches Atlas, which also records
nothing when its own checks fail.

## Assertion result shape

The `assert` predicate is contracted to return one row with one column.

- **More than one column** fails the check closed: the result cannot be
  interpreted, and a check that cannot be evaluated blocks the migration
  rather than passing it.
- **More than one row** is judged on the first row returned. Order is only
  defined if the predicate orders it, so prefer an aggregate
  (`SELECT count(*) = 0 FROM ...`) or an `EXISTS` form over a bare multi-row
  `SELECT`.

Because the check is a separate read that precedes the body, it is not atomic
with it: for a single migrator (the normal case) nothing else writes in between,
but a concurrent session committing between the check and the body is not
re-validated. Keep checks as guards against pre-existing state, not as
serialization primitives, and keep each `assert` cheap — it runs bounded only by
the caller's context, not the migration's `statement_timeout`.

## Atlas txtar `checks.sql` sections

An Atlas txtar migration can carry a `checks.sql` section. Ptah enforces it
through the same machinery as `-- +ptah check`, matching the licensed Atlas
build's semantics (measured against Atlas CLI v1.2.4): each statement in the
section is an assertion that must return a single truthy scalar, evaluated in
section order before any `migration.sql` statement runs. A failing assertion
aborts the migration with nothing applied; the error names the migration and
the failing statement's position (`checks.sql#1`, `checks.sql#2`, ...).

```sql
-- atlas:txtar

-- checks.sql --
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
```

Txtar checks follow every rule on this page: truthiness interpretation,
up-direction only, the `--tx-mode all` refusal, and the `--skip-checks`
bypass on native `ptah migrations up`. The compat `ptah-compat migrate apply`
has no `--skip-checks` flag (parity with Atlas), so checks always enforce
there.

The `#N` suffix counts only non-empty statements, so comment-only spans and
stray separators (`;;`) do not consume a number: the third assertion you can
read in the section is always `checks.sql#3`.

## Bypassing checks

Checks are an additive, finer-grained safety gate that composes with the coarse
`--check-destructive` / `--allow-destructive` gate. For an emergency override,
`ptah migrations up --skip-checks` skips all pre-migration checks — both
`-- +ptah check` directives and Atlas txtar `checks.sql` assertions — mirroring
the `--allow-destructive` bypass. Use it only after review.

## Integrity

Check directives live in the migration file, so they are covered by the existing
`ptah.sum` integrity verification with no new checksum surface — tampering with a
check changes the file hash and fails verification.

## Relationship to Atlas

Atlas keeps pre-migration checks in its proprietary Pro build (free-with-login on
a free tier, paid after a trial), requiring the closed-source binary and an Atlas
account, and not embeddable. Ptah offers the local assertion half as an open,
no-account, in-process capability. The Atlas Cloud approval-policy half is out of
scope.
