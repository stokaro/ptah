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
  before any statement or transaction, so nothing is applied, and the failure
  lands in the normal dirty-state handling.
- **`--tx-mode all`**: pre-migration checks are **not supported** and are
  rejected before anything is applied. Under one shared transaction a check
  reading committed state cannot see earlier batched migrations' uncommitted
  changes, so it would silently evaluate a precondition against stale state. Run
  such migrations with the default per-file mode, or pass `--skip-checks`.

Checks are evaluated for the **up** direction only (they guard forward,
typically destructive, migrations); a `-- +ptah check` in a down migration is
ignored. A failing check produces a `CheckFailedError` that names the migration
version and check, and `ptah migrations up` exits non-zero.

Because the check is a separate read that precedes the body, it is not atomic
with it: for a single migrator (the normal case) nothing else writes in between,
but a concurrent session committing between the check and the body is not
re-validated. Keep checks as guards against pre-existing state, not as
serialization primitives, and keep each `assert` cheap — it runs bounded only by
the caller's context, not the migration's `statement_timeout`.

## Bypassing checks

Checks are an additive, finer-grained safety gate that composes with the coarse
`--check-destructive` / `--allow-destructive` gate. For an emergency override,
`ptah migrations up --skip-checks` skips all pre-migration checks, mirroring the
`--allow-destructive` bypass. Use it only after review.

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
