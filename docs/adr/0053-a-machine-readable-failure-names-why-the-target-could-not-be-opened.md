# ADR 0053: A machine-readable failure names why the target could not be opened

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah-operator#760](https://github.com/stokaro/ptah-operator/issues/760)

Relates to: [ADR 0007](0007-agent-error-taxonomy.md)

## Context

The Kubernetes operator runs `schema drift --format json`, `schema plan
--json`, `schema apply --json` with and without `--dry-run`, `migrations status
--json` and `migrations up --json`, and reads the document each one prints. It
never copies text a child process wrote into a resource's status, because that
text can quote a row or a credential. So when the target database cannot be
opened, the operator reports only the exit status, `2`, and a person on call
has to find the Job's Pod and read its log.

Measured on master at ab6442961 against a URL whose port refuses the
connection, each command exits `2`, and no document says why in a form a
program can branch on:

- `schema plan` and `schema apply` print a document whose `error` is the
  sentence they print on standard error;
- `schema drift --format json` prints its report, with `error` set, on standard
  error, and nothing on standard output;
- `migrations status` and `migrations up` print nothing on standard output.

The drivers do say why, in types. pgx returns `*pgconn.PgError` with the
server's SQLSTATE, and keeps the `net` and `crypto/tls` errors under its own
wrappers. go-sql-driver/mysql returns `*mysql.MySQLError` with the server's
error number, the sentinel `mysql.ErrNoTLS`, or the `net` error itself. Ptah's
connectors and every command between them and the document wrap with `%w`,
except `schema drift`, which passed the message on as a string. The type reaches
the command; the documents drop it.

## Decision

A closed vocabulary of failure kinds lives in `internal/errorkind`:
`database_unreachable`, `authentication_failed`, `database_not_found`,
`permission_denied` and `tls_failed`. A kind is read from typed errors and
never from message text.

Only the failure to open the command's target connection has a kind. Each of
the commands above marks that failure where it connects, and an unmarked error
has no kind. A dev database, an OCI registry, a Git remote and a webhook dial
too, and a refused dial to any of them is not the target being unreachable.
Within the mark, the server's own answer decides first, and an answer the
vocabulary does not name leaves the failure with no kind. A TLS handshake
failure comes next, then a server that never answered: a name that did not
resolve, a dial that failed, or a deadline that passed while the connection was
opened.

Each command's own document carries the failure as an `error` object with a
`message`, the sentence printed on standard error, and a `kind` when there is
one. Each document also names its `command`. A command without a failure
document gets one, so every run that reaches the command prints one document on
standard output:

- `schema drift --format json` prints `contract_version`, `command` and
  `error` on standard output in place of the report, and the `error:` line on
  standard error;
- `migrations status --json` prints the same three fields in the status
  contract's version, and none of a status's fields;
- `migrations up --json` prints its run document with the outcome `failed`,
  nothing planned and no status, for a run that stopped before it selected
  anything.

The contract versions move where a field changed meaning or a document took a
new shape: plan 3, apply 4, migration run 2 and migration status 2. The drift
report carries a version for the first time, 1. A new kind does not move a
version; a reader treats a kind it does not know as no kind.

The exit code stays `2` for every kind.

### Alternatives

**An exit code per kind.** Codes from `3` up are reserved, the compatibility
surface promises `0` and `1` only, and a script that does not read the
document would receive codes it cannot interpret. The operator reads the
document either way, and the document can carry the message beside the kind.

**An `error_kind` field beside the `error` string.** It would keep every
version and every reader as they are, the operator's included. It was not
chosen because Ptah owes no compatibility to its own earlier output before v1,
and the operator pins Ptah by digest, so a version change reaches it only in
the change that consumes the kind. One object keeps the message and its kind
together.

**A kind for every error.** Misattribution is the cost: a refused dial to a
registry would read as an unreachable database, and the operator would send a
person to the target's URL Secret.

**Kinds for failures after the connection opened.** A timeout after the server
answered is a slow statement, a lock wait or a statement timeout, and says
nothing about reachability; `lock-timeout` already has a refusal code. A
statement a role may not run is already in a migration run's
`failure.sqlstate` and `failure.error_number`, and an apply whose statements
were sent is `unknown` whatever failed. `permission_denied` covers the refusal
at connect: no `CONNECT` privilege on PostgreSQL, no grant on the database on
MySQL. That one has a fixed place to look.

**Reading the message.** Driver wording changes between releases, and a
PostgreSQL server localizes its messages with `lc_messages`; the SQLSTATE does
not change.

## Consequences

- The operator maps each kind to a fixed sentence and never reads the message.
- The operator's decoders for plan 2, apply 3, run 1 and status 1 refuse the
  new versions, so the operator release that pins this Ptah has to read them.
- `migration/migrator.RunResult.Error` is an `*ErrorReport` rather than a
  string, an incompatible change to a stable package, recorded in
  `docs/public_api_approvals.txt`.
- pgx reports a server that refuses the TLS a URL requires as text with no
  type, so `sslmode=require` against a server without TLS has no kind. The
  MySQL driver's sentinel for the same refusal is classified.
- On macOS, Go's system verifier reports some certificate refusals as text,
  which pgx's `sslmode=verify-ca` returns unwrapped; those have no kind there.
  Linux, where the operator runs, returns the typed `crypto/x509` errors.
- MySQL answers an account that holds no privilege on a database name with
  access denied whether or not the database exists, so that account sees
  `permission_denied` rather than `database_not_found`.
- The agent surface (ADR 0007) keeps its own `database_unreachable` code for any
  connection that could not be opened. It could take its finer answers from
  this classifier.
