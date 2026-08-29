---
title: Lint and gate unsafe SQL
description: Analyze migration files for production-unsafe patterns, set the rule policy, declare rules of your own, and gate destructive statements at apply time.
---

Some SQL is legal and still wrong to run against a database people depend on.
This page shows how Ptah reports those statements before they run, how you set
the policy that decides which of them block a release, and what the apply-time
destructive gate does with that policy.

Prerequisites: a migration directory (see
[Generate migrations](../generate/)). Whether that directory is the one you
reviewed is a separate question, answered by
[Integrity and safety](../integrity-and-safety/).

`ptah migrations lint` analyzes migration files for patterns that are legal
SQL but dangerous in production — dropped tables and columns, lock-heavy
DDL, and dialect-specific hazards:

```bash
ptah migrations lint --dir ./migrations --dialect sqlite
```

A finding names the file, line, rule, and remediation (exit `1`):

```text
migrations/0000000003_drop_users.up.sql:1 [error] DS101: DROP TABLE permanently deletes table users and every row in it; take a verified backup first and consider a rename-and-retire window instead (table dropped)

1 finding(s).
```

A clean run prints `No lint findings.` and exits `0`.

Every rule identifier, with its one-line meaning, the dialects it applies to,
the surface that reports it, and whether the name is Atlas's or Ptah's, is
enumerated in [Lint rules](../../reference/lint-rules/).

## Which direction each rule reads

Most rules describe a forward schema change, so they read the `.up.sql` half
only: a down file dropping what its up created is the expected shape, not a
hazard.

Rules whose subject is the **cost or the executability** of a statement read
both halves, because PostgreSQL charges the same lock and enforces the same
transaction restriction whichever direction asked for it:

- `PG106` — `DROP INDEX` without `CONCURRENTLY` blocks writes. This is the
  statement a rollback file is normally made of, including the one Ptah
  generates whenever the forward statement was not a concurrent build.
- `PG103` — a `CONCURRENTLY` statement in a file with no `no_transaction`
  marker cannot execute at all. A rollback that cannot run is only discovered
  when someone needs it.
- `TX101` — a file mixing autocommit-only statements with transactional DDL.
  The classification is semantic, not keyword-based: a file that adds a value to
  an enum type it creates itself is not a mix, because PostgreSQL allows the new
  value immediately when the type is new in the same transaction, while a file
  adding a value to a pre-existing type and then using it is one. The migrator
  refuses that second shape before its first statement runs, using the same
  classification, so lint and apply cannot disagree about one file.

Useful controls, all designed for CI:

- `--latest N` lints only the newest N migration revision keys — the changeset
  of a pull request rather than all of history. Atlas-format repeatables keep
  their string keys (`R` or `<number>R`), and bare `R` sorts after numeric
  files. `--git-base <branch>` selects the changeset from Git instead.
- `--fail-on error` (default) fails only on error-severity findings;
  `--fail-on any` fails on warnings too; `--fail-on none` always exits `0`.
- `--format json`, `--format sarif`, and `--format github-actions` feed code
  scanners and PR annotations. The SARIF output is a SARIF 2.1.0 document
  that GitHub code scanning ingests — [CI](../../testing/ci/) shows the
  upload step.
- `--dialect` gates dialect-specific rules; accepted values are `postgres`,
  `mysql`, `mariadb`, `sqlite`, `sqlserver`, `clickhouse`, `cockroachdb`,
  `yugabytedb`, and `spanner`. Every documented alias of those names is
  accepted too and resolves to the canonical one, so `--dialect pgx`,
  `--dialect postgresql` and `--dialect postgres` are the same request — see
  [Dialects and capabilities](../../concepts/dialects-and-capabilities/) for
  the full spelling table. `--dev-url` infers the dialect and additionally
  replays the directory on the dev database.
- `--disable DS101` (or a family such as `MY`) skips rules ad hoc; a
  committed `.ptah-lint.yaml` does it persistently and adds per-rule
  severity and path scoping — see below.

For OCI-distributed directories, `lint --dir oci://...` lints the published
artifact, and `--attach` stores the canonical report next to it — see
[OCI registry artifacts](../../operate/oci-registry/).

## Configure rules with `.ptah-lint.yaml`

Commit a `.ptah-lint.yaml` next to the migration files to make lint policy
part of the reviewed migration directory. `ptah migrations lint --config`
selects an explicit lint policy; without that flag, lint loads
`<dir>/.ptah-lint.yaml` when present:

```yaml
dialect: postgres
disabled-rules:
  - MF103
  - MY
rules:
  DS103:
    severity: warning
  DS104:
    severity: info
  DS102:
    severity: error
    exclude:
      - legacy/**
```

- `dialect` sets the default lint dialect; `--dialect` overrides it. It takes
  the same spellings as `--dialect`, aliases included, and is stored
  canonicalized — `dialect: pgx` and `dialect: postgres` select the same rules.
- `disabled-rules` lists rule codes (`DS101`) or family prefixes (`MY`) to
  skip entirely; entries merge with `--disable` flags. Selectors and custom
  rule codes use uppercase ASCII letters and digits and start with a letter.
- `rules` keys name an exact code or a family prefix; the most specific key
  wins, so a `DS102` entry beats a `DS` entry.
- `severity` accepts `info`, `warning` or `error` and replaces the rule's
  default severity on its findings — the level that `--fail-on` and the
  apply-time destructive gate below evaluate. Only `error` gates: a rule set to
  `info` or `warning` is reported and exits `0`. `info` exists so a rule can be
  introduced to a repository that still violates it, and so a team can say
  "surface this, never block on it" without the alternatives being loud enough
  to fail or absent from the report entirely. In SARIF the three levels are
  `note`, `warning` and `error`. Any other value fails config parsing (exit
  `2`), so the vocabulary gained a level rather than becoming permissive.
- `exclude` lists slash-separated path globs (`**` crosses directory
  levels) where the rule is skipped. Prefer paths relative to the migration
  directory, such as `legacy/**`; these match regardless of how `--dir` was
  spelled. A directory-prefixed pattern such as `migrations/legacy/**`
  matches only when the command path has that prefix, for example
  `--dir migrations`, and need not match an absolute `--dir` path. Patterns
  must already be normalized: repeated separators, `.` or `..` segments, and
  trailing separators are configuration errors rather than being rewritten
  into a broader match. Empty patterns and malformed glob syntax are also
  errors; Ptah reports the rule and pattern instead of silently weakening the
  policy.

Configuration decoding is strict. Unknown keys, misspelled keys such as
`severty`, lowercase or whitespace-padded selectors, selectors that match no
registered rule, unsupported dialects or severities, empty or malformed
or non-normalized exclusion globs, and multiple YAML documents fail before
linting or migration execution instead of silently weakening policy.

Precedence: a rule listed in `disabled-rules` never runs, regardless of its
`rules` entry; then `exclude` skips the matching files; then `severity`
relabels the findings that remain. Per-rule entries apply to custom
analyzer codes the same way as to built-in ones — see
[Reusable components](../../extend/components/) for registering custom
rules from Go.

## Declare a rule of your own

A `rules` entry that carries `match` **defines** a rule instead of configuring
one. The rule runs on `ptah migrations lint` and on the compat `migrate lint`
alike, with no Go build anywhere:

```yaml
dialect: postgres
rules:
  NOVARCHAR:
    title: varchar(n) instead of text
    severity: warning
    match: 'strcontains(lower(statement.sql), "varchar(")'
    message: use text, not varchar(n) — postgres stores them identically
```

The same rule in `atlas.hcl`, where `match` is written as a bare expression:

```hcl
env "local" {
  lint {
    rule "NOVARCHAR" {
      title    = "varchar(n) instead of text"
      severity = "warning"
      match    = strcontains(lower(statement.sql), "varchar(")
      message  = "use text, not varchar(n)"
    }
  }
}
```

`match` is an expression evaluated once per statement; the rule fires where it
is true. `message` is required — a finding whose text is its own rule code says
what fired and not why it matters. `severity` defaults to `warning`, `title`
defaults to the code, `dialects` restricts the rule to named dialects, and
`applies-to-down` (`applies_to_down` in HCL) extends it to the down half of a
migration.

The code follows the same form as every other rule — uppercase ASCII letters
and digits — because it is what findings print and what `--disable`,
`disabled-rules` and a `ptah:nolint` directive select. Put the readable name in
`title`. A code that already belongs to a built-in rule is refused rather than
overridden: replacing a data-safety check with an expression that never fires
would leave the report naming a rule that is not running.

### What an expression can read

| Name | Meaning |
| --- | --- |
| `statement.sql` | the statement as written, comments included |
| `statement.canonical` | comment-stripped, whitespace-collapsed, uppercased |
| `statement.words` | token words; string literals and quoted identifiers stay whole |
| `statement.line` | 1-based line of the statement's first token |
| `file.path` | the path findings report |
| `file.is_up`, `file.is_down` | which direction the statement belongs to |
| `dialect` | the dialect being linted |

Prefer `statement.words` for a rule about SQL keywords: a column named `drop`
or a string literal containing `DROP COLUMN` cannot impersonate a keyword
there, and a substring match on `statement.sql` has no way to tell them apart.

The functions are the same set `atlas.hcl` evaluates — `lower`, `upper`,
`join`, `regexall`, `length` and the rest — plus `strcontains(haystack,
needle)` for substring matching. Note that `contains` tests **list** membership,
so it belongs on `statement.words`; using it on a string reports the mistake and
names `strcontains` as the fix.

There is deliberately no `file`, `fileset`, `getenv` or `print`. A rule that
could read a file or the environment would report findings that depend on the
machine it ran on, so the same migration would lint clean on one checkout and
fail in CI with nothing in the migration to explain it. Evaluation is a pure
function of the statement, which is what makes a finding reproducible.

An expression must evaluate to a boolean; anything else is refused rather than
coerced, since a coerced value fires on every statement or on none and both
look like a working rule. A malformed declaration — an empty or unparseable
expression, an unknown name, a missing message, an unsupported dialect — fails
the run before any findings are reported.

## Suppress a single statement inline

When one reviewed statement is acceptable but the rule should stay active
everywhere else, put a `ptah:nolint` comment directly above it:

```sql
-- ptah:nolint DS102
ALTER TABLE users DROP COLUMN archived_note;
```

The directive suppresses only the named rules, and only for the statement
directly below it — a blank line between the comment and the statement
detaches the two. List several codes separated by spaces or commas, name a
family (`DS`), or write a bare `-- ptah:nolint` to silence every rule for
that one statement.

`-- atlas:nolint DS102` is accepted as an alias with the same
statement-scoped behavior, so a directory shared with Atlas tooling keeps
one set of directives. Atlas analyzer-name selectors (`destructive`,
`data_depend`, `concurrent_index`, `incompatible`, `nestedtx`) name rule
families and work here too. A code selector always names the code the
command you ran printed, so on this surface it is the native code:
`ptah migrations lint` reports `DS102` for a dropped column and
`-- atlas:nolint DS102` silences it, while `ptah-compat migrate lint`
prints the same finding as `DS103`.

The `atlas:` spelling follows Atlas's matching rule rather than Ptah's, so a
code selector there matches one code exactly: `-- ptah:nolint DS` silences
the data-safety family and `-- atlas:nolint DS` silences nothing. An
unrecognized `atlas:nolint` selector is accepted and silences nothing,
without a warning; `.ptah-lint.yaml` `disabled-rules` stays strict and
rejects a selector matching no registered rule. Whole-file `atlas:nolint`
headers take effect only on the Atlas-compatible surface — see
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
- 0000000003_drop_users.up.sql:1 DS101 error: DROP TABLE permanently deletes table users and every row in it; take a verified backup first and consider a rename-and-retire window instead
```

Use `--allow-destructive` only after the plan has been reviewed and the
rollback path is understood.

`ptah migrations up` always loads and validates the conventional
`<migrations-dir>/.ptah-lint.yaml`; when the apply-time gate is active, it
blocks on error-severity `DS` data-safety findings. What the gate lints is
always the dialect the connection reports, never the policy's — a policy
`dialect` is a statement about the directory, not a scanner selector.

A nonempty policy `dialect` must name the **same engine family** as the
connected database, and a cross-family mismatch fails before migration analysis
or execution. The families are the ones on
[Dialects and capabilities](../../concepts/dialects-and-capabilities/):

| Policy `dialect` | Connected database | Verdict |
| --- | --- | --- |
| `postgres`, or any alias such as `pgx` | PostgreSQL | matches |
| `postgres` | CockroachDB, YugabyteDB, Spanner | matches — they ride the PostgreSQL family |
| `mysql` | MariaDB | matches — one family |
| `mariadb` | MySQL | matches — one family |
| `mysql` or `mariadb` | PostgreSQL | **does not match** |
| `postgres` | MySQL or MariaDB | **does not match** |
| `sqlite`, `sqlserver`, `clickhouse` | anything else | **does not match** — each stands alone |

Naming a family member rather than the exact engine is accepted because it
does not change the analysis: every built-in MySQL-family rule applies to both
`mysql` and `mariadb`, and the scanner treats them identically. Note the one
asymmetry inside the PostgreSQL family: the `PG` and `TX` rules apply to
PostgreSQL only, so a CockroachDB, YugabyteDB or Spanner database runs the
dialect-independent families alone — whether or not a policy file exists, and
regardless of what it declares.

On the standalone lint command, an explicit `--dialect` still overrides the
policy, and `--dev-url` is checked against the policy by exactly the same
family rule, so `ptah migrations lint` and `ptah migrations up` accept the same
policy files. The `--config` flag on `ptah migrations up` selects `ptah.yaml`;
it does not select a lint policy. This distinction prevents a project
configuration path from silently replacing the policy shipped with the
migration directory.

The gate otherwise uses the same dialect selection, rule configuration, and
path matching as `ptah migrations lint`. That makes the escape hatch
proportional: a rule downgraded with
`severity: warning`, listed under `disabled-rules`, or excluded for a path
stops blocking exactly that reviewed pattern — a widening
`ALTER COLUMN ... TYPE` under `rules: {DS103: {severity: warning}}` applies
without `--allow-destructive` — while a `DROP TABLE` in another pending
file of the same batch still aborts the apply. `--allow-destructive`
remains the all-or-nothing per-run override rather than the only way past
a single warning-grade finding. `--allow-destructive` bypasses the findings
gate, but it does not bypass loading or validating the lint policy.

Lint-policy severities are `warning` and `error`. Generated safety reports use
the operational vocabulary `safe`, `warning`, and `destructive`; an `error`
lint assessment and a `destructive` safety assessment are both blocking.

Know the limits of this gate: the policy file is not tamper-evident.

`ptah.sum` hashes only the migration `*.sql` files, so `ptah migrations
validate` and `up --verify-sum` still pass when `.ptah-lint.yaml` is added or
edited out-of-band, and the apply prints no notice when the config suppresses
a destructive finding — a one-line `disabled-rules: [DS]` dropped next to the
migrations at deploy time disables this gate silently.

Loosening the gate is a visible committed change only if your process makes it
one: commit `.ptah-lint.yaml`, treat any edit to it as an edit to the gate in
review, and restrict writes to the deployed migration directory, because the
integrity file does not protect the policy the way it protects the SQL.

## Next steps

- Every rule identifier, its meaning, and which surface reports it:
  [Lint rules](../../reference/lint-rules/).
- Running these gates on every pull request? [CI](../../testing/ci/).
- Proving the directory is the one you reviewed?
  [Integrity and safety](../integrity-and-safety/).
- Registering a rule from Go instead of YAML?
  [Reusable components](../../extend/components/).
