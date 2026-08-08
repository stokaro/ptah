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

The same drift blocks `ptah migrations up` on any hashed directory — the
apply verifies `ptah.sum` or `atlas.sum` before executing anything, so a
tampered migration never runs (exit `2`):

```text
error: migration sum verification failed:
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

`ptah-compat migrate apply`, `migrate status`, and `migrate set` enforce the
same gate on `atlas.sum` directories with Atlas's own checksum output, matching
official Atlas behavior. Reporting is not exempt: on a hashed directory whose
only migration was deleted, an ungated `migrate status` announced
"Database is up to date"
([#974](https://github.com/stokaro/ptah/issues/974)).

### The sum file has to agree with itself, too

Verification asks two questions, not one, and the second was missing until
[#1231](https://github.com/stokaro/ptah/issues/1231): the entries must match the
directory, **and** the directory-hash line on top must be the hash of the entry
lines below it, in the order they are written. Both hash schemes bind that
order, so moving a whole entry line — name and hash together — leaves a file
that no longer hashes to the line it still carries.

That is what a reordered `atlas.sum` is, and it used to verify clean: every file
was found with the hash the sum recorded, and the hash recomputed over the
*directory* still matched the stale line on top. `migrate validate` printed
nothing and exited 0 while the community CLI exited 1; `migrate apply` ran every
migration.

The two tampered shapes are distinguishable and are reported differently, which
is also what that CLI does:

| `atlas.sum` | reported as |
| --- | --- |
| entries reordered, top line untouched | `checksum mismatch`, with no entry named — the file contradicts itself, so no entry can be blamed |
| entries reordered, top line recomputed | `checksum mismatch` with `L2: <file> was added` — the file agrees with itself and disagrees with the directory |

Recovery is a decision, not a command: if the change is intentional, review
it and re-run `ptah migrations hash`; if it is not, restore the file from
version control. Use `git diff` on the migration directory to tell the two
apart. Run `validate` in CI and hash every shared directory so drift is
caught at review time, not at deploy time.

## A directory that was never hashed

Drift is one question; "there is no sum file at all" is a different one, and
the two surfaces answer it differently on purpose.

**Native `ptah migrations up` applies an unhashed directory.** Hashing is
opt-in on this surface: a directory with no `ptah.sum` or `atlas.sum` has
never claimed integrity, so demanding one would break every project that has
not adopted `ptah migrations hash` yet. Turn the missing file into an error
where that matters — in CI, and on deploys of directories you do hash — with
`--verify-sum` (exit `2`):

```bash
ptah migrations up \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations \
  --verify-sum
```

```text
error: migration sum verification failed: ptah.sum not found; run `ptah migrations hash` to create it
```

That flag is the only thing on the native surface that rejects a never-hashed
directory; a hashed one is always verified with or without it.

**A sum check is worth what the sum's provenance is worth.** Either gate
compares a directory against the sum stored beside it. For a local directory
that sum was reviewed in version control next to the migrations. For an
`oci://` artifact the sum travels inside the artifact, so anyone who can push
to the repository can rewrite the migrations, rehash them, repoint a tag, and
watch the check pass over bytes nobody reviewed. `ptah migrations up` therefore
qualifies the claim when a sum verifies over a tag-resolved artifact, naming
the digest the tag resolved to and the `@sha256:` reference that pins it:

```text
Warning: oci://ghcr.io/acme/app-migrations:release is a movable tag: ptah.sum
travels inside the artifact, so verifying it proves the pulled files are
internally consistent, not that they are the reviewed ones. This tag resolved
to sha256:<digest>; pass oci://ghcr.io/acme/app-migrations@sha256:<digest> to
pin these exact bytes.
```

The run still succeeds — the check did what it claims. A digest reference and
a local directory produce no such line, and
`oci://ghcr.io/acme/app-migrations:release@sha256:<digest>` is a digest
reference: the digest selects and is verified, the tag is only a label.

**`ptah-compat migrate apply`, `migrate status` and `migrate set` refuse an
unhashed directory.** Atlas treats a missing `atlas.sum` as a checksum error, so
the compatibility surface does too — measured against the pinned community
binary, which exits `1` and never creates the target database:

```text
You have a checksum error in your migration directory.
Please check your migration files and run 'atlas migrate hash' to re-hash the contents

Error: checksum file not found
```

Nothing executes and the target is never opened, exactly as with a checksum
mismatch. Run `ptah-compat migrate hash` once and commit the `atlas.sum` it
writes. A directory that holds no `.sql` file anywhere in its tree — a freshly
created or `.gitkeep`-only migrations directory — is not a checksum error: it
reports `No migration files to execute` and exits `0`, matching Atlas.

That scan reads the top level only, because an Atlas migration directory
executes exactly what its `atlas.sum` covers: top-level files whose name ends in
`.sql`, spelled in lower case. A `.sql` file in a subdirectory, or a top-level
`.SQL`, is not a migration and is not run
([#976](https://github.com/stokaro/ptah/issues/976)).

That rule replaced a recursive one. Ptah used to discover and execute nested
files while hashing only the top level, so a migration in a subdirectory ran
with no checksum reaching it: `migrate validate` reported the directory clean,
and editing that file afterwards changed what ran without changing any hash.
Converging the executed set onto the covered set is what makes `atlas.sum` mean
something; refusing such directories, which is what the previous release did for
the unhashed half only, never closed the hashed half at all.

Ptah names what it declined. A file it found but did not treat as a migration is
reported on stderr:

```text
warning: sub/2_b.sql is not covered by atlas.sum and will not run; Atlas migrations are top-level files named *.sql
```

Atlas prints nothing here, and that silence is the reason Ptah does not copy it:
a directory whose only migration sits one level down hashes zero files,
validates clean, and applies nothing at exit `0`, so a migration you committed
never runs and no output says so. Exit codes and stdout stay identical to Atlas;
only this stderr line is added. Move the file to the top level to have it run.

:::caution[Breaking change]
A project that relies on Atlas-format migrations in subdirectories will stop
executing them. The warning above names every affected file on each run.
:::

Native `ptah migrations` commands apply the same selection under
`--dir-format atlas`, which writes an `atlas.sum` and so is bound by the same
coverage. Auto mode without an `atlas.sum` keeps reading subdirectories and
stays self-consistent, because `ptah.sum` is computed from the same discovery
and covers every file it finds. The declined-file warning is currently printed
by the `ptah-compat` verbs only.

**`migrate lint` is deliberately not gated**, on either tool: inspecting a
directory that has drifted is the point of linting it. That exemption covers a
*missing* integrity file only — on a hashed directory that has since drifted,
both `lint` implementations exit 1.

**`migrate new` and `migrate diff` are gated before they write.** Both create a
migration file and rewrite `atlas.sum`, so an ungated run turned drift into
apparent cleanliness: the tampering survived and the checksum that would have
reported it was replaced. Since
[#1086](https://github.com/stokaro/ptah/issues/1086) the refusal is a preflight
— it happens before the migration file is created, before `atlas.sum` is
rewritten, and on `diff` before the dev database is connected to and before
`--to` and `--dev-url` are required at all, which is the order Atlas uses. A
`--dir` that does not exist yet is not a checksum error on either tool: both
verbs create it, which is how a project's first migration gets written.

**`migrate import` is gated on the source directory, with one exemption.** A
source that carries an `atlas.sum` must verify against it before anything is
converted or written; a source that carries no `atlas.sum` at all is imported,
because a directory another tool wrote has never been hashed and importing it is
what the verb is for. That is the one place the rule differs from `apply`,
`status` and `set`, which refuse an unhashed directory outright, and it matches
Atlas on both halves. Until
[#1095](https://github.com/stokaro/ptah/issues/1095) the source was not checked
at all: a directory `migrate apply` refused was converted anyway, and the
destination was hashed over whatever the conversion produced — so a tampered
source came out as a directory `migrate validate` calls clean. Nothing is
written when the check fails; the destination directory is not created.

**Directories read through `?format=` are gated too.** A goose, flyway,
liquibase, dbmate or golang-migrate directory is converted in memory and the
converted filesystem carries no integrity file — but the directory it was read
*from* carries `atlas.sum` beside its own migrations, and that is what is
verified, before the source layout is parsed and before the database is opened.
Run `ptah-compat migrate hash --dir 'file://migrations?format=goose'` once and
commit the file. The same applies when the layout comes from `atlas.hcl`
(`migration { format = goose }`) rather than from the URL.

Each layout is verified over the file set Atlas covers for it, which is not
always every `.sql` file:

| Layout | Covered by `atlas.sum` |
| --- | --- |
| `atlas`, `goose`, `dbmate`, `liquibase` | every top-level `*.sql` |
| `golang-migrate` | every top-level `*.up.sql` — the down file is never covered |
| `flyway` | `V`/`B`/`R` files anywhere in the tree; `U` undo files and everything a baseline squashes are dropped |

So editing a golang-migrate down file or a Flyway undo file is invisible to the
check, exactly as it is in Atlas. A directory that carries no `atlas.sum` and
whose covered set is empty — a golang-migrate directory holding only a down
file, say — is not a checksum error and is not refused.

:::caution[A **directory** whose name matches is covered too, and cannot be hashed]
Membership in the covered set is decided by the name, so a *directory* called
`weird.sql` is a member for `atlas`, `goose`, `dbmate` and `liquibase`, and one
called `weird.up.sql` is a member for `golang-migrate`. Reading it fails, so
there is no sum to write and no sum to verify: `hash`, `validate`, `apply`,
`status`, `set`, `lint` and `import` all refuse the directory and exit `1`,
matching Atlas. Rename the directory or move it out of the migrations
directory.

```text
$ ptah-compat migrate hash --dir 'file://migrations?format=goose'
Error: read file "weird.sql": is a directory, not a migration file; rename it or move it out of the migration directory
```

**This is a behavior change.** Releases up to v0.1.2 skipped such a directory
and wrote an `atlas.sum` over the remaining files — a file Atlas itself then
refused to read, so the directory looked hashed here and was rejected there
([#991](https://github.com/stokaro/ptah/issues/991)). A project with a
legitimately named `*.sql` directory that hashed cleanly before will now be
refused.

`flyway` is exempt on both tools, and not by special-casing: Atlas walks a
Flyway tree instead of globbing it, so a directory is a node it descends into
and never reads. A migration nested inside one is covered as usual.
:::

For every layout, "not covered by `atlas.sum`" also means "not executed": the
set `migrate apply` runs is the set the checksum it verified covers. Flyway was
the exception until [#982](https://github.com/stokaro/ptah/issues/982) — its
importer selected a wider set than Atlas hashes, so a superseded baseline or a
lowercase-prefixed file could run SQL no checksum protected. The importer and
the hasher now share one selection rule, so that class of gap cannot reopen
without a failing test.

:::note[Flyway baselines: Ptah refuses where Atlas decides for you]
A Flyway `B` file is a squash — it restates the schema the migrations it
supersedes built. Atlas CE decides what to do with one by comparing the version
token as a **string** against the highest version already recorded, and gives
three different answers to that one class of file. Measured against Atlas CE
v1.3.0, adding a baseline to a directory that has already been applied:

| Applied | Baseline added | Atlas CE |
| --- | --- | --- |
| `V2` | `B10__base.sql` | `No migration files to execute`, exit 0, table never created |
| `V1`, `V2`, `V3` | `B2.5__base.sql` | exit 1, `migration file B2.5__base.sql was added out of order` |
| `V2` | `B3__base.sql` | executes the baseline, exit 0 |

The first row is the one that costs you something. Nothing on stdout, nothing on
stderr and no `migrate status` line says the file was skipped — `migrate status`
reports `OK` with `Pending Files: 0` on a database missing the object — and once
the squash retires the superseded files from `atlas.sum`, nothing can re-derive
it. A production database and a fresh one built from the same directory diverge
permanently, both reported healthy.

`ptah-compat` reproduces neither that silence nor its own former behavior of
running the baseline regardless
([#1003](https://github.com/stokaro/ptah/issues/1003)). `migrate apply` refuses,
names the file, and stops before anything executes when the baseline cannot be
read as a forward migration for the target database — that is, when an
already-applied migration is still covered by the directory, or when a migration
carrying the baseline's own version has already been applied. Two paths are
deliberately left open:

- A database with **no recorded history** still runs the baseline. That is the
  fresh-install path a converted Flyway directory exists for.
- `--exec-order=non-linear` executes the baseline against a database that does
  have history, which is what the refusal points at. So does
  `--exec-order=linear-skip`: the refusal comes from the baseline check rather
  than from the linear guard, and that check stands aside whenever the operator
  has named an execution order explicitly.

Atlas CE follows only halfway there. For a baseline it refuses as out of order
it does the same thing — `B2.5__base.sql` on a database at `V1`, `V2`, `V3` is
exit 1 without the flag and `Migrating to version 2.5 from 3` with it. For a
baseline it *skips silently*, the first row of the table above, neither flag
changes anything: `V2` plus `B10__base.sql` still prints
`No migration files to execute` at exit 0 under `--exec-order=non-linear` and
under `--exec-order=linear-skip`. `ptah-compat` runs the baseline in both, which
is what makes the flag a way forward rather than a second dead end. Both tools
exit 0 either way.

A baseline that squashes away the whole recorded history still runs on both
tools — `B3__base.sql` on a database at `V2`, `B2__base.sql` on one at `V10` —
and both execute its SQL rather than merely recording it, so a baseline
restating DDL that is already there fails loudly on both. The exception is a
baseline whose own version is one the database has already applied
(`B2__base.sql` on a database at `V1`, `V2`): Atlas CE skips that one silently,
and `ptah-compat` refuses it under the second half of the rule above.

That last comparison is on the version **token**, the way Atlas CE makes it, and
not on the number the token parses to. Zero padding is ordinary Flyway practice
and does not trigger the refusal: `V01`, `V02` plus `B2__base.sql` is a
directory both tools apply — Atlas CE reports it as `Migrating to version 2 from
02`.
:::

:::danger[Upgrading a Flyway directory applied by Ptah v0.1.0–v0.1.2]
Converging the importer changed the Atlas version each Flyway file converts
to, and that version is the key `atlas_schema_revisions` stores. A database
migrated through `?format=flyway` by any of those releases therefore records
migrations under versions this release matches to no file, so **every migration
in the directory reads as pending**.

`ptah-compat migrate apply` refuses such a database before executing anything
and prints the `UPDATE` statements that migrate the recorded versions forward:

```text
error: this database was migrated by a Ptah build older than the one that fixed
stokaro/ptah#982 ... 2 already-applied migration(s) would run a second time and
nothing has been applied

recorded version -> version this build uses:
  10000                -> 4611686018427469511  V1__init.sql
  20000                -> 4611686018427510315  V2__seed.sql

to adopt the new encoding, migrate the recorded versions forward and re-run:
  UPDATE atlas_schema_revisions SET version = '4611686018427469511' WHERE version = '10000';
  UPDATE atlas_schema_revisions SET version = '4611686018427510315' WHERE version = '20000';
```

Run them against the schema `--revisions-schema` selects, then re-run the
apply. Rewriting the version column is enough on its own: the recorded hash
covers the converted SQL body, which this change does not touch.

`--baseline` refuses once the revision table is non-empty, so the rewrite is the
route for this case. Both encodings are Ptah's own, which is what makes moving
the version column sufficient.

Without the refusal this is not reliably loud: re-running a `CREATE TABLE`
fails and leaves a dirty revision, but re-running a backfill or a seed
succeeds, exits 0, and duplicates the rows.
:::

:::danger[A revision table written by another Atlas implementation]
The two implementations record a **converted** Flyway migration under different
versions. Atlas CE identifies a migration by an opaque version string and stores
the Flyway token verbatim; Ptah's migrator identifies one by an `int64`, so the
importer projects the token onto a number. Measured on Atlas CE v1.3.0 against
sqlite, on a directory holding `V1__a.sql` and `V2__b.sql`:

| Applied by | Versions recorded in `atlas_schema_revisions` |
| --- | --- |
| Atlas CE | `1`, `2` |
| `ptah-compat` | `4611686018427469511`, `4611686018427510315` |

One direction is harmless. Atlas CE reads a table `ptah-compat` wrote as already
ahead of the directory and prints `No migration files to execute` at exit 0. The
other is not: every converted file matches no row, the whole directory reads as
pending, and the migrations run a second time.

That second run is not reliably loud either. A `CREATE TABLE` fails and strands
a dirty revision, but `CREATE TABLE IF NOT EXISTS` followed by a seed re-runs at
exit 0 and inserts the row twice, with nothing in the exit status, on stdout, or
in `migrate status` saying so.

`migrate apply` — and `--dry-run`, which previewed the same re-run — refuses that
database before executing anything
([#1100](https://github.com/stokaro/ptah/issues/1100)):

```text
error: this database records converted Flyway migrations under their SOURCE
version token, which is how another Atlas implementation identifies them ...
2 already-applied migration(s) read as pending here and would run a second
time. Nothing has been applied

recorded version -> version this build uses:
  1                    -> 4611686018427469511  V1__init.sql
  2                    -> 4611686018427510315  V2__seed.sql
```

Two ways forward, both measured. They are **alternatives, not steps** — taking
the second closes the first:

- Keep applying that database with the implementation that wrote its revision
  table. It reads its own versions and is unaffected.
- Adopt the versions this build uses. The refusal prints
  `migrate set <head version>`; run it with the same `--dir` and `--url`.

`migrate set` records this build's versions and does **not** remove the source
tool's, so the revision table then carries both spellings. Measured on
`V1__a.sql` and `V2__b.sql` recorded by Atlas CE as `1` and `2`: right after the
route, Atlas CE v1.3.0 still prints `No migration files to execute` at exit 0 —
but add `V3__c.sql` and it prints
`migration file V3__c.sql was added out of order` at exit 1, while `ptah-compat`
applies it at exit 0. The controls separate that from the added file: the same
three-file directory on a fresh database, and on a database carrying only CE's
`1`,`2`, both apply at exit 0 on Atlas CE. So the switch is one way, and the
refusal says so rather than calling the result up to date.

The second route is withdrawn, by name, on the two shapes where `migrate set`
is not a no-op. It moves the database to exactly the version given, which both
**removes** the revisions above it and **records** every covered migration below
it, run or not:

- A converted baseline lands in a low band, so a directory whose head is a
  baseline can have real revisions above it. The refusal names the rows the
  command would have deleted.
- The head is the largest version among the migrations the other implementation
  *ran*, and the covered migrations below it need not all be among them.
  Measured on `V1__g1.sql` and `V3__g3.sql` recorded as `1` and `3` with
  `V2__g2.sql` added afterwards: `migrate set 4611686018427551119` reports
  `(3 set)`, the next apply prints `No migration files to execute.` at exit 0,
  and table `g2` is absent with its version recorded — it can never run again.
  The refusal names that file and the version the command would assert, so
  nothing is lost by following the printed instruction.

A baseline added to a directory another implementation has already applied is
covered by the same refusal, and it is not the same defect: nothing re-runs, a
squashed schema executes on top of the history it squashes. Measured, Atlas CE
prints `No migration files to execute` there while `ptah-compat` created the
table.

Rewriting the version column by hand is **not** enough here, unlike the upgrade
above. The two implementations also record the migration checksum differently —
a base64 `h1` digest against a hex SHA-256 — so a bare version rewrite fails the
next apply with `checksum mismatch`. `migrate set` writes the whole revision row.

`migrate status` is deliberately left alone. Its counts run over three different
sets on a mismatched revision table, so they can sum above the total, and
measured, Atlas CE reports the equivalent input the same way.

Only Flyway is affected, measured across all five converted layouts. Goose
records `00001` where Ptah records `1`, and those meet when the revision reader
parses them; golang-migrate, dbmate and liquibase record identical versions on
both tools.
:::

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
migrations/0000000003_drop_users.up.sql:1 [error] DS101: DROP TABLE permanently deletes table users and every row in it; take a verified backup first and consider a rename-and-retire window instead (table dropped)

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

### Configure rules with `.ptah-lint.yaml`

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
- `severity` accepts `warning` or `error` and replaces the rule's default
  severity on its findings — the level that `--fail-on` and the apply-time
  destructive gate below evaluate. Any other value fails config parsing
  (exit `2`).
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

### Suppress a single statement inline

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

Local migration commands capture the migration directory before database
connection, checksum verification, provider registration, and destructive
linting. `up`, `down`, `status`, `lint`, and `set` therefore use the same
immutable SQL and metadata bytes throughout one invocation. Each command
compares two captures and aborts only when the observed captures differ.

This best-effort check cannot defeat coordinated writers or ABA changes that
restore the original bytes before the next observation. Hostile writers
require trusted immutable input, manifest or process controls, or
filesystem-level snapshots. Relative CLI directories are rooted at the working
directory and symlink escapes are rejected, while explicit absolute paths
remain supported.

A contained relative `atlas.hcl` migration directory remains bound to the
project directory handle opened for config evaluation until capture completes;
replacing the project pathname does not retarget it. Parent-relative project
directories are external compatibility paths and do not inherit that root.

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
exits non-zero. Checks are rejected under `--tx-mode all` on a real apply (a
pooled read cannot see the batch's uncommitted state), and
`ptah migrations up --skip-checks` is an emergency bypass. On the
Atlas-compatible surface that bypass is spelled `PTAH_SKIP_CHECKS=1`, because
Atlas registers no `--skip-checks` on `migrate apply` and `ptah-compat` adds no
flags Atlas does not have. This is the open, local half of Atlas Pro's
pre-migration checks; the Cloud approval-policy half is intentionally out of
scope.

### Checks in a dry run

A check is a read, and a dry run intercepts only writes. Evaluating every
check in a preview would therefore ask each migration's guard about state that
only exists once its predecessors apply — state the dry run has, by
construction, refused to produce. The answer would be a fact about the preview,
not about the migrations.

So a dry run evaluates a migration's assertions only where the state it
observes is the state a real apply would evaluate them against: **the first
migration executed in the run**. That is a position in the run, not a version
and not a place in the directory — a migration sitting second in its directory
is first in the run once its predecessor is applied, and its checks are
evaluated normally from then on.

Every check is still parsed and statically validated wherever it sits. A
malformed `-- +ptah check` directive, or an assertion that is not a single
read-only `SELECT`, is decided by its text alone and is reported in a dry run
exactly as on a real apply. Only the database evaluation is deferred, and the
run names what it deferred on stderr:

```text
Deferred pre-migration checks for 1 migration (20260101000002): a dry run does
not create the state they assert on, so they are evaluated on apply.
```

A dry run under `--tx-mode all` follows the same rule rather than refusing the
directory: it opens no batch transaction, so the uncommitted-state problem that
motivates the refusal on a real apply does not arise.

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
