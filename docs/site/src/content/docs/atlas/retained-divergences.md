---
title: Compatibility differences
description: Cases where ptah-compat deliberately differs from the pinned Atlas community binary, with the measurement and current policy for each case.
type: status
audience:
  - "atlas-migrator"
  - "evaluator"
readerQuestion: "Where does ptah-compat deliberately differ from the pinned Atlas community binary?"
goal: "Evaluate each measured compatibility difference and its current policy."
sourceOfTruth:
  - "cmd/atlas"
  - "internal/atlascompatpolicy"
generated: false
lastVerified: "2026-08-30"
evidence:
  - "cmd/atlas/compat_1241_retained_divergence_test.go"
  - "stokaro/ptah#1241"
searchAliases:
  - "retained divergences"
overlaps: []
disposition: keep
---

The drop-in rule has two directions and they are not symmetric. `ptah-compat`
must never exit `0` where the pinned Atlas community binary v1.3.0 exits `1`;
that direction is a correctness failure and there are no entries for it. The
other direction — `ptah-compat` exits `1` where that binary exits `0` — is a
usability failure for a drop-in replacement, and it is allowed only where the
refusal is more useful than the acceptance.

Each retained case records what both binaries did, so a reader can evaluate the
decision rather than only the outcome. The
one-line status per capability is on the [Feature matrix](../feature-matrix/).
Most entries came out of
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241); the project
migration-directory boundary is tracked in
[`stokaro/ptah#1118`](https://github.com/stokaro/ptah/issues/1118).

## What parity means, and what it does not

The direction stated above — never exit `0` where the pinned binary exits `1` —
is the first of two commitments. The second is that a defect is not reproduced
for the sake of being identical. Where a measured behavior loses something the
author asked for, `ptah-compat` does the better thing and records the difference
here.

One case shows the second commitment on its own, and it is not an entry below
because nothing is refused. A migration can carry `-- atlas:txmode none`
directly above its statement, with no blank line between the two. That directive
marks a statement that must run outside a transaction, such as
`CREATE INDEX CONCURRENTLY`. In that shape the community binary drops the
directive, so the statement runs inside a transaction and the migration fails
partway through. `ptah-compat` honors both shapes.

`ptah-compat` therefore has two policy profiles. The default retains every
implemented Atlas Pro-like and best-effort capability on the drop-in surface.
`PTAH_ATLAS_STRICT_COMPAT=1` selects a Community Edition oracle profile for
conformance runs, which exposes the pinned CE command and flag inventory and
refuses extended authored or live schema content before output or mutation.

These boundaries also remain active under
`PTAH_ATLAS_STRICT_COMPAT=1`. Strict mode limits the CE capability inventory;
it does not reproduce an acceptance that discards an argument, hides an edited
migration, or loses recoverable state.

The same rule protects richer live schemas. The pinned community inspector can
omit object kinds outside its edition, and its cleanup can leave or handle a
catalog differently from Ptah's complete cleanup. Strict mode therefore
refuses a live Pro-only object before `schema inspect`, `schema apply`,
`schema diff`, or `schema clean` emits output, compares incomplete states, or
mutates the target. Default `ptah-compat` keeps Ptah's complete modeled-object
behavior; the refusal exists only in the CE oracle profile.

The command-specific inventory remains read-only. Cleanup validates the
writer's complete destruction inventory, including PostgreSQL procedures,
aggregates, foreign tables, collations, and default privileges. Inspection,
apply planning, and database-backed or replayed schema- and migration-diff
sources query those catalog-only kinds in the same schema scope. A dependent
Pro-only object such as a trigger cannot disappear with a table merely because
the cleanup plan does not print it as a separate line.

Cleanup validation uses the writer's schema scope: a global
extension installed in another PostgreSQL schema does not block cleaning the
selected schema. A sequence backing a `SERIAL` or identity column is likewise
not treated as a forbidden standalone sequence, because it rides with the
table that owns it. A selector cannot split that ownership: selecting the
sequence without its table, or excluding it while the table remains selected,
is refused before mutation.

`PTAH_ALLOW_NONINTERACTIVE_EDIT=1` remains available in strict mode. It permits
an already-configured scripted editor to run without a terminal; it does not
add an editor, command, flag, or migration semantic, so it is retained as an
execution-safety control rather than classified as a Pro capability.

The same fail-closed boundary covers authored extensions. Strict schema
workflows refuse YAML sources and a `schema apply` lint policy that the CE path
cannot enforce. Commands that execute, convert, or replay migration bodies
refuse Atlas txtar, Ptah directives, and SQL templates; a bare or unknown
`-- +ptah` directive marker is refused rather than ignored. Checksum-only reads
preserve those bytes. Default mode retains and executes the extensions. The
strict profile never turns an authored safety contract into an ignored comment
or configuration block merely to copy an edition limit.

The entries from here to [What holds the #1241
entries](#what-holds-the-1241-entries) were measured on
2026-08-09 against that binary, except where a section gives its own date, each
exit status read on its own line rather than through a pipe. Where a fixture
needed a hashed migration directory, that binary authored and hashed it, so no
checksum quoted in them was computed by Ptah. The sections after it carry their
own measurement conditions.

## The entries

| Entry | `ptah-compat` | Pinned community binary v1.3.0 |
| --- | --- | --- |
| [A trailing positional argument](#a-trailing-positional-argument) | refuses one on a verb that defines none, naming the flag the value belongs to | accepts it and discards it without a word |
| [An edited already-applied migration file](#an-edited-already-applied-migration-file) | compares each applied migration against the checksum recorded when it ran, and refuses on a mismatch | records the same value and never compares it |
| [A prefix migration inserted below the oldest applied revision](#a-prefix-migration-inserted-below-the-oldest-applied-revision) | refuses at the default `--exec-order linear` and names both ways forward | exits `0` with `No migration files to execute`, saying nothing about the file it passed over |
| [A dirty revision left by a non-transactional body](#a-dirty-revision-left-by-a-non-transactional-body) | discards the revision row only where it observed the rollback, so `--tx-mode none` needs `--allow-dirty` | keeps the row in both cases and resumes from it |
| [A project migration directory outside its root](#a-project-migration-directory-outside-its-root) | confines a project `migration.dir` to the directory holding `atlas.hcl` | writes the migration and `atlas.sum` wherever the project file points |
| [`sql()` inside `check.expr` and `index.where`](#sql-inside-checkexpr-and-indexwhere) | reduces `sql()` to the SQL it carries, in both attributes | refuses both with `incorrect type raw` |
| [Revision row for a migration whose body failed](#revision-row-for-a-migration-whose-body-failed) | matches the binary; native `ptah migrations up` keeps a durable failed row | writes no revision row when the body's transaction rolled back |
| [Failed rollback state](#failed-rollback-state) | marks direction and partial progress in the existing Atlas columns | hides the failed-down state |
| [Dirty retry verifies committed statements](#dirty-retry-verifies-committed-statements) | proves every skipped statement has unchanged source text before resuming | resumes from `applied + 1` by statement index |
| [Recorded revision `error` text on a failed migration](#recorded-revision-error-text-on-a-failed-migration) | records the SQLite driver's own wording | records its own driver's wording for the same condition |
| [Pre-migration checks in a dry run](#pre-migration-checks-in-a-dry-run) | exits `1` where the guard's verdict is knowable and negative | implements no check semantics, so a dry run exits `0` for every checked directory |
| [`atlas.hcl` `file()` confinement](#atlashcl-file-confinement) | reads `file()` only from the directory holding that `atlas.hcl` | reads an absolute, parent-traversal, or link-escaping path |
| [A migration that would be recorded over an invalid index](#a-migration-that-would-be-recorded-over-an-invalid-index) | refuses while `pg_index` reports the index unusable, and names `REINDEX` | applies the migration and records it over the unusable index |
| [`schema inspect --include`](#schema-inspect---include) | positively selects top-level resources through the apply and diff engine | does not register the flag: `Error: unknown flag: --include` |
| [Exclude field selectors](#exclude-field-selectors) | honors the suffixes it can carry out and refuses the rest | accepts every such suffix and honors none of them |
| [Leading schema type selector](#leading-schema-type-selector) | keeps the literal answer on every schema source | gives source-dependent answers, leaving the named table in a file diff's plan |

## A trailing positional argument

**Type.** Deliberate divergence

**Current boundary.** `ptah-compat` refuses a positional argument on verbs that
define none, and names the flag the value belongs to. That binary accepts one
and discards it without a word.

Measured on SQLite, one hashed migration directory shared by every row:

| command, with `trailingarg` appended | pinned community binary v1.3.0 | `ptah-compat` |
| --- | --- | --- |
| `migrate status --dir file://migrations --url sqlite://s.db` | exit `0`, status printed | exit `1`, `name the migration directory with --dir` |
| `migrate validate --dir file://migrations` | exit `0` | exit `1`, same wording |
| `migrate lint --dir file://migrations --dev-url … --latest 1` | exit `0`, report printed | exit `1`, same wording |
| `schema inspect --url sqlite://s.db` | exit `0`, HCL printed | exit `1`, `name the database with -u/--url` |

The argument that keeps the refusal is that on every one of these verbs the
discarded value is the one an operator meant to pass to a flag. `migrate status
migrations` and `schema inspect sqlite://s.db` are both the natural typo for
the correct command, and both are exit `0` on that binary against the wrong
target: the first reports on `./migrations` because the flag defaulted, not
because the positional was read. A tool that answers a question the operator
did not ask, with no sign that it did so, is worse than one that refuses. This
is the reasoning the `--dir` defaults landed under, where a default that
silently swallowed a typo would have migrated the wrong directory at exit `0`.

The refusal is not carried behind an environment variable. A variable that
restored the acceptance would have to default to the permissive side. Feature
toggles instead opt in to more permissive behavior so that a typo lands on the
strict default. The separate `PTAH_ATLAS_STRICT_COMPAT` policy selector narrows
the command inventory for CE oracle runs and does not relax this refusal.

`migrate hash` takes no positional either and refuses through the same helper,
`cmdutil.NoPositionalArgsHint`. That cell is measured now, and it lands on this
same argument. Read 2026-08-17 on a hashed SQLite directory, exit status taken
from an unpiped invocation:

| argv | Atlas CE v1.3.0 | Ptah |
| --- | --- | --- |
| `migrate hash --dir file://mig extra` | `0`, zero bytes of output | `1`, `unexpected positional arguments ["extra"]: name the migration directory with --dir` |

The zero bytes are the finding rather than the exit status. That binary does not
report the extra word, and it does the work anyway: a migration added before the
run is hashed into `atlas.sum` exactly as if the positional had not been typed.
So the operator who meant `--dir extra` and dropped the flag gets a rewritten
checksum file for a directory they did not name, at exit `0`, with nothing
printed (stokaro/ptah#1623).

## An edited already-applied migration file

**Type.** Deliberate divergence

**Current boundary.** `ptah-compat migrate apply` compares each applied
migration against the checksum recorded when it ran, and refuses when they
disagree. That binary records the same value and does not compare it, so an
edited applied file is a no-op there.

Measured on SQLite and again on PostgreSQL 17.10, identical on both. Two
migration directories sharing the version `20240101000000` and differing only
in the body — `CREATE TABLE t1 (id integer primary key)` against
`CREATE TABLE t1 (id integer primary key, note text)`:

| step | pinned community binary v1.3.0 | `ptah-compat` |
| --- | --- | --- |
| apply the first directory | exit `0`, table created | exit `0`, table created |
| apply the second directory | exit `0`, `No migration files to execute` | exit `1`, `checksum mismatch` |

The edit is real: the file that ran is not the file on disk any more. Treating
that as a no-op means the migration directory stops describing the database it
was applied to, and the difference surfaces later as a diff nobody can explain.
The refusal names the version and both checksums, and `--allow-dirty` is not
involved.

## A prefix migration inserted below the oldest applied revision

**Type.** Deliberate divergence

**Current boundary.** This entry covers a prefix migration that sorts below
every applied revision. `ptah-compat migrate apply` refuses at its default
`--exec-order linear` and names both ways forward. `--exec-order linear-skip`
leaves the insertion unapplied; `--exec-order non-linear` applies it. Those are
that binary's own flag and its own three values, not a Ptah addition.

An insertion between two applied revisions is a different observable state.
Measured on SQLite and PostgreSQL 17.10 on 2026-08-10, both binaries refuse
that interval insertion at the default order and apply it under `non-linear`.
The lower applied revision is the state that distinguishes that parity cell
from the prefix divergence below.

Measured on SQLite on 2026-08-12. Both directories were authored and hashed by
that binary through `migrate import`, so no checksum here was computed by Ptah.
`dirA` holds only the later migration; `dirB` prefixes it with an earlier one
and holds a byte-identical copy of the later one. `diff` between the two copies
exits `0`.

| step | pinned community binary v1.3.0 | `ptah-compat` |
| --- | --- | --- |
| apply `dirA` | exit `0`, later migration applied | exit `0`, later migration applied |
| apply `dirB`, default order | exit `0`, `No migration files to execute` | exit `1`, names the version and both remedies |
| apply `dirB`, `--exec-order linear-skip` | — | exit `0`, insertion left unapplied |
| apply `dirB`, `--exec-order non-linear` | — | exit `0`, insertion applied |

The second row is the entry. That binary's default order is `linear` by its own
`--help`, and on that row it exits `0` while never applying the inserted
migration: the table that migration creates is absent from the catalog
afterwards, and only the later revision is recorded. It prints nothing about
the file it passed over. An operator who adds a migration to a directory,
runs apply, and reads exit `0` has been told the directory is applied when one
of its migrations never ran and never will.

That is the argument for the refusal, and it is not a strictness preference.
Reproducing an exit `0` that discards an operator's migration silently is the
kind of defect the parity rule declines to copy. The refusal names the version
it will not apply and both flags that resolve it, so the operator chooses
between skipping and applying rather than having the choice made and not
reported.

Nothing is removed by refusing it as the default. The third row is the pinned
binary's own outcome, reachable on request through its own flag, and it is
pinned by a test so it stays reachable. The fourth row is the outcome an
operator who inserted the migration on purpose usually wants, which that
binary's default order does not offer at all.

One thing here changed rather than being retained. The refusal used to be
reported as a checksum mismatch on the later migration, whose bytes had not
changed: the per-file entry in an integrity file is chained over the entries
before it, so inserting anything earlier changes the recorded value of every
applied migration above it. The value is an ordering key, not a content
identity, and the message claimed otherwise. That is fixed; the diagnostic now
describes the ordering fact it actually found. The genuine content-change case
keeps the checksum wording and is the section above.

## A dirty revision left by a non-transactional body

**Type.** Deliberate divergence

**Current boundary.** When a migration body fails, `ptah-compat migrate apply`
discards the revision row it wrote if — and only if — this invocation observed
the body's transaction roll back with nothing committed. Under `--tx-mode none`
there is no transaction to observe, so the row stays and the next apply refuses
until `--allow-dirty` is passed. That binary keeps its row in both cases and
resumes from it.

Measured on SQLite and again on PostgreSQL 17.10, identical on both. Two
migrations; the second creates a unique index that fails against duplicate rows
inserted between the two applies; the duplicate is then deleted and the apply
repeated. No migration file is edited at any point, so the integrity file stays
valid throughout:

| step | pinned community binary v1.3.0 | `ptah-compat` |
| --- | --- | --- |
| apply the first migration | exit `0` | exit `0` |
| apply the second, duplicates present | exit `1` | exit `1` |
| repair the data, apply again, default `--tx-mode file` | exit `0` | exit `0` |
| repair the data, apply again, `--tx-mode none` | exit `0`, resumes | exit `1`, `is dirty: state=failed applied=0/1` |

The third row is the one that matters most and the two agree on it. The
fix-and-rerun flow that #1241 called permanently wedged recovers under the
default transaction mode, because a confirmed rollback that committed nothing
is discarded.

The fourth row is retained. `applied=0` is not proof that nothing happened when
no transaction wrapped the body: `--tx-mode none` exists so that statements
which cannot run inside a transaction can run, and on PostgreSQL a failed
`CREATE INDEX CONCURRENTLY` leaves an invalid index behind while the statement
counts as not applied. Ptah already refuses to record a migration over such an
index. Discarding the row automatically in that mode would hand the next run a
clean slate over a database that is not clean. `--allow-dirty` remains the way
through while the same migration body remains in the directory, and the
operator, unlike the tool, can look first. If that source file is removed,
Ptah refuses the now-unowned dirty exact identity even with `--allow-dirty`;
there is no body or verified prefix to resume.

## A project migration directory outside its root

**Type.** Deliberate divergence

**Current boundary.** A `migration.dir` declared in `atlas.hcl` must remain
inside the directory containing that project file after symbolic-link
resolution. The rule applies to relative and absolute values. An explicit CLI
`--dir` remains operator-owned: a directly named absolute directory keeps its
normal CLI behavior.

Measured on 2026-08-12 with `migrate diff`, a local SQL desired schema, and a
SQLite dev database:

| project `migration.dir` | pinned community binary v1.3.0 | `ptah-compat` |
| --- | --- | --- |
| `file://migrations` | exit `0`, writes migration and `atlas.sum` inside the project | exit `0`, same artifacts inside the project |
| `file://../outside` | exit `0`, writes migration and `atlas.sum` outside the project | exit `1`, `outside allowed root`, outside directory untouched |
| `file:///absolute/outside` | exit `0`, writes migration and `atlas.sum` outside the project | exit `1`, `outside allowed root`, outside directory untouched |

The project file is repository-controlled input. Letting it select an arbitrary
write destination means a configuration change in a pull request can publish a
migration and replace an `atlas.sum` anywhere the process can write. That is not
equivalent to an operator explicitly naming an absolute `--dir` at the command
line. Binding the project value to the already-open project handle preserves
the useful explicit CLI capability while refusing the implicit external write.

The refusal also closes the spelling hole. Previously a contained relative
value carried the project root, while parent-relative and absolute spellings
silently dropped the root and fell through to unbounded CLI resolution. The
destination now determines the answer: every project-owned value is checked
against the same root before output or mutation.

## What holds the #1241 entries

The issue #1241 entries above are pinned by a test in
`cmd/atlas/compat_1241_retained_divergence_test.go`, and the trailing-positional
rows for `migrate status`, `migrate validate` and `schema inspect` are pinned in
`cmd/atlas/compat_overstrict_test.go`. The same focused file also pins all three
orders of the prefix insertion: the default refuses, `--exec-order
non-linear` applies and remains idempotent, and `--exec-order linear-skip`
leaves the insertion unapplied and stays that way on a repeat run. The last of
those is the one that keeps the entry honest — it is the claim that the pinned
binary's own outcome is still reachable, so it is guarded rather than asserted.

The project-root boundary is pinned through the real `ptah-compat` process in
`integration/atlas_project_migration_dir_confinement_e2e_test.go` and its Unix
symbolic-link companion. The tests prove both outside spellings and an escaping
symbolic link fail without artifacts, relative and absolute inside paths and a
linked project root succeed, and an explicit absolute CLI `--dir` remains
reachable.

Every case above is SQLite-measured; the two noted as also measured on
PostgreSQL 17.10 were re-run against a live server. The rest carry the
SQLite-only caveat that
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241) declares.

The entries below this section come from other issues. Each names its own
measurement conditions, its own engine, and the test or tracking issue that
holds it.

## `sql()` inside `check.expr` and `index.where`

**Type.** Deliberate divergence, in the direction of accepting more

**Current boundary.** Ptah reduces a `sql()` call to the SQL it carries
everywhere an attribute is read as text, including `check.expr` and
`index.where`. The pinned community binary v1.3.0 refuses the same two with
`incorrect type raw`.

**Why it stays.** `sql()` exists so a schema file can carry an expression the
HCL grammar has no spelling for, and a CHECK body is the clearest case of one.
Refusing it in the two attributes that most need it would make the function
useless where it is most useful, to match a refusal that costs the operator a
capability rather than protecting them from anything. Nothing a document writes
this way renders differently: the call reduces to its argument text, which is
what an unquoted expression would have been.

The reduction is not optional. It landed because the fallback below it hands an
attribute's SOURCE TEXT to the renderer, which is how `CHECK (sql("n > 0"))`
reached a plan verbatim.

## Revision row for a migration whose body failed

**Type.** Compatibility behavior with a native safety difference

**Current boundary.** When a migration body fails inside a transaction and
rollback succeeds, the pinned community binary v1.3.0 writes no revision row.
`ptah-compat` reaches the same end state and retries the whole file on the next
apply. Native `ptah migrations up` keeps a durable failed row for status and
repair workflows.

Measured across the effective transaction-mode matrix:

```text
effective file transaction   binary 0 rows   ptah-compat 0 rows   native 1 row
effective all transaction    binary 0 rows   ptah-compat 0 rows   native 1 row
effective no transaction     binary 1 row    ptah-compat 1 row    native 1 row
```

The compatibility cleanup is fail-closed. It removes a zero-progress row only
after `Rollback` returns success. A rollback error, commit error, unknown
statement outcome, or any committed statement keeps the row. Those states
still require explicit recovery, so parity does not erase evidence when the
database outcome is uncertain.

**Evidence.** [`stokaro/ptah#1196`](https://github.com/stokaro/ptah/issues/1196),
[`stokaro/ptah#1333`](https://github.com/stokaro/ptah/issues/1333)

## Failed rollback state

**Type.** Deliberate divergence, in the direction of recording more

**Current boundary.** `ptah-compat migrate down` retains the Atlas
revision-table schema and does not reproduce the hidden failed-down state.
It marks rollback direction with the existing `operator_version` field and
makes a partial rollback recoverable through the existing progress and error
fields. A successful down still deletes the row. An Atlas reader can inspect
the same table, but may report a Ptah-recorded failed rollback differently from
one Atlas itself hid.

Native `ptah migrations down` records that state in both revision-table
formats. The row is marked failed with `error=<message>`, `direction=down`, and
`applied` set to the number of down statements that completed — `0` when the
first one fails or the dialect rolled the body back. Status therefore reports
the dirty state, repair has a row to act on, and a later up refuses to stack
work on the unfinished rollback.

Repair follows the recorded direction. `--resume-from` runs the remaining
*down* statements and removes the revision once the rollback finishes, and a
rollback that already committed a statement is refused rather than recorded
applied unless `--force` says the schema was restored. See
[Roll back migrations](../../versioned/rollback/).

The pinned community binary registers `migrate down` as a community-abort stub,
so the capability is unreachable there. Where the verb does run, a failed down
is not recorded: measured, after a down whose second statement fails, the body
is rolled back, the revision row still reads `applied=2, total=2, error=''`,
the status report names the version as applied, and a retry after repairing the
down file succeeds and deletes the row.

**Evidence.** [`stokaro/ptah#957`](https://github.com/stokaro/ptah/issues/957)

## Dirty retry verifies committed statements

**Type.** Deliberate safety divergence

**Current boundary.** The community binary resumes a dirty non-transactional
revision from `applied + 1` by statement index. Ptah first proves that every
statement it will skip has unchanged source text. Ptah-format rows store a
`partial:h1:` checksum for the committed prefix; Atlas-format rows use the
existing `partial_hashes` column. A changed prefix, malformed metadata, or
contradictory hash count is refused, as are negative progress counters,
`applied > total`, and a native `state=applied` row whose counters are
incomplete. Invalid metadata is rejected by revision listing, status, version,
and apply operations rather than being hidden as a clean row. A row carrying no
prefix metadata at all resumes only while its full-file hash still matches.

Editing only the unapplied suffix remains supported. If that retry changes from
`none` to `file` or `all` and its transaction rolls back, Ptah retains the
committed `applied` floor rather than making a later run replay SQL. Process
exit, context cancellation, or deadline while an autocommit statement is in
flight preserves the unknown-outcome marker.

This is stricter than the community binary in the safe direction: uncertain
recovery exits non-zero instead of skipping SQL on the strength of a stale
integer offset.

## Recorded revision `error` text on a failed migration

**Type.** Driver difference, not a behavior one

**Current boundary.** The Atlas revision table's `error` column records the
database's own message on both sides, but the two spell the same condition
differently because they use different SQLite drivers. On the same failing
migration:

```text
pinned community binary v1.3.0   no such table: missing_table
ptah-compat                      SQL logic error: no such table: missing_table (1)
```

The column carries the innermost error and nothing else — no Ptah-authored
prefix, and no repetition of the statement, which the adjacent `error_stmt`
column already holds in full. `error_stmt` matches byte for byte, terminating
semicolon included.

What is left is `modernc.org/sqlite`'s wording. Closing it would mean rewriting
driver messages per driver and per dialect to match a different driver's
phrasing, which trades a cosmetic difference for a table of string edits that
goes stale silently. The native revision format is unaffected either way: this
applies only where the revision table is Atlas-shaped, and Ptah's own surface
keeps the context it adds.

**Tracking.** [`stokaro/ptah#1196`](https://github.com/stokaro/ptah/issues/1196)

## Pre-migration checks in a dry run

**Type.** Deliberate divergence

**Current boundary.** The community binary implements no check semantics, so it
flattens the archive and runs `checks.sql` as an ordinary migration statement;
in a dry run it executes no SQL at all and exits `0` for every checked
directory.

`ptah-compat` exits `1` where the guard's verdict is knowable and negative:

- an assertion that is malformed or is not a read-only `SELECT`, decidable from
  the text alone;
- a failing assertion on the first migration executed in the run, decidable
  against the live database and confirmed by the real apply failing the same
  way;
- a checked directory under `--tx-mode all`, which the real apply refuses
  outright.

Matching the community binary on those inputs would make the preview report
success for a run that cannot succeed.

A failure that is an artifact of the preview rather than a finding — a later
migration's guard asking about state the dry run refused to create — is not one
of them. `ptah-compat` exits `0` there, which both matches the community binary
and matches what applying the directory does. See
[Checks in a dry run](../../versioned/integrity-and-safety/#checks-in-a-dry-run).

**Tracking.** [`stokaro/ptah#661`](https://github.com/stokaro/ptah/issues/661)

## `atlas.hcl` `file()` confinement

**Type.** Deliberate divergence

**Current boundary.** `ptah-compat` reads `file()` and `fileset()` inside an
`atlas.hcl` only from the directory holding that `atlas.hcl`. Three shapes are
refused that the pinned community binary reads: an absolute path, a
parent-traversal path, and a plain relative name that resolves out of the
directory through a symbolic link. The refusal names which of the three applies
and points at `getenv()` for passing a value in from outside.

Measured on the pinned v1.3.0 build: `file("/etc/passwd")` and
`file("../../../../etc/passwd")` in an `atlas.hcl` both exit `0` with the file
read, and the contents reach an observable place — a database URL, an error
message on standard error.

The reason is that an `atlas.hcl` is usually repository-controlled and `file()`
is evaluated before anything is applied. Without confinement, a config file
arriving in a pull request can read any file the process can and place the
contents somewhere a reader of the output can see. That is a different class of
cost from the footguns the drop-in rule was written for. The exit code is `1`
either way for a configuration that works, so nothing a project already runs
changes; the refusal names its reason.

Both halves are measured rather than remembered. The Ptah half is pinned by
`TestOraclePlacesOutsideFileContentsWhereTheCallerCanSeeThem` in
`config/projectconfig`; the community half is pinned by four `ce-gating`
scenarios in the conformance repository, so the day a community build starts
confining `file()` the gate goes red and this divergence can be retired.

**Tracking.** [`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042)

## A migration that would be recorded over an invalid index

**Type.** Deliberate divergence

**Current boundary.** On PostgreSQL, `ptah-compat migrate apply` refuses a
migration while an index that migration creates is reported unusable by
`pg_index` (`indisvalid` or `indisready` false). The pinned community binary
v1.3.0 applies it and records it.

Measured on PostgreSQL 17.10, identical fixture on both: a `members` table whose
duplicate rows made an earlier `CREATE UNIQUE INDEX CONCURRENTLY` fail, the
duplicates then removed, and one migration carrying `-- atlas:txmode none` and
the same `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS` statement.

| | pinned community binary v1.3.0 | `ptah-compat` |
| --- | --- | --- |
| `migrate apply --allow-dirty` | exit `0`, `-- ok`, 1 statement | exit `1`, names the index and `REINDEX` |
| `pg_index` afterwards | `indisvalid=f`, `indisready=f` | unchanged, nothing written |
| revision row afterwards | `20260808000001` applied 1/1, no error | no row |
| duplicate `INSERT` afterwards | accepted, 2 rows share the email | n/a |
| after `REINDEX INDEX CONCURRENTLY` | n/a | exit `0`, `indisvalid=t`, row recorded, duplicate rejected |

The leftover keeps the name, so `IF NOT EXISTS` skips it and the statement
reports success over an index that enforces nothing. Recording that as applied
means the tooling reports a constraint the database does not have, and nothing
will look again. Ptah also rejects another index or non-index relation that owns
the schema-level name. It resolves an unqualified drop and target through the
active `search_path`, permits cleanup only when that exact drop will run first
in this attempt, and positively rechecks the active transaction or connection
before recording the revision. A drop skipped by dirty-resume is not cleanup.

The divergence is stricter, not looser: `ptah-compat` exits `1` where the binary
exits `0`, never the reverse, so no invocation the binary refuses succeeds here.

**Tracking.** [`stokaro/ptah#1101`](https://github.com/stokaro/ptah/issues/1101)

## `schema inspect --include`

**Type.** Deliberate divergence

**Current boundary.** `ptah-compat schema inspect --include` positively selects
the top-level resources inspection keeps, through the same engine as
`schema apply` and `schema diff`: schema universe, then include selection, then
exclusion. The pinned community binary does not register the flag at all —
`schema inspect -u sqlite://app.db --include users` exits `1` with
`Error: unknown flag: --include`. Atlas registers it, and Ptah's behavior
diverges from it in two measured ways, both deliberate:

| Input | Atlas | Ptah |
| --- | --- | --- |
| `--include 't1'` | `t1` with its columns, plus `schema "main"` | same selection |
| `--include '*.t1'` | pattern is read at child depth: `t1` rendered without its columns and `t2` rendered as an empty shell, exit `0` | `*.t1` is the wildcard spelling of the qualified name `main.t1`, so `t1` is rendered whole and `t2` is dropped |
| `--include 'main.t1.*'` | `Error: too many parts in pattern: ["main" "main" "t1" "*"]`, exit `1` | rejected before any database is contacted: child resources ride along with their parent and cannot be selected on their own |

Ptah has no child-level include selection in either spelling, so it keeps whole
objects instead of emitting partial ones. It also refuses a selection that drops
a dependency of a selected object, where Atlas renders the reference anyway —
its `*.t1` output keeps `primary_key { columns = [column.id] }` on a table whose
`id` column the same output omits. The Atlas-side rows are the recorded
transcripts, so behavior beyond those inputs is not established here.

There is no `atlas.hcl` spelling of this selector. Atlas documents `exclude` but
no `include` attribute on the `env` block; the community binary accepts
`include = [...]` there only because it accepts any unknown env attribute. A run
with `not_a_real = [...]` is likewise accepted and prints the full schema. Ptah
accepts `env.include` under the same unknown-name rule, leaves it without
effect, and writes a location-aware warning to standard error.

**Tracking.** [`stokaro/ptah#933`](https://github.com/stokaro/ptah/issues/933)

## Exclude field selectors

**Type.** Deliberate divergence

**Current boundary.** A field selector is the `.field` suffix behind a
`[type=...]` selector: `--exclude '*[type=table].comment'` asks for the comment
of every table to be dropped while the tables themselves stay. The pinned Atlas
community binary accepts every such suffix and honors none of them. Measured on
PostgreSQL 16 with two commented tables across two schemas, all three of

```text
--exclude '*[type=table].comment'
--exclude 'public.*[type=table].comment'
--exclude '*[type=table].*'
```

are exit `0` there with output byte-identical to the same command without the
flag, comments included.

`ptah-compat` honors the ones it can carry out — `[type=extension].version`,
`.comment` on `table`, `view` and `materialized_view`, and `.*` for all of them
— and refuses the rest by name before a database is contacted. So the first and
third of those commands are exit `0` with the tables rendered and their comments
gone, and a suffix such as `.charset` is exit `1` naming the fields that would
have worked.

The reason not to copy accept-and-ignore is the one recorded for `file()` above:
an `--exclude` selector is a scoping instruction, and the reason to write one is
usually that the object must not be touched. Accepting one and silently not
carrying it out defeats that intent with no diagnostic, and on `schema apply`
and `schema diff` the same shape of miss reaches a `DROP`. Both directions here
are safe under the drop-in rule: honoring a selector subtracts more from a plan
rather than less, and refusing one exits `1` where that binary exits `0`, never
the reverse.

The second of those commands is decided by the pattern-depth rule rather than
the field rule, so its exit status follows the URL. On a schema-bound URL both
binaries refuse it — the schema slot is already filled by the connection. That
binary quotes the prefixed pattern,
`too many parts in pattern: "public.public.*[type=table].comment"`; Ptah quotes
the one that was typed. On a URL that names no schema the pattern is
realm-relative, `public` fills the schema slot itself, and both binaries accept
it. Measured on both URLs against the same PostgreSQL 17 database.

**Tracking.** [`stokaro/ptah#933`](https://github.com/stokaro/ptah/issues/933)

## Leading schema type selector

**Type.** Deliberate divergence

**Current boundary.** `--exclude '*[type=schema].*[type=table]'` means every
table inside every schema on every Ptah schema source. The leading schema glob
may be narrowed, as in `app[type=schema].*[type=table]`.

The pinned community binary v1.3.0 gives source-dependent answers. On a live
PostgreSQL database containing tables and enums in `public` and `app`, the
selector removes both tables and keeps both enums. On a SQLite file diff between
one table and the same schema plus a second table, it exits `0` but leaves the
second table's `CREATE TABLE` plan unchanged. Removing the selector from that
SQLite run produces byte-identical output; `*[type=table]` is the control that
does remove the plan.

Ptah keeps the literal PostgreSQL answer on both source kinds. Making the
selector a no-op only for a file diff would make one accepted scoping
instruction mean two things depending on its input. It would also report success
while leaving exactly the table the selector names in the migration plan. This
is a defect Ptah does not copy; the complete compatibility surface keeps the
coherent behavior rather than narrowing it to this community-binary result.

The integration contour pins the file-diff result in
`TestAtlasCompatLeadingSchemaTypeSelectorE2E`. The filter tests separately pin
the schema glob, the final resource type, and the surviving non-table objects.

**Tracking.** [`stokaro/ptah#933`](https://github.com/stokaro/ptah/issues/933)

## Not on this page

`--to file://../schema.sql` and `--dir file://../dir` used to be refused as
`outside allowed root` where that binary exits `0` (item 11 of that issue). They
are accepted now, and the divergence is closed rather than retained.

The refusal came from a containment that applied to relative CLI paths only: the
identical file named by an absolute path was accepted, and `migrate diff --to`
against it succeeded. A boundary that refuses one spelling of a path and accepts
another spelling of the same path contains nothing, so the refusal could not be
defended as a safety control — and it cost a behavior the community binary has.
`pathguard.ResolveCLIPath` therefore imposes no boundary at all now, and both
spellings of one destination answer identically
([`stokaro/ptah#1622`](https://github.com/stokaro/ptah/issues/1622)).

Containment did not disappear with it. `pathguard.ResolveWithinRoot` and
`OpenDirectoryWithinRoot` take an explicit root and bind every spelling against
it, which is what the project migration-directory confinement uses, and a
`write` directive in a `--format` template still cannot compute a filename that
leaves the root the operator chose.

Both `migrate hash` cells of that issue -- the trailing positional of item 13
and `--var` of item 12 -- were unread when this page was written, because the
sandbox those sweeps ran in refused any command containing that bare word. They
were read on 2026-08-17 in an environment that does not, and they split:

- the trailing positional is a divergence, argued under **Positional arguments
  a flag already names** above, where it joins the verbs that already refuse
  one;
- `--var` is parity. `migrate hash --dir file://mig --var x=1` exits `0` on
  both binaries with byte-identical output, which is none.

Neither is an unknown on this page any more (stokaro/ptah#1623).
