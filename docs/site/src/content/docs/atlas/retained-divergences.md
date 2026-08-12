---
title: Retained divergences
description: Cases where ptah-compat exits 1 and the pinned Atlas community binary exits 0, each with the measurement and the argument for keeping it.
---

The drop-in rule has two directions and they are not symmetric. `ptah-compat`
must never exit `0` where the pinned Atlas community binary v1.3.0 exits `1`;
that direction is a correctness failure and there are no entries for it. The
other direction — `ptah-compat` exits `1` where that binary exits `0` — is a
usability failure for a drop-in replacement, and it is allowed only where the
refusal is more useful than the acceptance.

This page holds those cases. Each one carries what both binaries did, so a
reader can argue with the decision rather than only with the outcome. The wider
gap register lives in [Comparison](../comparison/); these entries are the
subset that came out of
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241).

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

Every measurement below was taken on 2026-08-09 against that binary, each exit
status read on its own line rather than through a pipe. Where a fixture needed
a hashed migration directory, that binary authored and hashed it, so no
checksum quoted here was computed by Ptah.

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
`cmdutil.NoPositionalArgsHint`. No reading of that binary was taken for it: the
sandbox this sweep ran in refuses any command containing that bare word, and
the refusal was not scripted around.

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
through, and the operator, unlike the tool, can look first.

## What holds these

Each entry above is pinned by a test in
`cmd/atlas/compat_1241_retained_divergence_test.go`, and the trailing-positional
rows for `migrate status`, `migrate validate` and `schema inspect` are pinned in
`cmd/atlas/compat_overstrict_test.go`. The same focused file also pins the
resolved out-of-order insertion behavior: the default order refuses, while
`--exec-order non-linear` applies and remains idempotent.

Every case here is SQLite-measured; the two noted as also measured on
PostgreSQL 17.10 were re-run against a live server. The rest carry the
SQLite-only caveat that
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241) declares.
