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
restored the acceptance would have to default to the permissive side, and every
boolean `PTAH_*` variable opts in to the more permissive side so that a typo
lands on the strict default.

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

## An out-of-order insert, and a diagnostic that is not retained

**Type.** Product behavior, tracked rather than argued

Inserting a migration whose version sorts below an already applied one is
refused by `ptah-compat` and is exit `0` on that binary. The refusal is
defensible. The message it prints is not, and this entry exists so that
distinction is written down rather than assumed.

The per-file `h1` entry in an integrity file is chained over the entries before
it, not computed from the file alone. Two directories were authored and hashed
by that binary; one holds only `20240102000000_b.sql`, the other holds an
earlier `20240101000000_a.sql` and a byte-identical copy of that same file.
`diff` between the two copies reports no difference, and the two integrity
files still disagree about it:

```text
dirA:  20240102000000_b.sql h1:DYNKQS6GeTazipONZLq7+IhAl/67sJqipvGfoLz/fPU=
dirB:  20240102000000_b.sql h1:QeKXUEPrs5mM3XbCdSiRc6R/tUDmX4otsW0tB5L0Mmc=
```

A third directory built independently with the same two files in the same order
reproduces `dirB`'s pair exactly, which is the control that says this is
ordering rather than noise. Both binaries store the chained value in the
revision table's `hash` column and the stored strings are identical; only Ptah
compares it on the next apply.

So inserting any earlier migration changes the stored checksum of every applied
migration above it, and the refusal names a file whose bytes did not change.
Ptah already has a gate that describes the situation correctly — with the
checksum comparison mutated, the refusal becomes

```text
out-of-order pending migrations below current version 20240102000000:
[20240101000000] (use --exec-order=non-linear to apply or
--exec-order=linear-skip to ignore)
```

which is true and names the flag that resolves it. The checksum gate answers
first because `verifyAppliedMigrationChecksums` runs before `migrationsToApply`
in `migrateUpLocked`. Either the comparison needs a content identity, or the
out-of-order insert needs to be detected and reported as one; until then this
is a gap rather than a decision.

## What holds these

Each entry above is pinned by a test in
`cmd/atlas/compat_1241_retained_divergence_test.go`, and the trailing-positional
rows for `migrate status`, `migrate validate` and `schema inspect` are pinned in
`cmd/atlas/compat_overstrict_test.go`. The out-of-order entry pins the refusal
and records today's message as a characterization row, so closing the gap
changes that row rather than deleting the test.

Every case here is SQLite-measured; the two noted as also measured on
PostgreSQL 17.10 were re-run against a live server. The rest carry the
SQLite-only caveat that
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241) declares.
