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
gap register lives in [Comparison](../comparison/). Most entries came out of
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241); the project
migration-directory boundary is tracked in
[`stokaro/ptah#1118`](https://github.com/stokaro/ptah/issues/1118).

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

Every measurement below was taken on 2026-08-09 against that binary, except
where a section gives its own date, each exit status read on its own line
rather than through a pipe. Where a fixture needed a hashed migration
directory, that binary authored and hashed it, so no checksum quoted here was
computed by Ptah.

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

## What holds these

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

Every case here is SQLite-measured; the two noted as also measured on
PostgreSQL 17.10 were re-run against a live server. The rest carry the
SQLite-only caveat that
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241) declares.

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
