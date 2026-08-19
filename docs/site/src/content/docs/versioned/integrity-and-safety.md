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

The same drift blocks every native verb that **executes** SQL from the
directory. Each one verifies `ptah.sum` or `atlas.sum` before executing
anything, so a tampered migration never runs (exit `2`):

```text
error: migration sum verification failed:
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

The gated set is the executing class, not a list of verbs that happened to be
noticed:

| verb | what it executes from the directory |
| --- | --- |
| `up` | the pending up migrations, against `--db-url` |
| `down` | the rollback migrations, against `--db-url` |
| `test` | up **and** down, against `--db-url` |
| `checkpoint` | the whole history, against `--shadow-db` |
| `baseline` | the baselined history, against `--shadow-db` |
| `lint` | the whole history, against `--dev-url` |
| `repair --resume-from` | the remaining statements of the body that failed |
| `generate --replay` | the current schema history, against `--dev-url` |
| `generate --shadow-db` | prior history before candidate verification |

Each replaying command captures the migration directory once, verifies that
snapshot, and executes those same bytes. A generation or checkpoint writer
also compares the directory it is about to update with the authorized snapshot
before it adds files, and a checkpoint writer compares the expected
snapshot-plus-checkpoint again before publishing the sum. The sum is computed
from that authorized expected state rather than from a newly reopened path. A
change during the run is refused; the new checksum cannot legitimize history
the command did not verify and replay.

`repair` is on that list only in its `--resume-from` spelling, and the
distinction is deliberate. A plain `repair` rewrites revision metadata and
executes none of the directory's SQL, so it keeps working on a drifted
directory — clearing a dirty row is a recovery step you may need *before* you
can sensibly re-hash anything, and a gate there would send you to fix the
directory you are still trying to reason about. `--resume-from` executes
statements straight out of the migration file, so it gates like the rest.

`status` and `set` read the directory but execute none of its SQL. `hash`,
`edit`, `rebase` and `rm` exist to **rewrite** the integrity file, so verifying
it first would refuse their purpose — they are outside the class by the
predicate, not by oversight.

`down` was the last member outside the gate, and it was the worst one to leave
out. Measured on one hashed directory whose `_init.down.sql` was rewritten with
`ptah.sum` left stale, `up` exited `2` and refused while
`down --target 0 --confirm` exited `0` and executed the rewritten file; a
catalog census afterwards listed a table that appears in no committed
migration. Verification guarding the constructive direction and not the
destructive one is backwards, because `down` is the direction where the result
cannot be inspected afterwards — the objects are gone either way.

`checkpoint` mattered for a second reason. It replays the history onto a shadow
database and writes what it observed there into a new migration under a **fresh**
checksum, so drift was not merely executed but laundered into a directory that
verifies clean from then on.

### Overriding the gate during a recovery

Refusing outright would remove a capability, so the gate has an escape:
`PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR=1` executes a drifted directory anyway. It
exists for the case that genuinely needs it — rolling back through a directory
whose sum is stale while recovering from a botched edit, where re-hashing first
would record the botched bytes as the intended ones.

It is an environment variable rather than a flag because the Atlas-compatible
surface asserts flag parity with the community binary, the same reason
`PTAH_SKIP_CHECKS` is spelled that way.

The command that owns a replay gate parses this variable at entry, before
argument validation, directory access, or a database connection. A present
empty value or another invalid boolean is therefore always an error; it cannot
hide behind a dry run, a missing required flag, or a branch that happens not to
reach the integrity check.

Using it is never silent. A run that overrides a real refusal says so on stderr
and names what it accepted:

```text
warning: PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR is set; ptah.sum verification was
SKIPPED and this run is executing migration SQL that no reviewed checksum
covers:
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

A run against a directory that verifies skips nothing and therefore prints
nothing. The variable does **not** relax `ptah migrations up --verify-sum`:
that flag is an explicit request for a stricter contract than the default, and
an environment variable does not override what the command line asked for.

`ptah-compat migrate validate`, `apply`, `status`, `set`, `new` and `diff`
enforce the same gate on `atlas.sum` directories with Atlas's own checksum
output, matching official Atlas behavior. Reporting is not exempt: on a hashed
directory whose only migration was deleted, an ungated `migrate status`
announced "Database is up to date"
([#974](https://github.com/stokaro/ptah/issues/974)).

The verbs that **write** are on that list for a reason of their own. A gate that
fired only on the reading verbs would still let `migrate new` append a file to a
tampered directory and re-hash it on the way out, so the tampering would end up
inside a directory that verifies clean — the laundering shape recorded for
`migrate import` in [#1095](https://github.com/stokaro/ptah/issues/1095). All
six verbs refuse before anything is written, which is what the pinned Atlas
community binary v1.3.0 does on the same directory.

The fuller compatibility surface also gates formatted `migrate down`. Its
default policy matches native Ptah: an unhashed directory is allowed, a stale
sum is refused, and `PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR=1` permits an explicit
recovery with the warning above. Strict CE mode rejects the extension variable
and does not expose a successful `down` implementation, so the escape hatch
cannot make the strict surface more permissive than Atlas CE.

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

It is registered on `migrations up`, `down`, `status` and `push`. The
requirement is the same on each — carry a sum, and match it — but the subject
is not. On `up`, `down` and `status` it is the directory the run pulled, so
those three also print the resolved digest and the `@sha256:` reference that
pins it. On `push` it is the local directory about to be published, checked
before the upload; that command reports the tag it pushed and the resulting
digest as separate fields and constructs no pinned reference, because there is
no tag-resolved provenance to qualify yet.

`ptah migrations validate` asks the same question without executing anything,
and it takes an `oci://` reference too:

```bash
ptah migrations validate \
  --dir oci://registry.example.com/acme/app-migrations:v1
```

It exits 0 when the artifact matches the sum it carries, 1 when a migration was
added, removed or edited out of band, and 2 when the artifact carries no sum at
all. Earlier releases answered `stat oci://...: no such file or directory`
here, which left the read-only integrity question answerable only by a verb
that writes.

HTTPS is the default here as everywhere else. `--plain-http` is registered on
this verb too, and like every other registration it is only for an explicitly
trusted local registry — never for a reference that looks like the one above.

Because the reference above is a tag, a successful run also prints the
movable-tag qualifier described below, naming the digest the tag resolved to. A
digest-pinned reference prints nothing extra.

That does not retire `--verify-sum` on the consuming verbs, and the reason is
timing rather than coverage. `validate` resolves the reference in its own
process; the consuming verb resolves it again in the next one. A movable tag
can select different bytes in between, so only the flag verifies the artifact
the same invocation is about to execute. Pin a digest, or pass the flag, or
both.

`status` is the one verb that runs no gate without the flag. It executes none
of the directory's SQL, so it is outside the always-on class below, and it is
the verb an operator reaches for while diagnosing a directory that has drifted
— gating it by default would refuse to describe the thing being investigated.
Pass `--verify-sum` when the report itself has to be an integrity claim, such
as in a CI job that reads it to decide whether to deploy.

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

:::danger[Upgrading a Flyway directory applied by an older Ptah build]
Direct Flyway apply, status, lint, and set now use the exact source token as the
Atlas revision identity. The numeric projection still exists, but only as the
execution-order and linearity key. A pre-#1206 revision table may therefore
contain the current numeric ordering key, while a pre-#982 table may contain an
even older numeric encoding. Either obsolete row would otherwise leave the
corresponding migration pending.

`ptah-compat migrate apply` refuses before executing anything and prints the
one-way repair to the exact token. For example, the old `V1` key `10000` becomes
`1`, and the pre-#1206 `V2` ordering key becomes `2`:

```sql
UPDATE atlas_schema_revisions SET version = '1' WHERE version = '10000';
UPDATE atlas_schema_revisions SET version = '2'
  WHERE version = '4611686018427510315';
```

Run the statements against the schema selected by `--revisions-schema`, then
re-run apply. If the exact token row already exists, the repair deletes the
duplicate obsolete row instead of updating onto an occupied primary key. New
exact-source rows carry `operator_version='Ptah/source-identity'`; only the
generic `Ptah` marker written by older builds proves that a numeric candidate is
an internal ordering key. A numeric candidate that is another covered or
retired exact token, or that belongs to another writer, is ambiguous and is
never rewritten automatically.

This recovery is intentionally one way. Ptah is pre-v1, so the retired internal
key is not retained as a second readable identity. Rewriting the version column
is sufficient because the recorded hash still covers the same converted SQL
body.
:::

:::note[Flyway revision identity matches Atlas CE]
For direct Flyway directories, `migrate apply`, `migrate set`, `migrate status`,
and `migrate lint` preserve exact plain, dotted, dot-prefixed, zero-padded,
nonnumeric, token-ending-`R`, baseline, and empty repeatable tokens. Revision
metadata therefore interoperates with the pinned Atlas CE identity contract.
The internal numeric projection decides order and linearity only; normal direct
operations do not expose it as the active migration identity.

The configured revision column must keep every covered token distinct. New
MySQL and MariaDB revision tables use a binary version collation, as do new SQL
Server tables. If an existing table's collation aliases two source tokens,
Ptah refuses before migration SQL instead of allowing the primary key or a
version lookup to choose one identity for both.

Persisted exact tokens remain migrations after their Flyway source file is
removed. They stay visible in status and source-order checks without becoming
pending work; this includes a retired dot-prefixed token such as `.foo`.
Atlas's measured `.atlas_cloud_identifier` bookkeeping row remains excluded
from status, version math, and mutation. The compatibility status report
selects Current by the same textual token rule as the pinned binary while
execution keeps its separate numeric high-water mark.

`migrate import` deliberately keeps its fixed-width numeric output filenames.
Those names preserve the safe order Ptah computed, whereas Atlas CE's imported
token filenames can order `1.5`, `10`, and `2` lexically. Import naming is a
separate artifact-safety contract from direct revision identity.

Cross-tool application can still fail closed on a checksum mismatch because
the two implementations may encode the same migration digest differently.
That checksum boundary does not change the exact version-token match and never
causes an already-applied migration to be treated as pending.
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

Every rule identifier, with its one-line meaning, the dialects it applies to,
the surface that reports it, and whether the name is Atlas's or Ptah's, is
enumerated in [Lint rules](../../reference/lint-rules/).

### Which direction each rule reads

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

An `atlas.hcl` migration directory remains bound to the project directory
handle opened for config evaluation until capture completes; replacing the
project pathname does not retarget it. Relative and absolute project values
must remain inside that root after symbolic-link resolution. Parent traversal,
outside absolute paths, and symbolic-link escapes are refused when their
resolved destination leaves the root.

## Pre-migration checks

Guard a migration on a data-state precondition with a `-- +ptah check`
directive, which runs before the migration's statements and aborts if the
assertion fails:

```sql
-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
DROP TABLE users;
```

A check guards the direction it is written in. A `-- +ptah check` in the up
body runs before the migration; one in the down body runs before the rollback,
which is where a precondition is often worth asserting most. The two are
independent: an up check does not guard a rollback and a down check does not
guard the migration.

Each check is a separate read against committed state that runs before the
statements of its own body, so a failing assertion leaves nothing applied and
exits non-zero.

### What a check may say

The vocabulary is deliberately small, and every limit below is a refusal rather
than a silent narrowing:

| Attribute | Accepted |
| --- | --- |
| `name` | any label; it names the failure |
| `assert` | exactly one read-only top-level `SELECT` returning one column and one row |
| `on_fail` | `abort` only — there is no `warn`, `skip` or `continue` |

An assertion of any other shape fails closed before the migration runs, and so
does a write-shaped one. On SQL Server, `NEXT VALUE FOR` is refused statically
because it advances a sequence.

The read-only guarantee has two strengths depending on the target. PostgreSQL,
CockroachDB, YugabyteDB, Spanner, MySQL and MariaDB run the assertion in a
database-enforced read-only session. SQLite, SQL Server and ClickHouse get a
plain session, so there the guarantee rests on the static shape check alone. Checks are rejected under `--tx-mode all` on a real apply (a
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
