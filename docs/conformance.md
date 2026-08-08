# Atlas Conformance

Ptah's Atlas conformance scoreboard is maintained in the dedicated
[`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance)
repository.

That repository is the authoritative, CI-regenerated answer to "are we there
yet?" for Atlas OSS compatibility. It keeps Atlas's Apache-2.0 fixture corpus
outside Ptah's MIT source tree while importing Ptah as the system under test.
The dependency direction is intentionally one-way:

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

## What Conformance Means Here

A green scoreboard means Ptah does not diverge from the community binary in the
direction that costs a user something. It does not mean Ptah reproduces every
behavior the community binary has.

Two rules, and the second one is the reason this section exists:

1. **Never looser.** Anything the community binary refuses must not succeed on
   Ptah. A construct Ptah cannot yet implement is refused loudly rather than
   accepted and ignored.
2. **Never a copied defect.** Where the measured behavior is a defect -- it
   silently drops a directive the author wrote, corrupts state, or fails for a
   reason unrelated to the request -- Ptah does not reproduce it. Matching is
   the floor, not the ceiling.

A fixture that fails because Ptah is *better* is not a conformance failure. It
is recorded as a deliberate divergence, with the measurement that establishes
which behavior is the defective one, and the report says which of the two rules
it falls under.

An unknown name that the community binary itself accepts without acting on is
not a refused construct under rule 1. Ptah can accept the same no-op for
compatibility, but reports its source location so the behavior is not silent.

The distinction is not academic. `-- atlas:txmode none` written directly above
its statement, with no blank line between them, is silently dropped by the
community binary; the statement then runs inside the transaction it asked to
stay out of and the migration fails partway through. Ptah honors it. A change
that once removed that capability in the name of parity was reverted -- see
`AGENTS.md`, "Compatibility Policy".

## Current Scoreboard

As of Ptah `18ae5f9d4d63136248986263732524e2314f9d7c`:

| Tier | Purpose | Current result |
| --- | --- | --- |
| Offline Atlas corpus | Can Ptah ingest Atlas OSS fixture artifacts through public APIs? | 636 ok, 0 gap, 0 fail, 0 panic |
| Live round-trip | Can Ptah generate, apply, introspect, and diff first-party schemas on real databases? | 8 ok, 2 known gaps |
| Atlas CE differential | Do Atlas CE and Ptah agree on live end-state facts for shared fixtures? | 1 ok, 4 known gaps |
| CLI surface | Do Atlas CE and Ptah expose compatible command paths, help boundaries, flags, and runtime classifications? | Tracked in `cli-surface.md` |

The offline full-conformance gate is green. The live and differential full gates
remain intentionally red until the known gaps are closed, while their regression
budgets stay green when the reports are current and no new gaps appear.

## Workflow Parity

Each workflow below states the native Ptah command, the Atlas-compatible
surface, what Atlas CE does, and the evidence.

These rows record product workflow parity, not full Atlas Pro compatibility.
The Atlas-compatible test verbs run Ptah-native YAML/Go test cases; Atlas
`.test.hcl` files are not ingested.

### Declarative migration and schema tests

**Native Ptah.** `ptah migrations test` and `ptah schema test` run YAML/Go-authored cases locally.

**Atlas-compatible Ptah surface.** `ptah-compat migrate test` and `ptah-compat schema test` forward to the native runners with Atlas-shaped flags (`--dir`/`-u --url`, `--dev-url`, `--run`, project flags) and the native exit-code contract. `schema test -u` accepts three desired-state source kinds: a directory of Go schema annotations, a `.sql` or `.hcl` schema file, and a database URL whose live schema is introspected. A database source must share the dialect of the throwaway database, and the roles and grants it introspects are dropped before the schema is applied, with the omission reported on stderr so that stdout carries only the report.

**Atlas CE.** Cannot run either testing command; the framework is outside the open-source core.

**Evidence.** Unit tests cover parsing, assertions, reporting, and CLI behavior, including the Atlas-compatible forwards; integration-tagged PostgreSQL tests exercise both live runners. This workflow is not counted as a schema-object round-trip fixture.


### Migration directory maintenance

**Native Ptah.** `ptah migrations edit`, `rebase`, and `rm` mutate the directory and atomically rewrite the integrity file.

**Atlas-compatible Ptah surface.** `ptah-compat migrate edit`, `rebase`, and `rm` forward to the native commands with Atlas-shaped `--dir`/`--dir-format` flags, `{name | version}` positionals, and project flags; `migrate new --edit`, `migrate diff --edit`, and `schema apply --edit` open the operator's `$VISUAL`/`$EDITOR`.

**Atlas CE.** Cannot run any of the three verbs; they abort with the community-version boundary.

**Evidence.** Unit tests cover the forwards with hermetic editor scripts and assert `ptah migrations validate` passes on the mutated directory. Multi-version rebase and version ranges are rejected loudly (single-version forwarding only).


### Verified and reported rollback

**Native Ptah.** `ptah migrations down --shadow-db` replays the rollback plan on a disposable shadow database before the target is touched.

**Atlas-compatible Ptah surface.** `ptah-compat migrate down --dev-url` maps to the shadow verification, and `--format` renders an Atlas Go-template down report (`.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, `.Error`); real rollbacks never read stdin, matching Atlas, while native `ptah migrations down` keeps its prompt; the forward defaults to Atlas revision bookkeeping (`--revision-format atlas`, like `migrate set`), with the native `--revision-format ptah` pass-through as the escape hatch; the registry-bound `--to-tag`, `--skip-checks`, and `--plan` flags are recorded waivers that fail loudly with their rationale.

**Atlas CE.** `migrate down` does not exist in the community binary; the CE notice lists down migrations among excluded features.

**Evidence.** Unit tests over live SQLite cover verification success and pre-target abort on both paths, report rendering (including partial-failure reports), waiver rejections, non-interactive execution with EOF stdin, rejection of the non-Atlas `--confirm` flag, byte-identical execution output against a pre-approved native run, and revision-format regressions proving a bare `ptah-compat migrate down` reverts revisions written by `ptah-compat migrate apply` while an explicit ptah override leaves them untouched. A subprocess test runs the built `ptah-compat` binary with EOF stdin and checks the SQLite end state.


### Pre-approved declarative plans

**Native Ptah.** Ptah plans and applies declarative schema changes through the same `internal/atlasschema` engine that powers `schema apply`.

**Atlas-compatible Ptah surface.** `ptah-compat schema plan` atomically saves the computed plan in the Atlas `.plan.hcl` shape by default; an `--output` path ending in `.json` writes the native JSON plan instead, with ordered statements, per-statement safety severity, and SHA-256 source/desired schema fingerprints. `ptah-compat schema apply --plan file://<path>` reads both shapes: native JSON plans require a matching source fingerprint, while Atlas-format plans require dev-database replay against `--to` and an end-state check.

`--edit` rebuilds the plan from valid UTF-8 operator-edited SQL, preserves statement text and comments, and reclassifies safety metadata with the plan dialect, including MySQL/MariaDB executable comments. `--name-format` templates the plan name over `.FromHash` and `.ToHash` in Atlas's measured untagged standard-Base64 representation. `--skip-lint` is accepted as a no-op because this command runs no lint step, which remains a gap against Pro linting. `--auto-approve` is accepted for CLI compatibility — a locally saved plan file is approved by operator review, so there is no prompt to skip. Registry planning flags (`--push`, `--pending`, `--repo`) are recorded waivers, `--format` and `--directive` fail loudly because Atlas's shapes for them are unmeasured, and the registry sub-verbs (`approve`, `list`, `pull`, `push`, `rm`) stay unsupported-boundary stubs.

`ptah-compat schema plan new` creates a plan file for the transition. `ptah-compat schema plan validate` runs the same two verifications without touching the target, and its dev-database replay is unconditional because verification is the verb's only effect. A sanitized standard Atlas v1.3.0 help bundle with exact binary and artifact hashes confirms both command and flag surfaces, while their runtime behavior remains documentation-derived and tracked in [#1037](https://github.com/stokaro/ptah/issues/1037). Successful Ptah runs keep stderr free of development provenance. `lint` and `test` stay stubs — no measured output contract for the first, no `.test.hcl` reader for the second.

**Atlas CE.** `schema plan` aborts with the community-version boundary; the plan/approval flow is bound to the Atlas Pro registry.

**Evidence.** Unit tests over live SQLite cover plan computation and save, the saved-file contract, plan execution with schema assertions, stale-plan refusal after target drift, dry-run, declined confirmation, dialect mismatch, malformed documents, waiver rejections, and both entry points. Validation also consumes a versioned Atlas-authored plan bundle independently of Ptah's writer and rejects changed source, desired schema, migration SQL, extra statements, malformed HCL, and malformed foreign hashes without changing schema or rows.

The bundle records artifact hashes and the capture metadata that was not preserved. Destructive dev-database guards cover percent-encoded/equivalent paths, query options, symlinks, hard links, driver endpoint/database overrides, and comparison failures instead of comparing raw URLs. This workflow is not counted as a schema-object round-trip fixture.

## Atlas Pro Analyzer Coverage

Of the 30 analyzer check codes the Atlas analyzers documentation
(<https://atlasgo.io/lint/analyzers>, fetched 2026-07-28) marks as Atlas Pro,
Ptah's native lint covers 23 (`CD101`–`CD103`, `PG101`–`PG105` under Ptah codes
`PG101`/`PG106`/`PG103`/`PG104`/`PG105`, `PG302`/`PG303`/`PG305`–`PG311`,
`MY131`/`MY132`/`MY134`/`MY135`, `TX101`/`TX201`), flags 5 partially through
adjacent rules (`PG301` and `MY130` via `DS103`/`MY101`, `PG304` via `PG104`,
`MY133` via `CD103`, `MY136` via `MY101` for the `CONVERT TO CHARACTER SET`
form), and records 2 as waivers (`OW101`/`OW102` ownership policy, which binds
to Atlas Pro schema-ownership annotations and an account model). The
code-by-code table lives in
[Atlas Pro analyzer coverage](./site/src/content/docs/atlas/comparison.md#atlas-pro-analyzer-coverage).

## Verbs Beyond the CE Pin

`atlas migrate ls`, `atlas migrate show`, `atlas schema stats`, and
`atlas schema validate` appear in current Atlas documentation but are entirely
absent from the pinned conformance Atlas CE v1.3.0 binary (each resolves to
`unknown command`, not a community-version abort stub), so they are outside the
CLI-surface parity target today. Triage outcome, to revisit when the
conformance Atlas pin advances past v1.3.0:

| Atlas verb | Current Atlas docs behavior | Triage |
| --- | --- | --- |
| `migrate ls` | List migration files in the directory (`--latest`, `--short`). | Covered by native: `ptah migrations status` lists every migration with version, description, and applied/pending state. A thin drop-in forward is future work once the pin advances. |
| `migrate show` | Print the contents of one or more migration files. | Future work: no native verb prints a migration's SQL (the files are plain SQL on disk). A thin drop-in forward is a candidate once the pin advances. |
| `schema stats` | Inspect database schema statistics in OpenMetrics format. | Out of scope: statistics monitoring is a metrics/observability surface, not schema management; Ptah's schema-state surface is `ptah schema compare` and `ptah schema drift`. |
| `schema validate` | Check that a schema definition parses and loads, optionally against `--dev-url`. | Covered by native: `ptah schema render` parses and loads the desired schema and fails on invalid input; `ptah schema test` and `schema apply --dry-run` exercise it against a throwaway database. |

## Never a Copied Defect

Matching the pinned Atlas CE binary is the floor, not the ceiling. Where its
behavior is a defect — it silently discards something the author wrote, corrupts
recorded state, or fails for a reason unrelated to the user's intent — Ptah does
not reproduce it. Every entry below is stricter than Atlas CE, never looser, so
none of them can make `ptah-compat` accept input Atlas CE rejects.

Measured against the pinned Atlas CE v1.3.0 binary on SQLite targets. Each
reproduces on `migrate apply`, `migrate validate` and `migrate import`, under
both the `?format=` query and the `atlas.hcl` `migration { format = ... }`
spelling.

| Input | Atlas CE v1.3.0 | Ptah | Why not matched |
| --- | --- | --- | --- |
| Goose near-miss section directive, for example `-- +goose down` for `Down` | Exits 0. The misspelled name is not recognized, so it folds into the migration body as a comment and the rollback SQL beneath it executes: the table is created, then dropped, and the migration is recorded as successfully applied. | Refused, naming the offending line and the correct spelling. | A case error in a directive silently rolling back the migration it belongs to is a data-loss defect, not a semantic choice. Scoped to exact near-miss spellings of the four section directives, so `-- +goose Frobnicate` and prose such as `-- +goose up to date` still pass through as comments exactly as Atlas CE treats them. |
| dbmate migration with no `-- migrate:up` directive | Exits 0. `migrate apply` records the revision with 0 of 0 statements and creates nothing, so the migration is permanently marked done and no later apply will run it. `migrate import` writes a **zero-byte** file over the authored SQL and hashes the empty file into `atlas.sum` as if it were the migration. | Refused, stating that the file carries no `-- migrate:up` so none of its SQL would execute. | Discarding authored SQL and corrupting recorded state in one behavior. A file that *has* the directive with an empty section is a different, legitimate input and is converted and recorded normally. |

Not every difference is deliberate. Goose files carrying no directives at all are
matched rather than refused, because there Atlas CE is right: it executes the
file's bytes verbatim, drops nothing, and records the revision honestly. See
[`stokaro/ptah#981`](https://github.com/stokaro/ptah/issues/981).

### A trailing positional argument

`migrate status`, `migrate validate`, `migrate hash`, `migrate lint` and
`schema inspect` take no positional argument. Atlas CE accepts one anyway and
discards it; `ptah-compat` exits 1 and names it.

Measured on the pinned Atlas CE v1.3.0 binary, 2026-08-08, on SQLite and on
PostgreSQL 17, with `./migrations` and a second hashed directory `mig2` both
present:

```text
atlas migrate status --url "sqlite://app.db" file://mig2
  exit 0
  Migration Status: PENDING
    -- Pending Files:   1      <- the ONE file in ./migrations, not the two in mig2
```

The operator named `mig2` and was answered about `./migrations`. With no
`./migrations` at all the same argv exits 1 with
`sql/migrate: stat migrations: no such file or directory`, which is the same
argument being discarded, reported by accident. `atlas migrate status --help`
prints `Usage: atlas migrate status [flags]` and documents no positional, so
the tolerance is Cobra's default arity rather than a contract.

Silently answering about a directory the caller did not name is the defect this
project does not copy. The refusal names the rejected token and the flag that
takes a value there instead.

### An edited migration that has already been applied

A migration file whose bytes changed after it ran, with `atlas.sum` re-hashed
so the directory itself verifies, is a no-op on Atlas CE and a refusal here.

Measured on SQLite and on PostgreSQL 17, 2026-08-08, applying two migrations,
editing the first, re-hashing, and applying again:

| | Atlas CE v1.3.0 | Ptah |
| --- | --- | --- |
| `migrate validate` after the edit | 0 | 0 |
| `migrate apply` after the edit | 0, `No migration files to execute` | 1, `migration <version> checksum mismatch` |

Both binaries record a hash per revision; only Ptah compares it. The database
was built from SQL that is no longer in the repository, and every later
computation that replays the directory — `migrate lint`, `migrate diff`'s
desired state, a rollback's down file — assumes it was not. Reporting nothing
there is reporting a history that is not the one that ran.

The escape hatch is `ptah migrations repair --version <version> --force`, which
rewrites the recorded revision to match the file as it now stands. Run it for
the edited version **and for every version applied after it**: an `atlas.sum`
entry is a running hash over the preceding files, so editing one file changes
the recorded hash of every file below it. Measured on the two-migration fixture
above, repairing only the edited version moved the refusal to the next one, and
repairing both returned `migrate apply` to exit 0 with
`No migration files to execute.`

The refusal is exit 1 in the direction the rules allow, and it is stricter than
Atlas CE, never looser. See
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241) items 6
and 13.

### The same running hash makes the gate fire on files nobody edited

The property above has a second consequence, and there Ptah is the one that is
wrong. `migration/migrator` treats the stored `atlas.sum` entry as a content
hash of one file; it is not. Inserting a migration ahead of applied ones
rewrites the entry of every later file, and the gate then refuses citing a file
whose bytes never changed.

Measured on SQLite and on PostgreSQL 17, 2026-08-08, applying `one` and `three`
and then adding `two` between them:

| | Atlas CE v1.3.0 | Ptah |
| --- | --- | --- |
| `migrate apply` (default `--exec-order linear`) | 1, `migration file …_two.sql was added out of order` | 1, `migration …_three checksum mismatch` |
| `migrate apply --exec-order non-linear` | **0**, applies `two` | **1**, same checksum mismatch |

`…_three.sql` is byte-identical in both directories; only its position in the
sum chain moved. Both binaries stored the same value, so this is Ptah reading
its own recorded data under the wrong contract, and the second row is a
`ptah-compat exits 1 where Atlas CE exits 0` cell. It is open and tracked as
[`stokaro/ptah#1241`](https://github.com/stokaro/ptah/issues/1241) item 5; the
repair above is the operator's route through it in the meantime.

### `migrate lint` does not require `--dev-url`

Found while measuring the scope selector, and left open rather than widened into
that change. Atlas CE marks `--dev-url` required on `migrate lint`;
`ptah-compat` registers it as an ordinary flag and lints without it.

Measured 2026-08-08 in a directory holding a hashed `./migrations`, exit status
read on its own line after a redirect:

| | Atlas CE v1.3.0 | Ptah |
| --- | --- | --- |
| `migrate lint --dir file://migrations --latest 1`, no `--dev-url` | 1, `required flag(s) "dev-url" not set` | **0**, lints and reports |

This is a `ptah-compat exits 0 where Atlas CE exits 1` cell, and it predates the
scope selector: the same two answers come back on the commit before it. It is
recorded here rather than fixed in the same change, because the flag's
requiredness is a separate decision from what a run's scope is — Ptah's linter
can analyze SQL text with no dev database, so making the flag required deletes a
capability and needs the `PTAH_*` treatment of its own.

## PostgreSQL Introspection: Index and Domain Attributes

Reading a live PostgreSQL database once lost six attributes that the pinned
Atlas CE v1.3.0 binary preserves. All six are now read. Measured on PostgreSQL
17.10 by diffing an empty database against a source database with each binary,
replaying the emitted SQL into a fresh database with
`psql -v ON_ERROR_STOP=1` and reading psql's own exit status, then re-diffing
the replayed database against the source with the pinned binary as a neutral
observer. These affected the live-database read path only; an HCL source
carrying the same attributes always rendered correctly.

| Attribute | Read from | Before | Tracked in |
| --- | --- | --- | --- |
| Access method (`USING gin` / `gist` / `brin` / `hash`) | `pg_am.amname` | Every index collapsed to the btree default. **Not always silent** -- see below | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Operator class, for example `text_pattern_ops` | `pg_index.indclass`, non-default classes only | Dropped: psql exited 0 and left an index that no longer served the queries it was built for | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Sort order (`DESC`, `NULLS FIRST`, `NULLS LAST`) | `pg_index.indoption` | Dropped: psql exited 0 and left an ascending index | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| `INCLUDE` payload columns | `pg_index.indkey` past `indnkeyatts` | Dropped: psql exited 0 and left an index that cannot serve the index-only scans it was built for | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Expression index, for example `lower(name)` | `pg_index.indkey`, attnum 0 | Emitted as a quoted column identifier, which psql rejected at exit 3 | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Domain used as a column type | `information_schema.columns.domain_name` and `domain_schema` | The `CREATE DOMAIN` was emitted but the column was flattened to the domain's base type: psql exited 0 and left a column without the domain's `CHECK` | [#1242](https://github.com/stokaro/ptah/issues/1242) |

### The access-method loss was not silent in general

An earlier version of this table said the access-method loss "replays at exit 0
and leaves a btree index". That is fixture-dependent and false in general. It
holds only when the indexed column's type has a default btree operator class.

Measured, a `gist` index on a `point` column:

```text
emitted:  CREATE INDEX IF NOT EXISTS "i_gist" ON "t" ("p");
replay:   psql exits 3
          ERROR: data type point has no default operator class for access method "btree"
```

The `int4range` fixture the original claim was generalized from does have a
btree operator class, so there the same loss degraded quietly. The loss is loud
for `point`, `box`, `tsvector` and every other type with no btree class, and
quiet otherwise. A green replay never proved the access method survived; it only
proved the column type tolerated losing it.

### Domains are where Atlas CE is wrong in the opposite direction

CE keeps the column's declared type but never emits the `CREATE DOMAIN`, so its
own `schema diff` output fails to replay with
`ERROR: type "positive_int" does not exist` — measured, psql exit 3. Ptah emits
both the `CREATE DOMAIN` and the domain-typed column, so closing this gap meant
keeping both halves rather than matching CE. Ptah's output replays at psql exit
0 where CE's does not. The same divergence reaches `schema apply`: applying this
source database to an empty target, CE exits 1 with
`create "t" table: pq: type "positive" does not exist` and Ptah exits 0 with the
domain created first.

### A domain column has to survive the comparator too, and the JSON surface

Rendering a domain and reconciling one are separate claims, the same way they
are for indexes below. Reading the domain fixed what `schema inspect` writes as
HCL and left two other surfaces on the base type. Measured on PostgreSQL 17.10
against one database holding `CREATE DOMAIN positive AS integer CHECK (VALUE >
0)` and a column of it:

| Surface | Atlas CE v1.3.0 | Ptah before | Ptah now |
| --- | --- | --- | --- |
| `schema inspect`, HCL | `type = sql("positive")` | `type = sql("positive")` | `type = sql("positive")` |
| `schema inspect --format json` | `"type":"positive"` | `"type":"integer"` | `"type":"positive"` |
| `schema inspect --format json`, domain off the search path | `"type":"doms.positive"` | `"type":"integer"` | `"type":"doms.positive"` |
| `schema diff --from X --to X`, one database against itself | Schemas are synced | `ALTER TABLE "t" ALTER COLUMN "qty" TYPE positive;` | Schemas are synced |
| `schema apply` run twice against the same target | — | plans and executes the same `ALTER` on every run, exit 0 each time | second run reports the schema synced |

The diff row is the one that made the rendered domain worth nothing: the desired
side answered `positive` and the database side answered `integer`, so a database
was never in sync with itself and `schema apply` executed an `ALTER COLUMN` on
every run while reporting success.

Two details decided whether any of this was visible:

- **The name of the domain.** The type comparison folded a spelling into a
  category by substring, so any domain whose name contains `int` — the issue's
  own `positive_int` fixture — compared equal to `integer` by accident and the
  churn did not appear. A domain is now compared as the identifier it is: two
  domains are the same type when they are the same domain. The reverse follows,
  and is the point: a column of `positive_int` that a desired schema declares as
  `bigint` is now reported as drift instead of silently matching.
- **The array column next to it.** An array column and a domain column both make
  the reader ask the server for its own spelling of the type, and the two want
  opposite answers on the JSON surface: CE prints the bare category `ARRAY` for
  an array and the domain name for a domain. The read carries which one it was,
  rather than letting each consumer guess from which field happens to be empty.

A domain column that also draws from an owned sequence is not a `SERIAL`
column. PostgreSQL's `SERIAL` shorthand only ever builds a column of an integer
type, so writing such a column back as `SERIAL` rebuilds it without the domain.
The domain wins, the sequence default is written out beside it instead of being
folded into the shorthand, and the sequence itself is reported rather than
treated as the column's implicit backing sequence — without it the emitted DDL
names a sequence nothing creates, which is measured as psql exit 3.

### A domain over a user-defined base type is a different catalog shape

`CREATE DOMAIN positive AS integer` and `CREATE DOMAIN d_enum AS color` do not
read back the same way, and the difference decides which code answers the
question "what type is this column declared as". Measured on PostgreSQL 17.10:

| Column | `data_type` | `udt_name` | `domain_name` | `format_type` |
| --- | --- | --- | --- | --- |
| `qty positive`, `positive AS integer` | `integer` | `int4` | `positive` | `positive` |
| `c d_enum`, `d_enum AS color` | `USER-DEFINED` | `color` | `d_enum` | `d_enum` |
| `plain color`, no domain | `USER-DEFINED` | `color` | *(null)* | *(not read)* |

For a domain over a built-in base type `data_type` is the base type, so a
consumer that falls through to `format_type` reaches the domain by accident. For
a domain over a user-defined base type — an enum, a composite or a range —
`data_type` is the bare category `USER-DEFINED` and `udt_name` names the BASE
type, identically to the plain column on the last row. A consumer that answers
from `udt_name` there flattens `c` to `color` and drops the domain's `CHECK`
with it, and only `domain_name` separates row two from row three.

The split is not a synthetic one. Two domains arrive with PostgreSQL's own
contrib modules and land on opposite sides of it: `lo`, from the `lo` module, is
a domain over the built-in `oid`, and `earth`, from `earthdistance`, is a domain
over `cube`, a base type the `cube` module supplies. Measured on PostgreSQL
17.10 against one table holding a column of each, with a plain `cube` column
beside them as the control:

| Column | Atlas CE v1.3.0 | Answering from `udt_name` | Ptah |
| --- | --- | --- | --- |
| `l lo` | `type = sql("lo")` | `type = sql("lo")` | `type = sql("lo")` |
| `w earth` | `type = sql("earth")` | `type = sql("cube")` | `type = sql("earth")` |
| `cu cube`, no domain | `type = sql("cube")` | `type = sql("cube")` | `type = sql("cube")` |

The middle column is this same code with the `domain_name` gate taken back out,
and it is the reason the gate is not optional: two domains a user never wrote
disagree with each other about whether a domain survives introspection, decided
by nothing but whether the base type happens to be built-in.

Measured with `ptah-compat` against one database holding rows two and three, and
against the composite and range shapes beside them:

| Probe | Atlas CE v1.3.0 | Ptah |
| --- | --- | --- |
| `schema diff --from X --to X`, one database against itself | Schemas are synced | Schemas are synced |
| `schema apply` of a byte-identical twin | — | Schema is synced, no changes to be made |
| `schema inspect --format json` | `"type":"d_enum"` | `"type":"d_enum"` |
| `schema inspect`, HCL | `type = sql("d_enum")` | `type = sql("d_enum")` |
| `schema inspect`, HCL, plain enum column beside it | `type = enum.color` | `type = enum.color` |
| `schema diff` from empty, replayed with psql, then compared to the source by CE | — | psql exit 0, Schemas are synced |

The last row is the round trip, and CE is the neutral observer in it: Ptah emits
`CREATE TYPE`, `CREATE DOMAIN` and a `d_enum` column, the replay runs at psql
exit 0, and CE then reports the replayed database in sync with the one it was
described from. A description that spells the column `color` replays at exit 0
too and CE reports `ALTER TABLE "t" ALTER COLUMN "c" TYPE d_enum;` — the replay
lost the domain and the exit code did not say so.

That row carries a second claim, and it is the emitted script's ORDER. Naming a
domain in a column is worth nothing if the statement that creates the domain
runs before the statement that creates its base type, and PostgreSQL has no
forward declaration for a type. The emitter ran kind by kind — every domain,
then every range, then every composite — so a database holding
`CREATE DOMAIN d_comp AS addr` was described as `CREATE DOMAIN "d_comp" AS addr;`
four statements ahead of `CREATE TYPE "addr" AS ("street" text, "city" text);`
and the replay stopped where it had to. Measured on PostgreSQL 17.10 with the
enum, composite and range shapes in one database:

| Emitted script | `psql -v ON_ERROR_STOP=1` | CE compares the replay to the source |
| --- | --- | --- |
| every domain first | `ERROR: type "addr" does not exist`, exit 3 | not reached |
| dependency ordered | exit 0 | Schemas are synced |

No fixed order of kinds fixes this, because the three kinds share one namespace
and name each other in both directions: `CREATE DOMAIN d_comp AS addr` needs the
composite first and `CREATE TYPE addr AS (f d_int)` needs the domain first.
Domains, composites and ranges are ordered against each other by what their
definitions name. Enums stay ahead of all three: an enum names no other
user-defined type, so it has nothing to wait for.

The drops a modification emits are ordered by a different graph, and reversing
the creation order is not a substitute for it. A `DROP` executes against the
database as it stands, so only the references that database holds now can block
it. Measured on PostgreSQL 17.10, one database holding
`CREATE TYPE cc AS (f integer)` and `CREATE DOMAIN dd AS cc`, reconciled against
a target of `CREATE DOMAIN dd AS integer` and `CREATE TYPE cc AS (f dd)`:

| Drops ordered by | Emitted order | `schema apply --auto-approve` |
| --- | --- | --- |
| the target definitions | `DROP TYPE cc`, `DROP DOMAIN dd` | exit 1, `cannot drop type cc because other objects depend on it` / `DETAIL: type dd depends on type cc` (SQLSTATE 2BP01) |
| the current definitions | `DROP DOMAIN dd`, `DROP TYPE cc` | exit 0 |

The same root cause shows without a flip in it. A database holding
`CREATE DOMAIN qty AS integer CHECK (VALUE > 0)` and
`CREATE TYPE meas AS (q qty, label text)`, reconciled against a target that
widens `qty` to bigint and gives `meas` a plain bigint field, has no edge at all
on the target side: `DROP DOMAIN "qty"` came out first and the server answered
`column q of composite type meas depends on type qty`. The current side still
has the edge, and dropping `meas` first exits 0.

### Reading an attribute and reconciling it are different claims

The table above records what introspection preserves. It does not say what
`schema diff` does with an index that already exists, and until
[#1272](https://github.com/stokaro/ptah/issues/1272) the answer was: nothing.
The PostgreSQL branch of the index comparator compared the partial predicate
and `NULLS DISTINCT` and returned before reaching anything else, so every
attribute the reader had learned to preserve was discarded at reconciliation
time. Each pair below reported
`Schemas are synced, no changes to be made.` while the pinned Atlas CE v1.3.0
binary planned `DROP INDEX` + `CREATE INDEX`. Measured on PostgreSQL 17.10 by
loading each side into its own database and diffing one against the other.

| Current index | Desired index | Now planned |
| --- | --- | --- |
| `USING btree (value)` | `USING hash (value)` | Rebuild |
| `USING gin (tsv)` | `USING gist (tsv)` | Rebuild |
| `(value)` | `(value text_pattern_ops)`, and the reverse | Rebuild |
| `(value NULLS FIRST)` | `(value DESC)` | Rebuild |
| `(value)` | `(value NULLS FIRST)` | Rebuild |
| `(value DESC)` | `(value DESC NULLS LAST)` | Rebuild |
| `(a) INCLUDE (b)` | `(a) INCLUDE (c)`, added, removed, reordered | Rebuild |
| `(a)` | `(lower(a))`, and the reverse | Rebuild |
| `(lower(a))` | `(upper(a))` | Rebuild |
| `(value)` | `UNIQUE (value)` | Rebuild |
| `(a)` | `(b)` | Rebuild |

PostgreSQL cannot alter any of these in place, so the transition is a rebuild
rather than an `ALTER INDEX`. Every plan above was executed against the current
database with `psql -v ON_ERROR_STOP=1` at exit 0, and both Ptah and the pinned
binary reported the databases synced afterwards.

Equivalent spellings still compare equal, so nothing is rebuilt for the sake of
it: `btree` and `BTREE`; an omitted access method and `USING btree`; `ASC` and
`ASC NULLS LAST`; `DESC` and `DESC NULLS FIRST`; an index-level `ops` and the
same class written on each key.

Two consequences worth naming:

- The HCL surface now reads and writes `nulls_first` / `nulls_last` on an
  `index` `on` block. These are the attribute names Atlas CE's own
  `schema inspect` emits. `ptah-compat` previously dropped them under its
  unknown-attribute tolerance, so a file CE produced reached Ptah with the
  ordering gone; the native parser refused the file outright.
- An operator class is compared as written, case-insensitively. Ptah does not
  resolve a column type's *default* operator class, so a hand-written source
  that spells the default out — `ops = text_ops` on a `text` column — plans a
  rebuild on every run where CE reports the schemas synced. The catalog reports
  only non-default classes, so neither binary's `schema inspect` produces that
  input and no round trip reaches it. Omit a default class, which is how both
  binaries write one.

MySQL and MariaDB now compare uniqueness and the key columns, and nothing else.
That branch was added with the ownership rule below, which is what made a
database unique index reachable by the comparison at all; see
[#1245](https://github.com/stokaro/ptah/issues/1245) for what it deliberately
leaves out and why. SQLite, ClickHouse, CockroachDB, YugabyteDB and Spanner keep
the comparison they had, because what makes two indexes the same index is a
per-dialect question and those dialects were not measured.

A key those two engines compare has to be read whole first, and one is read one
part at a time for that reason. `information_schema.STATISTICS` describes a key
as one row per part; joining the names in SQL and splitting them again in Go
lost a key two ways, both of which a schema is free to hit:

| Fixture on MySQL 9.7.1 | Read as | Applying a database's own `schema inspect` output |
| --- | --- | --- |
| `` KEY idx_weird (`a,b`) `` | two columns, `a` and `b` | exit 1, `Error 1072 (42000): Key column 'a' doesn't exist in table` |
| a 16-part key of 64-character names, 1039 bytes past `group_concat_max_len` | the last name cut at 1024 bytes | exit 1, same error naming the truncated column |
| `KEY idx_expr ((b + 1))` | nothing — the read failed | exit 1, `converting NULL to string is unsupported` |

The pinned Atlas CE v1.3.0 binary reported all three synced, and so does Ptah
now. A comma is a legal character in a MySQL identifier and
`group_concat_max_len` defaults to 1024 bytes on MySQL 9.7 (1048576 on MariaDB
11.8, which is why only the comma reaches it there). A functional key part has a
`NULL` `COLUMN_NAME` and an `EXPRESSION` column MariaDB does not have, so the
reader still cannot name it — it now reports the part as one it could not read
instead of failing, and the key-column comparison reads what was read rather than
treating it as the whole key, which would plan a rebuild of a key that never
changed on every run.

A partly-read key is compared as far as it was read. `KEY idx_mixed (b, (b + 1))`
arrives as the one named column `b` plus the record of a part that could not be
named, so a desired `idx_mixed (c, (c + 1))` is a different key by the only part
either side can name, and the difference is reported. Proof runs one way only: a
desired `idx_mixed (b, (c + 1))` names the same column and differs solely in the
expression, and that pair is still reported synced. Closing it means reading
`information_schema.STATISTICS.EXPRESSION`, which MariaDB does not have, so
comparing a MySQL expression key remains unsupported — as does rebuilding one,
because the down direction would recreate the key without its expression.

### A unique constraint and its index are one object, and the schema says which

PostgreSQL, MySQL and MariaDB enforce a `UNIQUE` constraint with an index of the
constraint's own name on the constraint's own table. Introspection reports that
one object twice — once in the index catalog, once in the constraint catalog —
and MySQL and MariaDB do not even have a separate notion to report:
`ADD CONSTRAINT c UNIQUE (a)` and `CREATE UNIQUE INDEX c ON t (a)` leave the
identical catalog row, which is why `schema inspect` writes MySQL uniqueness
back out as `index { unique = true }` on both binaries and never as a
constraint.

The other two engines Ptah supports do not share the name, so nothing brings the
two representations into collision there. SQL Server keeps a `UNIQUE` constraint
and a unique index as separate objects. SQLite backs one with an index it names
itself: `CREATE TABLE t (a TEXT, CONSTRAINT uq_t_a UNIQUE(a))` leaves
`sqlite_master` holding `sqlite_autoindex_t_1`, and no statement can name that
object or the constraint that owns it.

The two representations used to be compared in two pools, and the database index
was filtered out of its pool unconditionally. The desired side is never
filtered, so a desired state that spells the object as an index had nothing to
match: index comparison reported it **added** and constraint comparison, finding
no `UNIQUE` constraint on the desired side, reported the same name **removed**,
in the same plan. Measured by replaying a database's own `schema inspect` output
against the database it came from, where the pinned Atlas CE v1.3.0 binary
reported the schema synced:

| Target | Fixture | Ptah before | Result of applying it |
| --- | --- | --- | --- |
| MySQL 9.7.1 | `UNIQUE KEY uq_users_email (email)` | `CREATE UNIQUE INDEX` + `DROP INDEX` | exit 1, `Error 1061 (42000): Duplicate key name` |
| MySQL 9.7.1 | `UNIQUE KEY uk_users_email (email)` | `CREATE UNIQUE INDEX` + `DROP INDEX` | exit 1, same error |
| MariaDB 11.8.8 | `UNIQUE KEY uq_users_email (email)` | `CREATE UNIQUE INDEX` + `DROP INDEX IF EXISTS` | exit 1, same error |
| PostgreSQL 17.10 | `CONSTRAINT uq_users_email UNIQUE (email)` against a desired `index { unique = true }` | `CREATE UNIQUE INDEX IF NOT EXISTS` + `DROP CONSTRAINT IF EXISTS` | **exit 0**, and the table left with no unique index at all |

The spurious pair rode along with real work. Adding one column to the MySQL
table above planned `ADD COLUMN`, then the same `CREATE UNIQUE INDEX` and
`DROP INDEX`; the apply added the column, failed on the create, and exited 1
with the migration half applied.

The PostgreSQL row is the one to read twice. The `IF NOT EXISTS` guard skipped
the create, the drop took the constraint and its index with it, and the command
reported success while deleting the uniqueness the desired state asked for.

Ownership now follows the desired state's spelling. An identity the desired
state declares as an index reaches index comparison, whichever filter would
otherwise have claimed it, and constraint comparison leaves that object alone; a
desired state that spells uniqueness as a constraint, or does not mention the
object at all, is unchanged — the constraint pool still owns it, and an
undeclared unique key is still reported removed. The rule never drops an object
the desired state did not ask to change: an identity only survives the filters
when the desired state names it, and a named identity is either matched, or
replaced by the definition the desired state gives it — an addition paired with
the drop of what it replaces, never a drop on its own.

Two objects that merely share a name stay two: index identity carries the owning
table, and a desired plain `index "uq_users_email"` against a database
`UNIQUE KEY uq_users_email` is reported as a replacement rather than being
collapsed into agreement.

**The drop half of that replacement is spelled per engine, because the object
is.** On MySQL and MariaDB the unique key and its constraint are one catalog row
and `DROP INDEX` removes it. On PostgreSQL the constraint is an object of its
own that owns the index, and the server refuses the index spelling. Both cells
below are the pinned binary's own plan, matched by Ptah:

| Target | Database | Desired | Plan |
| --- | --- | --- | --- |
| MySQL 9.7.1 | `UNIQUE KEY uq_users_email (email)` | `index "uq_users_email"` | `ALTER TABLE users DROP INDEX uq_users_email` then `ADD INDEX` |
| PostgreSQL 17.10 | `CONSTRAINT uq_users_email UNIQUE (email)` | `index "uq_users_email"` | `ALTER TABLE "users" DROP CONSTRAINT "uq_users_email"` then `CREATE INDEX` |

Dropping the PostgreSQL one as an index answers `cannot drop index
uq_users_email because constraint uq_users_email on table users requires it
(SQLSTATE 2BP01)`. The comparator therefore records **what the object is** — a
UNIQUE constraint's — for every engine that reports it under the constraint's
name, and each planner spells its own statement: the PostgreSQL-family planner
drops the constraint, the MySQL and MariaDB planner keeps `DROP INDEX` and their
plans are unchanged. SQL Server never reaches the rule, and SQLite names the
backing index itself, so neither records anything.

**The rollback restores a constraint, on every engine.** What the up direction
dropped was a UNIQUE constraint, so `ALTER TABLE … ADD CONSTRAINT … UNIQUE` is
what puts it back — on MySQL and MariaDB that lands the same catalog row
`CREATE UNIQUE INDEX` would, and on PostgreSQL nothing else restores the
`pg_constraint` row.

Reversing the removal into an index addition instead restored the wrong object
where it worked at all. Three facts govern the down direction:

- The down target is the introspected schema, which omits a constraint-backed
  index because the index is the constraint's. The index addition therefore had
  nothing to resolve, and generation failed outright with `invalid schema diff:
  added index users.uq_users_email at position 0 is missing or ambiguous in the
  target schema` — measured live on PostgreSQL 17.10 and MySQL 9.7.1, where no
  migration could be produced at all.
- Where it did resolve, it would rebuild a plain unique index in place of the
  constraint, leaving a catalog the migration never started from.
- `ADD CONSTRAINT … UNIQUE` builds an index of the constraint's name, so the
  plain index the up direction created is dropped first. Otherwise PostgreSQL
  answers `relation "uq_users_email" already exists` (SQLSTATE 42P07) and MySQL
  `Error 1061 (42000): Duplicate key name 'uq_users_email'`.

Applied end to end against a live `CONSTRAINT uq_users_email UNIQUE (email)`, the
up leaves a plain index and no constraint row, the down restores both, and a
second comparison against the post-up database reports synced:

| Target | up | catalog after up | down | catalog after down |
| --- | --- | --- | --- | --- |
| PostgreSQL 17.10 | exit 0 | `uq_users_email` a plain index, no `pg_constraint` row | exit 0 | `UNIQUE (email)` and its unique index, as before the up |
| MySQL 9.7.1 | exit 0 | `NON_UNIQUE=1`, no `TABLE_CONSTRAINTS` row | exit 0 | `NON_UNIQUE=0` and the `UNIQUE` row, as before the up |

**Losing the uniqueness is a destructive change, whichever statement says so.**
Replacing a unique key with a plain index deletes a data guarantee, and it was
classified only as `indexes_removed` — a warning, which passed
`--check-destructive` and every drift threshold keyed on destructive findings, on
all three engines. The diff now reports it as `unique_protections_removed`, and
on MySQL and MariaDB — where the removal is spelled `DROP INDEX`, exactly like
dropping an access path — the statement itself is classified destructive.

## SQLite: A Primary Key Is Not A NOT NULL

Ptah applied "a primary key column is NOT NULL" as if it were a rule of SQL. It
is a rule of most engines. In SQLite it is a rule of the **table**, and on the
ordinary rowid table it does not hold: `id INTEGER PRIMARY KEY` is an alias for
the rowid, `pragma table_info.notnull` reports 0, an explicit
`INSERT INTO t (id) VALUES (NULL)` is accepted, and a rowid is assigned for it.
Ptah folded the two together in six places — the AST node, the HCL reader, the
HCL writer, the SQL-DDL parser's shared node, the comparator, and the SQLite
renderer — so the assumption survived being fixed anywhere less than all of
them.

Two things followed, both at exit 0, measured on 2026-08-08 against the pinned
Atlas CE v1.3.0 binary with each binary in its own directory
([`stokaro/ptah#1235`](https://github.com/stokaro/ptah/issues/1235), findings
5.1 and 6.3):

| Command | Before | Pinned Atlas CE v1.3.0 |
| --- | --- | --- |
| `schema apply --to file://users.hcl --auto-approve`, key column `null = false` | wrote `"id" integer PRIMARY KEY`, dropping the declared NOT NULL. Asked whether that database matched the file it came from, the pinned binary planned a **full table rebuild** | wrote `` `id` integer NOT NULL ``, and answered `Schemas are synced, no changes to be made.` |
| `schema inspect --format '{{ json . }}'` over `id INTEGER PRIMARY KEY` | `"null"` omitted, meaning NOT NULL — the only column in the fixture whose flag the two binaries disagreed about | `"null": true` |

Both directions are now the source's answer on a rowid table, and both are
pinned: a declared NOT NULL survives onto the key column, and a key column
declared `null = true` does not acquire one. (On a `STRICT` or `WITHOUT ROWID`
table SQLite itself supplies the NOT NULL, whatever the document said; see
[the table's shape decides](#the-tables-shape-decides-not-the-dialect) below.)
After the fix the pinned binary answers
`Schemas are synced, no changes to be made.` at exit 0 for either document
against the database `ptah-compat schema apply` built from it.

The dialects that do imply NOT NULL from PRIMARY KEY keep doing so, but the rule
moved rather than vanished: it now lives in each renderer and in the comparator,
where the dialect is known. Their CREATE TABLE renderers had always applied it
themselves. Their **MODIFY COLUMN** renderers had not — PostgreSQL and SQL
Server read the flag — so a first version of this change made any modification
of a single-column key column plan `ALTER COLUMN "id" DROP NOT NULL`, which
PostgreSQL refuses outright with `column "id" is in a primary key`
(SQLSTATE 42P16). Both renderers take the primary-key branch now, and every
dialect in `renderer.SupportedDialects()` is pinned in both directions by
`TestModifyColumn_KeyColumnNeverRendersNullable` and
`TestModifyColumn_OrdinaryColumnStillRendersNullable`.

What that cost each engine, measured rather than reasoned about:

| Dialect | How it was established | Result |
| --- | --- | --- |
| PostgreSQL 17.10 | live: `ptah-compat schema apply` from SQL and from HCL, `ptah schema apply` from Go annotations, and `ptah-compat migrate apply` over a key column widened `integer` → `bigint`, with the catalog read back afterwards | plan byte-identical to before the change; applies at exit 0 |
| MySQL 9.7.1 | live: the same Go-model fixture through `ptah schema compare` | rendered SQL byte-identical to before the change |
| SQL Server | inspection only — no live server was available here | guarded in `renderColumnForAlter`; pinned by a renderer test, not by an engine |
| ClickHouse | inspection only | unaffected: its type renderer already excluded a key column from `Nullable()` wrapping on both paths |
| CockroachDB, YugabyteDB, Spanner | inspection only; they share the PostgreSQL renderer | inherit the PostgreSQL guard |

Two adjacent defects were found while measuring this and are **not** fixed here,
because both predate the change and reproduce identically without it:

- A composite PRIMARY KEY whose columns are not spelled NOT NULL in the source
  plans `ALTER COLUMN ... DROP NOT NULL` for each key column on PostgreSQL, and
  is refused the same way. Only a single-column key reaches the flag this
  section is about.
- MySQL plans `ALTER TABLE users MODIFY COLUMN id BIGINT PRIMARY KEY` for a key
  column type change, which MySQL rejects with `Multiple primary key defined`
  when the key already exists.

### The table's shape decides, not the dialect

"SQLite key columns are nullable" is the rowid table's answer, and reading it as
SQLite's answer is the same mistake in the other direction. A `STRICT` or
`WITHOUT ROWID` table does enforce NOT NULL on its key columns, and the catalog
reports them that way. Measured with `pragma table_info` on SQLite 3.51.0, and
confirmed against the pinned Atlas CE v1.3.0 binary, which models the same
nullability for each shape when it reads the same DDL through a dev database:

| Table | `notnull` on the key column(s) | Pinned Atlas CE v1.3.0 `{{ json . }}` |
| --- | --- | --- |
| `id TEXT PRIMARY KEY` | 0 | `"null": true` |
| `id INTEGER PRIMARY KEY` | 0 | `"null": true` |
| `PRIMARY KEY (team, member)` | 0, 0 | `"null": true` |
| `id TEXT PRIMARY KEY` + `WITHOUT ROWID` | 1 | `"null": false` |
| `id INTEGER PRIMARY KEY` + `WITHOUT ROWID` | 1 | `"null": false` |
| `PRIMARY KEY (team, member)` + `WITHOUT ROWID` | 1, 1 | `"null": false` |
| `id TEXT PRIMARY KEY` + `STRICT` | 1 | `"null": false` |
| `id INT PRIMARY KEY` + `STRICT` | 1 | `"null": false` |
| `PRIMARY KEY (team, member)` + `STRICT` | 1, 1 | `"null": false` |
| `id INTEGER PRIMARY KEY` + `STRICT` | 0 | `"null": true` |

The last row is not an exception to the rule but an instance of it: a `STRICT`
table still has a rowid, `id INTEGER PRIMARY KEY` is still its alias, and only
that exact declared type is — `INT` in the same position reports 1. A blanket
"`STRICT` or `WITHOUT ROWID` implies NOT NULL" would be wrong there, and
measurably so: it turns `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT
NOT NULL) STRICT` back into a permanent rebuild, which is this section's own
defect wearing a `STRICT` table.

The rule lives in `internal/sqlitekey`, and the comparator and the HCL writer
ask it rather than the dialect. Before they did, measured on 2026-08-08 at exit
0 with each binary in its own directory:

| Command | Table | Before | After |
| --- | --- | --- | --- |
| `schema apply --to file://schema.sql --auto-approve`, run twice | `WITHOUT ROWID` | second run planned a full table rebuild, and so would every run after it | `Schema is synced, no changes to be made.` |
| the same | `STRICT` | second run planned a full table rebuild | `Schema is synced, no changes to be made.` |
| the same | rowid | `Schema is synced, no changes to be made.` | unchanged |
| `migrate diff <name> --to file://schema.sql`, run twice | `WITHOUT ROWID` | wrote a second migration file containing that rebuild | `The migration directory is synced with the desired state, no changes to be made` |

The pinned binary answered `Schemas are synced, no changes to be made.` against
those databases throughout: the disagreement was Ptah's with itself, between the
database it had built and the model it built that database from.

### The uniqueness half

The same fold applied to uniqueness, and there it invented a constraint rather
than dropping one. `reconcileColumnUniqueness` marked a column UNIQUE from *any*
single-column unique index, including one an author had created by name, while
leaving that index in the schema. Rendering the result emitted both.

Measured on the same day, `schema inspect --format '{{ sql . }}'` over

```sql
CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE, c TEXT);
CREATE UNIQUE INDEX ux_t_c ON t(c);
```

replayed into a fresh database with **four** indexes where the source had three.
The extra one, `sqlite_autoindex_t_3`, was a phantom unique index on `c` backing
a constraint the author never wrote — a dump-and-restore that silently tightened
the schema. SQLite distinguishes the two cases itself: `pragma index_list`
reports `origin` `u` for the implicit index behind a declared UNIQUE constraint
and `c` for a standalone `CREATE UNIQUE INDEX`. Only the first is part of the
column's declaration, and only the first is folded now. A declared column
`UNIQUE` still round-trips through its own implicit index.

### A domain column is reconciled by identity

The same split applies to the domain row of the table above. Reading a column's
domain is one claim; comparing it is another, and the comparison must not go
through type normalization. Normalization matches by substring — anything
containing `int` is an integer and anything containing `text` is text — which
is right for type names and wrong for a name a schema author picked. Measured
on PostgreSQL 17.10, two domains over `integer` with ordinary names:

```sql
CREATE DOMAIN waypoint AS integer CHECK (VALUE > 0);
CREATE DOMAIN context  AS integer;
CREATE TABLE t (id serial PRIMARY KEY, a waypoint NOT NULL, b context NOT NULL);
```

against a desired `a bigint, b text`, `waypoint` matched `bigint` and `context`
matched `text`, so neither `ALTER COLUMN ... TYPE` was planned while the
`DROP DOMAIN ... CASCADE` both columns depend on stayed. `schema apply
--auto-approve` exited 0, printed `Schema apply completed successfully`, and
left the table with only its `id` column and the row's other two values gone
([#1138](https://github.com/stokaro/ptah/issues/1138)).

A domain column now agrees only with a desired type that names the same domain,
in `ptah-compat schema diff`, `ptah-compat schema apply` and native
`ptah schema compare` alike. `information_schema.columns.domain_name` is what
separates a domain from a plain column of the same base type, and it is read
for that reason: a domain over an array is reported with `data_type` `ARRAY`
exactly like a plain array column, and an array's spelling is a type that must
keep normalizing like one.

`domain_schema` is read beside it, because a domain's identity is both halves.
Measured on the same server, one database holding `public.status` and one
holding `other.status`, over a table with one row:

| | plan for the column | outcome of `schema apply --auto-approve` |
| --- | --- | --- |
| name alone | none; only `DROP DOMAIN IF EXISTS "status" CASCADE` | exit 0, `Schema apply completed successfully`, table left with `id` only |
| identity | `ALTER TABLE "t" ALTER COLUMN "s" TYPE other.status` ahead of the drop | the column and its row survive |

A desired type that names its schema is held to that schema exactly. One that
does not is resolved through the domain the desired schema declares by that
name; with nothing declaring it, the bare name decides on its own, since which
domain an unqualified reference reaches is a search-path question and Ptah does
not answer that for the server.

One case is still open and is stated rather than claimed closed: when the desired
schema declares the same bare name in two schemas, an unqualified reference to it
stays undecided, and a column that should move between those two domains is
reported only if one side spells its schema. Measured on PostgreSQL 17.10 with
`public.status` and `other.status` both declared and both schemas selected, a
column of `other.status` against a desired `public.status` reported
`Schemas are synced`. Nothing is dropped in that shape, so the cost is drift
rather than data loss. Deciding it needs the desired column to carry its domain's
schema instead of a type string.

The reverse pair was missed as well, and there the pinned binary was right where
Ptah was silent. A plain `integer` column against a desired schema that declares
`waypoint` and types the column with it:

| | plan |
| --- | --- |
| Atlas CE v1.3.0 | `ALTER TABLE "t" ALTER COLUMN "a" TYPE waypoint;` |
| Ptah, before | `Schemas are synced, no changes to be made.` |
| Ptah, now | the same `ALTER`, executed at exit 0 with the row intact |

A desired type names a domain when the desired schema declares one by that
name, which is how every source Ptah reads carries it. A bare name with no
declaration behind it stays an ordinary type name.

## Reports

- Offline corpus report:
  [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md)
- Live round-trip report:
  [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md)
- Atlas CE differential report:
  [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
- External ORM provider report:
  [`gaps-orm-providers.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-orm-providers.md)
- CLI surface report:
  [`cli-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md)
- Parity scope:
  [`PARITY.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md)

## Local Commands

From this repository:

```bash
make conformance
```

The repository's Atlas-oracle workflow independently rebuilds Atlas CE from an
immutable source archive, verifies that the release tag resolves to the locked
commit, checks the committed SHA-256 digest and exact
`atlas community version v1.3.0` output, then runs the differential migration
sum tests and regenerates the committed corpus. Reproduce that oracle locally:

```bash
scripts/build-atlas-ce-oracle.sh
GOWORK=off \
  PTAH_ATLAS_ORACLE="$PWD/bin/atlas-ce-oracle" \
  PTAH_ATLAS_FUZZ_N=200 \
  go test -count=1 \
  -run '^TestSumFileNamesDifferentialFuzz(RealisticFlyway|OtherFormats)?$' \
  ./internal/atlasmigrateimport
internal/atlasmigrateimport/testdata/ce-sums/regenerate.sh \
  "$PWD/bin/atlas-ce-oracle"
git diff --exit-code -- internal/atlasmigrateimport/testdata/ce-sums
```

Atlas's GitHub release publishes no CE binary asset. The lock therefore pins
the release tag's commit and the digest of its immutable source archive. The
archive is built only into a disposable external test executable; no Atlas
source or compiled code is imported, vendored, or linked into Ptah. Atlas Cloud
and commercial binaries are outside this oracle workflow.

From `ptah-atlas-conformance`:

```bash
make probe        # regenerate gaps.md / gaps.json
make budget       # offline regression budget
make gate         # full offline parity gate
make probe-live   # live DB round-trip report
make budget-live  # live DB regression budget
make gate-live    # full live parity gate
make probe-diff   # Atlas CE differential report
make budget-diff  # Atlas CE differential regression budget
make probe-orm-providers   # pinned GORM and SQLAlchemy provider report
make budget-orm-providers  # ORM provider regression budget
make gate-orm-providers    # full ORM provider gate
make probe-cli-surface   # Atlas CE CLI surface report
make budget-cli-surface  # Atlas CE CLI surface regression budget
make gate-cli-surface    # full CLI surface parity gate
```

Live and differential commands require real database URLs, and the differential
tier also requires an Atlas CE binary built from the pinned `atlas.version` in
the conformance repository.

## External Schema Coverage

The deterministic offline report includes a 20-observation external-schema
workflow. It covers static SQL files; external programs that emit SQL, HCL, and
YAML; the opt-in trust boundary for render, compare, drift, plan, and generate;
configuration and explicit CLI sources; migration generation and application
to an ephemeral SQLite database; table, primary-key, unique-index, and
cascading-foreign-key facts; and converged compare, drift, plan, and generate
results.

A separate ORM-provider tier installs pinned GORM and SQLAlchemy providers in
temporary isolated environments. Its regression-budget job requires the
committed report to remain current without adding non-OK results, while its
independent full gate requires every provider-output and Ptah-render
observation to pass.

This evidence applies to Ptah's native external-program source. It does not
claim evaluation of Atlas HCL `data.external_schema`, which remains a distinct
Atlas project-language feature.
