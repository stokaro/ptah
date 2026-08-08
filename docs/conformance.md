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
| Domain used as a column type | `information_schema.columns.domain_name` | The `CREATE DOMAIN` was emitted but the column was flattened to the domain's base type: psql exited 0 and left a column without the domain's `CHECK` | [#1242](https://github.com/stokaro/ptah/issues/1242) |

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
0 where CE's does not.

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

Only PostgreSQL is covered. MySQL, MariaDB, SQLite, ClickHouse, CockroachDB,
YugabyteDB and Spanner keep the comparison they had, because what makes two
indexes the same index is a per-dialect question and only PostgreSQL was
measured.

## SQLite: A Primary Key Is Not A NOT NULL

Ptah applied "a primary key column is NOT NULL" as if it were a rule of SQL. It
is a rule of most engines, and SQLite is not one of them. On a rowid table
`id INTEGER PRIMARY KEY` is an alias for the rowid: `pragma table_info.notnull`
reports 0, an explicit `INSERT INTO t (id) VALUES (NULL)` is accepted, and a
rowid is assigned for it. Ptah folded the two together in six places — the AST
node, the HCL reader, the HCL writer, the SQL-DDL parser's shared node, the
comparator, and the SQLite renderer — so the assumption survived being fixed
anywhere less than all of them.

Two things followed, both at exit 0, measured on 2026-08-08 against the pinned
Atlas CE v1.3.0 binary with each binary in its own directory
([`stokaro/ptah#1235`](https://github.com/stokaro/ptah/issues/1235), findings
5.1 and 6.3):

| Command | Before | Pinned Atlas CE v1.3.0 |
| --- | --- | --- |
| `schema apply --to file://users.hcl --auto-approve`, key column `null = false` | wrote `"id" integer PRIMARY KEY`, dropping the declared NOT NULL. Asked whether that database matched the file it came from, the pinned binary planned a **full table rebuild** | wrote `` `id` integer NOT NULL ``, and answered `Schemas are synced, no changes to be made.` |
| `schema inspect --format '{{ json . }}'` over `id INTEGER PRIMARY KEY` | `"null"` omitted, meaning NOT NULL — the only column in the fixture whose flag the two binaries disagreed about | `"null": true` |

Both directions are now the source's answer, and both are pinned: a declared
NOT NULL survives onto the key column, and a key column declared `null = true`
does not acquire one. After the fix the pinned binary answers
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
