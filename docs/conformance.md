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

## Known PostgreSQL Introspection Gaps

Reading a live PostgreSQL database currently loses index attributes that the
pinned Atlas CE v1.3.0 binary preserves. Measured on PostgreSQL 17.10 by
diffing an empty database against a source database with each binary and
replaying the emitted SQL into a fresh database with
`psql -v ON_ERROR_STOP=1`. These affect the live-database read path only; an
HCL source carrying the same attributes renders correctly.

| Attribute | Ptah reads it? | Replays? | Tracked in |
| --- | --- | --- | --- |
| Access method (`USING gin` / `gist` / `brin` / `hash`) | No, every index collapses to the btree default | psql exits 0 and leaves a btree index | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Operator class, for example `text_pattern_ops` | No, the opclass is dropped | psql exits 0 and leaves an index that no longer serves the queries it was built for | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Sort order (`DESC`, `NULLS FIRST`, `NULLS LAST`) | No, ordering is dropped | psql exits 0 and leaves an ascending index | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Domain used as a column type | The `CREATE DOMAIN` is emitted, but the column is flattened to the domain's base type | psql exits 0 and leaves a column without the domain's `CHECK` | [#1242](https://github.com/stokaro/ptah/issues/1242) |
| Expression index, for example `lower(name)` | Yes | Yes | fixed |

The first four replay without an error, so nothing reports the loss at the time
it happens. Treat a green replay of introspected PostgreSQL index DDL as
unverified until these are closed.

Domains are the one row where Atlas CE is also wrong, in the opposite
direction: it keeps the column's declared type and never emits the
`CREATE DOMAIN`, so its own output fails to replay with
`ERROR: type "..." does not exist`. Ptah emits the `CREATE DOMAIN` already, and
closing this gap means keeping both halves rather than matching CE.

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
