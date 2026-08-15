---
title: Conformance
description: Where Atlas compatibility evidence lives and how to interpret it.
---

Atlas compatibility evidence is maintained in [`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance).

The conformance repository keeps Atlas Apache-2.0 fixtures outside Ptah's MIT source tree and imports Ptah as the system under test:

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

## Current summary

The authoritative current numbers live in the conformance repository reports:

- [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md)
- [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md)
- [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
- [`gaps-orm-providers.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-orm-providers.md)
- [`cli-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md)
- [`PARITY.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md)

Green conformance reports mean that the current measured corpus has no red
results. They do not, by themselves, prove every Atlas OSS command, flag,
dialect feature, and output mode. Use the comparison gap register for product
and coverage gaps that are outside the current measured corpus.

## CE oracle policy

Atlas CE comparisons run the compatibility binary with
`PTAH_ATLAS_STRICT_COMPAT=1`. That opt-in policy constructs the CE command and
flag tree and refuses extension inputs or inspected live objects before output
or mutation. The normal `ptah-compat` surface does not set it and retains
implemented Atlas Pro-like and best-effort capabilities.

Strict inspection removes PostgreSQL's server-installed `plpgsql` extension
and baseline `PUBLIC USAGE` grant from the snapshot it renders. Full mode keeps
the original reader snapshot. Strict cleanup executes the validated and
confirmed plan itself. On PostgreSQL it locks every planned table, repeats the
strict inventory through the transaction session, compares the rebuilt cleanup
plan with the confirmed plan, and refuses catalog drift before the first drop.
A trigger, policy, view, or foreign key created while the prompt is open cannot
disappear with its table.

Strict schema workflows refuse YAML sources and an authored `schema apply` lint
policy that the CE execution path cannot enforce. Commands that execute,
convert, or replay migration bodies refuse Atlas txtar, every Ptah directive —
including malformed or bare `-- +ptah` markers — and SQL templates;
checksum-only reads preserve those bytes. Desired migration directories are
captured and validated before a target or dev database is opened or a migration
lock is acquired, and replay uses that same stable snapshot. Default mode keeps
the extensions.

After a target connection opens, strict `schema apply` inventories an explicit
`--schema` scope before acquiring the apply lock or replaying a desired
migration directory. Without that explicit scope, a PostgreSQL-family target
inventories the user realm because desired replay may name a schema beyond the
URL's `search_path`. The locked planning phase validates it again before
producing or executing a plan. Default mode performs no supplemental
strict-policy inventory.

Strict process startup also rejects both Atlas-facing `PTAH_*` flag bindings
and native aliases consumed after forwarding, such as
`PTAH_MIGRATIONS_DIR`. Ordinary environment variables used by `getenv` in
`atlas.hcl` remain available.

A required companion change in the separate conformance harness
([`stokaro/ptah-atlas-conformance#277`](https://github.com/stokaro/ptah-atlas-conformance/pull/277))
must keep the two environments separate: CE parity probes inject strict mode
into each subprocess, while Pro-retention and native Ptah probes leave it
absent. Until that change lands, invoke CE probes with per-process injection
rather than enabling strict mode for the whole harness. Strict mode still keeps
deliberate safety and correctness improvements, so a green result never
depends on copying a CE behavior that silently drops authored content or
corrupts migration state.

### SQL inspect statement terminators

Finding 4.6 in
[`stokaro/ptah#1235`](https://github.com/stokaro/ptah/issues/1235) was measured
on August 7, 2026, against the pinned Atlas CE v1.3.0 binary:

| Result | Empty SQLite database | Populated SQLite database |
| --- | --- | --- |
| Atlas CE v1.3.0 | 0 bytes | no semicolon-only lines |
| Ptah before | `;\n` | a semicolon-only line after every statement |
| Ptah now | 0 bytes | no semicolon-only lines |

The shared report serializer now keeps each renderer-produced statement
verbatim instead of adding another terminator. An indent argument still
prefixes every line of nonempty SQL, while empty SQL stays empty. Exact tests
cover `ptah schema inspect --format sql`, the Atlas-compatible
`ptah-compat schema inspect --format '{{ sql . }}'`, and HCL and JSON controls.
This closes only finding 4.6; the issue's comment, indentation, view, and object
ordering findings remain separate.

## How to read green and red checks

The conformance repository separates regression budgets from full parity:

| Gate type | Meaning |
| --- | --- |
| Regression budget | No new gaps beyond the accepted budget for that contour. Should stay green. |
| Full conformance | Every checked case in that contour passes. May stay red while the measured corpus still has non-OK results. |

A green regression-budget check does not mean Ptah has full Atlas OSS parity.
A red full-conformance check is expected while the report still lists measured
non-OK results.

Even when both regression-budget and full-conformance checks are green, the
claim is limited to the corpus represented by the generated reports. Expanding
live and differential coverage is tracked in
[`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167).

## `migrate new` success streams

Findings 3.1 and 3.2 in
[`stokaro/ptah#1235`](https://github.com/stokaro/ptah/issues/1235) were measured
against the pinned Atlas CE v1.3.0 binary on August 11, 2026. Both tools exit 0
and write the same migration and `atlas.sum` artifacts; only their process
output differed.

| Directory layout | Pinned binary | Ptah before | Ptah now |
| --- | --- | --- | --- |
| Atlas | Stdout and stderr are byte-empty | Stdout names the migration by absolute path | Both streams are byte-empty |
| Converted | Stdout and stderr are byte-empty | Stdout names one or two migrations by absolute path | Both streams are byte-empty |

The change is limited to the `ptah-compat migrate new` adapter. Migration
names, file contents, `atlas.sum`, editor execution, warnings, and failure
diagnostics remain unchanged. Native `ptah migrations create` still reports the
paths it creates.

Re-run the focused evidence from the Ptah repository:

```bash
go test ./cmd/atlas -run '^TestCompatMigrateNew' -count=1
go test ./cmd/ptah-compat -run '^TestCompatBinaryMigrateNew' -count=1
go test ./cmd/ptah -run '^TestPtahNativeMigrationsCreateKeepsSuccessReport$' -count=1
```

## Workflow parity

Each workflow below states the native Ptah command, the Atlas-compatible
surface, what Atlas CE does, and the evidence. This table is the index; the
sections carry the detail.

| Workflow | Native Ptah | Atlas CE |
| --- | --- | --- |
| [Declarative migration and schema tests](#declarative-migration-and-schema-tests) | `ptah migrations test`, `ptah schema test` | Cannot run either command |
| [Migration directory maintenance](#migration-directory-maintenance) | `ptah migrations edit`, `rebase`, `rm` | Cannot run any of the three |
| [Verified and reported rollback](#verified-and-reported-rollback) | `ptah migrations down --shadow-db` | `migrate down` is absent |
| [Pre-approved declarative plans](#pre-approved-declarative-plans) | Same engine as `schema apply` | `schema plan` aborts |

This is a workflow-parity record, not a claim of full Atlas Pro compatibility.
For the code-by-code status of the analyzer checks Atlas marks as Pro, see
[Lint rules](../../reference/lint-rules/).
The `atlas migrate ls`, `migrate show`, `schema stats`, and
`schema validate` verbs are absent from the pinned Atlas CE v1.3.0 binary and
are triaged in the comparison gap register rather than measured here.

### Declarative migration and schema tests

**Native Ptah.** `ptah migrations test` and `ptah schema test`

**Atlas-compatible Ptah surface.** `ptah-compat migrate test` and `ptah-compat schema test` forward to the native runners with Atlas-shaped flags and exit codes

**Atlas CE.** Cannot run either command; the framework is outside the open-source core

**Evidence.** Unit coverage (including the Atlas-compatible forwards) plus integration-tagged live PostgreSQL runner tests; not counted as a schema-object fixture


### Migration directory maintenance

**Native Ptah.** `ptah migrations edit`, `rebase`, and `rm`

**Atlas-compatible Ptah surface.** `ptah-compat migrate edit`, `rebase`, and `rm` forward to the native commands with Atlas-shaped flags and `{name | version}` positionals; the `--edit` flags on `migrate new`, `migrate diff`, and `schema apply` open the operator's editor

**Atlas CE.** Cannot run any of the three verbs; they abort with the community-version boundary

**Evidence.** Unit coverage with hermetic editor scripts, including `ptah migrations validate` passing on the mutated directory; not counted as a schema-object fixture


### Verified and reported rollback

**Native Ptah.** `ptah migrations down --shadow-db` replays the rollback plan on a disposable shadow database before the target is touched

**Atlas-compatible Ptah surface.** `ptah-compat migrate down --dev-url` maps to the shadow verification, and `--format` renders an Atlas Go-template down report (`.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, `.Error`); real rollbacks never read stdin, matching Atlas, while native `ptah migrations down` keeps its prompt; the forward defaults to the Atlas revision-table layout (`--revision-format atlas`) but retains Ptah's recoverable failed-down bookkeeping, with the native `--revision-format ptah` pass-through as the layout escape hatch; `--to-tag`, `--skip-checks`, and `--plan` are recorded registry-bound waivers that fail loudly.

**Atlas CE.** `migrate down` does not exist in the community binary; the CE notice lists down migrations among excluded features

**Evidence.** Unit coverage over live SQLite: verification success and pre-target abort on both paths, report rendering including partial-failure reports, waiver rejections, non-interactive execution with EOF stdin, rejection of the non-Atlas `--confirm` flag, byte-identical execution output against a pre-approved native run, and revision-format regressions proving a bare `ptah-compat migrate down` reverts revisions written by `ptah-compat migrate apply`. A subprocess test runs the built `ptah-compat` binary with EOF stdin and checks the SQLite end state


### Pre-approved declarative plans

**Native Ptah.** Ptah plans and applies declarative schema changes through the same engine that powers `schema apply`

**Atlas-compatible Ptah surface.** `ptah-compat schema plan` saves the computed plan in the Atlas `.plan.hcl` format by default (a `.json` output path keeps the native fingerprinted JSON plan); `ptah-compat schema apply --plan file://<path>` reads both formats, Atlas-authored files included — JSON plans execute only after the live database matches the plan's source fingerprint, Atlas-format plans require `--to` and are verified by dev-database replay plus the always-on post-apply end-state check; registry planning flags are recorded waivers; `schema plan new` and `schema plan validate` are implemented, while `approve`, `list`, `pull`, `push`, `rm`, `lint` and `test` stay boundary stubs

**Atlas CE.** `schema plan` aborts with the community-version boundary; the plan/approval flow is bound to the Atlas Pro registry

**Evidence.** Unit coverage over live SQLite: plan computation and save, plan execution with schema assertions, stale-plan refusal after target drift, dry-run, dialect mismatch, malformed documents, and waiver rejections. Validation consumes a versioned Atlas-authored plan bundle independently of Ptah's writer and rejects source, desired-schema, SQL, statement-set, HCL, and malformed-hash mutations without changing target schema or rows. The bundle records known capture provenance and artifact hashes; destructive dev-database guards cover percent-encoded/path/query-option/symlink/hard-link aliases, driver endpoint/database overrides, and fail-closed comparison errors. This workflow is not counted as a schema-object fixture

## Local commands

From the Ptah repository:

```bash
make conformance
```

Ptah's own CI also rebuilds the pinned Atlas CE oracle from an immutable source
archive on every run. It verifies the release tag's locked commit, the archive
SHA-256, and exact version output. It then runs the migration-directory query,
migrate-apply interoperability, and Flyway revision-identity controls through
both command-line processes, runs differential migration-sum tests, regenerates
the recorded corpus, and fails if the committed corpus changes. This is a black-box
executable used only by tests; Atlas source and compiled code are not imported,
vendored, or linked into Ptah.

Atlas Cloud and commercial binaries are outside this oracle workflow.

```bash
scripts/build-atlas-ce-oracle.sh
GOWORK=off \
  PTAH_ATLAS_ORACLE="$PWD/bin/atlas-ce-oracle" \
  go test -tags=integration -count=1 \
  ./integration/atlasoracle/migratedirquery
GOWORK=off \
  PTAH_ATLAS_ORACLE="$PWD/bin/atlas-ce-oracle" \
  go test -tags=integration -count=1 \
  ./integration/atlasoracle/migrateapply
GOWORK=off \
  PTAH_ATLAS_ORACLE="$PWD/bin/atlas-ce-oracle" \
  go test -tags=integration -count=1 \
  ./integration/atlasoracle/flywayrevision
GOWORK=off \
  PTAH_ATLAS_ORACLE="$PWD/bin/atlas-ce-oracle" \
  PTAH_ATLAS_FUZZ_N=200 \
  go test -count=1 \
  -run '^TestSumFileNamesDifferentialFuzz(RealisticFlyway|OtherFormats)?$' \
  ./internal/atlasmigrateimport
```

From `ptah-atlas-conformance`:

```bash
make probe
make budget
make gate
make probe-live
make budget-live
make gate-live
make probe-diff
make budget-diff
make probe-orm-providers
make budget-orm-providers
make gate-orm-providers
make probe-cli-surface
make budget-cli-surface
make gate-cli-surface
```

Live and differential probes require real database URLs. Differential probes also require an Atlas CE binary built from the pinned Atlas version in the conformance repository. CLI surface probes use the same pinned Atlas CE binary to compare command paths, help boundaries, flags, and runtime classifications.

## When to update reports

Update conformance after Ptah changes that affect Atlas command behavior,
schema parsing/rendering, migration directory semantics, live database
round-trips, or public compatibility APIs. Bump the Ptah module version in the
conformance repository, run `go mod tidy`, regenerate the relevant reports, and
let both regression and full-conformance checks show the expected state.

## External schema coverage

The deterministic offline report includes a 20-observation external-schema
workflow. It measures static SQL; external programs that emit SQL, HCL, and
YAML; trust denial without side effects for render, compare, drift, plan, and
generate; configuration and explicit CLI sources; migration generation and
application to ephemeral SQLite; table, primary-key, unique-index, and
cascading-foreign-key facts; and converged compare, drift, plan, and generate
results.

Pinned GORM and SQLAlchemy providers run in a separate tier so network-backed
dependency installation cannot weaken the deterministic corpus. The tier has
independent regression-budget and zero-gap full-conformance jobs.

This coverage measures Ptah's native external-program source. Atlas HCL
`data.external_schema` evaluation is a separate project-language feature and
is not implied by a green native provider report.
