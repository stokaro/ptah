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

Workflow capabilities that do not add schema objects are recorded separately
from the round-trip corpus:

| Workflow | Native Ptah | Atlas-compatible Ptah surface | Atlas CE | Evidence |
| --- | --- | --- | --- | --- |
| Declarative migration and schema tests | `ptah migrations test` and `ptah schema test` run YAML/Go-authored cases locally. | `ptah atlas migrate test` and `ptah atlas schema test` forward to the native runners with Atlas-shaped flags (`--dir`/`-u --url`, `--dev-url`, `--run`, project flags) and the native exit-code contract. | Cannot run either testing command; the framework is outside the open-source core. | Unit tests cover parsing, assertions, reporting, and CLI behavior, including the Atlas-compatible forwards; integration-tagged PostgreSQL tests exercise both live runners. This workflow is not counted as a schema-object round-trip fixture. |
| Migration directory maintenance | `ptah migrations edit`, `rebase`, and `rm` mutate the directory and atomically rewrite the integrity file. | `ptah atlas migrate edit`, `rebase`, and `rm` forward to the native commands with Atlas-shaped `--dir`/`--dir-format` flags, `{name \| version}` positionals, and project flags; `migrate new --edit`, `migrate diff --edit`, and `schema apply --edit` open the operator's `$VISUAL`/`$EDITOR`. | Cannot run any of the three verbs; they abort with the community-version boundary. | Unit tests cover the forwards with hermetic editor scripts and assert `ptah migrations validate` passes on the mutated directory. Multi-version rebase and version ranges are rejected loudly (single-version forwarding only). |
| Verified and reported rollback | `ptah migrations down --shadow-db` replays the rollback plan on a disposable shadow database before the target is touched. | `ptah atlas migrate down --dev-url` maps to the shadow verification, and `--format` renders an Atlas Go-template down report (`.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, `.Error`); the forward defaults to Atlas revision bookkeeping (`--revision-format atlas`, like `migrate set`), with the native `--revision-format ptah` pass-through as the escape hatch; the registry-bound `--to-tag`, `--skip-checks`, and `--plan` flags are recorded waivers that fail loudly with their rationale. | `migrate down` does not exist in the community binary; the CE notice lists down migrations among excluded features. | Unit tests over live SQLite cover verification success and pre-target abort on both paths, report rendering (including partial-failure reports), the waiver rejections, a byte-identity regression pinning the default forward output to the native command's output, and revision-format regressions proving a bare `atlas migrate down` reverts revisions written by `atlas migrate apply` while an explicit ptah override leaves them untouched. |
| Pre-approved declarative plans | Ptah plans and applies declarative schema changes through the same `internal/atlasschema` engine that powers `schema apply`. | `ptah atlas schema plan` saves the computed plan to a local JSON plan file with ordered statements, per-statement safety severity, and SHA-256 source/desired schema fingerprints; `ptah atlas schema apply --plan file://<path>` executes the saved statements only after the live database matches the plan's source fingerprint, refusing drifted targets. Registry planning flags (`--push`, `--pending`, `--repo`, `--auto-approve`) are recorded waivers and the plan registry sub-verbs stay CE boundary stubs. | `schema plan` aborts with the community-version boundary; the plan/approval flow is bound to the Atlas Pro registry. | Unit tests over live SQLite cover plan computation and save, the saved-file contract, plan execution with schema assertions, stale-plan refusal after target drift, dry-run, declined confirmation, dialect mismatch, malformed documents, waiver rejections, and both entry points. This workflow is not counted as a schema-object round-trip fixture. |

These rows record product workflow parity, not full Atlas Pro compatibility.
The Atlas-compatible test verbs run Ptah-native YAML/Go test cases; Atlas
`.test.hcl` files are not ingested.

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

## Upstream Atlas Verbs Beyond the CE Pin

`atlas migrate ls`, `atlas migrate show`, `atlas schema stats`, and
`atlas schema validate` appear in current Atlas documentation but are entirely
absent from the pinned conformance Atlas CE v1.2.0 binary (each resolves to
`unknown command`, not a community-version abort stub), so they are outside the
CLI-surface parity target today. Triage outcome, to revisit when the
conformance Atlas pin advances past v1.2.0:

| Atlas verb | Current Atlas docs behavior | Triage |
| --- | --- | --- |
| `migrate ls` | List migration files in the directory (`--latest`, `--short`). | Covered by native: `ptah migrations status` lists every migration with version, description, and applied/pending state. A thin drop-in forward is future work once the pin advances. |
| `migrate show` | Print the contents of one or more migration files. | Future work: no native verb prints a migration's SQL (the files are plain SQL on disk). A thin drop-in forward is a candidate once the pin advances. |
| `schema stats` | Inspect database schema statistics in OpenMetrics format. | Out of scope: statistics monitoring is a metrics/observability surface, not schema management; Ptah's schema-state surface is `ptah schema compare` and `ptah schema drift`. |
| `schema validate` | Check that a schema definition parses and loads, optionally against `--dev-url`. | Covered by native: `ptah schema render` parses and loads the desired schema and fails on invalid input; `ptah schema test` and `schema apply --dry-run` exercise it against a throwaway database. |

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
