---
title: Feature matrix
description: Ptah and Atlas capabilities side by side, with the evidence behind every row.
---

This page answers one question: for a given capability, what does Ptah do, what
does the open Atlas community binary do, and what does Atlas keep in its
licensed builds. Every row cites the evidence it rests on.

It is a status index, not an argument. The per-area detail behind these rows is
on [Comparison](../comparison/), the measured evidence is on
[Conformance](../conformance/), and the Atlas-documentation crosswalk is on
[Atlas docs coverage](../docs-coverage/).

## How to read the tables

| Symbol | Meaning |
| --- | --- |
| ✅ | Supported today |
| 🟡 | Partial. The difference column states what is missing |
| ❌ | Not implemented |
| ➖ | Does not apply to that product |
| ❔ | Not established by the evidence this page uses |

Each table has the same columns. **Ptah**, **CE**, and **Pro** carry one symbol
each:

- **Ptah** — the native `ptah` binary plus the separate `ptah-compat` drop-in.
- **CE** — the pinned Atlas community binary, version 1.2.0, which the
  conformance harness runs against.
- **Pro** — capabilities in Atlas's licensed builds, established either by the
  [Atlas feature availability](https://atlasgo.io/features) page and
  [pricing page](https://atlasgo.io/pricing), or by direct measurement of a
  licensed Atlas build run locally against disposable SQLite databases.

Every Atlas cell has to come from an Atlas-side source: the command, usage, and
flag inventory the conformance harness reads out of the pinned community
binary, measured behavior of an Atlas binary, or a classification Atlas
publishes. Measurement outranks published classification when they disagree.
Where nothing settles a question, the cell is ❔ rather than a guess.

## Atlas plans are not the CE column

Atlas's public plans are Starter (free), Pro, and Enterprise, and Atlas's own
[pricing page](https://atlasgo.io/pricing) classifies capabilities by plan.
That classification and the **CE** column answer different questions: the CE
column reports what the pinned community binary does when run logged out, and
the two diverge in both directions. Both examples below were measured on
2026-08-01 against CE v1.2.0:

- The pricing page places migration linting outside the Starter plan, yet the
  CE binary runs `migrate lint` logged out and reports destructive changes.
- The pricing page checks ERD visualization for Starter, yet the CE binary
  rejects `schema inspect --web` as an unknown flag; the ERD lives in the
  hosted service, not in the binary.

Where the pricing page settles a Pro-side question, the Pro column cites it.
Where plan marketing and measured binary behavior differ, the measured behavior
wins the CE cell and the difference column records the tension. That is why the schema-object rows carry ❔
in the Atlas columns: this page can show what Ptah emits for a domain or a
trigger, but nothing it cites establishes what Atlas CE emits for the same
object.

:::caution
A ✅ in the Ptah column means the capability works, not that it is
byte-identical to Atlas. Ptah is an independent pre-GA implementation, and the
conformance repository states plainly that no number it produces is a
full feature-set parity test. Read the difference column before relying on a
row for a migration decision.
:::

## At a glance

Across the 153 capabilities below:

| Reading | Count |
| --- | --- |
| Ptah supports it fully | 80 |
| Ptah supports it with a stated limitation | 47 |
| Ptah does not implement it | 26 |
| Ptah and Atlas CE both support it | 24 |
| Ptah implements it openly where Atlas gates it behind Pro or Cloud | 31 |
| Ptah has it and neither Atlas edition does | 14 |
| Atlas CE has it and Ptah does not, or only in part | 24 |
| An Atlas column is ❔ — not established by this page's evidence | 23 |

Every 🟡 in the Ptah column names its specific limitation, reproduced against a
binary built from this repository; Atlas-column verdicts rest on the cited
Atlas-side sources only. Confirmed gaps are tracked in
[#926 to #942](https://github.com/stokaro/ptah/issues/926) and [#944](https://github.com/stokaro/ptah/issues/944).

The command surface is counted separately, because it is measured rather than
assessed. The conformance harness inventories every command in the pinned Atlas
CE binary and compares it with the `ptah-compat` surface: 19 of the 37
inventoried commands are open parity targets, and every one of them matches on
help usage and flags — 107 observations, no gap. The remaining 18 are registry,
Cloud, or Pro verbs that are not drop-in targets. Ptah implements seven of them
as open capabilities regardless.

## Schema sources

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Annotated Go models from a live database | ✅ | ❌ | ❌ | Writes Ptah annotation source from introspection, with optional db/json tags. No Go-model generator in the CE inventory or Pro list. |
| Atlas CE inspect HCL parses back into Ptah | ✅ | ✅ | ✅ | 30 atlas-differential observations parse Atlas CE 1.2.0 `schema inspect` HCL on Postgres/MySQL/SQLite with zero schema-fact mismatches. |
| Atlas HCL data "external_schema" | ✅ | ❌ | ✅ | Ptah evaluates the data source and runs the program, gated behind `--allow-external-schema`/`PTAH_ALLOW_EXTERNAL_SCHEMA`. Community Atlas rejects `data.external_schema`. |
| Composite multi-source desired schema | ✅ | ❌ | ✅ | Repeatable `--root-dir`/`--schema-file` merge into one schema; conflicts error. Repo docs cite composite_schema as an Atlas Pro data source. |
| Desired-schema artifacts in an OCI registry | ✅ | ❌ | ✅ | `ptah schema push/pull` publish and fetch canonical HCL resolved from Go, YAML, HCL or SQL sources. Verified round trip against registry:2. |
| Directory of .hcl files as one schema source | ❌ | ❔ | ✅ | `--schema-file dir` and `--to file://dir` are refused (schema file is a directory); globs are not expanded. Multi-file needs one flag per file. |
| External program / ORM loaders | ✅ | ✅ | ✅ | `--schema-cmd` or `ptah.yaml` external_schema (needs `--allow-external-schema`) runs a program without a shell emitting SQL, HCL, or YAML. |
| Go struct annotations | ✅ | ❌ | ❌ | Ptah parses //ptah:schema:* comments into the desired schema. Atlas's route to Go models is an external ORM provider program. |
| HCL foreign_key deferrable | ❌ | ❔ | ❔ | Errors: unsupported foreign_key attribute "deferrable". DEFERRABLE is absent from the whole Ptah IR, so YAML and Go annotations lack it too. |
| HCL function calls in schema files | 🟡 | ❔ | 🟡 | sql() unwraps in column default/on_update/unique_expr/check, index ops, domain check only; in type, check.expr, index.where it leaks literally. |
| HCL locals, lock, atlas, dynamic/for_each | ❌ | ❔ | 🟡 | Named rejections, rejected (`ptah-compat` exit 1, native `ptah` exit 2): unsupported top-level block "locals"/"lock"/"atlas"; unsupported table block "dynamic". |
| HCL table and column child blocks | ✅ | ❔ | ❔ | column, primary_key, index, unique, foreign_key, check, partition, row_security, constraint, platform; column nests as, identity, platform. |
| HCL top-level blocks Ptah parses | ✅ | ❔ | ❔ | schema, enum, table, extension, sequence, domain, composite, range, function, view, materialized, trigger, policy, role, permission, data. |
| HCL variable blocks and var.* references | ❌ | ✅ | ✅ | `variable` is parsed then discarded and `var.x` reaches DDL as the literal text `var.x`, exit 0 — no substitution and no error. |
| Live database as desired state | ✅ | ✅ | ✅ | One connectable DB URL can be the desired side of compat schema apply/diff and migrate diff; `ptah db read` introspects natively. |
| Live database to Go annotation source | ✅ | ❌ | ❌ | `ptah introspect` writes annotated Go models from a live DB; repo docs record Go annotations as a first-party Ptah workflow. |
| Migration directory as a source | ✅ | ✅ | ✅ | Atlas-format directory with `atlas.sum`, replayed on a required `--dev-url`. Works on `ptah schema inspect` and compat apply/diff/migrate diff. |
| Ptah-only HCL schema extensions | ✅ | ❔ | ❔ | platform/override per dialect, EXCLUDE constraint block, column enum, table checks/custom, index ops, ClickHouse granularity, seed data block. |
| SQL DDL schema files | ✅ | ✅ | ✅ | Accepted by native `--schema-file` and by `ptah-compat schema apply`/diff `--to`/`--from`; unsupported DDL fails instead of being skipped. |
| YAML schema files | ✅ | ❌ | ❌ | Strict parser; unknown keys fail. Repo docs list Atlas OSS data sources as SQL, HCL, external schema, and remote/template dirs. |

## Declarative and direct schema changes

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `--dry-run`, `--auto-approve`, and `--edit` | ✅ | ✅ | ✅ | All three registered and functional; `--edit` opens $VISUAL/$EDITOR and the edited SQL is what gets planned, rehearsed, and applied. |
| `--exclude` glob and type selectors | 🟡 | ✅ | ✅ | Resource globs plus one final-segment [type=...]; schema-qualified globs never match default-schema tables, functions or enums. |
| `--include` resource selectors | ✅ | ❌ | ✅ | CE registers `--include` on apply/diff but aborts it as non-community; pinned CE inspect has no `--include`. Ptah selects with union semantics and cross-scope dependency diagnostics. |
| `--schema` / -s scoping of both sides | ✅ | ✅ | ✅ | Names define the schema universe for apply and diff; repeated and comma-separated values union deterministically. |
| `schema inspect --include` filtering | ❌ | ❌ | ✅ | Flag absent from the pinned Atlas CE v1.2.0 inspect flags; Ptah rejects it as unknown. The licensed build registers it and it filters on SQLite. |
| Apply advisory lock and `--lock-timeout` | ✅ | ✅ | ✅ | Real locks on PostgreSQL, YugabyteDB, MySQL, MariaDB, SQL Server; SQLite, ClickHouse, CockroachDB, Spanner run unlocked with a note. |
| Desired-state sources for `--to` and `--from` | 🟡 | ✅ | ✅ | Files, one DB URL, one atlas.sum dir, or env://. A plain file:// schema directory without atlas.sum is rejected; atlas:// fails early. |
| Dev-database rehearsal before apply | ✅ | ✅ | ✅ | Dev DB reset, target schema recreated, exact plan rehearsed; dev==target and failed rehearsal abort. docker:// dev URLs have their own row. |
| Drift detection against desired schema | ✅ | ❌ | ✅ | Native `ptah schema drift`: `--severity`, `--exit-code`, `--ignore`, text/json/github-actions. Atlas Cloud drift monitoring is out of scope. |
| Go-template `--format` output | 🟡 | ✅ | ✅ | schema apply and diff register only the sql helper: {{ json . }} fails to parse. json/hcl/mermaid/split/write and .Realm are inspect-only. |
| Inspect `--exclude` field selectors | 🟡 | ❔ | ❔ | Only [type=extension].version is honored; other .field suffixes and non-final [type=...] fail before any database is contacted. |
| Inspect non-database sources via `--dev-url` | ✅ | ✅ | ✅ | Schema file, `atlas.sum` migration dir, or env:// is materialized on a reset dev DB then introspected; without `--dev-url` it fails. |
| Inspect split/write file exports | ✅ | ❌ | ✅ | `{{ hcl . \| split \| write "dir" }}` writes object/schema/type trees; pinned Atlas CE rejects split, write, hcl as non-community. |
| JSON output for native schema diff | ✅ | ❔ | ❔ | Native ptah schema diff `--format` json emits a machine-readable statements document; the Go-template row only states that {{ json . }} fails on the compat diff, leaving native JSON unstated. |
| Local pre-approved plan files | ✅ | ❌ | ✅ | `schema plan` writes Atlas `.plan.hcl` by default (`.json` keeps the native plan); `apply --plan` reads both, Atlas-authored included, verified by replay against `--to` plus an end-state check. |
| schema apply against a live database | ✅ | ✅ | ✅ | Diffs `--url` against the `--to` desired state, prints the SQL plan, applies after confirmation. Verified end to end on SQLite. |
| schema clean | 🟡 | ✅ | ✅ | Plan and `--dry-run` list tables (plus PostgreSQL enums/sequences, SQL Server FKs), but the apply drops views too via DropAllTables. |
| schema diff between two schema states | 🟡 | ✅ | ✅ | SQLite refuses any change needing a table rebuild: column modify, NOT NULL change, constraint add/remove, enum CHECK change. Same on apply. |
| schema fmt (HCL canonical layout) | ✅ | ✅ | ✅ | Formats .hcl paths recursively and prints only changed files. Native `ptah schema fmt --check` adds a no-write CI gate. |
| schema inspect to HCL, SQL, or JSON | ✅ | ✅ | ✅ | Default HCL; `--format` sql\|json\|template. Native twin `ptah schema inspect` adds `--out-dir` and `--split` file export. |
| Schema-qualified exclude globs for enums and functions | 🟡 | ❔ | ❔ | Tables, views and extensions match schema-qualified exclude globs; enum and function filters compare the bare name only. |
| Upstream verb `schema stats` | ❌ | ❌ | ✅ | Beyond the CE pin: on the licensed build it exists as `schema stats inspect` (OpenMetrics) and rejects SQLite at runtime; the gap register triages it out of scope as observability. |
| Upstream verb `schema validate` | 🟡 | ❌ | ✅ | Beyond the CE pin: no validate verb; the gap register triage covers it with native schema render parse/load validation plus schema test and schema apply `--dry-run`. |

## Versioned migrations

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Apply pending migrations (apply/up) | 🟡 | ✅ | ✅ | Dry runs read stored revisions, select only pending files, and retain execution-time validation. ClickHouse Atlas-format table creation remains #950; Ptah-format is covered. |
| Atlas SQL template migrations (`--atlas-env`) | ✅ | ❔ | ❔ | Go-template actions in Atlas-format migration files render before execution with .Env set by `--atlas-env`; sibling *.sql files supply shared {{ template }} definitions. Flag on 10 migrations verbs. |
| Atlas txtar migration sections (-- atlas:txtar) | ✅ | ❔ | ✅ | `-- atlas:txtar` files execute migration.sql and down.sql sections and enforce checks.sql as a pre-migration gate (aborts before the body, Pro parity); other embedded files are ignored. |
| Atlas-format checkpoint output | ❌ | ❌ | ✅ | Writing stays a waiver (`--dir-format`=atlas); reading Atlas `-- atlas:checkpoint` dirs works: fresh databases bootstrap from the latest checkpoint, pre-checkpoint databases skip it silently. |
| Baseline an existing database | ✅ | ✅ | ✅ | Ptah adds a standalone baseline verb with `--shadow-db` verification and `--dry-run`; CE exposes baselining as migrate apply `--baseline`. |
| Create an empty migration file | ✅ | ✅ | ✅ | Native create writes an up/down pair; compat new writes one Atlas .sql skeleton, refreshes `atlas.sum`, `--edit` opens $VISUAL or $EDITOR. |
| Directory integrity file: hash and validate | ✅ | ✅ | ✅ | hash writes `ptah.sum`, or `atlas.sum` for atlas-format directories; validate checks it and with `--dev-url` cleans and replays the dir. |
| Directory maintenance: edit, rebase, rm | ✅ | ❌ | ✅ | Each rewrites `ptah.sum`/`atlas.sum` and refuses a migration applied in `--db-url` unless `--force`; CE aborts all three as non-community verbs. |
| Dynamic down planning (`migrate down --plan`) | ❌ | ❌ | ✅ | `--plan`, `--to-tag` and `--skip-checks` are recorded waivers that fail loudly; Ptah reverts only through pre-planned down files. |
| Execution order (`--exec-order`) | ✅ | ✅ | ✅ | linear fails on a pending migration below the current version, linear-skip warns and leaves it pending, non-linear applies it. |
| External `--dir-format` outside `migrate import` | ❌ | ✅ | ✅ | hash, lint, new, set, status and validate accept only `--dir-format`=atlas; other tool formats must first go through migrate import. |
| Failed rollback state is recorded and recoverable | ✅ | ❌ | ❌ | Native `ptah migrations down` marks the row failed with the error and completed-statement count, so status reports it dirty and `migrations repair` clears it; compat matches Atlas, which records none. |
| Flyway repeatable (`R__`) migration import | 🟡 | ✅ | ✅ | Compat import aborts the entire directory on any R__ file; native import rewrites R__ as a one-time migration, losing re-run-on-change. |
| Generate migrations from a schema diff | 🟡 | ✅ | ✅ | New versions are a Unix epoch in an empty dir and latest+1 otherwise, not the UTC YYYYMMDDHHMMSS stamp migrate new and Atlas dirs carry. |
| migrate apply `--allow-dirty` semantics | 🟡 | ✅ | ✅ | Flag gates a dirty revision row, not a non-empty target: a pre-populated database applies without it, and the recovery re-insert fails with UNIQUE on atlas_schema_revisions (SQLite). |
| Migration checkpoints (squash history) | ✅ | ❌ | ✅ | Replays the directory on `--shadow-db` into a cumulative checkpoint pair; CE has no checkpoint verb and Atlas lists it as Pro. |
| Migration import from other tools | 🟡 | ✅ | ✅ | Native import converts golang-migrate/Goose/Flyway/Liquibase-SQL to ptah format (`R__` becomes one-time); the compat path writes atlas format and rejects `R__`. Liquibase XML/YAML/JSON not parsed. |
| Migration linting | ✅ | 🟡 | ✅ | CE registers `migrate lint` with a basic Open rule set; Atlas's features page marks the lint CLI Pro. Ptah loads custom rules only through the Go API at compile time. |
| Migration lock and lock timeout | ✅ | ✅ | ✅ | Compat `--lock-timeout` bounds directory and dev-db locks; native splits per-migration `--lock-timeout` from `--migration-lock-timeout`. |
| Migration status report | ✅ | ✅ | ✅ | Compat status reads Atlas revision metadata and renders Go templates over .Env, .Available, .Applied, .Pending, .Current, .Next. |
| Online DDL routing via gh-ost or pt-osc | ✅ | ❔ | ❌ | ptah.yaml online_ddl.tool (ghost\|pt-osc), threshold_rows, args, and fallback (error\|plain) route large-table ALTERs through an online-DDL tool during migrations up/down. |
| Pre-migration database backups (`--pg-dump-to`) | ✅ | ❌ | ❌ | `--pg-dump-to` writes a pg_dump custom-format backup and `--mysqldump-to` a SQL backup before applying or rolling back; ptah.yaml key migration.pg_dump_to. |
| Pre-migration webhook and shell hook gates | ✅ | ❌ | ❌ | `--webhook` POSTs migration metadata and requires HTTP 200; `--pre-up-hook`/`--pre-down-hook` run a shell command that must exit 0, else the run aborts. Also ptah.yaml migration.webhook/pre_up_hook. |
| Prometheus metrics endpoint (`--metrics-addr`) | ✅ | ❌ | ❌ | migrations up, down, and status serve a Prometheus /metrics endpoint at the given address for the run. |
| Repair dirty or partial revision state | ✅ | ❌ | ❌ | `ptah migrations repair` `--resume-from` finishes remaining statements. No repair verb in the pinned CE inventory or reviewed Atlas evidence. |
| Revision table format and placement | ✅ | ✅ | ✅ | `--revision-format` ptah\|atlas plus `--migrations-table` and `--migrations-schema`; the compat path defaults to Atlas rows. |
| Roll back applied migrations (down) | 🟡 | ❌ | ✅ | Ptah validates all selected down bodies before changing state. Dry-run reports distinguish preflight rejection from attempted rollback. Registry flags remain waivers. |
| Set revision state to a version | ✅ | ✅ | ✅ | Removes revision rows above the target, keeps rows at or below it, and inserts missing rows through it as manually set. |
| Structured JSON log output (`--log-format`) | ✅ | ❌ | ❌ | migrations up, down, and status take `--log-format` text\|json and `--log-level` debug\|info\|warn\|error for machine-readable run logs. |
| Transaction modes (`--tx-mode` file/all/none) | ✅ | ✅ | ✅ | all is limited to transactional-DDL dialects and rejects no_transaction files, per-file timeouts, and pre-migration checks. |
| Upstream verb `migrate ls` | 🟡 | ❌ | ✅ | Beyond the CE pin: works on the licensed build against a local directory; ptah-compat rejects it as unknown command; native `ptah migrations status` lists versions and states. |
| Upstream verb `migrate show` | ❌ | ❌ | ✅ | Beyond the CE pin: prints a migration's SQL upstream; no compat or native Ptah verb exists, and the gap register triages it as future work. |

## Linting and safety

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Apply-time destructive-change gate | ✅ | ❌ | ➖ | migrations up refuses destructive pending files; .ptah-lint.yaml disabled-rules reopens the gate and ptah.sum does not hash that file. |
| Atlas Pro analyzer code coverage | 🟡 | ➖ | ✅ | OW101/OW102 have no rule; PG301, PG304, MY130, MY133, MY136 fire under broader codes (DS103, PG104, CD103, MY101), not dedicated ones. |
| Atlas web reports (`--web`) | ❌ | ❌ | ✅ | Not registered on migrate lint or schema diff; rejected as an unknown flag. Pinned Atlas CE v1.2.0 does not register it either. |
| CI integration (GitHub Action, annotations) | ✅ | ❔ | ❔ | stokaro/ptah-action@v1 posts a sticky PR comment; `--format` github-actions emits annotations. Atlas features page omits CI integrations. |
| Custom lint rules and check-level policy | 🟡 | ❌ | ✅ | Custom rules only from Go (lint.Register, Options.ExtraRules); atlas.hcl rule, review, naming, non_linear blocks and force all fail. |
| Default-firing Atlas analyzer concern mapping | ✅ | ➖ | ➖ | lint-analyzer-catalog maps every default-firing Atlas concern to a covering Ptah rule, severity and line; 0 gap on the committed corpus. |
| Generation-time destructive-change gate | ✅ | ❌ | ❌ | migrations generate and plan fail with `--check-destructive` when the generated SQL contains destructive statements; `--allow-destructive` reopens the gate. Distinct from the apply-time gate row. |
| Inline nolint suppression | 🟡 | ✅ | ✅ | Analyzer-name selectors suppress, but only codes DS102/DS103/MF103 map; atlas:nolint PG101 and unknown selectors are silently ignored. |
| Native migration lint rule set | ✅ | 🟡 | ✅ | 42 codes across 9 families, gated by `--dialect`. Atlas lists destructive and backward-incompatible rules Open; concurrent-index rules Pro. |
| Per-rule severity policy | 🟡 | ❔ | 🟡 | Severity vocabulary is warning\|error only (info errors out). Measured licensed build is no richer: analyzer and rule blocks reject a `severity` attribute; only boolean error toggles exist. |
| Pre-migration assertion checks | 🟡 | ❌ | ✅ | `-- +ptah check` and Atlas txtar checks.sql both enforce. `--tx-mode` all directs users to per-file mode; native `--skip-checks` remains an emergency bypass. CE leaves checks unenforced. |
| SARIF 2.1.0 lint report | ✅ | ❌ | ➖ | Native `--format` sarif emits SARIF 2.1.0 with ruleId, level and file:line; Atlas documents Go-template `--format` output for migrate lint. |
| Standalone SQL file linting (`ptah sql lint`) | ✅ | ❌ | ❔ | Lints arbitrary SQL files or stdin against per-dialect capability presets (9 dialects incl. sqlserver), refined by a `--version` server string; text/json output, rule disable. Not. |
| Statement safety classification report | ✅ | ➖ | ➖ | plan `--report` text\|html\|json and generate `--report` html\|json emit highest severity, a destructive flag, and per-statement assessments. |

## Testing

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Atlas `.test.hcl` ingestion | ❌ | ❌ | ✅ | No .test.hcl parser; a directory of them reports 'no test cases found' (`ptah-compat` exit 1, native exit 2). |
| Atlas-shaped migrate test / schema test verbs | 🟡 | ❌ | ✅ | schema test -u takes only a Go-annotation directory; SQL/HCL files and DB URLs fail. Neither verb exposes `--report` or `--seed-dir`. |
| Dev / shadow database verification | 🟡 | ✅ | ✅ | `--shadow-db` on generate, checkpoint, baseline and down; docker:// is refused, and schema apply `--dry-run` skips the rehearsal entirely. |
| Embeddable test runner (Go package) | ✅ | ❔ | ❔ | migration/dbtest exports RunMigrationTest and RunSchemaTest. Nothing cited establishes an Atlas Go test-runner package. |
| Exit-code contract for CI gates | ✅ | ✅ | ➖ | Native 0/1/2 separates expected negative results from command errors; ptah-compat collapses to Atlas CE 0/1, recovered panics still exit 2. |
| Migration test framework (`ptah migrations test`) | ✅ | ❌ | ✅ | Declarative YAML cases: migrate_to, apply_schema, seed, exec, assert. Fresh ephemeral SQLite per case unless `--db-url` is set. |
| Schema test framework (`ptah schema test`) | ✅ | ❌ | ✅ | Desired schema from Go annotations converges before steps; migrate_to is rejected. Atlas CE v1.2.0 registers no schema test verb. |

## Configuration and dev databases

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Atlas project config (atlas.hcl) | 🟡 | ✅ | ✅ | Rejects atlas, script, exporter, deployment; env for_each and schemas; migration baseline, skip_report, repo; data blocks but hcl_schema and external_schema. |
| atlas.hcl from the native ptah binary | 🟡 | ➖ | ➖ | ptah `--env` reads ./atlas.hcl only: no `--var` flag, and `--config` takes ptah.yaml, so a variable without a default cannot be resolved. |
| data "hcl_schema" reference | 🟡 | ✅ | ✅ | Takes path or paths and exports .url; the Atlas vars input, absolute paths, and any non-file:// value are rejected. |
| Docker dev databases (`docker://` `--dev-url`) | ❌ | ✅ | ✅ | migrate diff, lint and validate refuse docker:// and require a directly connectable dev database URL. |
| env:// desired-state references | 🟡 | ✅ | ✅ | Resolves only on `--to`/`--from` and only src, schema.src, url, dev, migration.dir; elsewhere (`--exclude`) the literal string is used silently. |
| Native project config (ptah.yaml) | ✅ | ➖ | ➖ | Keys url, dev, schemas, exclude, external_schema, migration, lint, migrate, diff, online_ddl. No variables or functions. Unknown keys fail. |
| PTAH_* environment-variable flag equivalents | ✅ | ❔ | ❌ | Every flag on every native verb has a documented PTAH_* environment variable ([env: PTAH_X] in help) that substitutes for the flag. |
| Remote and template directory sources | ❌ | ✅ | ✅ | An atlas.hcl data-source question, not a registry one: ptah-compat migration.dir takes file:// only. Native oci:// distribution is a separate ptah path. |
| Variables, locals, and HCL functions | 🟡 | ✅ | ✅ | Only file, fileset, format, getenv, jsonencode evaluate; variable type (string, number, bool, list(string)) and sensitive are honored; validation and env for_each are rejected. |

## Databases and schema objects

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| ClickHouse (clickhouse, ch) | 🟡 | ❌ | ✅ | Tables and indexes only. Views, matviews, functions, triggers, roles, grants, RLS, sequences, domains and CHECKs vanish from the plan with no diagnostic. |
| CockroachDB (cockroachdb, crdb) | 🟡 | ❌ | ✅ | Preset drops concurrent indexes, sequences, XML, advisory locks, roles and RLS. SERIAL is a hard error. Offline render also omits views, functions and triggers. |
| Domains, composite types, and range types | 🟡 | ❔ | ❔ | PostgreSQL only in `schema render`; `schema apply` also emits them on YugabyteDB. Range subtype changes produce no diff at all. |
| Enum types | 🟡 | ✅ | ✅ | MySQL/SQLite/SQL Server inline-enum rewrite fires only if the type name starts with `enum_`; other names emit the bare type name verbatim. |
| Extensions | 🟡 | ❔ | ❔ | PostgreSQL-family only; `plpgsql` ignored by default on compare. MySQL/MariaDB render an empty statement instead of the intended comment. |
| Functions | 🟡 | ❌ | ✅ | `schema render`: PostgreSQL only, silent on MySQL/MariaDB. `schema apply` also emits CREATE FUNCTION on YugabyteDB; MySQL drops it silently. |
| MySQL and MariaDB | 🟡 | ✅ | ✅ | Matviews are an explicit error; extensions, functions, domains, roles/grants, RLS and MariaDB SEQUENCE objects are dropped silently. DDL auto-commit blocks rollback. |
| Oracle, Snowflake, Redshift, Databricks | ❌ | ❌ | ✅ | No dialect entry; the names fail normalization the same way TiDB does. Listed as Atlas Pro drivers. |
| PostgreSQL 12+ (postgres, postgresql) | ✅ | ✅ | ✅ | Only engine where views, functions, sequences, roles, RLS and domains are emitted; presets 12-13, 14-16, 17+ from the server banner. |
| Roles, grants, and row-level security | 🟡 | ❌ | ✅ | PostgreSQL only in `schema render`; `schema apply` emits CREATE ROLE on YugabyteDB. SQL files reject ROLE, GRANT, POLICY, ENABLE RLS. Atlas prices this as Pro; CE no-ops a `role` block silently. |
| Spanner PostgreSQL interface (spanner) | 🟡 | ❌ | ✅ | Enums and FKs render as skip comments, SERIAL hard-errors, no dedicated driver (uses the PostgreSQL pgx path), and no live container or live test exists. |
| SQL Server and Azure SQL (sqlserver, mssql, tsql) | 🟡 | ❌ | ✅ | The mssql and tsql aliases silently drop views and triggers that sqlserver emits; render's default dialect list and `--dialect` help omit SQL Server; no sequences, RLS, roles/grants or matviews. |
| SQLite (sqlite, sqlite3) | 🟡 | ✅ | ✅ | Column drops do emit a full rebuild; type, nullability, default and uniqueness changes fail with "modifying columns ... requires a table rebuild plan". PG-only objects error one at a time. |
| Standalone sequences | 🟡 | ❌ | ✅ | PostgreSQL only in `schema render`; `schema apply` emits it on YugabyteDB too. SQL schema files reject CREATE SEQUENCE as unsupported. |
| TiDB and LibSQL | ❌ | ✅ | ✅ | Both names fail dialect normalization: "unsupported database dialect: tidb" / "...: libsql". No renderer, planner or driver entry. |
| Triggers | 🟡 | ❌ | ✅ | `--dialect mssql`/`tsql` drop them while `sqlserver` renders them; MySQL forces FOR EACH ROW; SQL Server rewrites BEFORE to AFTER silently. |
| Views and materialized views | 🟡 | ❌ | ✅ | Matviews render on PostgreSQL only. Elsewhere behavior differs by engine: MySQL/MariaDB apply errors, YugabyteDB live apply plans them, ClickHouse drops them silently. |
| YugabyteDB (yugabytedb, ysql) | 🟡 | ❌ | ✅ | Live apply does plan roles, grants, sequences, domains, views, matviews, functions and triggers; only RLS and concurrent indexes are gated off. Offline render omits all of them. |

## Go embedding and developer tooling

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Annotation metadata as JSON Schema | ✅ | ➖ | ➖ | Emits a JSON Schema describing every //ptah directive and attribute; Atlas has no //ptah annotation set for the concept to apply to. |
| API schema export: OpenAPI 3.0 and GraphQL | ✅ | ❌ | ❌ | Go annotations to OpenAPI components or GraphQL SDL; no handlers or resolvers. Absent from the CE inventory and the cited Pro list. |
| Concurrency-guarded migration plan publication | ✅ | ➖ | ➖ | generator.PlanMigration binds a plan to a directory snapshot; WriteFiles rejects changed history (ErrMigrationDirectoryChanged) under a cross-process lock; concurrent reuse fails (ErrMigrationPlanInUs |
| Go annotations to HCL export with cleanup | ✅ | ➖ | ➖ | schema export `--to` hcl writes an HCL schema from Go annotations; `--cleanup-go-annotations` removes the annotations after a lossless export, with `--cleanup-dry-run`/`--cleanup-diff` previews. |
| Pinned database sessions (WithSession) | ✅ | ➖ | ➖ | `WithSession` on `DatabaseConnection` pins one physical session for a callback, rebinding reader/writer/SQL runner, and discards the connection so session state cannot leak. |
| Protobuf schema export with pinned field numbers | ✅ | ❌ | ❌ | Edition 2023 output; `--out` pins field numbers, with policies for type removal, name reuse and incompatible change. Not in CE inventory. |
| ptah-ls annotation language server | ✅ | ➖ | ➖ | stdio LSP over //ptah annotations: hover, completion, diagnostics, plus a VS Code extension. Tied to Ptah's own annotation syntax. |
| Public API compatibility gate | ✅ | ➖ | ➖ | check-public-api.sh keeps the committed API baseline and the package tree in sync; pre-v1 breaks need a per-baseline approval line. |
| Query builder for parameterized SQL | 🟡 | ➖ | ➖ | Joins, DISTINCT, GROUP BY, HAVING and RETURNING work; no subqueries, CTEs, LIKE or upsert; SQL Server, ClickHouse, Spanner error. |
| Reusable Go packages (embedder API) | ✅ | ➖ | ➖ | Documented embedder packages cover parse, diff, plan, render, migrate, lint and seed. CE conformance measures CLI commands, not Go APIs. |
| Schema visualization (ERD diagrams) | ✅ | ❌ | ✅ | Mermaid, DOT or SVG ERD from Go annotations only; SVG shells out to Graphviz dot. Atlas ERD lives in the hosted service (any plan per its pricing page); the CE binary rejects `--web`. |
| Statement observer and validator hooks (Go API) | ✅ | ➖ | ➖ | migrator.WithStatementObserver runs a read-only callback per executed statement; WithStatementValidator gates all statements pre-execution; both compose with StatementInterceptor. |
| testkit companion module for database tests | ✅ | ➖ | ➖ | Separate module github.com/stokaro/ptah/testkit wraps testcontainers-go for tests needing real databases; versions independently and stays out of the main module graph. |

## Data and distribution

Ptah's registry story differs from Atlas's in storage, not in function.
Everything artifact distribution needs — publish a migration directory or a
desired schema, pull it elsewhere, pin an exact version, run migrations
straight from the registry — works against any OCI-compliant registry: GHCR,
ECR, GAR, Harbor, Docker Hub, or a self-hosted `registry:2`.

For a team this means the registry and credentials already used for container
images also serve schema artifacts; there is no separate account, login verb,
or hosted service to depend on; and a digest pin makes a deployment
reproducible byte for byte. The artifacts are ordinary OCI 1.1 manifests, so
registry-side controls — replication, retention, immutable-tag policy, access
control — come from the registry, not from Ptah. The full workflow is on
[OCI registry artifacts](../../operate/oci-registry/).

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Artifact integrity check (`--verify-sum`) | 🟡 | ➖ | ❔ | On migrations push and up only. It checks the directory against the ptah.sum inside the same artifact, so content rewritten with its sum passes. |
| Declarative reference data | ✅ | ❌ | ✅ | //ptah:schema:data rows diffed by key into a reversible data migration. Atlas lists declarative data management as a Pro feature. |
| Digest pinning and write-once version tags | ✅ | ➖ | ❔ | Pushing to an @sha256 reference is refused; `--version` is write-once and a conflict exits 2. The reference tag, `--tag` values and latest all move. |
| Environment-scoped SQL seed runner | ✅ | ❌ | ❌ | NNN_desc.env.sql files recorded in schema_seeds with protected-env gates. No seed verb in the CE inventory or the cited Pro list. |
| oci:// as a `--schema-file` desired-state source | 🟡 | ❌ | ✅ | Accepted by schema render/compare/drift/plan/apply and migrations plan/generate. schema inspect rejects it, and only three of those expose `--plain-http`. |
| Referrer attachments: lint, plan, deployment reports | 🟡 | ❌ | ❔ | lint `--attach`, migrations plan `--attach` and up attach reports to an exact digest. `oci referrers` lists descriptors only; no flag downloads the payload. |
| Registry-backed distribution: `oci://` vs `atlas://` | ✅ | ❌ | ✅ | `atlas://` functions — publish, pull, digest-pin, run migrations directly, schemas via `--schema-file` — over any OCI registry, no account. See [OCI registry artifacts](../../operate/oci-registry/). |

## Atlas Registry and Cloud

These rows are the services Atlas hosts on top of its registry — approvals,
reporting, monitoring, the `atlas://` scheme itself. They concern the hosted
service, not artifact storage; the storage function is covered under
[Data and distribution](#data-and-distribution).

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `atlas://` vendor protocol | ❌ | ❌ | ✅ | The scheme is Cloud-bound and rejected with a named error; every function behind it is available natively over `oci://`. The compat binary mirrors the Atlas surface, which has no `oci://`. |
| `migrate push` and `schema push` | ❌ | ❌ | ✅ | ptah-compat boundary stubs printing the CE abort text, exit 1. The native equivalents are `ptah schema push` and `ptah migrations push`. |
| `schema plan` registry and output flags | ❌ | ❌ | ✅ | `--push`/`--pending`/`--repo` are recorded waivers; `--format`/`--name-format`/`--directive`/`--edit`/`--skip-lint` fail as unimplemented. |
| `schema plan` registry sub-verbs | ❌ | ❌ | ✅ | approve, lint, list, new, pull, push, rm, test and validate all stay CE boundary stubs; only local plan files are implemented. |
| Atlas Cloud deployment reporting | ❌ | ❌ | ✅ | No Atlas account model or deployment API. Ptah attaches a deployment-report referrer to its own OCI artifact after an oci:// migrations up. |
| Atlas Copilot (AI assistant) | ❌ | ❌ | ✅ | AI assistant gated to Pro accounts; absent from the pinned CE v1.2.0 command inventory. No Ptah equivalent; the closest developer-assist surface is the `ptah-ls` language server. |
| Column-level data lineage | ❌ | ❌ | ✅ | Hosted Atlas Cloud view tracing column-to-column dependencies across schemas. No Ptah surface: no lineage verb or flag; the nearest output is `ptah viz`, whose ERD edges are table-level FKs only. |
| Hosted Schema Docs (schema documentation) | ❌ | ❌ | ✅ | Auto-generated schema documentation pages in Atlas Cloud. Ptah has no docs generator: `ptah schema export` emits HCL, OpenAPI, GraphQL, or protobuf definitions; `ptah viz` covers ERD only. |
| Reviewer approval and policy workflows | ❌ | ❌ | ✅ | Local `-- +ptah check` pre-migration assertions exist; the Cloud-gated reviewer-approval half is out of scope. |
| Schema monitoring, hosted UI, login | ❌ | ❌ | ✅ | Out of scope: no login, registry UI, promotion or monitoring. Native `ptah schema drift` is a local one-shot check. |

## How these rows were established

The Ptah column is derived from the built binaries. `ptah --help` and
`ptah-compat --help` are the strongest available proof that a capability
exists, and a row that could not be demonstrated that way is marked 🟡 with the
limitation named, or ❌.

The Atlas columns are narrower on purpose. Ptah is a clean-room implementation
that studies observable behavior only, so the Atlas CE column is derived from
the command, usage, and flag inventory the conformance harness reads out of the
pinned Atlas community binary, and the Pro/Cloud column from the classification
Atlas publishes on its feature availability page. Where neither source settles
a question, the row says so rather than guessing.

The full per-row evidence — the command that was run or the source cited for
every cell — is version-controlled at
`docs/site/scripts/data/feature-matrix-rows.json`, so a row can be re-verified
or disputed without archaeology.

Four sources carry most of the weight:

- [`cli-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md)
  inventories every command in Atlas CE v1.2.0 and classifies it as an OSS
  parity target or out of scope, with the reason recorded per command.
- [`ce-gating.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/ce-gating.md)
  goes further than the inventory: it runs the pinned CE binary logged out
  through the capability set this page asserts about the CE column and records
  the observed class per scenario — works, community-abort stub, absent verb,
  unknown flag, or silently unenforced. A version bump that changes Atlas's
  gating turns that gate red.
- [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md),
  [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md),
  and [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
  record measured outcomes over Atlas fixtures, live databases, and Atlas CE
  differential checks.
- [`docs-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/docs-surface.md)
  indexes the full atlasgo.io documentation universe — 351 pages from the
  site's own sitemap — into a triage registry, so parity is built against the
  whole documented Atlas surface rather than a hand-picked subset. The registry
  starts mostly untriaged and its budget ratchets down as pages are worked
  through
  ([campaign](https://github.com/stokaro/ptah-atlas-conformance/issues/239)); a
  weekly job re-fetches the sitemap so new or renamed Atlas docs pages surface
  as red rather than silently missing from this page.

## What this page does not claim

A green conformance run is a floor on the distance to Atlas, never a ceiling.
The conformance repository states it directly: no number it produces is a
full feature-set parity test, and several runtime dimensions stay unmeasured.

Specifically, this page does not claim that Ptah reproduces Atlas byte for
byte, that a ✅ row behaves identically under every flag combination, or that
the Atlas columns are exhaustive for capabilities Atlas ships outside its
documented CLI surface.

## Next steps

- Per-area detail behind these rows: [Comparison](../comparison/).
- The measured evidence and how to re-run it: [Conformance](../conformance/).
- Which Atlas documentation area maps where: [Atlas docs coverage](../docs-coverage/).
- Why Ptah can be Atlas-compatible without Atlas code:
  [License boundary](../license-boundary/).
