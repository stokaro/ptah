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

Each table has the same columns. **Ptah**, **CE**, and **Pro** carry one symbol
each:

- **Ptah** — the native `ptah` binary plus the separate `ptah-compat` drop-in.
- **CE** — the pinned Atlas community binary, version 1.2.0, which the
  conformance harness runs against.
- **Pro** — capabilities Atlas documents as licensed on the
  [Atlas feature availability](https://atlasgo.io/features) page, covering both
  Atlas Pro and Atlas Cloud.

:::caution
A ✅ in the Ptah column means the capability works, not that it is
byte-identical to Atlas. Ptah is an independent pre-GA implementation, and the
conformance repository states plainly that no number it produces is a
full feature-set parity test. Read the difference column before relying on a
row for a migration decision.
:::
## At a glance

Across the 120 capabilities below:

| Reading | Count |
| --- | --- |
| Ptah supports it fully | 56 |
| Ptah supports it with a stated limitation | 43 |
| Ptah does not implement it | 21 |
| Ptah and Atlas CE both support it | 23 |
| Ptah implements it openly where Atlas gates it behind Pro or Cloud | 27 |
| Ptah has it and neither Atlas edition does | 9 |
| Atlas CE has it and Ptah does not, or only in part | 26 |

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
| Atlas HCL data "external_schema" | ❌ | ✅ | ✅ | The `ptah.yaml` external_schema block fills the same desired-schema role but does not evaluate the Atlas HCL data source. |
| Composite multi-source desired schema | ✅ | ❌ | ✅ | Repeatable `--root-dir`/`--schema-file` merge into one schema; conflicts error. Repo docs cite composite_schema as an Atlas Pro data source. |
| External program / ORM loaders | ✅ | ✅ | ✅ | `--schema-cmd` or `ptah.yaml` external_schema (needs `--allow-external-schema`) runs a program without a shell emitting SQL, HCL, or YAML. |
| Go struct annotations | ✅ | ❌ | ❌ | Ptah parses //ptah:schema:* comments into the desired schema. Atlas's route to Go models is an external ORM provider program. |
| HCL schema files | 🟡 | ✅ | ✅ | Ptah parses a strict subset and fails explicitly on unsupported constructs; complete Atlas HCL coverage is not claimed. |
| Live database as desired state | ✅ | ✅ | ✅ | One connectable DB URL can be the desired side of compat schema apply/diff and migrate diff; `ptah db read` introspects natively. |
| Live database to Go annotation source | ✅ | ❌ | ❌ | `ptah introspect` writes annotated Go models from a live DB; repo docs record Go annotations as a first-party Ptah workflow. |
| Migration directory as a source | ✅ | ✅ | ✅ | Atlas-format directory with `atlas.sum`, replayed on a required `--dev-url`. Works on `ptah schema inspect` and compat apply/diff/migrate diff. |
| Registry-backed schema source | 🟡 | ❌ | ✅ | `ptah schema pull` fetches a canonical HCL artifact over oci://; it is then passed as `--schema-file`. atlas:// registry URLs are rejected. |
| SQL DDL schema files | ✅ | ✅ | ✅ | Accepted by native `--schema-file` and by `ptah-compat schema apply`/diff `--to`/`--from`; unsupported DDL fails instead of being skipped. |
| YAML schema files | ✅ | ❌ | ❌ | Strict parser; unknown keys fail. Repo docs list Atlas OSS data sources as SQL, HCL, external schema, and remote/template dirs. |

## Declarative and direct schema changes

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| --dry-run, --auto-approve, and --edit | ✅ | ✅ | ✅ | All three registered and functional; `--edit` opens $VISUAL/$EDITOR and the edited SQL is what gets planned, rehearsed, and applied. |
| --exclude glob and type selectors | 🟡 | ✅ | ✅ | Resource selectors plus the [type=extension].version field selector; other field selectors and non-final type selectors fail explicitly. |
| --include resource selectors | 🟡 | ❌ | ✅ | Ptah: top-level [type=...] selectors on apply/diff; child and field selectors rejected, none on inspect. Atlas CE aborts the flag. |
| --schema / -s scoping of both sides | ✅ | ✅ | ✅ | Names define the schema universe for apply and diff; repeated and comma-separated values union deterministically. |
| `schema inspect --include` filtering | ❌ | ❌ | ❌ | Flag absent from the pinned Atlas CE v1.2.0 inspect surface; Ptah rejects it as unknown and scopes inspect with `--schema`/`--exclude`. |
| Apply advisory lock and --lock-timeout | 🟡 | ✅ | ✅ | Real locks on PostgreSQL, MySQL, MariaDB, SQL Server; SQLite, ClickHouse, CockroachDB, YugabyteDB and Spanner run unlocked with a note. |
| Desired-state sources for --to and --from | 🟡 | ✅ | ✅ | Schema files, one DB URL, one `atlas.sum` migration dir, or env://. atlas:// registry URLs fail before the target is contacted. |
| Dev-database rehearsal before apply | 🟡 | ✅ | ✅ | Dev DB is reset, target schema recreated, exact plan run; failure aborts the apply. Needs a connectable dev URL — docker:// is a gap. |
| Drift detection against desired schema | ✅ | ❌ | ✅ | Native `ptah schema drift`: `--severity`, `--exit-code`, `--ignore`, text/json/github-actions. Atlas Cloud drift monitoring is out of scope. |
| Go-template --format output | 🟡 | ✅ | ✅ | sql/hcl/json/mermaid/split/write helpers over .Changes, .Realm, .MarshalSQL. Atlas web and Cloud report output remains a gap. |
| Inspect exclude field selectors and exporters | 🟡 | ✅ | ✅ | Only the `*[type=extension].version` field selector works; other field selectors, non-final type selectors and exporters fail. |
| Inspect non-database sources via --dev-url | ✅ | ✅ | ✅ | Schema file, `atlas.sum` migration dir, or env:// is materialized on a reset dev DB then introspected; without `--dev-url` it fails. |
| Inspect split/write file exports | ✅ | ❌ | ✅ | `{{ hcl . \| split \| write "dir" }}` writes object/schema/type trees; pinned Atlas CE rejects split, write, hcl as non-community. |
| Local pre-approved plan files | ✅ | ❌ | ✅ | `schema plan` writes a format_version-1 JSON plan with sha256 fingerprints; `apply --plan` refuses a drifted target as stale. |
| Plan registry sub-verbs and schema push | ❌ | ❌ | ✅ | approve, lint, list, new, pull, push, rm, test, validate and `schema push` are boundary stubs printing the CE abort text. |
| schema apply against a live database | ✅ | ✅ | ✅ | Diffs `--url` against the `--to` desired state, prints the SQL plan, applies after confirmation. Verified end to end on SQLite. |
| schema clean | 🟡 | ✅ | ✅ | Drops user tables on supported dialects, PostgreSQL enums and sequences, SQL Server FKs; other object kinds are not modeled yet. |
| schema diff between two schema states | 🟡 | ✅ | ✅ | Files, DB URLs, migration dirs, env:// per side; `--dev-url` pins the dialect. SQLite column type changes fail: rebuild plan not emitted. |
| schema fmt (HCL canonical layout) | ✅ | ✅ | ✅ | Formats .hcl paths recursively and prints only changed files. Native `ptah schema fmt --check` adds a no-write CI gate. |
| schema inspect to HCL, SQL, or JSON | ✅ | ✅ | ✅ | Default HCL; `--format` sql\|json\|template. Native twin `ptah schema inspect` adds `--out-dir` and `--split` file export. |

## Versioned migrations

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Apply pending migrations (apply/up) | ✅ | ✅ | ✅ | Compat apply matches the CE flag set and also runs goose, flyway, liquibase, dbmate and golang-migrate directories up-only. |
| Atlas-format checkpoint output | ❌ | ❌ | ✅ | migrate checkpoint `--dir-format`=atlas is a recorded waiver; Ptah writes only the ptah two-file checkpoint convention. |
| Baseline an existing database | ✅ | ✅ | ✅ | Ptah adds a standalone baseline verb with `--shadow-db` verification and `--dry-run`; CE exposes baselining as migrate apply `--baseline`. |
| Create an empty migration file | ✅ | ✅ | ✅ | Native create writes an up/down pair; compat new writes one Atlas .sql skeleton, refreshes `atlas.sum`, `--edit` opens $VISUAL or $EDITOR. |
| Directory integrity file: hash and validate | ✅ | ✅ | ✅ | hash writes `ptah.sum`, or `atlas.sum` for atlas-format directories; validate checks it and with `--dev-url` cleans and replays the dir. |
| Directory maintenance: edit, rebase, rm | ✅ | ❌ | ✅ | Each rewrites `ptah.sum`/`atlas.sum` and refuses a migration applied in `--db-url` unless `--force`; CE aborts all three as non-community verbs. |
| Dynamic down planning (`migrate down --plan`) | ❌ | ❌ | ✅ | `--plan`, `--to-tag` and `--skip-checks` are recorded waivers that fail loudly; Ptah reverts only through pre-planned down files. |
| Execution order (--exec-order) | ✅ | ✅ | ✅ | linear fails on a pending migration below the current version, linear-skip warns and leaves it pending, non-linear applies it. |
| External `--dir-format` outside `migrate import` | ❌ | ✅ | ✅ | hash, lint, new, set, status and validate accept only `--dir-format`=atlas; other tool formats must first go through migrate import. |
| Flyway repeatable (`R__`) migration import | 🟡 | ✅ | ✅ | Native `ptah migrations import` converts R__ into a one-time migration; Atlas-shaped `ptah-compat migrate import` rejects R__ files. |
| Generate migrations from a schema diff | 🟡 | ✅ | ✅ | Replays the directory on `--dev-url` and writes files before `atlas.sum`; `--qualifier`, `--edit`, `--format` work. Docker dev databases are a gap. |
| Import from other migration tools | 🟡 | ✅ | ✅ | Compat import takes local file:// dirs only and rejects Flyway R__ repeatable files; native import converts them to one-time migrations. |
| Migration checkpoints (squash history) | ✅ | ❌ | ✅ | Replays the directory on `--shadow-db` into a cumulative checkpoint pair; CE has no checkpoint verb and Atlas lists it as Pro. |
| Migration linting | 🟡 | 🟡 | ✅ | CE registers migrate lint; Atlas's features page marks the lint CLI Pro with a basic Open rule set. Ptah lacks custom rules, web reports. |
| Migration lock and lock timeout | ✅ | ✅ | ✅ | Compat `--lock-timeout` bounds directory and dev-db locks; native splits per-migration `--lock-timeout` from `--migration-lock-timeout`. |
| Migration status report | ✅ | ✅ | ✅ | Compat status reads Atlas revision metadata and renders Go templates over .Env, .Available, .Applied, .Pending, .Current, .Next. |
| Native migration import | ✅ | ✅ | ✅ | Both convert golang-migrate, Goose, Flyway and Liquibase dirs; ptah adds dbmate. Liquibase XML/YAML/JSON changelogs are rejected. |
| Pre-migration assertion checks | 🟡 | ❌ | ✅ | Local -- +ptah check assert=... aborts before the body; rejected under `--tx-mode` all. The reviewer-approval half is not implemented. |
| Repair dirty or partial revision state | ✅ | ❌ | ❌ | `ptah migrations repair` `--resume-from` finishes remaining statements. No repair verb in the pinned CE inventory or reviewed Atlas evidence. |
| Revision table format and placement | ✅ | ✅ | ✅ | `--revision-format` ptah\|atlas plus `--migrations-table` and `--migrations-schema`; the compat path defaults to Atlas rows. |
| Roll back applied migrations (down) | 🟡 | ✅ | ✅ | Ptah reverts via pre-planned down files; `--to-tag`, `--skip-checks` and `--plan` fail loudly as waivers. Atlas Pro adds plan approval. |
| Set revision state to a version | ✅ | ✅ | ✅ | Removes revision rows above the target, keeps rows at or below it, and inserts missing rows through it as manually set. |
| Transaction modes (--tx-mode file/all/none) | ✅ | ✅ | ✅ | all is limited to transactional-DDL dialects and rejects no_transaction files, per-file timeouts, and pre-migration checks. |

## Linting and safety

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Apply-time destructive-change gate | ✅ | ❌ | ➖ | migrations up refuses destructive pending files without `--allow-destructive`; .ptah-lint.yaml tunes it, but `ptah.sum` does not hash it. |
| Atlas Pro analyzer code coverage | 🟡 | ➖ | ✅ | 23 of 30 Pro-marked codes covered, 5 partial (PG301, PG304, MY130, MY133, MY136), 2 waived (OW101, OW102) as account-bound policy. |
| Atlas web reports (`--web`) | ❌ | ❌ | ✅ | Not registered on migrate lint or schema diff; rejected as an unknown flag. Pinned Atlas CE v1.2.0 does not register it either. |
| Check-level policy and custom lint rules | ❌ | ❌ | ✅ | Only analyzer severity policy for the five mapped families is read; check-level policy, custom rules and force/allow-lists are gaps. |
| CI integration (GitHub Action, annotations) | ✅ | 🟡 | ✅ | stokaro/ptah-action@v1 posts a sticky PR comment and destructive-change check run; `--format` github-actions emits inline annotations. |
| Custom lint rules and check-level policy | 🟡 | ❌ | ✅ | Custom rules register from Go (lint.Register, Options.ExtraRules); Atlas check-level policy and analyzer force/allow options are absent. |
| Default-firing Atlas analyzer concern mapping | ✅ | ➖ | ➖ | lint-analyzer-catalog maps every default-firing Atlas concern to a covering Ptah rule, severity and line; 0 gap on the committed corpus. |
| Inline nolint suppression | ✅ | 🟡 | ✅ | Native ptah:nolint is statement-scoped; atlas:nolint selectors and whole-file headers apply only under the ptah-compat lint profile. |
| Native migration lint rule set | ✅ | 🟡 | ✅ | 42 built-in codes in DS, CD, DD, MF, BC, PG, MY, LT, TX families; `--dialect` gates dialect-specific rules. Atlas has a basic Open rule set. |
| Per-rule severity policy | 🟡 | 🟡 | ✅ | .ptah-lint.yaml sets per-rule severity and path excludes; `atlas.hcl` maps only 5 analyzer blocks to severity and rejects force. |
| SARIF 2.1.0 lint report | ✅ | ❌ | ➖ | Native `--format` sarif emits SARIF 2.1.0 with ruleId, level and file:line; Atlas documents Go-template `--format` output for migrate lint. |
| Statement safety classification report | ✅ | ➖ | ➖ | plan `--report` text\|html\|json and generate `--report` html\|json emit highest severity, a destructive flag, and per-statement assessments. |

## Testing

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Atlas `.test.hcl` ingestion | ❌ | ❌ | ✅ | `ptah-compat migrate test` and schema test run Ptah-native YAML/Go cases only; .test.hcl files are not parsed. |
| Atlas-shaped migrate test / schema test verbs | 🟡 | ❌ | ✅ | ptah-compat forwards to native runners (`--dir`, -u, `--dev-url`, `--run`); Atlas .test.hcl is not ingested — Ptah YAML is the payload. |
| Dev / shadow database verification | 🟡 | ✅ | ✅ | Replay on `--dev-url`, `--shadow-db` for generate and checkpoint, apply rehearsal. Docker dev-database URLs unsupported. |
| Embeddable test runner (Go package) | ✅ | ❌ | ❌ | Runner exported as migration/dbtest (RunMigrationTest, RunSchemaTest); Atlas ships its test framework only in a closed-source binary. |
| Exit-code contract for CI gates | ✅ | ✅ | ➖ | Native 0/1/2 separates expected negative results from command errors; ptah-compat collapses to Atlas CE 0/1, recovered panics still exit 2. |
| Migration test framework (`ptah migrations test`) | ✅ | ❌ | ✅ | Declarative YAML cases: migrate_to, apply_schema, seed, exec, assert. Fresh ephemeral SQLite per case unless `--db-url` is set. |
| Registry plan testing (`schema plan test`) | ❌ | ❌ | ✅ | `ptah-compat schema plan` test stays an Atlas CE unsupported boundary stub: `--help` exits 0, direct execution aborts and exits 1. |
| Schema test framework (`ptah schema test`) | ✅ | ❌ | ✅ | Desired schema from Go annotations converges before steps; migrate_to is rejected. Atlas CE v1.2.0 registers no schema test verb. |

## Configuration and dev databases

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Atlas project config (atlas.hcl) | 🟡 | ✅ | ✅ | Documented local subset: variable, locals, data hcl_schema, env, schema, migration, format, diff, lint. Cloud and registry blocks fail. |
| data "hcl_schema" reference | 🟡 | ✅ | ✅ | Only data.hcl_schema.<name>.url for local schema files resolves; any other data block label fails explicitly. |
| Docker dev databases (`docker://` --dev-url) | ❌ | ✅ | ✅ | migrate diff, lint and validate refuse docker:// and require a directly connectable dev database URL. |
| env:// desired-state references | 🟡 | ✅ | ✅ | ptah-compat only; attributes src, schema.src, url, dev, migration, migration.dir. Nested env:// fails; native ptah has no equivalent. |
| Native project config (ptah.yaml) | ✅ | ➖ | ➖ | Ptah-owned file for url, src, dev, external_schema, migration, lint, diff, online_ddl. Unknown keys fail. Ranks below `atlas.hcl`. |
| Remote and template directory sources | ❌ | ✅ | ✅ | Rejected while parsing `atlas.hcl`. The native substitute is the bring-your-own oci:// artifact workflow, not an Atlas data source. |
| Variables, locals, and HCL functions | 🟡 | ✅ | ✅ | variable defaults, repeated `--var` strings/lists, locals, getenv, file, fileset, format, jsonencode. Typed and sensitive variables fail. |

## Databases and schema objects

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| ClickHouse (clickhouse, ch) | 🟡 | ❌ | ✅ | No foreign keys, enforced CHECKs, views, matviews, functions or triggers; standalone enum types are rejected. Atlas lists it as Pro. |
| CockroachDB (cockroachdb, crdb) | 🟡 | ❌ | ✅ | Preset drops concurrent indexes, sequences, XML, advisory locks, roles and RLS; views, functions and triggers are not emitted at all. |
| Domains, composite types, and range types | 🟡 | ❌ | ✅ | PostgreSQL only - not CockroachDB, YugabyteDB or Spanner. Domain check/default are create-only; base-type changes drop and recreate. |
| Enum types | 🟡 | ✅ | ✅ | Postgres CREATE TYPE; MySQL inline ENUM; SQLite TEXT+CHECK; SQL Server NVARCHAR+CHECK; ClickHouse passthrough; Spanner skips enums. |
| Extensions | 🟡 | ❌ | ✅ | PostgreSQL family only, with a default ignore list (plpgsql). SQLite, SQL Server and ClickHouse comment it out; MySQL/MariaDB emit nothing. |
| Functions | 🟡 | ❌ | ✅ | Renders on PostgreSQL only; MySQL/MariaDB emit a not-supported comment and error on DROP; CockroachDB, YugabyteDB, Spanner omit silently. |
| MySQL and MariaDB | 🟡 | ✅ | ✅ | Separate dialects with different capability sets; no PostgreSQL-only objects; implicit DDL commit blocks transactional rollback. |
| Oracle, Snowflake, Redshift, Databricks | ❌ | ❌ | ✅ | Listed as Atlas Pro drivers. Ptah has no dialect, renderer or driver for any of them. |
| PostgreSQL 12+ (postgres, postgresql) | ✅ | ✅ | ✅ | Only engine where views, functions, sequences, roles, RLS and domains are emitted; presets 12-13, 14-16, 17+ from the server banner. |
| Roles, grants, and row-level security | 🟡 | ❌ | ✅ | Emitted on PostgreSQL only; CockroachDB, YugabyteDB and Spanner get none despite PostgreSQL-family capability flags for roles. |
| Spanner PostgreSQL interface (spanner) | 🟡 | ❌ | ✅ | Most conservative preset: no enums, foreign keys, sequences, RLS, XML or advisory locks. Coverage is offline; no live container. |
| SQL Server and Azure SQL (sqlserver, mssql, tsql) | 🟡 | ❌ | ✅ | Portable T-SQL subset: no sequences, RLS, roles/grants or matviews, and automatic column removal is refused. Atlas lists it as Pro. |
| SQLite (sqlite, sqlite3) | 🟡 | ✅ | ✅ | Constraint changes and most column modifications report rebuild-required instead of generating a rebuild; PG-only objects rejected. |
| Standalone sequences | 🟡 | ❌ | ✅ | Standalone CREATE SEQUENCE renders on PostgreSQL only; YugabyteDB carries the capability flag but the generator emits none. |
| TiDB and LibSQL | ❌ | ✅ | ✅ | Atlas docs list both as Open drivers; Ptah's dialect normalizer has no TiDB or LibSQL entry, so the names do not resolve. |
| Triggers | 🟡 | ❌ | ✅ | Render on PostgreSQL, MySQL/MariaDB, SQLite (row-level only) and SQL Server; ClickHouse, CockroachDB, YugabyteDB and Spanner do not. |
| Views and materialized views | 🟡 | ❌ | ✅ | Views: PostgreSQL, MySQL/MariaDB, SQLite, SQL Server. Matviews: PostgreSQL only. CockroachDB, YugabyteDB, Spanner, ClickHouse: neither. |
| YugabyteDB (yugabytedb, ysql) | 🟡 | ❌ | ✅ | Preset drops concurrent indexes, advisory locks, RLS; roles and sequences stay, but views, functions, triggers are never emitted. |

## Go embedding and developer tooling

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Annotation metadata as JSON Schema | ✅ | ➖ | ➖ | Emits a JSON Schema describing every //ptah directive and attribute; Atlas has no //ptah annotation set for the concept to apply to. |
| API schema export: OpenAPI 3.0 and GraphQL | ✅ | ❌ | ❌ | Go annotations to OpenAPI components or GraphQL SDL; no handlers or resolvers. Absent from the CE inventory and the cited Pro list. |
| Protobuf schema export with pinned field numbers | ✅ | ❌ | ❌ | Edition 2023 output; `--out` pins field numbers, with policies for type removal, name reuse and incompatible change. Not in CE inventory. |
| ptah-ls annotation language server | ✅ | ➖ | ➖ | stdio LSP over //ptah annotations: hover, completion, diagnostics, plus a VS Code extension. Tied to Ptah's own annotation syntax. |
| Public API compatibility gate | ✅ | ➖ | ➖ | check-public-api.sh keeps the committed API baseline and the package tree in sync; pre-v1 breaks need a per-baseline approval line. |
| Query builder for parameterized SQL | 🟡 | ➖ | ➖ | SELECT/INSERT/UPDATE/DELETE with bound values and quoted identifiers. No subqueries, LIKE, arithmetic, window functions, CTEs or upsert. |
| Reusable Go packages (embedder API) | ✅ | ➖ | ➖ | Documented embedder packages cover parse, diff, plan, render, migrate, lint and seed. CE conformance measures CLI commands, not Go APIs. |
| Schema visualization (ERD diagrams) | ✅ | ❌ | ✅ | Mermaid, DOT or SVG ERD from Go annotations only; SVG shells out to Graphviz dot. Atlas features page lists visualization as Pro. |

## Data and distribution

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Declarative reference data | ✅ | ❌ | ✅ | //ptah:schema:data rows diffed by key into a reversible data migration. Atlas lists declarative data management as a Pro feature. |
| Environment-scoped SQL seed runner | ✅ | ❌ | ❌ | NNN_desc.env.sql files recorded in schema_seeds with protected-env gates. No seed verb in the CE inventory or the cited Pro list. |
| OCI deployment, lint and plan report referrers | 🟡 | ❌ | 🟡 | Lists referrer descriptors only; payload download is absent. Atlas Cloud deployment reporting is a hosted service, not registry referrers. |
| OCI desired-schema artifacts | ✅ | ❌ | 🟡 | Canonical schema.hcl push/pull; compare, drift and plan read `--schema-file` oci://. `atlas schema push` is a CE unsupported boundary stub. |
| OCI migration artifacts | ✅ | ❌ | 🟡 | push/pull plus direct up, status, down and lint over oci://. Atlas's registry equivalent is hosted and account-bound, not bring-your-own. |

## Atlas Registry and Cloud

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `migrate push` and `schema push` | ❌ | ❌ | ✅ | Registered as Atlas CE boundary stubs: help prints the community-version notice, direct execution prints the CE abort text. |
| `schema plan` registry and output flags | ❌ | ❌ | ✅ | `--push`/`--pending`/`--repo` are recorded waivers; `--format`/`--name-format`/`--directive`/`--edit`/`--skip-lint` fail as unimplemented. |
| `schema plan` registry sub-verbs | ❌ | ❌ | ✅ | approve, lint, list, new, pull, push, rm, test and validate all stay CE boundary stubs; only local plan files are implemented. |
| Atlas Cloud deployment reporting | ❌ | ❌ | ✅ | No Atlas account model or deployment API. Ptah attaches best-effort deployment reports to its own OCI registry instead. |
| Atlas Registry `atlas://` source URLs | ❌ | ❌ | ✅ | Rejected in `--from`/`--to`/`--plan` and migration.dir; Ptah offers a bring-your-own oci:// registry instead, not an atlas:// resolver. |
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

Two sources carry most of the weight:

- [`cli-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md)
  inventories every command in Atlas CE v1.2.0 and classifies it as an OSS
  parity target or out of scope, with the reason recorded per command.
- [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md),
  [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md),
  and [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
  record measured outcomes over Atlas fixtures, live databases, and Atlas CE
  differential checks.

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
