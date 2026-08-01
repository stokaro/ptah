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
- **Pro** — capabilities Atlas documents as licensed on the
  [Atlas feature availability](https://atlasgo.io/features) page, covering both
  Atlas Pro and Atlas Cloud.

Every Atlas cell has to come from an Atlas-side source: the command, usage, and
flag inventory the conformance harness reads out of the pinned community
binary, or a classification Atlas publishes. Where neither settles a question,
the cell is ❔ rather than a guess. That is why the schema-object rows carry ❔
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

Across the 129 capabilities below:

| Reading | Count |
| --- | --- |
| Ptah supports it fully | 63 |
| Ptah supports it with a stated limitation | 44 |
| Ptah does not implement it | 22 |
| Ptah and Atlas CE both support it | 25 |
| Ptah implements it openly where Atlas gates it behind Pro or Cloud | 26 |
| Ptah has it and neither Atlas edition does | 8 |
| Atlas CE has it and Ptah does not, or only in part | 25 |
| Atlas side not established by this page's evidence | 15 |

Every 🟡 on this page names the specific limitation, and each one was
reproduced against a binary built from this repository. Where the limitation is
work that has not been done, it is tracked in
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
| Atlas HCL data "external_schema" | ❌ | ✅ | ✅ | Ptah's `data` block is an unlabeled seed-row declaration; every labeled Atlas data source fails with `data block does not accept labels`. |
| Composite multi-source desired schema | ✅ | ❌ | ✅ | Repeatable `--root-dir`/`--schema-file` merge into one schema; conflicts error. Repo docs cite composite_schema as an Atlas Pro data source. |
| Desired-schema artifacts in an OCI registry | ✅ | ❌ | ✅ | `ptah schema push/pull` publish and fetch canonical HCL resolved from Go, YAML, HCL or SQL sources. Verified round trip against registry:2. |
| Directory of .hcl files as one schema source | ❌ | ❔ | ❔ | `--schema-file dir` and `--to file://dir` are refused (schema file is a directory); globs are not expanded. Multi-file needs one flag per file. |
| External program / ORM loaders | ✅ | ✅ | ✅ | `--schema-cmd` or `ptah.yaml` external_schema (needs `--allow-external-schema`) runs a program without a shell emitting SQL, HCL, or YAML. |
| Go struct annotations | ✅ | ❌ | ❌ | Ptah parses //ptah:schema:* comments into the desired schema. Atlas's route to Go models is an external ORM provider program. |
| HCL foreign_key deferrable | ❌ | ❔ | ❔ | Errors: unsupported foreign_key attribute "deferrable". DEFERRABLE is absent from the whole Ptah IR, so YAML and Go annotations lack it too. |
| HCL function calls in schema files | 🟡 | ❔ | ❔ | sql() unwraps in column default/on_update/unique_expr/check, index ops, domain check only; in type, check.expr, index.where it leaks literally. |
| HCL locals, lock, atlas, dynamic/for_each | ❌ | ❔ | ❔ | Named rejections, exit 1: unsupported top-level block "locals"/"lock"/"atlas"; unsupported table block "dynamic". |
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
| --dry-run, --auto-approve, and --edit | ✅ | ✅ | ✅ | All three registered and functional; `--edit` opens $VISUAL/$EDITOR and the edited SQL is what gets planned, rehearsed, and applied. |
| --exclude glob and type selectors | 🟡 | ✅ | ✅ | Resource globs plus one final-segment [type=...]; schema-qualified globs never match default-schema tables, functions or enums. |
| --include resource selectors | ✅ | ❌ | ✅ | Top-level globs and [type=...] on apply/diff, repeated values union; child types refused by design, schemas selected with --schema. |
| --schema / -s scoping of both sides | ✅ | ✅ | ✅ | Names define the schema universe for apply and diff; repeated and comma-separated values union deterministically. |
| `schema inspect --include` filtering | ❌ | ❌ | ❔ | Flag absent from the pinned Atlas CE v1.2.0 inspect flags; Ptah rejects it as unknown. No cited source settles the licensed builds. |
| Apply advisory lock and --lock-timeout | ✅ | ✅ | ✅ | Real locks on PostgreSQL, YugabyteDB, MySQL, MariaDB, SQL Server; SQLite, ClickHouse, CockroachDB, Spanner run unlocked with a note. |
| Desired-state sources for --to and --from | 🟡 | ✅ | ✅ | Files, one DB URL, one atlas.sum dir, or env://. A plain file:// schema directory without atlas.sum is rejected; atlas:// fails early. |
| Dev-database rehearsal before apply | ✅ | ✅ | ✅ | Dev DB reset, target schema recreated, exact plan rehearsed; dev==target and failed rehearsal abort. docker:// dev URLs have their own row. |
| Drift detection against desired schema | ✅ | ❌ | ✅ | Native `ptah schema drift`: `--severity`, `--exit-code`, `--ignore`, text/json/github-actions. Atlas Cloud drift monitoring is out of scope. |
| Go-template --format output | 🟡 | ✅ | ✅ | schema apply and diff register only the sql helper: {{ json . }} fails to parse. json/hcl/mermaid/split/write and .Realm are inspect-only. |
| Inspect --exclude field selectors | 🟡 | ❔ | ❔ | Only [type=extension].version is honored; other .field suffixes and non-final [type=...] fail before any database is contacted. |
| Inspect non-database sources via --dev-url | ✅ | ✅ | ✅ | Schema file, `atlas.sum` migration dir, or env:// is materialized on a reset dev DB then introspected; without `--dev-url` it fails. |
| Inspect split/write file exports | ✅ | ❌ | ✅ | `{{ hcl . \| split \| write "dir" }}` writes object/schema/type trees; pinned Atlas CE rejects split, write, hcl as non-community. |
| Local pre-approved plan files | ✅ | ❌ | ✅ | `schema plan` writes a format_version-1 JSON plan with sha256 fingerprints; `apply --plan` refuses a drifted target as stale. |
| schema apply against a live database | ✅ | ✅ | ✅ | Diffs `--url` against the `--to` desired state, prints the SQL plan, applies after confirmation. Verified end to end on SQLite. |
| schema clean | 🟡 | ✅ | ✅ | Plan and --dry-run list tables (plus PostgreSQL enums/sequences, SQL Server FKs), but the apply drops views too via DropAllTables. |
| schema diff between two schema states | 🟡 | ✅ | ✅ | SQLite refuses any change needing a table rebuild: column modify, NOT NULL change, constraint add/remove, enum CHECK change. Same on apply. |
| schema fmt (HCL canonical layout) | ✅ | ✅ | ✅ | Formats .hcl paths recursively and prints only changed files. Native `ptah schema fmt --check` adds a no-write CI gate. |
| schema inspect to HCL, SQL, or JSON | ✅ | ✅ | ✅ | Default HCL; `--format` sql\|json\|template. Native twin `ptah schema inspect` adds `--out-dir` and `--split` file export. |
| Schema-qualified exclude globs for enums and functions | 🟡 | ❔ | ❔ | Tables, views and extensions match schema-qualified exclude globs; enum and function filters compare the bare name only. |

## Versioned migrations

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Apply pending migrations (apply/up) | 🟡 | ✅ | ✅ | Also runs goose/flyway/liquibase/dbmate dirs via ?format=; --dry-run ignores the revision table and re-plans already-applied files. |
| Atlas-format checkpoint output | ❌ | ❌ | ✅ | migrate checkpoint `--dir-format`=atlas is a recorded waiver; Ptah writes only the ptah two-file checkpoint convention. |
| Baseline an existing database | ✅ | ✅ | ✅ | Ptah adds a standalone baseline verb with `--shadow-db` verification and `--dry-run`; CE exposes baselining as migrate apply `--baseline`. |
| Create an empty migration file | ✅ | ✅ | ✅ | Native create writes an up/down pair; compat new writes one Atlas .sql skeleton, refreshes `atlas.sum`, `--edit` opens $VISUAL or $EDITOR. |
| Directory integrity file: hash and validate | ✅ | ✅ | ✅ | hash writes `ptah.sum`, or `atlas.sum` for atlas-format directories; validate checks it and with `--dev-url` cleans and replays the dir. |
| Directory maintenance: edit, rebase, rm | ✅ | ❌ | ✅ | Each rewrites `ptah.sum`/`atlas.sum` and refuses a migration applied in `--db-url` unless `--force`; CE aborts all three as non-community verbs. |
| Dynamic down planning (`migrate down --plan`) | ❌ | ❌ | ✅ | `--plan`, `--to-tag` and `--skip-checks` are recorded waivers that fail loudly; Ptah reverts only through pre-planned down files. |
| Execution order (--exec-order) | ✅ | ✅ | ✅ | linear fails on a pending migration below the current version, linear-skip warns and leaves it pending, non-linear applies it. |
| External `--dir-format` outside `migrate import` | ❌ | ✅ | ✅ | hash, lint, new, set, status and validate accept only `--dir-format`=atlas; other tool formats must first go through migrate import. |
| Flyway repeatable (`R__`) migration import | 🟡 | ✅ | ✅ | Compat import aborts the entire directory on any R__ file; native import rewrites R__ as a one-time migration, losing re-run-on-change. |
| Generate migrations from a schema diff | 🟡 | ✅ | ✅ | New versions are a Unix epoch in an empty dir and latest+1 otherwise, not the UTC YYYYMMDDHHMMSS stamp migrate new and Atlas dirs carry. |
| Import from other migration tools | 🟡 | ✅ | ✅ | --from must be a local file:// dir; Liquibase XML/YAML/JSON changelogs are refused - only formatted-SQL changelogs import. |
| Migration checkpoints (squash history) | ✅ | ❌ | ✅ | Replays the directory on `--shadow-db` into a cumulative checkpoint pair; CE has no checkpoint verb and Atlas lists it as Pro. |
| Migration linting | ✅ | 🟡 | ✅ | Runs with Atlas's flag set and 42 codes; custom rules load only through the Go API at compile time, not from a CLI flag or config file. |
| Migration lock and lock timeout | ✅ | ✅ | ✅ | Compat `--lock-timeout` bounds directory and dev-db locks; native splits per-migration `--lock-timeout` from `--migration-lock-timeout`. |
| Migration status report | ✅ | ✅ | ✅ | Compat status reads Atlas revision metadata and renders Go templates over .Env, .Available, .Applied, .Pending, .Current, .Next. |
| Native migration import | ✅ | ✅ | ✅ | Both convert golang-migrate, Goose, Flyway and Liquibase dirs; ptah adds dbmate. Liquibase XML/YAML/JSON changelogs are rejected. |
| Repair dirty or partial revision state | ✅ | ❌ | ❌ | `ptah migrations repair` `--resume-from` finishes remaining statements. No repair verb in the pinned CE inventory or reviewed Atlas evidence. |
| Revision table format and placement | ✅ | ✅ | ✅ | `--revision-format` ptah\|atlas plus `--migrations-table` and `--migrations-schema`; the compat path defaults to Atlas rows. |
| Roll back applied migrations (down) | 🟡 | ✅ | ✅ | Atlas single-file dirs have no down body: down dirties the revision row before it discovers that, so apply then aborts on a dirty revision. |
| Set revision state to a version | ✅ | ✅ | ✅ | Removes revision rows above the target, keeps rows at or below it, and inserts missing rows through it as manually set. |
| Transaction modes (--tx-mode file/all/none) | ✅ | ✅ | ✅ | all is limited to transactional-DDL dialects and rejects no_transaction files, per-file timeouts, and pre-migration checks. |

## Linting and safety

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Apply-time destructive-change gate | ✅ | ❌ | ➖ | migrations up refuses destructive pending files; .ptah-lint.yaml disabled-rules reopens the gate and ptah.sum does not hash that file. |
| Atlas Pro analyzer code coverage | 🟡 | ➖ | ✅ | OW101/OW102 have no rule; PG301, PG304, MY130, MY133, MY136 fire under broader codes (DS103, PG104, CD103, MY101), not dedicated ones. |
| Atlas web reports (`--web`) | ❌ | ❌ | ✅ | Not registered on migrate lint or schema diff; rejected as an unknown flag. Pinned Atlas CE v1.2.0 does not register it either. |
| CI integration (GitHub Action, annotations) | ✅ | ❔ | ❔ | stokaro/ptah-action@v1 posts a sticky PR comment; --format github-actions emits annotations. Atlas features page omits CI integrations. |
| Custom lint rules and check-level policy | 🟡 | ❌ | ✅ | Custom rules only from Go (lint.Register, Options.ExtraRules); atlas.hcl rule, review, naming, non_linear blocks and force all fail. |
| Default-firing Atlas analyzer concern mapping | ✅ | ➖ | ➖ | lint-analyzer-catalog maps every default-firing Atlas concern to a covering Ptah rule, severity and line; 0 gap on the committed corpus. |
| Inline nolint suppression | 🟡 | ✅ | ✅ | Analyzer-name selectors suppress, but only codes DS102/DS103/MF103 map; atlas:nolint PG101 and unknown selectors are silently ignored. |
| Native migration lint rule set | ✅ | 🟡 | ✅ | 42 codes across 9 families, gated by --dialect. Atlas lists destructive and backward-incompatible rules Open; concurrent-index rules Pro. |
| Per-rule severity policy | 🟡 | ❔ | ✅ | Severity vocabulary is warning\|error only (info errors out); atlas.hcl exposes 5 analyzer blocks mapped to an error bool and rejects force. |
| Pre-migration assertion checks | 🟡 | ❌ | ✅ | Only the -- +ptah check spelling; --tx-mode all refuses checked files, and that error names --skip-checks, which compat apply lacks. |
| SARIF 2.1.0 lint report | ✅ | ❌ | ➖ | Native --format sarif emits SARIF 2.1.0 with ruleId, level and file:line; Atlas documents Go-template --format output for migrate lint. |
| Statement safety classification report | ✅ | ➖ | ➖ | plan --report text\|html\|json and generate --report html\|json emit highest severity, a destructive flag, and per-statement assessments. |

## Testing

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Atlas `.test.hcl` ingestion | ❌ | ❌ | ✅ | No .test.hcl parser; a directory of them exits 1 with "no test cases found" instead of naming the unsupported file format. |
| Atlas-shaped migrate test / schema test verbs | 🟡 | ❌ | ✅ | schema test -u takes only a Go-annotation directory; SQL/HCL files and DB URLs fail. Neither verb exposes --report or --seed-dir. |
| Dev / shadow database verification | 🟡 | ✅ | ✅ | --shadow-db on generate, checkpoint, baseline and down; docker:// is refused, and schema apply --dry-run skips the rehearsal entirely. |
| Embeddable test runner (Go package) | ✅ | ❔ | ❔ | migration/dbtest exports RunMigrationTest and RunSchemaTest. Nothing cited establishes an Atlas Go test-runner package. |
| Exit-code contract for CI gates | ✅ | ✅ | ➖ | Native 0/1/2 separates expected negative results from command errors; ptah-compat collapses to Atlas CE 0/1, recovered panics still exit 2. |
| Migration test framework (`ptah migrations test`) | ✅ | ❌ | ✅ | Declarative YAML cases: migrate_to, apply_schema, seed, exec, assert. Fresh ephemeral SQLite per case unless `--db-url` is set. |
| Schema test framework (`ptah schema test`) | ✅ | ❌ | ✅ | Desired schema from Go annotations converges before steps; migrate_to is rejected. Atlas CE v1.2.0 registers no schema test verb. |

## Configuration and dev databases

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Atlas project config (atlas.hcl) | 🟡 | ✅ | ✅ | Rejects atlas, script, exporter, deployment; env for_each and schemas; migration baseline, skip_report, repo; data blocks but hcl_schema. |
| atlas.hcl from the native ptah binary | 🟡 | ➖ | ➖ | ptah --env reads ./atlas.hcl only: no --var flag, and --config takes ptah.yaml, so a variable without a default cannot be resolved. |
| data "hcl_schema" reference | 🟡 | ✅ | ✅ | Takes path or paths and exports .url; the Atlas vars input, absolute paths, and any non-file:// value are rejected. |
| Docker dev databases (`docker://` --dev-url) | ❌ | ✅ | ✅ | migrate diff, lint and validate refuse docker:// and require a directly connectable dev database URL. |
| env:// desired-state references | 🟡 | ✅ | ✅ | Resolves only on --to/--from and only src, schema.src, url, dev, migration.dir; elsewhere (--exclude) the literal string is used silently. |
| Native project config (ptah.yaml) | ✅ | ➖ | ➖ | Keys url, dev, schemas, exclude, external_schema, migration, lint, migrate, diff, online_ddl. No variables or functions. Unknown keys fail. |
| Remote and template directory sources | ❌ | ✅ | ✅ | An atlas.hcl data-source question, not a registry one: ptah-compat migration.dir takes file:// only. Native oci:// distribution is a separate ptah path. |
| Variables, locals, and HCL functions | 🟡 | ✅ | ✅ | Only file, fileset, format, getenv, jsonencode evaluate; variable type, sensitive, validation and env for_each are rejected. |

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
| Roles, grants, and row-level security | 🟡 | ❔ | ❔ | PostgreSQL only in `schema render`; `schema apply` emits CREATE ROLE on YugabyteDB. SQL files reject ROLE, GRANT, POLICY and ENABLE RLS. |
| Spanner PostgreSQL interface (spanner) | 🟡 | ❌ | ✅ | Enums and FKs render as skip comments, SERIAL hard-errors, no dedicated driver (uses the PostgreSQL pgx path), and no live container or live test exists. |
| SQL Server and Azure SQL (sqlserver, mssql, tsql) | 🟡 | ❌ | ✅ | The mssql and tsql aliases silently drop views and triggers that sqlserver emits; render's default dialect list and --dialect help omit SQL Server; no sequences, RLS, roles/grants or matviews. |
| SQLite (sqlite, sqlite3) | 🟡 | ✅ | ✅ | Column drops do emit a full rebuild; type, nullability, default and uniqueness changes fail with "modifying columns ... requires a table rebuild plan". PG-only objects error one at a time. |
| Standalone sequences | 🟡 | ❌ | ✅ | PostgreSQL only in `schema render`; `schema apply` emits it on YugabyteDB too. SQL schema files reject CREATE SEQUENCE as unsupported. |
| TiDB and LibSQL | ❌ | ✅ | ✅ | Both names fail dialect normalization: "unsupported database dialect: tidb" / "...: libsql". No renderer, planner or driver entry. |
| Triggers | 🟡 | ❌ | ✅ | `--dialect mssql`/`tsql` drop them while `sqlserver` renders them; MySQL forces FOR EACH ROW; SQL Server rewrites BEFORE to AFTER silently. |
| Views and materialized views | 🟡 | ❌ | ✅ | `mssql`/`tsql` aliases drop views. Matviews are PostgreSQL-only: `schema render` silently drops them elsewhere, `schema apply` errors. |
| YugabyteDB (yugabytedb, ysql) | 🟡 | ❌ | ✅ | Live apply does plan roles, grants, sequences, domains, views, matviews, functions and triggers; only RLS and concurrent indexes are gated off. Offline render omits all of them. |

## Go embedding and developer tooling

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Annotation metadata as JSON Schema | ✅ | ➖ | ➖ | Emits a JSON Schema describing every //ptah directive and attribute; Atlas has no //ptah annotation set for the concept to apply to. |
| API schema export: OpenAPI 3.0 and GraphQL | ✅ | ❌ | ❌ | Go annotations to OpenAPI components or GraphQL SDL; no handlers or resolvers. Absent from the CE inventory and the cited Pro list. |
| Protobuf schema export with pinned field numbers | ✅ | ❌ | ❌ | Edition 2023 output; `--out` pins field numbers, with policies for type removal, name reuse and incompatible change. Not in CE inventory. |
| ptah-ls annotation language server | ✅ | ➖ | ➖ | stdio LSP over //ptah annotations: hover, completion, diagnostics, plus a VS Code extension. Tied to Ptah's own annotation syntax. |
| Public API compatibility gate | ✅ | ➖ | ➖ | check-public-api.sh keeps the committed API baseline and the package tree in sync; pre-v1 breaks need a per-baseline approval line. |
| Query builder for parameterized SQL | 🟡 | ➖ | ➖ | Joins, DISTINCT, GROUP BY, HAVING and RETURNING work; no subqueries, CTEs, LIKE or upsert; SQL Server, ClickHouse, Spanner error. |
| Reusable Go packages (embedder API) | ✅ | ➖ | ➖ | Documented embedder packages cover parse, diff, plan, render, migrate, lint and seed. CE conformance measures CLI commands, not Go APIs. |
| Schema visualization (ERD diagrams) | ✅ | ❌ | ✅ | Mermaid, DOT or SVG ERD from Go annotations only; SVG shells out to Graphviz dot. Atlas features page lists visualization as Pro. |

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
| Artifact integrity check (--verify-sum) | 🟡 | ➖ | ❔ | On migrations push and up only. It checks the directory against the ptah.sum inside the same artifact, so content rewritten with its sum passes. |
| Declarative reference data | ✅ | ❌ | ✅ | //ptah:schema:data rows diffed by key into a reversible data migration. Atlas lists declarative data management as a Pro feature. |
| Digest pinning and write-once version tags | ✅ | ➖ | ❔ | Pushing to an @sha256 reference is refused; --version is write-once and a conflict exits 2. The reference tag, --tag values and latest all move. |
| Environment-scoped SQL seed runner | ✅ | ❌ | ❌ | NNN_desc.env.sql files recorded in schema_seeds with protected-env gates. No seed verb in the CE inventory or the cited Pro list. |
| oci:// as a --schema-file desired-state source | 🟡 | ❌ | ✅ | Accepted by schema render/compare/drift/plan/apply and migrations plan/generate. schema inspect rejects it, and only three of those expose --plain-http. |
| Referrer attachments: lint, plan, deployment reports | 🟡 | ❌ | ❔ | lint --attach, migrations plan --attach and up attach reports to an exact digest. `oci referrers` lists descriptors only; no flag downloads the payload. |
| Registry-backed distribution: `oci://` vs `atlas://` | ✅ | ❌ | ✅ | Same functions as `atlas://` — publish, pull, digest-pin, consume directly — over any OCI registry with no vendor account. Full workflow: [OCI registry artifacts](../../operate/oci-registry/). |

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
