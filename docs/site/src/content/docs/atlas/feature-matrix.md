---
title: Feature matrix
description: Ptah and Atlas capabilities side by side, with the evidence behind every row.
---

This page answers one question: for a given capability, what does Ptah do, what
does the open Atlas community binary do, and what does Atlas keep outside its
community build. Every row cites the evidence it rests on.

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
- **Pro** — capabilities Atlas keeps outside its community build, per the
  [Atlas feature availability](https://atlasgo.io/features) page and
  [pricing page](https://atlasgo.io/pricing).

Every Atlas cell has to come from an Atlas-side source: the command, usage, and
flag inventory the conformance harness reads out of the pinned community
binary, measured behavior of an Atlas binary, or a classification Atlas
publishes. Measurement outranks published classification when they disagree.
Where nothing settles a question, the cell is ❔ rather than a guess.

## Atlas plans are not the CE column

Atlas's public plans are Starter (free), Pro, and Enterprise, and Atlas's own
[pricing page](https://atlasgo.io/pricing) classifies capabilities by plan.
That classification and the **CE** column answer different questions: the CE
column reports what the pinned community binary does, and
the two diverge in both directions. Both examples below were measured on
2026-08-01 against CE v1.2.0:

- The pricing page places migration linting outside the Starter plan, yet the
  CE binary runs `migrate lint` and reports destructive changes.
- The pricing page checks ERD visualization for Starter, yet the CE binary
  rejects `schema inspect --web` as an unknown flag; the ERD lives in the
  hosted service, not in the binary.

Where the pricing page settles a Pro-side question, the Pro column cites it.
Where plan marketing and measured binary behavior differ, the measured behavior
wins the CE cell and the difference column records the tension. The
schema-object rows are the clearest case: the HCL reference marks `partition` a
Pro feature and the pinned community binary plans `PARTITION BY RANGE` anyway,
while a live schema holding a domain, a composite type, a range type, a
sequence and an extension inspects on the same binary to nothing but its one
table. Both readings are in the row.

:::caution
A ✅ in the Ptah column means the capability works, not that it is
byte-identical to Atlas. Ptah is an independent pre-GA implementation, and the
conformance repository states plainly that no number it produces is a
full feature-set parity test. Read the difference column before relying on a
row for a migration decision.
:::

## At a glance

Across the 191 capabilities below:

| Reading | Count |
| --- | --- |
| Ptah supports it fully | 126 |
| Ptah supports it with a stated limitation | 47 |
| Ptah does not implement it | 18 |
| Ptah and Atlas CE both support it | 41 |
| Ptah implements it openly where Atlas gates it behind Pro or Cloud | 44 |
| Ptah has it and neither Atlas edition does | 27 |
| Atlas CE has it and Ptah does not, or only in part | 24 |
| An Atlas column is ❔ — not established by this page's evidence | 4 |

Every 🟡 in the Ptah column names its specific limitation, reproduced against a
binary built from this repository; Atlas-column verdicts rest on the cited
Atlas-side sources only. Confirmed gaps are tracked in
[#926 to #942](https://github.com/stokaro/ptah/issues/926) and [#944](https://github.com/stokaro/ptah/issues/944).

The command surface is counted separately, because it is measured rather than
assessed. The conformance harness inventories every command in the pinned Atlas
CE binary and compares it with the `ptah-compat` surface: 19 of the 37
inventoried commands are open parity targets, and they match on help usage and
flags across 107 observations with one gap — `schema inspect --include`, a
Pro-surface flag the pinned CE binary does not register and Ptah implements
openly ([#951](https://github.com/stokaro/ptah/issues/951)). The remaining 18
are registry, Cloud, or Pro verbs that are not drop-in targets. Ptah implements
seven of them as open capabilities regardless.

## Schema sources

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Annotated Go models from a live database | ✅ | ❌ | ❌ | Writes Ptah annotation source from introspection, with optional db/json tags. No Go-model generator in the CE inventory or Pro list. |
| Atlas CE inspect HCL parses back into Ptah | ✅ | ✅ | ✅ | 30 atlas-differential observations parse Atlas CE 1.2.0 `schema inspect` HCL on Postgres/MySQL/SQLite with zero schema-fact mismatches. |
| Atlas HCL data "external_schema" | ✅ | ❌ | ✅ | Ptah evaluates the data source and runs the program, gated behind `--allow-external-schema`/`PTAH_ALLOW_EXTERNAL_SCHEMA`. Community Atlas rejects `data.external_schema`. |
| Composite multi-source desired schema | ✅ | ❌ | ✅ | Repeatable `--root-dir`/`--schema-file` merge into one schema; conflicts error. Repo docs cite composite_schema as an Atlas Pro data source. |
| Desired-schema artifacts in an OCI registry | ✅ | ❌ | ✅ | `ptah schema push/pull` publish and fetch canonical HCL resolved from Go, YAML, HCL or SQL sources. Verified round trip against registry:2. |
| Directory of .hcl files as one schema source | ✅ | ✅ | ✅ | `--schema-file dir` and `--to file://dir` read a directory of .sql or .hcl files in filename order as an ordered script: mixed formats, an empty directory, a subdirectory and a redeclaration refuse. |
| External program / ORM loaders | ✅ | ✅ | ✅ | `--schema-cmd` or `ptah.yaml` external_schema (needs `--allow-external-schema`) runs a program without a shell emitting SQL, HCL, or YAML. |
| Go struct annotations | ✅ | ❌ | ❌ | Ptah parses //ptah:schema:* comments into the desired schema. Atlas's route to Go models is an external ORM provider program. |
| HCL foreign_key deferrable | ✅ | ❌ | ❌ | `deferrable` and `initially` are carried through HCL, the IR, the renderer and the PostgreSQL catalog read. The community binary plans no DEFERRABLE for the same file. |
| HCL function calls in schema files | 🟡 | 🟡 | 🟡 | sql() reduces to its SQL everywhere; other calls evaluate against a function set measured name by name on the community binary. uuid is a type, not a function. |
| HCL locals, lock, atlas, dynamic/for_each | 🟡 | 🟡 | 🟡 | `locals` is evaluated and `local.x` resolves. `dynamic` blocks are expanded: for_each, labels, iterator and content. `lock`/`atlas` are still dropped at exit 0 on the compat surface. |
| HCL names outside the parsed subset | 🟡 | ✅ | ❌ | `ptah-compat` drops an unmodeled top-level name whose body names a declared schema: `procedure { schema = schema.main }` exits 0, `schema.nope` exits 1 on both binaries. Native `ptah` refuses by name. |
| HCL table and column child blocks | ✅ | 🟡 | ✅ | column, primary_key, index, unique, foreign_key, check, partition, row_security, constraint, platform; column nests as, identity, platform. The binary drops row_security; ptah-compat plans it. |
| HCL top-level blocks Ptah parses | ✅ | 🟡 | ✅ | schema, enum, table, extension, sequence, domain, composite, range, function, view, materialized, trigger, policy, role, permission, data. The community binary plans DDL for table and enum. |
| HCL variable blocks and var.* references | ✅ | ✅ | ✅ | `variable` blocks bind `var.x`, `--var name=value` overrides them, and a typed variable with no value exits 1 with `missing value for required variable "x"` — the community binary's own text. |
| Live database as desired state | ✅ | ✅ | ✅ | One connectable DB URL can be the desired side of compat schema apply/diff and migrate diff; `ptah db read` introspects natively. |
| Live database to Go annotation source | ✅ | ❌ | ❌ | `ptah introspect` writes annotated Go models from a live DB; repo docs record Go annotations as a first-party Ptah workflow. |
| Migration directory as a source | ✅ | ✅ | ✅ | Atlas-format directory with `atlas.sum`, replayed on a required `--dev-url`. Works on `ptah schema inspect` and compat apply/diff/migrate diff. |
| Ptah-only HCL schema extensions | ✅ | ➖ | ➖ | platform/override per dialect, EXCLUDE constraint block, column enum, table checks/custom, index ops, ClickHouse granularity, seed data block. |
| SQL DDL schema files | ✅ | ✅ | ✅ | Accepted by native `--schema-file` and compat `--to`/`--from`. Reads back every object kind Ptah renders; DO blocks, COMMENT ON and raw routine bodies still parse and are dropped. |
| YAML schema files | ✅ | ❌ | ❌ | Strict parser; unknown keys fail. Repo docs list Atlas OSS data sources as SQL, HCL, external schema, and remote/template dirs. |

## Declarative and direct schema changes

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `--dry-run`, `--auto-approve`, and `--edit` | ✅ | ✅ | ✅ | All three registered and functional; `--edit` opens $VISUAL/$EDITOR and the edited SQL is applied. `--dry-run` and `--auto-approve` exclude each other on the command line, as on CE. |
| `--exclude` glob and type selectors | 🟡 | ✅ | ✅ | Resource globs, one final [type=...], and leading [type=schema]. Qualified globs reach every kind. Qualified children are a depth error. The schema form keeps one meaning across sources. |
| `--include` resource selectors | ✅ | ❌ | ✅ | CE registers `--include` on apply/diff but aborts it as non-community, and registers none on inspect. Ptah has all three, with union semantics and cross-scope dependency diagnostics. |
| `--schema` / -s scoping of both sides | ✅ | ✅ | ✅ | Schema names scope resources; non-extension matches carry extensions as support. Diff refuses exact PostgreSQL-family server namespaces before SQL because catalogs do not round-trip safely. |
| `schema diff --export` | ❌ | ❌ | ✅ | Registered and refused by name. The flag selects an exporter declared by an atlas.hcl `exporter` block, and Ptah evaluates no such block, so there is nothing to select. Tracked by stokaro/ptah#1620. |
| `schema inspect --include` filtering | ✅ | ❌ | ✅ | Compat and native inspect select top-level resources with the apply/diff selector engine. CE rejects the flag as unknown. A selection matching nothing renders nothing, exits 0, and says so on stderr. |
| `schema inspect --output` | ✅ | ❌ | ✅ | `-o/--output` writes the rendered schema to a file instead of stdout, published atomically so a reader never sees a partial document. |
| An include selector matching neither diff side fails closed | ✅ | ❌ | ❌ | Compat and native retain `--include`; no-match exits nonzero. Qualified PostgreSQL extension creates preserve installation schema; moves fail before SQL. |
| Apply advisory lock, `--lock-timeout`, `--lock-name`, `--skip-lock` | ✅ | 🟡 | ✅ | Real locks on PostgreSQL, YugabyteDB, MySQL, MariaDB, SQL Server; others run unlocked with a note. `--lock-name` and `--skip-lock` are Pro surface adopted openly; CE registers only `--lock-timeout`. |
| Compat inspect block superset opt-in | ✅ | ❌ | ❌ | Compat inspect omits unreferenced extension, sequence and policy blocks; an env variable restores them. |
| Desired-state sources for `--to` and `--from` | ✅ | ✅ | ✅ | Files, a file:// directory of .sql or .hcl schema files, one DB URL, one atlas.sum dir, or env://; atlas:// fails early. |
| Dev-database rehearsal before apply | ✅ | ✅ | ✅ | Dev DB reset, target schema recreated, exact plan rehearsed, under `--dry-run` too; dev==target and failed rehearsal abort. A non-database `--to` requires `--dev-url`. |
| Drift detection against desired schema | ✅ | ❌ | ✅ | Native `ptah schema drift`: `--severity`, `--exit-code`, `--ignore`, text/json/github-actions. Atlas Cloud drift monitoring is out of scope. |
| Exclude selector that matches nothing is diagnosed | ✅ | ❌ | ❌ | A selector naming no object warns on inspect and diff and exits 1 on apply, and only a filter that asked it may call it empty; a PTAH_ATLAS opt-in restores the permissive behavior. |
| Exclude subtracts a named schema and its contents | ✅ | ✅ | ✅ | A one-part selector names a schema by catalog or quoted spelling, and it leaves with all contents. Case-preserving names remain addressable without collapsing case or quoted whitespace. |
| Exclude subtracts sequences, domains, composite types and range types | ✅ | ❔ | ✅ | `--exclude` reaches the same object kinds `--include` selects. Before, these four were read and cloned but never offered to a pattern, so excluding one was a silent no-op that still planned its DROP. |
| Go-template `--format` output | 🟡 | ✅ | ✅ | schema apply registers the shared helper set, so {{ json . }} renders an Atlas-shaped document. schema diff registers only sql, as the community binary does. |
| Inspect `--exclude` field selectors | 🟡 | ❌ | 🟡 | [type=extension].version, and .comment on table, view and materialized_view; .* names all of them. A field Ptah cannot subtract is refused, not ignored as the community binary does. |
| Inspect non-database sources via `--dev-url` | ✅ | ✅ | ✅ | Schema file, `atlas.sum` migration dir, or env:// is materialized on a reset dev DB then introspected; without `--dev-url` it fails. |
| Inspect split/write file exports | ✅ | ❌ | ✅ | `{{ hcl . \| split \| write "dir" }}` writes object/schema/type trees; pinned Atlas CE rejects split, write, hcl as non-community. |
| Inspected document declares what it does not describe | ✅ | ❌ | ❌ | A compat inspect document records the block kinds it omitted, in its own header and in every split member, so a later apply reads the omission as unknown rather than as deletion intent. |
| JSON output for native schema diff | ✅ | ➖ | ➖ | Native ptah schema diff `--format` json emits a machine-readable statements document; the Go-template row only states that {{ json . }} fails on the compat diff, leaving native JSON unstated. |
| Local pre-approved plan files | ✅ | ❌ | ✅ | `schema plan` writes Atlas `.plan.hcl` by default (`.json` keeps the native plan); `apply --plan` reads both, Atlas-authored included, verified by replay against `--to` plus an end-state check. |
| schema apply against a live database | ✅ | ✅ | ✅ | Diffs `--url` against the `--to` desired state, prints the SQL plan, applies after confirmation. Verified end to end on SQLite. |
| schema clean | 🟡 | ✅ | ✅ | `--include`/`--exclude` narrow cleanup. PostgreSQL-family scoped drops are dependency-safe and transactional; `RESTRICT` cannot cascade outside the selection. |
| schema diff between two schema states | ✅ | ✅ | ✅ | SQLite rebuilds a table for changes ALTER TABLE cannot express, including one other tables refer to: the plan brackets itself in PRAGMA foreign_keys. |
| schema fmt (HCL canonical layout) | ✅ | ✅ | ✅ | Formats .hcl paths recursively and prints only changed files. Native `ptah schema fmt --check` adds a no-write CI gate. |
| schema inspect to HCL, SQL, or JSON | ✅ | ✅ | ✅ | Default HCL; rendered HCL/SQL/JSON use explicit helper templates. Bare and whitespace-wrapped hcl/sql/json are literal template text. Native shorthands still render and add file export. |
| Schema-qualified exclude globs for enums and functions | ✅ | 🟡 | ✅ | Enums and functions match schema-qualified globs on the rule tables and views use, and the match reaches the planned DROP. The community binary matches `app.mood`; it reports no functions. |
| Verb `schema stats` | ❌ | ❌ | ✅ | Beyond the CE pin: in Atlas it exists as `schema stats inspect` (OpenMetrics) and rejects SQLite at runtime; the gap register triages it out of scope as observability. |
| Verb `schema validate` | 🟡 | ❌ | ✅ | Beyond the CE pin: no validate verb; the gap register triage covers it with native schema render parse/load validation plus schema test and schema apply `--dry-run`. |

## Versioned migrations

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `--dir` defaults to `file://migrations` | ✅ | ✅ | ✅ | All eight migrate verbs registering `--dir` default it to `file://migrations`. Never a fallback: the flag, `PTAH_DIR`, `PTAH_MIGRATIONS_DIR` and `atlas.hcl` outrank it, and `atlas.sum` is still gated. |
| An empty migration directory is not a checksum error | ✅ | ✅ | ✅ | `ptah-compat migrate validate` and `migrate lint --latest` exit 0 on a directory holding no migration files. Native `ptah migrations validate` and `ptah migrations lint` keep their refusals. |
| Apply pending migrations (apply/up) | 🟡 | ✅ | ✅ | Dry runs read stored revisions, select only pending files, and retain execution-time validation. ClickHouse Atlas-format revision tables are covered. |
| Atlas R-suffixed (`1R_`, `R__`) migration execution | ✅ | ✅ | ✅ | Both execute a native Atlas `R` or `<number>R` file once and record its version token, and neither reapplies it when the body changes: reapply-on-checksum is a Flyway feature. |
| Atlas SQL template migrations (`--atlas-env`) | ✅ | ❌ | ❌ | Go-template actions in Atlas-format migration files render before execution with .Env set by `--atlas-env`; sibling *.sql files supply shared {{ template }} definitions. CE runs the braces as SQL. |
| Atlas txtar migration sections (-- atlas:txtar) | ✅ | ❌ | ✅ | `-- atlas:txtar` executes migration.sql/down.sql and enforces checks.sql plus ordered checks/*.sql, including atlas:assert oneof; unrelated files are ignored. CE runs every section as plain SQL. |
| Atlas-format checkpoint output | ✅ | ❌ | ✅ | `migrate checkpoint --dir-format atlas` writes the single `-- atlas:checkpoint` file plus atlas.sum, and is compat's default; `--dir-format ptah` writes the reversible pair. Up-only, as Atlas is. |
| Baseline an existing database | ✅ | ✅ | ✅ | Ptah adds a standalone baseline verb with `--shadow-db` verification and `--dry-run`; CE exposes baselining as migrate apply `--baseline`. |
| Create an empty migration file | ✅ | ✅ | ✅ | Native create writes an up/down pair; compat new writes the selected layout's skeleton for atlas and all five external formats, refreshes `atlas.sum`, `--edit` opens $VISUAL or $EDITOR on atlas. |
| Direct Flyway revision identity | ✅ | ✅ | ✅ | Flyway tokens persist; numeric keys order them. Empty identities render as "". Baselines use raw-token order. A repeatable target preserves history CE deletes; unknown roles and ties fail closed. |
| Directory integrity file and execution gate | ✅ | ✅ | ✅ | Both sums support hash/validate. Native SQL execution gates hashed dirs; compat apply/status/set/new/diff/down do too. Import verifies source sums. Replay uses one verified snapshot and refuses drift. |
| Directory maintenance: edit, rebase, rm | ✅ | ❌ | ✅ | Each rewrites `ptah.sum`/`atlas.sum` and refuses a migration applied in `--db-url` unless `--force`; CE aborts all three as non-community verbs. |
| Dynamic down planning (`migrate down --plan`) | ❌ | ❌ | ✅ | `--plan`, `--to-tag` and `--skip-checks` are recorded waivers that fail loudly; Ptah reverts only through pre-planned down files. Tracked by stokaro/ptah#1621. |
| Execution order (`--exec-order`) | ✅ | ✅ | ✅ | linear fails on a pending migration below the current version, linear-skip leaves it pending, and non-linear applies it. Atlas chained revision hashes remain valid after an insertion. |
| External `--dir-format` outside `migrate import` | 🟡 | ✅ | ✅ | All supporting verbs read the selected layout; new and diff write it with exact rollback. Goose represents whole-file no-transaction execution; four formats fail closed. Tracked by stokaro/ptah#1630. |
| Failed rollback state is recorded and recoverable | ✅ | ❌ | ❌ | Ptah records failed rollback direction, error, and completed-statement count in both revision-table formats; compat keeps the Atlas schema but does not copy Atlas's hidden failed-down state. |
| Flyway repeatable (`R__`) migration import | 🟡 | ✅ | ✅ | Compat import converts `R__` to a one-time migration ordered last, as Atlas CE runs it. Editing the body then fails on a Ptah revision checksum where CE exits 0. |
| Generate migrations from a schema diff | 🟡 | ✅ | ✅ | diff and new stamp the UTC YYYYMMDDHHMMSS second, stepping only past a version already taken, as CE does; checkpoint and rebase bump past the newest. Every step parses back as a second. |
| migrate apply `--allow-dirty` semantics and the not-clean adoption gate | ✅ | ✅ | ✅ | Exact-identity retries require the current provider to own the dirty body; the flag also permits unmanaged-object adoption. Recovery preserves the committed prefix. |
| Migration checkpoints (squash history) | ✅ | ❌ | ✅ | Replays the directory on `--shadow-db` into a cumulative checkpoint: the ptah reversible pair, or Atlas's single `-- atlas:checkpoint` file under `--dir-format atlas`. CE gates the verb. |
| Migration import from other tools | 🟡 | ✅ | ✅ | Native import writes Ptah format; compat import writes Atlas format and orders `R__` last. Conventional Liquibase SQL becomes a global numeric changeset stream. Liquibase XML/YAML/JSON is unsupported. |
| Migration linting | ✅ | 🟡 | ✅ | CE registers `migrate lint` with Open rules; its features page marks the CLI Pro. Compat requires `--dev-url`. `--latest 0` disables latest selection but preserves Git; opt-ins lift each precondition. |
| Migration lock, lock timeout, `--lock-name`, `--skip-lock` | ✅ | 🟡 | ✅ | Compat `--lock-timeout` bounds directory and dev-db locks. `--lock-name` and `--skip-lock` on `migrate apply` are Pro surface adopted openly; CE registers only `--lock-timeout`. |
| Migration status report | ✅ | ✅ | ✅ | Compat status mirrors the Atlas default report shape and renders Go templates over .Env, .Available, .Applied, .Pending, .Current, .Next. Native ptah keeps its own block. |
| Online DDL routing via gh-ost or pt-osc | ✅ | ❌ | ❌ | ptah.yaml online_ddl.tool (ghost\|pt-osc), threshold_rows, args, and fallback (error\|plain) route large-table ALTERs through an online-DDL tool during migrations up/down. CE has no such routing. |
| Pre-migration database backups (`--pg-dump-to`) | ✅ | ❌ | ❌ | `--pg-dump-to` writes a pg_dump custom-format backup and `--mysqldump-to` a SQL backup before applying or rolling back; ptah.yaml key migration.pg_dump_to. |
| Pre-migration webhook and shell hook gates | ✅ | ❌ | ❌ | `--webhook` POSTs migration metadata and requires HTTP 200; `--pre-up-hook`/`--pre-down-hook` run a shell command that must exit 0, else the run aborts. Also ptah.yaml migration.webhook/pre_up_hook. |
| Prometheus metrics endpoint (`--metrics-addr`) | ✅ | ❌ | ❌ | migrations up, down, and status serve a Prometheus /metrics endpoint at the given address for the run. |
| Repair dirty or partial revision state | ✅ | ❌ | ❌ | Under the migration advisory lock, `--resume-from` verifies the committed prefix, then finishes up before marking applied or down before removing the revision. Atlas CE has no repair verb. |
| Report of an ignored `--dir` URL query key | ✅ | ❌ | ❌ | Only `?format=` selects a layout. On the eight verbs accepting a `--dir` query, other keys are ignored, named on stderr, exit 0 unchanged; `PTAH_STRICT_DIR_QUERY=1` refuses. Six verbs take no query. |
| Revision table format and placement | ✅ | ✅ | ✅ | `--revision-format` ptah\|atlas plus `--migrations-table` and `--migrations-schema`; the compat path defaults to Atlas rows. |
| Roll back applied migrations (down) | 🟡 | ❌ | ✅ | Ptah validates all selected down bodies before changing state. Dry-run reports distinguish preflight rejection from attempted rollback. Registry flags remain waivers. |
| Set revision state to a version | ✅ | ✅ | ✅ | Removes revision rows above the target, keeps rows at or below it, and inserts missing rows through it as manually set. |
| Structured JSON log output (`--log-format`) | ✅ | ❌ | ❌ | migrations up, down, and status take `--log-format` text\|json and `--log-level` debug\|info\|warn\|error for machine-readable run logs. |
| Transaction modes (`--tx-mode` file/all/none) | 🟡 | ✅ | ✅ | File/all/none behavior is tested; none-mode partial progress is pinned both ways against Atlas CE v1.3.0. Txtar section modes remain a Ptah safety extension. |
| Verb `migrate ls` | ✅ | ❌ | ✅ | `ptah migrations ls` lists a migration directory with no database; `ptah-compat migrate ls` is the drop-in spelling (`--dir`, `-s`, `-l`). Beyond the CE pin, so strict compatibility omits it. |
| Verb `migrate show` | ✅ | ❌ | ✅ | `ptah migrations show` prints a stored migration's SQL with no database; `ptah-compat migrate show {name \| version}...` is the drop-in spelling. Beyond the CE pin, so strict compatibility omits it. |

## Linting and safety

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `migrate lint` requires `--latest` or `--git-base` | ✅ | ✅ | ✅ | Refused before the directory is read and before `--dev-url` is contacted. `PTAH_ATLAS_LINT_ALL_VERSIONS=1` restores linting the whole directory here; native `ptah migrations lint` needs no scope. |
| `schema apply --skip-lint` | ✅ | ❌ | ✅ | With an atlas.hcl `lint` policy, the planned SQL is linted against the rules it names and an error-rated finding refuses the apply; `--skip-lint` applies anyway. No policy, no lint pass, as in CE. |
| Analyzers that need a dev-database schema diff | 🟡 | ✅ | ✅ | Ptah's analyzers read SQL text, so a concern whose subject appears only in the resulting schema is unreported: a RENAME COLUMN draws DS103 from both, but only Atlas adds MF103 for the new column. |
| Apply-time destructive-change gate | ✅ | ❌ | ➖ | migrations up refuses destructive pending files; .ptah-lint.yaml disabled-rules reopens the gate and ptah.sum does not hash that file. |
| Atlas Pro analyzer code coverage | 🟡 | ➖ | ✅ | OW101/OW102 have no rule; PG301, PG304, MY130, MY133, MY136 fire under broader codes (DS103, PG104, CD103, MY101), not dedicated ones. Tracked by stokaro/ptah#1631. |
| Atlas web reports (`--web`) | ❌ | ❌ | ✅ | Not registered on migrate lint or schema diff; rejected as an unknown flag. Pinned Atlas CE v1.2.0 does not register it either. |
| Check bypass on the compat surface | ✅ | ❌ | ❌ | No Atlas build registers `--skip-checks` on migrate apply, so the compat bypass is PTAH_SKIP_CHECKS. Explicit-only on migrate down. |
| CI integration (GitHub Action, annotations) | ✅ | 🟡 | ✅ | stokaro/ptah-action@v1 posts a sticky PR comment; `--format` github-actions emits annotations. The community binary has no annotation mode; its lint `--format` takes a Go template only. |
| Custom lint rules and check-level policy | 🟡 | ❌ | ✅ | Custom rules run only through Go registration. Atlas project rule, review, naming, non_linear, and force names are accepted as Atlas CE no-ops and reported as having no effect. |
| Default-firing Atlas analyzer concern mapping | ✅ | ➖ | ➖ | lint-analyzer-catalog maps every default-firing Atlas concern to a covering Ptah rule, severity and line; 0 gap on the committed corpus. |
| Dev-URL schema scope on `migrate lint` | ✅ | ✅ | ✅ | `ptah-compat migrate lint` reviews only the schema the dev URL's search_path names, matching the pinned CE binary. Native `ptah migrations lint` reads SQL text and deliberately does not scope. |
| Generation-time destructive-change gate | ✅ | ❌ | ❌ | migrations generate and plan fail with `--check-destructive` when the generated SQL contains destructive statements; `--allow-destructive` reopens the gate. Distinct from the apply-time gate row. |
| Inline nolint suppression | ✅ | ✅ | ✅ | Every code the compat surface prints is silenced by that code; analyzer names work on both surfaces; a blank line detaches a directive. Unknown selectors accepted silently, matching CE. |
| Native migration lint rule set | ✅ | 🟡 | ✅ | 42 codes across 9 families, gated by `--dialect`. Atlas lists destructive and backward-incompatible rules Open; concurrent-index rules Pro. |
| Per-rule severity policy | ✅ | ❌ | 🟡 | Severity vocabulary is info\|warning\|error; only error gates. The community binary carries no severity attribute: it accepts one and ignores it, exactly as it treats an invented attribute. |
| Pre-migration assertion checks | 🟡 | ❌ | ✅ | Scalar SELECTs; txtar checks.sql and checks/*.sql support all-of/oneof groups. CE ignores checks. Compat bypass is PTAH_SKIP_CHECKS. |
| SARIF 2.1.0 lint report | ✅ | ❌ | ➖ | Native `--format` sarif emits SARIF 2.1.0 with ruleId, level and file:line; Atlas documents Go-template `--format` output for migrate lint. |
| Standalone SQL file linting (`ptah sql lint`) | ✅ | ❌ | ❌ | Lints arbitrary SQL files or stdin against per-dialect capability presets (9 dialects incl. sqlserver), refined by a `--version` server string; text/json output, rule disable. Not. |
| Statement safety classification report | ✅ | ➖ | ➖ | plan `--report` text\|html\|json and generate `--report` html\|json emit highest severity, a destructive flag, and per-statement assessments. |

## Testing

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Atlas `.test.hcl` ingestion | ✅ | ❌ | ✅ | Implemented: `.test.hcl` is read alongside native YAML by `schema test` and `migrate test`. Adding `output` to an `exec` makes it an assertion; step order is preserved and cases are selected by kind. |
| Atlas CE strict oracle profile | ✅ | ➖ | ➖ | Strict mode builds the CE tree and refuses unsafe sources, migration extensions, and catalog-only live objects before output, comparison, or mutation. Default retains the full surface. |
| Atlas-shaped migrate test / schema test verbs | 🟡 | ❌ | ✅ | Reports and seed directories are exposed. schema test accepts `-s/--schema`, Go, SQL/HCL, or database sources; `--var` and isolated `data.hcl_schema.vars` reach HCL files; env:// refuses. |
| Dev / shadow database verification | 🟡 | ✅ | ✅ | `--shadow-db` on generate, checkpoint, baseline and down; the replay path provisions a docker:// dev database. schema apply `--dry-run` runs the same rehearsal the real apply does. |
| Embeddable test runner (Go package) | ✅ | ❔ | ❌ | migration/dbtest exports RunMigrationTest and RunSchemaTest. CE stays unknown: a CLI probe cannot see a Go API; an Atlas-side source naming a test-runner entry point would settle it. |
| Exit-code contract for CI gates | ✅ | ✅ | ➖ | Native 0/1/2 separates expected negative results from command errors; ptah-compat collapses to Atlas CE 0/1, recovered panics still exit 2. |
| Migration test framework (`ptah migrations test`) | ✅ | ❌ | ✅ | Declarative YAML cases: migrate_to, apply_schema, seed, exec, assert. Fresh ephemeral SQLite per case unless `--db-url` is set. |
| Schema test framework (`ptah schema test`) | ✅ | ❌ | ✅ | Desired schema from Go annotations, SQL or HCL files, or a live database converges before steps. HCL sources take repeatable `--var`; migrate_to is rejected. |

## Configuration and dev databases

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `--var` does not require an `atlas.hcl` | ✅ | ✅ | ✅ | `-c` and `--env` select a project file and still require one. `--var` only supplies values to one, on every verb. Its syntax is still checked with no `atlas.hcl` present. |
| A malformed `--var` is refused wherever it is spelled | ✅ | ✅ | ✅ | CE parses `--var` while parsing flags, so a value with no `=` is refused before any project file is sought. Ptah checks it on every command under `schema` and `migrate`, even ones that never read it. |
| Atlas project config (atlas.hcl) | 🟡 | ✅ | ✅ | Validates every env. `migrate apply` expands labeled or unlabeled env `for_each` in stable order; typed list/map are Ptah extensions. Other commands require one instance. |
| atlas.hcl file() and fileset() path confinement | ✅ | ❌ | ❌ | Ptah confines file() and fileset() to the atlas.hcl directory: absolute, parent-traversal and symlink escapes are refused by name. Atlas reads them. Deliberate divergence; exit 1 either way today. |
| atlas.hcl from the native ptah binary | 🟡 | ➖ | ➖ | ptah `--env` reads ./atlas.hcl in the working directory; `--var name=value` supplies a variable with no default on every env-aware verb; `--config` still takes ptah.yaml only. |
| AWS RDS token project data source (data "aws_rds_token") | 🟡 | ✅ | ✅ | A valid unreferenced declaration is accepted lazily, matching Atlas CE. Resolving the token still fails explicitly because the provider is not implemented. Tracked by stokaro/ptah#1617. |
| data "hcl_schema" reference | ✅ | ✅ | ✅ | Takes path, paths and vars, and exports .url. `vars` is scoped to the files that data source selects and `--var` does not cross that boundary, as on CE. A bad path or scheme names its rule. |
| Docker dev databases (`docker://` `--dev-url`) | 🟡 | ✅ | ✅ | migrate diff, lint, validate, schema inspect, schema diff, apply, both test verbs, checkpoint and down start a container and remove it. `schema plan` reads the value and says it starts none. |
| env:// desired-state references | 🟡 | ✅ | ✅ | Resolves only on `--to`/`--from` and only src, schema.src, url, dev, migration.dir; native `--schema-file` refuses it by name; elsewhere (`--exclude`) the literal string is used silently. |
| External program project data source (data "external") | ✅ | ✅ | ✅ | Runs argv directly without a shell and returns untrimmed stdout. Caller cancellation, a 60-second timeout, bounded output, process-tree termination, and sanitized errors define the boundary. |
| GCP Cloud SQL token project data source (data "gcp_cloudsql_token") | 🟡 | ✅ | ✅ | A valid unreferenced declaration is accepted lazily, matching Atlas CE. Resolving the token still fails explicitly because the provider is not implemented. Tracked by stokaro/ptah#1617. |
| Native project config (ptah.yaml) | ✅ | ➖ | ➖ | Keys url, dev, schemas, exclude, external_schema, migration, lint, migrate, diff, online_ddl. No variables or functions. An unknown key fails, naming the key, its line, and the accepted keys. |
| PTAH_* environment-variable flag equivalents | ✅ | ❌ | ❌ | Every flag on every native verb has a documented PTAH_* environment variable ([env: PTAH_X] in help) that substitutes for the flag. CE annotates no flag with an environment variable. |
| Remote directory project data source (data "remote_dir") | 🟡 | ✅ | ✅ | A valid unreferenced declaration is accepted lazily, matching Atlas CE. Resolving the source still fails explicitly because remote directory fetching is not implemented. Tracked by stokaro/ptah#1617. |
| Remote schema project data source (data "remote_schema") | 🟡 | ✅ | ✅ | A valid unreferenced declaration is accepted lazily, matching Atlas CE. Resolving the source still fails explicitly because remote schema fetching is not implemented. Tracked by stokaro/ptah#1617. |
| Runtime variable project data source (data "runtimevar") | ✅ | ✅ | ✅ | Reads Go CDK runtime-variable URLs with byte-preserving string output and a configurable positive timeout. Constant, file, HTTP(S), AWS, and Google Cloud providers are registered. |
| SQL project data source (data "sql") | ✅ | ✅ | ✅ | Runs one query and exports count, first value, and all values. Requires one column and one HCL row type. Heterogeneous rows fail explicitly; Atlas CE panics. Unreferenced blocks stay lazy. |
| Template directory project data source (data "template_dir") | ✅ | ✅ | ✅ | Shared Go templates emit root lowercase .sql migrations only. New and diff synchronize new files and checksum to the confined source path; hash-only stays virtual. |
| Variables, locals, and HCL functions | 🟡 | ✅ | ✅ | file, fileset, format, getenv, jsonencode, and toset evaluate. Variables add map(string). Env `for_each` exposes atlas.env, each.key, and each.value. |

## Databases and schema objects

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Capability profile of a live target (`ptah db capabilities`) | ✅ | ❔ | ❌ | Reports the dialect, resolved preset and how it was reached, the support level and every capability key for a connected server, as text or stable JSON. No source here names an Atlas equivalent. |
| ClickHouse (clickhouse, ch) | 🟡 | ❌ | ✅ | Tables, indexes, plain views, roles and grants, and named table CHECKs. Other modeled objects produce named not-supported comments on `render` and `apply`; domains, composites and ranges still drop. |
| CockroachDB (cockroachdb, crdb) | 🟡 | ❌ | ✅ | `CONCURRENTLY`, XML and advisory locks are disabled. 25.4 refuses generic DROP CONSTRAINT and CREATE OR REPLACE TRIGGER; 26.2 accepts them. Row-level TTL is managed here only (#1027). |
| Declared support level per database release line | ✅ | ❔ | ❔ | 26 declared release lines: 19 certified, 2 legacy-tested, 5 best-effort. Upstream end-of-life lowers the level, not the behavior; a line Ptah does not declare resolves to best-effort. |
| Domains, composite types, and range types | 🟡 | ❌ | ✅ | Emitted across the PostgreSQL family in `schema render` and `schema apply` alike. A changed range type is planned as DROP TYPE + CREATE TYPE. The community binary reports none of the three. |
| Enum types | 🟡 | ✅ | ✅ | An enum is whatever the schema declares as one; the type name plays no part. The undocumented `enum_` prefix that gated the inline rewrite, the scoped-schema filter and the PostgreSQL cast is gone. |
| Extensions | 🟡 | ❌ | ✅ | PostgreSQL and YugabyteDB preserve non-default schemas. CockroachDB and Spanner refuse placement before SQL. Other targets name unsupported extensions without executing them; Atlas CE refuses blocks. |
| Functions | 🟡 | ❌ | ✅ | `schema render` and `schema apply` emit functions on PostgreSQL and MySQL families; MySQL/MariaDB read and plan them. Modified foreign `DEFINER` routines fail closed before SQL. Spanner skips. |
| MySQL and MariaDB | 🟡 | ✅ | ✅ | Stored functions render, read back and plan. Matviews and roles fail closed; extensions and MariaDB SEQUENCE objects get a not-supported comment. Domains, grants and RLS still drop silently. |
| Oracle, Snowflake, Redshift, Databricks | ❌ | ❌ | ✅ | No dialect entry; the names fail normalization the same way TiDB does. Listed as Atlas Pro drivers. Tracked by stokaro/ptah#1616. |
| PostgreSQL 12+ (postgres, postgresql) | ✅ | ✅ | ✅ | Reference engine of the PostgreSQL family: views, matviews, functions, triggers, sequences, roles, RLS and domains all render. Presets 12-13, 14-16, 17+ from the server banner. |
| Roles, grants, and row-level security | 🟡 | ❌ | ✅ | PostgreSQL, CockroachDB and YugabyteDB emit roles, grants and RLS; ClickHouse and SQL Server emit roles and grants, not RLS; Spanner names all three. Atlas prices this Pro; CE no-ops `role`. |
| Spanner PostgreSQL interface (spanner) | 🟡 | ❌ | ✅ | Enums, sequences, matviews, functions and triggers render as named skip comments; foreign keys render. SERIAL hard-errors, no dedicated driver (PostgreSQL pgx path), no live container or live test. |
| SQL Server and Azure SQL (sqlserver, mssql, tsql) | 🟡 | ❌ | ✅ | Every accepted spelling renders the same DDL, and SQL Server is in `schema render`'s dialect list and `--dialect` help. Sequences, roles and grants render, read back and plan; RLS and matviews do not. |
| SQLite (sqlite, sqlite3) | 🟡 | ✅ | ✅ | Column drops rebuild; other shape changes fail closed. Virtual tables preserve exact identity, and indexes on module-owned shadow tables are refused. |
| Standalone sequences | 🟡 | ❌ | ✅ | PostgreSQL, CockroachDB, YugabyteDB and SQL Server emit CREATE SEQUENCE. MySQL, MariaDB, ClickHouse, Spanner and SQLite name it as skipped rather than dropping it. |
| TiDB and LibSQL | ❌ | ✅ | ✅ | Both names fail dialect normalization: "unsupported database dialect: tidb" / "...: libsql". No renderer, planner or driver entry. Tracked by stokaro/ptah#1615. |
| Triggers | 🟡 | ❌ | ✅ | Every accepted spelling of an engine renders the same trigger DDL; Spanner names it skipped. MySQL/MariaDB refuse FOR EACH STATEMENT and SQL Server refuses BEFORE instead of downgrading it. |
| Views and materialized views | 🟡 | ❌ | ✅ | Plain views work on every dialect. Materialized views render on PostgreSQL, CockroachDB, YugabyteDB, and ClickHouse. Only manual refresh is supported; other targets or strategies fail closed. |
| YugabyteDB (yugabytedb, ysql) | 🟡 | ❌ | ✅ | Roles, grants, RLS, sequences, domains, views, matviews, functions, triggers and CREATE INDEX CONCURRENTLY are enabled. Only DROP INDEX CONCURRENTLY is gated off on measured 2026.1. |

## Go embedding and developer tooling

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| Annotation metadata as JSON Schema | ✅ | ➖ | ➖ | Emits a JSON Schema describing every //ptah directive and attribute; Atlas has no //ptah annotation set for the concept to apply to. |
| API schema export: OpenAPI 3.0 and GraphQL | ✅ | ❌ | ❌ | Go annotations, or a YAML, HCL or SQL schema file, to OpenAPI components or GraphQL SDL; no handlers or resolvers. Absent from the CE inventory and the cited Pro list. |
| Concurrency-guarded migration plan publication | ✅ | ➖ | ➖ | generator.PlanMigration binds a plan to a directory snapshot; WriteFiles rejects changed history (ErrMigrationDirectoryChanged) under a cross-process lock; concurrent reuse fails (ErrMigrationPlanInUs |
| Go annotations to HCL export with cleanup | ✅ | ➖ | ➖ | Writes HCL from Go annotations; cleanup requires zero diagnostics and refuses unparsed directives. Opaque function, view, materialized-view, and trigger bodies are reported and block cleanup. |
| Pinned database sessions (WithSession) | ✅ | ➖ | ➖ | `WithSession` on `DatabaseConnection` pins one physical session for a callback, rebinding reader/writer/SQL runner, and discards the connection so session state cannot leak. |
| Protobuf schema export with pinned field numbers | ✅ | ❌ | ❌ | Edition 2023 output from Go annotations or a YAML, HCL or SQL schema file; `--out` pins field numbers, with policies for type removal, name reuse and incompatible change. Not in CE inventory. |
| ptah-ls annotation language server | ✅ | ➖ | ➖ | stdio LSP over //ptah annotations: hover, completion, diagnostics, plus a VS Code extension. Tied to Ptah's own annotation syntax. |
| Public API compatibility gate | ✅ | ➖ | ➖ | check-public-api.sh keeps the committed API baseline and the package tree in sync; pre-v1 breaks need a per-baseline approval line. |
| Query builder for parameterized SQL | 🟡 | ➖ | ➖ | Joins, DISTINCT, GROUP BY, HAVING and RETURNING work; no subqueries, CTEs, LIKE or upsert; SQL Server and ClickHouse error. Tracked by stokaro/ptah#941. |
| Reusable Go packages (embedder API) | ✅ | ➖ | ➖ | Documented embedder packages cover parse, diff, plan, render, migrate, lint and seed. CE conformance measures CLI commands, not Go APIs. |
| Schema visualization (ERD diagrams) | ✅ | ❌ | ✅ | Mermaid, DOT or SVG ERD from Go annotations only; SVG shells out to Graphviz dot. Atlas ERD lives in the hosted service (any plan per its pricing page); the CE binary rejects `--web`. |
| Statement observer and validator hooks (Go API) | ✅ | ➖ | ➖ | migrator.WithStatementObserver runs a read-only callback per executed statement; WithStatementValidator gates all statements pre-execution; both compose with StatementInterceptor. |
| testkit companion module for database tests | ✅ | ➖ | ➖ | Separate module go.5x5.cz/ptah/testkit wraps testcontainers-go for tests needing real databases; versions independently and stays out of the main module graph. |

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
| Artifact integrity check (`--verify-sum`) | 🟡 | ➖ | ❌ | On migrations push (local, pre-upload) and up/down/status (pulled). A sum checks a directory against the sum stored beside it, so an `oci://` tag source proves internal consistency only; pin a digest. |
| Declarative reference data | ✅ | ❌ | ✅ | //ptah:schema:data rows diffed by key into a reversible data migration. Atlas lists declarative data management as a Pro feature. |
| Digest pinning and write-once version tags | ✅ | ➖ | ❌ | Pushing to an @sha256 reference is refused; `--version` is write-once and a conflict exits 2. The reference tag, `--tag` values and latest all move. |
| Environment-scoped SQL seed runner | ✅ | ❌ | ❌ | NNN_desc.env.sql files recorded in schema_seeds with protected-env gates. No seed verb in the CE inventory or the cited Pro list. |
| oci:// as a `--schema-file` desired-state source | ✅ | ❌ | ✅ | Accepted by schema render, export, inspect, compare, drift, plan, apply and push, plus migrations plan and generate. All ten expose `--plain-http`, and a walk of the command tree gates the pairing. |
| Referrer attachments: lint, plan, deployment reports | ✅ | ❌ | ❌ | lint `--attach`, migrations plan `--attach` and up attach reports to an exact digest, and `oci fetch` returns the payload behind a descriptor rather than the descriptor alone. |
| Registry-backed distribution: `oci://` vs `atlas://` | ✅ | ❌ | ✅ | `atlas://` functions — publish, pull, digest-pin, run migrations directly, schemas via `--schema-file` — over any OCI registry, no account. See [OCI registry artifacts](../../operate/oci-registry/). |

## Atlas Registry and Cloud

These rows are the services Atlas hosts on top of its registry — approvals,
reporting, monitoring, the `atlas://` scheme itself. They concern the hosted
service, not artifact storage; the storage function is covered under
[Data and distribution](#data-and-distribution).

| Capability | Ptah | CE | Pro | Difference |
| --- | :-: | :-: | :-: | --- |
| `atlas://` vendor protocol | ❌ | ❌ | ✅ | The scheme is Cloud-bound and rejected with a named error; every function behind it is available natively over `oci://`. The compat binary mirrors the Atlas surface, which has no `oci://`. |
| `migrate push` and `schema push` | ❌ | ❌ | ✅ | ptah-compat boundary stubs report that the command is not implemented and exit 1. The native equivalents are `ptah schema push` and `ptah migrations push`. |
| `schema plan --edit` and `--name-format` | ✅ | ❌ | ✅ | `--edit` preserves comments and re-derives dialect-aware severity; `--name-format` uses Atlas-shaped Base64 .FromHash/.ToHash values. |
| `schema plan --format` and `--directive` | ❌ | ❌ | ✅ | Both fail loudly, by design. `--format` is implemented on the eight compat verbs whose CE counterpart works and whose payload can therefore be measured; `schema plan` is the one verb where CE aborts. |
| `schema plan --push`, `--pending`, `--repo` | ❌ | ❌ | ✅ | `--push`, `--pending` and `--repo` stay recorded waivers: they address the Atlas Registry, and Ptah's plan workflow saves local files instead. |
| `schema plan --skip-lint` | ✅ | ❌ | ✅ | Accepted, and does nothing: `schema plan` runs no lint step, so there is nothing to skip. A Pro pipeline passing it keeps working; no check is loosened. |
| `schema plan lint` | ✅ | ❌ | ✅ | Implemented: the plan is verified against the transition, then Ptah's lint rules report on its SQL. Findings do not change the exit code; an opt-in variable makes an error-severity finding exit 1. |
| `schema plan new` and `schema plan validate` | ✅ | ❌ | ✅ | Implemented. Flag sets match standard Atlas v1.3.0 help; runtime parity remains unverified. Successful Ptah runs keep stderr free of development notes. |
| `schema plan test` | ❌ | ❌ | ✅ | Local by its flag set (it takes no `--url`) but deferred: it consumes `test "plan"` cases in `.test.hcl` files, and nothing in Ptah parses that format yet. |
| `schema plan` registry sub-verbs (approve, list, pull, push, rm) | ❌ | ❌ | ✅ | These five arbitrate plan state in a remote registry; Ptah's local plan-file workflow replaces the function rather than the service. |
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
  goes further than the inventory: it runs the pinned CE binary
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
