---
title: Comparison
description: Ptah native commands, Atlas-compatible commands, feature parity evidence, Pro analyzer coverage, and the tracked gap register.
---

This page carries the per-area detail. For a one-line status per capability,
start at the [Feature matrix](../feature-matrix/) and come back here for the
area that matters to you.

## Product positioning

Ptah is an independent MIT-licensed implementation. It does not use Atlas source
code; see [License boundary](../license-boundary/) for the repository
and test-asset boundary.

Ptah is also usable as importable Go packages, not only as a CLI. See
[Reusable components](../../extend/components/) for the stable embedder surface.

Atlas has both open and commercial/cloud feature sets. The current Atlas
[feature availability](https://atlasgo.io/features) page lists database
inspection, schema diffing, versioned migrations, and declarative migrations as
open CLI features. The same page lists the migration linting CLI feature as Pro
while also listing a basic Open lint-rule set. Checkpoints, visualization,
interactive migrations, testing, deployment rollout, database security as code,
and declarative data management are listed as Pro features.

## What parity means, and what it does not

Ptah aims to be a drop-in replacement, which means two commitments rather than
one.

Ptah does not accept what Atlas refuses. A configuration or invocation Atlas
rejects is rejected here too, because accepting it would let a mistake pass
silently and fail later. Where Ptah has not implemented something Atlas
enforces, it refuses with a message rather than accepting and ignoring it.

Ptah also does not reproduce defects for the sake of being identical. Where a
measured behavior loses something the author asked for, Ptah does the better
thing and documents the difference.

One concrete case: a migration carrying `-- atlas:txmode none` directly above
its statement, with no blank line between them. That directive marks a statement
that must run outside a transaction, such as `CREATE INDEX CONCURRENTLY`. In
that shape Atlas drops the directive, so the statement runs inside a transaction
and the migration fails partway through. Ptah honors it.

Differences of this kind are listed in the [gap register](#gap-register) with
the measurement behind them, so you can see which way each one goes.

`ptah-compat` therefore has two explicit policy profiles. The default profile
retains every implemented Atlas Pro-like and best-effort capability on the
drop-in surface. `PTAH_ATLAS_STRICT_COMPAT=1` is an oracle profile: it exposes
the pinned CE command and flag inventory and rejects extended authored or live
schema content before output or mutation. The strict profile is for CE
conformance testing, not a replacement for the default migration surface. It
still preserves the deliberate correctness differences listed under
[Retained divergences](../retained-divergences/).

The authored-content boundary makes strict schema workflows refuse YAML sources
and a `schema apply` lint policy that the CE path cannot enforce. Commands that
execute, convert, or replay migration bodies refuse Atlas txtar, Ptah
directives, and SQL templates; checksum-only reads preserve those bytes. The
default profile continues to support the extensions instead of silently
dropping their semantics.

### Capability parity, not interface parity

Ptah's Atlas compatibility layer does not define a separate feature set.

Capabilities implemented for Atlas compatibility are also available through
Ptah's native workflows when they are generally useful database-schema or
migration capabilities.

The interfaces may differ: `ptah-compat` preserves Atlas-shaped commands and
compatibility contracts, while `ptah` uses Ptah-native commands and
configuration.

Atlas-specific adapters and compatibility representations — for example
`atlas://` resolution, Atlas file and config codecs, revision-history
compatibility, or Atlas-specific CLI and output behavior — are not duplicated
in the native interface unless they have independent Ptah value.

So the promise is about capabilities, not about command lines. The native
binary accepts no Atlas CLI aliases, and the two binaries are not
command-for-command equivalent: Atlas command spellings live only in
`ptah-compat`. The [command parity table](#command-parity) below shows which
native verb answers each Atlas one, and the differences in what each verb
records or refuses are named in the sections that follow it.

## Command parity

| Task | Native Ptah | `ptah-compat` | Atlas OSS |
| --- | --- | --- | --- |
| Apply migrations | `ptah migrations up` | `ptah-compat migrate apply` | `atlas migrate apply` |
| Roll back migrations | `ptah migrations down` | `ptah-compat migrate down` | `atlas migrate down` |
| Migration status | `ptah migrations status` | `ptah-compat migrate status` | `atlas migrate status` |
| Hash migrations | `ptah migrations hash` | `ptah-compat migrate hash` | `atlas migrate hash` |
| Validate migrations | `ptah migrations validate` | `ptah-compat migrate validate` | `atlas migrate validate` |
| Lint migrations | `ptah migrations lint` | `ptah-compat migrate lint` | Pro CLI feature, basic Open rule set [^lint] |
| Create an empty migration | `ptah migrations create` | `ptah-compat migrate new` | `atlas migrate new` |
| Set revision state | `ptah migrations set` | `ptah-compat migrate set` | `atlas migrate set` |
| Checkpoint / squash migrations | `ptah migrations checkpoint` | `ptah-compat migrate checkpoint` | Pro only |
| Inspect schema | `ptah db read` | `ptah-compat schema inspect` | `atlas schema inspect` |
| Diff schema | `ptah schema compare` | `ptah-compat schema diff` | `atlas schema diff` |
| Format schema files | `ptah schema fmt` | `ptah-compat schema fmt` | `atlas schema fmt` |
| Clean schema objects | `ptah db drop-all` | `ptah-compat schema clean` | `atlas schema clean` |
| Atlas CE community-version unsupported commands | Not native Ptah features | `ptah-compat migrate push`, `ptah-compat schema push`, `schema plan` registry sub-verbs | Registered, unsupported [^ce] |

[^lint]: Current Atlas docs list the migration linting CLI feature as Pro while
    the same feature availability page also lists a basic Open lint-rule set.

[^ce]: Atlas CE registers these command paths and reports the community-version
    unsupported boundary. `migrate test`, `schema test`, `migrate edit`,
    `migrate rebase`, `migrate rm`, and `schema plan` forward to or implement
    native Ptah behavior instead of aborting.

Some Atlas command paths are intentionally registered before complete runtime
behavior exists, and some accepted Atlas flags still fail explicitly rather than
being silently ignored. The gap register below links that work to concrete
tracking issues.

The `ptah-compat` column shows the separate drop-in binary that exposes the
Atlas-compatible command tree at process root; invocations are written as
`ptah-compat <command> ...`.

For a page-by-page crosswalk against the official Atlas documentation, see
[Atlas docs coverage](../docs-coverage/).

## Detailed product comparison

Each area below states what Ptah does, what Atlas OSS documents, what Atlas
Commercial or Cloud adds, and the evidence behind the claim. This table is the
index; the sections carry the detail.

| Area | Ptah | Atlas OSS | Atlas Pro / Cloud |
| --- | --- | --- | --- |
| [License and implementation](#license-and-implementation) | MIT, independent | Independent product | Proprietary additions |
| [Command compatibility](#command-compatibility) | Native tree plus `ptah-compat` | Open CLI surface | Not drop-in targets |
| [Schema inspection](#schema-inspection) | Native plus Atlas-compatible | Open | Pro drivers and filters |
| [Schema diff, apply, formatting, and cleanup](#schema-diff-apply-formatting-and-cleanup) | Native plus Atlas-compatible | Open | Registry plans and approvals |
| [Versioned migrations](#versioned-migrations) | Native plus Atlas-compatible | Open | Registry and deployment reporting |
| [Failed rollback state](#failed-rollback-state) | Recorded and recoverable | Absent verb | Not recorded |
| [Migration directory maintenance](#migration-directory-maintenance) | Native, free | Absent | Pro only |
| [Migration checkpoints](#migration-checkpoints) | Native, free | Absent | Pro only |
| [Diff and plan policy](#diff-and-plan-policy) | Native `ptah.yaml` `diff` block | Equivalent policy | Not gated |
| [Pre-migration checks](#pre-migration-checks) | Both spellings, local half | Absent | Pro, account-bound |
| [Testing framework](#testing-framework) | Native, free | Absent | Pro only |
| [Declarative reference data](#declarative-reference-data) | Native, free | Absent | Pro only |
| [Native migration import](#native-migration-import) | Native | Open | Not gated |
| [Migration diff dry run](#migration-diff-dry-run) | Supported | Hidden accepted flag | Not applicable |
| [Atlas CLI shorthand aliases](#atlas-cli-shorthand-aliases) | Supported | Open | Not applicable |
| [Atlas CE community-version unsupported commands](#atlas-ce-community-version-unsupported-commands) | Boundary stubs | Registered as unsupported | Broader workflows |
| [Migration linting](#migration-linting) | Native, plus a compatible path | Basic Open rule set | Pro analyzers and reports |
| [Cloud and registry features](#cloud-and-registry-features) | Bring-your-own OCI registry | Outside the drop-in target | Atlas Cloud services |
| [Supported databases](#supported-databases) | PostgreSQL, SQLite, MySQL, MariaDB, SQL Server subsets | Six Open drivers | Many Pro drivers |
| [HCL and config](#hcl-and-config) | Strict supported subsets | Open sources | Pro data sources |
| [Conformance status](#conformance-status) | Measured in a separate repository | The comparison target | Outside the OSS target |

### License and implementation

**Ptah.** MIT-licensed independent implementation. Ptah compatibility code is written in this repository and does not import or vendor Atlas source.

**Atlas OSS.** Atlas is an independent product. Ptah treats its public command names, flags, file formats, and observable behavior as compatibility inputs.

**Atlas Commercial / Cloud.** Same Atlas product family plus its commercial Pro and Cloud capabilities.

**Evidence.** [License boundary](../license-boundary/), [Atlas feature availability](https://atlasgo.io/features)


### Command compatibility

**Ptah.** Native command tree plus Atlas-compatible paths under `ptah-compat <command> ...`. Some paths and flags are still tracked gaps.

**Atlas OSS.** Open CLI feature surface includes inspection, schema diffing, versioned migrations, and declarative migrations.

**Atlas Commercial / Cloud.** Pro and Cloud add capabilities that are not OSS drop-in targets, such as rollout, testing, and registry-backed workflows.

**Evidence.** [Atlas CLI reference](https://atlasgo.io/cli-reference), [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#514`](https://github.com/stokaro/ptah/issues/514)


### Schema inspection

**Ptah.** `ptah db read` remains Ptah's native schema-read command.

`ptah-compat schema inspect` inspects supported live databases, local schema
files, migration directories, and `env://` references, and emits Atlas-shaped
output without Ptah status banners:

- HCL by default
- SQL with `--format '{{ sql . }}'`
- JSON with `--format '{{ json . }}'`
- the literal text `hcl`, `sql`, or `json`, without a line feed, when that exact bare value is the whole `--format` template
- the original literal bytes when surrounding whitespace wraps `hcl`, `sql`, or `json`
- custom Go-template output using `.MarshalHCL`, `hcl`, `sql`, `json`, `base64url`, and `mermaid`
- HCL/SQL split-write exports through `split` and `write` with the documented Atlas split strategies (per object by default with a `main.sql` `atlas:import` entry point for SQL, `split "schema"`, `split "type"`, optional file-extension argument)

Rendering builds one output plan and a single writer applies it with explicit
failures for duplicate paths, traversal, file/directory collisions, and
existing-directory destinations.

Non-database sources require `--dev-url` and are evaluated Atlas-style on the dev database (reset, materialize, introspect); a missing dev database fails with Atlas's `--dev-url cannot be empty` message.

The OSS `--exclude` flag filters inspected resources with Atlas-style globs and type selectors, including the Atlas-documented `*[type=extension].version` field selector with schema-qualified globs. Other field-level exclude selectors and type selectors on non-final pattern segments fail explicitly; exporter blocks remain a gap.

The pinned Atlas CE binary rejects `split`, `write`, and `hcl` template functions as non-community features, so Ptah's split-write exports are an open extension beyond the pinned CE binary.

#### `schema inspect --include`

`ptah-compat schema inspect --include` positively selects the top-level
resources inspection keeps, through the same engine as `schema apply` and
`schema diff`: schema universe, then include selection, then exclusion.

Atlas CE v1.2.0 does not register the flag at all — `atlas schema inspect -u
sqlite://app.db --include users` exits 1 with `Error: unknown flag:
--include`. Atlas registers it, and Ptah's behavior
diverges from it in two measured ways, both deliberate:

| Input | Atlas | Ptah |
| --- | --- | --- |
| `--include 't1'` | `t1` with its columns, plus `schema "main"` | same selection |
| `--include '*.t1'` | pattern is read at child depth: `t1` rendered without its columns and `t2` rendered as an empty shell, exit 0 | `*.t1` is the wildcard spelling of the qualified name `main.t1`, so `t1` is rendered whole and `t2` is dropped |
| `--include 'main.t1.*'` | `Error: too many parts in pattern: ["main" "main" "t1" "*"]`, exit 1 | rejected before any database is contacted: child resources ride along with their parent and cannot be selected on their own |

Ptah has no child-level include selection in either spelling, so it keeps
whole objects instead of emitting partial ones. It also refuses a selection
that drops a dependency of a selected object, where Atlas renders
the reference anyway — its `*.t1` output keeps `primary_key { columns =
[column.id] }` on a table whose `id` column the same output omits. The
Atlas-side rows are the recorded transcripts, so behavior beyond those inputs is not established here.

There is no `atlas.hcl` spelling of this selector. Atlas documents `exclude`
but no `include` attribute on the `env` block; CE accepts `include = [...]`
there only because it accepts any unknown env attribute. A run with
`not_a_real = [...]` is likewise accepted and prints the full schema. Ptah
accepts `env.include` under the same unknown-name rule, leaves it without
effect, and writes a location-aware warning to stderr.

**Atlas OSS.** `atlas schema inspect` is documented as an open CLI feature for inspecting a database schema with HCL, SQL, JSON, template output forms, split/write file exports, and resource exclusion filters. The pinned Atlas CE flag surface does not register `schema inspect --include`.

**Atlas Commercial / Cloud.** Commercial database drivers and Pro-only include filters broaden the inspect surface.

**Evidence.** [Atlas CLI reference](https://atlasgo.io/cli-reference), [Capabilities](../../reference/capabilities/), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510)


### Schema diff, apply, formatting, and cleanup

**Ptah.** `ptah schema compare` covers Ptah's Go-annotations-to-live-DB comparison path.

`ptah-compat schema apply` now diffs a live database against local `file://`
`.hcl`, `.yaml`, `.yml`, or `.sql` desired schema files, prints planned SQL,
supports Atlas-style `--format` templates over planned changes, and applies
after interactive confirmation or explicit `--auto-approve`.

- `--dry-run` prints without applying
- `--tx-mode` supports `file`, `all`, and `none` for the generated plan
- `--exclude` plus disabled `schema.mode` resources filter matching objects out of both the current and desired local-file comparison before planning

With `--env` it reads `env.url`, `env.src`, `env.schema.src`, `env.dev`,
`env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff`
policy from `atlas.hcl`, including local variable defaults, locals, `getenv`,
`file`, `fileset`, `format`, `jsonencode`, and `data.hcl_schema.<name>.url`
references.

`schema apply --edit` opens the planned SQL in `$VISUAL`/`$EDITOR` before approval so the edited SQL is what gets applied; `schema apply --plan file://<path>` executes a pre-approved local plan file saved by `schema plan`, and `schema apply --lock-timeout` bounds waiting for the session advisory lock that serializes concurrent applies against one target. Strict CE mode preflights an explicit `--schema` target scope before that lock or any desired-source replay; without one, PostgreSQL-family targets inventory the user realm because desired replay may extend the URL scope. The authoritative inspection and planning remain inside the lock, which is released on every exit path; dialects without advisory locks proceed unlocked with a stderr note.

`ptah-compat schema diff` implements local schema-file diffs, reads `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and supported `diff` policy from `atlas.hcl`, prints migration SQL, supports Atlas-style `--format` templates with `sql` and `.MarshalSQL`, and applies the same Atlas-style filters to local `--from` and `--to` inputs.

Supported `diff` policy includes `skip.drop_table` and PostgreSQL `concurrent_index.create` where the command can run without a surrounding transaction.

Both declarative paths accept desired-state URL sources through one typed resolver: directly connectable database URLs (live schema introspected), migration directories (`file://` directories containing `atlas.sum`, replayed on a required `--dev-url` dev database), and `env://` references (`src`, `schema.src`, `url`, `dev`, `migration.dir`) resolved through the evaluated `atlas.hcl` env with variables and relative paths honored; one source kind per flag, deterministic conflict errors, and pre-target failures for unsupported schemes such as `atlas://`.

Before a non-dry-run apply, `--dev-url` rehearses the exact ordered plan on the dev database (reset, current target schema recreated, planned or edited statements executed under the apply's transaction mode); a failed rehearsal refuses the apply with the target unchanged.

`schema apply --schema/--include` and `schema diff --schema/--include` positively scope both comparison sides: `--schema` names define the schema universe, `--include` selects top-level resources with Atlas-style glob selectors and `[type=...]` filters (repeated values union deterministically), `--exclude` plus disabled `schema.mode` values subtract afterward, cross-scope dependencies refuse the plan with explicit diagnostics, and an empty selection reports a synced schema.

Atlas CE aborts `--include` as a non-community feature, so Ptah's implementation is an open extension beyond the pinned CE binary. `ptah-compat schema fmt` formats local `.hcl` files with HCL canonical layout.

`ptah-compat schema clean` plans supported cleanup objects from the live database, supports `--dry-run`, preserves destructive confirmation unless `--auto-approve` is explicit, reads `env.url` and `format.schema.clean` from `atlas.hcl`, and supports Atlas-style `--format` templates such as `{{ json . }}` over `.Env`, `.DryRun`, `.Applied`, `.Objects`, and `.Changes`.

Cleanup report changes cover the object kinds the target dialect's cleanup really destroys, so the report is not narrower than the apply: foreign keys, tables, views, materialized views, enum/domain/composite/range types and functions on the PostgreSQL family (plus standalone sequences on PostgreSQL itself); foreign keys, tables, views, stored functions and procedures, events and MariaDB sequences on MySQL and MariaDB; tables and views on SQLite; foreign keys and tables on SQL Server; base tables on ClickHouse. Objects that vanish as collateral of a listed drop — indexes, triggers, non-foreign-key constraints, RLS policies, comments — are not listed separately. Scoped PostgreSQL cleanup uses catalog dependency depth and one transaction, so selected same-kind dependents run first and a later `RESTRICT` refusal rolls back the complete narrowed plan.

**Atlas OSS.** Atlas OSS documents schema diffing, declarative migrations, HCL formatting, and schema cleanup as open CLI features.

**Atlas Commercial / Cloud.** Cloud/Pro workflows add registry-backed plans, approvals, and deployment tracking.

**Evidence.** [Atlas feature availability](https://atlasgo.io/features), [pre-planning schema migrations](https://atlasgo.io/declarative/plan), [Atlas CLI reference](https://atlasgo.io/cli-reference), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510)


### Versioned migrations

**Ptah.** `ptah migrations up`, `down`, `status`, `hash`, `validate`, `create`, `repair`, and Atlas-compatible counterparts cover local migration workflows.

`ptah-compat migrate apply` executes Atlas-format migration directories with Atlas revision-table metadata by default, reads `env.url`, `migration`, and `format.migrate.apply` from `atlas.hcl`, and supports positional `amount`, `--baseline`, `--allow-dirty`, `--tx-mode`, `--exec-order`, `--revisions-schema`, `--lock-timeout`, `--dry-run`, and Go-template `--format` output over a Ptah apply result that mirrors Atlas's public apply-template fields.

External `golang-migrate`, `goose`, `flyway`, `liquibase`, and `dbmate` formats are read and converted in memory to Atlas single-file, up-only migrations and applied directly, sharing format parsers and up/down semantics with `ptah-compat migrate import` ([`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742)); native Atlas directories preserve `R`/`<number>R` repeatable migration tokens and execute them once. Conventional Liquibase import additionally splits changesets into numeric Atlas files, while direct apply retains its numbered-file requirement and source-file boundary. Converted Flyway repeatables are represented as one-time versioned migrations. Unknown formats still fail before the target database is opened.

`ptah-compat migrate validate` verifies `atlas.sum`, emits Atlas-compatible exit-1 checksum diagnostics across stdout and stderr, and, with `--dev-url`, cleans the dev database and replays the migration directory to validate SQL execution. `ptah-compat migrate apply` runs the same `atlas.sum` verification before applying anything from a hashed directory and refuses tampered directories with the identical checksum-mismatch output, matching official Atlas apply-time enforcement ([`stokaro/ptah#955`](https://github.com/stokaro/ptah/issues/955)).

`ptah-compat migrate set [version]` implements Atlas's revision-boundary transition: it removes revision rows above the target, preserves existing rows at or below it, and inserts missing rows through the target as manually set, while reading `env.url`, `migration.dir`, and `migration.revisions_schema` from `atlas.hcl` and keeping explicit `--url`, `--dir`, and `--revisions-schema` precedence.

`ptah-compat migrate diff` validates an existing `atlas.sum`:

- validates an existing `atlas.sum`;
- replays a local Atlas migration directory on a directly connectable dev database;
- diffs it against local schema files, one directly connectable desired database URL, one local Atlas migration directory, or one env:// reference;
- rejects desired/dev database aliases;
- writes new Atlas-style migration files;
- updates `atlas.sum` only after every file was written (a failed write rolls the whole generation back);
- reads `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and supported `diff` policy from `atlas.hcl` including `diff.concurrent_index.create` (new indexes become `CREATE INDEX CONCURRENTLY`, their files carry the Atlas `-- atlas:txmode none` directive, and mixed plans split into a transactional file followed by a concurrent-index file);
- supports `--lock-timeout` for Ptah's local migration-directory lock;
- supports Atlas-style `--format` templates with `sql` and `.MarshalSQL` for generated migration SQL;
- supports `--edit` to open the generated migrations in `$VISUAL`/`$EDITOR` before `atlas.sum` is finalized;
- implements `--qualifier` with Atlas's single-schema custom-qualifier semantics on PostgreSQL, CockroachDB, YugabyteDB, MySQL, and MariaDB dev databases (invalid values, unsupported dialects, multi-schema plans, and not-yet-qualifiable statement kinds fail before any file or checksum is written);
- uses `--schema` to scope the desired schema plus the replayed dev database state.

Docker dev databases remain a gap.

`ptah-compat migrate new --edit` opens the created migration file in the same editor and refreshes `atlas.sum`. `ptah-compat migrate edit`, `rebase`, and `rm` forward to the native `ptah migrations edit`, `rebase`, and `rm` directory-maintenance commands with Atlas-shaped `--dir`/`--dir-format` flags and `{name | version}` positionals.

`ptah-compat migrate down` executes Ptah's pre-planned down-file rollback path, maps Atlas-compatible flags, and renders Atlas Go-template reports with `--format`. Dynamic down planning remains a recorded gap and fails explicitly.

`ptah-compat migrate import` imports local `file://` directories from Atlas-supported formats into a separate Atlas single-file directory and writes `atlas.sum`. Flyway repeatable migrations are converted to one-time versioned files instead of Atlas `R`-suffixed files. If a Liquibase directory contains a conventional formatted-SQL name, the complete covered SQL set becomes one globally numbered changeset stream; malformed or headerless members refuse the import before destination creation.

**Atlas OSS.** Atlas OSS includes versioned migrations and documents `atlas migrate apply`, `atlas migrate diff`, `atlas migrate down`, and `atlas migrate import` for applying, generating, reverting, and importing local migration directories.

**Atlas Commercial / Cloud.** Atlas Registry and deployment reporting add remote migration-directory storage, tagging, history, and environment promotion workflows. Pro adds approval workflows for protected down plans.

**Evidence.** [Atlas feature availability](https://atlasgo.io/features), [Atlas migration apply](https://atlasgo.io/versioned/apply), [Atlas down migrations](https://atlasgo.io/versioned/down), [Import from other migration tools](https://atlasgo.io/versioned/import), [Atlas Cloud deployment docs](https://atlasgo.io/cloud/deployment), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742), [`stokaro/ptah#842`](https://github.com/stokaro/ptah/issues/842).


### Failed rollback state

**Ptah.** `ptah migrations down` records a rollback that failed partway in both revision-table formats: the revision row is marked failed with `error=<message>`, `direction=down`, and `applied` set to the number of down statements that completed (`0` when the first one fails or the dialect rolled the body back), so status reports the dirty state, repair has a row to act on, and a later up refuses to stack work on the unfinished rollback. Repair follows the recorded direction: `--resume-from` runs the remaining *down* statements and removes the revision once the rollback finishes, and a rollback that already committed a statement is refused rather than recorded applied unless `--force` says the schema was restored.

`ptah-compat migrate down` retains the Atlas revision-table schema but deliberately does not reproduce Atlas's hidden failed-down state. Ptah uses the existing `operator_version` field to mark rollback direction and the existing progress/error fields to make a partial rollback recoverable. A successful down still deletes the row. An Atlas reader can inspect the same table, but may report a Ptah-recorded failed rollback differently from one Atlas itself hid. See [Roll back migrations](../../versioned/rollback/).

**Atlas OSS.** The pinned Atlas CE binary registers `migrate down` as a community-abort stub, so the capability is unreachable there.

**Atlas Commercial / Cloud.** Atlas runs the verb and records nothing when a down fails (measured): after a down whose second statement fails, the body is rolled back and the revision row still reads `applied=2, total=2, error=''`, `atlas migrate status` reports the version applied, and a retry after repairing the down file succeeds and deletes the row.

**Evidence.** [Atlas down migrations](https://atlasgo.io/versioned/down), [`stokaro/ptah#957`](https://github.com/stokaro/ptah/issues/957)


### Migration directory maintenance

**Ptah.** `ptah migrations edit`, `rebase`, and `rm` change a migration's SQL, re-timestamp a migration to the end of history, or delete a migration, and each atomically rewrites `ptah.sum` / `atlas.sum` so `ptah migrations validate` passes immediately.

They refuse to modify a migration that is already applied in the database given by `--db-url` unless `--force` is passed; without `--db-url` they warn that applied state was not verified. Both `ptah.sum` and `atlas.sum` directory formats round-trip through each operation.

The Atlas-compatible `ptah-compat migrate edit`, `rebase`, and `rm` verbs forward to these commands with Atlas-shaped `--dir`/`--dir-format` flags and `{name | version}` positionals; rebase forwards one migration per run and rejects multiple values and version ranges loudly.

**Atlas OSS.** Atlas OSS has no directory-maintenance commands.

**Atlas Commercial / Cloud.** Atlas lists `migrate edit`, `migrate rebase`, and `migrate rm` as Pro-only directory-maintenance commands, requiring the closed-source binary and an Atlas account. Ptah provides them natively as MIT, local, no-account capabilities, including the drop-in `ptah-compat` verbs.

**Evidence.** [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#662`](https://github.com/stokaro/ptah/issues/662), [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758)


### Migration checkpoints

**Ptah.** `ptah migrations checkpoint` squashes a directory's history into a cumulative-schema checkpoint generated by replaying the directory on a `--shadow-db`. By default it writes the reversible pair (`NNNNNNNNNN_name.checkpoint.up.sql` / `.checkpoint.down.sql`) and rewrites `ptah.sum`; under `--dir-format atlas` it writes Atlas's single up-only `<version>_name.sql` carrying `-- atlas:checkpoint` and rewrites `atlas.sum`. `ptah-compat migrate checkpoint` defaults to the Atlas convention.

A fresh database bootstraps from the newest checkpoint and skips the squashed pre-checkpoint history; an already-migrated database ignores the checkpoint and applies only genuinely pending migrations. `ptah-compat migrate checkpoint` exposes the same workflow on the Atlas-compatible surface.

The read side also honors Atlas's own `-- atlas:checkpoint` file directive ([`stokaro/ptah#954`](https://github.com/stokaro/ptah/issues/954)): checkpoint directories produced by Atlas Pro apply with the measured Atlas semantics through both `ptah-compat migrate apply` and native `ptah migrations up` — fresh databases execute only the latest checkpoint (single `type=2` revision row), and databases with pre-checkpoint history skip the checkpoint silently with a clean status.

**Atlas OSS.** Atlas OSS has no checkpoint command.

**Atlas Commercial / Cloud.** Atlas lists `migrate checkpoint` as a Pro-only capability requiring the closed-source binary and an Atlas account. Ptah provides it natively as an MIT, local, no-account, embeddable capability.

**Evidence.** [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#660`](https://github.com/stokaro/ptah/issues/660)


### Diff and plan policy

**Ptah.** `ptah migrations generate` reads a native `ptah.yaml` `diff` block. `diff.skip` omits destructive change kinds (`drop_table`, `drop_column`, `drop_index`, `drop_enum`) from the generated migration, writing a `-- SKIP:` comment in their place; skipping `drop_table` also omits the dependent index/constraint/trigger/policy/grant removals a kept table must retain.

`diff.concurrent_index` requests `CREATE INDEX CONCURRENTLY` for new indexes (PostgreSQL, capability-gated). A skipped change is never emitted, so it composes with rather than replaces the coarse `--check-destructive` gate. Skip is currently honored by the PostgreSQL-family planner.

**Atlas OSS.** Atlas exposes an equivalent declarative `diff { skip { ... } concurrent_index { ... } }` policy in its open-source core.

**Atlas Commercial / Cloud.** Not a Pro/Cloud feature; this is Atlas-OSS parity, not a proprietary-feature replacement.

**Evidence.** [Configuration](../../reference/configuration/), [`stokaro/ptah#668`](https://github.com/stokaro/ptah/issues/668)


### Pre-migration checks

**Ptah.** A `-- +ptah check name=... assert="<sql predicate>" on_fail=abort` directive runs before a migration's statements. Assertions are top-level `SELECT` queries that must return exactly one column and one row. Ptah uses a dedicated physical session and discards it afterward; transaction-capable drivers roll back, while ClickHouse uses the disposable session directly because its driver does not implement transactions. PostgreSQL-family and MySQL-family drivers also request database-enforced read-only mode. A mutating statement, invalid result shape, falsy result, or query error aborts with a `CheckFailedError` and nothing applied. The check precedes the migration body and does not serialize against concurrent writers; checks are rejected under `--tx-mode all` on a real apply.

Atlas's own artifact is honored too: `checks.sql` and ordered `checks/*.sql` sections in an `-- atlas:txtar` migration are enforced through the same engine as pre-migration gates, rather than executed as plain SQL or discarded. Every assertion in a file must pass unless its file has `-- atlas:assert oneof`, which requires at least one and fails closed when the file is empty. Dialect-aware splitting preserves PostgreSQL escape strings and MySQL/MariaDB semantic comments. Executable-comment bodies and version guards are validated as effective SQL, so they cannot hide additional statements. A failed check records no revision row, so the retry after fixing the data needs no bypass flag — which matters on `ptah-compat migrate apply`, where Atlas parity means no `--skip-checks` flag; its emergency bypass is the `PTAH_SKIP_CHECKS` environment variable.

Multiple ordered checks per migration; `ptah migrations up --skip-checks` is an emergency bypass, spelled `PTAH_SKIP_CHECKS=1` on `ptah-compat migrate apply` because Atlas registers no such flag there. This is the local, offline half of Atlas's pre-migration checks.

In a dry run, a check is evaluated only for the first migration executed in the run — the one position whose observed state is the state a real apply would give it. Later migrations' assertions are parsed and statically validated but not evaluated, and the run names them on stderr. See [Checks in a dry run](../../versioned/integrity-and-safety/#checks-in-a-dry-run).

**Retained divergence on dry runs.** The community binary implements no check
semantics, so it flattens the archive and runs `checks.sql` as an ordinary
migration statement; in a dry run it executes no SQL at all and exits `0` for
every checked directory.

`ptah-compat` deliberately exits `1` where the guard's verdict is knowable and
negative:

- an assertion that is malformed or is not a read-only `SELECT`, decidable from
  the text alone
- a failing assertion on the first migration executed in the run, decidable
  against the live database and confirmed by the real apply failing the same way
- a checked directory under `--tx-mode all`, which the real apply refuses
  outright

Matching the community binary on those inputs would make the preview report
success for a run that cannot succeed.

Where the old failure was an artifact of the preview rather than a finding — a
later migration's guard asking about state the dry run refused to create —
`ptah-compat` now exits `0`, which both matches the community binary and matches
what applying the directory actually does.

**Atlas OSS.** Atlas keeps pre-migration checks in its proprietary Pro build (free-with-login, then paid), requiring the closed-source binary and an Atlas account; not embeddable.

**Atlas Commercial / Cloud.** The reviewer-approval-policy half is gated on Atlas Cloud and is intentionally out of scope.

**Evidence.** [`stokaro/ptah#661`](https://github.com/stokaro/ptah/issues/661)


### Testing framework

**Ptah.** `ptah migrations test` and `ptah schema test` run declarative YAML test cases against a throwaway database. Migration cases support `migrate_to`, `apply_schema`, `seed`, `exec`, and `assert`; schema cases receive their Go-annotation desired schema before the steps and support `apply_schema` to recheck live state and repair supported drift, plus `seed`, `exec`, and `assert`.

Assertions cover `row_count`, `scalar`, and `error_contains`. Each case runs against its own fresh ephemeral SQLite database by default, or a shared `--db-url` throwaway. The runner is exported as `migration/dbtest` for embedding.

**Atlas OSS.** Atlas OSS has no schema/migration testing command.

**Atlas Commercial / Cloud.** Atlas lists the testing framework (`migrate test`, `schema test`) as a Pro-only capability requiring the closed-source binary and an Atlas account. Ptah provides both natively as MIT, local, no-account, embeddable capabilities.

**Evidence.** [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#659`](https://github.com/stokaro/ptah/issues/659)


### Declarative reference data

**Ptah.** `ptah migrations data` diffs declarative reference/lookup rows — declared with a `//ptah:schema:data table=... key=... file=...` annotation and a YAML rows file — against a live table by key and writes a reversible data migration (`INSERT`/`UPDATE`/`DELETE`) whose `down` is the exact inverse, folded into `ptah.sum`. Values render as dialect-correct, safely-escaped SQL literals.

It reconciles a *desired row state* rather than replaying one-off SQL, so it is distinct from the imperative `ptah seed`.

**Atlas OSS.** Atlas OSS has no declarative reference-data or data-migration-generation command; `atlas migrate diff` generates schema DDL, not row-level data reconciliation.

**Atlas Commercial / Cloud.** Atlas keeps declarative data management and data-migration generation in its proprietary Pro build, requiring the closed-source binary and an `atlas login` account, and not embeddable. Ptah provides it natively as an MIT, local, no-account, embeddable capability.

**Evidence.** [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#663`](https://github.com/stokaro/ptah/issues/663)


### Native migration import

**Ptah.** `ptah migrations import` converts another versioned-migration tool's directory into Ptah's native `NNNNNNNNNN_name.up.sql`/`.down.sql` layout, preserving version order and rewriting `ptah.sum`, so the result passes `ptah migrations validate` immediately.

Supports golang-migrate, Goose, Flyway, and Liquibase formatted-SQL changelogs (auto-detected or `--from`), with a placeholder down for missing rollbacks, a no-overwrite guard, and `--dry-run`; Liquibase XML/YAML/JSON changelogs are rejected with a message. Distinct from the Atlas-compat `ptah-compat migrate import` (which emits Atlas format).

**Atlas OSS.** Atlas OSS documents `atlas migrate import` for converting Flyway/Liquibase/Goose/golang-migrate directories into Atlas format.

**Atlas Commercial / Cloud.** Not a Pro/Cloud feature; this is Atlas-OSS parity.

**Evidence.** [Importing migrations](https://atlasgo.io/versioned/import), [`stokaro/ptah#667`](https://github.com/stokaro/ptah/issues/667)


### Migration diff dry run

**Ptah.** `ptah-compat migrate diff --dry-run` accepts Atlas OSS's hidden dry-run flag and prints generated migration SQL without writing a migration file or updating `atlas.sum`. It still validates existing checksums, replays the local migration directory on `--dev-url`, and uses the dev database as scratch space like the normal diff path.

**Atlas OSS.** Atlas OSS registers `migrate diff --dry-run` as a hidden accepted flag.

**Atlas Commercial / Cloud.** Not applicable to the OSS drop-in target.

**Evidence.** [`stokaro/ptah#618`](https://github.com/stokaro/ptah/issues/618)


### Atlas CLI shorthand aliases

**Ptah.** Ptah accepts Atlas OSS shorthand aliases on the compatible command tree: `-u` for `--url`, `-c` for `--config`, `-s` for `--schema` where Atlas registers schema selection, `schema diff -f` for `--from`, and the hidden deprecated `schema apply --file/-f` alias for local HCL or SQL paths.

**Atlas OSS.** Atlas OSS exposes these shorthand aliases in its CLI flag set, including hidden `schema apply --file/-f` compatibility.

**Atlas Commercial / Cloud.** Not applicable to the OSS drop-in target.

**Evidence.** [`stokaro/ptah#621`](https://github.com/stokaro/ptah/issues/621)


### Atlas CE community-version unsupported commands

**Ptah.** Ptah registers Atlas-shaped boundary stubs for `migrate push`, `schema push`, and the `schema plan` sub-verbs `approve`, `lint`, `list`, `pull`, `push`, `rm` and `test` in the `ptah-compat` binary. `schema plan new` and `schema plan validate` are no longer stubs — they are implemented, so neither the exit-0 help nor the exit-1 execution described below applies to them. Ptah-owned help reports that the command is not implemented and exits 0; direct execution reports the same status and exits 1.

These are compatibility boundaries, not implemented Ptah features. `migrate test`, `schema test`, `migrate edit`, `migrate rebase`, `migrate rm`, and `schema plan` now forward to or implement native Ptah behavior instead of reproducing the boundary.

**Atlas OSS.** Atlas CE registers all of these paths and reports that they are not supported by the community version.

**Atlas Commercial / Cloud.** Non-community Atlas builds implement or expose broader push, testing, and planning workflows.

**Evidence.** [`stokaro/ptah#638`](https://github.com/stokaro/ptah/issues/638), [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758)


### Migration linting

**Ptah.** Ptah ships first-party migration linting and the `ptah-compat migrate lint` compatibility path.

`--latest N` limits the run to the latest N migration revision keys; `--git-base` and `--git-dir` select migrations changed against a Git base branch; Atlas `R` and `<number>R` repeatable files are selected by those string tokens rather than by numeric projection. `--dev-url` infers the lint dialect and treats directly connectable dev databases as scratch databases by cleaning and replaying migrations to validate SQL execution.

Atlas Go-template `--format` output is supported over `.Env`, `.Steps`, and `.Files`, including `{{ json .Files }}`. `atlas.hcl` can configure the lint changeset selectors, `format.migrate.lint`, and the supported analyzer severity policy for `destructive`, `concurrent_index`, `data_depend`, `incompatible`, and `nestedtx` where those analyzers map to Ptah rule families.

Atlas check-level policy, custom rules, analyzer force/allow-list options, Docker dev databases, and Atlas web reports remain gaps. See [Atlas Pro analyzer coverage](#atlas-pro-analyzer-coverage) for the code-by-code audit of Pro-marked analyzer checks.

**Atlas OSS.** Current Atlas docs mark the official migration linting CLI feature as Pro while the feature availability page also lists a basic Open lint-rule set. Atlas documents latest, Git-base, dev-database changeset linting, analyzer blocks, custom policy rules, and Go-template output.

**Atlas Commercial / Cloud.** Pro migration linting includes Atlas analyzers, policy workflows, enforced checks, and browser reports.

**Evidence.** [Atlas feature availability](https://atlasgo.io/features), [Atlas migration linting docs](https://atlasgo.io/versioned/lint), [Atlas migration analyzers](https://atlasgo.io/lint/analyzers), [`stokaro/ptah#582`](https://github.com/stokaro/ptah/issues/582), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510)


### Cloud and registry features

**Ptah.** Ptah has no Atlas Cloud dependency and does not implement Atlas Registry or `atlas://`.

It instead provides a native, bring-your-own [OCI registry workflow](../../operate/oci-registry/) for migration and canonical desired-schema artifacts, direct native migration/schema consumption, digest pinning, Docker credential-store authentication, and best-effort deployment-report referrers.

Native lint and plan commands can attach canonical reports to exact migration or schema digests, and `ptah oci referrers` lists direct descriptor metadata. Referrer payload download and consumption are not implemented.

**Atlas OSS.** Atlas Registry is outside the open drop-in target. Ptah's OCI workflow is an independent native capability; `ptah-compat migrate push` and `ptah-compat schema push` remain community-edition boundary stubs by decision, because the Atlas Registry protocol is proprietary and account-bound — the bring-your-own OCI registry verbs are the open equivalent.

**Atlas Commercial / Cloud.** Atlas Cloud provides its own registry, deployment reporting, cloud CLI commands, UI, Pro seats, pipelines, policy, and schema monitoring. Ptah does not reproduce those hosted services.

**Evidence.** [OCI registry artifacts](../../operate/oci-registry/), [Atlas Registry](https://atlasgo.io/cloud/features/registry), [Atlas Cloud deployment docs](https://atlasgo.io/cloud/deployment), [Atlas pricing](https://atlasgo.io/cloud/pricing)


### Supported databases

**Ptah.** Ptah has first-party support for PostgreSQL, SQLite, MySQL/MariaDB, SQL Server subsets, and capability-gated PostgreSQL-compatible or specialty targets.

**Atlas OSS.** Atlas docs list PostgreSQL, MySQL, MariaDB, SQLite, TiDB, and LibSQL as Open drivers.

**Atlas Commercial / Cloud.** Atlas Pro adds SQL Server, ClickHouse, Redshift, Oracle, Spanner, Snowflake, Databricks, CockroachDB, Azure HorizonDB, YugabyteDB, Aurora DSQL, Azure Fabric, and related drivers.

**Evidence.** [Capabilities](../../reference/capabilities/), [Atlas feature availability](https://atlasgo.io/features)


### HCL and config

**Ptah.** Ptah parses strict HCL schema and Atlas project config subsets.

Evaluated local env, schema, format, diff, and lint settings feed
Atlas-compatible commands. The evaluator supports typed variables (`string`,
`number`, `bool`, `list(string)`, `map(string)`), string/list `--var` overrides,
locals, `getenv`, `file`, `fileset`, `format`, `jsonencode`, `toset`, local and
external schema data sources, and migration-lint changeset defaults. Env
`for_each` expansion is supported by `migrate apply`.

Ptah also composes a desired-schema schema from multiple sources — several Go roots, or a mix of Go annotations, YAML, HCL, and SQL — via repeatable `--root-dir` and `--schema-file` flags on `ptah schema render`, `ptah schema compare`, `ptah migrations plan`, and `ptah migrations generate`, an open, local, no-account counterpart to Atlas's Pro `composite_schema` data source.

Structurally unsupported constructs fail explicitly. Names that Atlas CE
accepts without acting on are accepted for compatibility and reported as
having no effect.

**Atlas OSS.** Atlas OSS supports SQL and HCL schema sources. The community binary rejects the `data "external_schema"` project data source (measured 2026-08-01, Atlas CE v1.2.0: exit 1, `Error: data.external_schema is not supported by the community version of Atlas.`); Ptah evaluates it in the open build behind the external-schema opt-in.

**Atlas Commercial / Cloud.** Pro data sources and policy features include composite schema, blob directory, custom lint rules, and review workflows.

**Retained divergence on `file()` paths.** The community binary resolves a
`file()` or `fileset()` argument against the whole filesystem. Measured on the
pinned v1.3.0 build: `file("/etc/passwd")` and `file("../../../../etc/passwd")`
in an `atlas.hcl` both exit `0` with the file read, and the contents reach an
observable place — a database URL, an error message on standard error.

Ptah confines both functions to the directory holding `atlas.hcl` on both
binaries, and refuses an absolute path, parent traversal, or a symbolic link
leaving that directory by name. An `atlas.hcl` is repository-controlled and
evaluated before anything is applied, so matching here would turn config
authorship into an arbitrary-file read on whatever machine runs the migration.
The exit code is `1` either way today, so no working configuration changes; the
refusal now names its reason and points at `getenv()`. See
[`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042).

**Evidence.** [HCL schema](../../reference/hcl-schema/), [Atlas project config](../project-config/), [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#582`](https://github.com/stokaro/ptah/issues/582), [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511), [`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042)


### Conformance status

**Ptah.** Ptah uses the separate `ptah-atlas-conformance` repository as measured evidence against Atlas fixtures and behavior. The regression budget and full-conformance gates are intentionally separate: budget green means no unexpected regression, while full-conformance can remain red for known Atlas OSS gaps such as dynamic down planning.

**Atlas OSS.** Atlas fixtures and CLI behavior provide the comparison target for OSS-compatible behavior.

**Atlas Commercial / Cloud.** Commercial/cloud-only behavior is separated from the OSS drop-in target and tracked as documentation scope.

**Evidence.** [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md), [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md), [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167).

## Other tools

For evaluators comparing categories rather than commands:

- `golang-migrate` and `goose` are simpler versioned migration runners. Ptah
  adds schema IR, diffing, planning, rendering, linting, safety
  classification, capabilities, and Atlas-compatible flows.
- Prisma and Ent tie schema workflows to their ecosystems. Ptah is Go-first
  but is not an ORM.
- Skeema is a MySQL/MariaDB declarative schema tool. Ptah is multi-source,
  Go-embeddable, and multi-dialect.
- `sqlc` generates typed code from queries. Ptah focuses on schema and
  migration tooling, not query-code generation.

## Atlas Pro analyzer coverage

Atlas gates whole `migrate lint` analyzer families behind Atlas Pro. This table
audits every Pro-marked analyzer check code from the
[Atlas analyzers documentation](https://atlasgo.io/lint/analyzers) (fetched
July 28, 2026) against Ptah's native lint rules. Codes the Atlas docs mark as
non-Pro (`DS`, `MF`, `BC`, `PG110`, `MY101`–`MY123`, `LT`, `NM`, `SA`) are
outside this audit. Ptah's native code namespace intentionally differs from
Atlas's where meanings differ: Ptah `PG102` is enum-value-in-transaction, not
Atlas's drop-index-concurrently (Ptah `PG106`), and Ptah `DS103` is
column-type-changed, not Atlas's column-dropped.

| Atlas Pro code | Atlas meaning | Ptah native rule(s) | Status |
| --- | --- | --- | --- |
| CD101 | Foreign-key constraint dropped | `CD101` | Covered |
| CD102 | Check constraint dropped | `CD102` | Covered |
| CD103 | Primary-key constraint dropped | `CD103` | Covered |
| PG101 | Index created without `CONCURRENTLY` | `PG101` | Covered |
| PG102 | Index dropped without `CONCURRENTLY` | `PG106` | Covered |
| PG103 | Missing `atlas:txmode none` header for concurrent operation | `PG103` | Covered — Ptah flags `CONCURRENTLY` inside a transactional migration and honors both the `atlas:txmode none` header and its native no-transaction directive |
| PG104 | `PRIMARY KEY` creation acquires `ACCESS EXCLUSIVE` lock | `PG104` | Covered |
| PG105 | `UNIQUE` constraint creation acquires `ACCESS EXCLUSIVE` lock | `PG105` | Covered |
| PG301 | Column type change requires table and index rewrite | `DS103` | Partial — flagged as a data-safety risk, without PostgreSQL rewrite/lock analysis |
| PG302 | Volatile `DEFAULT` on added column rewrites the table | `PG302` | Covered |
| PG303 | `SET NOT NULL` scans existing rows | `PG303` | Covered |
| PG304 | `PRIMARY KEY` on nullable columns requires full scan | `PG104` | Partial — every `ADD PRIMARY KEY` is flagged; the nullable-column refinement needs schema knowledge statement-scoped lint does not have |
| PG305 | `CHECK` constraint requires full table scan | `PG305` | Covered |
| PG306 | `FOREIGN KEY` requires full scan and blocks writes | `PG306` | Covered |
| PG307 | Logging mode change rewrites the table | `PG307` | Covered |
| PG308 | Trigger creation acquires `SHARE ROW EXCLUSIVE` lock | `PG308` | Covered |
| PG309 | `STORED` generated column rewrites the table | `PG309` | Covered |
| PG310 | Identity column rewrites the table | `PG310` | Covered |
| PG311 | Access method change rewrites the table | `PG311` | Covered |
| MY130 | Column type change requires table copy | `MY101`, `DS103` | Partial — `MODIFY`/`CHANGE` is flagged as lock-heavy DDL and the type change as a data-safety risk, without a dedicated table-copy code |
| MY131 | Foreign key added blocks DML | `MY131` | Covered |
| MY132 | Primary key added requires table rebuild | `MY132` | Covered |
| MY133 | Primary key dropped without replacement requires table copy | `CD103` | Partial — the drop is flagged as an error-severity constraint deletion; the table-copy concern has no dedicated code |
| MY134 | `FULLTEXT` index added blocks DML | `MY134` | Covered |
| MY135 | `SPATIAL` index added blocks DML | `MY135` | Covered |
| MY136 | Character set change requires table rebuild | `MY101` | Partial — `CONVERT TO CHARACTER SET`/`CHARSET` is flagged as lock-heavy DDL; other charset-change spellings are not scanned |
| TX101 | Statements cannot run in a single transaction | `TX101` | Covered |
| TX201 | Nested transaction detected | `TX201` | Covered |
| OW101 | User not authorized to modify resource | — | Waived — ownership policy binds to Atlas Pro schema-ownership annotations and an account/identity model; Ptah reviews destructive changes through its `DS`/`CD` safety gates instead |
| OW102 | User explicitly denied access to resource | — | Waived — same rationale as OW101 |

Summary: 23 of 30 Pro-marked codes covered, 5 partial, 2 waived. Under
`ptah-compat migrate lint`, native findings report under the `ptah` analyzer
with their native codes, except the proven Atlas identities: native `DS101`
reports as Atlas `destructive`/`DS102`, native `DS102` as
`destructive`/`DS103`, and native `DD101` as `data_depend`/`MF103`.

## Feature parity evidence

Each area below states the current status and links the evidence behind it.

### Offline Atlas fixture ingestion

**Ptah status.** The imported Atlas fixture corpus and CLI probes are tracked in the conformance repository. Treat a red full-conformance gate as product work, not as a broken regression gate; the regression budget records which known gaps are currently tolerated.

**Evidence.** [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510)


### Live database round trips

**Ptah status.** Current live corpus is green: 39 observations, 0 non-OK in the linked report, across PostgreSQL, MySQL, MariaDB, and SQLite. This is evidence for the covered scenarios, not proof of every Atlas OSS runtime path.

**Evidence.** [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md)


### Atlas CE differential checks

**Ptah status.** Current Atlas CE differential corpus is green: 30 observations, 0 non-OK in the linked report, across PostgreSQL, MySQL, and SQLite. This is evidence for the covered scenarios, not proof of every Atlas OSS schema object.

**Evidence.** [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)


### HCL schema files

**Ptah status.** Strict supported subset. Unsupported constructs fail explicitly instead of being ignored. PostgreSQL `include` columns are preserved for indexes, primary keys, and unique constraints across HCL parse/render, SQL parse/render, schema diff, and database introspection paths.

**Evidence.** [HCL schema](../../reference/hcl-schema/)


### Atlas project config

**Ptah status.** Strict supported subset.

Evaluated local env, schema, format, diff, and lint settings can feed
`ptah-compat ... --env` commands. Typed variables include `map(string)` for HCL
defaults and expressions; `toset`, `atlas.env`, `each.key`, and `each.value`
support ordered multi-target `migrate apply` expansion.

Structurally unsupported constructs fail explicitly. Names that Atlas CE
accepts without acting on are accepted and reported once per source location
instead of becoming silent no-ops.

**Evidence.** [Atlas project config](../project-config/)


### Native Go annotations

**Ptah status.** First-party Ptah workflow.

**Evidence.** [Go annotations](../../schema/go-annotations/)

## Gap register

Each gap below records what the current boundary is and where the work is
tracked. This table is the index; the sections carry the boundary detail.

| Gap | Type | Tracking |
| --- | --- | --- |
| [Atlas-compatible command runtime placeholders](#atlas-compatible-command-runtime-placeholders) | Product behavior | [#510](https://github.com/stokaro/ptah/issues/510) |
| [Atlas-compatible down semantics](#atlas-compatible-down-semantics) | Product behavior | [#510](https://github.com/stokaro/ptah/issues/510), [#758](https://github.com/stokaro/ptah/issues/758) |
| [Atlas-compatible pre-approved plan workflow](#atlas-compatible-pre-approved-plan-workflow) | Product behavior | [#758](https://github.com/stokaro/ptah/issues/758) |
| [Atlas-compatible Flyway repeatable import execution](#atlas-compatible-flyway-repeatable-import-execution) | Product behavior | [#510](https://github.com/stokaro/ptah/issues/510) |
| [Atlas-compatible flag semantics](#atlas-compatible-flag-semantics) | Product behavior | [#510](https://github.com/stokaro/ptah/issues/510), [#622](https://github.com/stokaro/ptah/issues/622), [#640](https://github.com/stokaro/ptah/issues/640) |
| [Atlas-compatible hidden `migrate diff --dry-run`](#atlas-compatible-hidden-migrate-diff---dry-run) | Product behavior | [#618](https://github.com/stokaro/ptah/issues/618) |
| [HCL schema and Atlas project config subset audit](#hcl-schema-and-atlas-project-config-subset-audit) | Product behavior and coverage | [#511](https://github.com/stokaro/ptah/issues/511) |
| [Live and differential corpus breadth](#live-and-differential-corpus-breadth) | Conformance coverage | [ptah-atlas-conformance#167](https://github.com/stokaro/ptah-atlas-conformance/issues/167) |
| [Verbs beyond the CE pin](#verbs-beyond-the-ce-pin) | Triage record | [#758](https://github.com/stokaro/ptah/issues/758) |
| [`atlas.hcl` `file()` confinement](#atlashcl-file-confinement) | Deliberate divergence | [#1042](https://github.com/stokaro/ptah/issues/1042) |
| [Exclude field selectors](#exclude-field-selectors) | Deliberate divergence | [#933](https://github.com/stokaro/ptah/issues/933) |
| [Leading schema type selector](#leading-schema-type-selector) | Deliberate divergence | [#933](https://github.com/stokaro/ptah/issues/933) |

### Atlas-compatible command runtime placeholders

**Type.** Product behavior

**Current boundary.** No registered Atlas-compatible path in the current focused #510 set is left as a pure runtime placeholder. `ptah-compat version`, `ptah-compat license`, `ptah-compat schema fmt`, `ptah-compat schema diff`, `ptah-compat schema apply`, `ptah-compat migrate diff`, and `ptah-compat migrate import` now execute Ptah-owned behavior, with command-specific gaps tracked separately.

**Tracking.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510)


### Atlas-compatible down semantics

**Type.** Product behavior

**Current boundary.** `ptah-compat migrate down` is an Atlas OSS command path and recognizes the documented Atlas-style flag names. Flags mapped to native behavior include `--url`, `--dir`, `--to-version`, `--dry-run`, `--revisions-schema`, and `--lock-timeout`.

The forward defaults to the Atlas revision-table layout (`--revision-format atlas`, like `migrate set`), so a bare invocation reverts the revisions `ptah-compat migrate apply` wrote; the native `--revision-format ptah` pass-through selects the Ptah layout. Both layouts retain Ptah's recoverable failed-down bookkeeping instead of hiding a partial rollback.

`--dev-url` replays and verifies the rollback plan on the dev database before the target is touched (the native `ptah migrations down --shadow-db` verification), and `--format` renders an Atlas Go-template report over `.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, and `.Error`. Both output paths start real rollbacks without reading stdin, matching Atlas; native `ptah migrations down` keeps its confirmation prompt.

The registry-bound `--to-tag`, `--skip-checks`, and `--plan` flags are recorded waivers that fail loudly with their rationale.

**Tracking.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758)


### Atlas-compatible pre-approved plan workflow

**Type.** Product behavior

**Current boundary.** `ptah-compat schema plan` computes the declarative migration from the `--from` target database to local `--to` schema files and saves it as a local plan file (`--save`/`--output`/`--name`/`--dry-run`, `--env` project defaults; `--auto-approve` accepted for CLI compatibility). The default format is the Atlas `.plan.hcl` shape, readable by Atlas's plan reader; an `--output` path ending in `.json` writes the native JSON plan with ordered statements, per-statement safety severity, dialect, exclude patterns, and SHA-256 source/desired fingerprints.

`ptah-compat schema apply --plan file://<path>` reads both formats, including `.plan.hcl` files written by Atlas. JSON plans execute after verifying the source fingerprint against the live database, refusing drifted targets loudly. Atlas-format plans require `--to` like Atlas and are verified by replaying the plan on a dev database (an ephemeral SQLite one when the target is SQLite) and comparing the reached state with the desired state; every `--plan` apply with a desired state re-verifies the end state on the target afterward.

`--edit` opens the planned SQL in `$VISUAL`, then `$EDITOR`, and saves the plan rebuilt from valid UTF-8 text, re-deriving dialect-aware statement severity and the destructive marker so the saved plan describes the SQL it actually carries; statement text round-trips verbatim, comments included, so quitting the editor unchanged reproduces the plan byte for byte, and an edit that leaves no statement is refused with nothing written. `--name-format` computes the plan name from a Go template over `.FromHash`/`.ToHash`, exposed as untagged standard-Base64 digests like the measured Atlas values, and cannot be combined with `--name`; default file names refuse path separators, control characters, `.`/`..`, and the characters Windows forbids. `--skip-lint` is accepted as an explicit no-op, because `schema plan` runs no lint step — that is a gap against Pro, which does lint, not parity with it.

The registry-bound `--push`, `--pending`, and `--repo` plan flags are recorded waivers. The registry sub-verbs `approve`, `list`, `pull`, `push` and `rm` stay boundary stubs, and so do `lint` and `test` — both are local by their flag sets, but neither has a measured output contract. `schema plan new` and `schema plan validate` are implemented. `--format` and `--directive` fail explicitly: neither was executed in Atlas, so Atlas's plan report payload and its directive artifact shape are both unmeasured, and guessing either would produce silent divergence rather than parity.

**Tracking.** [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758), [`stokaro/ptah#1037`](https://github.com/stokaro/ptah/issues/1037)


### Atlas-compatible Flyway repeatable import execution

**Type.** Product behavior

**Current boundary.** `ptah-compat migrate import` rejects Flyway `R__...sql` repeatable migrations because Ptah currently treats Atlas R-suffixed migrations as non-executable repeatable files. Importing them successfully would let a later apply skip schema objects silently.

**Tracking.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510)


### Atlas-compatible flag semantics

**Type.** Product behavior

**Current boundary.** `schema inspect --url` now accepts local schema files, one migration directory, and one `env://` reference in addition to database URLs; non-database sources require `schema inspect --dev-url` and are evaluated Atlas-style on the dev database (reset, materialize, introspect), with Atlas's `--dev-url cannot be empty` failure when the dev database is missing.

`schema apply --schema/--include` and `schema diff --schema/--include` now positively scope both comparison sides (schema universe, then include selection, then exclusion), with cross-scope dependency diagnostics and synced output for empty selections; Atlas CE aborts `--include` as non-community, so this is an open Ptah extension.

`migrate down --dev-url` now verifies the rollback plan on the dev database before the target is touched, and `migrate down --format` renders an Atlas Go-template down report; `migrate down --to-tag`, `--skip-checks`, and `--plan` are recorded registry-bound waivers that fail loudly with their rationale.

`schema apply --edit`, `migrate diff --edit`, and `migrate new --edit` now open the operator's `$VISUAL`/`$EDITOR`. `schema apply --plan file://<path>` executes a pre-approved local plan file — the Atlas `.plan.hcl` format or the native JSON plan — after verifying it against the target (fingerprint check, dev-database replay against `--to`, and post-apply end-state verification, per format); registry `atlas://` plan URLs are rejected.

`ptah-compat schema inspect --exclude` now filters inspection output with Atlas-style resource globs and type selectors, including the documented `*[type=extension].version` field selector with schema-qualified globs; other field selectors and type selectors on non-final pattern segments fail explicitly.

`ptah-compat schema inspect --include` positively selects which top-level resources inspection keeps, through the same engine as `schema apply` and `schema diff`. The pinned Atlas CE binary rejects the flag with `unknown flag: --include`, so this is a Pro-surface spelling Ptah implements openly; the two measured divergences from Atlas are tabulated under [Schema inspection](#schema-inspect---include).

The pinned Atlas CE flag surface does not register `schema diff --web`, and Ptah rejects it as unknown too. It does not register `migrate apply --to-version`, `--lock-name`, or `--skip-lock` either, but those three are documented on the wider Atlas distribution's `migrate apply` and Ptah implements them; see [Migration apply](../docs-coverage/#migration-apply) for the measured behavior.

`ptah-compat schema apply --to` and `ptah-compat schema diff --from/--to` accept local schema files, one database URL, one migration directory replayed on the required `--dev-url` dev database, or one `env://` reference resolved through the evaluated `atlas.hcl` env; source kinds cannot be mixed within a flag, and unsupported schemes such as `atlas://` fail before the target database is contacted.

`ptah-compat schema apply --exclude` and disabled `schema.mode` values filter matching resources out of both the current live schema and local desired schema before planning, and `ptah-compat schema diff --exclude` plus disabled `schema.mode` values filter matching resources out of both local `--from` and `--to` schema files.

`ptah-compat schema apply --env` reads evaluated local `env.src`, `env.schema.src`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff` policy from `atlas.hcl`, including `data.hcl_schema.<name>.url`; unsupported remote data sources still fail explicitly.

`ptah-compat schema inspect --env` reads `env.url`, `env.exclude`, `env.schema.mode`, and `format.schema.inspect`. `ptah-compat schema diff --env` reads `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and supported `diff` policy.

`ptah-compat schema inspect --format` supports rendered HCL, explicit `{{ sql . }}` and `{{ json . }}` helpers, custom templates, Mermaid helper output, and HCL/SQL `split | write` file exports. Bare `hcl`, `sql`, and `json` remain literal template text; surrounding whitespace is preserved byte for byte, matching the pinned Atlas CE binary. Split/write uses the documented Atlas strategies (per object by default, `split "schema"`, `split "type"`, optional file-extension argument), rendered as one output plan and applied by a single writer with explicit failures for duplicate paths, traversal, and overwrite hazards. The pinned Atlas CE binary rejects `split`, `write`, and `hcl` as non-community template functions, so these exports are an open Ptah extension.

`ptah-compat schema diff --format` now supports Atlas-style SQL/custom output with `sql` and `.MarshalSQL` for local schema-file diffs.

`ptah-compat schema apply --dev-url` now runs the dev-database plan simulation before a non-dry-run apply: the dev database is reset, the target's current schema is recreated on it, the exact ordered plan executes there, and a failed rehearsal refuses the apply with the target unchanged; `schema apply --lock-timeout` bounds the schema-apply advisory lock on PostgreSQL, MySQL, MariaDB, and SQL Server, with an explicit unlocked-with-note decision on dialects without advisory locks.

`ptah-compat schema apply --tx-mode` supports `file`, `all`, and `none` for generated local-schema plans. `ptah-compat migrate validate --dev-url` now validates SQL execution by cleaning the dev database and replaying the migration directory.

`ptah-compat migrate hash`, `lint`, `new`, `set`, `status`, and `validate` now register Atlas `--dir-format` with default `atlas`; `hash` writes `atlas.sum` by default, `new` creates a single Atlas `.sql` skeleton and updates `atlas.sum`, and `set`/`status` use Atlas revision-table metadata with `--revisions-schema` support.

Atlas's external migration-tool `--dir-format` values are read directly by `hash`, `validate`, `lint`, `status`, and `set`, under both that spelling and `?format=` on `--dir`; `migrate new` and `migrate diff` write the selected layout. Diff planning carries the prior database state into an exact rollback for all five foreign layouts. Goose writes its whole-file `-- +goose NO TRANSACTION` directive when either direction needs it. golang-migrate, Flyway, dbmate, and Liquibase refuse such plans before publication because their safe transaction metadata has not been proven. Ordinary transactional forward and rollback plans write normally.

A `--dir` query key other than `format` is ignored on the eight verbs that accept a `--dir` query — `apply`, `diff`, `hash`, `lint`, `new`, `set`, `status` and `validate` — as Atlas ignores it, and named on standard error so a misspelled one is not silent; `PTAH_STRICT_DIR_QUERY=1` refuses it instead. `checkpoint`, `down`, `edit`, `rebase`, `rm` and `test` still refuse a `--dir` query outright. This is stricter than a CLI with no contract on those verbs rather than a parity gap: the pinned community binary answers `unknown flag: --dir` on all of them.

`ptah-compat migrate lint --dev-url` now infers dialect and cleans and replays migrations on directly connectable dev databases; `--latest`, `--git-base`, and `--git-dir` select the linted changeset; `--format` renders Atlas Go-template output over `.Env`, `.Steps`, and `.Files`; Docker dev databases and web reports remain gaps.

`ptah-compat migrate status --format` renders Atlas Go-template output over `.Env`, `.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, `.Status`, and, on a half-applied migration, `.Count`, `.Total`, `.SQL`, and `.Error`. Its default report mirrors the Atlas shape (`Migration Status: OK` with `  -- Current Version:` / `  -- Next Version:` / `  -- Executed Files:` / `  -- Pending Files:` lines), because `migrate status` is the verb pipelines parse with a machine ([#1102](https://github.com/stokaro/ptah/issues/1102)); native `ptah migrations status` keeps its own block.

`ptah-compat migrate diff --to` now supports local schema files, one directly
connectable database URL, one local Atlas migration directory, or one `env://`
reference, with desired/dev database aliases rejected before connection.

- with `--env`, it reads `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and supported `diff` policy including `diff.concurrent_index.create`, tagging concurrent-index migration files with `-- atlas:txmode none` and splitting mixed plans into a transactional file followed by a concurrent-index file
- `migrate diff --qualifier` now applies Atlas's single-schema custom qualifier to every object in the generated statements on PostgreSQL, CockroachDB, YugabyteDB, MySQL, and MariaDB dev databases, failing explicitly before any file or checksum write for invalid values, unsupported dialects, multi-schema plans, and not-yet-qualifiable statement kinds
- `--schema` scopes the desired schema and the replayed dev database state
- `--lock-timeout` bounds Ptah's local migration-directory lock for checksum/write safety
- `--format` renders generated migration SQL with `sql` and `.MarshalSQL`

`atlas.sum` updates only after every generated file was written. Docker dev
databases remain a follow-up gap.

`ptah-compat migrate apply` now supports positional `amount`, `--baseline`, `--allow-dirty`, `--tx-mode`, `--exec-order`, `--revisions-schema`, `--lock-timeout`, `--lock-name`, `--skip-lock`, `--to-version`, `--dry-run`, `--format`, and matching `atlas.hcl` env defaults against Atlas-format migration directories and Atlas revision metadata; the pinned community binary does not register `migrate apply --dir-format`, and Ptah rejects it there.

Per-file `atlas:txmode` precedence and validation match the measured Atlas CE
`v1.3.0` plain-file matrix. Global `file` and `none` accept explicit file or
none overrides and validate files as execution reaches them. Global `all`
rejects every explicit file mode before starting the selected batch. Unknown,
duplicate, and file-level all values fail before the affected migration body
or revision row changes. The compatibility surface prints Atlas's leaf
transaction-mode diagnostic; native commands retain Ptah's broader apply
context.

Ptah also applies those modes independently to `migration.sql` and `down.sql`
inside txtar files. Atlas CE `v1.3.0` ignores section-local modes. Ptah rejects
a transaction-mode header before the `atlas:txtar` marker instead of treating
the archive as plain SQL, because plain execution could run both directions.
This is an intentional safety difference. Transactional failure revision
bookkeeping remains tracked separately in
[`stokaro/ptah#887`](https://github.com/stokaro/ptah/issues/887).

**Tracking.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#622`](https://github.com/stokaro/ptah/issues/622), [`stokaro/ptah#640`](https://github.com/stokaro/ptah/issues/640)

### Atlas-compatible hidden `migrate diff --dry-run`

**Type.** Product behavior

**Current boundary.** `ptah-compat migrate diff --dry-run` is supported as an Atlas-hidden flag: it is accepted by the command, omitted from normal help output, prints generated SQL, and does not write a migration file or `atlas.sum`.

**Tracking.** [`stokaro/ptah#618`](https://github.com/stokaro/ptah/issues/618)


### HCL schema and Atlas project config subset audit

**Type.** Product behavior and coverage

**Current boundary.** Current imported fixtures pass, and there are no concrete unsupported Atlas OSS schema/config constructs listed in the current conformance reports. Complete schema/config parity is not claimed until the remaining Atlas OSS surface is audited; newly discovered unsupported constructs should become focused implementation issues.

**Tracking.** [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511)


### Live and differential corpus breadth

**Type.** Conformance coverage

**Current boundary.** The live and Atlas CE differential reports are green for the current committed corpus only. More fixtures are needed before using those checks as broad Atlas OSS parity evidence.

**Tracking.** [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167)


### Verbs beyond the CE pin

**Type.** Triage record

**Current boundary.** `atlas migrate ls`, `atlas migrate show`, `atlas schema stats`, and `atlas schema validate` appear in current Atlas docs but are entirely absent from the pinned conformance Atlas CE v1.2.0 binary (each resolves to `unknown command`, not a community-version abort stub), so they are outside the CLI-surface parity target today.

Triage: `migrate ls` is covered by native `ptah migrations status` (lists every migration with version, description, and state); `schema validate` is covered by native `ptah schema render` (parse/load validation) plus `ptah schema test` / `schema apply --dry-run` for dev-database validation; `migrate show` (print a migration's SQL) is future work with no native verb today; `schema stats` (OpenMetrics database statistics) is out of scope as an observability surface rather than schema management.

Revisit when the conformance Atlas pin advances past v1.2.0.

**Tracking.** [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758)


### Revision row for a migration whose body failed

**Type.** Compatibility behavior with a native safety difference

**Current boundary.** When a migration body fails inside a transaction and
rollback succeeds, the pinned community binary v1.3.0 writes no revision row.
`ptah-compat` now reaches the same end state and retries the whole file on the
next apply. Native `ptah migrations up` keeps a durable failed row for status
and repair workflows.

Measured across the effective transaction-mode matrix:

```text
effective file transaction   binary 0 rows   ptah-compat 0 rows   native 1 row
effective all transaction    binary 0 rows   ptah-compat 0 rows   native 1 row
effective no transaction     binary 1 row    ptah-compat 1 row    native 1 row
```

The compatibility cleanup is fail-closed. It removes a zero-progress row only
after `Rollback` returns success. A rollback error, commit error, unknown
statement outcome, or any committed statement keeps the row. Those states
still require explicit recovery, so parity does not erase evidence when the
database outcome is uncertain.

**Evidence.** [`stokaro/ptah#1196`](https://github.com/stokaro/ptah/issues/1196),
[`stokaro/ptah#1333`](https://github.com/stokaro/ptah/issues/1333)

### Dirty retry verifies committed statements

**Type.** Deliberate safety divergence

**Current boundary.** Atlas CE resumes a dirty non-transactional revision from
`applied + 1` by statement index. Ptah first proves that every statement it
will skip has unchanged source text. Ptah-format rows store a `partial:h1:`
checksum for the committed prefix; Atlas-format rows use the existing
`partial_hashes` column. A changed prefix, malformed metadata, or contradictory
hash count is refused, as are negative progress counters, `applied > total`,
and a native `state=applied` row whose counters are incomplete. Invalid
metadata is rejected by revision listing, status, version, and apply operations
rather than being hidden as a clean row. Legacy rows without prefix metadata
resume only while their full-file hash still matches.

Editing only the unapplied suffix remains supported. If that retry changes from
`none` to `file` or `all` and its transaction rolls back, Ptah retains the
previously committed `applied` floor rather than making a later run replay SQL.
Process exit, context cancellation, or deadline while an autocommit statement
is in flight preserves the unknown-outcome marker.

This is stricter than Atlas CE in the safe direction: uncertain recovery exits
non-zero instead of skipping SQL based only on a stale integer offset.

### Recorded revision `error` text on a failed migration

**Type.** Driver difference, not a behavior one

**Current boundary.** The Atlas revision table's `error` column records the
database's own message on both sides, but the two spell the same condition
differently because they use different SQLite drivers. On the same failing
migration:

```text
pinned community binary v1.3.0   no such table: missing_table
ptah-compat                      SQL logic error: no such table: missing_table (1)
```

Ptah used to record more than that — its own `failed to execute migration SQL:`
prefix and a `SQL:` line repeating the statement that the adjacent `error_stmt`
column already holds in full. Both were Ptah's own additions and are gone; the
column now carries the innermost error and nothing else. `error_stmt` matches
byte for byte, terminating semicolon included.

What is left is `modernc.org/sqlite`'s wording. Closing it would mean rewriting
driver messages per driver and per dialect to match a different driver's
phrasing, which trades a cosmetic difference for a table of string surgery that
goes stale silently. The native revision format is unaffected either way: this
applies only where the revision table is Atlas-shaped, and Ptah's own surface
keeps the context it added.

**Tracking.** [`stokaro/ptah#1196`](https://github.com/stokaro/ptah/issues/1196)

### `atlas.hcl` `file()` confinement

**Type.** Deliberate divergence

**Current boundary.** `ptah-compat` reads `file()` and `fileset()` inside an
`atlas.hcl` only from the directory holding that `atlas.hcl`. Three shapes are
refused that the pinned community binary reads: an absolute path, a
parent-traversal path, and a plain relative name that resolves out of the
directory through a symbolic link. The refusal names which of the three applies
and points at `getenv()` for passing a value in from outside.

This is one of the deliberate stricter refusals `ptah-compat` keeps. The wider
retained set is listed on [Retained divergences](../retained-divergences/);
every entry is stricter rather than looser, so it cannot exit `0` where the
binary exits `1`. The reason here is that an `atlas.hcl` is usually
repository-controlled
and `file()` is evaluated before anything is applied: without confinement, a
config file arriving in a pull request can read any file the process can and
place the contents somewhere observable, such as a database URL or an error
message. That is a different class of cost from the footguns the drop-in rule
was written for.

Both halves are measured rather than remembered. The Ptah half is pinned by
`TestOraclePlacesOutsideFileContentsWhereTheCallerCanSeeThem` in
`config/projectconfig`; the community half is pinned by four `ce-gating`
scenarios in the conformance repository, so the day a community build starts
confining `file()` the gate goes red and this divergence can be retired.

**Tracking.** [`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042)

### A migration that would be recorded over an invalid index

**Type.** Deliberate divergence

**Current boundary.** On PostgreSQL, `ptah-compat migrate apply` refuses a
migration while an index that migration creates is reported unusable by
`pg_index` (`indisvalid` or `indisready` false). The pinned community binary
v1.3.0 applies it and records it.

Measured on PostgreSQL 17.10, identical fixture on both: a `members` table whose
duplicate rows made an earlier `CREATE UNIQUE INDEX CONCURRENTLY` fail, the
duplicates then removed, and one migration carrying `-- atlas:txmode none` and
the same `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS` statement.

| | pinned community binary v1.3.0 | `ptah-compat` |
| --- | --- | --- |
| `migrate apply --allow-dirty` | exit `0`, `-- ok`, 1 statement | exit `1`, names the index and `REINDEX` |
| `pg_index` afterwards | `indisvalid=f`, `indisready=f` | unchanged, nothing written |
| revision row afterwards | `20260808000001` applied 1/1, no error | no row |
| duplicate `INSERT` afterwards | accepted, 2 rows share the email | n/a |
| after `REINDEX INDEX CONCURRENTLY` | n/a | exit `0`, `indisvalid=t`, row recorded, duplicate rejected |

The leftover keeps the name, so `IF NOT EXISTS` skips it and the statement
reports success over an index that enforces nothing. Recording that as applied
means the tooling reports a constraint the database does not have, and nothing
will look again. Ptah also rejects another index or non-index relation that owns
the schema-level name. It resolves an unqualified drop and target through the
active `search_path`, permits cleanup only when that exact drop will run first
in this attempt, and positively rechecks the active transaction or connection
before recording the revision. A drop skipped by dirty-resume is not cleanup.

The divergence is stricter, not looser: `ptah-compat` exits `1` where the binary
exits `0`, never the reverse, so no invocation the binary refuses succeeds here.

**Tracking.** [`stokaro/ptah#1101`](https://github.com/stokaro/ptah/issues/1101)

### Exclude field selectors

**Type.** Deliberate divergence

**Current boundary.** A field selector is the `.field` suffix behind a
`[type=...]` selector: `--exclude '*[type=table].comment'` asks for the comment
of every table to be dropped while the tables themselves stay. The pinned Atlas
community binary accepts every such suffix and honors none of them. Measured on
PostgreSQL 16 with two commented tables across two schemas, all three of

```text
--exclude '*[type=table].comment'
--exclude 'public.*[type=table].comment'
--exclude '*[type=table].*'
```

are exit `0` there with output byte-identical to the same command without the
flag, comments included.

`ptah-compat` honors the ones it can carry out — `[type=extension].version`,
`.comment` on `table`, `view` and `materialized_view`, and `.*` for all of them
— and refuses the rest by name before a database is contacted. So the first and
third of those commands are exit `0` with the tables rendered and their comments
gone, and a suffix such as `.charset` is exit `1` naming the fields that would
have worked.

The reason not to copy accept-and-ignore is the one the gap register already
records for `file()`: an `--exclude` selector is a scoping instruction, and the
reason to write one is usually that the object must not be touched. Accepting
one and silently not carrying it out defeats that intent with no diagnostic, and
on `schema apply` and `schema diff` the same shape of miss reaches a `DROP`.
Both directions here are safe under the drop-in rule: honoring a selector
subtracts more from a plan rather than less, and refusing one exits `1` where
that binary exits `0`, never the reverse.

The second of those commands stays exit `1` in `ptah-compat`, and for a
different reason — the pattern-depth rule, not the field rule. That binary
refuses the identical pattern on a schema-bound URL as
`too many parts in pattern: "public.public.*[type=table].comment"`, and Ptah
applies one depth rule to every scope, so accepting it would exit `0` where that
binary exits `1`.

**Tracking.** [`stokaro/ptah#933`](https://github.com/stokaro/ptah/issues/933)

### Leading schema type selector

**Type.** Deliberate divergence

**Current boundary.** `--exclude '*[type=schema].*[type=table]'` means every
table inside every schema on every Ptah schema source. The leading schema glob
may be narrowed, as in `app[type=schema].*[type=table]`.

The pinned community binary v1.3.0 gives source-dependent answers. On a live
PostgreSQL database containing tables and enums in `public` and `app`, the
selector removes both tables and keeps both enums. On a SQLite file diff between
one table and the same schema plus a second table, it exits `0` but leaves the
second table's `CREATE TABLE` plan unchanged. Removing the selector from that
SQLite run produces byte-identical output; `*[type=table]` is the control that
does remove the plan.

Ptah keeps the literal PostgreSQL answer on both source kinds. Making the
selector a no-op only for a file diff would make one accepted scoping instruction
mean two things depending on its input. It would also report success while
leaving exactly the table the selector names in the migration plan. This is a
defect Ptah does not copy; the complete compatibility surface keeps the coherent
behavior rather than narrowing it to this Atlas CE result.

The integration contour pins the file-diff result in
`TestAtlasCompatLeadingSchemaTypeSelectorE2E`. The filter tests separately pin
the schema glob, the final resource type, and the surviving non-table objects.

**Tracking.** [`stokaro/ptah#933`](https://github.com/stokaro/ptah/issues/933)

A green docs build only proves the documentation site builds and internal links
resolve. It is not parity evidence. Use the conformance reports for measured
behavior and the gap register in this section for known product, coverage, and
documentation gaps.

## Related contracts

- Configuration precedence — which of CLI flags, environment variables,
  `atlas.hcl`, `ptah.yaml`, and built-in defaults wins — is documented in
  [Configuration](../../reference/configuration/).
- The safety gates around versioned migrations (integrity files, replay
  validation, lint, the destructive gate) are documented in
  [Integrity and safety](../../versioned/integrity-and-safety/).
- Process exit codes for every command are listed in
  [Exit codes](../../reference/exit-codes/).
