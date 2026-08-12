# Dialect capabilities

Ptah maps several real database targets onto shared implementations: MySQL and
MariaDB share one planner family; CockroachDB, YugabyteDB, and Spanner share
the PostgreSQL family with target-specific capability presets; SQL Server uses
its own T-SQL renderer and dbschema implementation while initially reusing the
closest generic planner path; and versions within a single dialect differ in
which DDL they accept. Instead of forking a new dialect for every variant,
capability-aware planners and renderers consult a **capability set** — a
validated `map[Capability]bool` describing what the concrete target accepts —
and restrict or enable individual emissions accordingly (issues #225/#226/#171).

Package: `core/platform/capability`.

## Cross-cutting artifact distribution

OCI artifact distribution is a cross-cutting Ptah capability, not a database
dialect capability key in `core/platform/capability`.

- **Bring-your-own OCI registry** — native migration and desired-schema push/pull against OCI-compliant registries, authenticated through the Docker credential store.
- **Reference semantics** — unqualified references resolve to `latest`; tags are movable; `@sha256:` digest pins are immutable; a `:tag@sha256:` reference is accepted and resolves by the digest while keeping the tag in the canonical form; pushes to any digest-carrying reference are rejected.
- **Direct migration consumption** — `ptah migrations up`, `status`, and `down` accept `oci://` through `--migrations-dir`; `up --verify-sum` verifies the pulled directory against the sum that traveled inside it, and `up` prints the resolved digest and its `@sha256:` pin whenever that check ran over a movable tag.
- **Direct schema consumption** — `ptah schema compare` and `drift` accept `oci://` through `--schema-file`.
- **Canonical desired schema** — schema publication emits exactly one lossless canonical `schema.hcl` and fails closed on managed data, lossy diagnostics, or unstable HCL round trips.
- **Deployment reporting** — successful, non-dry-run OCI-backed `migrations up` runs that add committed revisions attach a best-effort, redacted deployment report unless `--skip-report` is set. No-op runs do not publish a report.
- **OCI referrers** — deployment, lint, and plan reports attach to exact source digests. Native Referrers API discovery is preferred; Ptah merges the standard tag-schema fallback with per-attachment durable tags for concurrent Ptah writers. `ptah oci referrers` lists direct descriptor metadata with type and output-format filters; payload download and consumption are not implemented.
- **Atlas boundary** — this is a native Ptah capability. It does not implement Atlas Cloud, `atlas://`, or the Atlas-compatible push stubs.

See [OCI Registry Artifacts](./oci_registry.md) for commands, authentication,
pinning, integrity, security, GHCR CI, and the Atlas-to-OCI concept mapping.

## Workflow capability: declarative database testing

Ptah also provides a product-level testing capability through
`ptah migrations test`, `ptah schema test`, and the exported
`migration/dbtest` package. It can migrate to selected versions, apply a desired
schema, load seed fixtures, execute SQL, and assert row counts, scalar values,
or expected errors against a disposable database.

This workflow is not a dialect capability key: it composes the existing
migrator, schema renderer, seeder, database connection, and shadow-database
lifecycle. It runs locally under the MIT license with no account requirement.
Atlas CE cannot run the corresponding migration or schema test commands; Atlas
keeps that testing framework in its proprietary feature set. See
[Declarative database testing](testing.md).

Desired-schema source format is independent of dialect capabilities. Go
annotations, YAML/HCL/SQL files, and external commands that emit SQL, HCL, or
YAML all resolve into Ptah's schema IR before capability-aware planning and
rendering begin.

## The model

Two layers cooperate:

- **Intent (planner).** A planner configured with a capability set records
  intent on AST nodes — e.g. a MariaDB-preset planner sets `IfExists` on
  constraint drops because MariaDB accepts guarded drops.
- **Validity (renderer).** Capability-aware renderers check modifiers against
  *their own* target capability set and drop anything the target would reject —
  the `mysql` renderer strips `IF EXISTS` from constraint and index drops even
  if a stray intent flag reaches it. Some newer dialect surfaces, including the
  initial SQL Server renderer, still rely on planner/configuration boundaries
  for unsupported feature suppression unless the renderer-specific section below
  says a modifier is validated again.

At the `Capabilities` type level the nil/empty set is valid and reads as
"everything absent" (`Has` is nil-safe). The **planners** deliberately do NOT
treat nil as assume-nothing, though: a zero-value planner (`&mysql.Planner{}`,
`&postgres.Planner{}`) defaults to its dialect's current-line preset, so it
behaves exactly like `New()`. An assume-nothing planner would be a trap — it
would silently downgrade CHECK additions to warnings (turning a CHECK
modification into a destructive drop-without-re-add) and re-spell CHECK drops.
Restricting emissions is always an explicit choice: pass a legacy preset or a
composed set to `NewWithCapabilities` (which clones its argument).

## Registry

Capability keys are a **curated registry** — `Validate()` rejects unknown keys,
so typos fail fast. Current registry:

| Capability | Meaning |
|---|---|
| `drop_constraint_generic` | SQL-standard `ALTER TABLE … DROP CONSTRAINT` for non-FK constraints (MySQL 8.0.19+, MariaDB, PostgreSQL) |
| `drop_constraint_if_exists` | `IF EXISTS` guard on constraint drops (MariaDB, PostgreSQL; **rejected by MySQL**). Requires `drop_constraint_generic` |
| `drop_index_if_exists` | `IF EXISTS` guard on `DROP INDEX` (MariaDB 10.1.4+, PostgreSQL; **rejected by MySQL**) |
| `check_constraints_enforced` | CHECK constraints are enforced, not parsed-and-ignored (MySQL 8.0.16+, MariaDB 10.2.1+, PostgreSQL) |
| `drop_check_clause` | Dedicated `ALTER TABLE … DROP CHECK` spelling (MySQL 8.0.16+ only; **MariaDB rejects it** — verified live). Requires `check_constraints_enforced` |
| `enum_inline_column` | Enums are inline column types (MySQL/MariaDB `ENUM`, ClickHouse `Enum8/16`) |
| `enum_custom_type` | Enums are separate named types (PostgreSQL `CREATE TYPE … AS ENUM`) |
| `create_index_concurrently` | `CREATE [UNIQUE] INDEX CONCURRENTLY` (PostgreSQL; a compatibility no-op on CockroachDB) |
| `drop_index_concurrently` | `DROP INDEX CONCURRENTLY` (PostgreSQL; disabled on the PostgreSQL-compatible presets that do not emit `CONCURRENTLY`) |
| `index_include_spgist` | `SPGIST` indexes with `INCLUDE` payload columns (PostgreSQL 14+) |
| `views` | Standalone `CREATE VIEW … AS <query>` objects |
| `materialized_views` | `CREATE MATERIALIZED VIEW`: a view whose query result is stored. Requires `views` |
| `functions` | User-defined functions declared with a return type, a language, and a body |
| `triggers` | `CREATE TRIGGER` objects |
| `create_or_replace_trigger` | Single-statement trigger replacement: `CREATE OR REPLACE TRIGGER` on PostgreSQL 14+/MariaDB and `CREATE OR ALTER TRIGGER` on SQL Server. Not available on MySQL. Requires `triggers` |
| `alter_generated_column_expression` | In-place `ALTER COLUMN SET EXPRESSION` for generated columns (PostgreSQL 17+) |
| `row_level_security` | Row-level security policies (PostgreSQL) |
| `role_management` | PostgreSQL role and object privilege management (`CREATE/ALTER ROLE`, `GRANT`, `REVOKE`) |
| `foreign_keys` | Declarative `FOREIGN KEY` constraints |
| `foreign_keys_require_unique_reference` | Foreign keys require a declared primary or unique referenced key (MySQL 8.4+ default). Requires `foreign_keys` |
| `foreign_keys_require_indexed_reference` | Foreign keys may reference a complete leftmost index prefix (MySQL before 8.4 and MariaDB). Requires `foreign_keys` |
| `foreign_keys_create_backing_index` | The database creates the foreign key's backing index (Spanner). Requires `foreign_keys` |
| `sequences` | Database sequence objects: `SERIAL`/`BIGSERIAL` column backing and first-class standalone sequences via `//ptah:schema:sequence` (`CREATE`/`ALTER`/`DROP SEQUENCE`). See [Sequences](./sequences.md). |
| `xml_type` | PostgreSQL `XML` column type |
| `advisory_locks` | PostgreSQL advisory lock functions such as `pg_advisory_lock` |

### Validation rules

`Capabilities.Validate()` enforces:

1. **Known keys only** — anything outside the registry is an error.
2. **Requirement edges** — an enabled capability with a disabled prerequisite
   is a contradiction (`drop_constraint_if_exists` without
   `drop_constraint_generic`: an `IF EXISTS` variant of a statement the target
   does not have). The object-kind keys carry the same rule: a refinement
   cannot outlive its object, so `materialized_views` requires `views` and
   `create_or_replace_trigger` requires `triggers`.
3. **Mutual exclusion groups** — at most one member of a group may be enabled
   (`enum_inline_column` vs `enum_custom_type`: a dialect models enums one way
   or the other).
4. **Foreign-key reference policy** — a target with `foreign_keys` enabled has
   exactly one referenced-key policy: declared unique key, full leftmost index
   prefix, or engine-managed backing index.

Presets are valid by construction (unit-tested); validate hand-built or
composed sets yourself.

## Presets

| Capability | MySQL84 | MySQL8019 | MySQL8016 | MySQLLegacy | MariaDB1011 | MariaDBLegacy | Postgres17 | Postgres16 | Postgres13 | ClickHouse24 | CockroachDB23 | CockroachDB25 | CockroachDB26 | YugabyteDB25 | SQLite3 | SQLServer2022 | SpannerPG |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `drop_constraint_generic` | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ |
| `drop_constraint_if_exists` | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `drop_index_if_exists` | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| `check_constraints_enforced` | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| `drop_check_clause` | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `enum_inline_column` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `enum_custom_type` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `create_index_concurrently` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| `drop_index_concurrently` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `index_include_spgist` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `views` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `materialized_views` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `functions` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| `triggers` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| `create_or_replace_trigger` | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ |
| `alter_generated_column_expression` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `row_level_security` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `role_management` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `foreign_keys` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `foreign_keys_require_unique_reference` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| `foreign_keys_require_indexed_reference` | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `foreign_keys_create_backing_index` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `sequences` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `xml_type` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ |
| `advisory_locks` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |

Version lines: `MySQL84()` covers MySQL 8.4+, 9.x, and the measured 26.x
generation; `MySQL8019()` covers
8.0.19–8.3; `MySQL8016()` covers 8.0.16–8.0.18; `MySQLLegacy()` anything
older. `MariaDB1011()` covers the
supported MariaDB lines (10.6+ through 12.x); `MariaDBLegacy()` is the
conservative floor `ForServerVersion` assigns to pre-10.2 servers.
`Postgres17()` covers PostgreSQL 17–18; `Postgres16()` covers 14–16;
`Postgres13()` covers 12–13 (no
`CREATE OR REPLACE TRIGGER` and no `SPGIST` indexes with `INCLUDE` columns).

`CockroachDB25()` and `CockroachDB26()` are the measured CockroachDB release
arms. The 25.x arm disables generic and guarded `DROP CONSTRAINT` plus
`CREATE OR REPLACE TRIGGER`; the 26.x arm supports them. `CockroachDB23()`
remains the conservative historical arm. `YugabyteDB25()` is the current
YugabyteDB distributed-SQL preset; `SpannerPostgres()` is deliberately
conservative because Spanner's PostgreSQL interface is not a drop-in
PostgreSQL server.

`SQLServer2022()` covers Ptah's initial portable SQL Server/Azure SQL subset:
schemas, tables, `IDENTITY`, enforced CHECK/UNIQUE/FK constraints, basic
indexes, raw-SQL view/trigger rendering, and `XML` columns. Standalone
sequence objects and drift-safe normalization for SQL Server-specific view,
trigger, and index metadata are outside the initial SQL Server subset.

### Saturation: servers newer than the newest measured line

Every version ladder above ends in an open-topped arm, so a server newer than
anything Ptah measured still resolves to the newest preset in its dialect. That
is a stand-in, not a match: whatever the newer release gained or lost is
unmodeled.

The preset such a server receives is byte-identical to `ForDialect`'s, which is
exactly what `ForServerVersionResult`'s boolean means by "no version-specific
preset could be selected". So the boolean is `false` there:

```go
caps, versionSpecific := capability.ForServerVersionResult("mysql", "99.0")
// caps            == capability.MySQL84() == capability.ForDialect("mysql")
// versionSpecific == false   (99.x ran off the top of the ladder)
```

`ResolveServerVersion` separates that fallback from the other one — a version
that could not be parsed at all — and names the line the server was planned as:

```go
resolution := capability.ResolveServerVersion("mysql", "99.0")
// resolution.Capabilities    == capability.MySQL84()
// resolution.VersionSpecific == false   (the dialect default was used)
// resolution.Saturated       == true    (99.x is past the newest measured line)
// resolution.NewestMeasured  == "26.x"
```

`Saturated` and `VersionSpecific` are never both true.

The newest measured line per refined dialect:

| Dialect | Newest measured line | Saturates above |
| --- | --- | --- |
| MySQL | 26.x (`MySQL84()`) | 26 |
| MariaDB | 12.x (`MariaDB1011()`) | 12 |
| PostgreSQL | 18.x (`Postgres17()`) | 18 |
| CockroachDB | 26.2 (`CockroachDB26()`) | 26.2 |

The matrix measured PostgreSQL 18.4, MySQL 26.7.0, MariaDB 12.3.0, and both
CockroachDB 25.4 and 26.2 before promoting those lines. CockroachDB matches the
full major/minor line: an unmeasured sibling such as 26.1 is not reported as a
26.2-specific match, while 26.3 saturates above the newest measured line. The
resolver uses the preceding conservative preset between measured lines.

Raising one of those numbers is the deliberate act of claiming a newer server
line behaves like the preset it lands on. Do it in the change that measures
that line — never as a side effect of bumping a container tag.

Saturation is only defined where this package has a version ladder. ClickHouse,
SQLite, and SQL Server have no ladder at all, while YugabyteDB and Spanner are
resolved from the banner without consulting a version; those five report
`Saturated=false` and an empty `NewestMeasured`. Refining those dialects is the
remaining scope of issue #916.

`dbschema.ConnectToDatabase` is the one production caller of the version-aware
selector. It records a saturated resolution at `DEBUG`, naming the dialect, the
server version, and the line it was planned as; an unparseable version is
recorded at `DEBUG` too. Neither reaches a default run's stderr, and that is
deliberate: the CLI's default logger keeps `WARN` and above so that a clean run
emits nothing, and connecting to a supported server is a clean run. The
first connection after a vendor publishes an unmeasured major can saturate, so
a warning would be noise on every command against that server rather than a
diagnostic. Use `--log-level debug` to see it, or read `Saturated` and
`NewestMeasured` from `ResolveServerVersion` directly.

Surfacing an unrefined version to the user on a channel of its own is
criterion 6 of issue #916 and belongs with the CLI work that owns that channel.

### Composition

```go
caps := capability.MariaDB1011().With(capability.DropIndexIfExists, false)
if err := caps.Validate(); err != nil { /* reject configuration */ }
planner := mysql.NewWithCapabilities(caps)
```

`With` copies — presets are never mutated.

`capability.IndexIncludeSPGiST` distinguishes PostgreSQL 14 and newer from
PostgreSQL 12–13 when rendering `SPGIST` indexes with `INCLUDE` payload
columns. Whole-schema and direct-AST rendering consume the same resolved key,
so a PostgreSQL 13 connection refuses that syntax before emitting SQL.

### Resolving a preset

- `capability.DefaultDialects()` — normalized dialect names with a default
  preset. Use this when a guard or UI needs to cover every dialect Ptah routes
  through `ForDialect` without maintaining a second list.
- `capability.ForDialect("mariadb")` — default preset for a dialect name
  (aliases like `pgx`/`postgresql`, `crdb`/`cockroachdb`,
  `ysql`/`yugabytedb`, and `cloudspanner`/`spanner` normalize first). Used by
  `GetPlanner` and the renderers.
- `capability.ForServerVersion("mysql", version)` — refine using a live
  `SELECT version()` string. Recognizes shapes like `8.0.42-log`,
  `10.11.6-MariaDB-…`, the `5.5.5-10.11.6-MariaDB` replication-protocol prefix
  (MariaDB over the mysql driver resolves to the MariaDB preset), and
  `PostgreSQL 16.3 (…)`. PostgreSQL-wire banners containing `CockroachDB`,
  `YugabyteDB`/`Yugabyte`, or `Spanner` resolve to their distributed-SQL
  presets. `dbschema.ConnectToDatabase` stores this resolved set in
  `conn.Info().Capabilities`, and live migration generation passes that same
  set through planning, rendering, and safety assessment. Root MySQL 8.4+
  connections keep the conservative unique-key policy because a session value
  sampled through a pool may not belong to the session that executes the SQL.
  `DatabaseConnection.WithSession` reads
  `restrict_fk_on_non_standard_key` on its pinned physical connection before
  invoking the callback: `ON` keeps the unique-key policy, while `OFF` selects
  the indexed-left-prefix policy for planning and execution on that same
  session.

Offline SQL generation has no server banner to inspect. Factories such as
`planner.GetPlanner`, `renderer.NewRenderer`, and
`planner.GenerateSchemaDiffSQLStatements` therefore use `ForDialect`, which is
the current-version default for the normalized dialect. Use the
`...WithCapabilities` variants when a caller has a live `DBInfo.Capabilities`
value or wants to pin a specific server version in tests/CI.

## Supported release lines

This is the version matrix: every database release line Ptah covers, the
capability preset it claims, and whether continuous integration measures that
claim against a live server.

It is generated from `internal/capabilityprobe/cells.go`, which is the only
place a release line is declared. The tiered pipeline of stokaro/ptah#1341
reads the same declaration, so the workflow files carry no list of versions and
`scripts/check-version-matrix.sh` fails the build when this table drifts from
the declaration it was generated from.

Three columns need reading carefully.

**Refinement** says how a server on the line reaches its preset.
`version-ladder` means the parsed version selects the arm that answers, so an
observation belongs to that line alone. `measured-release-line` means the
resolver still matches an engine banner, but this line has been measured
directly. `banner-substring` and `dialect-default` mean every release of the
engine receives the same set, so an observation on one release cannot be
credited to one line rather than its siblings.

**Tag names the line** answers the scoping rule: the matrix pins a line and
resolves its newest patch rather than freezing a patch that goes stale when the
vendor ships another. `postgres:17` is a registry line tag and CockroachDB
publishes `latest-v<line>` aliases. YugabyteDB publishes neither form, so the
CI driver queries Docker Hub for the highest numeric tag under the declared
line immediately before Docker runs. The SQL Server tags name a marketing year
where the matrix line is the product version; a `no` records that mismatch.

**Probed per pull request** is tier 2. A line is probed when a container
reproduces it and the capability probe has a statement table for its dialect;
both halves are derived, so adding a ClickHouse plan turns four skipped cells
into four probed ones with no workflow edit.

<!-- BEGIN GENERATED VERSION MATRIX -->
| Dialect | Release line | Capability preset | Refinement | Container image | Tag names the line | Probed per pull request |
| --- | --- | --- | --- | --- | --- | --- |
| `postgres` | 18 | `Postgres17` | version-ladder | `postgres:18` | yes | yes |
| `postgres` | 17 | `Postgres17` | version-ladder | `postgres:17` | yes | yes |
| `postgres` | 16 | `Postgres16` | version-ladder | `postgres:16` | yes | yes |
| `postgres` | 15 | `Postgres16` | version-ladder | `postgres:15` | yes | yes |
| `postgres` | 14 | `Postgres16` | version-ladder | `postgres:14` | yes | yes |
| `postgres` | 13 | `Postgres13` | version-ladder | `postgres:13` | yes | yes |
| `mysql` | 26.7 | `MySQL84` | version-ladder | `mysql:26.7` | yes | yes |
| `mysql` | 9.7 | `MySQL84` | version-ladder | `mysql:9.7` | yes | yes |
| `mysql` | 8.4 | `MySQL84` | version-ladder | `mysql:8.4` | yes | yes |
| `mariadb` | 12.3 | `MariaDB1011` | version-ladder | `mariadb:12.3` | yes | yes |
| `mariadb` | 11.8 | `MariaDB1011` | version-ladder | `mariadb:11.8` | yes | yes |
| `mariadb` | 11.4 | `MariaDB1011` | version-ladder | `mariadb:11.4` | yes | yes |
| `mariadb` | 10.11 | `MariaDB1011` | version-ladder | `mariadb:10.11` | yes | yes |
| `cockroachdb` | 26.2 | `CockroachDB26` | version-ladder | `cockroachdb/cockroach:latest-v26.2` | yes | yes |
| `cockroachdb` | 25.4 | `CockroachDB25` | version-ladder | `cockroachdb/cockroach:latest-v25.4` | yes | yes |
| `yugabytedb` | 2026.1 | `YugabyteDB25` | measured-release-line | `yugabytedb/yugabyte:2026.1` | yes | yes |
| `yugabytedb` | 2025.2 | `YugabyteDB25` | banner-substring | `yugabytedb/yugabyte:2025.2` | yes | yes |
| `clickhouse` | 26.7 | `ClickHouse24` | dialect-default | `clickhouse/clickhouse-server:26.7` | yes | no: the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing |
| `clickhouse` | 26.3 | `ClickHouse24` | dialect-default | `clickhouse/clickhouse-server:26.3` | yes | no: the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing |
| `clickhouse` | 25.8 | `ClickHouse24` | dialect-default | `clickhouse/clickhouse-server:25.8` | yes | no: the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing |
| `clickhouse` | 24.10 | `ClickHouse24` | dialect-default | `clickhouse/clickhouse-server:24.10` | yes | no: the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing |
| `sqlserver` | 17.0 (SQL Server 2025) | `SQLServer2022` | dialect-default | `mcr.microsoft.com/mssql/server:2025-latest` | no | no: the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing |
| `sqlserver` | 16.0 (SQL Server 2022) | `SQLServer2022` | dialect-default | `mcr.microsoft.com/mssql/server:2022-latest` | no | no: the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing |
| `sqlserver` | 15.0 (SQL Server 2019) | `SQLServer2022` | dialect-default | `mcr.microsoft.com/mssql/server:2019-latest` | no | no: the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing |
| `sqlite` | 3 | `SQLite3` | dialect-default | none | n/a | no: no container image is declared for this line; the capability probe has no statement table for the sqlite dialect, so a server on this line would be asked nothing |
| `spanner` | 0 | `SpannerPostgres` | banner-substring | none | n/a | no: no container image is declared for this line |
<!-- END GENERATED VERSION MATRIX -->

Which versions are supported is the vendors' answer, not Ptah's, and the
sources are recorded next to each block of cells in
`internal/capabilityprobe/cells.go`. PostgreSQL does not label releases LTS: it
ships major versions with a five-year window and minor releases inside them, so
the reading here is the newest patch of each still-supported major line. The
`postgres` 13 cell is past that window and exists only because Ptah still ships
a `Postgres13` preset, and a preset with no cell is a claim nothing here can
measure.

Every currently runnable line names a measured preset. A newly declared line
without one fails the `preset coverage` job instead of borrowing the dialect
default and turning green through saturation; stokaro/ptah#916 tracks the
remaining resolver refinement work.

## Current consumers

- **Foreign key rendering.** Schema rendering validates every foreign key
  before producing statements, including owner/target tables, local and
  referenced columns, compatible column types, duplicate columns and names,
  referential actions, and the target's referenced-key policy. Targets other
  than SQLite create tables first and add foreign keys in a second phase, so
  circular and composite cycles are executable. SQLite keeps constraints
  inline and accepts only candidate keys whose collation semantics are present
  in the schema IR. MySQL before 8.4 and MariaDB accept a full leftmost BTREE
  index prefix on InnoDB tables; FULLTEXT, SPATIAL, HASH, parser-backed, prefix,
  and expression indexes do not qualify. The portable MySQL-family path also
  emits `ENGINE=InnoDB` explicitly for every table participating in a foreign
  key when no engine was declared, so the session default cannot silently
  disable referential integrity. It
  rejects MariaDB generated FK columns, MySQL virtual generated FK columns,
  invalid actions on MySQL stored generated columns, and mismatched signedness,
  character sets, or collations. `SET NULL` requires every affected local
  column to be nullable. Explicit foreign-key names must fit the target's
  identifier limit: 63 bytes for the PostgreSQL family, 64 characters for the
  MySQL family, and 128 characters for SQL Server and Spanner. Generated names
  are shortened deterministically before collision checks. Standards-oriented targets require a declared candidate key;
  Spanner creates its own backing index. A disabled `foreign_keys` capability
  or any invalid constraint fails the complete render instead of omitting the
  constraint or returning partial DDL. SQL Server also rejects cycles and
  multiple paths for cascading actions before SQL emission.
- **Constraint drops (MySQL family).** The MariaDB-preset planner requests
  `IF EXISTS` on `DROP CONSTRAINT` / `DROP FOREIGN KEY`; the mariadb renderer
  honors it, the mysql renderer strips it. On MySQL the exactly-once drop
  ownership from #207 remains the only idempotency mechanism — the guard is
  belt-and-braces on MariaDB, never a substitute.
- **`DROP CHECK` spelling.** A planner whose target lacks
  `drop_constraint_generic` (MySQL 8.0.16–8.0.18) requests
  `ALTER TABLE … DROP CHECK <name>` for CHECK removals; the renderer resolves
  the spelling against **its** target too, so the request degrades to the
  generic clause on MariaDB, which has no `DROP CHECK` at all (verified live).
  A CHECK removal with no valid spelling at all (`MySQLLegacy`) degrades to a
  loud WARNING comment.
- **UNIQUE removals use `DROP INDEX`** (#195). Every MySQL-family preset
  renders `ALTER TABLE … DROP INDEX <name>` for a UNIQUE constraint removal —
  the one spelling valid on every version (verified live on MySQL 9.7 and
  MariaDB 10.11), unlike the generic clause (8.0.19+ only). MariaDB guards it
  with `IF EXISTS` (also verified live, idempotent on absent indexes); the
  mysql renderer strips the guard.
- **CHECK adds on non-enforcing targets.** A target without
  `check_constraints_enforced` gets a loud `WARNING` comment instead of an
  `ADD CONSTRAINT … CHECK` the server would silently ignore. This covers the
  ALTER-time constraint paths (table-level and synthesized field-level);
  column-level `CHECK` clauses inside `CREATE TABLE` / `ADD COLUMN` remain
  emitted — they are valid, parsed-and-ignored syntax on such targets, exactly
  MySQL's own historical behavior.
- **`DROP INDEX` guard.** Intent is planner-side and capability-gated (the
  MariaDB preset requests it, the MySQL preset does not), and the renderer
  validates it again — so the capability is a real knob on both layers.
- **`CREATE INDEX CONCURRENTLY` (postgres).**
  `postgres.New().WithConcurrentIndexes()` emits `CONCURRENTLY` for new
  indexes **only** when the capability is present. It is a policy opt-in
  because concurrent builds cannot run inside a transaction block. The
  high-level migration generator uses the same capability to emit
  `CREATE INDEX CONCURRENTLY` plus `-- +ptah no_transaction` for new indexes on
  populated existing PostgreSQL tables; a capability-less target
  (CockroachDB-style preset, #171) keeps plain `CREATE INDEX` regardless of
  policy.
- **Distributed-SQL PostgreSQL-family adapters (#171).**
  `platform.CockroachDB`, `platform.YugabyteDB`, and `platform.Spanner`
  normalize as distinct dialects but reuse the PostgreSQL planner, renderer,
  reader, and writer with capability presets:
  CockroachDB disables `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`,
  `XML`, and advisory locks; live CockroachDB v26.2.5 accepts roles, grants,
  row-level security, standalone sequences, and `SERIAL` columns. YugabyteDB
  enables `CREATE INDEX CONCURRENTLY`, roles, grants, row-level security,
  standalone sequences, `XML`, and advisory locks on the measured 2026.1 line;
  only `DROP INDEX CONCURRENTLY` remains disabled. Bidirectional generation
  therefore keeps a concurrent YugabyteDB create and uses ordinary `DROP INDEX`
  for its rollback; only the forward file requires no-transaction execution.
  Spanner supports foreign
  keys, including circular and composite relationships, while disabling enums,
  sequences, RLS, XML, advisory locks, and concurrent indexes. Spanner accepts
  only `NO ACTION` and `CASCADE` for `ON DELETE`; any `ON UPDATE` action fails
  before rendering.
  CockroachDB and YugabyteDB integration coverage uses opt-in common-subset
  scenarios that run against live OSS containers in CI. The distributed-SQL
  reader gate also seeds table, index, view, materialized view, sequence, and
  row-level security policy objects and then exercises `ptah db read`,
  `ptah-compat schema inspect`, and the shared pgx reader so one broken catalog
  query cannot be hidden by a later broad integration step. Spanner currently
  has capability, planning,
  rendering, URL, and detection coverage only; there is no OSS Spanner
  PostgreSQL-interface container in the integration suite.
- **Object kinds across the PostgreSQL family (#929).** One planner and one
  renderer serve PostgreSQL, CockroachDB, YugabyteDB, and Spanner, so the
  `views`, `materialized_views`, `functions`, and `triggers` keys are what lets
  one member of that family refuse an object kind. Spanner disables the last
  three: a Spanner view does not store its query result, and Spanner does not
  run user code in the database. A refused object is not dropped in silence —
  the renderer writes
  `-- SPANNER: trigger users_touch is not supported by this target; skipped.`
  in its place, and because `schema render` and the apply planner both pass
  through that renderer, the two commands say the same thing about the same
  object. Every row of these four keys was measured against a live server of
  that engine except Spanner's, which has no container and no live test (#942)
  and follows Google's documentation.
- **One answer shape for every refused object kind (#929).** `sequences`,
  `role_management`, and `row_level_security` answer the same question as the
  four keys above and use the same named-skip path when a preset disables them.
  Within the PostgreSQL family that currently matters for Spanner: a role,
  grant, sequence, row-level security enablement, or policy is written as a
  named `-- SPANNER: ... skipped.` diagnostic instead of being dropped from a
  plan in silence. CockroachDB v26.2.5 and YugabyteDB 2026.1 no longer use that
  refusal path for these three categories because the measured servers accept
  them. Printed plans keep the diagnostic; the apply execution path drops
  comment-only statements before target or dev-database execution.
- **SQLite native DDL (#148).**
  `SQLite3()` enables enforced CHECK constraints, foreign keys, and
  `DROP INDEX IF EXISTS`. It deliberately leaves generic constraint drops and
  CHECK-drop spelling disabled because SQLite cannot add, drop, or modify
  table constraints in place; those operations require a table rebuild plan.
  The SQLite planner emits native `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN`,
  conservative simple column-drop rebuilds, indexes, views, triggers, drops,
  and explicit rebuild-required errors for unsupported structural changes.
- **Trigger replacement (#158).** Planners mark modified triggers as replacement
  intent. Renderers emit `CREATE OR REPLACE TRIGGER` only when
  `create_or_replace_trigger` is present. PostgreSQL 14+ and MariaDB use
  `CREATE OR REPLACE TRIGGER`; SQL Server uses `CREATE OR ALTER TRIGGER`.
  Targets without it use an explicit drop/create sequence.
- **SQL Server subset (#149).**
  `SQLServer2022()` enables generic constraint drops, enforced CHECK
  constraints, foreign keys, XML, and single-statement trigger replacement. It
  leaves enum and sequence capabilities disabled because Ptah models SQL Server
  enums as `NVARCHAR(255)` plus `CHECK` constraints and does not yet expose
  standalone SQL Server sequence objects. Raw view and trigger definitions can
  be rendered, but SQL Server catalog readback is not yet normalized enough for
  full drift-safe round trips. `DROP INDEX IF EXISTS` and `DROP CONSTRAINT IF
  EXISTS` are disabled in the portable preset, so plans should remain exactly
  scoped instead of relying on guards.

## Follow-ups

- Spanner remains lowest priority: the preset exists so callers get explicit
  routing and conservative rendering, but full Spanner-specific DDL such as
  interleaved tables is outside the PostgreSQL-family adapter.
