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
- **Direct migration consumption** — `ptah migrations up`, `status`, and `down` accept `oci://` through `--migrations-dir`. `--verify-sum` on those three verifies the *pulled* directory against the sum that traveled inside the artifact, so over a movable tag it proves internal consistency only, and each of the three prints the resolved digest together with the `@sha256:` reference that pins those exact bytes. Pinning that digest fixes which bytes a later pull gets; it does not establish who published them — see [Identity, integrity, and authenticity](./oci_registry.md#identity-integrity-and-authenticity) for the controls that do.
- **Integrity before publication** — `--verify-sum` on `ptah migrations push` requires the *local* directory to carry a sum and to match it before the upload. It is the same requirement on a different subject, and it publishes rather than consumes: the output reports the tag it pushed and the resulting digest as separate fields, and constructs no pinned reference.
- **Direct schema consumption** — `ptah schema render`, `export`, `inspect`, `compare`, `drift`, `plan`, `apply` and `push`, and `ptah migrations plan` and `generate`, accept `oci://` through `--schema-file`, and every one of them exposes `--plain-http`.
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
| `postgres_catalog_functions` | pg_catalog's introspection helpers resolve: `obj_description`, `format_type`, `pg_get_expr`, `pg_get_constraintdef` |
| `catalog_row_statistics` | the catalog exposes planner row-count statistics (`pg_stat_all_tables`) |
| `catalog_dependencies` | the catalog exposes `pg_depend`, the dependency table the user-defined-type read joins |
| `alter_generated_column_expression` | In-place `ALTER COLUMN SET EXPRESSION` for generated columns (PostgreSQL 17+) |
| `row_level_security` | Row-level security policies (PostgreSQL) |
| `role_management` | Named roles plus `GRANT`/`REVOKE` of object privileges. The PostgreSQL family spells it `CREATE`/`ALTER ROLE` with attributes; ClickHouse spells it `CREATE`/`DROP ROLE` with none, over database- and table-scoped grants. The key promises a round trip, not a vocabulary |
| `foreign_keys` | Declarative `FOREIGN KEY` constraints |
| `foreign_keys_require_unique_reference` | Foreign keys require a declared primary or unique referenced key (MySQL 8.4+ default). Requires `foreign_keys` |
| `foreign_keys_require_indexed_reference` | Foreign keys may reference a complete leftmost index prefix (MySQL before 8.4 and MariaDB). Requires `foreign_keys` |
| `foreign_keys_create_backing_index` | The database creates the foreign key's backing index (Spanner). Requires `foreign_keys` |
| `sequences` | Database sequence objects: `SERIAL`/`BIGSERIAL` column backing and first-class standalone sequences via `//ptah:schema:sequence` (`CREATE`/`ALTER`/`DROP SEQUENCE`). See [Sequences](./sequences.md). |
| `xml_type` | PostgreSQL `XML` column type |
| `advisory_locks` | PostgreSQL advisory lock functions such as `pg_advisory_lock` |
| `row_level_ttl` | Table storage parameters declaring a row-expiry policy the engine runs (CockroachDB row-level TTL). The one key that is true on a PostgreSQL-compatible engine and false on PostgreSQL itself |

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

### Values that are not booleans

Three capability values have no yes-or-no answer. `core/platform/capability/traits.go`
keeps them as a `Traits` value instead of flattening them into more flags, and
`capability.TraitsFor(dialect, caps)` resolves all three for a target.

| Value | Answers | Range |
|---|---|---|
| `identifier_limit` | The longest identifier the target accepts | A number with the unit it counts, or no modeled limit |
| `enum_modeling` | How the target spells an enumerated column type | `inline`, `named-type`, `unsupported` |
| `foreign_key_reference` | What the target requires of the columns a foreign key points at | `unique`, `indexed`, `backing-index`, `unsupported` |

The identifier limit is the value a boolean cannot carry, because the unit is
half the answer: 63 **bytes** for the PostgreSQL family (`postgres`,
`cockroachdb`, `yugabytedb`), 64 characters for MySQL and MariaDB, 128
characters for SQL Server and Spanner, and no modeled limit for ClickHouse and
SQLite. PostgreSQL truncates at 63 bytes, so a 32-character accented name is
already over the limit while its rune count says it fits. As flags the limit
would need one key per length, and the unit would have nowhere to live at all.
`IdentifierLimit.Exceeds` is where the byte-versus-character rule is **defined**,
so a caller can ask the question instead of enforcing the limit by rune count.

Those numbers were a dialect switch inside `core/renderer`'s foreign-key name
validation. The renderer now reads the limit from the capability model, which is
what makes the model load-bearing rather than decorative: it asks the question
instead of carrying its own copy of the switch.

`Exceeds` is not yet the only place the rule is **applied**. Two copies existed
when the model was introduced. `core/renderer` and
`dbschema.validateSQLServerIdentifierNames` now both consume it, and one
remains: `internal/convert/fromschema` (`foreignKeyNameFits`,
`foreignKeyNameWithSuffix`) keeps a three-arm switch because it *truncates* a
generated name to fit rather than refusing it, and the truncation needs a budget
in the limit's unit — something `IdentifierLimit` does not expose today.

Its predicate agrees with `capability.Identifiers`: compared against it over
nine dialects and 16 name shapes chosen to straddle every boundary, 144
verdicts, zero disagreements. So the cost of the remaining copy is drift, not a
wrong answer today. Giving `IdentifierLimit` unit-aware truncation is what would
retire it; until then, a further caller should ask `Exceeds` rather than add a
copy.

The other two values are read off the boolean set rather than declared beside
it. `enum_inline_column` against `enum_custom_type`, and the three referenced-key
policy keys, are already mutually exclusive groups that `Validate()` polices —
the enum pair under an at-most-one rule, the policy keys under a stricter
exactly-one rule that fires whenever foreign keys are supported. Either way it is
a mode wearing two or three booleans, and reading it as one adds no claim: the
same preset produces the same answer, spelled as what it always meant. `unsupported` is a real answer in both — SQLite models enums
neither way, and ClickHouse has no declarative foreign keys — and differs from a
set that names no mode at all, which only a hand-built set produces and
`Validate()` rejects.

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
| `functions` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `triggers` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| `create_or_replace_trigger` | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ |
| `postgres_catalog_functions` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `catalog_row_statistics` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `catalog_dependencies` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `alter_generated_column_expression` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `row_level_security` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `role_management` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `foreign_keys` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `foreign_keys_require_unique_reference` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| `foreign_keys_require_indexed_reference` | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `foreign_keys_create_backing_index` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `sequences` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `xml_type` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ |
| `advisory_locks` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| `row_level_ttl` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |

Version lines: `MySQL84()` covers MySQL 8.4+, 9.x, and the 26.x generation;
the exact measured lines are 8.4, 9.7, and 26.7. `MySQL8019()` covers
8.0.19–8.3; `MySQL8016()` covers 8.0.16–8.0.18; `MySQLLegacy()` anything
older. `MariaDB1011()` covers the
supported MariaDB lines (10.6+ through 12.x); the exact measured lines are
10.11, 11.4, 11.8, and 12.3. `MariaDBLegacy()` is the
conservative floor `ForServerVersion` assigns to pre-10.2 servers.
`Postgres17()` covers PostgreSQL 17–18; `Postgres16()` covers 14–16;
`Postgres13()` covers 12–13 (no
`CREATE OR REPLACE TRIGGER` and no `SPGIST` indexes with `INCLUDE` columns).

A preset that covers a newer line than the one it is named after is making a
claim about a server, so the evidence for those claims is kept in the tree
rather than in a commit message.
`core/platform/capability/capability_measured_lines_test.go` holds PostgreSQL
18, MySQL 26 and MariaDB 12 against the servers they were read from, one row
per registry key, recording the statement that decided each key and the
server's verdict. It also separates keys the probe actually asked about from
keys carried over from the line below, because a carried row is not a
measurement of the newer line.

Each entry names the probe run's per-cell artifact. GitHub Actions retains
those artifacts for seven days, so a reader can fetch the supporting transcript
only during that window. The checked-in statement and verdict are the durable
record; rerun the capability matrix to produce a fresh transcript after the
artifact expires.

`CockroachDB25()` and `CockroachDB26()` are the measured CockroachDB release
arms. The 25.x arm disables generic and guarded `DROP CONSTRAINT` plus
`CREATE OR REPLACE TRIGGER`; the 26.x arm supports them. `CockroachDB23()`
remains the conservative historical arm. Both spellings of a CockroachDB
version reach that ladder: the banner `CockroachDB CCL v25.4.5` and the dotted
`25.4.5` resolve identically. The dotted form used to fall through to
`ForDialect("cockroachdb")` — the 26.x arm — and report itself as a dialect with
no measured ladder. `YugabyteDB25()` is the current
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

`ResolveServerVersion` separates saturation from the other fallbacks — a
version that could not be parsed and an unmeasured major/minor gap below the
ceiling — and names the newest line the resolver has measured:

```go
resolution := capability.ResolveServerVersion("mysql", "99.0")
// resolution.Capabilities    == capability.MySQL84()
// resolution.VersionSpecific == false   (the dialect default was used)
// resolution.Saturated       == true    (99.x is past the newest measured line)
// resolution.NewestMeasured  == "26.7"
// resolution.Recognized      == true    (99.0 is a version, just an unmeasured one)
```

`Saturated` and `VersionSpecific` are never both true.

The newest measured line per refined dialect:

| Dialect | Newest measured line | Saturates above |
| --- | --- | --- |
| MySQL | 26.7 (`MySQL84()`) | 26.7 |
| MariaDB | 12.3 (`MariaDB1011()`) | 12.3 |
| PostgreSQL | 18.x (`Postgres17()`) | 18 |
| CockroachDB | 26.2 (`CockroachDB26()`) | 26.2 |

The matrix measured PostgreSQL 18.4, MySQL 26.7.0, MariaDB 12.3.2, and both
CockroachDB 25.4 and 26.2 before promoting those lines. MySQL, MariaDB, and
CockroachDB match exact major/minor lines: an unmeasured sibling such as MySQL
26.6, MariaDB 12.2, or CockroachDB 26.1 is not reported as a line-specific
match. A sibling above the newest measurement saturates. The resolver uses a
conservative preset between measured lines.

Raising one of those numbers is the deliberate act of claiming a newer server
line behaves like the preset it lands on. Do it in the change that measures
that line — never as a side effect of bumping a container tag.

Saturation is only defined where this package has a version ladder. SQL Server
has no ladder at all, while YugabyteDB and Spanner are resolved from the banner
without consulting a version; those three report `Saturated=false` and an empty
`NewestMeasured`. SQLite has a ladder but no measured line, so it reports
`VersionSpecific=true` with an empty `NewestMeasured` — a version DID select an
arm, and the matrix declares one SQLite cell with no container behind it.
Refining the remaining dialects is the open scope of issue #916.

SQLite's ladder is one step, at 3.25, and it carries one key.
`ALTER TABLE ... RENAME COLUMN` arrived in that release; below it a rename is
the create-new/copy-rows/drop-old rebuild, which is a different plan than the
statement describes. An unreadable version takes the LOWER arm, because the
conservative direction for a rendered file is the one whose statements the
older engine also accepts.

SQL Server keeps its absence of a ladder as a measured result rather than an
omission. Every registered key with a T-SQL shape was asked of the three
release lines Microsoft supports — 15.0.4480.2, 16.0.4265.3 and 17.0.4075.5 —
and all three answered identically on all of them, so an arm would select the
same preset from every version and refine nothing. Two of those answers
corrected the preset: both `IF EXISTS` guards are accepted on every line and
had read `false` from the PostgreSQL and MySQL answers rather than from a SQL
Server statement.

ClickHouse gained one in that issue, and it is one step long. Measured on the
two declared lines furthest apart, `CHECK GRANT SHOW DATABASES, SHOW TABLES ON
*.*` answers `1` on 26.7.3.19 and is a syntax error on 24.10.4.191; every other
registered key answers identically on both. So the arm at 24.11 adds exactly
`check_grant_statement` and changes nothing else — a ladder is worth having when
a key differs across it, and worth being honest about when only one does.

`dbschema.ConnectToDatabase` records a saturated resolution at `DEBUG`, naming
the dialect, the server version, and the line it was planned as; an unparseable
version is recorded at `DEBUG` too. Neither reaches a default run's stderr, and
that is deliberate: the CLI's default logger keeps `WARN` and above so that a
clean run emits nothing, and connecting to a supported server is a clean run.
The first connection after a vendor publishes an unmeasured major can saturate,
so a warning would be noise on every command against that server rather than a
diagnostic. Use `--log-level debug` to see it, or read `Saturated` and
`NewestMeasured` from `ResolveServerVersion` directly.

### Recognized: a string that named no server

Silent degradation is correct for a banner read from a live `SELECT version()`
— a server does not typo its own name — and wrong for a string a person typed.
`Recognized` is the field that tells the two apart, and it exists because the
other three cannot:

```go
capability.ResolveServerVersion("postgres", "not-a-version")
// VersionSpecific == false, Saturated == false, NewestMeasured == ""
capability.ResolveServerVersion("sqlite", "3.53.0")
// VersionSpecific == false, Saturated == false, NewestMeasured == ""
```

Those two agree in every field published before `Recognized`, and they are
opposite answers to the only question an operator's input raises: the first
string was ignored, the second is a perfectly good version for a dialect that
has no ladder to spend it on. `Recognized` is `false` only for the first.

`Recognized == false` implies `VersionSpecific == false`, so a caller holding
operator input reads this field alone and refuses. A caller holding a live
banner should keep ignoring it.

`ptah sql lint`, `ptah schema render` and `ptah schema diff` are those callers. All three refuse an
unrecognized value and never report it as the version they planned against; a
recognized value that did not select an exact measured release line is applied
and announced, on stderr as a warning line and — on `sql lint --format json` —
as `version_note`. Silence is reserved for an exact measured-line match. This is criterion 6 of issue #916
for those commands; every surface that reads a live `SELECT version()` banner
still degrades silently by design, because a server does not typo its own name.

`schema diff` differs from the other two in where its dialect comes from: it
has no `--dialect`, taking the dialect from `--dev-url` or from a source URL, so
the version is resolved inside the diff rather than at the flag. The refusal and
the warning are the same, and they still arrive before a migration-directory
source is replayed into the dev database.

All three also refuse a version that names a **different server product** than
the dialect it was supplied with. A live connection resolves that contradiction in
favor of the string — MariaDB announces itself over the MySQL protocol and
CockroachDB over the PostgreSQL one — and between two values a person typed it
is a silent contradiction instead: `--dialect mysql --server-version
10.11.6-MariaDB` rendered MySQL DDL against MariaDB capabilities at exit `0`
where the same command without a version exited `2`. `capability.BannerPlatform`
answers which product a string names, which is what makes the contradiction
observable at all; `internal/servertarget` reads it and refuses. A banner naming
the dialect it was given with still resolves.

`BannerPlatform` is the only ordered table of product tokens in the tree —
`ResolveServerVersion` dispatches on it and `dbschema`'s wire-dialect detection
reads it, because a second copy is how a live connection and an offline
resolution come to disagree about which server a banner describes. Within the
PostgreSQL wire family the order is load-bearing: PostgreSQL is detected after
CockroachDB, YugabyteDB and Spanner, because all three contain the word —
CockroachDB speaks the PostgreSQL wire protocol, YugabyteDB reports
`PostgreSQL 11.2-YB-…` and Spanner `Cloud Spanner PostgreSQL`. Checking
PostgreSQL first would plan every one of those engines as PostgreSQL.

A product belongs in the table when the **server's own version surface names
it**, because that string is the only evidence the refusal can act on. Seven of
the nine dialects qualify:

| dialect | surface that names the product |
| --- | --- |
| `postgres` | `SELECT version()` — `PostgreSQL 16.3 (Debian …)` |
| `mariadb` | `SELECT VERSION()` — `10.11.6-MariaDB-…`, including the `5.5.5-` replication prefix |
| `cockroachdb` | `SELECT version()` — `CockroachDB CCL v25.4.5 …` |
| `yugabytedb` | `SELECT version()` — `PostgreSQL 15.12-YB-…` |
| `spanner` | `SELECT version()` — `Cloud Spanner PostgreSQL …` |
| `sqlserver` | `@@VERSION` — `Microsoft SQL Server 2025 (RTM-CU7) … - 17.0.4065.4 …` |
| `clickhouse` | `system.build_options` `VERSION_FULL` — `ClickHouse 26.7.3.19` |

`mysql` and `sqlite` are deliberately absent. Measured, a live `mysql:9.7`
answers `SELECT VERSION()` with `9.7.2`, and `sqlite_version()` answers a bare
dotted version. Neither server names its own product anywhere a version string
is read, so `BannerPlatform` correctly returns `""` for both. A token would have
to come from a client banner instead, and the MySQL client's is shared with
MariaDB's (`mysql  Ver 15.1 Distrib 10.11.6-MariaDB`), so it names no server.

SQL Server is the entry with teeth. `@@VERSION` opens with the marketing year,
so a resolver that reads the first number out of it reads `2025`; before the
token existed, `ptah schema render --dialect postgres --server-version '<that
banner>'` exited `0` and announced that it had planned a PostgreSQL saturated
past release line `18.x`. SQL Server has no version ladder, so
`ResolveServerVersion` answers that banner from the product alone and never
spends the number in it.

A banner naming **only** PostgreSQL names the family and not the product, so it
does not displace a declared dialect already in that family. CockroachDB,
YugabyteDB and Spanner all speak this wire protocol, and a deployment of any of
them may report a banner carrying no token of its own — Cloud Spanner's
PostgreSQL interface answers `SELECT version()` with exactly `PostgreSQL 14.1`,
measured live against the Cloud Spanner emulator behind PGAdapter 0.55.2.

`dbschema.getDatabaseInfo` answers that case by keeping the dialect the operator
connected with, and `ResolveServerVersion` keeps that dialect's preset for the
same reason: claiming the banner there replaced `SpannerPostgres()` with a
PostgreSQL line across 19 keys, among them `materialized_views`, `functions` and
`triggers`, which exist to stop Ptah emitting DDL Spanner refuses. The two
readers are held to one answer by
`dbschema.TestWireDialectDetectionAgreesWithTheCapabilityResolver`.

The offline flags are stricter than the live path here, deliberately: a
PostgreSQL banner typed alongside `--dialect cockroachdb` is two values naming
two servers with nothing to prefer between them, so `internal/servertarget`
refuses it even though a live CockroachDB reporting the same banner keeps its
own preset.

All three spell it `--server-version`, and none of them spells it `--version`:
on a CLI that is conventionally the program's own version, and `ptah --version`
prints one. `cmd/internal/serverversion` registers the flag and marks it with one
annotation, so `cmd/root`'s flag-surface walk can tell it from the two
`--version` flags on the same command tree that mean something else entirely —
`migrations checkpoint --version` names a checkpoint and `schema push
--version` names an artifact tag. The annotation is what the walk reads, so a
command counts because it opted into the contract and never because of how the
flag is spelled.

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
- `capability.ResolveServerVersion("mysql", version)` — the full answer:
  the preset plus `VersionSpecific`, `Saturated`, `NewestMeasured`,
  `Recognized`, and `ResolvedDialect`. Callers acting on a version string a
  person supplied must use this and refuse `Recognized == false`;
  `ForServerVersion` throws that signal away and cannot report it. Do not key
  the product-contradiction refusal on `ResolvedDialect`: it names the ladder
  the preset came from, not the product the string names, and the two disagree
  for exactly the case the commands refuse. Measured,
  `ResolveServerVersion("cockroachdb", "PostgreSQL 16.3")` reports
  `ResolvedDialect == "cockroachdb"` — correctly, because a live CockroachDB
  may report that banner and must keep its own preset — while
  `ptah schema render --dialect cockroachdb --server-version 'PostgreSQL 16.3'`
  exits `2`. YugabyteDB and Spanner behave the same way. A caller comparing
  `ResolvedDialect` therefore accepts input the commands reject.
- `capability.BannerPlatform(version)` — which product a version string names,
  or `""` when it names none. This is the question operator input raises:
  compare it with `platform.NormalizeDialect(dialect)` and refuse when it is
  non-empty and different from that. Both halves carry weight. The empty answer
  is not a mismatch — `"8.0.42"` names a version and no product, and
  `--dialect mysql --server-version 8.0.42` exits `0`. And the comparison is
  against the normalized name, because no alias ever appears in the answer:
  `BannerPlatform("CockroachDB CCL v25.4.0")` returns `cockroachdb`, which a
  check against a raw `crdb` would refuse even though
  `--dialect crdb --server-version 'CockroachDB CCL v25.4.0'` exits `0`.
  `internal/servertarget.Resolve` is those two refusals, and it is what both
  `ptah sql lint`, `ptah schema render` and `ptah schema diff` call.
- `capability.ForServerVersion("mysql", version)` — refine using a live
  `SELECT version()` string. Recognizes shapes like `8.0.42-log`,
  `10.11.6-MariaDB-…`, the `5.5.5-10.11.6-MariaDB` replication-protocol prefix
  (MariaDB over the mysql driver resolves to the MariaDB preset), and
  `PostgreSQL 16.3 (…)`. PostgreSQL-wire banners containing `CockroachDB`,
  `YugabyteDB`/`Yugabyte`, or `Spanner` resolve to their distributed-SQL
  presets, and a `Microsoft SQL Server …` or `ClickHouse …` banner resolves to
  that product's default preset — neither has a version ladder, so the number
  in the banner is never spent. Whenever the resolution reports
  `Recognized == false`, the preset it carries is the default of the dialect
  named by `ResolvedDialect`, never some other engine's.
  `dbschema.ConnectToDatabase` stores this resolved set in
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

`ptah schema render --server-version` and `ptah schema diff --server-version`
are the command-line spellings of that pin, and the default they correct is not
a theoretical one.

On `schema diff` the sharpest case is a changed generated-column expression.
`ALTER COLUMN … SET EXPRESSION` arrived in PostgreSQL 17, and every version from
14 is still supported, so an unpinned diff emits

```sql
ALTER TABLE "t" ALTER COLUMN "b" SET EXPRESSION AS (a * 3);
```

which a PostgreSQL 16 server rejects outright. `--server-version 16` plans the
answer that server can act on instead:

```sql
-- WARNING: Generated column t.b changed, but ALTER COLUMN SET EXPRESSION requires target capability alter_generated_column_expression, unavailable on this target (PostgreSQL added it in 17); manual migration required. --;
```

The refusal names the capability, not the version, because the version is only
one of the ways to reach it: a PostgreSQL-compatible engine, a managed provider
that withholds the statement, or a preset composed with
`.With(capability.AlterGeneratedColumnExpression, false)` all land on the same
plan while reporting a version number that explains nothing. The release that
added the statement stays in the sentence as the reason the capability is
absent here.

`--server-version 17` still emits the `ALTER`, so pinning selects a plan rather
than degrading every one. `ForDialect("mysql")`
answers `MySQL84()`, which sets `foreign_keys_require_unique_reference`, so a
foreign key onto a plain-indexed column is refused at exit `2` while MySQL 8.0
— and `--dialect mariadb`, at exit `0` — accept it. Passing
`--server-version 8.0.42` selects `MySQL8019()` and the same schema renders;
`--server-version 8.4.0` still refuses it, which is the correct answer for that
server. The flag requires `--dialect`, because without one the command renders
every supported target in a single pass and one server version does not
describe nine engines.

## Declared release lines

This is the version matrix, and it is the declared set: the line, the capability
preset it claims, how much testing stands behind that claim, and whether the
capability probe measures it against a live server. Declared is not the same as
usable. A release line absent from the table still connects, still resolves
capabilities, and still performs the operations those capabilities allow.

It is generated from `internal/capabilityprobe/cells.go`, which is the only
place a release line is declared. The tiered pipeline of stokaro/ptah#1341
reads the same declaration, so the workflow files carry no list of versions and
`scripts/check-version-matrix.sh` fails the build when this table drifts from
the declaration it was generated from.

Four columns need reading carefully.

**Support** is how much testing stands behind the line — `certified`,
`legacy-tested`, `best-effort`, or `known-incompatible` — and it claims
something about this repository's continuous integration rather than about the
server in front of you. [Support is a separate question from
capability](#support-is-a-separate-question-from-capability), further down, says
what each level does and does not promise.

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
| Dialect | Release line | Support | Capability preset | Refinement | Container image | Tag names the line | Probed per pull request |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `postgres` | 18 | certified | `Postgres17` | version-ladder | `postgres:18` | yes | yes |
| `postgres` | 17 | certified | `Postgres17` | version-ladder | `postgres:17` | yes | yes |
| `postgres` | 16 | certified | `Postgres16` | version-ladder | `postgres:16` | yes | yes |
| `postgres` | 15 | certified | `Postgres16` | version-ladder | `postgres:15` | yes | yes |
| `postgres` | 14 | certified | `Postgres16` | version-ladder | `postgres:14` | yes | yes |
| `postgres` | 13 | legacy-tested | `Postgres13` | version-ladder | `postgres:13` | yes | yes |
| `mysql` | 26.7 | certified | `MySQL84` | version-ladder | `mysql:26.7` | yes | yes |
| `mysql` | 9.7 | certified | `MySQL84` | version-ladder | `mysql:9.7` | yes | yes |
| `mysql` | 8.4 | certified | `MySQL84` | version-ladder | `mysql:8.4` | yes | yes |
| `mariadb` | 12.3 | certified | `MariaDB1011` | version-ladder | `mariadb:12.3` | yes | yes |
| `mariadb` | 11.8 | certified | `MariaDB1011` | version-ladder | `mariadb:11.8` | yes | yes |
| `mariadb` | 11.4 | certified | `MariaDB1011` | version-ladder | `mariadb:11.4` | yes | yes |
| `mariadb` | 10.11 | certified | `MariaDB1011` | version-ladder | `mariadb:10.11` | yes | yes |
| `cockroachdb` | 26.2 | certified | `CockroachDB26` | version-ladder | `cockroachdb/cockroach:latest-v26.2` | yes | yes |
| `cockroachdb` | 25.4 | certified | `CockroachDB25` | version-ladder | `cockroachdb/cockroach:latest-v25.4` | yes | yes |
| `yugabytedb` | 2026.1 | certified | `YugabyteDB25` | measured-release-line | `yugabytedb/yugabyte:2026.1` | yes | yes |
| `yugabytedb` | 2025.2 | certified | `YugabyteDB25` | measured-release-line | `yugabytedb/yugabyte:2025.2` | yes | yes |
| `spanner` | 0 | best-effort | `SpannerPostgres` | banner-substring | `gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator:v0.55.2` | no | yes |
| `clickhouse` | 26.7 | certified | `ClickHouse2411` | version-ladder | `clickhouse/clickhouse-server:26.7` | yes | no: the clickhouse dialect has a probe plan and no launch recipe, so nothing here can start the server to run it |
| `clickhouse` | 26.3 | best-effort | `ClickHouse2411` | version-ladder | `clickhouse/clickhouse-server:26.3` | yes | no: the clickhouse dialect has a probe plan and no launch recipe, so nothing here can start the server to run it |
| `clickhouse` | 25.8 | best-effort | `ClickHouse2411` | version-ladder | `clickhouse/clickhouse-server:25.8` | yes | no: the clickhouse dialect has a probe plan and no launch recipe, so nothing here can start the server to run it |
| `clickhouse` | 24.10 | legacy-tested | `ClickHouse24` | version-ladder | `clickhouse/clickhouse-server:24.10` | yes | no: the clickhouse dialect has a probe plan and no launch recipe, so nothing here can start the server to run it |
| `sqlserver` | 17.0 (SQL Server 2025) | certified | `SQLServer2022` | dialect-default | `mcr.microsoft.com/mssql/server:2025-latest` | no | no: the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing |
| `sqlserver` | 16.0 (SQL Server 2022) | best-effort | `SQLServer2022` | dialect-default | `mcr.microsoft.com/mssql/server:2022-latest` | no | no: the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing |
| `sqlserver` | 15.0 (SQL Server 2019) | best-effort | `SQLServer2022` | dialect-default | `mcr.microsoft.com/mssql/server:2019-latest` | no | no: the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing |
| `sqlite` | 3 | certified | `SQLite3` | dialect-default | none | n/a | no: no container image is declared for this line; the capability probe has no statement table for the sqlite dialect, so a server on this line would be asked nothing |
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

### Support is a separate question from capability

The `Support` column states what this repository's continuous integration
covers. It is not a statement about the server in front of you, and upstream
end-of-life is not Ptah incompatibility: no code here consults a support level
to decide whether an operation may proceed. A line the vendor retires moves from
`certified` to `legacy-tested` and keeps working. A release line the table does
not declare at all resolves to `best-effort` at runtime — capabilities are
resolved for it and it is not refused.

Measured live on 2026-08-16: PostgreSQL 13.23, whose upstream final release was
2025-11-13, resolves to `legacy-tested`, has its capabilities resolved, and
works. MySQL 8.0.46, a line the matrix does not declare, resolves to
`best-effort`, works, and carries the note `mysql 8.0.46 is not a measured
release line; capabilities fall back to the preset its ladder assigns (newest
measured line: 26.7)`. A fourth level, `known-incompatible`, requires a concrete
technical incompatibility rather than a vendor's calendar date; no release line
carries it today. Each level is defined once, on the reader-facing
[Database support matrix](./site/src/content/docs/databases/support-matrix.md).

`ptah db capabilities --db-url <url>` reports the resolved profile of one live
server: the preset, how that preset was reached, the support level and release
line with the reason for it, the non-boolean values above, and every capability
key with its value there. It answers "why did Ptah do that against this server"
by asking the resolver documented on this page rather than a second copy of it,
and it executes no DDL and modifies no schema object. Connecting is still
connecting: a `sqlite://` URL naming a file that does not exist creates that
file, exactly as any other command reaching the same URL would.

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
- **Stored functions outside the PostgreSQL family (#929).** `functions` is the
  worked example of the rule the key's own doc comment states: it describes
  Ptah's generator, not the engine's brochure, so a preset may claim it only
  where a path emits, reads back **and** plans the object. All three of MySQL,
  MariaDB and SQL Server declared it while nothing implemented it, and
  `ptah schema render --dialect mysql` answered a declared function with
  `-- CREATE FUNCTION f1 not supported in MySQL` — a claim about the server that
  the server contradicts.

  MySQL and MariaDB now have all three parts. The renderer emits the engine's
  own spelling; the reader reads `information_schema.ROUTINES` and
  `information_schema.PARAMETERS`; and the MySQL-family planner gates on this
  key instead of listing functions as an object it cannot host.

  **One node renders one statement.** A modified function still needs the
  drop-then-create pair, because MySQL 26.7.0 refuses
  `CREATE OR REPLACE FUNCTION` with Error 1064, but the drop is a separate node
  the planner emits rather than a line the CREATE visitor prefixes. Putting both
  in one visitor put two statements in one element of
  `GetOrderedCreateStatements`, and the compatibility dev-database path passes
  each element to `ExecuteSQL` unchanged over a DSN that does not enable
  go-sql-driver's `multiStatements`; materializing any desired schema containing
  a function failed with Error 1064 on both engines. An *added* function gets no
  drop at all.

  **Volatility survives a read.** A characteristic is always emitted, because
  with binary logging on and `log_bin_trust_function_creators` off — the pinned
  image's own defaults — a function carrying none of `DETERMINISTIC`, `NO SQL`
  or `READS SQL DATA` is refused with Error 1418. Measured across all fifteen
  combinations of the two axes on MySQL 26.7.0, exactly six
  (`IS_DETERMINISTIC`, `SQL_DATA_ACCESS`) cells survive, and only two of them
  are `NOT DETERMINISTIC`: `NO SQL` and `READS SQL DATA`. So the three
  volatilities take three distinct cells — `IMMUTABLE` → `DETERMINISTIC`,
  `STABLE` → `NOT DETERMINISTIC NO SQL`, `VOLATILE` → `READS SQL DATA` — and the
  reader recovers the value from both columns. They used to share one clause, so
  a declared `STABLE` function read back as `VOLATILE` and planned the same
  destructive replacement on every apply, forever. `SQL_DATA_ACCESS` is advisory
  rather than enforced, which is what makes it usable as the encoding channel
  and is also its cost: a `STABLE` routine's catalog row says `NO SQL` whatever
  its body reads.

  **What cannot be represented is refused, not dropped.** A security mode that
  is neither `DEFINER` nor `INVOKER` used to render no clause at all, so the
  server applied its `DEFINER` default and every later comparison reported
  `security: DEFINER -> INVKOER` — an operator who asked for invoker rights got
  definer rights and a permanent diff. It is an error now, raised before the
  leading drop is planned. An unknown volatility is refused the same way, and so
  are two type spellings whose catalog form the declaration alone does not
  decide: `REAL` reads back as `double` or `float` depending on whether the
  connection's `sql_mode` includes `REAL_AS_FLOAT` (measured both ways on MySQL
  26.7.0, while `DOUBLE`, `DOUBLE PRECISION` and `FLOAT` are mode-independent),
  and the `NATIONAL`/`NCHAR`/`NVARCHAR` spellings report the SAME
  `DTD_IDENTIFIER` as the plain ones and differ only in `CHARACTER_SET_NAME`
  (`utf8mb3` against `utf8mb4`), a column this comparison does not read. A third
  is `ZEROFILL` written without a display width: the width is what `ZEROFILL`
  pads to, and both engines substitute their own default — `INT ZEROFILL` is
  reported as `int(10) unsigned zerofill` — which the declaration cannot
  predict. Written *with* a width it round-trips exactly, so that width is kept
  rather than stripped as an integer display width would be. All of these are
  refused rather than merely left out of the synonym table, because leaving a
  spelling unfolded keeps it on the desired side against a different catalog
  spelling — permanent drift, the failure this whole section is about.

  **A replacement's two halves travel together.** A modification is planned as
  DROP followed by CREATE, and the DROP is planned only when the CREATE will
  actually render DDL. When it was planned unconditionally, a desired
  declaration whose language this target cannot run produced an executable drop
  in front of a CREATE the renderer answered with a comment: `schema apply`
  deleted the live routine, created nothing, and reported success. Measured on
  MySQL 26.7.0 and MariaDB 12.3.2, zero rows in `information_schema.ROUTINES`
  afterwards. The shape needs no exotic schema — `Function.Canonicalize`
  defaults an omitted `language=` to `plpgsql`, so an ordinary annotation
  reaches it. The predicate the renderer and the planner share lives in one
  place so the two cannot drift apart again.

  **A foreign definer is not adopted silently.** MySQL and MariaDB execute a
  `SQL SECURITY DEFINER` routine as its catalog `DEFINER`, not necessarily as
  the account connected to Ptah. The reader captures both that owner and
  `CURRENT_USER()`. If a modified routine would be dropped and recreated by a
  different account, comparison refuses before any migration SQL is planned.
  Connect as the existing definer, declare `SQL SECURITY INVOKER` explicitly,
  or leave the foreign routine unchanged. Missing ownership facts also fail
  closed for a modified definer routine.

  What is generated also depends on the declared `language`, not on the target
  alone. MySQL and MariaDB run exactly one routine language, SQL, so a function
  declared `language="plpgsql"` is PostgreSQL procedural code that no envelope
  makes runnable there — `RETURNS VOID ... BEGIN PERFORM set_config(...); END;`
  is Error 1064 on MySQL 26.7.0 at the return type, before the body is parsed.
  Such a declaration gets a named skip comment. A function whose body the target
  can run still gets real DDL. That distinction is the point: skipping every
  function would be `-- CREATE FUNCTION f1 not supported in MySQL` in a new
  spelling, and would make this key vacuous again.

  It is a skip rather than a refusal because Ptah cannot yet say which targets a
  declared object belongs to. `//ptah:schema:function` accepts `name`, `params`,
  `returns`, `language`, `security`, `volatility`, `body` and `comment`, and
  `platform.<dialect>.<key>` overrides are granted to exactly three directives —
  field, embedded and table — none of which is a function. An unknown attribute
  is a parse error, so there is no spelling for "this object is PostgreSQL's".
  Refusing here would therefore make one schema applied across postgres, mysql
  and mariadb impossible, and the only alternative available today is `exclude`
  in `ptah.yaml`, which is an operator-side filter at invocation rather than a
  property of the declaration.

  The skip does say more than it used to. `Function.Canonicalize` defaults an
  unset language to `plpgsql`, so an annotation that omits `language=` lands on
  this branch as well and is passed over when it should have been generated —
  `schema apply` exits 0 having created nothing and the diff asks for the same
  function on every run. The renderer cannot tell that case apart from a
  deliberate `plpgsql` declaration, because both arrive as the same value, so
  the comment names both readings and the one word that settles it,
  `language="sql"`.

  **Routine identity is the engine's, not the table rules'.** Stored-routine
  names are case-insensitive on both engines — with `foo` in the catalog,
  `SELECT Foo(1)` resolves to it and `CREATE FUNCTION BAR` is Error 1304 while
  `bar` exists — and that is independent of the `TableNames` comparison the
  identifier semantics carry. Keying routines by exact spelling made live `foo`
  and desired `Foo` two objects: the diff carried an addition *and* a removal,
  and a successful apply left the database with no function at all. The
  declaration check uses that same identity, so two functions declared as `Foo`
  and `foo` are refused rather than silently reduced to one — while they were
  folded only for comparison and keyed exactly for duplicate detection, the two
  disagreed, and the disagreement discarded a declaration: measured on both
  engines, two declared functions produced one planned statement and one row in
  `information_schema.ROUTINES` after an apply that exited 0. The check is
  dialect-aware rather than part of the shared duplicate-definition validator,
  because PostgreSQL routine names *are* case-sensitive and both spellings are
  legitimate there. Routine
  types are likewise normalized on both sides, because the engines resolve
  synonyms themselves: a declared `INTEGER` is reported as `int`, and the two
  engines further disagree with each other about the legacy display width
  (`int` on MySQL, `int(11)` on MariaDB). Parameter rows are restricted to
  `ROUTINE_TYPE = 'FUNCTION'`, since a procedure of the same name shares the
  function's `SPECIFIC_NAME` and its arguments used to be appended to the
  function's signature.

  Stored-function DDL is never transactional on these engines. Measured on
  MySQL 26.7.0, `CREATE FUNCTION` inside `START TRANSACTION` survives a
  `ROLLBACK` — it commits implicitly — while an `INSERT` in the same shape rolls
  back. A generated function therefore cannot be grouped into a file-level
  transaction and have that transaction mean anything, which is what the
  migrator's transaction witness refuses.

  One MySQL deployment shape refuses stored functions regardless of what Ptah
  renders, and it is a **privilege** condition rather than a capability one.
  With binary logging enabled and `log_bin_trust_function_creators` off, MySQL
  applies two gates in sequence: a function declaring no characteristic is
  refused with Error 1418, and once it declares one, a connected user without
  the `SUPER` privilege is refused with Error 1419. The renderer answers the
  first; nothing it can emit answers the second. The remedy belongs to the
  operator — grant `SUPER` to the migrating user, run the migration as a user
  that holds it, or do not declare functions for that target — so Ptah replaces
  MySQL's own wording with a message that says exactly that instead of
  forwarding a server error code.

  `functions` stays `true` for the MySQL presets. The key answers "does a Ptah
  path emit, read back and plan this object", and all three exist and are
  proven by a live round trip; it does not answer "may the account you happen to
  be connected as run the statement". No capability key does — a `GRANT` can
  refuse a view or a table as readily — and the presets are resolved from
  the server version, not from the connected user's grants. MySQL with binary
  logging off, and MySQL with binary logging on and a provisioned migrating
  user, both create functions normally.

  Note that `log_bin_trust_function_creators` is the variable MySQL's own error
  text calls "the less safe" option, and measured on MySQL 26.7.0 it removes the
  characteristic gate as well: a function declaring nothing at all is accepted
  with it on. It is not a recommendation here, and Ptah's diagnostic does not
  offer it as one.

  SQL Server declares `false`. The engine hosts scalar functions and accepts
  both `CREATE FUNCTION` and `CREATE OR ALTER FUNCTION` on 2025 (RTM-CU7), but
  `sys.sql_modules.definition` returns the whole original `CREATE` statement as
  one string rather than a body plus attributes, so reading one back into the
  fields a diff compares needs a T-SQL routine-header parser that does not exist
  yet. Emitting without reading is the permanent diff this issue is about, so
  the key stays `false` and the renderer answers with the named skip
  `-- SQLSERVER: CREATE FUNCTION "f1" is not generated for this target; skipped.`
  — a sentence about Ptah, not about SQL Server.
- **One answer shape for every safely skippable object kind (#929).**
  `sequences`, `role_management`, and `row_level_security` use the same named
  skip when a PostgreSQL-family preset disables them. That currently matters
  for Spanner: a role, grant, sequence, row-level security enablement, or policy
  is written as a named `-- SPANNER: ... skipped.` diagnostic instead of being
  dropped from a plan in silence. CockroachDB v26.2.5 and YugabyteDB 2026.1 no
  longer use that refusal path for these three categories because the measured
  servers accept them. MySQL and MariaDB roles are deliberately different:
  complete-schema validation and rendering fail before SQL because Ptah cannot
  read or converge their role state, so a comment-only success would lose an
  authored security declaration. Printed plans keep safe skip diagnostics; the
  apply execution path drops comment-only statements before target or
  dev-database execution.
- **Every declared object reaches a renderer (#929 item 5).** Whether an object
  kind is converted at all is not decided by the dialect name. Every sequence,
  domain, composite type, range, role, function, view, materialized view,
  trigger, grant and row-level security declaration in a schema becomes an AST
  node for every target, and the target's renderer answers with a statement, a
  supported equivalent, a named skip, or a fail-closed error when continuing
  would lose state. Deleting the node before rendering was the previous
  behavior and it left nothing to report the deletion —
  `ptah schema render` dropped a declared sequence, domain, role or function on
  clickhouse, mysql, mariadb, sqlserver and sqlite and exited 0 with no comment
  and no warning. A diagnostic naming a target that cannot host the object
  states what Ptah generates rather than what the engine can do, because the
  two differ: SQL Server has had `CREATE SEQUENCE` since 2012 and MySQL 8 has
  roles, while Ptah still lacks convergent readers and planners for those kinds
  there. SQL Server sequences therefore use a named skip; MySQL-family roles
  fail closed because silently continuing would discard security state.
  ClickHouse roles and grants have since left that list: they are planned,
  rendered, read back and diffed as statements (#1025, below), so the diagnostic
  they used to receive is now reserved for the kinds ClickHouse has no concept
  of.
- **ClickHouse roles and grants (#1025).** `ClickHouse24()` sets
  `role_management`. A declared role and a database- or table-scoped grant are
  planned, applied, read back from `system.roles` and `system.grants`, and
  compared to zero difference on the next run. The renderer emits
  `CREATE ROLE IF NOT EXISTS`,
  `GRANT <privileges> ON <scope> TO <role> [WITH GRANT OPTION]`,
  `REVOKE [GRANT OPTION FOR ]<privileges> ON <scope> FROM <role>`, and
  `DROP ROLE IF EXISTS` for a source that spells one out; the planner
  emits roles before grants, because a grant to a role the server does not know
  fails with `Code: 511 ... (UNKNOWN_ROLE)`, and revokes before grants, because
  the server absorbs a narrower grant into a broader one.

  The key is the same key PostgreSQL carries, and it promises a round trip
  rather than a vocabulary. ClickHouse's spelling is its own: `ALTER ROLE` is
  refused, since `system.roles` is `(name, id, storage)` and a role has no
  attribute to alter. Ptah manages ClickHouse **roles and grants only**. Users
  and therefore all credentials, role membership (`GRANT role TO role`), quotas,
  row policies, settings profiles, column-scoped grants, and wildcard and global
  (`*.*`) scopes are outside it. `system.users` is never queried, so no
  credential reaches a description, a plan, or a log.

  Five refusals are the surprising part, and `internal/clickhouserbac` states
  each one before a server is touched:

  - A declared `password`, `login`, `superuser`, `createdb`, `createrole` or
    `replication` is refused rather than dropped. Dropping a password would
    leave an operator believing a credential was set on an object that cannot
    hold one.
  - Declaring the same privilege on both `db.*` and `db.t` is refused. The
    server records only the broader row, in either order, so the narrower grant
    reads as missing on every inspection and the plan re-issues it forever.
  - A grant scope must be written `database.table`. An unqualified table is
    refused because a render is offline and has no current database to resolve
    it against, and resolving an access-control decision against the wrong
    database is not a formatting mistake. A trailing dot is refused too, rather
    than read as the whole database.
  - A grant must name a role the same schema declares. ClickHouse resolves a
    grantee across users and roles with no syntax to choose, so an undeclared
    name either fails at `Code: 511 (UNKNOWN_ROLE)` partway through a migration
    or lands on a USER of that name — where the reader's `user_name IS NULL`
    filter never sees it again, the plan re-issues it forever, and a real
    account holds a privilege nobody declared for it.
  - A privilege name the server REWRITES is refused: `ALL`, `CREATE`, `DROP`,
    `SYSTEM`, `SYSTEM FLUSH`, `ACCESS MANAGEMENT`, `SHOW ACCESS`,
    `SHOW FILESYSTEM CACHES`, and `SHOW` and `ALTER` at table scope. Measured
    per scope on both declared lines: `GRANT ALL` records 45 rows on 26.7 and 39
    on 24.10, `GRANT SHOW ACCESS` is stored as `SHOW ROW POLICIES`, and
    `GRANT SHOW FILESYSTEM CACHES` is accepted and stored nowhere at all. The
    group names that DO read back as written stay declarable, and a name the
    server itself refuses needs no gate here.

  Three live shapes are handled rather than assumed away. A managed role
  carrying a partial revoke — a `GRANT` with an exception, which Ptah's model
  cannot express — fails the comparison instead of comparing equal and reporting
  convergence. Roles are never dropped: `RolesRemoved` is reported as a named
  comment, because a ClickHouse role is server-wide and may carry grants no
  declared schema describes. For the same reason a read describes only the roles
  the described grants name, and leaves roles whose `storage` is `users_xml` out
  entirely, since SQL does not own them. And an account that may not read the
  access catalog — measured, an account holding only `SELECT`, `SHOW TABLES` and
  `SHOW COLUMNS` is answered `Code: 497 (ACCESS_DENIED)` by both — still gets
  the rest of its schema: the read records `coverage.Role` as not described, so
  comparison withholds every declared role rather than planning a `CREATE ROLE`
  it could not verify. Failing the whole read would have broken reading a
  ClickHouse schema that declares no role at all, a capability this preset had
  no right to remove.

  Two limitations to plan around. ClickHouse RBAC statements carry no
  `ON CLUSTER` clause, so on a cluster they affect the connected replica only;
  Ptah does not model cluster propagation. And a ClickHouse role is not scoped
  to a database, so a role created by a dev or throwaway workflow outlives the
  database that workflow drops.
- **CockroachDB row-level TTL (#1027).** `CockroachDB23()` and the per-line
  presets derived from it set `row_level_ttl`. It is the only key in the
  registry that a CockroachDB preset turns ON rather than off, and the only one
  that is true on a PostgreSQL-compatible engine and false on PostgreSQL itself,
  so a reader meeting it should not assume the usual polarity.

  A declared policy is planned, applied, read back from `pg_class.reloptions`,
  and compared to zero difference on the next run. `CREATE TABLE ... WITH (...)`
  carries a new policy, `ALTER TABLE ... SET (...)` changes one, and
  `ALTER TABLE ... RESET (ttl)` removes the whole configuration in one
  statement. Measured on both declared lines, `SET` replaces only the parameters
  it names, so a declaration that stops naming one has its `RESET` planned
  explicitly.

  What the key promises is the surface `internal/crdbttl` models, not every
  parameter the engine spells, and that boundary is the interesting part. Nine
  parameters read back from the catalog exactly as written. `ttl_expire_after`
  does not — the server canonicalizes the interval, so `'72 hours'` is stored as
  `'72:00:00'` — and is compared by the interval it DENOTES instead, through
  `internal/crdbinterval` (#1605). That is deliberately not a canonicalizer:
  predicting the stored spelling would mean re-implementing PostgreSQL's
  interval rendering and keeping it right forever, while reading both sides into
  a (months, days, microseconds) triple only has to agree with the server about
  what an interval MEANS. The three fields stay apart, because the server keeps
  them apart: a month is not thirty days.

  Two parameters remain refused by name with the measurement in the error:
  `ttl_row_stats_poll_interval`, whose duration the server canonicalizes and
  which stores nothing at all below one second — a value that denotes nothing
  cannot be compared by value either — and `ttl` itself, which is derived.

  `ttl_expire_after` also adds a hidden `crdb_internal_expiration` column, so the
  CockroachDB column read now excludes hidden columns. That closes an older leak
  as well: a table declaring no primary key gets a hidden `rowid`, and
  `ptah db read` described it as a third column of a two-column table long
  before row-level TTL existed. The filter is capability-gated because
  `attishidden` is a CockroachDB column; PostgreSQL and YugabyteDB have neither
  it nor `information_schema.columns.is_hidden`.

  Two refusals are about values rather than names. A zero or negative knob is
  refused because the server rejects the negative one and accepts zero while
  storing the parameter nowhere, so neither reads back. A `false` boolean
  normalizes to "not declared", because on the server those are the same state:
  `ttl_pause = false` is stored nowhere and setting it erases an existing
  `true` exactly as a reset does.

  The dialect gate is Ptah's rather than the server's, deliberately. PostgreSQL
  18.4 answers `unrecognized parameter "ttl_expiration_expression"` on its own,
  but YugabyteDB 2026.1 answers `WARNING: storage parameter
  ttl_expiration_expression is unsupported, ignoring` before its error — and an
  engine that IGNORES a retention policy would accept the statement and never
  apply it, leaving an operator believing rows expire when they do not.

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
  standalone SQL Server sequence objects. A declared `//ptah:schema:sequence` is
  therefore reported rather than emitted:
  `-- SQLSERVER: CREATE SEQUENCE "order_number_seq" is not generated for this
  target; skipped.` The sentence names Ptah's generator on purpose — SQL Server
  has had sequences since 2012, so a bare "is not supported" would be a false
  statement about the engine. Turning the key on requires a SQL Server sequence
  reader and planner in the same change, or `schema apply` would plan a `CREATE`
  that `db read` never sees again. Raw view and trigger definitions can
  be rendered, but SQL Server catalog readback is not yet normalized enough for
  full drift-safe round trips. `DROP INDEX IF EXISTS` and `DROP CONSTRAINT IF
  EXISTS` are disabled in the portable preset, so plans should remain exactly
  scoped instead of relying on guards.

## Follow-ups

- Spanner remains lowest priority: the preset exists so callers get explicit
  routing and conservative rendering, but full Spanner-specific DDL such as
  interleaved tables is outside the PostgreSQL-family adapter.
