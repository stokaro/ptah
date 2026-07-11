# Dialect capabilities

Ptah maps several real database targets onto shared implementations: MySQL and
MariaDB share one planner and one renderer family, and versions within a single
dialect differ in which DDL they accept. Instead of forking a new dialect for
every variant, planners and renderers consult a **capability set** — a
validated `map[Capability]bool` describing what the concrete target accepts —
and restrict or enable individual emissions accordingly (issues #225/#226).

Package: `core/platform/capability`.

## The model

Two layers cooperate:

- **Intent (planner).** A planner configured with a capability set records
  intent on AST nodes — e.g. a MariaDB-preset planner sets `IfExists` on
  constraint drops because MariaDB accepts guarded drops.
- **Validity (renderer).** A renderer checks modifiers against *its own*
  capability set for the target dialect and drops anything the target would
  reject — the `mysql` renderer strips `IF EXISTS` from constraint and index
  drops even if a stray intent flag reaches it.

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
| `create_or_replace_trigger` | `CREATE OR REPLACE TRIGGER` (PostgreSQL 14+, MariaDB; not MySQL). Reserved for the trigger work (#158) |
| `row_level_security` | Row-level security policies (PostgreSQL) |

### Validation rules

`Capabilities.Validate()` enforces:

1. **Known keys only** — anything outside the registry is an error.
2. **Requirement edges** — an enabled capability with a disabled prerequisite
   is a contradiction (`drop_constraint_if_exists` without
   `drop_constraint_generic`: an `IF EXISTS` variant of a statement the target
   does not have).
3. **Mutual exclusion groups** — at most one member of a group may be enabled
   (`enum_inline_column` vs `enum_custom_type`: a dialect models enums one way
   or the other).

Presets are valid by construction (unit-tested); validate hand-built or
composed sets yourself.

## Presets

| Capability | MySQL80 | MySQL8016 | MySQLLegacy | MariaDB1011 | MariaDBLegacy | Postgres16 | Postgres13 | ClickHouse24 |
|---|---|---|---|---|---|---|---|---|
| `drop_constraint_generic` | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `drop_constraint_if_exists` | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `drop_index_if_exists` | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `check_constraints_enforced` | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `drop_check_clause` | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `enum_inline_column` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| `enum_custom_type` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ |
| `create_index_concurrently` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ |
| `create_or_replace_trigger` | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ |
| `row_level_security` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ |

Version lines: `MySQL80()` covers MySQL 8.0.19+ and 9.x; `MySQL8016()` covers
8.0.16–8.0.18; `MySQLLegacy()` anything older. `MariaDB1011()` covers the
supported MariaDB lines (10.6+/11.x); `MariaDBLegacy()` is the conservative
floor `ForServerVersion` assigns to pre-10.2 servers. `Postgres16()` covers
PostgreSQL 14+; `Postgres13()` covers 12–13 (no `CREATE OR REPLACE TRIGGER`).

### Composition

```go
caps := capability.MariaDB1011().With(capability.DropIndexIfExists, false)
if err := caps.Validate(); err != nil { /* reject configuration */ }
planner := mysql.NewWithCapabilities(caps)
```

`With` copies — presets are never mutated.

### Resolving a preset

- `capability.ForDialect("mariadb")` — default preset for a dialect name
  (aliases like `pgx`/`postgresql` normalize first). Used by `GetPlanner` and
  the renderers.
- `capability.ForServerVersion("mysql", version)` — refine using a live
  `SELECT version()` string. Recognizes shapes like `8.0.42-log`,
  `10.11.6-MariaDB-…`, the `5.5.5-10.11.6-MariaDB` replication-protocol prefix
  (MariaDB over the mysql driver resolves to the MariaDB preset), and
  `PostgreSQL 16.3 (…)`. Wiring this into live connections at `read-db` /
  `migrate` time is a follow-up.

## Current consumers

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
  On the same no-generic targets a UNIQUE removal uses
  `ALTER TABLE … DROP INDEX <name>` (valid family-wide — verified live), and a
  CHECK removal with no valid spelling at all (`MySQLLegacy`) degrades to a
  loud WARNING comment. Universal `DROP INDEX` branching for modern versions
  stays with #195.
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
  because concurrent builds cannot run inside a transaction block (#152 tracks
  migrator support); a capability-less target (CockroachDB-style preset, #171)
  keeps plain `CREATE INDEX` regardless of policy.

## Follow-ups

- `SELECT version()` → preset wiring at connect time (`ForServerVersion` is
  ready; the dbschema plumbing is tracked with #163/#152 work).
- `create_or_replace_trigger` consumer lands with triggers (#158).
- UNIQUE drop syntax selection (`DROP INDEX` vs generic clause) — #195.
- Distributed-SQL presets (CockroachDB / Yugabyte / Spanner) — #171.
