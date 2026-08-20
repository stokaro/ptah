// Package capability models what a concrete database target (a dialect plus
// a version line) can accept, as a validated set of feature flags.
//
// Ptah maps several real targets onto one implementation: MySQL and MariaDB
// share a planner and a renderer; CockroachDB, YugabyteDB, and Spanner use the
// PostgreSQL family with target-specific restrictions; and versions within one
// dialect differ in what DDL they accept (MySQL gained generic DROP CONSTRAINT
// in 8.0.19, enforced CHECK constraints in 8.0.16; IF EXISTS on constraint
// drops is MariaDB-only; and so on). Encoding each variant as a separate
// dialect would multiply planners and renderers; instead, planners and
// renderers consult a capability set and restrict or enable individual
// emissions per target (issues #225/#226/#171).
//
// # Model
//
// A Capability is a named feature flag from a curated registry — free-form
// keys are rejected by Validate. A Capabilities value is a plain
// map[Capability]bool set. The nil set is valid and behaves conservatively:
// Has reports false for everything, so consumers fall back to the most
// compatible emission.
//
// Some capabilities relate to each other: a capability may require another
// (IF EXISTS on constraint drops presupposes the generic DROP CONSTRAINT
// statement), and some are mutually exclusive by construction of the SQL
// model (a dialect models enums either inline in the column type or as a
// separate named type, never both). Validate enforces both rule kinds.
//
// # Presets
//
// Presets describe the current supported version lines. Compose from a preset
// with With rather than building sets by hand:
//
//	caps := capability.MariaDB1011().With(capability.DropIndexIfExists, false)
//	if err := caps.Validate(); err != nil { ... }
//
// ForDialect resolves the default preset for a normalized dialect name;
// ForServerVersion refines that using a live server version string (e.g. the
// result of SELECT version()).
package capability

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/capabilityline"
)

// Capability is a single named feature flag from the curated registry below.
type Capability string

const (
	// DropConstraintGeneric marks support for the SQL-standard
	// ALTER TABLE ... DROP CONSTRAINT <name> clause for non-FK constraints.
	// MySQL gained it in 8.0.19; MariaDB has it on all supported lines.
	// Without it, a CHECK constraint drop must use ALTER TABLE ... DROP CHECK
	// (MySQL 8.0.16–8.0.18) and UNIQUE drops must use DROP INDEX.
	DropConstraintGeneric Capability = "drop_constraint_generic"

	// DropConstraintIfExists marks support for the IF EXISTS guard on
	// constraint drops (ALTER TABLE ... DROP CONSTRAINT IF EXISTS ... and
	// ALTER TABLE ... DROP FOREIGN KEY IF EXISTS ...). MariaDB-only within
	// the MySQL family; MySQL 8/9 reject it. PostgreSQL supports it.
	// Requires DropConstraintGeneric.
	DropConstraintIfExists Capability = "drop_constraint_if_exists"

	// DropIndexIfExists marks support for the IF EXISTS guard on DROP INDEX
	// (MariaDB 10.1.4+: DROP INDEX IF EXISTS <name> ON <table>; PostgreSQL:
	// DROP INDEX IF EXISTS <name>). MySQL has no such form.
	DropIndexIfExists Capability = "drop_index_if_exists"

	// CheckConstraintsEnforced marks targets that actually enforce CHECK
	// constraints. MySQL parsed-and-ignored CHECK before 8.0.16; MariaDB
	// enforces from 10.2.1; PostgreSQL always enforces. When absent, emitting
	// an ADD CONSTRAINT ... CHECK would silently do nothing on the target, so
	// planners surface a warning instead.
	CheckConstraintsEnforced Capability = "check_constraints_enforced"

	// DropCheckClause marks support for the dedicated
	// ALTER TABLE ... DROP CHECK <name> spelling (MySQL 8.0.16+, including
	// the 9.x line). MariaDB does NOT accept it (verified live on 10.11) —
	// its CHECK drops go through the generic DROP CONSTRAINT clause.
	// Planners request the spelling via DropConstraintOperation.Check for
	// targets that lack DropConstraintGeneric; renderers resolve the final
	// spelling against their own set, so a stray Check flag reaching a
	// MariaDB renderer degrades safely to the generic clause. Requires
	// CheckConstraintsEnforced (a server without enforced CHECKs has nothing
	// to drop).
	DropCheckClause Capability = "drop_check_clause"

	// EnumInlineColumn marks dialects whose enums live inline in the column
	// definition (MySQL/MariaDB ENUM(...), ClickHouse Enum8/Enum16).
	// Mutually exclusive with EnumCustomType.
	EnumInlineColumn Capability = "enum_inline_column"

	// EnumCustomType marks dialects whose enums are separate named types
	// (PostgreSQL CREATE TYPE ... AS ENUM). Mutually exclusive with
	// EnumInlineColumn.
	EnumCustomType Capability = "enum_custom_type"

	// CreateIndexConcurrently marks support for PostgreSQL's non-locking
	// CREATE [UNIQUE] INDEX CONCURRENTLY build. Postgres-compatible engines
	// differ here: CockroachDB only parses the keyword as a compatibility
	// no-op (its schema changes are online by design), so a future preset
	// for it would disable this capability rather than pretend the keyword
	// changes behavior (issue #171).
	CreateIndexConcurrently Capability = "create_index_concurrently"

	// DropIndexConcurrently marks support for PostgreSQL's non-locking
	// DROP INDEX CONCURRENTLY. It is deliberately a separate flag from
	// CreateIndexConcurrently with no implication edge between them: a caller
	// composing a set with .With(CreateIndexConcurrently, false) is restricting
	// index builds, not asserting anything about drops, and Validate must not
	// turn that composition into an error. The PostgreSQL-compatible presets
	// that already decline CREATE INDEX CONCURRENTLY decline this too.
	DropIndexConcurrently Capability = "drop_index_concurrently"

	// IndexIncludeSPGiST marks PostgreSQL versions whose SP-GiST access
	// method accepts INCLUDE payload columns. PostgreSQL added that support in
	// 14; PostgreSQL 12–13 support INCLUDE only with B-tree and GiST.
	IndexIncludeSPGiST Capability = "index_include_spgist"

	// Views marks support for the standalone CREATE VIEW ... AS <query>
	// object.
	//
	// The key exists so that a preset answers for its own target instead of
	// the question being settled by comparing the dialect name — which is how
	// the offline converter came to drop a view for three PostgreSQL-family
	// engines while the live planner emitted one for the same schema
	// (stokaro/ptah#929).
	Views Capability = "views"

	// MaterializedViews marks support for CREATE MATERIALIZED VIEW: a view
	// whose query result is stored rather than recomputed on read. A target
	// may host plain views and no materialized ones, so this is a separate
	// key that requires Views.
	MaterializedViews Capability = "materialized_views"

	// DomainTypes marks support for CREATE DOMAIN: a base type carrying its
	// own NOT NULL, DEFAULT and CHECK. Measured 2026-08-19: PostgreSQL 18.4
	// and YugabyteDB 2026.1.1.1 accept it; CockroachDB v26.2.5 answers "not
	// yet implemented" (cockroachdb/cockroach#27796) and Spanner's PostgreSQL
	// interface answers "Statement is not supported".
	//
	// The CockroachDB answer has an expiry date on it: #27796 was closed on
	// 2026-07-23, so a release after v26.2.5 -- the newest published when this
	// was measured -- will carry it, and that release wants its own preset
	// rather than an edit here.
	DomainTypes Capability = "domain_types"

	// CompositeTypes marks support for CREATE TYPE ... AS (field type, ...).
	// Measured 2026-08-19: PostgreSQL, CockroachDB and YugabyteDB all accept
	// it; only Spanner's PostgreSQL interface refuses. It is a separate key
	// from DomainTypes for exactly that reason -- the three user-type kinds do
	// not travel together, and one key for "user types" would have told
	// CockroachDB it cannot do something it does.
	CompositeTypes Capability = "composite_types"

	// RangeTypes marks support for CREATE TYPE ... AS RANGE. Measured
	// 2026-08-19: PostgreSQL and YugabyteDB accept it; CockroachDB v26.2.5
	// answers "not yet implemented" (cockroachdb/cockroach#27791, still open)
	// and Spanner refuses.
	RangeTypes Capability = "range_types"

	// Functions marks support for a user-defined function declared as a
	// standalone object with a return type, a language and a body — the shape
	// ast.CreateFunctionNode carries. A target whose routines are declared
	// differently enough that the node cannot describe one reads as absent
	// here, the same way ClickHouse's row policies read as absent from
	// RowLevelSecurity.
	Functions Capability = "functions"

	// Procedures marks support for CREATE PROCEDURE: a routine that returns
	// nothing and is invoked with CALL.
	//
	// It is separate from [Functions] because the two do not travel together.
	// SQL Server hosts both and Ptah reads back only the function; ClickHouse
	// has neither. A preset claims this one only where a renderer emits the
	// procedure, a reader returns it, and a planner reconciles it
	// (stokaro/ptah#1722).
	Procedures Capability = "procedures"

	// Triggers marks support for the CREATE TRIGGER object itself. Whether
	// that object can be replaced in a single statement is
	// CreateOrReplaceTrigger, which requires this one — replace syntax for a
	// statement the target does not have is a contradiction.
	Triggers Capability = "triggers"

	// CreateOrReplaceTrigger marks support for replacing triggers in one
	// statement (PostgreSQL 14+ and MariaDB 10.1.4+ use
	// CREATE OR REPLACE TRIGGER; SQL Server uses CREATE OR ALTER TRIGGER;
	// MySQL has no equivalent). Trigger renderers use this to choose between
	// replace syntax and an explicit drop/create sequence. Requires Triggers.
	CreateOrReplaceTrigger Capability = "create_or_replace_trigger"

	// AlterGeneratedColumnExpression marks support for changing a generated
	// column expression in place. PostgreSQL added
	// ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION AS (...) in 17. Older
	// versions require destructive workarounds that Ptah does not plan
	// automatically.
	AlterGeneratedColumnExpression Capability = "alter_generated_column_expression"

	// RowLevelSecurity marks that a target models row-level security: that a
	// declared policy is rendered, read back and compared -- not that any
	// particular clause exists, and not that the object is spelled the way
	// PostgreSQL spells it.
	//
	// The two shapes it covers really are different objects. PostgreSQL splits
	// the switch from the rule: ALTER TABLE ... ENABLE ROW LEVEL SECURITY, then
	// CREATE POLICY carrying an inline USING expression and an optional TO role
	// list. SQL Server has one object, a SECURITY POLICY, whose predicates must
	// invoke an existing inline table-valued function; it has no table-level
	// switch, no inline expression and no role list, and it permits only one
	// ENABLED policy per table.
	//
	// A target claiming this key therefore promises convergence, not
	// portability: what it renders it reads back and stops planning. A
	// PostgreSQL policy does not port to SQL Server unchanged, and the SQL
	// Server renderer says so by name rather than rendering something with a
	// different meaning (stokaro/ptah#1699).
	RowLevelSecurity Capability = "row_level_security"

	// PostgresCatalogFunctions marks that pg_catalog's introspection helpers
	// resolve: obj_description for a stored comment, format_type for a type as
	// the server spells it, pg_get_expr for a stored expression.
	//
	// They are one key because they are one fact -- whether the catalog is
	// PostgreSQL's own or an emulation of its shape. Measured on the Cloud
	// Spanner emulator through PGAdapter 0.55.2, which refuses all three and
	// with a different message each time:
	//
	//	obj_description(2200, 'pg_namespace')  The Postgres Type is not supported: name
	//	format_type(a.atttypid, a.atttypmod)   function format_type(bigint, bigint) does not exist
	//	pg_get_expr('x', 1::bigint)            cannot accept a value of type pg_node_tree
	//
	// A refused function is the whole statement's fate, not a null column, and
	// it is refused even inside a CASE branch no row would take, because the
	// name must resolve before any row is read. So a reader that asks anyway
	// cannot read the schema at all (stokaro/ptah#942).
	PostgresCatalogFunctions Capability = "postgres_catalog_functions"

	// CatalogRowStatistics marks that the catalog exposes planner row-count
	// statistics, which Ptah reads to tell an empty table from a populated one
	// before choosing a blocking or a concurrent index build.
	//
	// Measured on the Cloud Spanner emulator through PGAdapter 0.55.2:
	// `pg_stat_all_tables` answers `relation "pg_stat_all_tables" does not
	// exist`, while the pg_class columns beside it -- reltuples, relkind,
	// relrowsecurity -- all answer. The catalog is emulated deeply enough to
	// carry the tables and not the statistics views (stokaro/ptah#942).
	CatalogRowStatistics Capability = "catalog_row_statistics"

	// CatalogDependencies marks that the catalog exposes pg_depend, the
	// dependency table the user-defined-type read joins to tell a type an
	// extension owns from one the user declared.
	//
	// It names the relation rather than the feature on purpose. An earlier
	// draft of this key named the type system -- domains, composite types and
	// range types -- and the live probe refuted it: CockroachDB 26.2 refuses
	// CREATE DOMAIN (crdb#27796) and CREATE TYPE ... AS RANGE (crdb#27791) yet
	// accepts a composite type, so no single key can stand for the three, and
	// gating the read on "has the type system" would have skipped a read
	// CockroachDB serves.
	//
	// The relation is the honest gate because it is what actually fails.
	// Measured: `SELECT 1 FROM pg_depend LIMIT 1` is accepted by PostgreSQL and
	// by CockroachDB, and answers `relation "pg_depend" does not exist` on the
	// Cloud Spanner emulator through PGAdapter 0.55.2 -- and a missing relation
	// cannot be stood in for by a constant the way a missing function can, so
	// the read is skipped rather than reduced (stokaro/ptah#942).
	CatalogDependencies Capability = "catalog_dependencies"

	// RoleManagement marks support for the role and object-privilege
	// management Ptah models: named roles plus GRANT/REVOKE of privileges on
	// schema objects.
	//
	// It is deliberately NOT "PostgreSQL role management", which is what this
	// doc said while PostgreSQL was the only implementation. Each engine
	// satisfies it with its own vocabulary, and what the key promises is that
	// a declared role and grant can be planned, rendered, introspected and
	// compared — not that any particular attribute exists. A ClickHouse role
	// carries no attributes at all, and ClickHouse still has this capability.
	RoleManagement Capability = "role_management"

	// ForeignKeys marks support for declarative FOREIGN KEY constraints.
	// PostgreSQL, CockroachDB, YugabyteDB, Spanner's PostgreSQL interface,
	// MySQL, MariaDB, SQLite, and SQL Server support them.
	ForeignKeys Capability = "foreign_keys"

	// ForeignKeysRequireUniqueReference marks targets that reject a foreign
	// key unless its referenced columns already form a declared unique key.
	// PostgreSQL-family standards-oriented targets, SQLite, and SQL Server use
	// this policy. MySQL 8.4+ enables it by default through
	// restrict_fk_on_non_standard_key; MySQL 8.0 and MariaDB instead retain
	// indexed, nonunique referenced-key support.
	ForeignKeysRequireUniqueReference Capability = "foreign_keys_require_unique_reference"

	// ForeignKeysRequireIndexedReference marks targets that permit a foreign
	// key to reference a nonunique key, but require the referenced columns to
	// be a full leftmost prefix of an existing index. MySQL before 8.4 and
	// MariaDB use this policy.
	ForeignKeysRequireIndexedReference Capability = "foreign_keys_require_indexed_reference"

	// ForeignKeysCreateBackingIndex marks targets that create and manage the
	// referenced-key backing index themselves. Cloud Spanner uses this policy,
	// so Ptah must not reject an otherwise valid foreign key merely because the
	// referenced columns are not declared unique or indexed in the input.
	ForeignKeysCreateBackingIndex Capability = "foreign_keys_create_backing_index"

	// Sequences marks that Ptah generates standalone sequence objects for the
	// target: PostgreSQL SERIAL/BIGSERIAL backing and explicit CREATE SEQUENCE.
	//
	// It describes the generator, not the engine's brochure. A preset that sets
	// it must have a code path that emits, reads back and plans sequences for
	// that target, because every reader of this key -- the PostgreSQL renderer's
	// CREATE/ALTER/DROP SEQUENCE visitors and the PostgreSQL introspection
	// reader -- assumes all three. MariaDB is the worked example: the engine has
	// had SEQUENCE since 10.3, this key claimed it, and no code path anywhere
	// emitted, read or planned one (stokaro/ptah#931 item 8).
	Sequences Capability = "sequences"

	// XMLType marks support for the PostgreSQL XML column type. CockroachDB
	// and Spanner PostgreSQL disable it; callers should use platform-specific
	// type overrides for those targets.
	XMLType Capability = "xml_type"

	// AdvisoryLocks marks support for PostgreSQL advisory locks such as
	// pg_advisory_lock. Migration-level lock selection is outside this
	// package, but the flag lets callers avoid assuming PostgreSQL lock
	// functions exist on every PostgreSQL-wire engine.
	AdvisoryLocks Capability = "advisory_locks"

	// RowLevelTTL marks support for a table-level row-expiry policy declared
	// as storage parameters and executed by a background job the engine runs.
	//
	// It is the first key here that is TRUE on a PostgreSQL-compatible engine
	// and FALSE on PostgreSQL itself, and the direction matters: every other
	// key names something PostgreSQL has and a compatible engine may lack, so
	// a reader meeting this one should not assume the usual polarity.
	// Measured, `CREATE TABLE t (...) WITH (ttl_expiration_expression =
	// 'expires_at')` is accepted by CockroachDB v25.4.14 and v26.2.5, and
	// answered `ERROR: unrecognized parameter "ttl_expiration_expression"` by
	// PostgreSQL 18.4 and by YugabyteDB 2026.1 — which first emits `WARNING:
	// storage parameter ttl_expiration_expression is unsupported, ignoring`,
	// which is exactly why a declaration reaching an engine without this key
	// is refused by Ptah rather than left to the server (stokaro/ptah#1027).
	//
	// Acceptance alone does NOT decide this key, and the Spanner PostgreSQL
	// interface is what proved it: through PGAdapter it accepts the same
	// CREATE TABLE at exit 0 while having no such feature. So the probe reads
	// pg_class.reloptions back and decides on whether the policy was STORED —
	// the same distinction capability.MaterializedViews draws for MySQL, where
	// CREATE MATERIALIZED VIEW is parsed and the word discarded.
	//
	// What the key promises is the surface [go.5x5.cz/ptah/internal/crdbttl]
	// models, not every parameter the engine spells. The two whose values the
	// server rewrites on the way in are refused there, and that refusal is
	// part of the capability rather than a gap in it.
	RowLevelTTL Capability = "row_level_ttl"

	// MigrationTimeouts marks that Ptah can bound a migration with a lock and a
	// statement timeout on this target.
	//
	// It names a runtime policy rather than an object: the migrator wraps a
	// migration in the server's own timeout settings and restores them
	// afterwards. Before this key the decision was a switch over three dialect
	// names, so CockroachDB and YugabyteDB answered "migration timeouts are not
	// supported" while both accept `SET LOCAL statement_timeout` and
	// `SET LOCAL lock_timeout` -- measured on CockroachDB v25.4.0 and
	// YugabyteDB 2026.1. A timeout is the safety belt on a migration that takes
	// a lock, and those are the two deployments where a long lock hurts most
	// (stokaro/ptah#1713).
	MigrationTimeouts Capability = "migration_timeouts"

	// TransactionalDDL marks that this target runs schema changes inside a
	// transaction that rolls back as a unit, which is what `--tx-mode all`
	// asks for.
	//
	// MySQL, MariaDB and ClickHouse commit DDL implicitly, so a failed
	// migration leaves whatever ran before it; that is the engine rather than
	// Ptah, and the refusal says so (stokaro/ptah#1713).
	TransactionalDDL Capability = "transactional_ddl"

	// CheckGrantStatement marks a server that answers a direct question about
	// whether the CONNECTED account holds a privilege, rather than leaving the
	// caller to infer it from a failure.
	//
	// It names ClickHouse's `CHECK GRANT`, which is the only spelling Ptah
	// consults, and it exists because a version comparison was standing in for
	// it: internal/dbschema/clickhouse refuses database-realm cleanup on a
	// server that cannot answer, and decided that by parsing the banner against
	// 24.11 with a hand-rolled comparison outside this package
	// (stokaro/ptah#916 item 3).
	//
	// The refusal it guards is not a nicety. A realm cleanup drops every object
	// in the database, and the proof that the read which enumerated them SAW
	// everything is the privilege answer. A server that cannot be asked cannot
	// supply that proof, so the cleanup is refused rather than run on a read
	// that may have been silently partial.
	//
	// Measured live: `CHECK GRANT SHOW DATABASES, SHOW TABLES ON *.*` answers 1
	// on ClickHouse 26.7.3.19 and is `Syntax error` on 24.10.4.191. It is the
	// first key on which two declared ClickHouse lines differ at all, which is
	// what gives that dialect a version ladder to have.
	CheckGrantStatement Capability = "check_grant_statement"

	// CatalogViewDependencies marks a catalog that names the tables a view
	// reads, rather than leaving a caller to parse the view body for them.
	//
	// It names information_schema.VIEW_TABLE_USAGE, which MySQL added in
	// 8.0.13. internal/dbschema/mysql refuses a database clean without it,
	// because an external view -- one outside the schema being cleaned that
	// reads a table inside it -- cannot be found otherwise, and dropping the
	// table under it leaves that view broken with nothing to say so.
	//
	// MariaDB does not have the view at any version, and the refusal it guards
	// stays scoped to MySQL: the capability is a fact about a catalog, and
	// which dialect refuses on its absence is a policy that lives with the
	// writer (stokaro/ptah#916 item 3).
	CatalogViewDependencies Capability = "catalog_view_dependencies"

	// ShowRoutinePrivilege marks a server on which reading routine metadata
	// requires the global SHOW_ROUTINE privilege, which MySQL introduced in
	// 8.0.20.
	//
	// The polarity is the unusual part and it is deliberate: this key is true
	// where MORE is demanded of the account, not where more is available. A
	// server too old to grant SHOW_ROUTINE reads routines under the lesser
	// privileges it does have, so demanding it there would refuse a clean that
	// works. The metadata-visibility check therefore asks for it exactly where
	// the server can grant it (stokaro/ptah#916 item 3).
	ShowRoutinePrivilege Capability = "show_routine_privilege"

	// RenameColumnClause marks support for renaming a column in place with
	// ALTER TABLE ... RENAME COLUMN <old> TO <new>.
	//
	// SQLite gained the clause in 3.25 and the SQLite renderer emitted it
	// unconditionally -- one of the five ad-hoc version gates stokaro/ptah#916
	// item 3 names, and the only one whose threshold was not written down
	// anywhere at all: there was no comparison to move, just an emission with
	// nothing behind it.
	//
	// Measured live: accepted by PostgreSQL 18, MySQL 8.4.11, MariaDB 11.8.8
	// and ClickHouse 26.7.3.19 (on a column that is not the sorting key --
	// renaming that one is refused for a different reason); refused by SQL
	// Server 2022, which renames through sp_rename instead, and by the Spanner
	// PostgreSQL interface with `Only <TABLE> is supported for renaming`.
	RenameColumnClause Capability = "rename_column_clause"

	// CatalogCheckConstraintTableName marks targets whose
	// information_schema.CHECK_CONSTRAINTS view carries a TABLE_NAME column,
	// so a CHECK clause read out of it can be attributed to the table that
	// declares it rather than to a name alone.
	//
	// MariaDB is the only engine Ptah targets that has the column, and it is a
	// MariaDB extension to the standard view rather than a newer version of it:
	// measured, `information_schema.CHECK_CONSTRAINTS` is
	// CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, TABLE_NAME, CONSTRAINT_NAME,
	// LEVEL, CHECK_CLAUSE on MariaDB 11.8.8 and drops to
	// CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, CHECK_CLAUSE on
	// MySQL 8.4.11, PostgreSQL 18.4, SQL Server 16.0.4265.3, CockroachDB
	// v26.2.5, YugabyteDB 2026.1 and the Spanner PostgreSQL interface. The view
	// does not exist at all on ClickHouse 26.7.3.19.
	//
	// The engines without it are not losing information: MySQL requires CHECK
	// constraint names to be unique per schema, which is why its view has no
	// TABLE_NAME to carry, while MariaDB allows the same name on two tables and
	// added the column to tell them apart.
	//
	// This is the fifth of the ad-hoc gates stokaro/ptah#916 item 3 names, and
	// the one that must keep its error handling: the reader sniffed the shape
	// by asking the richer spelling and reading MySQL error 1054 off the
	// failure. That fallback stays, because the server's own answer about its
	// own catalog outranks a preset -- the key only decides which spelling is
	// asked FIRST, so the common path stops paying for a failed round trip.
	CatalogCheckConstraintTableName Capability = "catalog_check_constraint_table_name"

	// GeneratedColumns marks support for a column declared
	// GENERATED ALWAYS AS (<expr>) STORED -- the SQL-standard spelling three
	// of Ptah's renderers emit and the only one they emit.
	//
	// Measured live: accepted by PostgreSQL 18.4, MySQL 8.4.11, MariaDB
	// 11.8.8, CockroachDB v26.2.5, SQLite 3.53.3, the Spanner PostgreSQL
	// interface and YugabyteDB 2025.1 and 2026.1; refused by ClickHouse
	// 26.7.3.19 and SQL Server 16.0.4265.3, which parse no such clause, and by
	// YugabyteDB 2024.2 with `syntax error at or near "("` because the engine
	// underneath it is still PostgreSQL 11 and the feature arrived in
	// PostgreSQL 12.
	//
	// That last one is why the key exists rather than being assumed: the
	// PostgreSQL renderer serves YugabyteDB, so before this an offline plan for
	// a 2024 LTS server emitted a clause that server cannot parse
	// (stokaro/ptah#916).
	//
	// There is deliberately NO implication edge from
	// AlterGeneratedColumnExpression to this key, even though altering a
	// generated column plainly needs one to exist. The two name spellings, not
	// abilities, and the spellings come apart: ClickHouse has generated columns
	// as `MATERIALIZED <expr>` and alters them with MODIFY COLUMN, so it
	// answers false here and true there, and an edge would make that pair
	// invalid. The prerequisite that IS real lives in the capability probe,
	// where the PostgreSQL-family and MySQL-family plans set up with this exact
	// clause and so cannot ask their alter question without it.
	GeneratedColumns Capability = "generated_columns"

	// DeferrableConstraints marks support for a foreign key declared
	// DEFERRABLE, whose check can be postponed to the end of a transaction.
	//
	// Deferred checking is the standard answer to a circular reference and to a
	// bulk load that transiently violates a constraint, so the targets that
	// have it are the ones where Ptah can express either.
	//
	// Measured live with `CONSTRAINT fk FOREIGN KEY (id) REFERENCES p(id)
	// DEFERRABLE INITIALLY DEFERRED`: accepted by PostgreSQL 18.4 and
	// YugabyteDB 2026.1, both reporting condeferrable and condeferred true in
	// pg_constraint, and by SQLite 3.53.3, whose foreign-key grammar carries
	// the clause. Refused by MySQL 8.4.11 and MariaDB 11.8.8 (error 1064), by
	// SQL Server 16.0.4265.3 (`Incorrect syntax near 'DEFERRABLE'`), by
	// ClickHouse 26.7.3.19, which parses no FOREIGN KEY clause at all, and by
	// the Spanner PostgreSQL interface with `<DEFERRABLE> constraints are not
	// supported`.
	//
	// CockroachDB v26.2.5 refuses it too, which is worth stating because the
	// PostgreSQL family is otherwise where this feature lives: every form is
	// `unimplemented: this syntax`, including `NOT DEFERRABLE`, so the key is
	// false there rather than partially true (stokaro/ptah#1624).
	DeferrableConstraints Capability = "deferrable_constraints"
)

// spec documents a registry entry and its implication edges.
type spec struct {
	doc string
	// requires lists capabilities that must also be enabled whenever this
	// one is enabled; Validate rejects sets that violate an edge.
	requires []Capability
}

// registry is the curated set of known capabilities. Validate rejects any
// key not present here, so typos fail fast instead of silently reading as
// "capability absent".
var registry = map[Capability]spec{
	DropConstraintGeneric: {
		doc: "SQL-standard ALTER TABLE ... DROP CONSTRAINT for non-FK constraints (MySQL 8.0.19+, MariaDB, PostgreSQL)",
	},
	DropConstraintIfExists: {
		doc:      "IF EXISTS guard on constraint drops (MariaDB, PostgreSQL; rejected by MySQL)",
		requires: []Capability{DropConstraintGeneric},
	},
	DropIndexIfExists: {
		doc: "IF EXISTS guard on DROP INDEX (MariaDB 10.1.4+, PostgreSQL; rejected by MySQL)",
	},
	CheckConstraintsEnforced: {
		doc: "CHECK constraints are enforced, not parsed-and-ignored (MySQL 8.0.16+, MariaDB 10.2.1+, PostgreSQL)",
	},
	DropCheckClause: {
		doc:      "dedicated ALTER TABLE ... DROP CHECK spelling (MySQL 8.0.16+; NOT MariaDB — verified live)",
		requires: []Capability{CheckConstraintsEnforced},
	},
	EnumInlineColumn: {
		doc: "enums are inline column types (MySQL/MariaDB ENUM, ClickHouse Enum8/16)",
	},
	EnumCustomType: {
		doc: "enums are separate named types (PostgreSQL CREATE TYPE ... AS ENUM)",
	},
	CreateIndexConcurrently: {
		doc: "CREATE [UNIQUE] INDEX CONCURRENTLY (PostgreSQL; a compatibility no-op on CockroachDB)",
	},
	DropIndexConcurrently: {
		doc: "DROP INDEX CONCURRENTLY (PostgreSQL; disabled on the PostgreSQL-compatible presets that do not emit CONCURRENTLY)",
	},
	IndexIncludeSPGiST: {
		doc: "SP-GiST indexes with INCLUDE payload columns (PostgreSQL 14+)",
	},
	Views: {
		doc: "standalone CREATE VIEW ... AS <query> objects",
	},
	MaterializedViews: {
		doc:      "CREATE MATERIALIZED VIEW: a view whose query result is stored",
		requires: []Capability{Views},
	},
	DomainTypes: {
		doc: "CREATE DOMAIN: a base type carrying its own NOT NULL, DEFAULT and CHECK",
	},
	CompositeTypes: {
		doc: "CREATE TYPE ... AS (field type, ...)",
	},
	RangeTypes: {
		doc: "CREATE TYPE ... AS RANGE (SUBTYPE = ...)",
	},
	Functions: {
		doc: "user-defined functions declared with a return type, a language, and a body",
	},
	Procedures: {
		doc: "stored procedures: routines that return nothing and are invoked with CALL",
	},
	Triggers: {
		doc: "CREATE TRIGGER objects",
	},
	CreateOrReplaceTrigger: {
		doc:      "single-statement trigger replacement (PostgreSQL/MariaDB CREATE OR REPLACE, SQL Server CREATE OR ALTER; not MySQL)",
		requires: []Capability{Triggers},
	},
	AlterGeneratedColumnExpression: {
		doc: "in-place ALTER COLUMN SET EXPRESSION for generated columns (PostgreSQL 17+)",
	},
	RowLevelSecurity: {
		doc: "row-level security policies (PostgreSQL)",
	},
	PostgresCatalogFunctions: {
		doc: "obj_description reads a comment back out of the catalog",
	},
	CatalogRowStatistics: {
		doc: "the catalog exposes planner row-count statistics (pg_stat_all_tables)",
	},
	CatalogDependencies: {
		doc: "the catalog exposes pg_depend",
	},
	RoleManagement: {
		doc: "named roles plus GRANT/REVOKE of object privileges (PostgreSQL family, ClickHouse)",
	},
	ForeignKeys: {
		doc: "declarative FOREIGN KEY constraints",
	},
	ForeignKeysRequireUniqueReference: {
		doc:      "foreign keys require a declared unique referenced key",
		requires: []Capability{ForeignKeys},
	},
	ForeignKeysRequireIndexedReference: {
		doc:      "foreign keys require the referenced columns as a full leftmost index prefix (MySQL before 8.4 and MariaDB)",
		requires: []Capability{ForeignKeys},
	},
	ForeignKeysCreateBackingIndex: {
		doc:      "the database creates the referenced-key backing index (Cloud Spanner)",
		requires: []Capability{ForeignKeys},
	},
	Sequences: {
		doc: "database sequence objects (SERIAL/BIGSERIAL or explicit CREATE SEQUENCE support)",
	},
	XMLType: {
		doc: "PostgreSQL XML column type",
	},
	AdvisoryLocks: {
		doc: "PostgreSQL advisory lock functions",
	},
	MigrationTimeouts: {
		doc: "a migration can be bounded by a lock timeout and a statement timeout the migrator sets and restores",
	},
	TransactionalDDL: {
		doc: "schema changes run inside a transaction that rolls back as a unit, which is what --tx-mode all needs",
	},
	RowLevelTTL: {
		doc: "table storage parameters declaring a row-expiry policy (CockroachDB row-level TTL)",
	},
	CheckGrantStatement: {
		doc: "a statement answering whether the connected account holds a privilege (ClickHouse CHECK GRANT)",
	},
	CatalogViewDependencies: {
		doc: "a catalog naming the tables a view reads (MySQL information_schema.VIEW_TABLE_USAGE)",
	},
	ShowRoutinePrivilege: {
		doc: "routine metadata requires the global SHOW_ROUTINE privilege (MySQL 8.0.20+)",
	},
	RenameColumnClause: {
		doc: "ALTER TABLE ... RENAME COLUMN renames a column in place (SQLite 3.25+)",
	},
	CatalogCheckConstraintTableName: {
		doc: "information_schema.CHECK_CONSTRAINTS carries TABLE_NAME (MariaDB only)",
	},
	GeneratedColumns: {
		doc: "columns declared GENERATED ALWAYS AS (expr) STORED (PostgreSQL 12+, MySQL 5.7+, MariaDB 10.2+, SQLite 3.31+)",
	},
	DeferrableConstraints: {
		doc: "foreign keys declared DEFERRABLE, whose check can be postponed to the end of a transaction (PostgreSQL, YugabyteDB, SQLite)",
		// Deliberately no `requires: ForeignKeys`, for the reason
		// DropIndexConcurrently states about CreateIndexConcurrently: a caller
		// composing a set with .With(ForeignKeys, false) is restricting foreign
		// keys, not asserting anything about this key, and Validate must not
		// turn that composition into an error. Measured, the edge did exactly
		// that -- four renderer tests that disable foreign keys stopped
		// reaching their own refusal and failed on an invalid set instead.
	},
}

// mutexGroups lists capability groups in which AT MOST ONE member may be
// enabled: they describe mutually exclusive modelings of the same concept.
var mutexGroups = [][]Capability{
	{EnumInlineColumn, EnumCustomType},
	{ForeignKeysRequireUniqueReference, ForeignKeysRequireIndexedReference, ForeignKeysCreateBackingIndex},
}

var foreignKeyReferencePolicies = []Capability{
	ForeignKeysRequireUniqueReference,
	ForeignKeysRequireIndexedReference,
	ForeignKeysCreateBackingIndex,
}

// Capabilities is a set of feature flags describing one concrete target, as
// map[Capability]bool. The nil set is valid and conservative (Has always
// reports false). Presets in this package enumerate every registry key
// explicitly; hand-built sets should be checked with Validate.
type Capabilities map[Capability]bool

// Has reports whether the capability is present AND enabled. It is nil-safe:
// a nil set has nothing, which is the conservative reading consumers rely on.
func (c Capabilities) Has(key Capability) bool {
	return c != nil && c[key]
}

// Clone returns an independent copy of the set (nil stays nil).
func (c Capabilities) Clone() Capabilities {
	if c == nil {
		return nil
	}
	out := make(Capabilities, len(c))
	maps.Copy(out, c)
	return out
}

// With returns a copy of the set with one capability overridden. The receiver
// is never mutated, so presets can be composed safely:
//
//	caps := capability.Postgres16().With(capability.CreateIndexConcurrently, false)
func (c Capabilities) With(key Capability, enabled bool) Capabilities {
	out := c.Clone()
	if out == nil {
		out = make(Capabilities, 1)
	}
	out[key] = enabled
	return out
}

// Validate checks the set against the registry:
//
//   - every key must be a known, registered capability (typos fail fast);
//   - every enabled capability's requirements must also be enabled (e.g.
//     DropConstraintIfExists requires DropConstraintGeneric — an IF EXISTS
//     variant of a statement the target does not have is a contradiction);
//   - within each mutual-exclusion group at most one member may be enabled
//     (e.g. a dialect models enums inline OR as custom types, never both).
//
// A nil or empty set is valid.
func (c Capabilities) Validate() error {
	for key := range c {
		if _, known := registry[key]; !known {
			return fmt.Errorf("unknown capability %q", key)
		}
	}
	for key, enabled := range c {
		if !enabled {
			continue
		}
		for _, req := range registry[key].requires {
			if !c.Has(req) {
				return fmt.Errorf("capability %q requires %q, which is not enabled", key, req)
			}
		}
	}
	for _, group := range mutexGroups {
		var enabled []string
		for _, key := range group {
			if c.Has(key) {
				enabled = append(enabled, string(key))
			}
		}
		if len(enabled) > 1 {
			return fmt.Errorf("capabilities %s are mutually exclusive", strings.Join(enabled, " and "))
		}
	}
	if c.Has(ForeignKeys) {
		enabled := 0
		for _, key := range foreignKeyReferencePolicies {
			if c.Has(key) {
				enabled++
			}
		}
		if enabled != 1 {
			return fmt.Errorf("capability %q requires exactly one foreign-key reference policy", ForeignKeys)
		}
	}
	return nil
}

// Doc returns the registry documentation line for a capability (empty for
// unknown keys).
func Doc(key Capability) string {
	return registry[key].doc
}

// All returns every registered capability. The order is unspecified; sort
// before rendering user-facing output.
func All() []Capability {
	out := make([]Capability, 0, len(registry))
	for key := range registry {
		out = append(out, key)
	}
	return out
}

// MySQL84 is the preset for MySQL 8.4 and newer. Notably NO IF EXISTS on
// constraint or index drops — plans must be exactly-once by construction
// (see the MySQL planner's constraint-drop ownership rules, issue #207).
//
// Object kinds, measured live on MySQL 9.7.1: CREATE VIEW, CREATE FUNCTION
// and CREATE TRIGGER all succeed. MaterializedViews is off for a reason worth
// reading, because accepting the statement is the wrong test here —
// CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) FROM t is ACCEPTED at exit 0
// (the nonsense sibling CREATE NONSENSE VIEW is refused at exit 1, so the
// probe can see a refusal), and information_schema then reports mv with
// table_type VIEW. Selecting from it before and after an INSERT returns 0 and
// then 1: the result is recomputed, not stored, so the word MATERIALIZED is
// parsed and dropped. This key names a view whose result is stored, and
// MySQL has none.
func MySQL84() Capabilities {
	return Capabilities{
		DomainTypes:                    false,
		CompositeTypes:                 false,
		RangeTypes:                     false,
		DropConstraintGeneric:          true,
		DropConstraintIfExists:         false,
		DropIndexIfExists:              false,
		CheckConstraintsEnforced:       true,
		DropCheckClause:                true,
		EnumInlineColumn:               true,
		EnumCustomType:                 false,
		CreateIndexConcurrently:        false,
		DropIndexConcurrently:          false,
		IndexIncludeSPGiST:             false,
		Views:                          true,
		MaterializedViews:              false,
		Functions:                      true,
		Procedures:                     true,
		Triggers:                       true,
		CreateOrReplaceTrigger:         false,
		AlterGeneratedColumnExpression: false,
		RowLevelSecurity:               false,
		PostgresCatalogFunctions:       false,
		CatalogRowStatistics:           false,
		CatalogDependencies:            false,
		// RoleManagement is on because the read half exists. It was off with the
		// recorded reason that Ptah cannot read or compare a role here, and the
		// catalog says otherwise: measured on MySQL 8.4, a role is a row in
		// mysql.user marked account_locked with its password expired and an empty
		// authentication string, and on MariaDB 11.8 it carries is_role -- which
		// is why the reader asks which columns exist rather than branching on a
		// dialect name (stokaro/ptah#1762).
		//
		// What the key does not claim is the PostgreSQL role model. CREATE ROLE
		// takes a name and nothing else here: LOGIN and PASSWORD are ERROR 1064,
		// because what they ask for is a USER, and a declaration carrying one is
		// refused rather than created without it.
		RoleManagement:                     true,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          false,
		XMLType:                            false,
		AdvisoryLocks:                      false,
		RowLevelTTL:                        false,
		MigrationTimeouts:                  true,
		TransactionalDDL:                   false,
		CheckGrantStatement:                false,
		CatalogViewDependencies:            true,
		ShowRoutinePrivilege:               true,
		RenameColumnClause:                 true,
		CatalogCheckConstraintTableName:    false,
		GeneratedColumns:                   true,
		DeferrableConstraints:              false,
	}
}

// MySQL8019 is the preset for MySQL 8.0.19–8.3. It permits foreign keys that
// reference nonunique indexes, unlike MySQL 8.4 and newer.
func MySQL8019() Capabilities {
	return MySQL84().
		With(ForeignKeysRequireUniqueReference, false).
		With(ForeignKeysRequireIndexedReference, true).
		// SHOW_ROUTINE arrived in 8.0.20, one patch above this arm.
		With(ShowRoutinePrivilege, false)
}

// MySQL8020 is the preset for MySQL 8.0.20 through 8.0.x: the same set as
// [MySQL8019] plus the global SHOW_ROUTINE privilege, which MySQL introduced in
// 8.0.20 and which the metadata-visibility check demands exactly where a server
// can grant it (stokaro/ptah#916 item 3).
func MySQL8020() Capabilities {
	return MySQL8019().With(ShowRoutinePrivilege, true)
}

// MySQL8016 is the preset for MySQL 8.0.16–8.0.18: CHECK constraints are
// enforced, but the generic DROP CONSTRAINT clause does not exist yet (CHECK
// drops must use ALTER TABLE ... DROP CHECK).
func MySQL8016() Capabilities {
	return MySQL8019().With(DropConstraintGeneric, false)
}

// MySQLLegacy is the preset for MySQL before 8.0.16: no generic
// DROP CONSTRAINT, no DROP CHECK, and CHECK constraints are parsed but not
// enforced.
func MySQLLegacy() Capabilities {
	return MySQL8016().
		With(CheckConstraintsEnforced, false).
		With(DropCheckClause, false).
		// information_schema.VIEW_TABLE_USAGE arrived in 8.0.13, which is
		// inside this arm's range rather than above it -- hence [MySQL8013].
		With(CatalogViewDependencies, false)
}

// MySQL8013 is the preset for MySQL 8.0.13 through 8.0.15: [MySQLLegacy] plus
// information_schema.VIEW_TABLE_USAGE, which MySQL added in 8.0.13.
//
// It exists because that threshold falls INSIDE the legacy arm rather than at
// its edge: 8.0.13 is below the 8.0.16 CHECK-enforcement step, so the catalog
// arrives before the constraint behavior does and neither arm can carry both
// (stokaro/ptah#916 item 3).
func MySQL8013() Capabilities {
	return MySQLLegacy().With(CatalogViewDependencies, true)
}

// MariaDB1011 is the preset for the current MariaDB LTS line (10.6+ /
// 10.11 / 11.x share these): IF EXISTS guards are available on both
// constraint and index drops.
//
// Object kinds, measured live on MariaDB 10.11.18: CREATE VIEW,
// CREATE FUNCTION and CREATE TRIGGER succeed; CREATE MATERIALIZED VIEW is
// refused at exit 1, the same exit the nonsense control gets. Unlike MySQL,
// MariaDB does not quietly accept the keyword.
func MariaDB1011() Capabilities {
	return Capabilities{
		DomainTypes:                    false,
		CompositeTypes:                 false,
		RangeTypes:                     false,
		DropConstraintGeneric:          true,
		DropConstraintIfExists:         true,
		DropIndexIfExists:              true,
		CheckConstraintsEnforced:       true,
		DropCheckClause:                false,
		EnumInlineColumn:               true,
		EnumCustomType:                 false,
		CreateIndexConcurrently:        false,
		DropIndexConcurrently:          false,
		IndexIncludeSPGiST:             false,
		Views:                          true,
		MaterializedViews:              false,
		Functions:                      true,
		Procedures:                     true,
		Triggers:                       true,
		CreateOrReplaceTrigger:         true,
		AlterGeneratedColumnExpression: false,
		RowLevelSecurity:               false,
		PostgresCatalogFunctions:       false,
		CatalogRowStatistics:           false,
		CatalogDependencies:            false,
		// RoleManagement is on because the read half exists. It was off with the
		// recorded reason that Ptah cannot read or compare a role here, and the
		// catalog says otherwise: measured on MySQL 8.4, a role is a row in
		// mysql.user marked account_locked with its password expired and an empty
		// authentication string, and on MariaDB 11.8 it carries is_role -- which
		// is why the reader asks which columns exist rather than branching on a
		// dialect name (stokaro/ptah#1762).
		//
		// What the key does not claim is the PostgreSQL role model. CREATE ROLE
		// takes a name and nothing else here: LOGIN and PASSWORD are ERROR 1064,
		// because what they ask for is a USER, and a declaration carrying one is
		// refused rather than created without it.
		RoleManagement:                     true,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  false,
		ForeignKeysRequireIndexedReference: true,
		ForeignKeysCreateBackingIndex:      false,
		// Sequences is on because the three halves the key requires now all
		// exist. MariaDB the engine has had SEQUENCE objects since 10.3; what
		// was missing was Ptah's side, named here as introspection and
		// MySQL-family planning. The planning was added with SQL Server and is
		// capability-gated, the renderer emits MariaDB's own grammar -- NOCYCLE
		// is one word here, `NO CYCLE` is ERROR 1064 -- and the reader reads the
		// sequence's own row, which is the only place MariaDB reports the cache
		// size. Measured on MariaDB 12.3, and MySQL keeps the key off because
		// `CREATE SEQUENCE` there is a syntax error (stokaro/ptah#1759).
		Sequences:                       true,
		XMLType:                         false,
		AdvisoryLocks:                   false,
		RowLevelTTL:                     false,
		MigrationTimeouts:               true,
		TransactionalDDL:                false,
		CheckGrantStatement:             false,
		CatalogViewDependencies:         false,
		ShowRoutinePrivilege:            false,
		RenameColumnClause:              true,
		CatalogCheckConstraintTableName: true,
		GeneratedColumns:                true,
		DeferrableConstraints:           false,
	}
}

// MariaDBLegacy is the conservative preset for MariaDB before 10.2 (EOL
// lines): no generic DROP CONSTRAINT, no enforced CHECK constraints, and no
// IF EXISTS guards are assumed (a floor, deliberately below what late 10.1
// releases could do). ForServerVersion maps pre-10.2 version strings here so
// a modern preset is never over-promised to an old server.
func MariaDBLegacy() Capabilities {
	return MariaDB1011().
		With(DropConstraintGeneric, false).
		With(DropConstraintIfExists, false).
		With(DropIndexIfExists, false).
		With(CheckConstraintsEnforced, false).
		With(CreateOrReplaceTrigger, false)
}

// Postgres16 is the preset for PostgreSQL 14–16.
//
// Object kinds, measured live on PostgreSQL 17: all four create at exit 0
// (with CREATE NONSENSE VIEW refused at exit 1 as the control), and each is
// reported back by the catalog — pg_views, pg_matviews, pg_proc, pg_trigger.
// The materialized view is a real one: selecting from it returns the same
// count before and after an INSERT into its source table, so the result is
// stored rather than recomputed.
func Postgres16() Capabilities {
	return Capabilities{
		DropConstraintGeneric:              true,
		DropConstraintIfExists:             true,
		DropIndexIfExists:                  true,
		CheckConstraintsEnforced:           true,
		DropCheckClause:                    false,
		EnumInlineColumn:                   false,
		EnumCustomType:                     true,
		CreateIndexConcurrently:            true,
		DropIndexConcurrently:              true,
		IndexIncludeSPGiST:                 true,
		Views:                              true,
		MaterializedViews:                  true,
		DomainTypes:                        true,
		CompositeTypes:                     true,
		RangeTypes:                         true,
		Functions:                          true,
		Procedures:                         true,
		Triggers:                           true,
		CreateOrReplaceTrigger:             true,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   true,
		PostgresCatalogFunctions:           true,
		CatalogRowStatistics:               true,
		CatalogDependencies:                true,
		RoleManagement:                     true,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          true,
		XMLType:                            true,
		AdvisoryLocks:                      true,
		RowLevelTTL:                        false,
		MigrationTimeouts:                  true,
		TransactionalDDL:                   true,
		CheckGrantStatement:                false,
		CatalogViewDependencies:            true,
		ShowRoutinePrivilege:               false,
		RenameColumnClause:                 true,
		CatalogCheckConstraintTableName:    false,
		GeneratedColumns:                   true,
		DeferrableConstraints:              true,
	}
}

// Postgres17 is the preset for PostgreSQL 17+.
func Postgres17() Capabilities {
	return Postgres16().With(AlterGeneratedColumnExpression, true)
}

// Postgres13 is the preset for PostgreSQL 12–13: unlike Postgres16 it lacks
// CREATE OR REPLACE TRIGGER and SP-GiST INCLUDE columns, which both arrived in
// PostgreSQL 14.
func Postgres13() Capabilities {
	return Postgres16().
		With(CreateOrReplaceTrigger, false).
		With(IndexIncludeSPGiST, false)
}

// ClickHouse24 is the preset for the ClickHouse 24.x line. It is deliberately
// minimal: ClickHouse models constraints and indexes so differently that the
// shared capability gates mostly do not apply; enums are inline column types
// (Enum8/Enum16).
//
// Object kinds, measured live on ClickHouse 24.8.14: CREATE VIEW and
// CREATE MATERIALIZED VIEW succeed and system.tables reports engines View and
// MaterializedView; CREATE TRIGGER is a syntax error, and the server's own
// error text enumerates what CREATE accepts, with no TRIGGER in the list.
// Functions is off because ClickHouse's CREATE FUNCTION takes a lambda
// (CREATE FUNCTION f AS (x) -> x + 1, which succeeds) rather than the return
// type, language and body this key names — that spelling is a syntax error,
// the same reason ClickHouse's row policies read as absent from
// RowLevelSecurity.
//
// Ptah renders, plans, and introspects plain views and materialized views
// alike, so MaterializedViews describes the generator and not only the engine.
// The three parts differ from the PostgreSQL spelling and were measured on
// server 26.7.3.19, the image docker-compose.yaml pins:
//
//   - CREATE writes ENGINE = MergeTree ORDER BY tuple(), which is what the
//     server records for a materialized view created without a storage clause.
//     POPULATE is never written: it backfills existing rows once, and two views
//     created with and without it report identical as_select, so no read could
//     tell them apart.
//   - DROP is spelled DROP VIEW. DROP MATERIALIZED VIEW is a syntax error, and
//     the server's own list of what DROP accepts has VIEW and not MATERIALIZED
//     VIEW.
//   - REFRESH has no statement at all, so RefreshMaterializedViewNode stays a
//     named diagnostic. A ClickHouse materialized view is kept current by
//     inserts into its source rather than by a refresh command.
//
// Stored rather than recomputed, the reading this key names: a plain view and a
// materialized view over the same "SELECT count(*) AS c FROM users" both moved
// to 1 on an INSERT, and a following TRUNCATE TABLE users left the materialized
// view at 1 while the plain view fell back to 0.
//
// The TO target form is not emitted. The shared materialized-view node carries
// a name and a body, so a target table it does not name cannot be planned; the
// storage clause is the self-contained shape that node can express.
func ClickHouse24() Capabilities {
	return Capabilities{
		DomainTypes:    false,
		CompositeTypes: false,
		RangeTypes:     false,
		// Five keys below were false until stokaro/ptah#916 measured them.
		// ClickHouse 24.10.4.191 and 26.7.3.19 answer identically on every one,
		// so the corrections belong to the dialect rather than to a line:
		// ALTER TABLE DROP CONSTRAINT is accepted, its IF EXISTS guard is
		// honored (the unguarded form on an absent constraint is refused), a
		// CHECK constraint refuses the violating INSERT and accepts the control
		// one, ALTER TABLE DROP INDEX honors IF EXISTS, and
		// MODIFY COLUMN ... MATERIALIZED rewrites a generated expression in
		// place.
		//
		// Two more looked wrong and are not, which is why the experiments below
		// them ask the shape the KEY names rather than any statement the server
		// accepts: see the Functions and RowLevelTTL comments further down.
		//
		// Every one understated the server, so nothing was emitting DDL
		// ClickHouse refuses; the cost was capability rather than correctness.
		DropConstraintGeneric:    true,
		DropConstraintIfExists:   true,
		DropIndexIfExists:        true,
		CheckConstraintsEnforced: true,
		DropCheckClause:          false,
		EnumInlineColumn:         true,
		EnumCustomType:           false,
		CreateIndexConcurrently:  false,
		DropIndexConcurrently:    false,
		IndexIncludeSPGiST:       false,
		Views:                    true,
		MaterializedViews:        true,
		// NOT the lambda alias `CREATE FUNCTION fn AS (x) -> x + 1`, which
		// ClickHouse accepts. This key names the object ast.CreateFunctionNode
		// describes -- a return type, a language and a body -- and that shape is
		// a syntax error here. Measured both ways on 26.7.3.19.
		Functions:                      false,
		Procedures:                     false,
		Triggers:                       false,
		CreateOrReplaceTrigger:         false,
		AlterGeneratedColumnExpression: true,
		// RowLevelSecurity is on because all three halves the key requires now
		// exist: the renderer emits CREATE/ALTER/DROP ROW POLICY, this reader
		// takes system.row_policies back into DBSchema.RLSPolicies, and the
		// planner plans them. The key used to be false with no reason recorded
		// at all -- unlike Functions above it, whose false is measured and
		// explained -- for an engine that has had row policies for years
		// (stokaro/ptah#1736).
		//
		// What it does not claim is a PostgreSQL policy running here unchanged.
		// FOR covers ALL and SELECT and nothing else, and WITH CHECK parses and
		// is then ignored, so a declaration carrying either is named and skipped
		// rather than rendered into something weaker than it says.
		RowLevelSecurity:         true,
		PostgresCatalogFunctions: false,
		CatalogRowStatistics:     false,
		CatalogDependencies:      false,
		// Measured live on 24.10.4.191 and 26.7.3.19: CREATE ROLE, DROP ROLE,
		// GRANT, REVOKE and REVOKE GRANT OPTION FOR all work on both lines, and
		// system.roles and system.grants read them back. Only the catalog's
		// column set differs (24.10 has 8 columns in system.grants, 26.7 adds
		// access_object and is_wildcard), which is a projection question for
		// the reader rather than a capability difference — so this key is true
		// for every ClickHouse line the matrix declares, and there is no
		// version ladder to hang it off.
		//
		// What ClickHouse does NOT have, and this key does not claim: role
		// attributes of any kind (system.roles is name, id, storage), users,
		// role membership, quotas, row policies and settings profiles.
		// internal/clickhouserbac refuses what cannot be represented.
		RoleManagement:                     true,
		ForeignKeys:                        false,
		ForeignKeysRequireUniqueReference:  false,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          false,
		XMLType:                            false,
		AdvisoryLocks:                      false,
		// NOT the MergeTree `TTL <expr>` clause, which ClickHouse accepts. This
		// key names a row-expiry policy declared as STORAGE PARAMETERS, the
		// shape CockroachDB answers and the probe reads back out of
		// pg_class.reloptions; `WITH (ttl_expiration_expression = ...)` is a
		// syntax error here. Measured both ways on 26.7.3.19.
		RowLevelTTL:                     false,
		MigrationTimeouts:               false,
		TransactionalDDL:                false,
		CheckGrantStatement:             false,
		CatalogViewDependencies:         false,
		ShowRoutinePrivilege:            false,
		RenameColumnClause:              true,
		CatalogCheckConstraintTableName: false,
		GeneratedColumns:                false,
		DeferrableConstraints:           false,
	}
}

// ClickHouse2411 is the preset for ClickHouse 24.11 and above.
//
// It differs from [ClickHouse24] in exactly one key, and that key is the reason
// this dialect has a version ladder at all. Measured on the two lines the matrix
// declares furthest apart, `CHECK GRANT SHOW DATABASES, SHOW TABLES ON *.*`
// answers 1 on 26.7.3.19 and is `Syntax error` on 24.10.4.191; every other
// registered key answers identically on both, which is why the arm is one line
// long rather than a second transcription of the whole set
// (stokaro/ptah#916 item 1).
func ClickHouse2411() Capabilities {
	return ClickHouse24().With(CheckGrantStatement, true)
}

// SQLite3 is the preset for modern SQLite 3.x. SQLite enforces CHECK
// constraints and declarative foreign keys when PRAGMA foreign_keys is enabled
// per connection, but it has no native enum, schema, sequence, role, RLS, or
// advisory-lock surface.
//
// Object kinds, measured live on SQLite 3.51.0: CREATE VIEW and
// CREATE TRIGGER succeed and sqlite_master reports both;
// CREATE MATERIALIZED VIEW and CREATE FUNCTION are syntax errors. A SQLite
// user-defined function is registered by the host application through
// sqlite3_create_function, so there is no DDL object for one to plan.
func SQLite3() Capabilities {
	return Capabilities{
		DomainTypes:                        false,
		CompositeTypes:                     false,
		RangeTypes:                         false,
		DropConstraintGeneric:              false,
		DropConstraintIfExists:             false,
		DropIndexIfExists:                  true,
		CheckConstraintsEnforced:           true,
		DropCheckClause:                    false,
		EnumInlineColumn:                   false,
		EnumCustomType:                     false,
		CreateIndexConcurrently:            false,
		DropIndexConcurrently:              false,
		IndexIncludeSPGiST:                 false,
		Views:                              true,
		MaterializedViews:                  false,
		Functions:                          false,
		Procedures:                         false,
		Triggers:                           true,
		CreateOrReplaceTrigger:             false,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   false,
		PostgresCatalogFunctions:           false,
		CatalogRowStatistics:               false,
		CatalogDependencies:                false,
		RoleManagement:                     false,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          false,
		XMLType:                            false,
		AdvisoryLocks:                      false,
		RowLevelTTL:                        false,
		MigrationTimeouts:                  false,
		TransactionalDDL:                   true,
		CheckGrantStatement:                false,
		CatalogViewDependencies:            false,
		ShowRoutinePrivilege:               false,
		RenameColumnClause:                 true,
		CatalogCheckConstraintTableName:    false,
		GeneratedColumns:                   true,
		DeferrableConstraints:              true,
	}
}

// SQLite324 is the preset for SQLite below 3.25, which has no
// ALTER TABLE ... RENAME COLUMN clause: a rename there is a table rebuild.
//
// The engine Ptah links is the modernc.org/sqlite amalgamation pinned in
// go.mod, and it is far above that floor -- so this arm is not about the
// database Ptah opens. It is about the one a rendered file is destined for:
// `--server-version 3.24` on an offline render is a user saying the consumer of
// this DDL is older than Ptah's own engine, which is exactly the case
// stokaro/ptah#916 item 5 exists for.
func SQLite324() Capabilities {
	return SQLite3().
		With(RenameColumnClause, false).
		// Generated columns arrived in 3.31, well above this arm's ceiling, so
		// a target pinned here cannot parse the clause either. Measured on the
		// linked engine for the upper arm: sqlite_version() 3.53.3 accepts
		// `GENERATED ALWAYS AS (n + 1) STORED`.
		With(GeneratedColumns, false)
}

// SQLServer2022 is the preset for the portable SQL Server/Azure SQL DDL subset
// Ptah targets initially: schemas, tables, IDENTITY columns, CHECK/UNIQUE/FK
// constraints, indexes, views, and triggers are available; standalone sequence
// objects, native enum, PostgreSQL RLS, extension, and advisory-lock surfaces
// are not.
//
// Object kinds, measured live on SQL Server product version 17.0.4065.4 (the
// image docker-compose.yaml pins): CREATE VIEW, CREATE FUNCTION and
// CREATE TRIGGER succeed; CREATE MATERIALIZED VIEW is "Incorrect syntax",
// the same refusal the nonsense control gets. SQL Server's stored-result
// equivalent is an indexed view — a plain view plus a clustered index rather
// than its own object kind.
func SQLServer2022() Capabilities {
	return Capabilities{
		DomainTypes:           false,
		CompositeTypes:        false,
		RangeTypes:            false,
		DropConstraintGeneric: true,
		// Both IF EXISTS guards are ACCEPTED, measured on the three release
		// lines Microsoft supports -- 15.0.4480.2, 16.0.4265.3 and
		// 17.0.4075.5 -- with `ALTER TABLE ... DROP CONSTRAINT IF EXISTS` and
		// `DROP INDEX IF EXISTS <name> ON <table>`. They read false here until
		// stokaro/ptah#916 because the preset was written from the PostgreSQL
		// and MySQL answers, where the guards are the exception rather than
		// the rule, and no SQL Server statement had been asked.
		DropConstraintIfExists:   true,
		DropIndexIfExists:        true,
		CheckConstraintsEnforced: true,
		DropCheckClause:          false,
		EnumInlineColumn:         false,
		EnumCustomType:           false,
		CreateIndexConcurrently:  false,
		DropIndexConcurrently:    false,
		IndexIncludeSPGiST:       false,
		Views:                    true,
		MaterializedViews:        false,
		// Functions is off for the reason Sequences is off on MariaDB, and the
		// key's own doc comment states the rule: it describes Ptah's generator,
		// not the engine's brochure. SQL Server hosts scalar functions happily
		// -- CREATE FUNCTION [dbo].[f]() RETURNS int AS BEGIN RETURN 1 END and
		// its CREATE OR ALTER form are both ACCEPTED on 2025 (RTM-CU7) -- but
		// no Ptah code path reads one back. sys.sql_modules.definition returns
		// the whole original CREATE statement as one string rather than a body
		// plus attributes, so reconstructing the parameters, return type and
		// body that a diff compares needs a T-SQL routine-header parser that
		// does not exist here. Emitting without reading is precisely the
		// permanent diff stokaro/ptah#929 is about, so the key stays false
		// until the reader lands.
		// Functions is on because the read half now exists. It was off because
		// nothing could recover a function from the catalog, and a key that
		// promises a create with no read plans the same statement forever.
		//
		// The blocker turned out to be narrower than it looked. The header does
		// not have to be parsed out of sys.sql_modules.definition:
		// INFORMATION_SCHEMA.PARAMETERS publishes the argument list and the
		// return type as rows, ordinal zero being the return, exactly as MySQL's
		// information_schema.ROUTINES does. Only the body comes out of the
		// statement text (stokaro/ptah#1720).
		Functions:                      true,
		Procedures:                     false,
		Triggers:                       true,
		CreateOrReplaceTrigger:         true,
		AlterGeneratedColumnExpression: false,
		// RowLevelSecurity is on because all three halves the key requires now
		// exist for this target: the renderer emits CREATE/DROP SECURITY
		// POLICY, internal/dbschema/mssql reads sys.security_policies joined
		// to sys.security_predicates back into DBSchema.RLSPolicies, and the
		// shared planner plans them. The engine has had it since 2016
		// (stokaro/ptah#1699).
		//
		// What the key does not claim is that a PostgreSQL policy runs here
		// unchanged. It cannot: T-SQL rejects an inline predicate expression
		// outright, so a declaration carrying one is named and skipped.
		RowLevelSecurity:         true,
		PostgresCatalogFunctions: false,
		CatalogRowStatistics:     false,
		CatalogDependencies:      false,
		// RoleManagement is on for the same reason Sequences is: the three
		// halves the key requires exist for this target. The renderer emits
		// T-SQL CREATE ROLE, GRANT and REVOKE, internal/dbschema/mssql reads
		// sys.database_principals and sys.database_permissions back, and the
		// shared planner plans them. A SQL Server DATABASE role has none of
		// PostgreSQL's cluster attributes -- `CREATE ROLE [r] LOGIN` is
		// `Incorrect syntax near 'LOGIN'` on 17.0.4075.5 -- so a declaration
		// carrying one gets the role and a line naming what was not honored
		// (stokaro/ptah#1698).
		RoleManagement:                     true,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		// Sequences is on because all three halves the key requires now exist
		// for this target: the renderer emits T-SQL CREATE/ALTER/DROP SEQUENCE,
		// internal/dbschema/mssql reads sys.sequences back into
		// DBSchema.Sequences, and the shared planner plans them. It was false
		// while any of those was missing, which is what the key means -- it
		// describes Ptah's generator, not the engine's brochure -- and the
		// engine itself has had CREATE SEQUENCE since 2012
		// (stokaro/ptah#1626).
		Sequences:                       true,
		XMLType:                         true,
		AdvisoryLocks:                   false,
		RowLevelTTL:                     false,
		MigrationTimeouts:               false,
		TransactionalDDL:                false,
		CheckGrantStatement:             false,
		CatalogViewDependencies:         true,
		ShowRoutinePrivilege:            false,
		RenameColumnClause:              false,
		CatalogCheckConstraintTableName: false,
		GeneratedColumns:                false,
		DeferrableConstraints:           false,
	}
}

// CockroachDB23 is the historical preset for CockroachDB's
// PostgreSQL-compatible surface.
// CockroachDB runs schema changes online by design, so PostgreSQL's
// CONCURRENTLY keyword is not a meaningful or portable emission target. It
// accepts the keyword inside an explicit transaction on v26.2.5, which proves
// the syntax is parsed as a compatibility no-op rather than as PostgreSQL's
// non-transactional concurrent index build. XML columns and PostgreSQL
// advisory-lock functions are outside Ptah's portable CockroachDB subset.
//
// Object kinds, measured live on CockroachDB CCL v26.2.5 (the image
// docker-compose.yaml pins): CREATE VIEW, CREATE MATERIALIZED VIEW,
// CREATE FUNCTION ... LANGUAGE plpgsql and CREATE TRIGGER all succeed, with
// CREATE NONSENSE VIEW refused at exit 1 as the control. The materialized
// view is a real one: after two INSERTs into the source table the plain view
// reports 2 while the materialized view still reports 0, so the result is
// stored rather than recomputed.
//
// The trigger row is the one worth spelling out, because this preset is named
// for a line that did not have triggers — CockroachDB added them in v24.3.
// The exported historical preset retains its pre-matrix behavior; the live
// resolver now selects CockroachDB25 or CockroachDB26 for those measured lines.
//
// The same v26.2.5 probe accepted CREATE ROLE plus GRANT SELECT, ALTER TABLE
// ... ENABLE ROW LEVEL SECURITY plus CREATE POLICY, CREATE SEQUENCE, and
// CREATE TABLE with SERIAL. Those keys stay enabled because otherwise Ptah
// refuses objects this measured line can host.
//
// RowLevelTTL is the one key this preset turns ON rather than off, and it is
// the only place in this file where a CockroachDB preset ADDS to PostgreSQL's
// surface. Measured on v25.4.14 and v26.2.5 alike: `CREATE TABLE t (id INT
// PRIMARY KEY, expires_at TIMESTAMPTZ) WITH (ttl_expiration_expression =
// 'expires_at')` succeeds and pg_class.reloptions then reports
// `{ttl='on',ttl_expiration_expression='expires_at',...}`, while PostgreSQL
// 18.4 answers `ERROR: unrecognized parameter`. It is set here rather than on
// the per-line presets because both measured lines answered identically to
// every probe in [go.5x5.cz/ptah/internal/crdbttl]'s measured table
// (stokaro/ptah#1027).
func CockroachDB23() Capabilities {
	return Postgres16().
		// CockroachDB is the one PostgreSQL-family target without this, and it
		// is a whole absence rather than a partial one: v26.2.5 answers
		// `unimplemented: this syntax` to DEFERRABLE, to DEFERRABLE INITIALLY
		// IMMEDIATE and to NOT DEFERRABLE alike (stokaro/ptah#1624).
		With(DeferrableConstraints, false).
		With(CreateIndexConcurrently, false).
		With(DropIndexConcurrently, false).
		With(IndexIncludeSPGiST, false).
		With(XMLType, false).
		With(AdvisoryLocks, false).
		// Measured 2026-08-19 on v26.2.5, with CREATE TYPE ... AS ENUM and
		// CREATE TABLE as the controls: `CREATE DOMAIN` and
		// `CREATE TYPE ... AS RANGE` both answer "not yet implemented"
		// (cockroachdb/cockroach#27796 and #27791), while a composite
		// `CREATE TYPE ... AS (...)` is accepted. Three keys rather than one
		// because of that split (stokaro/ptah#1717).
		//
		// #27796 was closed on 2026-07-23 and v26.2.5 is the newest published
		// release, so the domain answer belongs to a preset for the release
		// that carries it rather than to an edit of this one. That expiry is
		// tracked in stokaro/ptah#1735, whose blocker is the release itself --
		// a comment alone outlives the fact it records.
		With(DomainTypes, false).
		With(RangeTypes, false).
		With(RowLevelTTL, true)
}

// CockroachDB25 is the preset measured on CockroachDB 25.4. The line refuses
// both generic DROP CONSTRAINT and CREATE OR REPLACE TRIGGER, which the 26.2
// line accepts. The remaining registered capabilities match CockroachDB26.
func CockroachDB25() Capabilities {
	return CockroachDB23().
		With(DropConstraintGeneric, false).
		With(DropConstraintIfExists, false).
		With(CreateOrReplaceTrigger, false)
}

// CockroachDB26 is the preset measured on CockroachDB 26.2. It retains the
// full current CockroachDB surface documented by CockroachDB23 while giving
// the version resolver a truthful current-line name.
func CockroachDB26() Capabilities {
	return CockroachDB23()
}

// YugabyteDB25 is the preset for YugabyteDB YSQL. It stays close to
// PostgreSQL for the common DDL subset. Live YugabyteDB 2026.1.0.0 accepted
// CREATE INDEX CONCURRENTLY outside a transaction and refused it inside one
// with PostgreSQL's "cannot run inside a transaction block" shape, so Ptah
// treats the create-side CONCURRENTLY keyword as meaningful there. DROP INDEX
// CONCURRENTLY remains disabled: the same probe refused it as unsupported.
//
// Object kinds, measured live on YugabyteDB 2026.1.0.0 (PostgreSQL 15.12-YB,
// the image docker-compose.yaml pins): all four create at exit 0 with the
// nonsense control refused at exit 1, and pg_views, pg_matviews, pg_proc and
// pg_trigger each report their object. The materialized view stores its
// result — after an INSERT it still reports 0 while the plain view reports 1.
// The same probe accepted advisory lock/unlock calls and row-level security
// policy DDL, matching the enabled keys below.
func YugabyteDB25() Capabilities {
	return Postgres16().
		With(DropIndexConcurrently, false).
		With(IndexIncludeSPGiST, false)
}

// YugabyteDB24 is the preset for the 2024 LTS line, which is where YugabyteDB's
// PostgreSQL engine is still 11.
//
// The two keys below it is the whole difference, and the engine swap is why:
// 2024.2 reports "PostgreSQL 11.2-YB-2024.2.4.0-b0" and 2025.1 reports
// "PostgreSQL 15.12-YB-2025.1.0.0-b0". Measured on both, plus 2026.1, with the
// capability probe -- every other registered key answers identically across the
// three, so this ladder has exactly one step and carries exactly two keys:
//
//   - `SELECT pg_advisory_lock(1)` answers
//     `advisory locks are not yet implemented` (SQLSTATE 0A000) on 2024.2 and
//     succeeds on 2025.1 and 2026.1.
//
//   - `CREATE OR REPLACE TRIGGER` is `syntax error at or near "TRIGGER"` on
//     2024.2, which is PostgreSQL 11 behaving as PostgreSQL 11: the spelling
//     arrived in PostgreSQL 14.
//
//   - `GENERATED ALWAYS AS (n + 1) STORED` is `syntax error at or near "("`
//     here and accepted on both newer lines: generated columns arrived in
//     PostgreSQL 12. This is the one the ladder was actually needed for. The
//     PostgreSQL renderer serves YugabyteDB, so before `generated_columns`
//     existed an offline plan for a 2024 LTS server emitted a clause that
//     server cannot parse.
//
// alter_generated_column_expression is false on both arms and so carries
// nothing: what differs there is why, not what (stokaro/ptah#916).
func YugabyteDB24() Capabilities {
	return YugabyteDB25().
		With(AdvisoryLocks, false).
		With(CreateOrReplaceTrigger, false).
		With(GeneratedColumns, false)
}

// SpannerPostgres is the conservative preset for Cloud Spanner's PostgreSQL
// interface. Spanner's SQL surface is sufficiently different that Ptah only
// routes the simplest PostgreSQL-family statements through this preset; enums,
// sequences, RLS, advisory locks, and XML are disabled. Enforced foreign keys,
// including circular relationships added with ALTER TABLE, are supported.
//
// Object kinds: views yes, the other three no, and this is the row that made
// the four keys necessary. Spanner shares the PostgreSQL planner and renderer,
// so before them nothing could stop a plpgsql function or a trigger from being
// planned and rendered for it. Google documents CREATE VIEW and states in the
// same place that a Spanner view is not a materialized view because it does
// not store the query result; the PostgreSQL-interface migration guidance
// states that Spanner does not run user code in the database, so triggers and
// user-defined stored procedures and functions belong in the application.
//
// This row rests on that documentation alone. Ptah has no Spanner container
// and no live Spanner test (issue #942), so unlike every other preset in this
// file nothing here was executed against a server. Re-measure these four when
// #942 lands.
func SpannerPostgres() Capabilities {
	return Postgres16().
		// Measured on the Cloud Spanner emulator behind PGAdapter:
		// `<DEFERRABLE> constraints are not supported` (stokaro/ptah#1624).
		With(DeferrableConstraints, false).
		// Measured: `Only <TABLE> is supported for renaming`.
		With(RenameColumnClause, false).
		// Measured on the Cloud Spanner emulator behind PGAdapter:
		// `relation "information_schema.view_table_usage" does not exist`.
		With(CatalogViewDependencies, false).
		// Measured, unlike the rest of this preset: on the Cloud Spanner
		// emulator through PGAdapter 0.55.2, `obj_description(2200,
		// 'pg_namespace')` answers `The Postgres Type is not supported: name`.
		With(PostgresCatalogFunctions, false).
		With(CatalogRowStatistics, false).
		With(CatalogDependencies, false).
		// Measured 2026-08-19 on the emulator behind PGAdapter 0.55.2, with
		// CREATE TABLE as the control: all three user-type kinds answer
		// `Statement is not supported` (stokaro/ptah#1717).
		With(DomainTypes, false).
		With(CompositeTypes, false).
		With(RangeTypes, false).
		// Measured on the same endpoint AND confirmed against the PostgreSQL
		// dialect's own reference, which is what separates these three from the
		// assumptions below them: `ALTER TABLE Concerts DROP CONSTRAINT
		// concert_id_gt_0`, `DROP INDEX [ IF EXISTS ] name`, and of a CHECK
		// expression, "if the expression evaluates to FALSE, the data change is
		// not allowed" (stokaro/ptah#942).
		With(DropConstraintGeneric, true).
		With(DropConstraintIfExists, false).
		With(DropIndexIfExists, true).
		With(CheckConstraintsEnforced, true).
		With(EnumCustomType, false).
		With(CreateIndexConcurrently, false).
		With(DropIndexConcurrently, false).
		With(IndexIncludeSPGiST, false).
		With(MaterializedViews, false).
		// Neither runtime policy is measured on this endpoint, and the
		// migrator excluded it from both before there were keys to say so.
		// The key describes what Ptah supports on a target rather than what
		// the wire protocol suggests (stokaro/ptah#1713).
		With(MigrationTimeouts, false).
		With(TransactionalDDL, false).
		With(Functions, false).
		// A procedure is the same routine object with its return type removed,
		// so an endpoint that takes no CREATE FUNCTION takes no CREATE
		// PROCEDURE either (stokaro/ptah#1722).
		With(Procedures, false).
		With(Triggers, false).
		With(CreateOrReplaceTrigger, false).
		With(RowLevelSecurity, false).
		With(RoleManagement, false).
		With(Sequences, false).
		With(XMLType, false).
		With(AdvisoryLocks, false).
		With(ForeignKeysRequireUniqueReference, false).
		With(ForeignKeysCreateBackingIndex, true)
}

var defaultDialectPresets = map[string]func() Capabilities{
	platform.ClickHouse:  ClickHouse24,
	platform.CockroachDB: CockroachDB26,
	platform.MariaDB:     MariaDB1011,
	platform.MySQL:       MySQL84,
	platform.Postgres:    Postgres17,
	platform.Spanner:     SpannerPostgres,
	platform.SQLite:      SQLite3,
	platform.SQLServer:   SQLServer2022,
	platform.YugabyteDB:  YugabyteDB25,
}

// DefaultDialects returns the normalized dialect names for which [ForDialect]
// has a default capability preset.
func DefaultDialects() []string {
	return slices.Sorted(maps.Keys(defaultDialectPresets))
}

// NamedPreset pairs a preset with the name its constructor carries, so a
// document or a report can print the preset a server was given.
type NamedPreset struct {
	// Name is the constructor's name, e.g. "MySQL84".
	Name string
	// Capabilities is what that constructor returns.
	Capabilities Capabilities
}

// NamedPresets returns every preset this package ships, ordered by dialect
// family and then oldest-first inside it, which is the order a reader of the
// documentation matrix follows across a row.
//
// The list is written out rather than derived because Go cannot enumerate the
// exported functions of its own package at run time. What keeps it honest is
// that the same source is parsed by the capability-probe tests: a constructor
// added to this file and left out of the list below fails there, so the list
// cannot go stale in the comfortable direction (stokaro/ptah#916).
func NamedPresets() []NamedPreset {
	return []NamedPreset{
		{"MySQLLegacy", MySQLLegacy()},
		{"MySQL8013", MySQL8013()},
		{"MySQL8016", MySQL8016()},
		{"MySQL8019", MySQL8019()},
		{"MySQL8020", MySQL8020()},
		{"MySQL84", MySQL84()},
		{"MariaDBLegacy", MariaDBLegacy()},
		{"MariaDB1011", MariaDB1011()},
		{"Postgres13", Postgres13()},
		{"Postgres16", Postgres16()},
		{"Postgres17", Postgres17()},
		{"ClickHouse24", ClickHouse24()},
		{"ClickHouse2411", ClickHouse2411()},
		{"CockroachDB23", CockroachDB23()},
		{"CockroachDB25", CockroachDB25()},
		{"CockroachDB26", CockroachDB26()},
		{"YugabyteDB24", YugabyteDB24()},
		{"YugabyteDB25", YugabyteDB25()},
		{"SQLite324", SQLite324()},
		{"SQLite3", SQLite3()},
		{"SQLServer2022", SQLServer2022()},
		{"SpannerPostgres", SpannerPostgres()},
	}
}

// ForDialect returns the default preset for a dialect name (normalized via
// platform.NormalizeDialect): the current supported version line of that
// dialect. Unknown dialects get nil — the conservative empty set.
func ForDialect(dialect string) Capabilities {
	preset := defaultDialectPresets[platform.NormalizeDialect(dialect)]
	if preset == nil {
		return nil
	}
	return preset()
}

// Newest measured version boundary per refined dialect.
//
// Each ladder in this file ends in an open-topped arm: MySQL sends everything
// above 8.4 to MySQL84, MariaDB everything above 10.2 to MariaDB1011, and
// PostgreSQL everything at or above 17 to Postgres17. That arm is a stand-in,
// not a measurement — a server newer than the exact line below was never
// observed behaving like the preset it receives. VersionResolution.Saturated
// is true exactly past that line, so a caller can tell "inside a measured
// line" from "past the newest line this package knows". A sibling minor below
// the measured line is conservative fallback too, but not saturation.
//
// Above these boundaries the preset that comes back is byte-identical to
// ForDialect's, so VersionSpecific is false there too. MySQL, MariaDB and
// CockroachDB distinguish exact measured major/minor lines from conservative
// fallbacks between them.
//
// Raising one of these numbers is the deliberate act of claiming a newer
// server line behaves like the preset it lands on. Do it in the change that
// measures that line, together with the preset it deserves — never as a side
// effect of bumping a container tag.
const (
	// Postgres17 also covers PostgreSQL 18, measured on PostgreSQL 18.4 by the
	// capability matrix.
	newestMeasuredPostgresMajor = 18
)

// VersionResolution reports how a server version string was mapped onto a
// capability preset.
//
// Saturated names the case the resolver used to answer wrongly: the version
// parsed, it selected the newest preset in its dialect's ladder, and it is
// itself newer than the newest line that ladder was measured against — so the
// preset is a stand-in and any capability the newer server gained or lost is
// unmodeled. That is not a version-specific answer, so VersionSpecific is
// false whenever Saturated is true, and the two fields together say which of
// the reasons a caller did not receive an exact measured-line answer: the
// version could not be parsed, it fell between measured lines, or it ran off
// the top of the ladder.
//
// Saturation is defined where this package has a version ladder: MySQL,
// MariaDB, PostgreSQL, and CockroachDB. YugabyteDB and Spanner are resolved
// from the banner without consulting a version at all, and ClickHouse, SQLite,
// and SQL Server have no ladder to saturate; those five report Saturated=false
// and an empty NewestMeasured. Refining those dialects is remaining scope of
// issue #916 and is deliberately not answered here.
type VersionResolution struct {
	// Capabilities is the resolved preset, never nil for a known dialect.
	Capabilities Capabilities
	// VersionSpecific is false when no exact measured release line selected the
	// preset. Most ladders use the dialect default in that case; a ladder with
	// measured minor lines may use the preceding conservative preset between
	// those lines.
	VersionSpecific bool
	// Saturated is true when the resolved preset is the top of a version
	// ladder and the server is newer than the newest line that ladder was
	// measured against. It is the reason VersionSpecific is false, and is
	// never true at the same time as VersionSpecific.
	Saturated bool
	// NewestMeasured names the newest measured version line for the dialect,
	// for example "26.7" or "26.2". It is empty for dialects with no ladder.
	NewestMeasured string
	// Recognized is false when the string named no server at all: the
	// resolver found neither a product banner it answers from directly nor a
	// numeric version its dialect's ladder could read, so the preset it
	// returned was chosen without consulting the string.
	//
	// It is a separate field because the other three cannot express it.
	// Measured on every dialect this package knows: an unreadable string on a
	// laddered dialect (postgres "not-a-version") reports VersionSpecific
	// false, Saturated false and an empty NewestMeasured — and so does a
	// perfectly good version on a dialect with no ladder (sqlite "3.45.0",
	// sqlserver "16.0.4025.1", clickhouse "24.8.4.13"). The two are opposite
	// answers to "was the operator's string usable" and were indistinguishable
	// before this field existed. internal/capabilityprobe.Refinement records
	// the same collision from the other side.
	//
	// Recognized false implies VersionSpecific false; a caller that must
	// refuse an unusable string therefore reads this field alone. A caller
	// holding a live banner should keep ignoring it: SELECT version() is not
	// a typo, and degrading quietly is right there.
	Recognized bool
	// ResolvedDialect names the platform the returned preset belongs to,
	// which is NOT always the dialect that was asked for.
	//
	// A product banner outranks the declared dialect here — that is deliberate
	// and correct on a live connection, where MariaDB announces itself over
	// the MySQL protocol and CockroachDB over the PostgreSQL one, so the
	// string is better evidence than the driver name. Without this field that
	// override is unobservable, because the returned Capabilities carry no
	// record of which platform produced them.
	//
	// This is NOT the answer to "which product does the string name" — that is
	// BannerPlatform, and a caller refusing a contradiction between two typed
	// values must ask it instead. The two disagree for a banner naming only
	// PostgreSQL on a dialect already in the PostgreSQL wire family, which
	// keeps its own preset and so reports its own name here.
	//
	// It is the normalized dialect name, empty only when the dialect itself
	// normalized to nothing.
	ResolvedDialect string
}

// ForServerVersion refines ForDialect using a live server version string —
// typically the result of SELECT version() — so callers can map a concrete
// server to the closest preset at connect time. Recognized shapes include
// "8.0.42", "8.0.42-log", "10.11.6-MariaDB-1:10.11.6+maria~ubu2204",
// "5.5.5-10.11.6-MariaDB" (the replication-protocol prefix MariaDB reports
// over the MySQL protocol) and "PostgreSQL 16.3 (Debian ...)". When the
// version cannot be parsed, the dialect's default preset is returned.
//
// The fallback is silent by design, which is correct for a live banner and
// wrong for a string an operator typed. A caller holding operator input must
// use ResolveServerVersion and refuse a resolution whose Recognized is false,
// because nothing in the returned Capabilities says the string was ignored.
func ForServerVersion(dialect, version string) Capabilities {
	return ResolveServerVersion(dialect, version).Capabilities
}

// ForServerVersionResult is ForServerVersion plus an explicit fallback signal.
// The boolean is false when no exact measured release line selected a preset.
// Callers with a live connection can log that degradation while offline
// callers can keep using ForDialect.
//
// A version newer than the newest measured line for its dialect is one of
// those fallbacks: the ladder's open-topped arm hands back exactly ForDialect's
// preset, so the boolean is false. ResolveServerVersion separates that case
// from a version that could not be parsed at all.
func ForServerVersionResult(dialect, version string) (Capabilities, bool) {
	resolution := ResolveServerVersion(dialect, version)
	return resolution.Capabilities, resolution.VersionSpecific
}

// BannerPlatform reports the database product a version string names, or the
// empty string when it names none — a bare "8.0.42" names no product, and
// neither does an unreadable one.
//
// It answers a different question from VersionResolution.ResolvedDialect.
// This one is about the string alone: which product does it announce? That is
// the question a caller holding operator input has to ask, because two typed
// values naming two different servers are a contradiction no preset can
// resolve. ResolvedDialect answers which ladder the capabilities actually came
// from, and the two deliberately disagree for a PostgreSQL banner on a
// PostgreSQL-family dialect — see ResolveServerVersion.
//
// It is also the only ordered table of product tokens in the tree.
// dbschema's wire-dialect detection reads it rather than repeating the
// substrings, because a second copy is how a live connection and an offline
// resolution come to disagree about which server a banner describes.
//
// The order is load-bearing inside the PostgreSQL wire family: three of those
// products announce the PostgreSQL engine in their own banner — CockroachDB
// speaks the PostgreSQL wire protocol, YugabyteDB reports
// "PostgreSQL 11.2-YB-…" and Spanner "Cloud Spanner PostgreSQL" — so
// PostgreSQL is claimed after every token that is more specific than it. The
// products below it announce no engine but their own, so their order among
// themselves is free.
//
// Membership rule: a product belongs here when the SERVER's own version
// surface names it. That is the string this function is handed, live or typed,
// and it is the only evidence a mismatch guard can act on. Measured across the
// nine dialects platform names:
//
//   - postgres, mariadb, cockroachdb, yugabytedb and spanner put the product in
//     the version string itself, and are claimed above.
//   - sqlserver puts it in @@VERSION: "Microsoft SQL Server 2025 (RTM-CU7) … -
//     17.0.4065.4 …", the banner capability_internal_test.go pins verbatim from
//     a live container. It is the one that had teeth: the generic parser reads
//     the marketing year 2025 out of it, so before this token existed
//     `schema render --dialect postgres --server-version '<that banner>'`
//     exited 0 planning it as a PostgreSQL 2025 that does not exist.
//   - clickhouse has no product token in SELECT version(), which answers a bare
//     "26.7.3.19" — but system.build_options reports VERSION_FULL
//     "ClickHouse 26.7.3.19" and VERSION_NAME "ClickHouse" (measured on
//     clickhouse/clickhouse-server:26.7), and that is the server naming itself.
//   - mysql and sqlite are deliberately absent. Measured, SELECT VERSION() on a
//     live mysql:9.7 answers "9.7.2" and sqlite_version() answers a bare dotted
//     version ("3.51.0" on the SQLite this was run against); neither server has
//     a version surface that names its product, so the empty answer is the
//     correct one and any token would have to come from a client banner
//     instead — and the MySQL client's is shared with MariaDB's
//     ("mysql  Ver 15.1 Distrib 10.11.6-MariaDB"), so it names no server.
func BannerPlatform(version string) string {
	versionLower := strings.ToLower(version)
	switch {
	case strings.Contains(versionLower, "cockroachdb"):
		return platform.CockroachDB
	case strings.Contains(versionLower, "yugabytedb"),
		strings.Contains(versionLower, "yugabyte"),
		strings.Contains(versionLower, "-yb-"):
		return platform.YugabyteDB
	case strings.Contains(versionLower, "spanner"):
		return platform.Spanner
	case strings.Contains(versionLower, "postgres"):
		return platform.Postgres
	case strings.Contains(versionLower, "mariadb"):
		return platform.MariaDB
	case strings.Contains(versionLower, "sql server"):
		return platform.SQLServer
	case strings.Contains(versionLower, "clickhouse"):
		return platform.ClickHouse
	default:
		return ""
	}
}

// ResolveServerVersion is ForServerVersionResult with the saturation answer
// attached. It selects exactly the same preset and reports exactly the same
// VersionSpecific value; Saturated and NewestMeasured say why a saturated
// version is not version-specific and which line it was planned as, and
// Recognized says whether the string was read at all. See VersionResolution
// for what saturation means and which dialects can report it.
func ResolveServerVersion(dialect, version string) VersionResolution {
	normalized := platform.NormalizeDialect(dialect)

	switch banner := BannerPlatform(version); banner {
	case platform.CockroachDB:
		return resolvedAs(cockroachDBResolution(version), platform.CockroachDB)
	case platform.YugabyteDB:
		// YugabyteDB DOES have a ladder, of exactly one step, and it was in
		// this arm returning YugabyteDB25 unconditionally until
		// stokaro/ptah#916 measured the 2024 LTS line: advisory locks are "not
		// yet implemented" there and CREATE OR REPLACE TRIGGER is a syntax
		// error, both because the engine underneath is still PostgreSQL 11.
		return resolvedAs(yugabyteResolution(version), platform.YugabyteDB)
	case platform.Spanner:
		return VersionResolution{
			Capabilities:    SpannerPostgres(),
			VersionSpecific: true,
			Recognized:      true,
			ResolvedDialect: platform.Spanner,
		}
	case platform.Postgres:
		// "PostgreSQL" names the FAMILY, and only the family. CockroachDB,
		// YugabyteDB and Spanner are all reached over the PostgreSQL wire
		// protocol, and each may report a banner carrying no token of its own
		// — which is exactly the case dbschema's detectPostgresWireDialect
		// answers by keeping the dialect the operator connected with. Claiming
		// the banner as PostgreSQL there would overrule that decision and hand
		// a live distributed server the PostgreSQL preset: measured, it turned
		// SpannerPostgres into Postgres14 across 19 keys, among them
		// materialized_views, functions and triggers, which are the keys that
		// exist to stop Ptah emitting DDL Spanner refuses.
		//
		// So the banner is claimed only for a dialect outside that family,
		// where it is a genuine contradiction rather than a less specific
		// spelling of the same server: "PostgreSQL 16.3 (Debian)" on
		// --dialect mysql used to read as MySQL 16.3 and answer MySQL84.
		// A version a person typed is refused before it gets this far by
		// internal/servertarget, which asks BannerPlatform directly and so
		// still refuses a PostgreSQL banner paired with --dialect cockroachdb.
		if !platform.IsPostgresFamily(normalized) {
			return resolvedAs(postgresBannerResolution(version), platform.Postgres)
		}
	case platform.SQLServer:
		// SQL Server has no version ladder, so the banner's own name is the
		// whole answer and the number in it is never spent: the preset is that
		// product's default either way. Answering from the banner rather than
		// falling through to parseVersion is what keeps the marketing year out
		// of a PostgreSQL ladder — "Microsoft SQL Server 2025 … - 17.0.4065.4"
		// parses as major 2025, and on --dialect postgres that used to select
		// Postgres17 and report itself saturated past release line 18.
		//
		// Recognized is true for the same reason it is on the YugabyteDB and
		// Spanner arms: the string named a server, even though nothing in it was
		// read as a number. VersionSpecific stays false because no measured
		// release line was selected — the same answer the unladdered arm at the
		// bottom of this function gives.
		return VersionResolution{
			Capabilities:    ForDialect(banner),
			Recognized:      true,
			ResolvedDialect: banner,
		}
	case platform.ClickHouse:
		// ClickHouse DOES have a ladder, of exactly one step. It was in this arm
		// until stokaro/ptah#916 measured the two declared lines furthest apart
		// and found one key on which they differ: CHECK GRANT is a statement on
		// 26.7.3.19 and a syntax error on 24.10.4.191.
		//
		// The banner is the version here -- ClickHouse answers `SELECT
		// version()` with 24.10.4.191 and nothing else -- so unlike SQL Server
		// there is no marketing year to keep out of a ladder.
		return resolvedAs(clickHouseResolution(version), platform.ClickHouse)
	case platform.MariaDB:
		// MariaDB announces itself in the version string even when connected
		// via the mysql dialect/driver; trust the string over the declared
		// dialect.
		return resolvedAs(mariaDBResolution(version), platform.MariaDB)
	}

	v, ok := parseVersion(version)
	if !ok {
		// Recognized stays false: the preset below is ForDialect's, picked
		// without reading anything out of the string.
		return VersionResolution{Capabilities: ForDialect(dialect), ResolvedDialect: normalized}
	}

	switch normalized {
	case platform.MySQL:
		return resolvedAs(mysqlResolution(v), platform.MySQL)
	case platform.MariaDB:
		return resolvedAs(mariaDBResolution(version), platform.MariaDB)
	case platform.Postgres:
		return resolvedAs(postgresResolution(v), platform.Postgres)
	case platform.CockroachDB:
		// The banner branch above is not the only way to reach this ladder.
		// A dotted "25.4.5" is the documented shape on every other dialect, and
		// before this arm existed it fell through to the default below and
		// received ForDialect's CockroachDB26 — which differs from the measured
		// CockroachDB25 preset on create_or_replace_trigger,
		// drop_constraint_generic and drop_constraint_if_exists, and reported
		// itself as a dialect with no ladder at all.
		return resolvedAs(cockroachDBResolution(version), platform.CockroachDB)
	case platform.SQLite:
		// SQLite's ladder is one step, at 3.25, and it exists for an offline
		// render rather than for a live connection: the engine Ptah links is
		// pinned far above the step, so only a user pinning an older target
		// with --server-version can reach the lower arm (stokaro/ptah#916).
		return resolvedAs(sqliteResolution(version), platform.SQLite)
	case platform.ClickHouse:
		// Reached the same way the CockroachDB arm above is: a bare dotted
		// "26.7.3.19" carries no product token, so BannerPlatform names nothing
		// and the banner switch never sees it. Without this arm the ladder
		// existed and only a banner spelling the word "ClickHouse" could climb
		// it, which is not the shape `SELECT version()` returns.
		return resolvedAs(clickHouseResolution(version), platform.ClickHouse)
	case platform.YugabyteDB:
		// Same reason: a typed "2026.1.0.0" carries no "-YB-" marker, so the
		// banner switch never sees it and the ladder would be reachable only
		// from a live connection.
		return resolvedAs(yugabyteResolution(version), platform.YugabyteDB)
	default:
		// The version parsed; this dialect simply has no ladder to spend it
		// on. That is not the operator's mistake, so it is recognized.
		return VersionResolution{Capabilities: ForDialect(dialect), Recognized: true, ResolvedDialect: normalized}
	}
}

// resolvedAs stamps the platform a per-dialect resolution answered from.
//
// It is applied at the dispatch rather than inside each resolver because the
// dispatch is the only place that knows a banner outranked the declared
// dialect, and that overriding is the whole reason the field exists.
func resolvedAs(resolution VersionResolution, dialect string) VersionResolution {
	resolution.ResolvedDialect = dialect
	return resolution
}

// measuredLine renders a major version number as the version line label
// reported in VersionResolution.NewestMeasured.
func measuredLine(major int) string {
	return strconv.Itoa(major) + ".x"
}

// ladderResolution assembles the answer for a dialect that has a version
// ladder. saturated is the single place VersionSpecific and Saturated are tied
// together: a version past the top of the ladder receives ForDialect's preset,
// so it is a fallback and not a version-specific selection.
func ladderResolution(caps Capabilities, newestMeasuredMajor int, saturated bool) VersionResolution {
	return VersionResolution{
		Capabilities:    caps,
		VersionSpecific: !saturated,
		Saturated:       saturated,
		NewestMeasured:  measuredLine(newestMeasuredMajor),
		// Only reached with a parsed version in hand.
		Recognized: true,
	}
}

// clickHouseResolution refines a ClickHouse banner onto its one ladder step.
//
// A version this function cannot read takes the lower arm, and that direction is
// the safe one: ClickHouse2411 differs only by claiming CHECK GRANT, and the
// refusal that key guards -- a database-realm cleanup that drops every object --
// is one Ptah would rather decline on an unreadable banner than run on a
// privilege answer it could not obtain (stokaro/ptah#916).
// sqliteResolution refines a SQLite version onto its one ladder step.
//
// An unreadable version takes the LOWER arm, which is the conservative
// direction here: the upper arm only adds a clause, and emitting a clause an
// older consumer cannot parse is worse than rebuilding a table that did not
// need rebuilding.
func sqliteResolution(version string) VersionResolution {
	v, ok := parseVersion(version)
	if !ok {
		return VersionResolution{Capabilities: SQLite324()}
	}
	return VersionResolution{
		Capabilities:    sqliteForVersion(v),
		VersionSpecific: true,
		Recognized:      true,
	}
}

// sqliteForVersion picks the arm. RENAME COLUMN arrived in 3.25.
func sqliteForVersion(v serverVersion) Capabilities {
	if v.major > 3 || (v.major == 3 && v.minor >= 25) {
		return SQLite3()
	}
	return SQLite324()
}

func clickHouseResolution(version string) VersionResolution {
	v, ok := parseVersion(version)
	if !ok {
		return VersionResolution{Capabilities: ClickHouse24()}
	}
	return measuredMinorLineResolution(
		clickHouseForVersion(v), v, capabilityline.ClickHouseMeasured(), capabilityline.ClickHouse26)
}

// clickHouseForVersion picks the arm. CHECK GRANT arrived in 24.11.
func clickHouseForVersion(v serverVersion) Capabilities {
	if v.major > 24 || (v.major == 24 && v.minor >= 11) {
		return ClickHouse2411()
	}
	return ClickHouse24()
}

// yugabyteResolution answers a YugabyteDB version or banner.
//
// The cut at "-YB-" is the whole reason this is not the shared parse. A
// YugabyteDB banner opens with the PostgreSQL compatibility version --
// "PostgreSQL 11.2-YB-2024.2.4.0-b0" -- so parseVersion reads 11.2, and a
// ladder given 11.2 would put a 2024 server below every declared line and
// report it saturated by nothing. The product version is what follows the
// marker (stokaro/ptah#916).
func yugabyteResolution(version string) VersionResolution {
	v, ok := parseVersion(yugabyteProductVersion(version))
	if !ok || !isYugabyteReleaseYear(v) {
		// Either nothing parsed, or what parsed is not a YugabyteDB product
		// version: a banner with no "-YB-" marker yields the PostgreSQL
		// compatibility number, and answering "11.2 is not a measured release
		// line" would report a number the product does not have.
		//
		// The string named a server, so it is recognized, but no line was
		// selected -- and the arm taken is the LOWER one. A plan that avoids
		// pg_advisory_lock and CREATE OR REPLACE TRIGGER runs on both engines;
		// the reverse is a syntax error on the older one, and degrading to
		// DROP + CREATE for a trigger costs a statement rather than a plan.
		return VersionResolution{Capabilities: YugabyteDB24(), Recognized: true}
	}
	return measuredMinorLineResolution(
		yugabyteForVersion(v), v, capabilityline.YugabyteDBMeasured(), capabilityline.YugabyteDB2026)
}

// isYugabyteReleaseYear reports whether a parsed major looks like a YugabyteDB
// release line rather than a PostgreSQL major. YugabyteDB has numbered its
// releases by year since 2024.1; every number below that floor is the
// compatibility version of the engine underneath.
func isYugabyteReleaseYear(v serverVersion) bool {
	return v.major >= 2024
}

// yugabyteProductVersion strips the PostgreSQL compatibility version a
// YugabyteDB banner opens with. A bare "2026.1.0.0" carries no marker and is
// returned unchanged, which is the shape a typed --server-version has.
func yugabyteProductVersion(version string) string {
	if _, after, found := strings.Cut(strings.ToUpper(version), "-YB-"); found {
		return after
	}
	return version
}

// yugabyteForVersion picks the arm. The PostgreSQL 11 to 15 engine swap landed
// in the 2025 line.
func yugabyteForVersion(v serverVersion) Capabilities {
	if v.major >= 2025 {
		return YugabyteDB25()
	}
	return YugabyteDB24()
}

func mysqlResolution(v serverVersion) VersionResolution {
	return measuredMinorLineResolution(
		mysqlForVersion(v), v, capabilityline.MySQLMeasured(), capabilityline.MySQL26)
}

func mariaDBResolution(version string) VersionResolution {
	v, ok := parseVersion(strings.TrimPrefix(version, mariaDBReplicationPrefix))
	if !ok {
		// The banner said MariaDB but carried no version, so the ladder was
		// not consulted and mariaDBForVersion returned its own fallback.
		// Recognized stays false for the same reason it does on any other
		// laddered dialect: no version was read.
		return VersionResolution{
			Capabilities:   mariaDBForVersion(version),
			NewestMeasured: capabilityline.MariaDB12,
		}
	}
	return measuredMinorLineResolution(
		mariaDBForVersion(version), v, capabilityline.MariaDBMeasured(), capabilityline.MariaDB12)
}

// measuredMinorLineResolution recognizes only the major/minor pairs backed by
// direct matrix evidence. A gap below the newest line is a conservative
// fallback but not saturation; a line above the newest measurement is both
// unattributed and saturated.
func measuredMinorLineResolution(
	caps Capabilities,
	v serverVersion,
	measuredLines []string,
	newestLine string,
) VersionResolution {
	newest, _ := parseVersion(newestLine)
	return VersionResolution{
		Capabilities: caps,
		VersionSpecific: slices.ContainsFunc(measuredLines, func(line string) bool {
			measured, _ := parseVersion(line)
			return sameMajorMinor(v, measured)
		}),
		Saturated:      compareServerVersion(v, newest) > 0,
		NewestMeasured: newestLine,
		// Only reached with a parsed version in hand.
		Recognized: true,
	}
}

// postgresBannerResolution answers a string that named PostgreSQL.
//
// A banner carrying no readable version selects no release line, so the preset
// is PostgreSQL's own default and Recognized stays false — the same shape
// mariaDBResolution uses for "MariaDB something" and cockroachDBResolution for
// a CockroachDB banner with no number in it.
//
// It deliberately does not take the declared dialect. This branch is reached
// only for a dialect OUTSIDE the PostgreSQL family, and the dispatch stamps
// ResolvedDialect as postgres on whatever comes back; falling back to the
// declared dialect's default there returned MySQL84() under a postgres stamp,
// so ResolveServerVersion("mysql", "PostgreSQL server") handed a caller that
// trusts the field a preset from a different engine. Every other product-banner
// arm answers from the product the banner named, and this one now does too.
func postgresBannerResolution(version string) VersionResolution {
	v, ok := parseVersion(version)
	if !ok {
		return VersionResolution{Capabilities: ForDialect(platform.Postgres)}
	}
	return postgresResolution(v)
}

func postgresResolution(v serverVersion) VersionResolution {
	return ladderResolution(postgresForVersion(v), newestMeasuredPostgresMajor, v.major > newestMeasuredPostgresMajor)
}

func cockroachDBResolution(version string) VersionResolution {
	v, ok := parseVersion(version)
	if !ok {
		// A CockroachDB banner with no version in it. The ladder was not
		// consulted, so Recognized stays false.
		return VersionResolution{Capabilities: CockroachDB26()}
	}
	newest, _ := parseVersion(capabilityline.CockroachDB26)
	saturated := compareServerVersion(v, newest) > 0
	return VersionResolution{
		Capabilities:    cockroachDBForVersion(v),
		VersionSpecific: cockroachDBMeasuredLine(v),
		Saturated:       saturated,
		NewestMeasured:  capabilityline.CockroachDB26,
		Recognized:      true,
	}
}

func cockroachDBForVersion(v serverVersion) Capabilities {
	line25, _ := parseVersion(capabilityline.CockroachDB25)
	line26, _ := parseVersion(capabilityline.CockroachDB26)
	switch {
	case compareServerVersion(v, line26) >= 0:
		return CockroachDB26()
	case compareServerVersion(v, line25) >= 0:
		return CockroachDB25()
	default:
		return CockroachDB23()
	}
}

func cockroachDBMeasuredLine(v serverVersion) bool {
	return slices.ContainsFunc(capabilityline.CockroachDBMeasured(), func(line string) bool {
		measured, _ := parseVersion(line)
		return sameMajorMinor(v, measured)
	})
}

func sameMajorMinor(left, right serverVersion) bool {
	return left.major == right.major && left.minor == right.minor
}

func compareServerVersion(left, right serverVersion) int {
	if left.major != right.major {
		return left.major - right.major
	}
	return left.minor - right.minor
}

func mysqlForVersion(v serverVersion) Capabilities {
	switch {
	case v.major > 8 || (v.major == 8 && v.minor >= 4):
		return MySQL84()
	case v.major == 8 && (v.minor > 0 || v.patch >= 20):
		return MySQL8020()
	case v.major == 8 && v.patch >= 19:
		return MySQL8019()
	case v.major == 8 && v.patch >= 16:
		return MySQL8016()
	case v.major == 8 && v.patch >= 13:
		return MySQL8013()
	default:
		return MySQLLegacy()
	}
}

func postgresForVersion(v serverVersion) Capabilities {
	switch {
	case v.major >= 17:
		return Postgres17()
	case v.major >= 14:
		return Postgres16()
	default:
		return Postgres13()
	}
}

// mariaDBReplicationPrefix is the fake version prefix MariaDB servers prepend
// when speaking the MySQL protocol ("5.5.5-10.11.6-MariaDB").
const mariaDBReplicationPrefix = "5.5.5-"

// mariaDBForVersion picks the MariaDB preset for a server version string.
// MariaDB servers speaking the MySQL protocol prepend a fake "5.5.5-"
// replication-compatibility prefix ("5.5.5-10.11.6-MariaDB"); that prefix is
// stripped before parsing so the REAL version decides. 10.2+ gets the modern
// preset (generic DROP CONSTRAINT, enforced CHECKs, IF EXISTS guards);
// anything older — or an unparseable string — degrades to MariaDBLegacy /
// the modern preset respectively.
func mariaDBForVersion(version string) Capabilities {
	trimmed := strings.TrimPrefix(version, mariaDBReplicationPrefix)
	v, ok := parseVersion(trimmed)
	if !ok {
		return MariaDB1011()
	}
	if v.major > 10 || (v.major == 10 && v.minor >= 2) {
		return MariaDB1011()
	}
	return MariaDBLegacy()
}

// serverVersion is a parsed dotted server version.
type serverVersion struct {
	major, minor, patch int
}

// parseVersion extracts the first dotted numeric version from a server
// version string. It tolerates prefixes ("PostgreSQL 16.3") and suffixes
// ("8.0.42-log"); missing minor/patch components default to zero.
func parseVersion(s string) (serverVersion, bool) {
	i := 0
	for i < len(s) && (s[i] < '0' || s[i] > '9') {
		i++
	}
	if i == len(s) {
		return serverVersion{}, false
	}
	nums := [3]int{}
	for n := range 3 {
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			nums[n] = nums[n]*10 + int(s[i]-'0')
			i++
		}
		if start == i {
			break
		}
		if i == len(s) || s[i] != '.' {
			break
		}
		i++ // skip the dot
	}
	return serverVersion{major: nums[0], minor: nums[1], patch: nums[2]}, true
}
