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

	// Functions marks support for a user-defined function declared as a
	// standalone object with a return type, a language and a body — the shape
	// ast.CreateFunctionNode carries. A target whose routines are declared
	// differently enough that the node cannot describe one reads as absent
	// here, the same way ClickHouse's row policies read as absent from
	// RowLevelSecurity.
	Functions Capability = "functions"

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

	// RowLevelSecurity marks support for row-level security policies
	// (PostgreSQL ALTER TABLE ... ENABLE ROW LEVEL SECURITY + CREATE POLICY).
	RowLevelSecurity Capability = "row_level_security"

	// RoleManagement marks support for PostgreSQL role and object privilege
	// management (CREATE/ALTER ROLE plus GRANT/REVOKE).
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
	Functions: {
		doc: "user-defined functions declared with a return type, a language, and a body",
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
	RoleManagement: {
		doc: "PostgreSQL role and object privilege management",
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
		DropConstraintGeneric:              true,
		DropConstraintIfExists:             false,
		DropIndexIfExists:                  false,
		CheckConstraintsEnforced:           true,
		DropCheckClause:                    true,
		EnumInlineColumn:                   true,
		EnumCustomType:                     false,
		CreateIndexConcurrently:            false,
		DropIndexConcurrently:              false,
		IndexIncludeSPGiST:                 false,
		Views:                              true,
		MaterializedViews:                  false,
		Functions:                          true,
		Triggers:                           true,
		CreateOrReplaceTrigger:             false,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   false,
		RoleManagement:                     false,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          false,
		XMLType:                            false,
		AdvisoryLocks:                      false,
	}
}

// MySQL8019 is the preset for MySQL 8.0.19–8.3. It permits foreign keys that
// reference nonunique indexes, unlike MySQL 8.4 and newer.
func MySQL8019() Capabilities {
	return MySQL84().
		With(ForeignKeysRequireUniqueReference, false).
		With(ForeignKeysRequireIndexedReference, true)
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
		With(DropCheckClause, false)
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
		DropConstraintGeneric:              true,
		DropConstraintIfExists:             true,
		DropIndexIfExists:                  true,
		CheckConstraintsEnforced:           true,
		DropCheckClause:                    false,
		EnumInlineColumn:                   true,
		EnumCustomType:                     false,
		CreateIndexConcurrently:            false,
		DropIndexConcurrently:              false,
		IndexIncludeSPGiST:                 false,
		Views:                              true,
		MaterializedViews:                  false,
		Functions:                          true,
		Triggers:                           true,
		CreateOrReplaceTrigger:             true,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   false,
		RoleManagement:                     false,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  false,
		ForeignKeysRequireIndexedReference: true,
		ForeignKeysCreateBackingIndex:      false,
		// MariaDB the engine does have SEQUENCE objects (10.3+, verified live on
		// 10.11.18: TABLE_TYPE = SEQUENCE). Ptah the generator does not: there is
		// no MariaDB sequence introspection and no MySQL-family sequence
		// planning, so `schema render` emitting a CREATE SEQUENCE would produce a
		// statement `schema apply` never plans and never sees converge. This key
		// describes the generator, so it is false until those land -- do NOT flip
		// it back on the engine's behalf (stokaro/ptah#931 item 8).
		Sequences:     false,
		XMLType:       false,
		AdvisoryLocks: false,
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
		Functions:                          true,
		Triggers:                           true,
		CreateOrReplaceTrigger:             true,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   true,
		RoleManagement:                     true,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          true,
		XMLType:                            true,
		AdvisoryLocks:                      true,
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
// Ptah renders, plans, and introspects plain views. MaterializedViews still
// records the engine rather than Ptah's current model: ClickHouse materialized
// views need TO, ENGINE, and refresh semantics that the shared materialized-view
// node cannot represent, so the renderer names them as unsupported instead of
// emitting an incomplete object.
func ClickHouse24() Capabilities {
	return Capabilities{
		DropConstraintGeneric:              false,
		DropConstraintIfExists:             false,
		DropIndexIfExists:                  false,
		CheckConstraintsEnforced:           false,
		DropCheckClause:                    false,
		EnumInlineColumn:                   true,
		EnumCustomType:                     false,
		CreateIndexConcurrently:            false,
		DropIndexConcurrently:              false,
		IndexIncludeSPGiST:                 false,
		Views:                              true,
		MaterializedViews:                  true,
		Functions:                          false,
		Triggers:                           false,
		CreateOrReplaceTrigger:             false,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   false,
		RoleManagement:                     false,
		ForeignKeys:                        false,
		ForeignKeysRequireUniqueReference:  false,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          false,
		XMLType:                            false,
		AdvisoryLocks:                      false,
	}
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
		Triggers:                           true,
		CreateOrReplaceTrigger:             false,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   false,
		RoleManagement:                     false,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          false,
		XMLType:                            false,
		AdvisoryLocks:                      false,
	}
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
		DropConstraintGeneric:              true,
		DropConstraintIfExists:             false,
		DropIndexIfExists:                  false,
		CheckConstraintsEnforced:           true,
		DropCheckClause:                    false,
		EnumInlineColumn:                   false,
		EnumCustomType:                     false,
		CreateIndexConcurrently:            false,
		DropIndexConcurrently:              false,
		IndexIncludeSPGiST:                 false,
		Views:                              true,
		MaterializedViews:                  false,
		Functions:                          true,
		Triggers:                           true,
		CreateOrReplaceTrigger:             true,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   false,
		RoleManagement:                     false,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  true,
		ForeignKeysRequireIndexedReference: false,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          false,
		XMLType:                            true,
		AdvisoryLocks:                      false,
	}
}

// CockroachDB23 is the preset for CockroachDB's PostgreSQL-compatible surface.
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
// ForServerVersionResult maps EVERY version string containing "cockroachdb"
// here, so writing false would retire triggers for every CockroachDB user on
// a current release. Splitting the key by version is issue #916's job; until
// then this preset follows the engine that was measured, not its own name.
//
// The same v26.2.5 probe accepted CREATE ROLE plus GRANT SELECT, ALTER TABLE
// ... ENABLE ROW LEVEL SECURITY plus CREATE POLICY, CREATE SEQUENCE, and
// CREATE TABLE with SERIAL. Those keys stay enabled because otherwise Ptah
// refuses objects this measured line can host.
func CockroachDB23() Capabilities {
	return Postgres16().
		With(CreateIndexConcurrently, false).
		With(DropIndexConcurrently, false).
		With(IndexIncludeSPGiST, false).
		With(XMLType, false).
		With(AdvisoryLocks, false)
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
		With(DropConstraintGeneric, false).
		With(DropConstraintIfExists, false).
		With(DropIndexIfExists, false).
		With(CheckConstraintsEnforced, false).
		With(EnumCustomType, false).
		With(CreateIndexConcurrently, false).
		With(DropIndexConcurrently, false).
		With(IndexIncludeSPGiST, false).
		With(MaterializedViews, false).
		With(Functions, false).
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
	platform.CockroachDB: CockroachDB23,
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

// Newest measured major version line per refined dialect.
//
// Each ladder in this file ends in an open-topped arm: MySQL sends everything
// above 8.4 to MySQL84, MariaDB everything above 10.2 to MariaDB1011, and
// PostgreSQL everything at or above 17 to Postgres17. That arm is a stand-in,
// not a measurement — a server newer than the line below was never observed
// behaving like the preset it receives. VersionResolution.Saturated is true
// exactly there, so a caller can tell "inside a measured line" from "past the
// newest line this package knows".
//
// Above these numbers the preset that comes back is byte-identical to
// ForDialect's, which is the definition of "no version-specific preset could be
// selected", so VersionSpecific is false there too.
//
// Raising one of these numbers is the deliberate act of claiming a newer
// server line behaves like the preset it lands on. Do it in the change that
// measures that line, together with the preset it deserves — never as a side
// effect of bumping a container tag.
const (
	// MySQL84 covers 8.4 LTS through the 9.x LTS line. The integration matrix
	// already runs mysql:26.7, which therefore resolves saturated: Ptah has no
	// measured MySQL 26 capability line yet.
	newestMeasuredMySQLMajor = 9
	// MariaDB1011 covers 10.2 through the 11.x lines; the integration matrix
	// runs mariadb:10.11.
	newestMeasuredMariaDBMajor = 11
	// Postgres17 covers 17 only. The integration matrix already runs
	// postgres:18, which therefore resolves saturated: Ptah has no measured
	// PostgreSQL 18 capability line yet.
	newestMeasuredPostgresMajor = 17
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
// the two ways a caller ended up on the dialect default: the version could not
// be parsed at all, or it parsed and ran off the top of the ladder.
//
// Saturation is only defined where this package has a version ladder: MySQL,
// MariaDB and PostgreSQL. CockroachDB, YugabyteDB and Spanner are resolved
// from the banner without consulting a version at all, and ClickHouse, SQLite
// and SQL Server have no ladder to saturate; all six report Saturated=false
// and an empty NewestMeasured. Refining those dialects is the remaining scope
// of issue #916 and is deliberately not answered here.
type VersionResolution struct {
	// Capabilities is the resolved preset, never nil for a known dialect.
	Capabilities Capabilities
	// VersionSpecific is false when no version-specific preset could be
	// selected and the dialect default was used instead. It carries exactly
	// the meaning of ForServerVersionResult's second return value, and it is
	// false for a saturated version because the preset such a version lands
	// on is exactly ForDialect's.
	VersionSpecific bool
	// Saturated is true when the resolved preset is the top of a version
	// ladder and the server is newer than the newest line that ladder was
	// measured against. It is the reason VersionSpecific is false, and is
	// never true at the same time as VersionSpecific.
	Saturated bool
	// NewestMeasured names the newest measured version line for the dialect,
	// for example "9.x". It is empty for dialects with no version ladder.
	NewestMeasured string
}

// ForServerVersion refines ForDialect using a live server version string —
// typically the result of SELECT version() — so callers can map a concrete
// server to the closest preset at connect time. Recognized shapes include
// "8.0.42", "8.0.42-log", "10.11.6-MariaDB-1:10.11.6+maria~ubu2204",
// "5.5.5-10.11.6-MariaDB" (the replication-protocol prefix MariaDB reports
// over the MySQL protocol) and "PostgreSQL 16.3 (Debian ...)". When the
// version cannot be parsed, the dialect's default preset is returned.
func ForServerVersion(dialect, version string) Capabilities {
	return ResolveServerVersion(dialect, version).Capabilities
}

// ForServerVersionResult is ForServerVersion plus an explicit fallback signal.
// The boolean is false when no version-specific preset could be selected and
// the dialect default was used instead. Callers with a live connection can log
// that degradation while offline callers can keep using ForDialect.
//
// A version newer than the newest measured line for its dialect is one of
// those fallbacks: the ladder's open-topped arm hands back exactly ForDialect's
// preset, so the boolean is false. ResolveServerVersion separates that case
// from a version that could not be parsed at all.
func ForServerVersionResult(dialect, version string) (Capabilities, bool) {
	resolution := ResolveServerVersion(dialect, version)
	return resolution.Capabilities, resolution.VersionSpecific
}

// ResolveServerVersion is ForServerVersionResult with the saturation answer
// attached. It selects exactly the same preset and reports exactly the same
// VersionSpecific value; Saturated and NewestMeasured say why a saturated
// version is not version-specific and which line it was planned as. See
// VersionResolution for what saturation means and which dialects can report
// it.
func ResolveServerVersion(dialect, version string) VersionResolution {
	normalized := platform.NormalizeDialect(dialect)
	versionLower := strings.ToLower(version)

	switch {
	case strings.Contains(versionLower, "cockroachdb"):
		return VersionResolution{Capabilities: CockroachDB23(), VersionSpecific: true}
	case strings.Contains(versionLower, "yugabytedb") || strings.Contains(versionLower, "yugabyte") || strings.Contains(versionLower, "-yb-"):
		return VersionResolution{Capabilities: YugabyteDB25(), VersionSpecific: true}
	case strings.Contains(versionLower, "spanner"):
		return VersionResolution{Capabilities: SpannerPostgres(), VersionSpecific: true}
	}

	// MariaDB announces itself in the version string even when connected via
	// the mysql dialect/driver; trust the string over the declared dialect.
	if strings.Contains(versionLower, "mariadb") {
		return mariaDBResolution(version)
	}

	v, ok := parseVersion(version)
	if !ok {
		return VersionResolution{Capabilities: ForDialect(dialect)}
	}

	switch normalized {
	case platform.MySQL:
		return mysqlResolution(v)
	case platform.MariaDB:
		return mariaDBResolution(version)
	case platform.Postgres:
		return postgresResolution(v)
	default:
		return VersionResolution{Capabilities: ForDialect(dialect)}
	}
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
	}
}

func mysqlResolution(v serverVersion) VersionResolution {
	return ladderResolution(mysqlForVersion(v), newestMeasuredMySQLMajor, v.major > newestMeasuredMySQLMajor)
}

func mariaDBResolution(version string) VersionResolution {
	v, ok := parseVersion(strings.TrimPrefix(version, mariaDBReplicationPrefix))
	if !ok {
		return VersionResolution{
			Capabilities:   mariaDBForVersion(version),
			NewestMeasured: measuredLine(newestMeasuredMariaDBMajor),
		}
	}
	return ladderResolution(
		mariaDBForVersion(version),
		newestMeasuredMariaDBMajor,
		v.major > newestMeasuredMariaDBMajor,
	)
}

func postgresResolution(v serverVersion) VersionResolution {
	return ladderResolution(postgresForVersion(v), newestMeasuredPostgresMajor, v.major > newestMeasuredPostgresMajor)
}

func mysqlForVersion(v serverVersion) Capabilities {
	switch {
	case v.major > 8 || (v.major == 8 && v.minor >= 4):
		return MySQL84()
	case v.major == 8 && (v.minor > 0 || v.patch >= 19):
		return MySQL8019()
	case v.major == 8 && v.patch >= 16:
		return MySQL8016()
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
