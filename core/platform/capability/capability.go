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

	// CreateOrReplaceTrigger marks support for replacing triggers in one
	// statement (PostgreSQL 14+ and MariaDB 10.1.4+ use
	// CREATE OR REPLACE TRIGGER; SQL Server uses CREATE OR ALTER TRIGGER;
	// MySQL has no equivalent). Trigger renderers use this to choose between
	// replace syntax and an explicit drop/create sequence.
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

	// Sequences marks support for database sequence objects used by
	// PostgreSQL SERIAL/BIGSERIAL or explicit CREATE SEQUENCE support.
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
	CreateOrReplaceTrigger: {
		doc: "single-statement trigger replacement (PostgreSQL/MariaDB CREATE OR REPLACE, SQL Server CREATE OR ALTER; not MySQL)",
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
		CreateOrReplaceTrigger:             true,
		AlterGeneratedColumnExpression:     false,
		RowLevelSecurity:                   false,
		RoleManagement:                     false,
		ForeignKeys:                        true,
		ForeignKeysRequireUniqueReference:  false,
		ForeignKeysRequireIndexedReference: true,
		ForeignKeysCreateBackingIndex:      false,
		Sequences:                          true,
		XMLType:                            false,
		AdvisoryLocks:                      false,
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

// Postgres13 is the preset for PostgreSQL 12–13: identical to Postgres16
// except CREATE OR REPLACE TRIGGER, which arrived in PostgreSQL 14.
func Postgres13() Capabilities {
	return Postgres16().With(CreateOrReplaceTrigger, false)
}

// ClickHouse24 is the preset for the ClickHouse 24.x line. It is deliberately
// minimal: ClickHouse models constraints and indexes so differently that the
// shared capability gates mostly do not apply; enums are inline column types
// (Enum8/Enum16).
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
// also lacks PostgreSQL's SERIAL/sequence surface, XML type, and advisory-lock
// functions in Ptah's portable subset.
func CockroachDB23() Capabilities {
	return Postgres16().
		With(CreateIndexConcurrently, false).
		With(DropIndexConcurrently, false).
		With(XMLType, false).
		With(AdvisoryLocks, false).
		With(RowLevelSecurity, false).
		With(RoleManagement, false).
		With(Sequences, false)
}

// YugabyteDB25 is the preset for YugabyteDB YSQL. It stays close to
// PostgreSQL for the common DDL subset, but regular CREATE INDEX is already
// asynchronous in YugabyteDB, so the PostgreSQL CONCURRENTLY keyword is not
// emitted.
func YugabyteDB25() Capabilities {
	return Postgres16().
		With(CreateIndexConcurrently, false).
		With(DropIndexConcurrently, false).
		With(AdvisoryLocks, false).
		With(RowLevelSecurity, false)
}

// SpannerPostgres is the conservative preset for Cloud Spanner's PostgreSQL
// interface. Spanner's SQL surface is sufficiently different that Ptah only
// routes the simplest PostgreSQL-family statements through this preset; enums,
// sequences, RLS, advisory locks, and XML are disabled. Enforced foreign keys,
// including circular relationships added with ALTER TABLE, are supported.
func SpannerPostgres() Capabilities {
	return Postgres16().
		With(DropConstraintGeneric, false).
		With(DropConstraintIfExists, false).
		With(DropIndexIfExists, false).
		With(CheckConstraintsEnforced, false).
		With(EnumCustomType, false).
		With(CreateIndexConcurrently, false).
		With(DropIndexConcurrently, false).
		With(CreateOrReplaceTrigger, false).
		With(RowLevelSecurity, false).
		With(RoleManagement, false).
		With(Sequences, false).
		With(XMLType, false).
		With(AdvisoryLocks, false).
		With(ForeignKeysRequireUniqueReference, false).
		With(ForeignKeysCreateBackingIndex, true)
}

// ForDialect returns the default preset for a dialect name (normalized via
// platform.NormalizeDialect): the current supported version line of that
// dialect. Unknown dialects get nil — the conservative empty set.
func ForDialect(dialect string) Capabilities {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres:
		return Postgres17()
	case platform.MySQL:
		return MySQL84()
	case platform.MariaDB:
		return MariaDB1011()
	case platform.ClickHouse:
		return ClickHouse24()
	case platform.SQLite:
		return SQLite3()
	case platform.SQLServer:
		return SQLServer2022()
	case platform.CockroachDB:
		return CockroachDB23()
	case platform.YugabyteDB:
		return YugabyteDB25()
	case platform.Spanner:
		return SpannerPostgres()
	default:
		return nil
	}
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
// Raising one of these numbers is the deliberate act of claiming a newer
// server line behaves like the preset it lands on. Do it in the change that
// measures that line, together with the preset it deserves — never as a side
// effect of bumping a container tag.
const (
	// MySQL84 covers 8.4 LTS through the 9.x LTS line, which is the newest
	// MySQL the integration matrix runs (mysql:9.7).
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
// capability preset. It is what ForServerVersionResult's boolean cannot say on
// its own: that boolean answers "did a parsed version select this preset", and
// stays true for a version far newer than anything this package has a preset
// for.
//
// Saturated distinguishes those two cases. It is true when the version parsed,
// selected the newest preset in its dialect's ladder, and is itself newer than
// the newest line that ladder was measured against — so the preset is a
// stand-in and any capability the newer server gained or lost is unmodeled.
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
	// the meaning of ForServerVersionResult's second return value.
	VersionSpecific bool
	// Saturated is true when the resolved preset is the top of a version
	// ladder and the server is newer than the newest line that ladder was
	// measured against.
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
// The boolean says nothing about whether the server is newer than the newest
// preset this package holds; ResolveServerVersion answers that.
func ForServerVersionResult(dialect, version string) (Capabilities, bool) {
	resolution := ResolveServerVersion(dialect, version)
	return resolution.Capabilities, resolution.VersionSpecific
}

// ResolveServerVersion is ForServerVersionResult with the saturation answer
// attached. It selects exactly the same preset and reports exactly the same
// VersionSpecific value; the extra fields describe how far the mapping was
// trusted. See VersionResolution for what saturation means and which dialects
// can report it.
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

func mysqlResolution(v serverVersion) VersionResolution {
	return VersionResolution{
		Capabilities:    mysqlForVersion(v),
		VersionSpecific: true,
		Saturated:       v.major > newestMeasuredMySQLMajor,
		NewestMeasured:  measuredLine(newestMeasuredMySQLMajor),
	}
}

func mariaDBResolution(version string) VersionResolution {
	resolution := VersionResolution{
		Capabilities:   mariaDBForVersion(version),
		NewestMeasured: measuredLine(newestMeasuredMariaDBMajor),
	}
	v, ok := parseVersion(strings.TrimPrefix(version, mariaDBReplicationPrefix))
	if !ok {
		return resolution
	}
	resolution.VersionSpecific = true
	resolution.Saturated = v.major > newestMeasuredMariaDBMajor
	return resolution
}

func postgresResolution(v serverVersion) VersionResolution {
	return VersionResolution{
		Capabilities:    postgresForVersion(v),
		VersionSpecific: true,
		Saturated:       v.major > newestMeasuredPostgresMajor,
		NewestMeasured:  measuredLine(newestMeasuredPostgresMajor),
	}
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
