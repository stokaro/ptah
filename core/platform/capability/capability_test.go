package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

func TestCapabilities_Has_NilSafe(t *testing.T) {
	c := qt.New(t)

	var caps capability.Capabilities
	c.Assert(caps.Has(capability.DropConstraintIfExists), qt.IsFalse)
	c.Assert(caps.Validate(), qt.IsNil)
}

func TestCapabilities_Has(t *testing.T) {
	c := qt.New(t)

	caps := capability.Capabilities{
		capability.DropConstraintGeneric: true,
		capability.DropIndexIfExists:     false,
	}
	c.Assert(caps.Has(capability.DropConstraintGeneric), qt.IsTrue)
	c.Assert(caps.Has(capability.DropIndexIfExists), qt.IsFalse, qt.Commentf("present-but-disabled must read as absent"))
	c.Assert(caps.Has(capability.RowLevelSecurity), qt.IsFalse, qt.Commentf("missing key must read as absent"))
}

func TestCapabilities_Validate_UnknownKey(t *testing.T) {
	c := qt.New(t)

	caps := capability.Capabilities{"definately_a_typo": true}
	c.Assert(caps.Validate(), qt.ErrorMatches, `unknown capability "definately_a_typo"`)

	// Disabled unknown keys are rejected too: a typo must never validate.
	caps = capability.Capabilities{"another_typo": false}
	c.Assert(caps.Validate(), qt.ErrorMatches, `unknown capability "another_typo"`)
}

func TestCapabilities_Validate_RequiresEdge(t *testing.T) {
	c := qt.New(t)

	// IF EXISTS on a statement the target does not have is a contradiction.
	caps := capability.Capabilities{
		capability.DropConstraintIfExists: true,
		capability.DropConstraintGeneric:  false,
	}
	c.Assert(caps.Validate(), qt.ErrorMatches,
		`capability "drop_constraint_if_exists" requires "drop_constraint_generic", which is not enabled`)

	// A missing (rather than disabled) requirement is equally invalid.
	caps = capability.Capabilities{capability.DropConstraintIfExists: true}
	c.Assert(caps.Validate(), qt.ErrorMatches,
		`capability "drop_constraint_if_exists" requires "drop_constraint_generic", which is not enabled`)

	// Satisfying the edge fixes the set.
	caps = capability.Capabilities{
		capability.DropConstraintIfExists: true,
		capability.DropConstraintGeneric:  true,
	}
	c.Assert(caps.Validate(), qt.IsNil)

	// DROP CHECK presupposes enforced CHECK constraints.
	caps = capability.Capabilities{capability.DropCheckClause: true}
	c.Assert(caps.Validate(), qt.ErrorMatches,
		`capability "drop_check_clause" requires "check_constraints_enforced", which is not enabled`)

	// A materialized view is a view whose result is stored, so a target that
	// has no views cannot have one (stokaro/ptah#929).
	caps = capability.Capabilities{capability.MaterializedViews: true}
	c.Assert(caps.Validate(), qt.ErrorMatches,
		`capability "materialized_views" requires "views", which is not enabled`)
	caps = capability.Capabilities{capability.MaterializedViews: true, capability.Views: true}
	c.Assert(caps.Validate(), qt.IsNil)

	// Replace syntax for a statement the target does not have is the same
	// contradiction as IF EXISTS above.
	caps = capability.Capabilities{capability.CreateOrReplaceTrigger: true}
	c.Assert(caps.Validate(), qt.ErrorMatches,
		`capability "create_or_replace_trigger" requires "triggers", which is not enabled`)
	caps = capability.Capabilities{capability.CreateOrReplaceTrigger: true, capability.Triggers: true}
	c.Assert(caps.Validate(), qt.IsNil)
}

func TestCapabilities_Validate_MutexGroup(t *testing.T) {
	c := qt.New(t)

	caps := capability.Capabilities{
		capability.EnumInlineColumn: true,
		capability.EnumCustomType:   true,
	}
	c.Assert(caps.Validate(), qt.ErrorMatches,
		`capabilities enum_inline_column and enum_custom_type are mutually exclusive`)

	// One of the two (or neither) is fine.
	c.Assert(capability.Capabilities{capability.EnumInlineColumn: true}.Validate(), qt.IsNil)
	c.Assert(capability.Capabilities{capability.EnumCustomType: true}.Validate(), qt.IsNil)
	c.Assert(capability.Capabilities{}.Validate(), qt.IsNil)
}

func TestCapabilities_Validate_ForeignKeyReferencePolicy(t *testing.T) {
	c := qt.New(t)

	c.Assert(capability.Capabilities{
		capability.ForeignKeys: true,
	}.Validate(), qt.ErrorMatches,
		`capability "foreign_keys" requires exactly one foreign-key reference policy`)

	c.Assert(capability.Capabilities{
		capability.ForeignKeys:                        true,
		capability.ForeignKeysRequireUniqueReference:  true,
		capability.ForeignKeysRequireIndexedReference: true,
	}.Validate(), qt.ErrorMatches,
		`capabilities foreign_keys_require_unique_reference and foreign_keys_require_indexed_reference are mutually exclusive`)

	c.Assert(capability.Capabilities{
		capability.ForeignKeys:                        true,
		capability.ForeignKeysRequireIndexedReference: true,
	}.Validate(), qt.IsNil)
}

func TestPresets_AllValid_AndCoverEveryRegisteredCapability(t *testing.T) {
	c := qt.New(t)

	presets := map[string]capability.Capabilities{
		"MySQL84":       capability.MySQL84(),
		"MySQL8019":     capability.MySQL8019(),
		"MySQL8016":     capability.MySQL8016(),
		"MySQLLegacy":   capability.MySQLLegacy(),
		"MariaDB1011":   capability.MariaDB1011(),
		"MariaDBLegacy": capability.MariaDBLegacy(),
		"Postgres17":    capability.Postgres17(),
		"Postgres16":    capability.Postgres16(),
		"Postgres13":    capability.Postgres13(),
		"ClickHouse24":  capability.ClickHouse24(),
		"SQLite3":       capability.SQLite3(),
		"CockroachDB23": capability.CockroachDB23(),
		"YugabyteDB25":  capability.YugabyteDB25(),
		"SQLServer2022": capability.SQLServer2022(),
		"Spanner":       capability.SpannerPostgres(),
	}
	for name, preset := range presets {
		c.Assert(preset.Validate(), qt.IsNil, qt.Commentf("preset %s must validate", name))
		// Presets enumerate every registered capability explicitly, so the
		// docs matrix and the code never drift silently.
		for _, key := range capability.All() {
			_, present := preset[key]
			c.Assert(present, qt.IsTrue, qt.Commentf("preset %s is missing registry key %q", name, key))
		}
		c.Assert(preset, qt.HasLen, len(capability.All()), qt.Commentf("preset %s carries an extra key", name))
	}
}

func TestPresets_KeyDifferences(t *testing.T) {
	c := qt.New(t)

	// The MySQL family never gets IF EXISTS guards; MariaDB does.
	c.Assert(capability.MySQL84().Has(capability.DropConstraintIfExists), qt.IsFalse)
	c.Assert(capability.MySQL84().Has(capability.DropIndexIfExists), qt.IsFalse)
	c.Assert(capability.MySQL84().Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(capability.MySQL8019().Has(capability.ForeignKeysRequireUniqueReference), qt.IsFalse)
	c.Assert(capability.MySQL8019().Has(capability.ForeignKeysRequireIndexedReference), qt.IsTrue)
	c.Assert(capability.MariaDB1011().Has(capability.DropConstraintIfExists), qt.IsTrue)
	c.Assert(capability.MariaDB1011().Has(capability.DropIndexIfExists), qt.IsTrue)
	c.Assert(capability.MariaDB1011().Has(capability.ForeignKeysRequireUniqueReference), qt.IsFalse)
	c.Assert(capability.MariaDB1011().Has(capability.ForeignKeysRequireIndexedReference), qt.IsTrue)

	// Version ladder within MySQL.
	c.Assert(capability.MySQL84().Has(capability.DropConstraintGeneric), qt.IsTrue)
	c.Assert(capability.MySQL8016().Has(capability.DropConstraintGeneric), qt.IsFalse)
	c.Assert(capability.MySQL8016().Has(capability.CheckConstraintsEnforced), qt.IsTrue)
	c.Assert(capability.MySQLLegacy().Has(capability.CheckConstraintsEnforced), qt.IsFalse)

	// Postgres version presets gate CREATE OR REPLACE TRIGGER (PG 14+) and
	// generated-column SET EXPRESSION (PG 17+).
	c.Assert(capability.Postgres17().Has(capability.AlterGeneratedColumnExpression), qt.IsTrue)
	c.Assert(capability.Postgres16().Has(capability.AlterGeneratedColumnExpression), qt.IsFalse)
	c.Assert(capability.Postgres16().Has(capability.CreateOrReplaceTrigger), qt.IsTrue)
	c.Assert(capability.Postgres13().Has(capability.CreateOrReplaceTrigger), qt.IsFalse)
	c.Assert(capability.Postgres13().Has(capability.AlterGeneratedColumnExpression), qt.IsFalse)
	c.Assert(capability.Postgres13().Has(capability.CreateIndexConcurrently), qt.IsTrue)
	c.Assert(capability.Postgres16().Has(capability.RoleManagement), qt.IsTrue)

	// Enum modeling is mutually exclusive and dialect-appropriate.
	c.Assert(capability.MySQL84().Has(capability.EnumInlineColumn), qt.IsTrue)
	c.Assert(capability.MySQL84().Has(capability.EnumCustomType), qt.IsFalse)
	c.Assert(capability.MySQL84().Has(capability.RoleManagement), qt.IsFalse)
	c.Assert(capability.Postgres16().Has(capability.EnumCustomType), qt.IsTrue)
	c.Assert(capability.Postgres16().Has(capability.EnumInlineColumn), qt.IsFalse)

	// DROP CHECK is a MySQL-only spelling (MariaDB rejects it — verified
	// live), present exactly in the enforcing MySQL windows.
	c.Assert(capability.MySQL84().Has(capability.DropCheckClause), qt.IsTrue)
	c.Assert(capability.MySQL8016().Has(capability.DropCheckClause), qt.IsTrue)
	c.Assert(capability.MySQLLegacy().Has(capability.DropCheckClause), qt.IsFalse)
	c.Assert(capability.MariaDB1011().Has(capability.DropCheckClause), qt.IsFalse)
	c.Assert(capability.Postgres16().Has(capability.DropCheckClause), qt.IsFalse)

	// The legacy MariaDB floor drops every modern guarantee.
	legacy := capability.MariaDBLegacy()
	c.Assert(legacy.Has(capability.DropConstraintGeneric), qt.IsFalse)
	c.Assert(legacy.Has(capability.DropConstraintIfExists), qt.IsFalse)
	c.Assert(legacy.Has(capability.DropIndexIfExists), qt.IsFalse)
	c.Assert(legacy.Has(capability.CheckConstraintsEnforced), qt.IsFalse)
	c.Assert(legacy.Has(capability.EnumInlineColumn), qt.IsTrue)

	// Distributed-SQL targets share the PostgreSQL family but not every
	// PostgreSQL feature.
	cockroach := capability.CockroachDB23()
	c.Assert(cockroach.Has(capability.EnumCustomType), qt.IsTrue)
	c.Assert(cockroach.Has(capability.ForeignKeys), qt.IsTrue)
	c.Assert(cockroach.Has(capability.CreateIndexConcurrently), qt.IsFalse)
	c.Assert(cockroach.Has(capability.XMLType), qt.IsFalse)
	c.Assert(cockroach.Has(capability.AdvisoryLocks), qt.IsFalse)
	c.Assert(cockroach.Has(capability.RoleManagement), qt.IsFalse)
	c.Assert(cockroach.Has(capability.Sequences), qt.IsFalse)

	yugabyte := capability.YugabyteDB25()
	c.Assert(yugabyte.Has(capability.EnumCustomType), qt.IsTrue)
	c.Assert(yugabyte.Has(capability.ForeignKeys), qt.IsTrue)
	c.Assert(yugabyte.Has(capability.CreateIndexConcurrently), qt.IsFalse)
	c.Assert(yugabyte.Has(capability.RoleManagement), qt.IsTrue)
	c.Assert(yugabyte.Has(capability.Sequences), qt.IsTrue)

	spanner := capability.SpannerPostgres()
	c.Assert(spanner.Has(capability.EnumCustomType), qt.IsFalse)
	c.Assert(spanner.Has(capability.ForeignKeys), qt.IsTrue)
	c.Assert(spanner.Has(capability.ForeignKeysCreateBackingIndex), qt.IsTrue)
	c.Assert(spanner.Has(capability.Sequences), qt.IsFalse)
	c.Assert(spanner.Has(capability.XMLType), qt.IsFalse)
	c.Assert(spanner.Has(capability.RoleManagement), qt.IsFalse)

	// Object kinds (stokaro/ptah#929 item 5). Every row below was measured
	// against a live server of that engine except Spanner, which has no
	// container and no live test (#942) and follows Google's documentation.
	//
	// Spanner is the row the keys exist for: it shares the PostgreSQL planner
	// and renderer with the three presets above, and is the only family member
	// that refuses a materialized view, a function or a trigger.
	c.Assert(capability.Postgres16().Has(capability.Views), qt.IsTrue)
	c.Assert(capability.Postgres16().Has(capability.MaterializedViews), qt.IsTrue)
	c.Assert(capability.Postgres16().Has(capability.Functions), qt.IsTrue)
	c.Assert(capability.Postgres16().Has(capability.Triggers), qt.IsTrue)
	c.Assert(cockroach.Has(capability.MaterializedViews), qt.IsTrue)
	c.Assert(cockroach.Has(capability.Triggers), qt.IsTrue)
	c.Assert(yugabyte.Has(capability.MaterializedViews), qt.IsTrue)
	c.Assert(yugabyte.Has(capability.Triggers), qt.IsTrue)
	c.Assert(spanner.Has(capability.Views), qt.IsTrue)
	c.Assert(spanner.Has(capability.MaterializedViews), qt.IsFalse)
	c.Assert(spanner.Has(capability.Functions), qt.IsFalse)
	c.Assert(spanner.Has(capability.Triggers), qt.IsFalse)

	// Outside the PostgreSQL family the keys record the engine rather than
	// gating an emission yet. MySQL accepts CREATE MATERIALIZED VIEW and then
	// recomputes the result on every read, so the key that names a STORED
	// result is off for it just as it is for MariaDB, which refuses the
	// statement outright.
	c.Assert(capability.MySQL84().Has(capability.Views), qt.IsTrue)
	c.Assert(capability.MySQL84().Has(capability.MaterializedViews), qt.IsFalse)
	c.Assert(capability.MariaDB1011().Has(capability.MaterializedViews), qt.IsFalse)
	c.Assert(capability.MariaDB1011().Has(capability.Triggers), qt.IsTrue)
	c.Assert(capability.SQLite3().Has(capability.Triggers), qt.IsTrue)
	c.Assert(capability.SQLite3().Has(capability.Functions), qt.IsFalse)
	c.Assert(capability.ClickHouse24().Has(capability.MaterializedViews), qt.IsTrue)
	c.Assert(capability.ClickHouse24().Has(capability.Triggers), qt.IsFalse)
	c.Assert(capability.ClickHouse24().Has(capability.Functions), qt.IsFalse)

	sqlServer := capability.SQLServer2022()
	c.Assert(sqlServer.Has(capability.Views), qt.IsTrue)
	c.Assert(sqlServer.Has(capability.MaterializedViews), qt.IsFalse)
	c.Assert(sqlServer.Has(capability.Functions), qt.IsTrue)
	c.Assert(sqlServer.Has(capability.Triggers), qt.IsTrue)
	c.Assert(sqlServer.Has(capability.DropConstraintGeneric), qt.IsTrue)
	c.Assert(sqlServer.Has(capability.CheckConstraintsEnforced), qt.IsTrue)
	c.Assert(sqlServer.Has(capability.ForeignKeys), qt.IsTrue)
	c.Assert(sqlServer.Has(capability.CreateOrReplaceTrigger), qt.IsTrue)
	c.Assert(sqlServer.Has(capability.Sequences), qt.IsFalse)
	c.Assert(sqlServer.Has(capability.EnumInlineColumn), qt.IsFalse)
	c.Assert(sqlServer.Has(capability.EnumCustomType), qt.IsFalse)
	c.Assert(sqlServer.Has(capability.RowLevelSecurity), qt.IsFalse)
}

func TestCapabilities_With_DoesNotMutateReceiver(t *testing.T) {
	c := qt.New(t)

	base := capability.MariaDB1011()
	derived := base.With(capability.DropIndexIfExists, false)

	c.Assert(base.Has(capability.DropIndexIfExists), qt.IsTrue, qt.Commentf("receiver must stay untouched"))
	c.Assert(derived.Has(capability.DropIndexIfExists), qt.IsFalse)
	c.Assert(derived.Validate(), qt.IsNil)

	// With on a nil set allocates.
	var empty capability.Capabilities
	derived = empty.With(capability.DropConstraintGeneric, true)
	c.Assert(derived.Has(capability.DropConstraintGeneric), qt.IsTrue)
	c.Assert(empty.Has(capability.DropConstraintGeneric), qt.IsFalse)
}

func TestCapabilities_Clone(t *testing.T) {
	c := qt.New(t)

	var nilSet capability.Capabilities
	c.Assert(nilSet.Clone(), qt.IsNil)

	orig := capability.MySQL84()
	cl := orig.Clone()
	cl[capability.DropIndexIfExists] = true
	c.Assert(orig.Has(capability.DropIndexIfExists), qt.IsFalse, qt.Commentf("clone must be independent"))
}

func TestForDialect(t *testing.T) {
	c := qt.New(t)

	c.Assert(capability.ForDialect("mysql").Has(capability.DropConstraintIfExists), qt.IsFalse)
	c.Assert(capability.ForDialect("mariadb").Has(capability.DropConstraintIfExists), qt.IsTrue)
	c.Assert(capability.ForDialect("postgres").Has(capability.CreateIndexConcurrently), qt.IsTrue)
	c.Assert(capability.ForDialect("postgres").Has(capability.AlterGeneratedColumnExpression), qt.IsTrue)
	// Dialect aliases go through platform.NormalizeDialect.
	c.Assert(capability.ForDialect("postgresql").Has(capability.EnumCustomType), qt.IsTrue)
	c.Assert(capability.ForDialect("pgx").Has(capability.EnumCustomType), qt.IsTrue)
	c.Assert(capability.ForDialect("ch").Has(capability.EnumInlineColumn), qt.IsTrue)
	c.Assert(capability.ForDialect("sqlite3").Has(capability.CheckConstraintsEnforced), qt.IsTrue)
	c.Assert(capability.ForDialect("sqlite").Has(capability.ForeignKeys), qt.IsTrue)
	c.Assert(capability.ForDialect("sqlite").Has(capability.EnumInlineColumn), qt.IsFalse)
	c.Assert(capability.ForDialect("sqlite").Has(capability.EnumCustomType), qt.IsFalse)
	c.Assert(capability.ForDialect("crdb").Has(capability.CreateIndexConcurrently), qt.IsFalse)
	c.Assert(capability.ForDialect("yugabyte").Has(capability.CreateIndexConcurrently), qt.IsFalse)
	c.Assert(capability.ForDialect("mssql").Has(capability.CreateOrReplaceTrigger), qt.IsTrue)
	c.Assert(capability.ForDialect("sql-server").Has(capability.ForeignKeys), qt.IsTrue)
	c.Assert(capability.ForDialect("spanner").Has(capability.ForeignKeys), qt.IsTrue)
	// Unknown dialects get the conservative nil set.
	c.Assert(capability.ForDialect("oracle"), qt.IsNil)
}

func TestForServerVersion(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		// probe capability and its expected value pin the resolved preset
		probe capability.Capability
		want  bool
	}{
		{"mysql 8.0.19+ line", "mysql", "8.0.42", capability.DropConstraintGeneric, true},
		{"mysql 8.0.42-log suffix", "mysql", "8.0.42-log", capability.DropConstraintGeneric, true},
		{"mysql 8.0 permits nonunique referenced keys", "mysql", "8.0.42", capability.ForeignKeysRequireUniqueReference, false},
		{"mysql 8.4 requires unique referenced keys", "mysql", "8.4.0", capability.ForeignKeysRequireUniqueReference, true},
		{"mysql 9.x line", "mysql", "9.7.1", capability.DropConstraintGeneric, true},
		{"mysql 9 requires unique referenced keys", "mysql", "9.7.1", capability.ForeignKeysRequireUniqueReference, true},
		{"mysql 8.1 minor above zero", "mysql", "8.1.0", capability.DropConstraintGeneric, true},
		{"mysql 8.0.19 exact boundary", "mysql", "8.0.19", capability.DropConstraintGeneric, true},
		{"mysql 8.0.18 upper edge of the DROP CHECK window", "mysql", "8.0.18", capability.DropConstraintGeneric, false},
		{"mysql 8.0.16 exact boundary", "mysql", "8.0.16", capability.CheckConstraintsEnforced, true},
		{"mysql 8.0.15 below the CHECK window", "mysql", "8.0.15", capability.CheckConstraintsEnforced, false},
		{"mysql 8.0.16 window has checks", "mysql", "8.0.17", capability.CheckConstraintsEnforced, true},
		{"mysql 8.0.16 window lacks generic drop", "mysql", "8.0.17", capability.DropConstraintGeneric, false},
		{"mysql 8.0.16 window has DROP CHECK", "mysql", "8.0.17", capability.DropCheckClause, true},
		{"mysql legacy lacks enforced checks", "mysql", "5.7.44", capability.CheckConstraintsEnforced, false},
		{"mariadb via own dialect", "mariadb", "10.11.6-MariaDB-1:10.11.6+maria~ubu2204", capability.DropConstraintIfExists, true},
		{"mariadb over mysql protocol prefix", "mysql", "5.5.5-10.11.6-MariaDB", capability.DropConstraintIfExists, true},
		{"mariadb 10.2 exact boundary", "mariadb", "10.2.44-MariaDB", capability.DropConstraintIfExists, true},
		{"mariadb 11.x line", "mariadb", "11.4.2-MariaDB", capability.DropConstraintIfExists, true},
		{"mariadb pre-10.2 degrades to the legacy floor", "mariadb", "10.1.48-MariaDB", capability.DropConstraintIfExists, false},
		{"mariadb pre-10.2 over mysql protocol prefix", "mysql", "5.5.5-10.1.48-MariaDB", capability.CheckConstraintsEnforced, false},
		{"mariadb unparseable stays on the modern preset", "mariadb", "MariaDB something", capability.DropConstraintIfExists, true},
		{"postgres 17 banner", "postgres", "PostgreSQL 17.5", capability.AlterGeneratedColumnExpression, true},
		{"postgres 16 banner", "postgres", "PostgreSQL 16.3 (Debian 16.3-1.pgdg120+1)", capability.CreateOrReplaceTrigger, true},
		{"postgres 16 lacks generated expression alter", "postgres", "PostgreSQL 16.3", capability.AlterGeneratedColumnExpression, false},
		{"postgres 14 exact boundary", "postgres", "PostgreSQL 14.0", capability.CreateOrReplaceTrigger, true},
		{"postgres 13 plain", "postgres", "13.14", capability.CreateOrReplaceTrigger, false},
		{"postgres 13 still concurrent-capable", "postgres", "13.14", capability.CreateIndexConcurrently, true},
		{"cockroach banner disables concurrent indexes", "postgres", "CockroachDB CCL v23.2.5 (x86_64-pc-linux-gnu)", capability.CreateIndexConcurrently, false},
		{"cockroach banner disables XML", "postgres", "CockroachDB CCL v23.2.5 (x86_64-pc-linux-gnu)", capability.XMLType, false},
		{"yugabytedb banner disables concurrent indexes", "postgres", "PostgreSQL 11.2-YB-2.25.1.0-b0 on x86_64-pc-linux-gnu, compiled by clang", capability.CreateIndexConcurrently, false},
		{"spanner banner supports foreign keys", "postgres", "Cloud Spanner PostgreSQL interface", capability.ForeignKeys, true},
		{"unparseable falls back to dialect default", "mysql", "who knows", capability.DropConstraintGeneric, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.ForServerVersion(tt.dialect, tt.version)
			c.Assert(caps.Validate(), qt.IsNil)
			c.Assert(caps.Has(tt.probe), qt.Equals, tt.want,
				qt.Commentf("dialect=%s version=%q probe=%s", tt.dialect, tt.version, tt.probe))
		})
	}
}

func TestForServerVersionResultReportsFallback(t *testing.T) {
	c := qt.New(t)

	caps, versionSpecific := capability.ForServerVersionResult("mysql", "8.0.17")
	c.Assert(versionSpecific, qt.Equals, true)
	c.Assert(caps.Has(capability.DropCheckClause), qt.Equals, true)
	c.Assert(caps.Has(capability.DropConstraintGeneric), qt.Equals, false)

	caps, versionSpecific = capability.ForServerVersionResult("mysql", "who knows")
	c.Assert(versionSpecific, qt.Equals, false)
	c.Assert(caps.Has(capability.DropConstraintGeneric), qt.Equals, true)

	caps, versionSpecific = capability.ForServerVersionResult("mariadb", "MariaDB something")
	c.Assert(versionSpecific, qt.Equals, false)
	c.Assert(caps.Has(capability.DropConstraintIfExists), qt.Equals, true)
}

// TestResolveServerVersionReportsSaturation pins the version-saturation
// answer for every refined dialect: one row per (dialect, version) below the
// newest measured line, inside it, exactly at it, and above it.
//
// The "above" rows are the point of the test. Each ladder ends in an
// open-topped arm, so a server newer than anything Ptah measured still lands
// on the newest preset; Saturated is the only signal that separates that
// stand-in from a real match, and #791 (MySQL 26.x) and #108 (MariaDB 12.x)
// unblock on exactly these rows. The wantCaps column pins the preset at the
// same time, so a row cannot go green by resolving to some other set.
func TestResolveServerVersionReportsSaturation(t *testing.T) {
	tests := []struct {
		name                string
		dialect             string
		version             string
		wantCaps            capability.Capabilities
		wantVersionSpecific bool
		wantSaturated       bool
		wantNewestMeasured  string
	}{
		// MySQL: the ladder tops out at MySQL84, measured through the 9.x line.
		{"mysql below", "mysql", "5.7.44", capability.MySQLLegacy(), true, false, "9.x"},
		{"mysql inside", "mysql", "8.0.42-log", capability.MySQL8019(), true, false, "9.x"},
		{"mysql at the newest measured line", "mysql", "9.7.1", capability.MySQL84(), true, false, "9.x"},
		{"mysql at the top of the newest measured major", "mysql", "9.99.0", capability.MySQL84(), true, false, "9.x"},
		{"mysql above (#791 bumps CI to 26.x)", "mysql", "26.7.0", capability.MySQL84(), true, true, "9.x"},
		{"mysql far above", "mysql", "99.0", capability.MySQL84(), true, true, "9.x"},

		// MariaDB: MariaDB1011 is measured through the 11.x lines.
		{"mariadb below", "mariadb", "10.1.48-MariaDB", capability.MariaDBLegacy(), true, false, "11.x"},
		{"mariadb inside", "mariadb", "10.11.6-MariaDB", capability.MariaDB1011(), true, false, "11.x"},
		{"mariadb at the newest measured line", "mariadb", "11.8.2-MariaDB", capability.MariaDB1011(), true, false, "11.x"},
		{"mariadb above (#108 bumps CI to 12.x)", "mariadb", "12.3.0-MariaDB", capability.MariaDB1011(), true, true, "11.x"},
		{"mariadb far above", "mariadb", "99.0.0-MariaDB", capability.MariaDB1011(), true, true, "11.x"},
		{
			"mariadb above over the mysql replication prefix",
			"mysql", "5.5.5-12.3.0-MariaDB", capability.MariaDB1011(), true, true, "11.x",
		},

		// PostgreSQL: Postgres17 is the top preset and covers 17 only, while
		// the integration matrix already runs postgres:18.
		{"postgres below", "postgres", "PostgreSQL 13.14 (Debian)", capability.Postgres13(), true, false, "17.x"},
		{"postgres inside", "postgres", "PostgreSQL 16.3 (Debian)", capability.Postgres16(), true, false, "17.x"},
		{"postgres at the newest measured line", "postgres", "PostgreSQL 17.5", capability.Postgres17(), true, false, "17.x"},
		{"postgres above (CI runs postgres:18)", "postgres", "PostgreSQL 18.4 (Debian)", capability.Postgres17(), true, true, "17.x"},
		{"postgres far above", "postgres", "PostgreSQL 99.0", capability.Postgres17(), true, true, "17.x"},

		// Controls. An unparseable version never reports saturation, and a
		// dialect with no version ladder reports neither a ceiling nor
		// saturation — refining those is the rest of issue #916.
		{"mysql unparseable", "mysql", "who knows", capability.MySQL84(), false, false, ""},
		{"mariadb unparseable", "mariadb", "MariaDB something", capability.MariaDB1011(), false, false, "11.x"},
		{"postgres empty", "postgres", "", capability.Postgres17(), false, false, ""},
		{
			"cockroachdb resolves from the banner, no ladder",
			"postgres", "CockroachDB CCL v26.2.4 (x86_64-pc-linux-gnu)", capability.CockroachDB23(), true, false, "",
		},
		{
			"yugabytedb resolves from the banner, no ladder",
			"postgres", "PostgreSQL 15.2-YB-2026.1.0.0-b0 on x86_64-pc-linux-gnu", capability.YugabyteDB25(), true, false, "",
		},
		{
			"spanner is intentionally version-free",
			"postgres", "Cloud Spanner PostgreSQL interface", capability.SpannerPostgres(), true, false, "",
		},
		{"clickhouse has no ladder", "clickhouse", "25.3.1.100", capability.ClickHouse24(), false, false, ""},
		{"sqlite has no ladder", "sqlite", "3.53.0", capability.SQLite3(), false, false, ""},
		{
			"sqlserver has no ladder",
			"sqlserver", "Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4115.5", capability.SQLServer2022(), false, false, "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := capability.ResolveServerVersion(tt.dialect, tt.version)

			comment := qt.Commentf("dialect=%s version=%q", tt.dialect, tt.version)
			c.Assert(got.Capabilities.Validate(), qt.IsNil, comment)
			c.Assert(got.Capabilities, qt.DeepEquals, tt.wantCaps, comment)
			c.Assert(got.VersionSpecific, qt.Equals, tt.wantVersionSpecific, comment)
			c.Assert(got.Saturated, qt.Equals, tt.wantSaturated, comment)
			c.Assert(got.NewestMeasured, qt.Equals, tt.wantNewestMeasured, comment)

			// The two older entry points must keep answering exactly what they
			// answered before saturation existed, on every row above.
			caps, versionSpecific := capability.ForServerVersionResult(tt.dialect, tt.version)
			c.Assert(caps, qt.DeepEquals, tt.wantCaps, comment)
			c.Assert(versionSpecific, qt.Equals, tt.wantVersionSpecific, comment)
			c.Assert(capability.ForServerVersion(tt.dialect, tt.version), qt.DeepEquals, tt.wantCaps, comment)
		})
	}
}

func TestDoc(t *testing.T) {
	c := qt.New(t)

	for _, key := range capability.All() {
		c.Assert(capability.Doc(key), qt.Not(qt.Equals), "", qt.Commentf("registry entry %q must carry documentation", key))
	}
	c.Assert(capability.Doc("nope"), qt.Equals, "")
}
