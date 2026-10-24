package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestTablesAndColumns_CountsThePairs pairs one table of three on each side.
// The columns are paired in that table alone: a table one side holds is
// counted as an addition or a removal, and its columns are not compared.
func TestTablesAndColumns_CountsThePairs(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "users", StructName: "User"},
			{Name: "audit", StructName: "Audit"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
			{StructName: "User", Name: "nick", Type: "TEXT", Nullable: true},
			{StructName: "Audit", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Audit", Name: "at", Type: "TEXT"},
		},
	}
	database := &catalog.Database{Tables: []catalog.Table{
		{Name: "users", Columns: []catalog.Column{
			{Name: "id", DataType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "email", DataType: "TEXT", IsNullable: "NO"},
			{Name: "legacy", DataType: "TEXT", IsNullable: "YES"},
		}},
		{Name: "old", Columns: []catalog.Column{{Name: "id", DataType: "INTEGER", IsNullable: "NO"}}},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.TablesAndColumns(desired, database, diff)

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Tables: 1, Columns: 2})
	c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{"audit"})
	c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{"old"})
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsAdded, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsRemoved, qt.HasLen, 1)
}

// TestTablesAndColumns_CountsATableThatDoesNotDiffer is the pair the
// modifications leave out: a table both sides hold alike has no entry in
// TablesModified, and it is still held by both sides.
func TestTablesAndColumns_CountsATableThatDoesNotDiffer(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "email", Type: "TEXT", Nullable: true}},
	}
	database := &catalog.Database{Tables: []catalog.Table{
		{Name: "users", Columns: []catalog.Column{{Name: "email", DataType: "TEXT", IsNullable: "YES"}}},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.TablesAndColumns(desired, database, diff)

	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Tables: 1, Columns: 1})
}

// TestIndexes_CountsTheIndexesThePlanKeeps pairs an index both sides define
// alike, and one the plan keeps and only hides from the optimizer. An index
// whose definition changed, here its predicate, is dropped and created again,
// so it is the removal and the addition rather than a pair.
func TestIndexes_CountsTheIndexesThePlanKeeps(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Indexes: []schemamodel.Index{
		{Name: "idx_kept", TableName: "t", Fields: []string{"a"}},
		{Name: "idx_hidden", TableName: "t", Fields: []string{"h"}, Invisible: true},
		{Name: "idx_changed", TableName: "t", Fields: []string{"b"}, Condition: "b > 0"},
		{Name: "idx_new", TableName: "t", Fields: []string{"c"}},
	}}
	database := &catalog.Database{Indexes: []catalog.Index{
		{Name: "idx_kept", TableName: "t", Columns: []string{"a"}},
		{Name: "idx_hidden", TableName: "t", Columns: []string{"h"}},
		{Name: "idx_changed", TableName: "t", Columns: []string{"b"}},
		{Name: "idx_gone", TableName: "t", Columns: []string{"e"}},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Indexes(desired, database, diff)

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Indexes: 2})
	c.Assert(diff.IndexVisibilityChanged, qt.HasLen, 1)
	c.Assert(diff.IndexesAdded, qt.HasLen, 2)
	c.Assert(diff.IndexesRemoved, qt.HasLen, 2)
}

// TestIndexes_YDBCountsARenamedIndexAsAPair is the one index the plan keeps
// under another name: one object on each side, neither added nor removed.
func TestIndexes_YDBCountsARenamedIndexAsAPair(t *testing.T) {
	c := qt.New(t)
	desired := []schemamodel.Index{namedIndex(ydbDeclaredIndex("async", false, []string{"a"}, nil), "by_a")}
	database := []catalog.Index{namedCatalogIndex(ydbIndex("GLOBAL ASYNC", false, []string{"a"}, nil), "old_a")}

	diff := compareYDBIndexes(desired, database, capability.YDB262())

	c.Assert(diff.IndexesRenamed, qt.HasLen, 1)
	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Indexes: 1})
}

// TestEnums_CountsTheTypesAndValuesBothSidesHold pairs the enum both sides
// hold and the values it holds on both, and leaves a type one side holds to
// its addition or removal.
func TestEnums_CountsTheTypesAndValuesBothSidesHold(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Enums: []schemamodel.Enum{
		{Name: "mood", Values: []string{"happy", "sad", "calm"}},
		{Name: "status", Values: []string{"open"}},
	}}
	database := &catalog.Database{Enums: []catalog.Enum{
		{Name: "mood", Values: []string{"happy", "sad", "angry"}},
		{Name: "legacy", Values: []string{"x"}},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Enums(desired, database, diff)

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Enums: 1, EnumValues: 2})
	c.Assert(diff.EnumsAdded, qt.HasLen, 1)
	c.Assert(diff.EnumsRemoved, qt.HasLen, 1)
}

// TestFunctions_CountsTheRoutinesBothSidesHold covers both pairing paths: the
// one a comparison without a default schema takes, and the one that pairs
// overloads under the target's default schema.
func TestFunctions_CountsTheRoutinesBothSidesHold(t *testing.T) {
	tests := []struct {
		name      string
		semantics identifier.Semantics
	}{
		{name: "without a default schema", semantics: identifier.Semantics{}},
		{name: "under the default schema", semantics: identifier.ForDialect("postgres")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "add_one", Parameters: "i integer", Returns: "integer", Language: "sql", Body: "SELECT i + 1"},
				{Name: "fresh", Parameters: "", Returns: "integer", Language: "sql", Body: "SELECT 1"},
			}}
			database := &catalog.Database{Functions: []catalog.Function{
				{Name: "add_one", Parameters: "i integer", Returns: "integer", Language: "sql", Body: "SELECT i + 1"},
				{Name: "stale", Parameters: "", Returns: "integer", Language: "sql", Body: "SELECT 2"},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.FunctionsWithSemantics(desired, database, diff, "postgres", test.semantics, nil, nil)

			c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Functions: 1})
			c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
			c.Assert(diff.FunctionsRemoved, qt.HasLen, 1)
		})
	}
}

// TestRLS_CountsPoliciesAndEnabledTablesBothSidesHold pairs a policy and an
// enablement each side holds, and leaves the ones one side holds to their
// additions and removals.
func TestRLS_CountsPoliciesAndEnabledTablesBothSidesHold(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "p_kept", Table: "docs", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "true"},
			{Name: "p_new", Table: "docs", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "true"},
		},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "docs"}, {Table: "notes"}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{Name: "docs", RLSEnabled: true}, {Name: "notes"}, {Name: "logs", RLSEnabled: true}},
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "p_kept", Table: "docs", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "true"},
			{Name: "p_gone", Table: "docs", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "true"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPolicies(desired, database, diff, compare.CoverageOf(desired, database))
	compare.RLSEnabledTables(desired, database, diff)

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{RLSPolicies: 1, RLSEnabledTables: 1})
	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 1)
	c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 1)
	c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 1)
	c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 1)
}

// TestRoles_CountsTheDeclaredRolesTheDatabaseHolds pairs a declared role the
// database holds. A role the declaration does not name is not managed, so it
// is counted on neither side.
func TestRoles_CountsTheDeclaredRolesTheDatabaseHolds(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Login: true}, {Name: "fresh"}}}
	database := &catalog.Database{Roles: []catalog.Role{{Name: "app", Login: true}, {Name: "dba"}}}
	diff := &difftypes.SchemaDiff{}

	compare.Roles(desired, database, diff, compare.CoverageOf(desired, database))

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Roles: 1})
	c.Assert(diff.RolesAdded, qt.HasLen, 1)
	c.Assert(diff.RolesRemoved, qt.HasLen, 0)
}

// TestConstraints_CountsTheConstraintsThePlanKeeps pairs a CHECK both sides
// define alike. A CHECK whose expression changed is dropped and added again,
// so it is the removal and the addition rather than a pair.
func TestConstraints_CountsTheConstraintsThePlanKeeps(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t", StructName: "T"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "a", Type: "INTEGER"}},
		Constraints: []schemamodel.Constraint{
			{StructName: "T", Table: "t", Name: "chk_kept", Type: "CHECK", CheckExpression: "a > 0"},
			{StructName: "T", Table: "t", Name: "chk_changed", Type: "CHECK", CheckExpression: "a < 10"},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{{Name: "a", DataType: "INTEGER", IsNullable: "NO"}}}},
		Constraints: []catalog.Constraint{
			{TableName: "t", Name: "chk_kept", Type: "CHECK", CheckClause: new("a > 0")},
			{TableName: "t", Name: "chk_changed", Type: "CHECK", CheckClause: new("a < 20")},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Constraints(desired, database, diff, &config.CompareOptions{})

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Constraints: 1})
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
}

// TestServerSchemas_CountsTheDatabasesBothSidesHold pairs a database a whole
// server holds and the declaration names.
func TestServerSchemas_CountsTheDatabasesBothSidesHold(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "fresh"}}}
	diff := &difftypes.SchemaDiff{}

	compare.ServerSchemas(diff, desired, []catalog.Schema{{Name: "app"}, {Name: "legacy"}}, identifier.ForDialect("mysql"))

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Schemas: 1})
	c.Assert(diff.SchemasAdded, qt.HasLen, 1)
	c.Assert(diff.SchemasRemoved, qt.HasLen, 1)
}
