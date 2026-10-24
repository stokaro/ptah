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

// TestSequences_CountsTheSequencesBothSidesHold pairs a sequence each side
// holds, whether or not its options differ, and leaves the ones one side holds
// to their additions and removals.
func TestSequences_CountsTheSequencesBothSidesHold(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Sequences: []schemamodel.Sequence{
		{Name: "kept"}, {Name: "changed", Increment: new(int64(2))}, {Name: "fresh"},
	}}
	database := &catalog.Database{Sequences: []catalog.Sequence{
		{Name: "kept"}, {Name: "changed", Increment: new(int64(1))}, {Name: "stale"},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Sequences(desired, database, diff, compare.CoverageOf(desired, database))

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Sequences: 2})
	c.Assert(diff.SequencesModified, qt.HasLen, 1)
	c.Assert(diff.SequencesAdded.Names(), qt.DeepEquals, []string{"fresh"})
	c.Assert(diff.SequencesRemoved.Names(), qt.DeepEquals, []string{"stale"})
}

// TestViewLikes_CountsTheViewsBothSidesHold covers both walks a view and a
// materialized view are paired by: by name, where the target has no default
// schema, and by identity under the target's default schema. A view whose
// body differs is a pair as well as a modification.
func TestViewLikes_CountsTheViewsBothSidesHold(t *testing.T) {
	desired := &schemamodel.Database{
		Views: []schemamodel.View{
			{Name: "kept", Body: "SELECT 1"}, {Name: "changed", Body: "SELECT 2"}, {Name: "fresh", Body: "SELECT 3"},
		},
		MaterializedViews: []schemamodel.MaterializedView{
			{Name: "daily", Body: "SELECT 1"}, {Name: "weekly", Body: "SELECT 2"},
		},
	}
	database := &catalog.Database{
		Views: []catalog.View{
			{Schema: "public", Name: "kept", Body: "SELECT 1"},
			{Schema: "public", Name: "changed", Body: "SELECT 4"},
			{Schema: "public", Name: "stale", Body: "SELECT 5"},
		},
		MatViews: []catalog.MaterializedView{
			{Schema: "public", Name: "daily", Body: "SELECT 1"},
			{Schema: "public", Name: "monthly", Body: "SELECT 3"},
		},
	}
	tests := []struct {
		name    string
		compare func(*difftypes.SchemaDiff)
	}{
		{
			name: "by name",
			compare: func(diff *difftypes.SchemaDiff) {
				compare.ViewsWithDialect(desired, database, diff, "postgres")
				compare.MaterializedViewsWithDialect(desired, database, diff, "postgres")
			},
		},
		{
			name: "by identity",
			compare: func(diff *difftypes.SchemaDiff) {
				semantics := identifier.ForDialect("postgres")
				compare.ViewsWithSemantics(desired, database, diff, "postgres", semantics, nil, nil)
				compare.MaterializedViewsWithSemantics(desired, database, diff, "postgres", semantics, nil, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			test.compare(diff)

			c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Views: 2, MaterializedViews: 1})
			c.Assert(diff.ViewsModified, qt.HasLen, 1)
			c.Assert(diff.ViewsAdded, qt.HasLen, 1)
			c.Assert(diff.ViewsRemoved, qt.HasLen, 1)
			c.Assert(diff.MaterializedViewsAdded, qt.HasLen, 1)
			c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 1)
		})
	}
}

// TestTriggers_CountsTheTriggersBothSidesHold pairs the trigger each side
// holds and leaves the ones one side holds to their additions and removals.
func TestTriggers_CountsTheTriggersBothSidesHold(t *testing.T) {
	c := qt.New(t)
	desired := declaringTrigger("orders")
	desired.Triggers = append(desired.Triggers, schemamodel.Trigger{
		StructName: "Order", Name: "fresh", Table: "orders",
		Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SET @x = 1;",
	})
	database := holdingTrigger("", "orders")
	database.Triggers = append(database.Triggers, catalog.Trigger{
		Name: "stale", Table: "orders", Timing: "AFTER", Event: "DELETE", ForEach: "ROW", Body: "SET @x = 1;",
	})
	diff := &difftypes.SchemaDiff{}

	compare.TriggersWithDialect(desired, database, diff, "mysql")

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Triggers: 1})
	c.Assert(diff.TriggersAdded, qt.HasLen, 1)
	c.Assert(diff.TriggersRemoved, qt.HasLen, 1)
}

// TestGrants_CountsTheGrantsBothSidesHold pairs a declared privilege the
// database holds, and one whose grant option differs: the option is a setting
// of the grant, so the grant is still one both sides hold. A declared ALL the
// database reports as one row per privilege is one pair.
func TestGrants_CountsTheGrantsBothSidesHold(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Roles: []schemamodel.Role{{Name: "reader"}},
		Grants: []schemamodel.Grant{
			{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "users"},
			{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "users", WithOption: true},
			{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "users"},
			{Role: "reader", Privileges: []string{"USAGE"}, OnSequence: "order_seq"},
		},
	}
	database := &catalog.Database{
		Roles: []catalog.Role{{Name: "reader"}},
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users"},
			{Role: "reader", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
			{Role: "reader", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "order_seq"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Grants(desired, database, diff)

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Grants: 3})
	c.Assert(diff.GrantOptionsAdded, qt.HasLen, 1)
	c.Assert(diff.GrantsAdded, qt.HasLen, 1)
	c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
}

// TestRoleMemberships_CountsTheMembershipsBothSidesHold pairs a declared
// membership the database holds, on the target that compares memberships.
func TestRoleMemberships_CountsTheMembershipsBothSidesHold(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{
		{Name: "readers"}, {Name: "writers"}, {Name: "app", MemberOf: []string{"readers"}},
		{Name: "batch", MemberOf: []string{"writers"}},
	}}
	database := &catalog.Database{RoleMemberships: []catalog.RoleMembership{
		{Role: "readers", Member: "app"}, {Role: "readers", Member: "batch"},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.RoleMemberships(desired, database, diff, capability.YDB262())

	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{RoleMemberships: 1})
	c.Assert(diff.RoleMembershipsAdded, qt.DeepEquals, []difftypes.RoleMembershipRef{{Role: "writers", Member: "batch"}})
	c.Assert(diff.RoleMembershipsRemoved, qt.DeepEquals, []difftypes.RoleMembershipRef{{Role: "readers", Member: "batch"}})
}
