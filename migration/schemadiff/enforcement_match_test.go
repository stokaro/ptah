package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// checkTable declares `t (id, n)` with a CHECK t_n_positive the server checks
// or not.
func checkTable(notEnforced bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "n", Type: "int", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Table: "t", Name: "t_n_positive", Type: "CHECK", CheckExpression: "n > 0",
			NotEnforced: notEnforced,
		}},
	}
}

// liveCheckTable is the same table as a catalog reports it.
func liveCheckTable(notEnforced bool) *catalog.Database {
	clause := "n > 0"
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{
			{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "n", DataType: "int", IsNullable: "YES"},
		}}},
		Constraints: []catalog.Constraint{{
			Name: "t_n_positive", TableName: "t", Type: "CHECK", CheckClause: &clause, NotEnforced: notEnforced,
		}},
	}
}

// foreignKeyPair declares `p (id)` and `c (id, p_id)` with no key yet.
func foreignKeyPair() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p", PrimaryKey: []string{"id"}},
			{StructName: "C", Name: "c", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "p_id", Type: "int", Nullable: true},
		},
	}
}

// columnForeignKey declares c_p_fkey on the column, of the given MATCH type
// and enforcement.
func columnForeignKey(match string, notEnforced bool) *schemamodel.Database {
	database := foreignKeyPair()
	database.Fields[2].Foreign = "p(id)"
	database.Fields[2].ForeignKeyName = "c_p_fkey"
	database.Fields[2].ForeignKeyMatch = match
	database.Fields[2].ForeignKeyNotEnforced = notEnforced
	return database
}

// tableForeignKey declares c_p_fkey on the table.
func tableForeignKey(match string, notEnforced bool) *schemamodel.Database {
	database := foreignKeyPair()
	database.Constraints = []schemamodel.Constraint{{
		StructName: "C", Table: "c", Name: "c_p_fkey", Type: "FOREIGN KEY", Columns: []string{"p_id"},
		ForeignTable: "p", ForeignColumn: "id", Match: match, NotEnforced: notEnforced,
	}}
	return database
}

// liveForeignKeyTable is the same pair as a catalog reports it.
func liveForeignKeyTable(match string, notEnforced bool) *catalog.Database {
	parent, column, action := "p", "id", "NO ACTION"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true}}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "p_id", DataType: "int", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name: "c_p_fkey", TableName: "c", Type: "FOREIGN KEY", ColumnName: "p_id", ColumnNames: []string{"p_id"},
			ForeignTable: &parent, ForeignColumn: &column, ForeignColumns: []string{"id"},
			DeleteRule: &action, UpdateRule: &action, Match: match, NotEnforced: notEnforced,
		}},
	}
}

// changedConstraints lists what a diff adds and removes, as `+name` and
// `-name`.
func changedConstraints(diff *difftypes.SchemaDiff) []string {
	var changed []string
	for _, added := range diff.ConstraintsAdded {
		changed = append(changed, "+"+added.Name)
	}
	for _, removed := range diff.ConstraintsRemoved {
		changed = append(changed, "-"+removed.Name)
	}
	return changed
}

// TestCompare_EnforcementAndMatch_Synced pairs a declaration with the
// constraint the server built from it (stokaro/ptah#3853). MySQL reports
// MATCH SIMPLE as NONE, which the reader leaves empty.
func TestCompare_EnforcementAndMatch_Synced(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
		live    *catalog.Database
	}{
		{name: "a CHECK not enforced", dialect: platform.Postgres, desired: checkTable(true), live: liveCheckTable(true)},
		{name: "a CHECK enforced", dialect: platform.MySQL, desired: checkTable(false), live: liveCheckTable(false)},
		{
			name: "a column's foreign key", dialect: platform.Postgres,
			desired: columnForeignKey("FULL", true), live: liveForeignKeyTable("FULL", true),
		},
		{
			name: "a table's foreign key", dialect: platform.MySQL,
			desired: tableForeignKey("PARTIAL", false), live: liveForeignKeyTable("PARTIAL", false),
		},
		{
			name: "MATCH SIMPLE spelled out", dialect: platform.Postgres,
			desired: tableForeignKey("SIMPLE", false), live: liveForeignKeyTable("", false),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(test.dialect, test.desired, test.live)

			c.Assert(changedConstraints(diff), qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
		})
	}
}

// TestCompare_EnforcementAndMatch_Changed plans a drop and an add of a
// constraint whose enforcement or MATCH type differs from the one the database
// holds, and the addition carries the clause. PostgreSQL 18.6 cannot change a
// CHECK's enforcement in place (`cannot alter enforceability`), and a drop and
// an add is the spelling every target shares.
func TestCompare_EnforcementAndMatch_Changed(t *testing.T) {
	tests := []struct {
		name            string
		desired         *schemamodel.Database
		live            *catalog.Database
		wantMatch       string
		wantNotEnforced bool
	}{
		{
			name: "a CHECK stops being enforced", desired: checkTable(true), live: liveCheckTable(false),
			wantNotEnforced: true,
		},
		{name: "a CHECK is enforced again", desired: checkTable(false), live: liveCheckTable(true)},
		{
			name: "a column's foreign key takes MATCH FULL", desired: columnForeignKey("FULL", false),
			live: liveForeignKeyTable("", false), wantMatch: "FULL",
		},
		{
			name: "a table's foreign key stops being enforced", desired: tableForeignKey("", true),
			live: liveForeignKeyTable("", false), wantNotEnforced: true,
		},
		{
			name: "a foreign key drops MATCH FULL", desired: tableForeignKey("", false),
			live: liveForeignKeyTable("FULL", false),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.Postgres, test.desired, test.live)

			c.Assert(changedConstraints(diff), qt.HasLen, 2)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
			c.Assert(diff.ConstraintsAdded[0].Match, qt.Equals, test.wantMatch)
			c.Assert(diff.ConstraintsAdded[0].NotEnforced, qt.Equals, test.wantNotEnforced)
		})
	}
}

// TestDeduplicate_KeepsConstraintsThatDifferInHowTheyCheck keeps two unnamed
// CHECKs of one condition, one of them not enforced: they are two constraints.
func TestDeduplicate_KeepsConstraintsThatDifferInHowTheyCheck(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Constraints: []schemamodel.Constraint{
		{StructName: "T", Table: "t", Type: "CHECK", CheckExpression: "n > 0"},
		{StructName: "T", Table: "t", Type: "CHECK", CheckExpression: "n > 0", NotEnforced: true},
		{StructName: "T", Table: "t", Type: "FOREIGN KEY", Columns: []string{"p"}, ForeignTable: "p", ForeignColumn: "id"},
		{StructName: "T", Table: "t", Type: "FOREIGN KEY", Columns: []string{"p"}, ForeignTable: "p", ForeignColumn: "id", Match: "FULL"},
	}}

	schemamodel.Deduplicate(database)

	c.Assert(database.Constraints, qt.HasLen, 4)
}

// TestCompareSchemas_UnnamedChecksThatDifferInEnforcementPairWithThemselves
// compares a desired state with itself where a table declares one condition
// twice without a name, enforced and not: nothing is planned. Keyed by the
// condition alone, the two would be one on each side, and each side would keep
// a different one of them.
func TestCompareSchemas_UnnamedChecksThatDifferInEnforcementPairWithThemselves(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareSchemas(oneConditionTwice(), oneConditionTwice(), platform.Postgres)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

// TestCompareSchemas_AnUnnamedCheckAddedBesideItsEnforcedTwin adds the
// unenforced copy of a condition the table already checks: the plan adds that
// one CHECK and drops nothing. Keyed by the condition alone, the declared pair
// would be one CHECK, and the plan would either add nothing or swap the
// enforced CHECK for the other.
func TestCompareSchemas_AnUnnamedCheckAddedBesideItsEnforcedTwin(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareSchemas(oneConditionTwice(), unnamedChecksDesired("hi > 0"), platform.Postgres)

	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].NotEnforced, qt.IsTrue)
}

// oneConditionTwice declares table d with the unnamed CHECK `hi > 0` twice, the
// second not enforced.
func oneConditionTwice() *schemamodel.Database {
	database := unnamedChecksDesired("hi > 0", "hi > 0")
	database.Constraints[1].NotEnforced = true
	return database
}
