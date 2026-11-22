package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

func checkNamingDatabase(constraints ...schemamodel.Constraint) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}, {StructName: "U", Name: "u"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "v", Type: "Int32", Check: "v > 0"},
			{StructName: "U", Name: "id", Type: "UInt64"},
		},
		Constraints: constraints,
	}
}

func check(table, name, expression string) schemamodel.Constraint {
	return schemamodel.Constraint{StructName: map[string]string{"t": "T", "u": "U"}[table], Table: table, Type: "CHECK", Name: name, CheckExpression: expression}
}

// ClickHouse refuses a CHECK without a name, so each one a schema leaves
// unnamed takes the name an entry of the table's checks list would: past the
// names the table declares and the names its columns' CHECKs take, and per
// table.
func TestAssignDefaultCheckNames_NamesAnUnnamedCheckOnClickHouse(t *testing.T) {
	for _, test := range []struct {
		name  string
		given []schemamodel.Constraint
		// columnCheckName is the name column v declares for its CHECK.
		columnCheckName string
		want            []string
	}{
		{
			name:  "the first takes <table>_check and the next a number",
			given: []schemamodel.Constraint{check("t", "", "v < 10"), check("t", "", "v != 5")},
			want:  []string{"t_check", "t_check1"},
		},
		{
			name:  "a declared name is passed over",
			given: []schemamodel.Constraint{check("t", "", "v < 10"), check("t", "t_check1", "v != 5"), check("t", "", "v != 6"), check("t", "t_check", "v != 7")},
			want:  []string{"t_check2", "t_check1", "t_check3", "t_check"},
		},
		{
			name:            "a column CHECK's name is passed over",
			given:           []schemamodel.Constraint{check("t", "", "v < 10")},
			columnCheckName: "t_check",
			want:            []string{"t_check1"},
		},
		{
			name:  "each table numbers its own",
			given: []schemamodel.Constraint{check("t", "", "v < 10"), check("u", "", "id > 0"), check("u", "", "id < 9")},
			want:  []string{"t_check", "u_check", "u_check1"},
		},
		{
			name:  "a constraint of another kind keeps no name",
			given: []schemamodel.Constraint{{StructName: "U", Table: "u", Type: "UNIQUE", Columns: []string{"id"}}, check("u", "", "id > 0")},
			want:  []string{"", "u_check"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := checkNamingDatabase(test.given...)
			database.Fields[0].CheckName = test.columnCheckName

			assigned := schemaprep.AssignDefaultCheckNames(database, platform.ClickHouse)

			names := make([]string, len(assigned.Constraints))
			for i, constraint := range assigned.Constraints {
				names[i] = constraint.Name
			}
			c.Assert(names, qt.DeepEquals, test.want)
			c.Assert(database.Constraints, qt.DeepEquals, test.given)
		})
	}
}

// A server that names a CHECK itself is left to name it, since the comparison
// looks for the name that server gives; and a schema with nothing to name is
// the input itself.
func TestAssignDefaultCheckNames_LeavesTheSchemaAsItIs(t *testing.T) {
	for _, test := range []struct {
		name     string
		dialect  string
		database *schemamodel.Database
	}{
		{name: "PostgreSQL names a CHECK", dialect: platform.Postgres, database: checkNamingDatabase(check("t", "", "v < 10"))},
		{name: "MySQL names a CHECK", dialect: platform.MySQL, database: checkNamingDatabase(check("t", "", "v < 10"))},
		{name: "every CHECK has a name", dialect: platform.ClickHouse, database: checkNamingDatabase(check("t", "c", "v < 10"))},
		{name: "no schema", dialect: platform.ClickHouse},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := schemaprep.AssignDefaultCheckNames(test.database, test.dialect)

			c.Assert(got, qt.Equals, test.database)
		})
	}
}

// The constraint names every path gives a schema are the foreign key's and the
// CHECK's together.
func TestAssignDefaultConstraintNames_NamesForeignKeysAndChecks(t *testing.T) {
	c := qt.New(t)
	database := checkNamingDatabase(check("t", "", "v < 10"), schemamodel.Constraint{
		StructName: "U", Table: "u", Type: "FOREIGN KEY", Columns: []string{"id"}, ForeignTable: "t", ForeignColumn: "v",
	})

	assigned := schemaprep.AssignDefaultConstraintNames(database, platform.ClickHouse)

	c.Assert(assigned.Constraints[0].Name, qt.Equals, "t_check")
	c.Assert(assigned.Constraints[1].Name, qt.Not(qt.Equals), "")
}
