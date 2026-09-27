package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// keyedTable is a table `ex` with an integer primary key `id` and a column `r`,
// holding constraints.
func keyedTable(constraints ...schemamodel.Constraint) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "E", Name: "ex"}},
		Fields: []schemamodel.Field{
			{StructName: "E", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "E", Name: "r", Type: "INTEGER"},
		},
		Constraints: constraints,
	}
	schemamodel.Finalize(database)
	return database
}

func excludeOnR(name, method string) schemamodel.Constraint {
	return schemamodel.Constraint{
		StructName: "E", Table: "ex", Name: name, Type: "EXCLUDE", UsingMethod: method, ExcludeElements: "r WITH =",
	}
}

func uniqueOn(name string, columns ...string) schemamodel.Constraint {
	return schemamodel.Constraint{StructName: "E", Table: "ex", Name: name, Type: "UNIQUE", Columns: columns}
}

// TestGetOrderedCreateStatements_FoldedIndexConstraintFollowsItsTable renders a
// UNIQUE or EXCLUDE that PostgreSQL would fold into another index constraint
// of the same CREATE TABLE as an ALTER TABLE right after it. Measured on
// PostgreSQL 18.6, both constraints of each row exist when they are added this
// way, and one does when both are written into the CREATE TABLE.
func TestGetOrderedCreateStatements_FoldedIndexConstraintFollowsItsTable(t *testing.T) {
	const table = "-- POSTGRES TABLE: ex --\nCREATE TABLE \"ex\" (\n" +
		"  \"id\" INTEGER PRIMARY KEY NOT NULL,\n  \"r\" INTEGER NOT NULL"
	tests := []struct {
		name     string
		database *schemamodel.Database
		want     []string
	}{
		{
			name:     "an EXCLUDE declared twice",
			database: keyedTable(excludeOnR("ex_r_excl", "btree"), excludeOnR("ex_r_excl1", "btree")),
			want: []string{
				table + ",\n  CONSTRAINT \"ex_r_excl\" EXCLUDE USING btree (r WITH =)\n);\n\n",
				"-- ALTER statements: --\nALTER TABLE \"ex\" ADD CONSTRAINT \"ex_r_excl1\" EXCLUDE USING btree (r WITH =);\n\n",
			},
		},
		{
			name:     "a UNIQUE over the primary key",
			database: keyedTable(uniqueOn("ex_id_key", "id")),
			want: []string{
				table + "\n);\n\n",
				"-- ALTER statements: --\nALTER TABLE \"ex\" ADD CONSTRAINT \"ex_id_key\" UNIQUE (\"id\");\n\n",
			},
		},
		{
			name:     "a UNIQUE declared twice, then another",
			database: keyedTable(uniqueOn("ex_r_key", "r"), uniqueOn("ex_r_key1", "r"), uniqueOn("ex_r_id_key", "r", "id")),
			want: []string{
				table + ",\n  CONSTRAINT \"ex_r_key\" UNIQUE (\"r\"),\n  CONSTRAINT \"ex_r_id_key\" UNIQUE (\"r\", \"id\")\n);\n\n",
				"-- ALTER statements: --\nALTER TABLE \"ex\" ADD CONSTRAINT \"ex_r_key1\" UNIQUE (\"r\");\n\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(test.database, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatements_IndexConstraintsTheServerKeepsStayInTheTable
// is the control: constraints PostgreSQL builds one index each for, and
// constraints on a dialect this rule does not describe, stay in the CREATE
// TABLE.
func TestGetOrderedCreateStatements_IndexConstraintsTheServerKeepsStayInTheTable(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
		want     string
	}{
		{
			name:     "an EXCLUDE under two access methods",
			dialect:  platform.Postgres,
			database: keyedTable(excludeOnR("ex_r_excl", "btree"), excludeOnR("ex_r_excl1", "hash")),
			want: "-- POSTGRES TABLE: ex --\nCREATE TABLE \"ex\" (\n" +
				"  \"id\" INTEGER PRIMARY KEY NOT NULL,\n  \"r\" INTEGER NOT NULL,\n" +
				"  CONSTRAINT \"ex_r_excl\" EXCLUDE USING btree (r WITH =),\n" +
				"  CONSTRAINT \"ex_r_excl1\" EXCLUDE USING hash (r WITH =)\n);\n\n",
		},
		{
			name:     "a UNIQUE declared twice on MySQL",
			dialect:  platform.MySQL,
			database: keyedTable(uniqueOn("ex_r_key", "r"), uniqueOn("ex_r_key1", "r")),
			want: "-- MYSQL TABLE: ex --\nCREATE TABLE `ex` (\n" +
				"  `id` INTEGER PRIMARY KEY,\n  `r` INTEGER NOT NULL,\n" +
				"  CONSTRAINT `ex_r_key` UNIQUE (`r`),\n  CONSTRAINT `ex_r_key1` UNIQUE (`r`)\n);\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(test.database, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}
