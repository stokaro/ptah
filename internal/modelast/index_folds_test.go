package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
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

func excludeOnR(name, method string) schemaext.Object {
	return schemaext.Object{Ref: pgtable.ExclusionRef("ex", name, identifier.ForDialect("postgres")),
		Value: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: method, Elements: "r WITH ="}}}
}

func exclusionKeyedTable(objects ...schemaext.Object) *schemamodel.Database {
	database := keyedTable()
	database.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	return database
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
	const table = "CREATE TABLE \"ex\" (\n" +
		"  \"id\" INTEGER PRIMARY KEY NOT NULL,\n  \"r\" INTEGER NOT NULL"
	tests := []struct {
		name     string
		database *schemamodel.Database
		want     []string
	}{
		{
			name:     "an EXCLUDE declared twice",
			database: exclusionKeyedTable(excludeOnR("ex_r_excl", "btree"), excludeOnR("ex_r_excl1", "btree")),
			want: []string{
				table + "\n);\n\n",
				"ALTER TABLE \"ex\" ADD CONSTRAINT \"ex_r_excl\" EXCLUDE USING btree (r WITH =);\n\n",
				"ALTER TABLE \"ex\" ADD CONSTRAINT \"ex_r_excl1\" EXCLUDE USING btree (r WITH =);\n\n",
			},
		},
		{
			name:     "a UNIQUE over the primary key",
			database: keyedTable(uniqueOn("ex_id_key", "id")),
			want: []string{
				table + "\n);\n\n",
				"ALTER TABLE \"ex\" ADD CONSTRAINT \"ex_id_key\" UNIQUE (\"id\");\n\n",
			},
		},
		{
			name:     "a UNIQUE declared twice, then another",
			database: keyedTable(uniqueOn("ex_r_key", "r"), uniqueOn("ex_r_key1", "r"), uniqueOn("ex_r_id_key", "r", "id")),
			want: []string{
				table + ",\n  CONSTRAINT \"ex_r_key\" UNIQUE (\"r\"),\n  CONSTRAINT \"ex_r_id_key\" UNIQUE (\"r\", \"id\")\n);\n\n",
				"ALTER TABLE \"ex\" ADD CONSTRAINT \"ex_r_key1\" UNIQUE (\"r\");\n\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(test.database, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// Every declared identity survives when definitions or the target differ.
func TestGetOrderedCreateStatements_KeepsDistinctIndexConstraints(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
		want     []string
	}{
		{
			name:     "an EXCLUDE under two access methods",
			dialect:  platform.Postgres,
			database: exclusionKeyedTable(excludeOnR("ex_r_excl", "btree"), excludeOnR("ex_r_excl1", "hash")),
			want: []string{"CREATE TABLE \"ex\" (\n" +
				"  \"id\" INTEGER PRIMARY KEY NOT NULL,\n  \"r\" INTEGER NOT NULL\n);\n\n",
				"ALTER TABLE \"ex\" ADD CONSTRAINT \"ex_r_excl\" EXCLUDE USING btree (r WITH =);\n\n",
				"ALTER TABLE \"ex\" ADD CONSTRAINT \"ex_r_excl1\" EXCLUDE USING hash (r WITH =);\n\n"},
		},
		{
			name:     "a UNIQUE declared twice on MySQL",
			dialect:  platform.MySQL,
			database: keyedTable(uniqueOn("ex_r_key", "r"), uniqueOn("ex_r_key1", "r")),
			want: []string{"CREATE TABLE `ex` (\n" +
				"  `id` INTEGER PRIMARY KEY,\n  `r` INTEGER NOT NULL,\n" +
				"  CONSTRAINT `ex_r_key` UNIQUE (`r`),\n  CONSTRAINT `ex_r_key1` UNIQUE (`r`)\n);\n\n"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(test.database, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// keyedTableWithColumnKey is [keyedTable] with column's own UNIQUE declared.
func keyedTableWithColumnKey(column string, constraints ...schemamodel.Constraint) *schemamodel.Database {
	database := keyedTable(constraints...)
	for i := range database.Fields {
		if database.Fields[i].Name == column {
			database.Fields[i].Unique = true
		}
	}
	return database
}

// TestGetOrderedCreateStatements_FoldingColumnKeyFollowsItsTable renders a
// column's own UNIQUE that PostgreSQL would fold into an equal key of the same
// CREATE TABLE so that both are built. Measured on PostgreSQL 18.6,
// `a int UNIQUE, CONSTRAINT uq_a UNIQUE (a)` and `a int UNIQUE, PRIMARY KEY
// (a)` build one key each in one CREATE TABLE, and two when the second is
// added by ALTER TABLE.
func TestGetOrderedCreateStatements_FoldingColumnKeyFollowsItsTable(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
		want     []string
	}{
		{
			name:     "beside a UNIQUE over the column",
			database: keyedTableWithColumnKey("r", uniqueOn("uq_r", "r")),
			want: []string{
				"CREATE TABLE \"ex\" (\n" +
					"  \"id\" INTEGER PRIMARY KEY NOT NULL,\n  \"r\" INTEGER UNIQUE NOT NULL\n);\n\n",
				"ALTER TABLE \"ex\" ADD CONSTRAINT \"uq_r\" UNIQUE (\"r\");\n\n",
			},
		},
		{
			name: "on the column of a PRIMARY KEY entry",
			database: func() *schemamodel.Database {
				database := &schemamodel.Database{
					Tables: []schemamodel.Table{{StructName: "E", Name: "ex"}},
					Fields: []schemamodel.Field{{StructName: "E", Name: "r", Type: "INTEGER", Unique: true}},
					Constraints: []schemamodel.Constraint{
						{StructName: "E", Table: "ex", Name: "ex_pk", Type: "PRIMARY KEY", Columns: []string{"r"}},
					},
				}
				schemamodel.Finalize(database)
				return database
			}(),
			want: []string{
				"CREATE TABLE \"ex\" (\n" +
					"  \"r\" INTEGER NOT NULL,\n  CONSTRAINT \"ex_pk\" PRIMARY KEY (\"r\")\n);\n\n",
				"ALTER TABLE \"ex\" ADD UNIQUE (\"r\");\n\n",
			},
		},
		{
			name:     "on the primary key column",
			database: keyedTableWithColumnKey("id"),
			want: []string{
				"CREATE TABLE \"ex\" (\n" +
					"  \"id\" INTEGER PRIMARY KEY NOT NULL,\n  \"r\" INTEGER NOT NULL\n);\n\n",
				"ALTER TABLE \"ex\" ADD UNIQUE (\"id\");\n\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(test.database, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatements_ColumnKeyTheServerKeepsStaysInTheTable is the
// control: a column's own UNIQUE beside a UNIQUE the column only leads is its
// own index, and stays in the CREATE TABLE.
func TestGetOrderedCreateStatements_ColumnKeyTheServerKeepsStaysInTheTable(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(
		keyedTableWithColumnKey("r", uniqueOn("uq_r_id", "r", "id")), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE \"ex\" (\n" +
			"  \"id\" INTEGER PRIMARY KEY NOT NULL,\n  \"r\" INTEGER UNIQUE NOT NULL,\n" +
			"  CONSTRAINT \"uq_r_id\" UNIQUE (\"r\", \"id\")\n);\n\n",
	})
}
