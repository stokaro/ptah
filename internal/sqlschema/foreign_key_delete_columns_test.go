package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/postgres/pgtable"
)

// A table-level foreign key's ON DELETE column list reaches the model's
// constraint, normalized the way its key columns are.
func TestRead_OnDeleteColumnListReachesTheConstraint_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantColumns []string
	}{
		{
			name: "one column of a composite key",
			sql: "CREATE TABLE p (a int, b int, PRIMARY KEY (a, b));\n" +
				"CREATE TABLE c (x int NOT NULL, a int, CONSTRAINT c_fk FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET NULL (a));",
			wantColumns: []string{"a"},
		},
		{
			name: "a quoted column",
			sql: "CREATE TABLE p (a int, b int, PRIMARY KEY (a, b));\n" +
				`CREATE TABLE c ("X" int, "Aa" int, CONSTRAINT c_fk FOREIGN KEY ("X", "Aa") REFERENCES p (a, b) ON DELETE SET NULL ("Aa"));`,
			wantColumns: []string{"Aa"},
		},
		{
			name: "added by ALTER TABLE",
			sql: "CREATE TABLE p (a int, b int, PRIMARY KEY (a, b));\nCREATE TABLE c (x int, a int);\n" +
				"ALTER TABLE c ADD CONSTRAINT c_fk FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET DEFAULT (a);",
			wantColumns: []string{"a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithOwners([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(database.Constraints, qt.HasLen, 1)
			columns, _, err := pgtable.DeleteColumnsOf(database.Constraints[0].Facets)
			c.Assert(err, qt.IsNil)
			c.Assert(columns, qt.DeepEquals, test.wantColumns)
		})
	}
}

// An inline reference with owned settings is lifted to a constraint, so its
// explicitly declared column selection survives SQL conversion.
func TestRead_ColumnLevelDeleteColumnListNamingItsColumn(t *testing.T) {
	c := qt.New(t)

	database, _, err := readWithOwners([]byte(
		"CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (a int REFERENCES p (id) ON DELETE SET NULL (a));",
	), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Fields[1].Foreign, qt.Equals, "")
	c.Assert(database.Constraints[0].OnDelete, qt.Equals, "SET NULL")
	columns, _, err := pgtable.DeleteColumnsOf(database.Constraints[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.DeepEquals, []string{"a"})
}

// A column-level list naming any other column is refused, as PostgreSQL
// refuses it: the column is not part of that key.
func TestRead_ColumnLevelDeleteColumnListNamingAnotherColumn(t *testing.T) {
	c := qt.New(t)

	database, _, err := readWithOwners([]byte(
		"CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (a int REFERENCES p (id) ON DELETE SET NULL (b), b int);",
	), "postgres")

	c.Assert(err, qt.ErrorMatches,
		`.*ON DELETE SET NULL names column "b", which is not part of the foreign key`)
	c.Assert(database.Tables, qt.HasLen, 0)
}

// TestRead_AddedInlineDeleteSelectionPreservesIdentity covers the ADD COLUMN
// path and identifiers whose punctuation is part of their logical name.
func TestRead_AddedInlineDeleteSelectionPreservesIdentity(t *testing.T) {
	c := qt.New(t)
	database, _, err := readWithOwners([]byte(`CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int);
ALTER TABLE c ADD COLUMN "Parent,ID" int CONSTRAINT "Parent.Key" REFERENCES p (id) ON DELETE SET NULL ("Parent,ID");`), "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Constraints, qt.HasLen, 1)
	key := database.Constraints[0]
	c.Assert(key.Name, qt.Equals, "Parent.Key")
	c.Assert(key.Columns, qt.DeepEquals, []string{"Parent,ID"})
	columns, _, err := pgtable.DeleteColumnsOf(key.Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.DeepEquals, []string{"Parent,ID"})
	c.Assert(database.Fields[2].Foreign, qt.Equals, "")
}
