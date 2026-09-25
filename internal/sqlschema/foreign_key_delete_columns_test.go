package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
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

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(database.Constraints, qt.HasLen, 1)
			c.Assert(database.Constraints[0].OnDeleteColumns, qt.DeepEquals, test.wantColumns)
		})
	}
}

// A column-level REFERENCES whose list names the column itself describes the
// whole key, which the field already says; the list adds nothing to keep.
func TestRead_ColumnLevelDeleteColumnListNamingItsColumn(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (a int REFERENCES p (id) ON DELETE SET NULL (a));",
	), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Constraints, qt.HasLen, 0)
	c.Assert(database.Fields[1].Foreign, qt.Equals, "p(id)")
	c.Assert(database.Fields[1].OnDelete, qt.Equals, "SET NULL")
}

// A column-level list naming any other column is refused, as PostgreSQL
// refuses it: the column is not part of that key.
func TestRead_ColumnLevelDeleteColumnListNamingAnotherColumn(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (a int REFERENCES p (id) ON DELETE SET NULL (b), b int);",
	), "postgres")

	c.Assert(err, qt.ErrorMatches,
		`column a: ON DELETE SET NULL names column b, which is not part of the foreign key; `+
			`a column-level REFERENCES covers only its own column`)
	c.Assert(database.Tables, qt.HasLen, 0)
}
