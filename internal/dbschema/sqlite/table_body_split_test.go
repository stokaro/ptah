package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/sqlite"
)

// TestReader_KeepsTheConstraintsBesideAQuotedOrCommentedColumn reads a table
// whose body holds a column the comma and parenthesis splitter has to skip
// whole: a name with a parenthesis or a comma in it, or a comment with an
// apostrophe. A splitter that reads into the name or the comment cuts the
// table body at the wrong place, and the CHECK and the generated expression
// after it are lost.
func TestReader_KeepsTheConstraintsBesideAQuotedOrCommentedColumn(t *testing.T) {
	tests := []struct {
		name     string
		neighbor string
	}{
		{name: "a backquoted name with a parenthesis", neighbor: "`bt(` TEXT,"},
		{name: "a quoted name with a comma", neighbor: `"code,part" TEXT,`},
		{name: "a bracketed name with a parenthesis", neighbor: "[br)] TEXT,"},
		{name: "a line comment with an apostrophe", neighbor: "-- it's a note\n\t\tnote TEXT,"},
		{name: "a block comment with a parenthesis", neighbor: "/* ( */ note TEXT,"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMemoryDB(t)
			execSQL(t, db, `CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		`+test.neighbor+`
		total INTEGER,
		doubled INTEGER GENERATED ALWAYS AS (total * 2) STORED,
		CONSTRAINT t_total_positive CHECK (total > 0)
	)`)

			schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			check := findConstraint(schema.Constraints, "t_total_positive")
			c.Assert(check, qt.IsNotNil)
			c.Assert(check.CheckClause, qt.DeepEquals, new("total > 0"))
			c.Assert(schema.Tables, qt.HasLen, 1)
			generated := make(map[string]*string)
			for _, column := range schema.Tables[0].Columns {
				generated[column.Name] = column.GeneratedExpression
			}
			c.Assert(generated["doubled"], qt.DeepEquals, new("total * 2"))
		})
	}
}
