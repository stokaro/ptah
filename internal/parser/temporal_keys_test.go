package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// temporalConstraints parses sql as PostgreSQL and answers the constraints of
// its last CREATE TABLE.
func temporalConstraints(c *qt.C, sql string) []*ast.ConstraintNode {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect("postgres")).Parse()
	c.Assert(err, qt.IsNil)
	table, ok := statements.Statements[len(statements.Statements)-1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	return table.Constraints
}

// TestParse_TemporalKeys_HappyPath reads WITHOUT OVERLAPS and PERIOD onto the
// constraint they follow. Each row's SQL was created by PostgreSQL 18.6, which
// recorded pg_constraint.conperiod for it (stokaro/ptah#6330).
func TestParse_TemporalKeys_HappyPath(t *testing.T) {
	t.Run("a primary key", func(t *testing.T) {
		c := qt.New(t)

		keys := temporalConstraints(c, `CREATE TABLE p (id int, valid_at daterange,
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))`)

		c.Assert(keys, qt.HasLen, 1)
		c.Assert(keys[0].Type, qt.Equals, ast.PrimaryKeyConstraint)
		c.Assert(keys[0].Columns, qt.DeepEquals, []string{"id", "valid_at"})
		c.Assert(keys[0].WithoutOverlaps, qt.IsTrue)
	})
	t.Run("a named unique constraint with an INCLUDE payload", func(t *testing.T) {
		c := qt.New(t)

		keys := temporalConstraints(c, `CREATE TABLE u (id int, k int, valid_at tstzrange, note text,
  CONSTRAINT u_uq UNIQUE (id, k, valid_at WITHOUT OVERLAPS) INCLUDE (note))`)

		c.Assert(keys, qt.HasLen, 1)
		c.Assert(keys[0].Name, qt.Equals, "u_uq")
		c.Assert(keys[0].Columns, qt.DeepEquals, []string{"id", "k", "valid_at"})
		c.Assert(keys[0].IncludeColumns, qt.DeepEquals, []string{"note"})
		c.Assert(keys[0].WithoutOverlaps, qt.IsTrue)
	})
	t.Run("a foreign key", func(t *testing.T) {
		c := qt.New(t)

		keys := temporalConstraints(c, `CREATE TABLE c (pid int, valid_at daterange,
  CONSTRAINT c_fk FOREIGN KEY (pid, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at))`)

		c.Assert(keys, qt.HasLen, 1)
		c.Assert(keys[0].Columns, qt.DeepEquals, []string{"pid", "valid_at"})
		c.Assert(keys[0].Reference.ReferencedColumns(), qt.DeepEquals, []string{"id", "valid_at"})
		c.Assert(keys[0].Reference.Period, qt.IsTrue)
	})
	t.Run("a key added by ALTER TABLE", func(t *testing.T) {
		c := qt.New(t)

		statements, err := parser.NewParser(`ALTER TABLE p ADD CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);
ALTER TABLE c ADD CONSTRAINT c_fk FOREIGN KEY (pid, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at);`,
			parser.WithDialect("postgres")).Parse()

		c.Assert(err, qt.IsNil)
		c.Assert(statements.Statements, qt.HasLen, 2)
		primary := statements.Statements[0].(*ast.AlterTableNode).Operations[0].(*ast.AddConstraintOperation).Constraint
		foreign := statements.Statements[1].(*ast.AlterTableNode).Operations[0].(*ast.AddConstraintOperation).Constraint
		c.Assert(primary.WithoutOverlaps, qt.IsTrue)
		c.Assert(foreign.Reference.Period, qt.IsTrue)
	})
	t.Run("a plain key stays plain", func(t *testing.T) {
		c := qt.New(t)

		keys := temporalConstraints(c, `CREATE TABLE p (id int, valid_at daterange, PRIMARY KEY (id, valid_at))`)

		c.Assert(keys[0].WithoutOverlaps, qt.IsFalse)
	})
	t.Run("a column named period is a column", func(t *testing.T) {
		c := qt.New(t)

		keys := temporalConstraints(c, `CREATE TABLE c (a int, period int,
  FOREIGN KEY (a, period) REFERENCES p (a, period))`)

		c.Assert(keys[0].Columns, qt.DeepEquals, []string{"a", "period"})
		c.Assert(keys[0].Reference.Period, qt.IsFalse)
	})
}

// TestParse_TemporalKeys_FailurePath refuses the spellings PostgreSQL 18.6
// refuses, and a PERIOD written on one side only.
func TestParse_TemporalKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "WITHOUT OVERLAPS before the last column",
			sql:  `CREATE TABLE p (a int, b int, PRIMARY KEY (a WITHOUT OVERLAPS, b))`,
			want: `(?s).*WITHOUT OVERLAPS applies to the last column of a key`,
		},
		{
			name: "PERIOD before the first of two columns",
			sql:  `CREATE TABLE c (a int, b int, FOREIGN KEY (PERIOD a, b) REFERENCES p (a, b))`,
			want: `(?s).*PERIOD applies to the last column of a foreign key column list`,
		},
		{
			name: "PERIOD on the referencing side only",
			sql:  `CREATE TABLE c (a int, b int, FOREIGN KEY (a, PERIOD b) REFERENCES p (a, b))`,
			want: `(?s).*PERIOD must be written before the last column of both the referencing and the referenced column list`,
		},
		{
			name: "PERIOD on the referenced side only",
			sql:  `CREATE TABLE c (a int, b int, FOREIGN KEY (a, b) REFERENCES p (a, PERIOD b))`,
			want: `(?s).*PERIOD must be written before the last column of both the referencing and the referenced column list`,
		},
		{
			name: "PERIOD in a column's REFERENCES",
			sql:  `CREATE TABLE c (a int REFERENCES p (a, PERIOD b))`,
			want: `(?s).*PERIOD belongs to a table-level FOREIGN KEY, where both column lists can carry it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
