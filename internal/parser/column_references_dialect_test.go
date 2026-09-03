package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/parser"
)

// A column-level REFERENCES clause means different things on the two engines
// of the MySQL family, and reading it as one thing made Ptah invent a
// constraint. Measured 2026-09-03 against `CREATE TABLE parents (id INT
// PRIMARY KEY)`:
//
//	MySQL 8.4.11    a INT REFERENCES parents(id)               accepted; SHOW CREATE TABLE
//	                                                           reports the column alone, no
//	                                                           key, no constraint
//	MySQL 8.4.11    a INT CONSTRAINT f REFERENCES parents(id)  error 1064 (42000)
//	MariaDB 11.8.9  a INT REFERENCES parents(id)               enforced: KEY `a`,
//	                                                           CONSTRAINT `child_ibfk_1`
//	MariaDB 11.8.9  a INT CONSTRAINT f REFERENCES parents(id)  enforced: KEY `f`,
//	                                                           CONSTRAINT `f`
//
// So the clause is refused for MySQL and read for MariaDB. See
// stokaro/ptah#2791.

const columnReferencesParents = "CREATE TABLE parents (id INT PRIMARY KEY);\n"

func TestParseColumnReferences_MySQLFailurePath(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantAnswer string
	}{
		{
			name:       "bare clause",
			sql:        "CREATE TABLE child (a INT REFERENCES parents(id));",
			wantAnswer: "MySQL accepts the clause and creates neither a foreign key nor an index",
		},
		{
			name:       "named clause",
			sql:        "CREATE TABLE child (a INT CONSTRAINT f REFERENCES parents(id));",
			wantAnswer: "MySQL refuses that spelling outright with error 1064 (42000)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				columnReferencesParents+test.sql,
				parser.WithDialect(platform.MySQL),
			).Parse()

			c.Assert(result, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, test.wantAnswer)
			c.Assert(err.Error(), qt.Contains, "write a table-level FOREIGN KEY clause")
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Dialect, qt.Equals, platform.MySQL)
			c.Assert(capabilityErr.Feature, qt.Equals, "enforced column-level REFERENCES")
		})
	}
}

// MariaDB is the control that keeps the refusal scoped to the engine that
// ignores the clause: both spellings still parse, and the reference still
// reaches the model with the name the author wrote.
func TestParseColumnReferences_MariaDBHappyPath(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
	}{
		{
			name:     "bare clause",
			sql:      "CREATE TABLE child (a INT REFERENCES parents(id));",
			wantName: "",
		},
		{
			name:     "named clause",
			sql:      "CREATE TABLE child (a INT CONSTRAINT f REFERENCES parents(id));",
			wantName: "f",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				columnReferencesParents+test.sql,
				parser.WithDialect(platform.MariaDB),
			).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 2)
			table, ok := result.Statements[1].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 1)
			c.Assert(table.Columns[0].ForeignKey, qt.IsNotNil)
			c.Assert(table.Columns[0].ForeignKey.Table, qt.Equals, "parents")
			c.Assert(table.Columns[0].ForeignKey.Column, qt.Equals, "id")
			c.Assert(table.Columns[0].ForeignKey.Name, qt.Equals, test.wantName)
		})
	}
}

// A parse with no dialect keeps reading the clause. The syntax is not one
// engine's alone, so a dialect-neutral document carrying it is not yet wrong;
// the refusal belongs to the target that would be misread.
func TestParseColumnReferences_NoDialectHappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := parser.NewParser(
		columnReferencesParents + "CREATE TABLE child (a INT REFERENCES parents(id));",
	).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(result.Statements, qt.HasLen, 2)
	table, ok := result.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Columns[0].ForeignKey, qt.IsNotNil)
	c.Assert(table.Columns[0].ForeignKey.Table, qt.Equals, "parents")
}

// The control that keeps the refusal off the spelling its own message
// advises, and off foreign keys as such. Without it, removing MySQL foreign
// keys outright would read as a fix for this issue.
func TestParseTableLevelForeignKey_MySQLHappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := parser.NewParser(
		columnReferencesParents+
			"CREATE TABLE child (a INT, FOREIGN KEY (a) REFERENCES parents(id));",
		parser.WithDialect(platform.MySQL),
	).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(result.Statements, qt.HasLen, 2)
	table, ok := result.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Constraints, qt.HasLen, 1)
	c.Assert(table.Constraints[0].Type, qt.Equals, ast.ForeignKeyConstraint)
	c.Assert(table.Constraints[0].Reference, qt.IsNotNil)
	c.Assert(table.Constraints[0].Reference.Table, qt.Equals, "parents")
}
