package parser_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/parser"
)

// A column-level REFERENCES clause means different things on the engines of
// the MySQL family, and on MySQL it depends on the line. Measured against
// `CREATE TABLE parents (id INT PRIMARY KEY)`:
//
//	MySQL 8.4.11          a INT REFERENCES parents(id)               accepted; no key, no
//	                                                                 constraint
//	MySQL 9.7.2, 26.7.0   a INT REFERENCES parents(id)               enforced: KEY `a`,
//	                                                                 CONSTRAINT `child_ibfk_1`
//	MySQL, every line     a INT CONSTRAINT f REFERENCES parents(id)  error 1064 (42000)
//	MariaDB 11.8.9        a INT REFERENCES parents(id)               enforced: KEY `a`,
//	                                                                 CONSTRAINT `child_ibfk_1`
//	MariaDB 11.8.9        a INT CONSTRAINT f REFERENCES parents(id)  enforced: KEY `f`,
//	                                                                 CONSTRAINT `f`
//
// The reader does not know the MySQL line, so the clause is refused for MySQL
// and read for MariaDB. See stokaro/ptah#2791 and stokaro/ptah#3760. The named
// spelling MySQL answers with error 1064 is refused with the other named
// column constraints the engine refuses, in
// named_column_constraint_dialect_test.go.

const columnReferencesParents = "CREATE TABLE parents (id INT PRIMARY KEY);\n"

// TestParseColumnReferences_MySQLFailurePath refuses the bare clause, which
// MySQL 8.4.11 builds nothing from and MySQL 9.7.2 and 26.7.0 build a foreign
// key from, in CREATE TABLE and in ADD COLUMN alike. The message says what each
// line does, because the reader cannot tell which one the file is for.
func TestParseColumnReferences_MySQLFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name: "CREATE TABLE",
			sql:  "CREATE TABLE child (a INT REFERENCES parents(id));",
			wantErr: "a column-level REFERENCES clause at position 69: MySQL 8.4 builds nothing from the " +
				"clause, while MySQL 9.7 and 26.7 build a foreign key and its index, and the SQL file " +
				"is read without the server version, so Ptah refuses the clause rather than guess which " +
				"schema it declares; write a table-level FOREIGN KEY clause, which every MySQL line builds",
		},
		{
			name: "ADD COLUMN",
			sql:  "CREATE TABLE child (id INT);\nALTER TABLE child ADD COLUMN a INT REFERENCES parents(id);",
			wantErr: "a column-level REFERENCES clause at position 107: MySQL 8.4 builds nothing from the " +
				"clause, while MySQL 9.7 and 26.7 build a foreign key and its index, and the SQL file " +
				"is read without the server version, so Ptah refuses the clause rather than guess which " +
				"schema it declares; write a table-level FOREIGN KEY clause, which every MySQL line builds",
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
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Dialect, qt.Equals, platform.MySQL)
			c.Assert(capabilityErr.Feature, qt.Equals, "column-level REFERENCES without a server version")
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
