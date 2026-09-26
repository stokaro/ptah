package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// The MySQL family allows an identifier between FOREIGN KEY and the column
// list, and it names the BACKING INDEX rather than the constraint symbol.
// Measured 2026-09-03 against `CREATE TABLE parents (id INT PRIMARY KEY)`,
// reading information_schema.referential_constraints and
// information_schema.statistics back:
//
//	body                                             MySQL 8.4.11            MariaDB 11.8.9
//	FOREIGN KEY zidx (a) REFERENCES parents(id)      child_ibfk_1 / zidx     zidx / zidx
//	KEY zidx (a), FOREIGN KEY (a) REFERENCES ...     child_ibfk_1 / zidx     child_ibfk_1 / zidx
//	CONSTRAINT zsym FOREIGN KEY zidx (a) REF ...     zsym / zsym             zsym / zsym
//	FOREIGN KEY (a) REFERENCES parents(id)           child_ibfk_1 / a        child_ibfk_1 / a
//
// Rows one and two are the same catalog on MySQL, which is why the name is
// read there as the index it builds rather than into a field beside the
// constraint. On MariaDB row one is the catalog `CONSTRAINT zidx FOREIGN KEY
// (a)` builds, so the name is read there as the key's symbol
// (stokaro/ptah#3743). Row three is why a name written beside a symbol is
// discarded: both engines record the symbol for the index too. See
// stokaro/ptah#2789.

const foreignKeyIndexParents = "CREATE TABLE parents (id INT PRIMARY KEY);\n"

func parsedChildTable(c *qt.C, sql, dialect string) *ast.CreateTableNode {
	c.Helper()
	result, err := parser.NewParser(foreignKeyIndexParents+sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(result.Statements, qt.HasLen, 2)
	table, ok := result.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	return table
}

// On MySQL the name declares the index the server builds for an unnamed key.
func TestParseForeignKeyIndexName_MySQLHappyPath(t *testing.T) {
	for _, body := range []string{
		"FOREIGN KEY zidxonly27 (a) REFERENCES parents(id)",
		"CONSTRAINT FOREIGN KEY zidxonly27 (a) REFERENCES parents(id)",
	} {
		t.Run(body, func(t *testing.T) {
			c := qt.New(t)

			table := parsedChildTable(c, "CREATE TABLE child (a INT, "+body+");", platform.MySQL)

			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Name, qt.Equals, "zidxonly27")
			c.Assert(table.Indexes[0].Columns, qt.DeepEquals, []string{"a"})
			c.Assert(table.Indexes[0].Unique, qt.IsFalse)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].Type, qt.Equals, ast.ForeignKeyConstraint)
			c.Assert(table.Constraints[0].Name, qt.Equals, "")
			c.Assert(table.Constraints[0].Reference, qt.IsNotNil)
			c.Assert(table.Constraints[0].Reference.Table, qt.Equals, "parents")
		})
	}
}

// On MariaDB the name is the key's, and the key's index takes it as the index
// of any named key does, so no index of its own is declared.
func TestParseForeignKeyIndexName_MariaDBHappyPath(t *testing.T) {
	for _, body := range []string{
		"FOREIGN KEY zidxonly27 (a) REFERENCES parents(id)",
		"CONSTRAINT FOREIGN KEY zidxonly27 (a) REFERENCES parents(id)",
	} {
		t.Run(body, func(t *testing.T) {
			c := qt.New(t)

			table := parsedChildTable(c, "CREATE TABLE child (a INT, "+body+");", platform.MariaDB)

			c.Assert(table.Indexes, qt.HasLen, 0)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].Type, qt.Equals, ast.ForeignKeyConstraint)
			c.Assert(table.Constraints[0].Name, qt.Equals, "zidxonly27")
			c.Assert(table.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
			c.Assert(table.Constraints[0].Reference, qt.IsNotNil)
			c.Assert(table.Constraints[0].Reference.Table, qt.Equals, "parents")
		})
	}
}

// parsedAlterOperations is the operations of the ALTER TABLE sql holds, after
// the parents and a child table the fixture declares.
func parsedAlterOperations(c *qt.C, sql, dialect string) []ast.AlterOperation {
	c.Helper()
	result, err := parser.NewParser(
		foreignKeyIndexParents+"CREATE TABLE child (a INT);\n"+sql, parser.WithDialect(dialect),
	).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(result.Statements, qt.HasLen, 3)
	alter, ok := result.Statements[2].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	return alter.Operations
}

// The same clause in ALTER TABLE ... ADD declares the same objects. On MySQL
// it is the index and the unnamed key, measured on 8.4.11 and 26.7.0 as key
// `child_ibfk_1` over index `zidx`; the clause answered with the index alone
// loses the key.
func TestParseAlterAddForeignKeyIndexName_MySQL(t *testing.T) {
	c := qt.New(t)

	operations := parsedAlterOperations(c,
		"ALTER TABLE child ADD FOREIGN KEY zidx (a) REFERENCES parents(id);", platform.MySQL)

	c.Assert(operations, qt.HasLen, 2)
	index, ok := operations[0].(*ast.AddIndexOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Index.Name, qt.Equals, "zidx")
	c.Assert(index.Index.Columns, qt.DeepEquals, []string{"a"})
	key, ok := operations[1].(*ast.AddConstraintOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(key.Constraint.Type, qt.Equals, ast.ForeignKeyConstraint)
	c.Assert(key.Constraint.Name, qt.Equals, "")
	c.Assert(key.Constraint.Reference.Table, qt.Equals, "parents")
}

// On MariaDB the clause adds the key under the name, measured on 11.8.9 as key
// `zidx` over index `zidx`.
func TestParseAlterAddForeignKeyIndexName_MariaDB(t *testing.T) {
	c := qt.New(t)

	operations := parsedAlterOperations(c,
		"ALTER TABLE child ADD FOREIGN KEY zidx (a) REFERENCES parents(id);", platform.MariaDB)

	c.Assert(operations, qt.HasLen, 1)
	key, ok := operations[0].(*ast.AddConstraintOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(key.Constraint.Type, qt.Equals, ast.ForeignKeyConstraint)
	c.Assert(key.Constraint.Name, qt.Equals, "zidx")
	c.Assert(key.Constraint.Reference.Table, qt.Equals, "parents")
}

// A name written beside an explicit symbol declares nothing, because both
// engines record the symbol for the index as well.
func TestParseForeignKeyIndexName_NameBesideASymbolIsDiscarded(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			table := parsedChildTable(c,
				"CREATE TABLE child (a INT, CONSTRAINT zsym FOREIGN KEY zidx (a) REFERENCES parents(id));",
				dialect)

			c.Assert(table.Indexes, qt.HasLen, 0)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].Type, qt.Equals, ast.ForeignKeyConstraint)
			c.Assert(table.Constraints[0].Name, qt.Equals, "zsym")
		})
	}
}

// The control that keeps the reading off a foreign key that names no index:
// without it, a change that invented an index for every foreign key would
// still pass the rows above.
func TestParseForeignKeyWithoutIndexName_BuildsNoIndex(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			table := parsedChildTable(c,
				"CREATE TABLE child (a INT, FOREIGN KEY (a) REFERENCES parents(id));",
				dialect)

			c.Assert(table.Indexes, qt.HasLen, 0)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].Type, qt.Equals, ast.ForeignKeyConstraint)
		})
	}
}

// PostgreSQL and a parse with no dialect keep refusing the spelling, which is
// what both did before it was read at all. The syntax renders on the MySQL
// family alone, and a dialect-neutral document is one meant to render
// anywhere.
func TestParseForeignKeyIndexName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "no dialect", dialect: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				foreignKeyIndexParents+
					"CREATE TABLE child (a INT, FOREIGN KEY zidxonly27 (a) REFERENCES parents(id));",
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(result, qt.IsNil)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "zidxonly27")
		})
	}
}
