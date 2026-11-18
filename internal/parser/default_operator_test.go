package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
)

// addedColumn returns the column a single ALTER TABLE ... ADD COLUMN adds, read
// as PostgreSQL.
func addedColumn(c *qt.C, column string) *ast.ColumnNode {
	c.Helper()
	statements := parsePostgres(c, "ALTER TABLE t ADD COLUMN "+column+";")
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[0]))
	c.Assert(alter.Operations, qt.HasLen, 1)
	added, ok := alter.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", alter.Operations[0]))
	return added.Column
}

// defaultOperatorRows are column definitions whose default continues with a
// binary operator after its first operand. Each but the last two ends in a
// column clause, and the want fields after the default say the clause was
// read as the column's rather than as part of the default. The last two keep
// a negative number a value.
var defaultOperatorRows = []struct {
	name, column               string
	wantExpression, wantValue  string
	wantNullable               bool
	wantCollate, wantCheck     string
	wantGenerated              string
	wantUnique, wantReferences bool
}{
	{name: "subtraction", column: "n int DEFAULT 5 - 2", wantExpression: "5 - 2", wantNullable: true},
	{name: "subtraction, then NOT NULL", column: "n int DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
	{name: "a negative number times a number, then NOT NULL", column: "n int DEFAULT -5 * 2 NOT NULL", wantExpression: "-5 * 2"},
	{
		name: "a call minus an interval", column: "e timestamptz DEFAULT now() - interval '1 day'",
		wantExpression: "now() - interval '1 day'", wantNullable: true,
	},
	{
		name: "concatenation, then COLLATE", column: `s text DEFAULT 'a' || 'b' COLLATE "C"`,
		wantExpression: "'a' || 'b'", wantNullable: true, wantCollate: "C",
	},
	{
		name: "modulo, then CHECK", column: "n int DEFAULT 5 % 2 CHECK (n >= 0)",
		wantExpression: "5 % 2", wantNullable: true, wantCheck: "n >= 0",
	},
	{
		name: "division, then REFERENCES", column: "n int DEFAULT 6 / 2 REFERENCES p (id)",
		wantExpression: "6 / 2", wantNullable: true, wantReferences: true,
	},
	// No engine takes a default and a generation expression on one column.
	// The row pins where the default ends, not that the pair is accepted.
	{
		name: "an exponent, then GENERATED", column: "n int DEFAULT 2 ^ 3 GENERATED ALWAYS AS (id) STORED",
		wantExpression: "2 ^ 3", wantNullable: true, wantGenerated: "id",
	},
	{
		name: "a bitwise or, then UNIQUE", column: "n int DEFAULT 1 | 2 UNIQUE",
		wantExpression: "1 | 2", wantNullable: true, wantUnique: true,
	},
	{name: "a negative number", column: "n int DEFAULT -5", wantValue: "-5", wantNullable: true},
	{name: "a negative number, then NOT NULL", column: "n int DEFAULT -5 NOT NULL", wantValue: "-5"},
}

// A default that continues with an operator is read whole, in CREATE TABLE and
// in ALTER TABLE ... ADD COLUMN alike, and ends where the column's next clause
// begins.
func TestParse_DefaultOperators_HappyPath(t *testing.T) {
	for _, row := range defaultOperatorRows {
		t.Run(row.name+"/CREATE TABLE", func(t *testing.T) {
			c := qt.New(t)

			column := declaredColumn(c, row.column)

			c.Assert(column.Default, qt.IsNotNil)
			c.Assert(column.Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(column.Default.Value, qt.Equals, row.wantValue)
			c.Assert(column.Nullable, qt.Equals, row.wantNullable)
			c.Assert(column.Collate, qt.Equals, row.wantCollate)
			c.Assert(column.Check, qt.Equals, row.wantCheck)
			c.Assert(column.GeneratedExpression, qt.Equals, row.wantGenerated)
			c.Assert(column.Unique, qt.Equals, row.wantUnique)
			c.Assert(column.ForeignKey != nil, qt.Equals, row.wantReferences)
		})
		t.Run(row.name+"/ADD COLUMN", func(t *testing.T) {
			c := qt.New(t)

			column := addedColumn(c, row.column)

			c.Assert(column.Default, qt.IsNotNil)
			c.Assert(column.Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(column.Default.Value, qt.Equals, row.wantValue)
			c.Assert(column.Nullable, qt.Equals, row.wantNullable)
			c.Assert(column.Collate, qt.Equals, row.wantCollate)
			c.Assert(column.Check, qt.Equals, row.wantCheck)
			c.Assert(column.GeneratedExpression, qt.Equals, row.wantGenerated)
			c.Assert(column.Unique, qt.Equals, row.wantUnique)
			c.Assert(column.ForeignKey != nil, qt.Equals, row.wantReferences)
		})
	}
}

// A comma outside any group ends an operator default, so the next column
// keeps its own definition.
func TestParse_DefaultOperatorEndsAtAComma_HappyPath(t *testing.T) {
	t.Run("CREATE TABLE", func(t *testing.T) {
		c := qt.New(t)

		statements := parsePostgres(c, "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 5 - 2, s text DEFAULT 'a' || 'b');")

		table, ok := statements.Statements[0].(*ast.CreateTableNode)
		c.Assert(ok, qt.IsTrue)
		c.Assert(table.Columns, qt.HasLen, 3)
		c.Assert(table.Columns[1].Default.Expression, qt.Equals, "5 - 2")
		c.Assert(table.Columns[2].Name, qt.Equals, "s")
		c.Assert(table.Columns[2].Default.Expression, qt.Equals, "'a' || 'b'")
	})
	t.Run("ADD COLUMN", func(t *testing.T) {
		c := qt.New(t)

		statements := parsePostgres(c, "ALTER TABLE t ADD COLUMN n int DEFAULT 5 - 2, ADD COLUMN m int;")

		alter, ok := statements.Statements[0].(*ast.AlterTableNode)
		c.Assert(ok, qt.IsTrue)
		c.Assert(alter.Operations, qt.HasLen, 2)
		added, ok := alter.Operations[0].(*ast.AddColumnOperation)
		c.Assert(ok, qt.IsTrue)
		c.Assert(added.Column.Default.Expression, qt.Equals, "5 - 2")
		next, ok := alter.Operations[1].(*ast.AddColumnOperation)
		c.Assert(ok, qt.IsTrue)
		c.Assert(next.Column.Name, qt.Equals, "m")
		c.Assert(next.Column.Default, qt.IsNil)
	})
}

// ALTER COLUMN ... SET DEFAULT reads the same operator defaults, and the
// default ends at the comma before the next action or at the semicolon.
func TestParse_AlterColumnSetDefaultOperators_HappyPath(t *testing.T) {
	rows := []struct {
		name, sql                 string
		wantActions               []ast.AlterColumnAction
		wantExpression, wantValue string
	}{
		{
			name:           "subtraction, then a comma",
			sql:            "ALTER TABLE t ALTER COLUMN a SET DEFAULT 5 - 2, ALTER COLUMN b DROP NOT NULL;",
			wantActions:    []ast.AlterColumnAction{ast.AlterColumnSetDefault, ast.AlterColumnDropNotNull},
			wantExpression: "5 - 2",
		},
		{
			name:           "concatenation",
			sql:            "ALTER TABLE t ALTER COLUMN a SET DEFAULT 'a' || 'b';",
			wantActions:    []ast.AlterColumnAction{ast.AlterColumnSetDefault},
			wantExpression: "'a' || 'b'",
		},
		{
			name:           "a call minus an interval, then a comma",
			sql:            "ALTER TABLE t ALTER COLUMN a SET DEFAULT now() - interval '1 day', ALTER COLUMN b SET NOT NULL;",
			wantActions:    []ast.AlterColumnAction{ast.AlterColumnSetDefault, ast.AlterColumnSetNotNull},
			wantExpression: "now() - interval '1 day'",
		},
		{
			name:        "a negative number",
			sql:         "ALTER TABLE t ALTER COLUMN a SET DEFAULT -5;",
			wantActions: []ast.AlterColumnAction{ast.AlterColumnSetDefault},
			wantValue:   "-5",
		},
		{
			name:        "a negative number, then a comma",
			sql:         "ALTER TABLE t ALTER COLUMN a SET DEFAULT -5, ALTER COLUMN b DROP DEFAULT;",
			wantActions: []ast.AlterColumnAction{ast.AlterColumnSetDefault, ast.AlterColumnDropDefault},
			wantValue:   "-5",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements := parsePostgres(c, row.sql)

			alter, ok := statements.Statements[0].(*ast.AlterTableNode)
			c.Assert(ok, qt.IsTrue)
			actions := make([]ast.AlterColumnAction, 0, len(alter.Operations))
			for _, operation := range alter.Operations {
				altered, ok := operation.(*ast.AlterColumnOperation)
				c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", operation))
				actions = append(actions, altered.Action)
			}
			c.Assert(actions, qt.DeepEquals, row.wantActions)
			setDefault, ok := alter.Operations[0].(*ast.AlterColumnOperation)
			c.Assert(ok, qt.IsTrue)
			c.Assert(setDefault.Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(setDefault.Default.Value, qt.Equals, row.wantValue)
		})
	}
}

// A second DEFAULT on one column is refused on every dialect, PostgreSQL's
// refusal and SQLite's last-one-wins alike, and MySQL's column visibility is
// refused where it ends an operator default as it is after a plain one.
func TestParse_DefaultOperators_FailurePath(t *testing.T) {
	rows := []struct {
		name, dialect, sql, wantErr string
	}{
		{
			name:    "a second DEFAULT",
			dialect: "postgres",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 1 DEFAULT 3);",
			wantErr: `line 1, column 53: column n carries a second DEFAULT: a column has one default; remove one of them`,
		},
		{
			name:    "a second DEFAULT after an operator default",
			dialect: "postgres",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 1 + 2 DEFAULT 3);",
			wantErr: `line 1, column 57: column n carries a second DEFAULT: .*`,
		},
		{
			name:    "a second DEFAULT after NOT NULL",
			dialect: "postgres",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT (1) NOT NULL DEFAULT 3);",
			wantErr: `line 1, column 64: column n carries a second DEFAULT: .*`,
		},
		{
			name:    "a second DEFAULT on a column ADD COLUMN adds",
			dialect: "postgres",
			sql:     "ALTER TABLE t ADD COLUMN n int DEFAULT 1 DEFAULT 3;",
			wantErr: `line 1, column 42: column n carries a second DEFAULT: .*`,
		},
		{
			name:    "a second DEFAULT on SQLite",
			dialect: "sqlite",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 1 DEFAULT 3);",
			wantErr: `line 1, column 53: column n carries a second DEFAULT: .*`,
		},
		{
			name:    "INVISIBLE after a plain default",
			dialect: "mysql",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 1 INVISIBLE);",
			wantErr: `line 1, column 53: unsupported column attribute: INVISIBLE`,
		},
		{
			name:    "INVISIBLE after an operator default",
			dialect: "mysql",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 1 + 2 INVISIBLE);",
			wantErr: `line 1, column 57: unsupported column attribute: INVISIBLE`,
		},
		{
			name:    "VISIBLE after an operator default",
			dialect: "mysql",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 1 - 2 VISIBLE);",
			wantErr: `line 1, column 57: unsupported column attribute: VISIBLE`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect(row.dialect), parser.WithSQL(builtintest.Runtime().SQL())).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
