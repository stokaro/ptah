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

// thirdColumnTable parses `CREATE TABLE t (id int, <column>, m int);` as
// dialect and returns the table, after asserting that m was read as the third
// column: whatever the default of column holds, it ends before the comma.
func thirdColumnTable(c *qt.C, dialect, column string) *ast.CreateTableNode {
	c.Helper()
	statements, err := parser.NewParser(
		"CREATE TABLE t (id int, "+column+", m int);",
		parser.WithDialect(dialect),
		parser.WithSQL(builtintest.Runtime().SQL()),
	).Parse()
	c.Assert(err, qt.IsNil)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[0]))
	c.Assert(table.Columns, qt.HasLen, 3)
	c.Assert(table.Columns[2].Name, qt.Equals, "m")
	return table
}

// parseAs parses sql as dialect.
func parseAs(dialect, sql string) (*ast.StatementList, error) {
	return parser.NewParser(sql, parser.WithDialect(dialect), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
}

// A comment inside an operator default separates tokens as whitespace does
// and is not kept: kept, a `--` comment would swallow the comma a renderer
// writes after the default. A run of whitespace and comments reads as one
// space, so a default written over two lines is the same expression.
func TestParse_DefaultOperatorComment_HappyPath(t *testing.T) {
	rows := []struct {
		name, column   string
		wantExpression string
		wantNullable   bool
	}{
		{
			name: "a line comment before NOT NULL", column: "c int DEFAULT 5 - 2 -- the answer\n    NOT NULL",
			wantExpression: "5 - 2",
		},
		{
			name: "a block comment inside the expression", column: "c int DEFAULT 5 /* five */ - /* two */ 2",
			wantExpression: "5 - 2", wantNullable: true,
		},
		{
			name: "a block comment before the comma", column: "c int DEFAULT 5 - 2 /* the answer */",
			wantExpression: "5 - 2", wantNullable: true,
		},
		{
			name: "a line comment before the comma", column: "c int DEFAULT 5 - 2 -- the answer\n",
			wantExpression: "5 - 2", wantNullable: true,
		},
		{
			name: "a line comment after the operator", column: "c int DEFAULT 5 - -- the answer\n 2 NOT NULL",
			wantExpression: "5 - 2",
		},
		{
			name: "a default over two lines", column: "c int DEFAULT 1 +\n 2",
			wantExpression: "1 + 2", wantNullable: true,
		},
		{
			name: "a default over two lines, then NOT NULL", column: "c int DEFAULT 1\n  + 2\n  NOT NULL",
			wantExpression: "1 + 2",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			table := thirdColumnTable(c, "postgres", row.column)

			c.Assert(table.Columns[1].Default, qt.IsNotNil)
			c.Assert(table.Columns[1].Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(table.Columns[1].Nullable, qt.Equals, row.wantNullable)
		})
	}
}

// An engine that takes an operator default without parentheses reads one up
// to the column's next clause, and so does a document read with no dialect.
// MySQL, MariaDB and SQLite take the parenthesized form, which every engine
// reads.
func TestParse_DefaultOperatorPerDialect_HappyPath(t *testing.T) {
	rows := []struct {
		name, dialect, column string
		wantExpression        string
	}{
		{name: "PostgreSQL", dialect: "postgres", column: "n int DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
		{name: "CockroachDB", dialect: "cockroachdb", column: "n int DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
		{name: "YugabyteDB", dialect: "yugabytedb", column: "n int DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
		{name: "SQL Server", dialect: "sqlserver", column: "n int DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
		{name: "Oracle", dialect: "oracle", column: "n int DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
		{name: "ClickHouse", dialect: "clickhouse", column: "n Int32 DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
		{name: "no dialect", dialect: "", column: "n int DEFAULT 5 - 2 NOT NULL", wantExpression: "5 - 2"},
		{name: "MySQL, in parentheses", dialect: "mysql", column: "n int DEFAULT (5 - 2) NOT NULL", wantExpression: "(5 - 2)"},
		{name: "MariaDB, in parentheses", dialect: "mariadb", column: "n int DEFAULT (5 - 2) NOT NULL", wantExpression: "(5 - 2)"},
		{name: "SQLite, in parentheses", dialect: "sqlite", column: "n int DEFAULT (5 - 2) NOT NULL", wantExpression: "(5 - 2)"},
		{
			name: "MySQL, a bitwise or in parentheses", dialect: "mysql",
			column: "n int DEFAULT (1 | 2) NOT NULL", wantExpression: "(1 | 2)",
		},
		{
			name: "MySQL, pipes in a string", dialect: "mysql",
			column: "n varchar(4) DEFAULT ('a||b') NOT NULL", wantExpression: "('a||b')",
		},
		{
			name: "SQLite, a concatenation in parentheses", dialect: "sqlite",
			column: "n text DEFAULT ('a' || 'b') NOT NULL", wantExpression: "('a' || 'b')",
		},
		{
			name: "MySQL, a gate after the parentheses", dialect: "mysql",
			column: "n int DEFAULT (5 - 2) /*!80023 INVISIBLE */ NOT NULL", wantExpression: "(5 - 2)",
		},
		{
			name: "MariaDB, a gate after the parentheses", dialect: "mariadb",
			column: "n int DEFAULT (5 - 2) /*M!100301 INVISIBLE */ NOT NULL", wantExpression: "(5 - 2)",
		},
		// PostgreSQL reads a version gate as a comment, so it ends nothing:
		// the NOT NULL after it is still the column's.
		{
			name: "PostgreSQL, a MySQL gate read as a comment", dialect: "postgres",
			column: "n int DEFAULT 5 - 2 /*!80023 INVISIBLE */ NOT NULL", wantExpression: "5 - 2",
		},
		{
			name: "PostgreSQL, a MySQL gate between operands", dialect: "postgres",
			column: "n int DEFAULT 1 + 2 /*!80023 x */ + 3 NOT NULL", wantExpression: "1 + 2 + 3",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			table := thirdColumnTable(c, row.dialect, row.column)

			c.Assert(table.Columns[1].Default, qt.IsNotNil)
			c.Assert(table.Columns[1].Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(table.Columns[1].Nullable, qt.IsFalse)
		})
	}
}

// MySQL, MariaDB and SQLite take an expression default only in parentheses,
// and answer a syntax error to an operator outside them, `+` included. The
// default is refused at its first operator, and the refusal writes the
// parenthesized form from the whole expression, without the clause or the
// version gate after it.
func TestParse_DefaultOperatorOutsideParentheses_FailurePath(t *testing.T) {
	const mysql = `MySQL 26\.10 takes an expression default only in parentheses and answers ERROR 1064 \(42000\) to an operator outside them`
	const mariadb = `MariaDB 12\.3 takes an expression default only in parentheses and answers ERROR 1064 \(42000\) to an operator outside them`
	const sqlite = `SQLite 3\.54 takes an expression default only in parentheses and answers a syntax error at an operator outside them`
	rows := []struct {
		name, dialect, sql, wantErr string
	}{
		{
			name: "MySQL, an addition", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 + 2);",
			wantErr: `line 1, column 41: expected default value: 1 \+ 2 is an operator expression outside parentheses: write DEFAULT \(1 \+ 2\); ` + mysql,
		},
		{
			name: "MySQL, a subtraction, then NOT NULL", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 5 - 2 NOT NULL);",
			wantErr: `line 1, column 41: expected default value: 5 - 2 is an operator expression outside parentheses: write DEFAULT \(5 - 2\); ` + mysql,
		},
		{
			name: "MySQL, a bitwise or", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 | 2);",
			wantErr: `line 1, column 41: expected default value: 1 \| 2 is an operator expression outside parentheses: write DEFAULT \(1 \| 2\); ` + mysql,
		},
		{
			name: "MySQL, an operator after a parenthesized operand", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT (1) + 2);",
			wantErr: `line 1, column 43: expected default value: \(1\) \+ 2 is an operator expression outside parentheses: write DEFAULT \(1 \+ 2\); ` + mysql,
		},
		{
			name: "MySQL, an operator after a parenthesized string", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT ( 'a' ) + 2);",
			wantErr: `line 1, column 47: expected default value: \( 'a' \) \+ 2 is an operator expression outside parentheses: write DEFAULT \('a' \+ 2\); ` + mysql,
		},
		{
			name: "MySQL, an operator after a parenthesized operation", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT (1 - 2) * 3);",
			wantErr: `line 1, column 47: expected default value: \(1 - 2\) \* 3 is an operator expression outside parentheses: write DEFAULT \(\(1 - 2\) \* 3\); ` + mysql,
		},
		{
			name: "MySQL, a gate after the expression", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 - 2 /*!80023 INVISIBLE */);",
			wantErr: `line 1, column 41: expected default value: 1 - 2 is an operator expression outside parentheses: write DEFAULT \(1 - 2\); ` + mysql,
		},
		{
			name: "MySQL, MariaDB's gate after the expression, read as a comment", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 - 2 /*M!100301 INVISIBLE */);",
			wantErr: `line 1, column 41: expected default value: 1 - 2 is an operator expression outside parentheses: write DEFAULT \(1 - 2\); ` + mysql,
		},
		{
			name: "MySQL, INVISIBLE after the expression", dialect: "mysql",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 + 2 INVISIBLE);",
			wantErr: `line 1, column 41: expected default value: 1 \+ 2 is an operator expression outside parentheses: write DEFAULT \(1 \+ 2\); ` + mysql,
		},
		{
			name: "MySQL, SET DEFAULT", dialect: "mysql",
			sql:     "ALTER TABLE t ALTER COLUMN n SET DEFAULT 1 + 2;",
			wantErr: `line 1, column 44: expected default value after ALTER COLUMN n SET DEFAULT: 1 \+ 2 is an operator expression outside parentheses: write DEFAULT \(1 \+ 2\); ` + mysql,
		},
		{
			name: "MariaDB, a subtraction", dialect: "mariadb",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 5 - 2 NOT NULL);",
			wantErr: `line 1, column 41: expected default value: 5 - 2 is an operator expression outside parentheses: write DEFAULT \(5 - 2\); ` + mariadb,
		},
		{
			name: "MariaDB, a gate after the expression", dialect: "mariadb",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 + 2 /*M!100301 INVISIBLE */);",
			wantErr: `line 1, column 41: expected default value: 1 \+ 2 is an operator expression outside parentheses: write DEFAULT \(1 \+ 2\); ` + mariadb,
		},
		{
			name: "SQLite, a subtraction", dialect: "sqlite",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 5 - 2);",
			wantErr: `line 1, column 41: expected default value: 5 - 2 is an operator expression outside parentheses: write DEFAULT \(5 - 2\); ` + sqlite,
		},
		{
			name: "SQLite, an addition, then NOT NULL", dialect: "sqlite",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 + 2 NOT NULL);",
			wantErr: `line 1, column 41: expected default value: 1 \+ 2 is an operator expression outside parentheses: write DEFAULT \(1 \+ 2\); ` + sqlite,
		},
		{
			name: "SQLite, a concatenation", dialect: "sqlite",
			sql:     "CREATE TABLE t (id int, s text DEFAULT 'a' || 'b');",
			wantErr: `line 1, column 44: expected default value: 'a' \|\| 'b' is an operator expression outside parentheses: write DEFAULT \('a' \|\| 'b'\); ` + sqlite,
		},
		{
			name: "SQLite, a default over two lines", dialect: "sqlite",
			sql:     "CREATE TABLE t (id int, n int DEFAULT 1 +\n  2);",
			wantErr: `line 1, column 41: expected default value: 1 \+ 2 is an operator expression outside parentheses: write DEFAULT \(1 \+ 2\); ` + sqlite,
		},
		{
			name: "SQLite, ADD COLUMN", dialect: "sqlite",
			sql:     "ALTER TABLE t ADD COLUMN n int DEFAULT 1 - 2;",
			wantErr: `line 1, column 42: expected default value: 1 - 2 is an operator expression outside parentheses: write DEFAULT \(1 - 2\); ` + sqlite,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseAs(row.dialect, row.sql)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// MySQL and MariaDB read || as OR under their default SQL mode and as a
// concatenation under PIPES_AS_CONCAT, so a default that holds it means what
// the session's mode says. MySQL 26.10 stores DEFAULT ('a' || 'b') as
// ((0 <> 'a') or (0 <> 'b')) and refuses the INSERT that takes it; MariaDB
// 12.3 stores ('a' or 'b') and inserts 0. The default is refused at the
// first ||, in parentheses or without, and the refusal names the CONCAT call
// its operands make.
func TestParse_DefaultPipes_FailurePath(t *testing.T) {
	const mysql = `MySQL 26\.10 reads \|\| as OR under its default SQL mode and stores DEFAULT \('a' \|\| 'b'\) as ` +
		`\(\(0 <> 'a'\) or \(0 <> 'b'\)\), so an INSERT that takes the default answers ERROR 1292 \(22007\); ` +
		`under PIPES_AS_CONCAT it is a concatenation`
	const mariadb = `MariaDB 12\.3 reads \|\| as OR under its default SQL mode and stores DEFAULT \('a' \|\| 'b'\) as ` +
		`\('a' or 'b'\), which inserts 0; under PIPES_AS_CONCAT it is a concatenation`
	rows := []struct {
		name, dialect, column, wantErr string
	}{
		{
			name: "MySQL, in parentheses", dialect: "mysql", column: "s varchar(4) DEFAULT ('a' || 'b') NOT NULL",
			wantErr: `line 1, column 51: expected default value: \('a' \|\| 'b'\) holds \|\|: write DEFAULT ` +
				`\(CONCAT\('a', 'b'\)\) for a concatenation, or OR for a logical or; ` + mysql,
		},
		{
			name: "MySQL, outside parentheses", dialect: "mysql", column: "s varchar(4) DEFAULT 'a' || 'b'",
			wantErr: `line 1, column 50: expected default value: 'a' \|\| 'b' holds \|\|: write DEFAULT ` +
				`\(CONCAT\('a', 'b'\)\) for a concatenation, or OR for a logical or; ` + mysql,
		},
		{
			name: "MySQL, three operands and a call", dialect: "mysql",
			column: "s varchar(9) DEFAULT (concat('x', 'y') || 'b'||'c')",
			wantErr: `line 1, column 64: expected default value: \(concat\('x', 'y'\) \|\| 'b'\|\|'c'\) holds \|\|: write ` +
				`DEFAULT \(CONCAT\(concat\('x', 'y'\), 'b', 'c'\)\) for a concatenation, or OR for a logical or; ` + mysql,
		},
		{
			name: "MariaDB, in parentheses", dialect: "mariadb", column: "s varchar(4) DEFAULT ('a' || 'b')",
			wantErr: `line 1, column 51: expected default value: \('a' \|\| 'b'\) holds \|\|: write DEFAULT ` +
				`\(CONCAT\('a', 'b'\)\) for a concatenation, or OR for a logical or; ` + mariadb,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseAs(row.dialect, "CREATE TABLE t (id int, "+row.column+", m int);")

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// After IS, NOT, AND or OR an operand follows, so a NOT or a NULL there is
// the predicate's and not the column's. Oracle and ClickHouse take such a
// predicate in a default, and so does a document read with no dialect; the
// PostgreSQL family takes IS [NOT] DISTINCT FROM.
func TestParse_DefaultOperatorPredicate_HappyPath(t *testing.T) {
	rows := []struct {
		name, dialect, column string
		wantExpression        string
		wantNullable          bool
	}{
		{
			name: "Oracle, IS NOT NULL", dialect: "oracle",
			column: "b boolean DEFAULT 1 + 2 IS NOT NULL", wantExpression: "1 + 2 IS NOT NULL", wantNullable: true,
		},
		{
			name: "Oracle, IS NOT NULL, then NOT NULL", dialect: "oracle",
			column: "b boolean DEFAULT 1 + 2 IS NOT NULL NOT NULL", wantExpression: "1 + 2 IS NOT NULL",
		},
		{
			name: "Oracle, AND NOT", dialect: "oracle",
			column: "b boolean DEFAULT 1 + 2 AND NOT 3", wantExpression: "1 + 2 AND NOT 3", wantNullable: true,
		},
		{
			name: "ClickHouse, OR NULL", dialect: "clickhouse",
			column: "b Bool DEFAULT 1 + 2 OR NULL", wantExpression: "1 + 2 OR NULL",
		},
		{
			name: "no dialect, IS NULL", dialect: "",
			column: "b boolean DEFAULT 1 + 2 IS NULL", wantExpression: "1 + 2 IS NULL", wantNullable: true,
		},
		{
			name: "PostgreSQL, IS DISTINCT FROM, then NOT NULL", dialect: "postgres",
			column: "b boolean DEFAULT 1 + 2 IS DISTINCT FROM 3 NOT NULL", wantExpression: "1 + 2 IS DISTINCT FROM 3",
		},
		{
			name: "CockroachDB, IS NOT DISTINCT FROM", dialect: "cockroachdb",
			column: "b boolean DEFAULT 1 + 2 IS NOT DISTINCT FROM 3", wantExpression: "1 + 2 IS NOT DISTINCT FROM 3", wantNullable: true,
		},
		{
			name: "PostgreSQL, IS NOT NULL in parentheses", dialect: "postgres",
			column: "b boolean DEFAULT (1 + 2 IS NOT NULL) NOT NULL", wantExpression: "(1 + 2 IS NOT NULL)",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			table := thirdColumnTable(c, row.dialect, row.column)

			c.Assert(table.Columns[1].Default, qt.IsNotNil)
			c.Assert(table.Columns[1].Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(table.Columns[1].Nullable, qt.Equals, row.wantNullable)
		})
	}
}

// The PostgreSQL family reads no IS test but IS [NOT] DISTINCT FROM, and no
// AND or OR, in a default without parentheses, and SQL Server reads no
// predicate in a default at all. Each answers a syntax error, and the default
// is refused at the word.
func TestParse_DefaultOperatorPredicate_FailurePath(t *testing.T) {
	const postgres = `PostgreSQL 18 reads no IS test but IS \[NOT\] DISTINCT FROM, and no AND or OR, in a default written ` +
		`without parentheses, and answers a syntax error there; write the whole expression in parentheses, as ` +
		`DEFAULT \(1 \+ 2 IS NOT NULL\)`
	rows := []struct {
		name, dialect, column, wantErr string
	}{
		{
			name: "PostgreSQL, IS NOT NULL", dialect: "postgres", column: "b boolean DEFAULT 1 + 2 IS NOT NULL",
			wantErr: `line 1, column 49: expected default value: 1 \+ 2 continues with IS outside parentheses: ` + postgres,
		},
		{
			name: "PostgreSQL, AND NOT", dialect: "postgres", column: "b boolean DEFAULT 1 + 2 AND NOT 3",
			wantErr: `line 1, column 49: expected default value: 1 \+ 2 continues with AND outside parentheses: ` + postgres,
		},
		{
			name: "PostgreSQL, or", dialect: "postgres", column: "b boolean DEFAULT 1 + 2 or false",
			wantErr: `line 1, column 49: expected default value: 1 \+ 2 continues with OR outside parentheses: ` + postgres,
		},
		{
			name: "CockroachDB, IS NULL", dialect: "cockroachdb", column: "b boolean DEFAULT 1 + 2 IS NULL",
			wantErr: `line 1, column 49: expected default value: 1 \+ 2 continues with IS outside parentheses: CockroachDB v26\.4\.1 .*`,
		},
		{
			name: "YugabyteDB, IS NOT NULL", dialect: "yugabytedb", column: "b boolean DEFAULT 1 + 2 IS NOT NULL",
			wantErr: `line 1, column 49: expected default value: 1 \+ 2 continues with IS outside parentheses: YugabyteDB 2026\.1\.2 .*`,
		},
		{
			name: "SQL Server, IS NOT NULL", dialect: "sqlserver", column: "b bit DEFAULT 1 + 2 IS NOT NULL",
			wantErr: `line 1, column 45: expected default value: 1 \+ 2 continues with IS outside parentheses: SQL Server 2025 takes ` +
				`no predicate as a default, in parentheses or without, and answers Msg 156, Incorrect syntax near the keyword`,
		},
		{
			name: "SQL Server, IS DISTINCT FROM", dialect: "sqlserver", column: "b bit DEFAULT 1 + 2 IS DISTINCT FROM 3",
			wantErr: `line 1, column 45: expected default value: 1 \+ 2 continues with IS outside parentheses: SQL Server 2025 .*`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseAs(row.dialect, "CREATE TABLE t (id int, "+row.column+", m int);")

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
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
		// MySQL takes an operator default only in parentheses, so a document
		// read with no dialect, which takes one without, shows where the
		// expression ends.
		{
			name:    "INVISIBLE after an operator default",
			dialect: "",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int DEFAULT 1 + 2 INVISIBLE);",
			wantErr: `line 1, column 57: unsupported column attribute: INVISIBLE`,
		},
		{
			name:    "VISIBLE after an operator default",
			dialect: "",
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
