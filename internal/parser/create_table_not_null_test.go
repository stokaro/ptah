package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

func TestParseCreateTableNotNull_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		dialect    string
		columnName string
		nameOfRule string
	}{
		{name: "named after column", sql: "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL a);", dialect: platform.Postgres, columnName: "a", nameOfRule: "nn"},
		{name: "named before column", sql: "CREATE TABLE t (CONSTRAINT nn NOT NULL a, a int);", dialect: platform.Postgres, columnName: "a", nameOfRule: "nn"},
		{name: "unnamed", sql: "CREATE TABLE t (a int, NOT NULL a);", dialect: platform.Postgres, columnName: "a"},
		{name: "quoted column", sql: `CREATE TABLE t ("A" int, CONSTRAINT "Nn" NOT NULL "A");`, dialect: platform.Postgres, columnName: `"A"`, nameOfRule: `"Nn"`},
		{name: "unquoted column folds", sql: `CREATE TABLE t (a int, CONSTRAINT nn NOT NULL A);`, dialect: platform.Postgres, columnName: "a", nameOfRule: "nn"},
		{name: "without a dialect", sql: "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL a);", columnName: "a", nameOfRule: "nn"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 1)
			c.Assert(table.Columns[0].Name, qt.Equals, test.columnName)
			c.Assert(table.Columns[0].Nullable, qt.IsFalse)
			c.Assert(table.Columns[0].NotNullConstraintName, qt.Equals, test.nameOfRule)
		})
	}
}

func TestParseCreateTableNotNull_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		wantErr string
	}{
		{name: "missing column", sql: "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL b);", dialect: platform.Postgres, wantErr: `.*NOT NULL names column b, which table t does not declare.*`},
		{name: "quoted case differs", sql: `CREATE TABLE t (a int, CONSTRAINT nn NOT NULL "A");`, dialect: platform.Postgres, wantErr: `.*NOT NULL names column "A", which table t does not declare.*`},
		{name: "missing column name", sql: "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL);", dialect: platform.Postgres, wantErr: `.*expected column name after NOT NULL.*`},
		{name: "NO INHERIT", sql: "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL a NO INHERIT);", dialect: platform.Postgres, wantErr: `.*unexpected NO after a table element.*`},
		{name: "name conflict", sql: "CREATE TABLE t (a int CONSTRAINT first NOT NULL, CONSTRAINT second NOT NULL a);", dialect: platform.Postgres, wantErr: `.*already has a named NOT NULL constraint.*`},
		{name: "MySQL", sql: "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL a);", dialect: platform.MySQL, wantErr: `.*unsupported constraint type: NOT.*`},
		{name: "CockroachDB", sql: "CREATE TABLE t (a int, CONSTRAINT nn NOT NULL a);", dialect: platform.CockroachDB, wantErr: `.*unsupported constraint type: NOT.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
