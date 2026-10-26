package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// TestFromFunctionFor_KeepsABodyStatementOutsidePostgreSQL pins which body is
// SQL-standard. On the PostgreSQL family a LANGUAGE sql body that starts with
// BEGIN ATOMIC or RETURN is SQL, written after the routine's attributes
// (stokaro/ptah#4981), and a BEGIN ATOMIC body that names no language is
// written in sql, as PostgreSQL takes it. A MySQL, MariaDB or SQL Server
// function whose body is `RETURN a + 1` keeps that statement whole, RETURN
// included, and so does a PostgreSQL routine in another language.
func TestFromFunctionFor_KeepsABodyStatementOutsidePostgreSQL(t *testing.T) {
	tests := []struct {
		name     string
		function schemamodel.Function
		target   string
		kind     ast.FunctionBodyKind
		body     string
		language string
	}{
		{name: "a BEGIN ATOMIC body on PostgreSQL",
			function: schemamodel.Function{Name: "f", Language: "sql", Body: "BEGIN ATOMIC SELECT 1; END"},
			target:   platform.Postgres, kind: ast.FunctionBodyAtomic, body: "BEGIN ATOMIC SELECT 1; END", language: "sql"},
		{name: "a BEGIN ATOMIC body naming no language on PostgreSQL",
			function: schemamodel.Function{Name: "f", Body: "BEGIN ATOMIC SELECT 1; END"},
			target:   platform.Postgres, kind: ast.FunctionBodyAtomic, body: "BEGIN ATOMIC SELECT 1; END", language: "sql"},
		{name: "a RETURN body on PostgreSQL",
			function: schemamodel.Function{Name: "f", Language: "sql", Body: "RETURN (SELECT 1)"},
			target:   platform.Postgres, kind: ast.FunctionBodyReturn, body: "(SELECT 1)", language: "sql"},
		{name: "a quoted body on PostgreSQL",
			function: schemamodel.Function{Name: "f", Language: "sql", Body: "SELECT 1"},
			target:   platform.Postgres, kind: ast.FunctionBodyQuoted, body: "SELECT 1", language: "sql"},
		{name: "a PL/pgSQL body that starts with RETURN",
			function: schemamodel.Function{Name: "f", Language: "plpgsql", Body: "RETURN 1"},
			target:   platform.Postgres, kind: ast.FunctionBodyQuoted, body: "RETURN 1", language: "plpgsql"},
		{name: "a MySQL function body",
			function: schemamodel.Function{Name: "f", Language: "SQL", Body: "RETURN a + 1"},
			target:   platform.MySQL, kind: ast.FunctionBodyQuoted, body: "RETURN a + 1", language: "SQL"},
		{name: "a SQL Server function body",
			function: schemamodel.Function{Name: "f", Language: "sql", Body: "RETURN @a + 1"},
			target:   platform.SQLServer, kind: ast.FunctionBodyQuoted, body: "RETURN @a + 1", language: "sql"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			node := modelast.FromFunctionFor(test.function, test.target)

			c.Assert(node.BodyKind, qt.Equals, test.kind)
			c.Assert(node.Body, qt.Equals, test.body)
			c.Assert(node.Language, qt.Equals, test.language)
		})
	}
}
