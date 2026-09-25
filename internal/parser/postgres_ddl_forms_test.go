package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// The four PostgreSQL forms stokaro/ptah#3562 found refused in one real schema
// file: an array cast in a default, ADD COLUMN IF NOT EXISTS, and the STRICT
// and PARALLEL routine attributes.

func parsePostgres(c *qt.C, sql string) *ast.StatementList {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect("postgres")).Parse()
	c.Assert(err, qt.IsNil)
	return statements
}

// defaultOf returns the default of the one column a single CREATE TABLE
// declares after its id.
func defaultOf(c *qt.C, statements *ast.StatementList) *ast.DefaultValue {
	c.Helper()
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[0]))
	c.Assert(table.Columns, qt.HasLen, 2)
	return table.Columns[1].Default
}

// The type after `::` is a whole type. Reading one word of it left `[]`, a
// modifier list or the rest of a multi-word name behind, and the column list
// then failed on the leftover.
func TestParse_CastInADefaultIsReadWhole_HappyPath(t *testing.T) {
	rows := []struct {
		name   string
		column string
		want   string
	}{
		{name: "array", column: "ids uuid[] NOT NULL DEFAULT '{}'::uuid[]", want: "'{}'::uuid[]"},
		{name: "two dimensions", column: "grid text[][] DEFAULT '{}'::text[][]", want: "'{}'::text[][]"},
		{name: "modifier list and brackets", column: "a numeric(5,2)[] DEFAULT '{1.5}'::numeric(5,2)[]", want: "'{1.5}'::numeric(5,2)[]"},
		{name: "multi-word type", column: "v varchar(10)[] DEFAULT '{}'::character varying(10)[]", want: "'{}'::character varying(10)[]"},
		{name: "schema-qualified element type", column: "m text[] DEFAULT '{}'::app.mood[]", want: "'{}'::app.mood[]"},
		{name: "quoted type", column: `m text DEFAULT 'x'::"Mood"`, want: `'x'::"Mood"`},
		{name: "time zone words", column: "t timestamptz DEFAULT '2020-01-01'::timestamp with time zone", want: "'2020-01-01'::timestamp with time zone"},
		{name: "a chain of casts", column: "s text DEFAULT 'x'::text::character varying", want: "'x'::text::character varying"},
		{name: "a scalar cast, as before", column: "d jsonb DEFAULT '{}'::jsonb", want: "'{}'::jsonb"},
		{name: "no cast", column: "n text DEFAULT 'x'", want: "'x'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements := parsePostgres(c, "CREATE TABLE t (id int PRIMARY KEY, "+row.column+");")

			c.Assert(defaultOf(c, statements).Value, qt.Equals, row.want)
		})
	}
}

// A `::` that does not introduce a type is refused rather than dropped, which
// is what the old single-word read did with anything but an identifier.
func TestParse_CastInADefaultIsReadWhole_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		column  string
		wantErr string
	}{
		{name: "no type after ::", column: "n text DEFAULT 'x'::", wantErr: `expected default value: expected type after '::': .*`},
		{name: "a single colon", column: "n text DEFAULT 'x':text", wantErr: `expected default value: expected '::' for a type cast at position \d+`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(
				"CREATE TABLE t (id int PRIMARY KEY, "+row.column+");", parser.WithDialect("postgres"),
			).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func addColumnOf(c *qt.C, statements *ast.StatementList) *ast.AddColumnOperation {
	c.Helper()
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[0]))
	c.Assert(alter.Operations, qt.HasLen, 1)
	operation, ok := alter.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", alter.Operations[0]))
	return operation
}

func TestParse_AddColumnIfNotExists_HappyPath(t *testing.T) {
	rows := []struct {
		name            string
		sql             string
		wantIfNotExists bool
		wantColumn      string
	}{
		{name: "guarded", sql: "ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at timestamptz;", wantIfNotExists: true, wantColumn: "password_changed_at"},
		{name: "unguarded", sql: "ALTER TABLE users ADD COLUMN note text;", wantColumn: "note"},
		{name: "without the COLUMN keyword", sql: "ALTER TABLE users ADD note text;", wantColumn: "note"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			operation := addColumnOf(c, parsePostgres(c, row.sql))

			c.Assert(operation.IfNotExists, qt.Equals, row.wantIfNotExists)
			c.Assert(operation.Column.Name, qt.Equals, row.wantColumn)
		})
	}
}

func TestParse_AddColumnIfNotExists_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		"ALTER TABLE users ADD COLUMN IF EXISTS note text;", parser.WithDialect("postgres"),
	).Parse()

	c.Assert(err, qt.ErrorMatches, `expected NOT after IF: .*`)
	c.Assert(statements, qt.IsNil)
}

// The routine attributes a schema file states are read into the node rather
// than refused, and the long spellings land on the same properties.
func TestParse_RoutineNullInputAndParallelClauses_HappyPath(t *testing.T) {
	rows := []struct {
		name          string
		clauses       string
		wantStrict    bool
		wantParallel  string
		wantLeakproof bool
	}{
		{name: "STRICT", clauses: "IMMUTABLE STRICT", wantStrict: true},
		{name: "RETURNS NULL ON NULL INPUT", clauses: "RETURNS NULL ON NULL INPUT", wantStrict: true},
		{name: "CALLED ON NULL INPUT", clauses: "CALLED ON NULL INPUT"},
		{name: "the default, stated nowhere", clauses: "IMMUTABLE"},
		{name: "PARALLEL SAFE", clauses: "PARALLEL SAFE", wantParallel: "SAFE"},
		{name: "PARALLEL RESTRICTED", clauses: "PARALLEL restricted", wantParallel: "RESTRICTED"},
		{name: "PARALLEL UNSAFE", clauses: "PARALLEL UNSAFE", wantParallel: "UNSAFE"},
		{name: "LEAKPROOF", clauses: "LEAKPROOF", wantLeakproof: true},
		{name: "NOT LEAKPROOF", clauses: "NOT LEAKPROOF"},
		{
			name:         "the declaration that raised the issue",
			clauses:      "IMMUTABLE STRICT PARALLEL SAFE",
			wantStrict:   true,
			wantParallel: "SAFE",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			function := functionOf(c, parsePostgres(c,
				"CREATE FUNCTION add_arrays(a integer[], b integer[]) RETURNS integer[] LANGUAGE sql "+
					row.clauses+" AS $$ SELECT a || b $$;"))

			c.Assert(function.Strict, qt.Equals, row.wantStrict)
			c.Assert(function.Parallel, qt.Equals, row.wantParallel)
			c.Assert(function.Leakproof, qt.Equals, row.wantLeakproof)
			c.Assert(function.Returns, qt.Equals, "integer[]")
		})
	}
}

// A clause written straight after the return type ends it. A keyword missing
// from the list the type stops at was read as part of the type, so the clause
// vanished into a return type nobody wrote.
func TestParse_RoutineReturnTypeEndsAtAClause_HappyPath(t *testing.T) {
	rows := []struct {
		name        string
		sql         string
		wantReturns string
		wantStrict  bool
	}{
		{
			name:        "STRICT after the type",
			sql:         "CREATE FUNCTION f(a int) RETURNS int STRICT LANGUAGE sql AS $$ SELECT a $$;",
			wantReturns: "int",
			wantStrict:  true,
		},
		{
			name:        "RETURNS NULL ON NULL INPUT after the type",
			sql:         "CREATE FUNCTION f(a int) RETURNS int RETURNS NULL ON NULL INPUT LANGUAGE sql AS $$ SELECT a $$;",
			wantReturns: "int",
			wantStrict:  true,
		},
		{
			// A clause keyword inside the parentheses is a column name.
			name:        "a table result whose column is called rows",
			sql:         "CREATE FUNCTION f() RETURNS TABLE (rows int, parallel text) LANGUAGE sql AS $$ SELECT 1, 'x' $$;",
			wantReturns: "TABLE (rows int, parallel text)",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			function := functionOf(c, parsePostgres(c, row.sql))

			c.Assert(function.Returns, qt.Equals, row.wantReturns)
			c.Assert(function.Strict, qt.Equals, row.wantStrict)
		})
	}
}

// What PostgreSQL would refuse, or what Ptah does not model, is refused here
// rather than read as a default.
func TestParse_RoutineNullInputAndParallelClauses_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an unknown PARALLEL level",
			sql:     "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql PARALLEL MAYBE AS $$ SELECT a $$;",
			wantErr: `unsupported PARALLEL level MAYBE at position \d+`,
		},
		{
			name:    "CALLED without ON NULL INPUT",
			sql:     "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql CALLED ON INPUT AS $$ SELECT a $$;",
			wantErr: `expected CALLED ON NULL INPUT: .*`,
		},
		{
			name:    "RETURNS NULL without ON NULL INPUT",
			sql:     "CREATE FUNCTION f(a int) RETURNS int RETURNS NULL LANGUAGE sql AS $$ SELECT a $$;",
			wantErr: `expected RETURNS NULL ON NULL INPUT: .*`,
		},
		{
			name:    "a NOT clause Ptah does not know",
			sql:     "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql NOT STRICT AS $$ SELECT a $$;",
			wantErr: `unsupported CREATE FUNCTION clause: NOT STRICT at position \d+`,
		},
		{
			// COST ended nothing before, and read as part of the return type.
			name:    "a clause Ptah does not model, after the type",
			sql:     "CREATE FUNCTION f(a int) RETURNS int COST 100 LANGUAGE sql AS $$ SELECT a $$;",
			wantErr: `unsupported CREATE FUNCTION clause: COST at position \d+`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
