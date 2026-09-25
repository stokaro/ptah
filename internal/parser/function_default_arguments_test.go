package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/parser"
)

// A default that calls a function with arguments is read whole, arguments as
// written (stokaro/ptah#3611). Only an empty argument list was read, so
// nextval('t_id_seq'::regclass), which pg_dump writes for every serial column,
// made the whole file unreadable.
func TestParse_FunctionDefaultArguments_HappyPath(t *testing.T) {
	rows := []struct {
		name   string
		column string
		want   string
	}{
		{name: "a sequence", column: "a int DEFAULT nextval('s')", want: "nextval('s')"},
		{name: "a sequence as a regclass", column: "a int DEFAULT nextval('s'::regclass)", want: "nextval('s'::regclass)"},
		{name: "a quoted, qualified sequence", column: `a int DEFAULT nextval('"app".s'::regclass)`, want: `nextval('"app".s'::regclass)`},
		{name: "a string argument", column: "a text DEFAULT lower('X')", want: "lower('X')"},
		{name: "nested calls", column: "a text DEFAULT concat('a', lower('B'), 'c')", want: "concat('a', lower('B'), 'c')"},
		{name: "a qualified function", column: "a timestamptz DEFAULT pg_catalog.now()", want: "pg_catalog.now()"},
		{name: "a cast after the call", column: "a timestamp DEFAULT now()::timestamp", want: "now()::timestamp"},
		{name: "no arguments, as before", column: "a timestamptz DEFAULT now()", want: "now()"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements := parsePostgres(c, "CREATE TABLE t (id int PRIMARY KEY, "+row.column+" NOT NULL);")

			value := defaultOf(c, statements)
			c.Assert(value.Expression, qt.Equals, row.want)
			c.Assert(value.Value, qt.Equals, "")
		})
	}
}

func TestParse_FunctionDefaultArguments_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an argument list that never closes",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT nextval('s'",
			wantErr: `expected default value: unterminated argument list in default at position \d+`,
		},
		{
			name:    "a qualified name that ends at the dot",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT pg_catalog.());",
			wantErr: `expected default value: expected function name after '\.' in default: .*`,
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
