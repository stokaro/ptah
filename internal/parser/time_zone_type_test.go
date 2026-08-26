package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/parser"
)

// columnTypeOf parses one CREATE TABLE and returns its second column's type.
func columnTypeOf(c *qt.C, sql string) string {
	c.Helper()

	statements, err := parser.NewParser(sql, parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.Not(qt.HasLen), 0)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(len(table.Columns) > 1, qt.IsTrue)
	return table.Columns[1].Type
}

// TestParseColumnType_ATimeZoneQualifierGoesOnTheEnd is the defect.
//
// The qualifier was PREPENDED: `timestamp with time zone` parsed to
// `with timestamp time zone`, which no server accepts. It reached a user
// through the round trip Ptah renders itself -- `ptah schema inspect
// --format sql` writes `timestamp with time zone`, and applying that document
// answered `ERROR: syntax error at or near "with"` (SQLSTATE 42601), measured on
// PostgreSQL 18.6.
//
// One of PostgreSQL's most common column types, and the SQL Ptah produced for it
// could not be read back by Ptah.
func TestParseColumnType_ATimeZoneQualifierGoesOnTheEnd(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "timestamp with time zone",
			sql:  `CREATE TABLE z (id BIGINT, a timestamp with time zone);`,
			want: "timestamp with time zone",
		},
		{
			name: "timestamp without time zone",
			sql:  `CREATE TABLE z (id BIGINT, a timestamp without time zone);`,
			want: "timestamp without time zone",
		},
		{
			name: "time with time zone",
			sql:  `CREATE TABLE z (id BIGINT, a time with time zone);`,
			want: "time with time zone",
		},
		{
			name: "time without time zone",
			sql:  `CREATE TABLE z (id BIGINT, a time without time zone);`,
			want: "time without time zone",
		},
		{
			name: "the upper-case spelling",
			sql:  `CREATE TABLE z (id BIGINT, a TIMESTAMP WITH TIME ZONE);`,
			want: "TIMESTAMP WITH TIME ZONE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(columnTypeOf(c, test.sql), qt.Equals, test.want)
		})
	}
}

// TestParseColumnType_TheUnqualifiedAndOtherMultiWordTypesAreUntouched is the
// control.
//
// The qualifier is consumed only after WITH or WITHOUT, and the other multi-word
// types go through the same function. A fix that moved the wrong tokens, or
// consumed one too many, would show here.
func TestParseColumnType_TheUnqualifiedAndOtherMultiWordTypesAreUntouched(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a bare timestamp", sql: `CREATE TABLE z (id BIGINT, a timestamp);`, want: "timestamp"},
		{name: "a bare time", sql: `CREATE TABLE z (id BIGINT, a time);`, want: "time"},
		{
			name: "a parameterized timestamp",
			sql:  `CREATE TABLE z (id BIGINT, a timestamp(3));`,
			want: "timestamp(3)",
		},
		{name: "double precision", sql: `CREATE TABLE z (id BIGINT, a double precision);`, want: "double precision"},
		{
			name: "character varying",
			sql:  `CREATE TABLE z (id BIGINT, a character varying(20));`,
			want: "character varying(20)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(columnTypeOf(c, test.sql), qt.Equals, test.want)
		})
	}
}

// TestParseColumnType_TheColumnAfterAQualifiedTypeIsStillRead is the other
// control: the qualifier consumes exactly its own words.
//
// Consuming one token too many would swallow the next column's name, and every
// assertion above would still pass.
func TestParseColumnType_TheColumnAfterAQualifiedTypeIsStillRead(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		`CREATE TABLE z (id BIGINT, a timestamp with time zone, b INTEGER NOT NULL);`,
		parser.WithDialect(platform.Postgres),
	).Parse()

	c.Assert(err, qt.IsNil)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Columns, qt.HasLen, 3)
	c.Assert(table.Columns[2].Name, qt.Equals, "b")
	c.Assert(table.Columns[2].Type, qt.Equals, "INTEGER")
	c.Assert(table.Columns[2].Nullable, qt.IsFalse)
}
