package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/parser"
)

// TestParse_ARowDeletionPolicyIsReadBack pins that `db read` output can be read
// back.
//
// The clause has to be READ, not merely rendered. This parser refuses a table
// option it does not know, so once the renderer emitted it, Ptah could no
// longer read its own description of a table that had one — and reading that
// description back is what the policy being modeled is for
// (stokaro/ptah#2236).
func TestParse_ARowDeletionPolicyIsReadBack(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		column   string
		interval string
	}{
		{
			name: "the form db read writes, with every identifier quoted",
			sql: `CREATE TABLE "ttl_a" (
  "id" bigint PRIMARY KEY NOT NULL,
  "created_at" timestamp with time zone NOT NULL
) TTL INTERVAL '4 WEEKS 2 DAYS' ON "created_at";`,
			column:   "created_at",
			interval: "4 WEEKS 2 DAYS",
		},
		{
			name: "the form a person writes, bare",
			sql: `CREATE TABLE ttl_b (id bigint PRIMARY KEY, ts timestamptz NOT NULL)
  TTL INTERVAL '30 days' ON ts;`,
			column:   "ts",
			interval: "30 days",
		},
		{
			// A doubled quote is how a name containing one is written, so
			// stripping only the outer pair stores `a""b` and the renderer
			// escapes it again into a column no database has.
			name:     "a column name containing a quote",
			sql:      `CREATE TABLE ttl_c (id bigint PRIMARY KEY) TTL INTERVAL '1 days' ON "a""b";`,
			column:   `a"b`,
			interval: "1 days",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql,
				parser.WithDialect(platform.Spanner)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			table, isTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(isTable, qt.IsTrue)
			c.Assert(table.RowDeletionPolicy, qt.IsNotNil)
			c.Assert(table.RowDeletionPolicy.Column, qt.Equals, test.column)
			c.Assert(table.RowDeletionPolicy.Interval, qt.Equals, test.interval)
		})
	}
}

// TestParse_TTLStaysClickHousesOnEveryOtherDialect is the control for the
// keyword the two clauses share.
//
// ClickHouse's TTL takes a date expression; a row deletion policy takes an
// interval and names its column separately, and neither statement is readable
// as the other. Without this row the dialect split could be dropped and every
// row above would still pass.
func TestParse_TTLStaysClickHousesOnEveryOtherDialect(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		"CREATE TABLE events (id Int64, created_at DateTime) ENGINE = MergeTree "+
			"ORDER BY id TTL created_at + INTERVAL 1 MONTH;",
		parser.WithDialect(platform.ClickHouse)).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table, isTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(isTable, qt.IsTrue)
	c.Assert(table.RowDeletionPolicy, qt.IsNil)
	c.Assert(table.Options["TTL"], qt.Contains, "created_at")
}
