package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// The MySQL family's `USING {BTREE|HASH}` is a clause the DDL asks for, and
// whether the server keeps it belongs to the storage engine rather than to the
// dialect. Measured 2026-09-03 with `KEY k USING HASH (a)`:
//
//	MySQL 8.4.11    InnoDB   INDEX_TYPE BTREE, SHOW CREATE TABLE drops the clause
//	MySQL 8.4.11    MEMORY   INDEX_TYPE HASH
//	MariaDB 11.8.9  InnoDB   INDEX_TYPE HASH, SHOW CREATE TABLE prints it back
//	MariaDB 11.8.9  MEMORY   INDEX_TYPE HASH
//
// So the parser carries what was asked for. Before this, the plain KEY
// spelling was a parse error -- `expected '(' ... got Identifier` for DDL both
// engines accept -- and the UNIQUE spelling parsed with the clause discarded,
// which turned a MariaDB HASH index into a BTREE one on the way out. See
// stokaro/ptah#2825.

func TestParseIndexAccessMethod_HashIsCarried(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantUnique bool
	}{
		{
			name:       "plain key",
			sql:        "CREATE TABLE t (a INT NOT NULL, KEY kh USING HASH (a));",
			wantName:   "kh",
			wantUnique: false,
		},
		{
			name:       "unique key",
			sql:        "CREATE TABLE t (a INT NOT NULL, UNIQUE KEY uh USING HASH (a));",
			wantName:   "uh",
			wantUnique: true,
		},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				result, err := parser.NewParser(test.sql, parser.WithDialect(dialect)).Parse()

				c.Assert(err, qt.IsNil)
				c.Assert(result.Statements, qt.HasLen, 1)
				table, ok := result.Statements[0].(*ast.CreateTableNode)
				c.Assert(ok, qt.IsTrue)
				c.Assert(table.Constraints, qt.HasLen, 0)
				c.Assert(table.Indexes, qt.HasLen, 1)
				c.Assert(table.Indexes[0].Name, qt.Equals, test.wantName)
				c.Assert(table.Indexes[0].Type, qt.Equals, "HASH")
				c.Assert(table.Indexes[0].Unique, qt.Equals, test.wantUnique)
			})
		}
	}
}

// BTREE is not carried, and the control matters: every index Ptah reads back
// from a server reports INDEX_TYPE BTREE, so a parser that kept the declared
// spelling would make a declared `USING BTREE` differ from the identical index
// declared without one -- and would put the clause into the DDL of every index
// ever read from a server.
func TestParseIndexAccessMethod_BtreeIsTheDefault(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "declared btree", sql: "CREATE TABLE t (a INT NOT NULL, KEY kb USING BTREE (a));"},
		{name: "declared nothing", sql: "CREATE TABLE t (a INT NOT NULL, KEY kb (a));"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				result, err := parser.NewParser(test.sql, parser.WithDialect(dialect)).Parse()

				c.Assert(err, qt.IsNil)
				table, ok := result.Statements[0].(*ast.CreateTableNode)
				c.Assert(ok, qt.IsTrue)
				c.Assert(table.Indexes, qt.HasLen, 1)
				c.Assert(table.Indexes[0].Type, qt.Equals, "")
			})
		}
	}
}

// A UNIQUE that asks for no method stays the constraint it was. Without this,
// promoting every UNIQUE to an index would pass the rows above.
func TestParseUniqueWithoutAccessMethod_StaysAConstraint(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				"CREATE TABLE t (a INT NOT NULL, UNIQUE KEY uq (a));",
				parser.WithDialect(dialect),
			).Parse()

			c.Assert(err, qt.IsNil)
			table, ok := result.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Indexes, qt.HasLen, 0)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].Type, qt.Equals, ast.UniqueConstraint)
			c.Assert(table.Constraints[0].Name, qt.Equals, "uq")
		})
	}
}

// The method belongs to the element that asked for it. A key carrying one
// beside a key that does not must not spread it, which is what a value left on
// the parser between elements would do.
func TestParseIndexAccessMethod_DoesNotLeakToTheNextElement(t *testing.T) {
	c := qt.New(t)

	result, err := parser.NewParser(
		"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, KEY kh USING HASH (a), KEY kn (b));",
		parser.WithDialect(platform.MariaDB),
	).Parse()

	c.Assert(err, qt.IsNil)
	table, ok := result.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Indexes, qt.HasLen, 2)
	c.Assert(table.Indexes[0].Name, qt.Equals, "kh")
	c.Assert(table.Indexes[0].Type, qt.Equals, "HASH")
	c.Assert(table.Indexes[1].Name, qt.Equals, "kn")
	c.Assert(table.Indexes[1].Type, qt.Equals, "")
}
