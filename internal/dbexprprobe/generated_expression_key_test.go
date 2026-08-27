package dbexprprobe_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/dbexprprobe"
	"go.5x5.cz/ptah/internal/exprkey"
)

// TestGeneratedExpressionProbe_KeyForIsTheKeyTheComparisonReads holds the two
// halves of one map to each other.
//
// This package WRITES the map and `migration/schemadiff/internal/compare` reads
// it, and neither can see the other's spelling. A key that differed between them
// would leave every entry unfound — which is not an error anywhere: the
// comparison would leave the expression uncompared, exactly as it does when
// nobody asked a server, so a broken key looks like a working one.
//
// Everything else on this path needs a live Oracle connection. The key does not,
// and this is where it is measured (stokaro/ptah#2315).
func TestGeneratedExpressionProbe_KeyForIsTheKeyTheComparisonReads(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		table   string
		column  string
	}{
		{name: "unqualified", dialect: platform.Oracle, table: "ora_posts", column: "doubled"},
		{name: "qualified", dialect: platform.Oracle, schema: "app", table: "ora_posts", column: "doubled"},
		{name: "a quoted table carrying a dot", dialect: platform.Oracle, table: `"a.b"`, column: "c"},
		{name: "a quoted column on PostgreSQL", dialect: platform.Postgres, table: "posts", column: `"Doubled"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(test.dialect)
			probe := dbexprprobe.GeneratedExpressionProbe{
				Schema: test.schema, Table: test.table, Generated: []string{test.column},
			}

			c.Assert(probe.KeyFor(semantics, test.column), qt.Equals,
				exprkey.Generated(semantics, test.schema, test.table, test.column))
		})
	}
}

// TestGeneratedExpressionProbe_KeyForSeparatesComponentsADotWouldMerge is the
// property a joined string cannot hold.
//
// `table "a.b", column "c"` and `schema "a", table "b", column "c"` are two
// columns, and a key that joined its components with dots made them one entry —
// so one column's resolved expression would answer for the other's declaration.
func TestGeneratedExpressionProbe_KeyForSeparatesComponentsADotWouldMerge(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.Oracle)

	dotInTable := dbexprprobe.GeneratedExpressionProbe{Table: `"a.b"`}
	schemaAndTable := dbexprprobe.GeneratedExpressionProbe{Schema: "a", Table: "b"}

	c.Assert(dotInTable.KeyFor(semantics, "c"), qt.Not(qt.Equals), schemaAndTable.KeyFor(semantics, "c"))
}
