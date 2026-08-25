package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// sequenceSQL renders one CREATE SEQUENCE.
func sequenceSQL(c *qt.C, build func(*ast.CreateSequenceNode)) string {
	c.Helper()
	node := ast.NewCreateSequence("s")
	build(node)
	sql, err := renderer.RenderSQL("oracle", node)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestCreateSequence_WritesTheCacheSize pins the clause.
//
// sequenceOptions handled start, increment, minvalue, maxvalue and cycle and had
// no branch for cache, so a declared cache was never applied: Oracle used its
// own default of 20, the reader reported 20, and the comparator recorded
// `cache: 20 -> N` on every run against a change the plan could not make.
// Measured on Oracle 23, two consecutive dry runs planned the same modification
// (stokaro/ptah#2207).
//
// The function's own doc comment already recorded that Oracle accepts the clause
// -- `CREATE SEQUENCE s START WITH 5 INCREMENT BY 2 MAXVALUE 100 CACHE 5 CYCLE`
// was measured whole -- so the clause was known and simply not written.
func TestCreateSequence_WritesTheCacheSize(t *testing.T) {
	c := qt.New(t)

	sql := sequenceSQL(c, func(node *ast.CreateSequenceNode) { node.SetCache(42) })

	c.Assert(sql, qt.Contains, "CACHE 42")
}

// TestCreateSequence_LeavesAnUndeclaredCacheAlone is the control.
//
// A sequence that declares no cache must not gain one: Oracle's default is 20
// and writing it would put a number nobody chose into every rendered sequence.
func TestCreateSequence_LeavesAnUndeclaredCacheAlone(t *testing.T) {
	c := qt.New(t)

	sql := sequenceSQL(c, func(_ *ast.CreateSequenceNode) {})

	c.Assert(sql, qt.Not(qt.Contains), "CACHE")
}
