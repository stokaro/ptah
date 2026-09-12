package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// unloggedDatabase declares one table and says whether its writes skip the
// write-ahead log.
func unloggedDatabase(unlogged bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Cache", Name: "cache", Unlogged: unlogged,
		}},
		Fields: []schemamodel.Field{
			{StructName: "Cache", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
		},
	}
}

// TestGetOrderedCreateStatements_AnUnloggedTableRendersTheKeyword pins where
// the word goes: between CREATE and TABLE, not among the trailing options.
func TestGetOrderedCreateStatements_AnUnloggedTableRendersTheKeyword(t *testing.T) {
	for _, dialect := range []string{"postgres", "yugabytedb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(unloggedDatabase(true), dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, `CREATE UNLOGGED TABLE "cache"`)
		})
	}
}

// TestGetOrderedCreateStatements_ALoggedTableRendersNoKeyword is the control
// the test above needs.
//
// Without it, a renderer that wrote UNLOGGED into every CREATE TABLE would
// satisfy the assertion, and every table in the tree would be unlogged.
func TestGetOrderedCreateStatements_ALoggedTableRendersNoKeyword(t *testing.T) {
	for _, dialect := range []string{"postgres", "yugabytedb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(unloggedDatabase(false), dialect)
			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, `CREATE TABLE "cache"`)
			c.Assert(rendered, qt.Not(qt.Contains), "UNLOGGED")
		})
	}
}

// TestGetOrderedCreateStatements_ADialectWithoutUnloggedTablesOmitsTheKeyword
// covers the other half of the predicate the renderer and the reader share.
//
// CockroachDB and Spanner speak the PostgreSQL wire protocol and neither
// creates an unlogged table, so a family-wide test would have asserted the
// keyword onto two targets that answer it with a syntax error or a notice that
// nothing happened.
func TestGetOrderedCreateStatements_ADialectWithoutUnloggedTablesOmitsTheKeyword(t *testing.T) {
	for _, dialect := range []string{"cockroachdb", "mysql", "mariadb", "sqlite", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(unloggedDatabase(true), dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "UNLOGGED")
		})
	}
}
