package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
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
	c := qt.New(t)

	statements, omissions, err := builtin.GetOrderedCreateStatementsReportingOmissions(
		unloggedDatabase(true), "postgres", capability.ForDialect("postgres"))

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `CREATE UNLOGGED TABLE "cache"`)
	c.Assert(omissions, qt.HasLen, 0)
}

// TestGetOrderedCreateStatements_ALoggedTableRendersNoKeyword is the control
// the test above needs.
//
// Without it, a renderer that wrote UNLOGGED into every CREATE TABLE would
// satisfy the assertion, and every table in the tree would be unlogged.
func TestGetOrderedCreateStatements_ALoggedTableRendersNoKeyword(t *testing.T) {
	for _, dialect := range []string{"postgres", "yugabytedb", "cockroachdb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatements(unloggedDatabase(false), dialect)
			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, `CREATE TABLE "cache"`)
			c.Assert(rendered, qt.Not(qt.Contains), "UNLOGGED")
		})
	}
}

// TestGetOrderedCreateStatements_UnloggedTableOnYDB_FailurePath refuses the
// declaration on YDB, whose renderer refuses what it cannot hold.
func TestGetOrderedCreateStatements_UnloggedTableOnYDB_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(unloggedDatabase(true), "ydb")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*UNLOGGED is PostgreSQL's; YDB has no unlogged table.*`)
	c.Assert(statements, qt.IsNil)
}

// TestGetOrderedCreateStatements_ADialectWithoutUnloggedTablesOmitsTheKeyword
// covers the other half of the predicate the renderer, the reader and the
// comparison share.
func TestGetOrderedCreateStatements_ADialectWithoutUnloggedTablesOmitsTheKeyword(t *testing.T) {
	for _, dialect := range []string{"cockroachdb", "yugabytedb", "spanner", "mysql", "mariadb", "sqlite", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatements(unloggedDatabase(true), dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "UNLOGGED")
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesAnUnloggedTableATargetDrops
// covers every target that creates an ordinary table for the declaration:
// CockroachDB and Spanner have no unlogged table, YugabyteDB takes the keyword
// and creates an ordinary table, and the other engines have no such table
// either. Each reports the declaration lost rather than dropping it unsaid.
func TestGetOrderedCreateStatementsReportingOmissions_NamesAnUnloggedTableATargetDrops(t *testing.T) {
	for _, dialect := range []string{"cockroachdb", "yugabytedb", "spanner", "mysql", "mariadb", "sqlite", "sqlserver", "oracle", "clickhouse"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			_, omissions, err := builtin.GetOrderedCreateStatementsReportingOmissions(
				unloggedDatabase(true), dialect, capability.ForDialect(dialect))
			c.Assert(err, qt.IsNil)
			c.Assert(omissionSubjects(c, omissions), qt.DeepEquals, []string{"table cache unlogged"})
		})
	}
}
