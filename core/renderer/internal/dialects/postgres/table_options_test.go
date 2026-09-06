package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// TestVisitCreateTable_NamesEveryTableOptionItDoesNotRender pins the
// disposition stokaro/ptah#2969 settled for the MySQL-family table options
// on this family: the statement carries none of them, and each is named on
// a skip line above it, in key order, the way a foreign key the target
// cannot host is named. Before this the options were written after the
// column list as `KEY=value`, which every PostgreSQL-family server refuses
// as a syntax error, so a render succeeded and the apply failed.
func TestVisitCreateTable_NamesEveryTableOptionItDoesNotRender(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "postgres", dialect: platform.Postgres, caps: capability.Postgres17()},
		{name: "cockroachdb", dialect: platform.CockroachDB, caps: capability.CockroachDB25()},
		{name: "spanner", dialect: platform.Spanner, caps: capability.SpannerPostgres()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r := postgres.NewWithCapabilities(test.caps, test.dialect)
			r.Reset()

			node := ast.NewCreateTable("t").
				AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
				SetOption("ENGINE", "InnoDB").
				SetOption("COLLATE", "utf8mb4_bin").
				SetOption("CHARSET", "utf8mb4")
			c.Assert(node.Accept(r), qt.IsNil)

			out := r.Output()
			upper := strings.ToUpper(test.dialect)
			c.Assert(out, qt.Contains, "-- "+upper+": table option CHARSET=utf8mb4 is not supported by this target; skipped.\n"+
				"-- "+upper+": table option COLLATE=utf8mb4_bin is not supported by this target; skipped.\n"+
				"-- "+upper+": table option ENGINE=InnoDB is not supported by this target; skipped.\n"+
				"CREATE TABLE \"t\" (")
			c.Assert(out, qt.Contains, "\n);")
			c.Assert(out, qt.Not(qt.Contains), ") ENGINE")
			c.Assert(out, qt.Not(qt.Contains), ") CHARSET")
			c.Assert(out, qt.Not(qt.Contains), "=utf8mb4;")
		})
	}
}

// TestVisitCreateTable_SaysWhereAnAutoIncrementStartGoes: the one option
// whose value the author would miss names the column attribute this family
// keeps it under.
func TestVisitCreateTable_SaysWhereAnAutoIncrementStartGoes(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)
	r.Reset()

	node := ast.NewCreateTable("t").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		SetOption("AUTO_INCREMENT", "100")
	c.Assert(node.Accept(r), qt.IsNil)

	out := r.Output()
	c.Assert(out, qt.Contains, "-- POSTGRES: table option AUTO_INCREMENT=100 is not supported by this target; skipped.\n"+
		"-- POSTGRES: declare the start on the key column with identity_start to keep it.\n"+
		"CREATE TABLE \"t\" (")
	c.Assert(out, qt.Not(qt.Contains), "AUTO_INCREMENT=100;")
}

// TestVisitCreateTable_SaysNothingWhereThereIsNoOption is the control: a
// table without options carries no skip line, so the lines above are the
// options' and not the renderer's habit.
func TestVisitCreateTable_SaysNothingWhereThereIsNoOption(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)
	r.Reset()

	node := ast.NewCreateTable("t").AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	c.Assert(node.Accept(r), qt.IsNil)

	c.Assert(r.Output(), qt.Not(qt.Contains), "skipped")
	c.Assert(r.Output(), qt.Not(qt.Contains), "identity_start")
}
