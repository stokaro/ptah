package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
)

// tableCollateSource declares a table collation with the table directive's
// collate, as introspect writes one read from a MySQL or MariaDB server.
const tableCollateSource = `package entities

//ptah:schema:table name="docs" collate="utf8mb4_bin"
type Doc struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`

// TestTableCollateAnnotation_TheMySQLFamilyWritesIt pins that MySQL and
// MariaDB write a declared table collation as the COLLATE table option and
// report nothing left out.
func TestTableCollateAnnotation_TheMySQLFamilyWritesIt(t *testing.T) {
	database := must.Must(goschema.ParseSource(builtintest.Annotations(), "docs.go", tableCollateSource))
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := builtin.GetOrderedCreateStatementsReportingOmissions(&database, dialect, capability.ForDialect(dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(statements[0], qt.Contains, ") COLLATE=utf8mb4_bin;")
		})
	}
}

// TestTableCollateAnnotation_AnotherTargetReportsItSkipped pins that every
// other built-in target, which has no table collation, leaves the option out
// of its statement and reports it as a skipped table option with the declared
// value, so a render never drops it silently. A PostgreSQL-family target also
// names it on a comment above the statement.
func TestTableCollateAnnotation_AnotherTargetReportsItSkipped(t *testing.T) {
	database := must.Must(goschema.ParseSource(builtintest.Annotations(), "docs.go", tableCollateSource))
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.SQLite,
		platform.SQLServer, platform.Oracle, platform.ClickHouse} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := builtin.GetOrderedCreateStatementsReportingOmissions(&database, dialect, capability.ForDialect(dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.DeepEquals, []renderer.Omission{{Dialect: dialect, Reason: "unsupported", Kind: "table",
				Name: "docs", Property: "table option COLLATE", Detail: "utf8mb4_bin"}})
			c.Assert(statements[0], qt.Not(qt.Contains), ") COLLATE")
		})
	}
}
