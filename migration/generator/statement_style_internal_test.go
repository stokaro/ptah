package generator

// White-box testing required: the statement style of a generated pair is read
// off planGeneratedMigrationSpecs, which needs no server; the exported path
// reads a live database, and integration/native_exact_statement_style_e2e_test.go
// drives it against PostgreSQL.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasmigrate"
)

// TestPlanGeneratedMigrationSpecs_PlansInTheExactStatementStyle generates the
// removal of one table on each kind of planner. Every planner leaves out the
// comment that only describes the drop and the guard no earlier statement needs.
// Planned in the annotated style, the PostgreSQL row writes `DROP TABLE IF
// EXISTS "gone" CASCADE;`, every other row writes its guard, and every row writes
// a `-- WARNING` comment.
func TestPlanGeneratedMigrationSpecs_PlansInTheExactStatementStyle(t *testing.T) {
	const comment = "-- WARNING"
	tests := []struct {
		name    string
		dialect string
		wantUp  string
	}{
		{name: "PostgreSQL", dialect: platform.Postgres, wantUp: `DROP TABLE "gone";`},
		{name: "MySQL", dialect: platform.MySQL, wantUp: "DROP TABLE `gone`;"},
		{name: "SQLite", dialect: platform.SQLite, wantUp: `DROP TABLE "gone";`},
		{name: "SQL Server", dialect: platform.SQLServer, wantUp: `DROP TABLE [gone];`},
		{name: "Oracle", dialect: platform.Oracle, wantUp: `DROP TABLE gone PURGE;`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			gone := catalog.Table{Name: "gone", Type: "BASE TABLE", Columns: []catalog.Column{{Name: "id", DataType: "integer", UDTName: "int4"}}}
			diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "gone", Current: schemacapture.TableObservation{Table: gone}}}}

			specs, _, err := planGeneratedMigrationSpecs(t.Context(), must.Must(builtin.New()),
				diff,
				&schemamodel.Database{},
				&catalog.Database{Tables: []catalog.Table{gone}},
				catalog.ServerInfo{Dialect: test.dialect, Capabilities: capability.ForDialect(test.dialect)},
				100,
				"remove_gone",
				DiffPolicy{},
				atlasmigrate.Qualifier{},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(specs, qt.HasLen, 1)
			c.Assert(specs[0].UpSQL, qt.Contains, test.wantUp)
			c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), comment)
		})
	}
}
