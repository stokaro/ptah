package generator

// White-box testing required: the statement style of a generated pair is read
// off planGeneratedMigrationSpecs, which needs no server; the exported path
// reads a live database, and integration/native_exact_statement_style_e2e_test.go
// drives it against PostgreSQL.

import (
	"strings"
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
// removal of one table on each kind of planner. The PostgreSQL family settles
// its guards and CASCADE and leaves out the comment that only describes the
// drop, the MySQL family leaves out the comment and keeps its guard, and SQLite
// does not honor the style. Planned in the annotated style, the PostgreSQL row
// writes `DROP TABLE IF EXISTS "gone" CASCADE;` and the MySQL row writes the
// comment.
func TestPlanGeneratedMigrationSpecs_PlansInTheExactStatementStyle(t *testing.T) {
	const comment = "-- WARNING: This will delete all data!"
	tests := []struct {
		name         string
		dialect      string
		capabilities capability.Capabilities
		wantUp       string
		wantComment  bool
	}{
		{name: "PostgreSQL", dialect: platform.Postgres, capabilities: capability.Postgres16(), wantUp: `DROP TABLE "gone";`},
		{name: "MySQL", dialect: platform.MySQL, capabilities: capability.ForDialect(platform.MySQL), wantUp: "DROP TABLE IF EXISTS `gone`;"},
		{
			name: "SQLite", dialect: platform.SQLite, capabilities: capability.ForDialect(platform.SQLite),
			wantUp: `DROP TABLE IF EXISTS "gone";`, wantComment: true,
		},
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
				catalog.ServerInfo{Dialect: test.dialect, Capabilities: test.capabilities},
				100,
				"remove_gone",
				DiffPolicy{},
				atlasmigrate.Qualifier{},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(specs, qt.HasLen, 1)
			c.Assert(specs[0].UpSQL, qt.Contains, test.wantUp)
			c.Assert(strings.Contains(specs[0].UpSQL, comment), qt.Equals, test.wantComment, qt.Commentf("%s", specs[0].UpSQL))
		})
	}
}
