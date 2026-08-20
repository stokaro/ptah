package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// TestMaterializedViewRenderingEmitsNoRefresh is what this file asserts now,
// and it is the decision itself rather than a consequence of it.
//
// Every test here used to be about a refresh STRATEGY the renderer validated
// and then ignored. Ptah does not refresh materialized views as part of schema
// reconciliation: one is populated when it is created, a changed definition is
// reconciled as DROP and CREATE, and it goes stale only when its source data
// changes -- which a schema comparison cannot observe (stokaro/ptah#1625).
//
// So the property worth pinning is that rendering a schema emits no REFRESH on
// any target. A future explicit refresh command would emit one from its own
// path; nothing here may.
func TestMaterializedViewRenderingEmitsNoRefresh(t *testing.T) {
	dialects := []string{
		platform.Postgres,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLite,
		platform.ClickHouse,
		platform.SQLServer,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Spanner,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(materializedViewDatabase(), dialect)

			// A target that hosts no materialized view at all refuses the
			// OBJECT, which is a different and unrelated refusal. What must
			// never happen on any target is a refusal about the refresh, or a
			// REFRESH statement in the output.
			c.Assert(strings.ToUpper(strings.Join(statements, "\n")), qt.Not(qt.Contains), "REFRESH MATERIALIZED VIEW")
			c.Assert(strings.ToLower(errorText(err)), qt.Not(qt.Contains), "refresh")
		})
	}
}

// TestMaterializedViewRendersOnEveryTargetThatHasOne is the control in the
// other direction: removing the strategy removed a refusal, not the object.
//
// The targets listed here are the ones whose renderer emits the statement; the
// rest name a skip, which the whole-schema path above already exercises.
func TestMaterializedViewRendersOnEveryTargetThatHasOne(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres,
		platform.ClickHouse,
		platform.CockroachDB,
		platform.YugabyteDB,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, materializedViewNode())

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW")
			c.Assert(strings.ToUpper(sql), qt.Not(qt.Contains), "REFRESH")
		})
	}
}

func materializedViewNode() *ast.CreateMaterializedViewNode {
	return ast.NewCreateMaterializedView("analytics.user_counts").
		SetBody("SELECT count(*) AS total FROM analytics.users")
}

func materializedViewDatabase() *goschema.Database {
	return &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		Name: "analytics.user_counts",
		Body: "SELECT count(*) AS total FROM analytics.users"}}}
}
