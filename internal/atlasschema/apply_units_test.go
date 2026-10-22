package atlasschema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

func TestApplyRenderedPlanRefusesNativeSuffixOnSQLExecutor(t *testing.T) {
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeNone, migrator.MigrationTxModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "target.db")
			conn := connectSQLite(c, path)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			plan := planner.RenderedPlan{
				Request: renderer.Request{Target: "sqlite", Capabilities: conn.Info().Capabilities, Nodes: []ast.Node{ast.NewRawSQL("CREATE TABLE prefix (id INTEGER);"), &ast.ExtensionStatement{Payload: transactionOperation{}}}},
				Result:  renderer.Result{Complete: true, ExecutableUnits: true, Fragments: []renderer.Fragment{{SQL: "CREATE TABLE prefix (id INTEGER);"}, {Native: transactionOperation{}}}},
				Dialect: "sqlite",
			}
			c.Assert(atlasschema.ApplyRenderedPlan(c.Context(), conn, mode, plan), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sqliteTableExists(c, path, "prefix"), qt.IsFalse)
		})
	}
}
