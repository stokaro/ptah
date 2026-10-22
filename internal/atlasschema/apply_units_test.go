package atlasschema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

func TestCapturedApplyPlanRefusesNativeSuffixOnSQLExecutor(t *testing.T) {
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
			captured, err := atlasschema.CaptureApplyPlan(conn.Info(), plan)
			c.Assert(err, qt.IsNil)
			c.Assert(captured.Execute(c.Context(), conn, mode), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sqliteTableExists(c, path, "prefix"), qt.IsFalse)
		})
	}
}

func TestCapturedApplyKeepsBaseAndProvidedCapabilitiesSeparate(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "target.db")
	conn := connectSQLite(c, path)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	info := conn.Info()
	rendered := planner.RenderedPlan{
		Request: renderer.Request{Target: info.Dialect, Capabilities: info.Capabilities.With(capability.Hypertables, true), Nodes: []ast.Node{ast.NewRawSQL("CREATE TABLE prefix (id INTEGER);")}},
		Result:  renderer.Result{Complete: true, ExecutableUnits: true, Fragments: renderer.SQLFragments([]string{"CREATE TABLE prefix (id INTEGER);"})},
		Dialect: info.Dialect,
	}
	captured, err := atlasschema.CaptureApplyPlan(info, rendered)
	c.Assert(err, qt.IsNil)
	c.Assert(captured.Execute(c.Context(), conn, migrator.MigrationTxModeNone), qt.IsNil)
	c.Assert(sqliteTableExists(c, path, "prefix"), qt.IsTrue)
}
