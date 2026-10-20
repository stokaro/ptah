//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// declaredExtension is a desired schema that declares one extension to its
// owner, and claims that it declares every extension.
func declaredExtension(c *qt.C, name string, extension pgextension.DesiredExtension) *schemamodel.Database {
	c.Helper()
	object, err := pgextension.DesiredObject(pgextension.Ref(name), extension)
	c.Assert(err, qt.IsNil)
	objects, err := schemaext.NewObjects(object)
	c.Assert(err, qt.IsNil)
	known, err := pgextension.CompleteCoverage(schemaext.Desired)
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{FeatureObjects: objects, FeatureCoverage: known}
}

// extensionChanges returns the extension owner's changes a diff holds.
func extensionChanges(diff *difftypes.SchemaDiff) []*pgextension.Change {
	var changes []*pgextension.Change
	for _, record := range diff.FeatureChanges {
		if change, ok := record.Value.(*pgextension.Change); ok {
			changes = append(changes, change)
		}
	}
	return changes
}

// observedExtensions returns what a read reports of each extension, by name.
func observedExtensions(c *qt.C, objects schemaext.Objects) map[string]pgextension.ObservedExtension {
	c.Helper()
	all, err := objects.All()
	c.Assert(err, qt.IsNil)
	observed := make(map[string]pgextension.ObservedExtension)
	for _, object := range all {
		if extension, ok := object.Value.(*pgextension.ObservedExtension); ok {
			observed[pgextension.Name(object.Ref)] = *extension
		}
	}
	return observed
}

// TestExtensionVersionAndSchemaConvergeLive is the test the extension halves
// could not have been changed without.
//
// The version gap was a silent no-op on the one attribute of an extension that
// moves over time: the version was rendered on the create and never compared,
// so raising a pin left `Schema is synced` against a database still running the
// old one. Step 3 is what proves it is gone, and no offline assertion reaches
// it -- the comparison has to run against what a real catalog reports.
//
// The schema move was the opposite failure: refused for every target with
// "extension schema moves are not yet supported", on an engine that has
// `ALTER EXTENSION ... SET SCHEMA` (stokaro/ptah#1718).
func TestExtensionVersionAndSchemaConvergeLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	home := fmt.Sprintf("ptah_ext_%d", time.Now().UnixNano())
	away := home + "_away"
	for _, schema := range []string{home, away} {
		c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE SCHEMA "+schema), qt.IsNil)
	}
	defer func() {
		_ = conn.Writer().ExecuteSQL(ctx, "DROP EXTENSION IF EXISTS pg_trgm")
		for _, schema := range []string{home, away} {
			_ = conn.Writer().ExecuteSQL(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		}
	}()

	// The starting point is deliberately an older version, installed out of
	// band: this test is about noticing a difference, so the difference has to
	// exist before Ptah is asked anything.
	//
	// The drop first is not paranoia. This suite connects to a database it
	// shares, and an extension is database-wide: a previous run that died
	// before its cleanup leaves one behind, and CREATE EXTENSION then fails
	// with "already exists" for a reason that has nothing to do with what is
	// being tested.
	c.Assert(conn.Writer().ExecuteSQL(ctx, "DROP EXTENSION IF EXISTS pg_trgm"), qt.IsNil)
	c.Assert(conn.Writer().ExecuteSQL(ctx,
		"CREATE EXTENSION pg_trgm WITH SCHEMA "+home+" VERSION '1.5'"), qt.IsNil)

	// 1. A raised pin is seen at all. This is the assertion the whole issue is
	// about: before it, the comparison reported nothing.
	declared := declaredExtension(c, "pg_trgm", pgextension.DesiredExtension{Schema: home, Version: "1.6"})
	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	raised := must.Must(schemadiff.CompareWithDialect(t.Context(), declared, live, platform.Postgres, must.Must(builtin.New())))
	c.Assert(extensionChanges(raised), qt.HasLen, 1)
	c.Assert(extensionChanges(raised)[0].Before.Version, qt.Equals, "1.5")
	c.Assert(extensionChanges(raised)[0].After.Version, qt.Equals, "1.6")
	c.Assert(extensionChanges(raised)[0].UpdatesVersion(), qt.IsTrue)

	// 2. It plans a statement the server accepts.
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		raised, platform.Postgres,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "ALTER EXTENSION")
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 3. Convergence. The same declaration against a freshly read database has
	// nothing left to do.
	settled, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	after := must.Must(schemadiff.CompareWithDialect(t.Context(), declared, settled, platform.Postgres, must.Must(builtin.New())))
	c.Assert(extensionChanges(after), qt.HasLen, 0)

	// 4. The schema move, which is refused for no target.
	moved := declaredExtension(c, "pg_trgm", pgextension.DesiredExtension{Schema: away, Version: "1.6"})
	move := must.Must(schemadiff.CompareWithDialect(t.Context(), moved, settled, platform.Postgres, must.Must(builtin.New())))
	c.Assert(extensionChanges(move), qt.HasLen, 1)
	c.Assert(extensionChanges(move)[0].Schema, qt.Equals, away)
	c.Assert(extensionChanges(move)[0].Before.Relocatable, qt.IsTrue)
	moveStatements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		move, platform.Postgres,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(moveStatements, "\n"), qt.Contains, "SET SCHEMA")
	for _, statement := range moveStatements {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	relocated, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(extensionChanges(must.Must(schemadiff.CompareWithDialect(t.Context(), moved, relocated, platform.Postgres, must.Must(builtin.New())))), qt.HasLen, 0)
}

// TestExtensionMovesIntoASchemaThePlanCreatesLive pins that a move into a
// schema nothing created yet runs. The plan once wrote ALTER EXTENSION ...
// SET SCHEMA ahead of every schema precondition and created no schema for a
// move, and the server answered `schema "..." does not exist` (SQLSTATE
// 3F000), measured on PostgreSQL 18.
func TestExtensionMovesIntoASchemaThePlanCreatesLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	home := fmt.Sprintf("ptah_ext_%d", time.Now().UnixNano())
	away := home + "_new"
	c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE SCHEMA "+home), qt.IsNil)
	defer func() {
		_ = conn.Writer().ExecuteSQL(ctx, "DROP EXTENSION IF EXISTS pg_trgm")
		for _, schema := range []string{home, away} {
			_ = conn.Writer().ExecuteSQL(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		}
	}()
	// The drop first for the reason TestExtensionVersionAndSchemaConvergeLive
	// gives: the extension is database-wide and the database is shared.
	c.Assert(conn.Writer().ExecuteSQL(ctx, "DROP EXTENSION IF EXISTS pg_trgm"), qt.IsNil)
	c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE EXTENSION pg_trgm WITH SCHEMA "+home), qt.IsNil)

	moved := declaredExtension(c, "pg_trgm", pgextension.DesiredExtension{Schema: away})
	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	move := must.Must(schemadiff.CompareWithDialect(t.Context(), moved, live, platform.Postgres, must.Must(builtin.New())))
	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), move, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, `CREATE SCHEMA IF NOT EXISTS "`+away+`"`)
	c.Assert(statements[1], qt.Contains, `SET SCHEMA "`+away+`"`)
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	relocated, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), moved, relocated, platform.Postgres, must.Must(builtin.New())))
	c.Assert(extensionChanges(settled), qt.HasLen, 0)
}

// TestExtensionAlterationRefusalsMatchTheServerLive pins that the two shapes
// the planner refuses are ones the server refuses too.
//
// Without this the refusals are only this planner's opinion, and an opinion
// that turned out to be wrong would be a capability withheld for no reason.
func TestExtensionAlterationRefusalsMatchTheServerLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schema := fmt.Sprintf("ptah_extref_%d", time.Now().UnixNano())
	c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE SCHEMA "+schema), qt.IsNil)
	defer func() {
		_ = conn.Writer().ExecuteSQL(ctx, "DROP EXTENSION IF EXISTS pg_trgm")
		_ = conn.Writer().ExecuteSQL(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	}()
	c.Assert(conn.Writer().ExecuteSQL(ctx, "DROP EXTENSION IF EXISTS pg_trgm"), qt.IsNil)
	c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE EXTENSION pg_trgm VERSION '1.6'"), qt.IsNil)

	t.Run("a downgrade has no update path", func(t *testing.T) {
		c := qt.New(t)

		err := conn.Writer().ExecuteSQL(ctx, "ALTER EXTENSION pg_trgm UPDATE TO '1.5'")
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, "no update path")
	})

	t.Run("a fixed extension does not support SET SCHEMA", func(t *testing.T) {
		c := qt.New(t)

		// plpgsql is installed in every database and reports
		// extrelocatable = false, which is what the planner reads to decide.
		err := conn.Writer().ExecuteSQL(ctx, "ALTER EXTENSION plpgsql SET SCHEMA "+schema)
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, "does not support SET SCHEMA")
	})

	t.Run("the read reports which extension may move", func(t *testing.T) {
		c := qt.New(t)

		live, readErr := conn.Reader().ReadSchemaContext(ctx)
		c.Assert(readErr, qt.IsNil)
		byName := observedExtensions(c, live.FeatureObjects)
		c.Assert(byName["pg_trgm"].Relocatable, qt.IsTrue)
		c.Assert(byName["plpgsql"].Relocatable, qt.IsFalse)
	})
}
