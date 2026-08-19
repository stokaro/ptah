//go:build integration

package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

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
	declared := &goschema.Database{Extensions: []goschema.Extension{{
		Name: "pg_trgm", Schema: home, Version: "1.6",
	}}}
	live, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	raised := schemadiff.CompareWithDialect(declared, live, platform.Postgres)
	c.Assert(raised.ExtensionsModified, qt.HasLen, 1)
	c.Assert(raised.ExtensionsModified[0].FromVersion, qt.Equals, "1.5")
	c.Assert(raised.ExtensionsModified[0].ToVersion, qt.Equals, "1.6")

	// 2. It plans a statement the server accepts.
	statements, err := planner.GenerateSchemaDiffSQLStatements(raised, declared, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "ALTER EXTENSION")
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 3. Convergence. The same declaration against a freshly read database has
	// nothing left to do.
	settled, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	after := schemadiff.CompareWithDialect(declared, settled, platform.Postgres)
	c.Assert(after.ExtensionsModified, qt.HasLen, 0)
	c.Assert(after.ExtensionsAdded, qt.HasLen, 0)
	c.Assert(after.ExtensionsRemoved, qt.HasLen, 0)

	// 4. The schema move, which used to be refused for every target.
	moved := &goschema.Database{Extensions: []goschema.Extension{{
		Name: "pg_trgm", Schema: away, Version: "1.6",
	}}}
	move := schemadiff.CompareWithDialect(moved, settled, platform.Postgres)
	c.Assert(move.ExtensionsModified, qt.HasLen, 1)
	c.Assert(move.ExtensionsModified[0].Relocatable, qt.IsTrue)
	moveStatements, err := planner.GenerateSchemaDiffSQLStatements(move, moved, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(moveStatements, "\n"), qt.Contains, "SET SCHEMA")
	for _, statement := range moveStatements {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	relocated, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schemadiff.CompareWithDialect(moved, relocated, platform.Postgres).ExtensionsModified, qt.HasLen, 0)
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

		live, readErr := conn.Reader().ReadSchema()
		c.Assert(readErr, qt.IsNil)
		byName := map[string]bool{}
		for _, extension := range live.Extensions {
			byName[extension.Name] = extension.Relocatable
		}
		c.Assert(byName["pg_trgm"], qt.IsTrue)
		c.Assert(byName["plpgsql"], qt.IsFalse)
	})
}
