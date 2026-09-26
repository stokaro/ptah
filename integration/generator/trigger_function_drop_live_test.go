//go:build integration

package generator_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// A PostgreSQL trigger runs a function. A trigger declared with a body gets one
// Ptah generates for it, and a DROP TRIGGER takes that function with it. A
// trigger that executes a function the schema declares on its own must leave
// that function in place (stokaro/ptah#3722).
//
// The catalog reports the function every trigger runs, the generated one
// included, so the removal has to tell the two apart. Reading that report as
// "declared" for every trigger would keep the generated function and leave it
// behind with nothing running it; only a real catalog read produces the report,
// so this applies the plan and reads pg_proc. The server cannot see the other
// half, a DROP FUNCTION IF EXISTS for a generated name the shared trigger never
// had: that statement changes nothing on the server, and the unit tests in
// migration/generator pin its absence.

// triggerDropDeclaration is a table with two triggers: one with a body and one
// that executes a separately declared function. withTriggers false is the same
// schema with both triggers gone and the function kept.
func triggerDropDeclaration(withTriggers bool) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
		Functions: []schemamodel.Function{{
			Name: "normalize_email", Returns: "TRIGGER", Language: "plpgsql",
			Body: "BEGIN NEW.email := lower(NEW.email); RETURN NEW; END;",
		}},
	}
	if withTriggers {
		database.Triggers = []schemamodel.Trigger{
			{
				Name: "owned", Table: "users", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
				Body: "NEW.email := lower(NEW.email); RETURN NEW;",
			},
			{
				Name: "shared", Table: "users", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW",
				ExecuteFunction: "normalize_email",
			},
		}
	}
	return database
}

// triggerDropConnection opens a connection whose search_path is a schema of
// its own, so the bare names above land there rather than in the shared
// public schema, and removes the schema when the test ends.
func triggerDropConnection(c *qt.C, dbURL string) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	schemaName := fmt.Sprintf("ptah_trigger_drop_%d", time.Now().UnixNano())

	admin, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	_, err = admin.ExecContext(c.Context(), `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		c.Check(err, qt.IsNil)
	})

	scoped, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := scoped.Query()
	query.Set("search_path", schemaName)
	scoped.RawQuery = query.Encode()
	conn, err := dbschema.ConnectToDatabase(c.Context(), scoped.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn, schemaName
}

// functionsIn lists the routines a schema holds, by name.
func functionsIn(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), `
		SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = $1 ORDER BY p.proname`, schemaName)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestPostgresLiveTriggerDropTakesOnlyTheGeneratedFunction applies a schema with
// both triggers, then plans and applies the change that removes them. The
// generated function goes with its trigger; the declared one stays.
func TestPostgresLiveTriggerDropTakesOnlyTheGeneratedFunction(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	conn, schemaName := triggerDropConnection(c, dbURL)

	created, err := renderer.GetOrderedCreateStatements(triggerDropDeclaration(true), platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range created {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	c.Assert(functionsIn(c, conn, schemaName), qt.DeepEquals,
		[]string{"normalize_email", "ptah_trigger_users_owned"})

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	wanted := triggerDropDeclaration(false)
	diff := schemadiff.CompareWithDialect(wanted, live, platform.Postgres)
	c.Assert(diff.TriggersRemoved, qt.HasLen, 2)
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: wanted,
		CurrentSchema: live,
		Dialect:       platform.Postgres,
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	c.Assert(err, qt.IsNil)
	forward, err := planner.GenerateSchemaDiffSQLStatements(plan.Forward.Diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range forward {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	c.Assert(functionsIn(c, conn, schemaName), qt.DeepEquals, []string{"normalize_email"},
		qt.Commentf("the generated function went with its trigger and the declared one stayed; forward:\n%v", forward))
}
