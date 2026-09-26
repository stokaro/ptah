//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// triggerReadDocument is a table, a trigger function and a trigger on two
// events with a condition, in schemaName. Every PostgreSQL-family engine
// accepts it: CockroachDB refuses a condition spelled NEW.a, `no data source
// matches prefix: new`, and takes (NEW).a, which PostgreSQL and YugabyteDB
// take too.
func triggerReadDocument(schemaName string) string {
	q := `"` + schemaName + `".`
	return `CREATE TABLE ` + q + `w (id INT PRIMARY KEY, a INT);
CREATE FUNCTION ` + q + `wf() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER w_t BEFORE UPDATE OR INSERT ON ` + q + `w FOR EACH ROW WHEN ((NEW).a > 0) EXECUTE FUNCTION ` + q + `wf();`
}

// TestTriggerRead_LiveOnEveryPostgresFamilyEngine reads a trigger with a
// condition back from each engine and compares the declaration with it
// (stokaro/ptah#3707).
//
// Reading is the first thing that has to hold. CockroachDB 25.4 has no
// pg_get_triggerdef, and a reader that names it fails the whole statement, so
// reading any schema there failed. What the read reports then depends on the
// function: an engine with it reports the condition in its own spelling, and
// one without it reports none. The comparison has to agree with both, which
// is the second half: the declaration applied through the server compares as
// equal, with nothing to replace.
func TestTriggerRead_LiveOnEveryPostgresFamilyEngine(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
		// when is the condition as the engine prints it where it has
		// pg_get_triggerdef.
		when string
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL, when: "(new.a > 0)"},
		{name: "CockroachDB", engine: dbtarget.CockroachDB, when: "(new).a > 0:::INT8"},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB, when: "(new.a > 0)"},
	}
	for _, test := range engines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			defer cancel()
			conn, schemaName := prepareTriggerReadFixture(c, ctx, test.engine)
			caps := conn.Info().Capabilities
			wantWhen := map[bool]string{true: test.when, false: ""}[caps.Has(capability.CatalogTriggerDefinitions)]

			live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

			c.Assert(err, qt.IsNil)
			c.Assert(live.Triggers, qt.HasLen, 1)
			trigger := live.Triggers[0]
			c.Assert([]string{trigger.Name, trigger.Event, trigger.ForEach, trigger.When},
				qt.DeepEquals, []string{"w_t", "INSERT OR UPDATE", "ROW", wantWhen})

			declared, _, err := sqlschema.Read([]byte(triggerReadDocument(schemaName)), conn.Info().Dialect)
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabase(ctx, conn, &declared, live, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TriggersAdded, qt.HasLen, 0)
			c.Assert(diff.TriggersModified, qt.HasLen, 0)
			c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
		})
	}
}

// prepareTriggerReadFixture creates a schema of its own holding the document
// above, and drops it when the test ends.
func prepareTriggerReadFixture(c *qt.C, ctx context.Context, engine dbtarget.Engine) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	schemaName := fmt.Sprintf("ptah_trigread_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	})
	_, err = conn.ExecContext(ctx, triggerReadDocument(schemaName))
	c.Assert(err, qt.IsNil)
	return conn, schemaName
}
