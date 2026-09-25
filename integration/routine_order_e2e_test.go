//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlschema"
)

// PostgreSQL resolves a routine's signature, and a LANGUAGE sql body, when the
// routine is created. So a plan that creates a routine before the type or the
// relation it names is refused at that statement, and one that creates it
// after a table whose column default calls it is refused at the table. Each
// schema below is one of those shapes; stokaro/ptah#3602 was the first.

// routineReadingATable is the schema stokaro/ptah#3602 was filed with.
const routineReadingATable = `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION order_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM orders $$;`

// routineBetweenTwoViews has a routine read one view and another view call it.
const routineBetweenTwoViews = `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE VIEW big AS SELECT id, total FROM orders WHERE total > 100;
CREATE FUNCTION big_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM big $$;
CREATE VIEW summary AS SELECT big_count() AS n;`

// routineOrderSchemas are applied to an empty database and must converge.
var routineOrderSchemas = []struct {
	name string
	sql  string
}{
	{
		name: "a LANGUAGE sql body reads a table",
		sql:  routineReadingATable,
	},
	{
		name: "a SQL-standard body reads a table",
		sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION order_total(order_id bigint) RETURNS integer LANGUAGE sql STABLE RETURN (SELECT total FROM orders WHERE id = order_id);`,
	},
	{
		name: "a LANGUAGE sql body calls a routine that reads a table",
		sql: routineReadingATable + `
CREATE FUNCTION has_orders() RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT order_count() > 0 $$;`,
	},
	{
		name: "a signature returns a table's row type",
		sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION big_orders() RETURNS SETOF orders LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN QUERY SELECT * FROM orders WHERE total > 100; END $$;`,
	},
	{
		name: "a signature returns an enum a column default calls it for",
		sql: `CREATE TYPE mood AS ENUM ('happy', 'sad');
CREATE FUNCTION default_mood() RETURNS mood LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN 'happy'; END $$;
CREATE TABLE people (id bigint PRIMARY KEY, feeling mood NOT NULL DEFAULT default_mood());`,
	},
	{
		name: "a column default calls a routine that names nothing",
		sql: `CREATE FUNCTION next_code() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT md5(random()::text) $$;
CREATE TABLE codes (id bigint PRIMARY KEY, code text NOT NULL DEFAULT next_code());`,
	},
	{
		name: "a column default calls a PL/pgSQL routine reading another table",
		sql: `CREATE TABLE rates (code text PRIMARY KEY, pct integer NOT NULL);
CREATE FUNCTION default_pct() RETURNS integer LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN coalesce((SELECT pct FROM rates WHERE code = 'std'), 0); END $$;
CREATE TABLE invoices (id bigint PRIMARY KEY, pct integer NOT NULL DEFAULT default_pct());`,
	},
	{
		name: "a domain CHECK calls a routine that names nothing",
		sql: `CREATE FUNCTION positive(v integer) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT v > 0 $$;
CREATE DOMAIN pct AS integer CHECK (positive(VALUE));
CREATE TABLE items (id bigint PRIMARY KEY, share pct NOT NULL);`,
	},
	{
		name: "a routine reads a view another view reads it through",
		sql:  routineBetweenTwoViews,
	},
	{
		name: "a policy calls a routine reading another table",
		sql: `CREATE TABLE members (user_name text PRIMARY KEY);
CREATE TABLE docs (id bigint PRIMARY KEY, owner text NOT NULL);
CREATE FUNCTION is_member(name text) RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT EXISTS (SELECT 1 FROM members WHERE user_name = name) $$;
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_member ON docs USING (is_member(owner));`,
	},
}

// writeRoutineOrderSchema writes sql to a schema file of its own.
func writeRoutineOrderSchema(c *qt.C, sql string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql+"\n"), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyCreatesRoutinesAfterWhatTheyNameLive applies each schema to
// an empty PostgreSQL database with the native apply, and asks again: the
// second plan is empty, so the first one created everything it declared.
func TestSchemaApplyCreatesRoutinesAfterWhatTheyNameLive(t *testing.T) {
	for _, test := range routineOrderSchemas {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			schema := writeRoutineOrderSchema(c, test.sql)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRehearsesARoutineReadingATableLive is the shape the issue
// was filed from: ptah-compat rehearses the plan on the dev database, first
// rebuilding the target's current state there. Both the rebuild and the plan
// create a LANGUAGE sql routine, so both have to order it after its table.
func TestSchemaApplyRehearsesARoutineReadingATableLive(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	dev, _ := scratchReplayDatabase(c)
	schema := writeRoutineOrderSchema(c, routineReadingATable)

	out, err := runCompatVerb("schema", "apply", "--url", target, "--to", "file://"+schema, "--dev-url", dev, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	grown := writeRoutineOrderSchema(c, routineReadingATable+"\nCREATE TABLE notes (id bigint PRIMARY KEY);")
	out, err = runCompatVerb("schema", "apply", "--url", target, "--to", "file://"+grown, "--dev-url", dev, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
}

// TestSchemaApplyReplacesARoutineAfterTheColumnItReadsLive pins the replaced
// routine: a new body that reads a column the same plan adds is created after
// the column.
func TestSchemaApplyReplacesARoutineAfterTheColumnItReadsLive(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	runPtahNative(c, "schema", "apply", "--db-url", target,
		"--schema-file", writeRoutineOrderSchema(c, routineReadingATable), "--auto-approve")
	changed := writeRoutineOrderSchema(c, `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL, discount integer NOT NULL DEFAULT 0);
CREATE FUNCTION order_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM orders WHERE discount = 0 $$;`)

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", changed, "--auto-approve")

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", changed, "--dry-run")
	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestSchemaRenderCreatesRoutinesInAnOrderPostgreSQLAcceptsLive runs the
// rendered DDL of each schema on an empty database. A render creates everything
// it declares and places routines by the rule a plan uses, so a routine comes
// after the types and relations it names and before the domain, the column
// default or the view that calls it.
func TestSchemaRenderCreatesRoutinesInAnOrderPostgreSQLAcceptsLive(t *testing.T) {
	for _, test := range routineOrderSchemas {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.IsNil)
			statements, err := renderer.GetOrderedCreateStatements(&database, platform.Postgres)
			c.Assert(err, qt.IsNil)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			for _, statement := range statements {
				_, execErr := conn.ExecContext(c.Context(), statement)
				c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
			}

			out := runPtahNative(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeRoutineOrderSchema(c, test.sql), "--dry-run")
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyCreatesARoutineAfterTheTypeItRebuildsLive pins a rebuilt type
// as a created one: a domain whose base type changes is dropped and created
// again, so a new routine taking it has to follow the rebuild. Created first,
// the routine holds the old domain and the drop is refused.
func TestSchemaApplyCreatesARoutineAfterTheTypeItRebuildsLive(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	runPtahNative(c, "schema", "apply", "--db-url", target,
		"--schema-file", writeRoutineOrderSchema(c, "CREATE DOMAIN pct AS integer CHECK (VALUE >= 0);"), "--auto-approve")
	rebuilt := writeRoutineOrderSchema(c, `CREATE DOMAIN pct AS numeric CHECK (VALUE >= 0);
CREATE FUNCTION half(p pct) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$ SELECT p / 2 $$;`)

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", rebuilt, "--auto-approve")

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", rebuilt, "--dry-run")
	c.Assert(out, qt.Contains, "Schema is synced")
}
