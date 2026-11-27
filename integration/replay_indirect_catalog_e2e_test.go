//go:build integration

package integration_test

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// indirectCatalogEngines are the engines that write through a view, keep
// rules and define operators and aggregates. CockroachDB has none of these,
// so it has a row only in the control.
var indirectCatalogEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "PostgreSQL 16", engine: dbtarget.PostgreSQL16},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// indirectRoleWrites are histories that give the role %[1]s CREATEDB through
// an object the history creates rather than by writing pg_authid itself, and
// through a name of pg_authid the database qualifies. Measured on PostgreSQL
// 18 with no trust, each replays and sets rolcreatedb where the guard reads
// only what a statement writes itself. The database's name is %[2]s.
var indirectRoleWrites = []struct {
	name, body, wantErr string
}{
	{
		name: "a view of pg_authid",
		body: "CREATE VIEW roles AS SELECT * FROM pg_authid;\n" +
			"UPDATE roles SET rolcreatedb = true WHERE rolname = '%[1]s';\n",
		wantErr: `rejects protected namespace "pg_catalog" mutation through the relation "roles" because`,
	},
	{
		name: "a view of a view of pg_authid",
		body: "CREATE VIEW roles AS SELECT * FROM pg_authid;\n" +
			"CREATE VIEW public.role_names AS SELECT oid, rolname, rolcreatedb FROM roles;\n" +
			"UPDATE public.role_names SET rolcreatedb = true WHERE rolname = '%[1]s';\n",
		wantErr: `rejects protected namespace "pg_catalog" mutation through the relation "role_names" because`,
	},
	{
		name: "a rule whose action writes through a view",
		body: "CREATE TABLE events (id integer);\n" +
			"CREATE VIEW roles AS SELECT * FROM pg_authid;\n" +
			"CREATE RULE grant_createdb AS ON INSERT TO events DO ALSO UPDATE roles SET rolcreatedb = true WHERE rolname = '%[1]s';\n" +
			"INSERT INTO events VALUES (1);\n",
		wantErr: `rejects protected namespace "pg_catalog" mutation through the relation "roles" because`,
	},
	{
		name:    "a name the database qualifies",
		body:    "UPDATE %[2]s.pg_catalog.pg_authid SET rolcreatedb = true WHERE rolname = '%[1]s';\n",
		wantErr: `rejects protected namespace "pg_catalog" mutation because`,
	},
}

// TestReplayIndirectCatalogWriteE2E_FailurePath replays each history with no
// trust and reads the role back: it still may not create a database.
// YugabyteDB refuses a write to pg_authid itself, so its rows show the guard
// refuses first.
func TestReplayIndirectCatalogWriteE2E_FailurePath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		for _, test := range indirectRoleWrites {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				h, dev := newPgDumpHistory(c, engine.engine)
				role := fmt.Sprintf("ptah_indirect_role_%d", time.Now().UnixNano())
				compatDriftExec(c, dbtarget.URL(c, engine.engine), "CREATE ROLE "+quoteE2EIdent(role)+" NOCREATEDB")
				c.Cleanup(func() { dropPgDumpRole(c, engine.engine, role) })

				got := pgDumpRefused(c, h, dev, fmt.Sprintf(test.body, role, quoteE2EIdent(devDatabaseName(c, dev))))

				c.Assert(got, qt.Contains, test.wantErr)
				c.Assert(pgDumpQuery[bool](c, dev, "SELECT rolcreatedb FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.IsFalse)
			})
		}
	}
}

// settingsViewEscape writes standard_conforming_strings off through a view
// of pg_settings, whose update rule calls set_config, and the next statement,
// one SELECT to the guard, creates the role %s on a server that reads its
// backslash as an escape.
const settingsViewEscape = "CREATE VIEW settings AS SELECT name, setting FROM pg_settings;\n" +
	"UPDATE settings SET setting = 'off' WHERE name = 'standard_conforming_strings';\n" +
	"SELECT 'x\\' AS a, ' ; CREATE ROLE %s; -- ';\n"

// TestReplaySettingsViewE2E_FailurePath replays settingsViewEscape with no
// trust and reads the server's roles back: the role the escape would create
// does not exist. PostgreSQL 19 refuses standard_conforming_strings off
// itself; PostgreSQL 16, PostgreSQL 18 and YugabyteDB run it.
func TestReplaySettingsViewE2E_FailurePath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			role := fmt.Sprintf("ptah_settings_view_%d", time.Now().UnixNano())
			c.Cleanup(func() { dropPgDumpRole(c, engine.engine, role) })

			got := pgDumpRefused(c, h, dev, fmt.Sprintf(settingsViewEscape, role))

			c.Assert(got, qt.Contains, `rejects protected namespace "pg_catalog" mutation through the relation "settings" because`)
			c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.Equals, 0)
		})
	}
}

// indirectSettingEscape names set_config as the state function of an
// aggregate, so running the aggregate turns standard_conforming_strings off
// with arguments the statement does not spell out, and the next statement,
// one SELECT to the guard, creates the role %s on a server that reads its
// backslash as an escape.
const indirectSettingEscape = "CREATE AGGREGATE public.set_all(text, boolean) " +
	"(SFUNC = set_config, STYPE = text, INITCOND = 'standard_conforming_strings');\n" +
	"SELECT public.set_all('off', false);\n" +
	"SELECT 'x\\' AS a, ' ; CREATE ROLE %s; -- ';\n"

// TestReplayReferencedControlFunctionE2E_FailurePath replays, with no trust,
// an aggregate and an operator that name a function the guard refuses as a
// call. It reads the server's roles back after the aggregate: the role the
// escape would create does not exist. The operator would terminate the
// backend it names, so its row asserts the refusal alone.
func TestReplayReferencedControlFunctionE2E_FailurePath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		t.Run(engine.name+"/aggregate", func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			role := fmt.Sprintf("ptah_indirect_escape_%d", time.Now().UnixNano())
			c.Cleanup(func() { dropPgDumpRole(c, engine.engine, role) })

			got := pgDumpRefused(c, h, dev, fmt.Sprintf(indirectSettingEscape, role))

			c.Assert(got, qt.Contains, `rejects cluster control function "set_config" named by SFUNC because`)
			c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.Equals, 0)
		})
		t.Run(engine.name+"/operator", func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)

			got := pgDumpRefused(c, h, dev,
				"CREATE OPERATOR public.!!! (FUNCTION = pg_terminate_backend, LEFTARG = integer, RIGHTARG = bigint);\n"+
					"SELECT 0 OPERATOR(public.!!!) 0::bigint;\n")

			c.Assert(got, qt.Contains, `rejects cluster control function "pg_terminate_backend" named by FUNCTION because`)
		})
	}
}

// describedFunctionComment counts the descriptions pg_description holds for
// pg_catalog.now(), a row of the dev database's own catalog.
const describedFunctionComment = "SELECT count(*) FROM pg_catalog.pg_description WHERE objoid = 'pg_catalog.now()'::pg_catalog.regprocedure"

// TestReplayTriggerArgumentE2E_FailurePath replays, with no trust, a trigger
// of the refint extension whose arguments name pg_description. Measured on
// PostgreSQL 18, check_foreign_key runs them as a DELETE when a row of the
// trigger's table is deleted, and the history below deletes the description
// of pg_catalog.now(). The test reads that description back.
func TestReplayTriggerArgumentE2E_FailurePath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			before := pgDumpQuery[int](c, dev, describedFunctionComment)

			got := pgDumpRefused(c, h, dev, "CREATE EXTENSION refint;\n"+
				"CREATE TABLE events (id oid);\n"+
				"CREATE TRIGGER events_fk AFTER DELETE ON events FOR EACH ROW EXECUTE FUNCTION "+
				"check_foreign_key(1, 'cascade', 'id', 'pg_catalog.pg_description', 'objoid');\n"+
				"INSERT INTO events VALUES ('pg_catalog.now()'::regprocedure);\n"+
				"DELETE FROM events;\n")

			c.Assert(got, qt.Contains,
				`rejects protected namespace "pg_catalog" mutation through the trigger argument 'pg_catalog.pg_description' because`)
			c.Assert(before, qt.Equals, 1)
			c.Assert(pgDumpQuery[int](c, dev, describedFunctionComment), qt.Equals, before)
		})
	}
}

// creationPathEscape drops the first schema of a search path that lists
// pg_catalog after it, so the server creates the next objects in pg_catalog:
// measured on PostgreSQL 18, the history leaves pg_catalog.d, pg_catalog.e
// and pg_catalog.||| behind, and the cleanup recreates public.
const creationPathEscape = "DROP SCHEMA public CASCADE;\n" +
	"CREATE DOMAIN d AS int;\n" +
	"CREATE OPERATOR ||| (FUNCTION = int4pl, LEFTARG = int, RIGHTARG = int);\n" +
	"CREATE TYPE e AS ENUM ('a');\n"

// createdInCatalogObjects counts the objects creationPathEscape would put in
// pg_catalog.
const createdInCatalogObjects = `SELECT (SELECT count(*) FROM pg_catalog.pg_type
	WHERE typnamespace = 'pg_catalog'::regnamespace AND typname IN ('d', 'e')) +
	(SELECT count(*) FROM pg_catalog.pg_operator
	WHERE oprnamespace = 'pg_catalog'::regnamespace AND oprname = '|||')`

// TestReplayCreationPathE2E_FailurePath replays creationPathEscape with no
// trust on a dev URL whose search path is public, pg_catalog, and reads
// pg_catalog back: it holds none of the objects.
func TestReplayCreationPathE2E_FailurePath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)

			got := pgDumpRefused(c, h, withSearchPath(c, dev, "public,pg_catalog"), creationPathEscape)

			c.Assert(got, qt.Contains,
				`rejects protected namespace "pg_catalog" mutation through the unqualified name "d" because`)
			c.Assert(got, qt.Contains, "qualify the name with the schema of the object it means")
			c.Assert(pgDumpQuery[int](c, dev, createdInCatalogObjects), qt.Equals, 0)
		})
	}
}

// indirectCatalogControl reads the catalog through views, writes through a
// view of a user's table, and defines an operator, an aggregate, a rule and a
// trigger over builtin functions and the user's tables. It drops the operator
// itself: the dev database cleanup drops no operator, and refuses to drop a
// schema that holds one.
const indirectCatalogControl = `CREATE SCHEMA app;
CREATE TABLE app.events (id integer PRIMARY KEY, active boolean NOT NULL DEFAULT true, body text, tsv tsvector);
CREATE TABLE app.log (id integer);
CREATE VIEW app.relations AS SELECT relname FROM pg_catalog.pg_class;
CREATE VIEW app.kinds AS SELECT c.relkind, count(*) AS n
	FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace GROUP BY c.relkind;
CREATE VIEW app.active AS SELECT id, active FROM app.events WHERE active;
CREATE OPERATOR app.=== (FUNCTION = int4eq, LEFTARG = integer, RIGHTARG = integer);
CREATE AGGREGATE app.total(integer) (SFUNC = int4pl, STYPE = integer, INITCOND = '0');
CREATE RULE log_event AS ON INSERT TO app.events DO ALSO INSERT INTO app.log VALUES (new.id);
CREATE TRIGGER events_tsv BEFORE INSERT OR UPDATE ON app.events FOR EACH ROW
	EXECUTE FUNCTION tsvector_update_trigger(tsv, 'pg_catalog.english', body);
INSERT INTO app.events (id, body) VALUES (1, 'x');
UPDATE app.active SET active = false WHERE id = 1;
SELECT app.total(id), count(*) FILTER (WHERE id OPERATOR(app.===) 1) FROM app.events;
SELECT relname FROM app.relations LIMIT 1;
DROP OPERATOR app.=== (integer, integer);
`

// indirectCatalogCockroachControl is the part of indirectCatalogControl
// CockroachDB runs: it has no rules, operators or aggregates, and writes
// through no view.
const indirectCatalogCockroachControl = `CREATE SCHEMA app;
CREATE TABLE app.events (id INT8 PRIMARY KEY, active BOOL NOT NULL DEFAULT true);
CREATE VIEW app.relations AS SELECT relname FROM pg_catalog.pg_class;
CREATE VIEW app.kinds AS SELECT c.relkind, count(*) AS n
	FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace GROUP BY c.relkind;
INSERT INTO app.events (id) VALUES (1);
SELECT relname FROM app.relations LIMIT 1;
`

// TestReplayIndirectCatalogE2E_HappyPath is the control for the tests above:
// views that read the catalog, a write through a view of the user's table,
// and objects over builtin functions replay with no trust on every engine,
// and the dev database ends without the schema the history created.
func TestReplayIndirectCatalogE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		body   string
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL, body: indirectCatalogControl},
		{name: "PostgreSQL 16", engine: dbtarget.PostgreSQL16, body: indirectCatalogControl},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB, body: indirectCatalogControl},
		{name: "CockroachDB", engine: dbtarget.CockroachDB, body: indirectCatalogCockroachControl},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, test.engine)
			writePgDumpMigration(c, h, 1, "indirect", test.body, "DROP SCHEMA app CASCADE;\n")

			got := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)

			c.Assert(got, qt.Contains, "migration SQL validated on dev database")
			c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'app'"), qt.Equals, 0)
		})
	}
}

// TestReplayCreationPathE2E_HappyPath is the control for the creation path: the
// history with every name qualified replays on the same search path, and
// pg_catalog ends without the objects. The history drops the operator, as
// indirectCatalogControl does.
func TestReplayCreationPathE2E_HappyPath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			writePgDumpMigration(c, h, 1, "qualified", "CREATE SCHEMA app;\n"+
				"CREATE DOMAIN app.d AS int;\n"+
				"CREATE OPERATOR app.||| (FUNCTION = int4pl, LEFTARG = int, RIGHTARG = int);\n"+
				"CREATE TYPE app.e AS ENUM ('a');\n"+
				"DROP OPERATOR app.||| (int, int);\n",
				"DROP SCHEMA app CASCADE;\n")

			got := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", withSearchPath(c, dev, "public,pg_catalog"))

			c.Assert(got, qt.Contains, "migration SQL validated on dev database")
			c.Assert(pgDumpQuery[int](c, dev, createdInCatalogObjects), qt.Equals, 0)
		})
	}
}

// devDatabaseName is the database the dev URL rawURL selects.
func devDatabaseName(c *qt.C, rawURL string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	return strings.TrimPrefix(parsed.Path, "/")
}
