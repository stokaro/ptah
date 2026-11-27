//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// replacedViewChain gives the role %s CREATEDB through a view over a view the
// history replaces after the outer view exists: the outer view reaches no
// catalog relation when it is created.
const replacedViewChain = "CREATE TABLE harmless (rolname name, rolcreatedb boolean);\n" +
	"CREATE VIEW inner_v AS SELECT rolname, rolcreatedb FROM harmless;\n" +
	"CREATE VIEW outer_v AS SELECT * FROM inner_v;\n" +
	"CREATE OR REPLACE VIEW inner_v AS SELECT rolname, rolcreatedb FROM pg_authid;\n" +
	"UPDATE outer_v SET rolcreatedb = true WHERE rolname = '%s';\n"

// laterTriggerView deletes the description of pg_catalog.now() through a
// refint trigger whose argument names a view the history creates after the
// trigger.
const laterTriggerView = "CREATE EXTENSION refint;\n" +
	"CREATE TABLE events (id oid);\n" +
	"CREATE TRIGGER events_fk AFTER DELETE ON events FOR EACH ROW EXECUTE FUNCTION " +
	"check_foreign_key(1, 'cascade', 'id', 'descs', 'objoid');\n" +
	"CREATE VIEW descs AS SELECT objoid FROM pg_catalog.pg_description;\n" +
	"INSERT INTO events VALUES ('pg_catalog.now()'::regprocedure);\n" +
	"DELETE FROM events;\n"

// TestReplayWriteTimeReachE2E_FailurePath replays, with no trust, a write
// through a view whose view the history replaces after it, and a delete
// whose trigger names a view created after the trigger. Measured on
// PostgreSQL 18, a guard that judges a relation when it is created accepts
// both, the first sets rolcreatedb and the second deletes the description.
// The test reads the role and the description back.
func TestReplayWriteTimeReachE2E_FailurePath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		t.Run(engine.name+"/a replaced view under a view", func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			role := fmt.Sprintf("ptah_write_time_role_%d", time.Now().UnixNano())
			compatDriftExec(c, dbtarget.URL(c, engine.engine), "CREATE ROLE "+quoteE2EIdent(role)+" NOCREATEDB")
			c.Cleanup(func() { dropPgDumpRole(c, engine.engine, role) })

			got := pgDumpRefused(c, h, dev, fmt.Sprintf(replacedViewChain, role))

			c.Assert(got, qt.Contains, `rejects protected namespace "pg_catalog" mutation through the relation "outer_v" because`)
			c.Assert(pgDumpQuery[bool](c, dev, "SELECT rolcreatedb FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.IsFalse)
		})
		t.Run(engine.name+"/a trigger created before its argument's view", func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			before := pgDumpQuery[int](c, dev, describedFunctionComment)

			got := pgDumpRefused(c, h, dev, laterTriggerView)

			c.Assert(got, qt.Contains, `rejects protected namespace "pg_catalog" mutation through the relation "events" because`)
			c.Assert(before, qt.Equals, 1)
			c.Assert(pgDumpQuery[int](c, dev, describedFunctionComment), qt.Equals, before)
		})
	}
}

// triggerArgumentSetting turns standard_conforming_strings off from the
// column argument of a refint trigger, which check_foreign_key runs as SQL
// when a row of parent is deleted, and the next statement, one SELECT to the
// guard, creates the role %s on a server that reads its backslash as an
// escape.
const triggerArgumentSetting = "CREATE EXTENSION refint;\n" +
	"CREATE TABLE parent (id int);\n" +
	"CREATE TABLE child (pid int);\n" +
	"CREATE TRIGGER parent_fk AFTER DELETE ON parent FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'child', " +
	"'pid = pid AND pg_catalog.set_config(''standard_conforming_strings'',''off'',false) IS NOT NULL AND pid');\n" +
	"INSERT INTO parent VALUES (1);\n" +
	"INSERT INTO child VALUES (1);\n" +
	"DELETE FROM parent;\n" +
	"SELECT 'x\\' AS a, ' ; CREATE ROLE %s; -- ';\n"

// TestReplayTriggerArgumentCallE2E_FailurePath replays triggerArgumentSetting
// with no trust and reads the server's roles back: the role the escape would
// create does not exist. Measured on PostgreSQL 18 and YugabyteDB 2026.1, a
// guard that reads the argument only for the relations it names creates it.
func TestReplayTriggerArgumentCallE2E_FailurePath(t *testing.T) {
	for _, engine := range indirectCatalogEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			role := fmt.Sprintf("ptah_trigger_call_%d", time.Now().UnixNano())
			c.Cleanup(func() { dropPgDumpRole(c, engine.engine, role) })

			got := pgDumpRefused(c, h, dev, fmt.Sprintf(triggerArgumentSetting, role))

			c.Assert(got, qt.Contains, `rejects cluster control function "set_config" called by the trigger argument`)
			c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.Equals, 0)
		})
	}
}

// userTableBesideCatalogView writes a table of public while a schema the
// search path does not reach holds a view of pg_roles under its name, and
// then asserts on the server that the row landed in public.users: the last
// statement divides by the rows it finds there, and fails on none.
const userTableBesideCatalogView = "CREATE SCHEMA admin;\n" +
	"CREATE VIEW admin.users AS SELECT rolname FROM pg_catalog.pg_roles;\n" +
	"CREATE TABLE users (id int, name text);\n" +
	"INSERT INTO users VALUES (1, 'alice');\n" +
	"SELECT 1 / (SELECT count(*)::int FROM public.users WHERE name = 'alice');\n"

// filteredUpdatableView writes through a view of app.docs whose filter reads
// pg_roles, and asserts on the server that the row landed in app.docs.
const filteredUpdatableView = "CREATE SCHEMA app;\n" +
	"CREATE TABLE app.docs (id int, owner name);\n" +
	"CREATE VIEW app.member_docs AS SELECT * FROM app.docs WHERE owner IN (SELECT rolname FROM pg_catalog.pg_roles);\n" +
	"INSERT INTO app.member_docs VALUES (1, current_user);\n" +
	"SELECT 1 / (SELECT count(*)::int FROM app.docs WHERE id = 1);\n"

// TestReplayUserTableWriteE2E_HappyPath replays, with no trust, a write to a
// table named like a catalog view of another schema, and a write through an
// automatically updatable view whose filter reads the catalog. Each history
// reads the row back from the user's table on the server before the run
// ends, and the dev database ends without the schema the history created.
// CockroachDB writes through no view, so it runs the first history alone.
func TestReplayUserTableWriteE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		body   string
	}{
		{name: "PostgreSQL/a table beside a catalog view", engine: dbtarget.PostgreSQL, body: userTableBesideCatalogView},
		{name: "PostgreSQL 16/a table beside a catalog view", engine: dbtarget.PostgreSQL16, body: userTableBesideCatalogView},
		{name: "YugabyteDB/a table beside a catalog view", engine: dbtarget.YugabyteDB, body: userTableBesideCatalogView},
		{name: "CockroachDB/a table beside a catalog view", engine: dbtarget.CockroachDB, body: userTableBesideCatalogView},
		{name: "PostgreSQL/an updatable view with a catalog filter", engine: dbtarget.PostgreSQL, body: filteredUpdatableView},
		{name: "PostgreSQL 16/an updatable view with a catalog filter", engine: dbtarget.PostgreSQL16, body: filteredUpdatableView},
		{name: "YugabyteDB/an updatable view with a catalog filter", engine: dbtarget.YugabyteDB, body: filteredUpdatableView},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, test.engine)
			writePgDumpMigration(c, h, 1, "user_table", test.body, "SELECT 1;\n")

			got := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)

			c.Assert(got, qt.Contains, "migration SQL validated on dev database")
			c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname IN ('admin', 'app')"), qt.Equals, 0)
		})
	}
}

// serverFileRead copies a file of the server's host into a table of the dev
// database, and asserts on the server that it read a line.
const serverFileRead = "CREATE TABLE host_files (line text);\n" +
	"COPY host_files FROM '/etc/hostname';\n" +
	"SELECT 1 / (SELECT count(*)::int FROM host_files);\n"

// TestReplayServerFileE2E_FailurePath replays, with no trust, a COPY from a
// file of the server's host and a call of pg_read_file: each is refused.
func TestReplayServerFileE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name, body, wantErr string
	}{
		{name: "COPY from a file", body: serverFileRead, wantErr: "rejects external COPY because"},
		{name: "pg_read_file", body: "SELECT pg_read_file('/etc/hostname');\n", wantErr: "rejects server file read because"},
	}
	for _, engine := range indirectCatalogEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				h, dev := newPgDumpHistory(c, engine.engine)

				got := pgDumpRefused(c, h, dev, test.body)

				c.Assert(got, qt.Contains, test.wantErr)
			})
		}
	}
}

// TestReplayServerFileE2E_HappyPath is the control: under --trust-replay-sql
// the COPY runs, and the server reads its host's file into the table.
func TestReplayServerFileE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.PostgreSQL)
	writePgDumpMigration(c, h, 1, "server_file", serverFileRead, "SELECT 1;\n")

	got := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev, "--trust-replay-sql")

	c.Assert(got, qt.Contains, "migration SQL validated on dev database")
}

// TestReplayCrossDatabaseE2E_FailurePath replays on CockroachDB, with no
// trust, a write to a table of another database named with two parts and
// with three, and reads the table back: it holds no row.
func TestReplayCrossDatabaseE2E_FailurePath(t *testing.T) {
	for _, form := range []string{"%s.t", "%s.public.t"} {
		t.Run(form, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, dbtarget.CockroachDB)
			other := newHistoryReplayDatabase(c, dbtarget.CockroachDB, "other")
			compatDriftExec(c, other, "CREATE TABLE t (id INT8)")
			body := "INSERT INTO " + fmt.Sprintf(form, devDatabaseName(c, other)) + " VALUES (1);\n"

			got := pgDumpRefused(c, h, dev, body)

			c.Assert(got, qt.Contains, "of another database because")
			c.Assert(pgDumpQuery[int](c, other, "SELECT count(*) FROM t"), qt.Equals, 0)
		})
	}
}

// TestReplayCrossDatabaseE2E_HappyPath is the control: under
// --trust-replay-sql the write runs, and the table of the other database
// holds the row, which is the measurement the refusal rests on.
func TestReplayCrossDatabaseE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.CockroachDB)
	other := newHistoryReplayDatabase(c, dbtarget.CockroachDB, "other")
	compatDriftExec(c, other, "CREATE TABLE t (id INT8)")
	writePgDumpMigration(c, h, 1, "other_database", "INSERT INTO "+devDatabaseName(c, other)+".t VALUES (1);\n", "SELECT 1;\n")

	got := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev, "--trust-replay-sql")

	c.Assert(got, qt.Contains, "migration SQL validated on dev database")
	c.Assert(pgDumpQuery[int](c, other, "SELECT count(*) FROM t"), qt.Equals, 1)
}
