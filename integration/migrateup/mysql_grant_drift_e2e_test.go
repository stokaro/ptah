//go:build integration

package migrateup_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests run `ptah migrations up --check-drift` on the MySQL family over
// a history that grants privileges, with a shadow database whose name differs
// from the target's. A one-database read names the database of each grant's
// object, so the replay's grants name the shadow database. They are the
// target's own grants, and the check must say so.

// mysqlGrantDriftRole names a role of the test's own and drops it from the
// target's server and from the shadow's when the test ends. A role belongs to
// the server, so the history creates it on both and no reset of a database
// removes it.
func mysqlGrantDriftRole(c *qt.C, engine mysqlViewDriftEngine) string {
	c.Helper()
	role := fmt.Sprintf("ptah_gdrift_%d", time.Now().UnixNano())
	for _, server := range []dbtarget.Engine{engine.admin, engine.shadowServer} {
		conn, err := sql.Open("mysql", dbtarget.DriverDSN(c, server))
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() {
			_, dropErr := conn.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
			c.Check(dropErr, qt.IsNil)
			c.Check(conn.Close(), qt.IsNil)
		})
	}
	return role
}

// mysqlGrantDriftFiles is a history that creates a table and a view and
// grants role SELECT on each of them and SHOW VIEW on the whole database,
// which `ON *` names as the database the statement runs in.
func mysqlGrantDriftFiles(role string) map[string]string {
	return map[string]string{
		"0000000001_items.up.sql": "-- atlas:txmode none\n" +
			"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
			"CREATE VIEW item_ids AS SELECT id FROM items;\n" +
			"CREATE ROLE IF NOT EXISTS " + role + ";\n" +
			"GRANT SELECT ON items TO " + role + ";\n" +
			"GRANT SELECT ON item_ids TO " + role + ";\n" +
			"GRANT SHOW VIEW ON * TO " + role + ";\n",
		"0000000001_items.down.sql": "DROP VIEW item_ids;\nDROP TABLE items;\n",
	}
}

func TestMySQLGrantDriftCheckE2E_HappyPath(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			role := mysqlGrantDriftRole(c, engine)

			result, document := mysqlViewDriftRun(c, engine, mysqlGrantDriftFiles(role))

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
			c.Assert(document.DriftCheck, qt.IsNotNil)
			c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", document.DriftCheck.Findings))
			c.Assert(document.DriftCheck.Findings, qt.HasLen, 0)
		})
	}
}

// A grant the target lost is still drift, and so is one the target holds and
// the history does not write. Without these the happy path would pass for a
// check that ignores grants.
func TestMySQLGrantDriftCheckE2E_FailurePath(t *testing.T) {
	for _, engine := range mysqlViewDriftEngines {
		t.Run(engine.name+"/a grant revoked on the target", func(t *testing.T) {
			c := qt.New(t)
			role := mysqlGrantDriftRole(c, engine)

			result, document := mysqlViewDriftRun(c, engine, mysqlGrantDriftFiles(role), "REVOKE SELECT ON items FROM "+role)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(document.DriftCheck, qt.IsNotNil)
			c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(document.DriftCheck.Findings, qt.HasLen, 1, qt.Commentf("%+v", document.DriftCheck.Findings))
			c.Assert(document.DriftCheck.Findings[0].Kind, qt.Equals, "missing_grant")
			c.Assert(document.DriftCheck.Findings[0].Object, qt.Equals, role+" SELECT ON TABLE items")
		})
		t.Run(engine.name+"/a grant added on the target", func(t *testing.T) {
			c := qt.New(t)
			role := mysqlGrantDriftRole(c, engine)

			result, document := mysqlViewDriftRun(c, engine, mysqlGrantDriftFiles(role), "GRANT INSERT ON items TO "+role)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(document.DriftCheck, qt.IsNotNil)
			c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(document.DriftCheck.Findings, qt.HasLen, 1, qt.Commentf("%+v", document.DriftCheck.Findings))
			c.Assert(document.DriftCheck.Findings[0].Kind, qt.Equals, "extra_grant")
			c.Assert(document.DriftCheck.Findings[0].Object, qt.Matches, role+` INSERT ON TABLE ptah_view_drift_\d+\.items`)
		})
	}
}

// MariaDB keeps a sequence's privileges as a table's, and its read records the
// sequence with the database that holds it. A grant on the shadow's sequence
// is the target's grant on its own.
func TestMariaDBSequenceGrantDriftCheckE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	engine := mysqlViewDriftEngines[1]
	role := mysqlGrantDriftRole(c, engine)
	files := map[string]string{
		"0000000001_items.up.sql": "-- atlas:txmode none\n" +
			"CREATE TABLE items (id INT PRIMARY KEY);\nCREATE SEQUENCE item_seq;\n" +
			"CREATE ROLE IF NOT EXISTS " + role + ";\n" +
			"GRANT SELECT ON item_seq TO " + role + ";\n",
		"0000000001_items.down.sql": "DROP SEQUENCE item_seq;\nDROP TABLE items;\n",
	}

	result, document := mysqlViewDriftRun(c, engine, files)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", result.Stderr, result.Stdout))
	c.Assert(document.DriftCheck, qt.IsNotNil)
	c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftClean, qt.Commentf("%+v", document.DriftCheck.Findings))
}
