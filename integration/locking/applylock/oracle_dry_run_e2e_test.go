//go:build integration

package applylock_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

// A dry run of `schema apply` on Oracle takes the apply lock as a run that
// writes does, through the preview arm of the lock, which takes Oracle's row
// lock on a session of its own. These tests run the shipped binary against an
// account of their own, since a declarative apply plans a DROP for every table
// the schema does not declare.

// oracleDryRunSchema is the whole desired schema the dry runs plan.
const oracleDryRunSchema = "CREATE TABLE dry_widgets (id NUMBER(10) PRIMARY KEY);\n"

// oracleApplyTarget creates an Oracle account for one test, drops it with
// everything it owns when the test ends, and returns its URL and a
// connection to it.
func oracleApplyTarget(c *qt.C) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	account := fmt.Sprintf("ptah_dry_apply_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, "DROP USER "+account+" CASCADE")
		c.Check(dropErr, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE USER " + account + " IDENTIFIED BY ptah_password",
		"ALTER USER " + account + " QUOTA UNLIMITED ON USERS",
		"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO " + account,
	} {
		_, err := admin.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, "ptah_password")
	conn, err := dbschema.ConnectToDatabase(context.Background(), parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return parsed.String(), conn
}

// dryRun runs `ptah schema apply --dry-run` against dbURL with the given lock
// timeout.
func dryRun(c *qt.C, dbURL, timeout string) clirun.Result {
	c.Helper()
	work := c.TempDir()
	schemaPath := filepath.Join(work, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(oracleDryRunSchema), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "apply",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--lock-timeout", timeout,
		"--dry-run",
	)
}

// countTables counts the tables of the account conn logs in as.
func countTables(c *qt.C, conn *dbschema.DatabaseConnection, name string) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM user_tables WHERE table_name = :1", name).Scan(&count), qt.IsNil)
	return count
}

// A dry run with the apply lock free plans the schema and writes nothing: no
// table, and no lock table either, since a preview creates neither the table
// nor the row of the lock. With no row to lock it holds no lock, and says so
// on stderr rather than run as if it held one.
func TestSchemaApplyDryRunOnOracleE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbURL, conn := oracleApplyTarget(c)

	got := dryRun(c, dbURL, "5s")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "CREATE TABLE dry_widgets")
	c.Assert(got.Stderr, qt.Contains, `note: the dry run holds no schema apply lock: no run has taken lock "ptah_schema_apply" `+
		`on this database yet, and a dry run creates nothing to take it on`)
	c.Assert(countTables(c, conn, "DRY_WIDGETS"), qt.Equals, 0)
	c.Assert(countTables(c, conn, "PTAH_LOCKS"), qt.Equals, 0)
}

// Once a run has taken the apply lock, its row is there, and a dry run takes
// the lock as a run that writes does: it plans, writes nothing, and has
// nothing to say about the lock.
func TestSchemaApplyDryRunOnOracleE2E_TakesAnExistingLock(t *testing.T) {
	c := qt.New(t)
	dbURL, conn := oracleApplyTarget(c)
	taken, err := dblock.Acquire(c.Context(), conn, atlasschema.ApplyLockName, dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Assert(taken.Release(context.Background()), qt.IsNil)

	got := dryRun(c, dbURL, "5s")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "CREATE TABLE dry_widgets")
	c.Assert(got.Stderr, qt.Not(qt.Contains), "holds no schema apply lock")
	c.Assert(countTables(c, conn, "DRY_WIDGETS"), qt.Equals, 0)
}

// A dry run waits for the apply lock another session holds, and refuses once
// --lock-timeout passes, having planned and written nothing.
func TestSchemaApplyDryRunOnOracleE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	dbURL, conn := oracleApplyTarget(c)
	held, err := dblock.Acquire(c.Context(), conn, atlasschema.ApplyLockName, dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = held.Release(context.Background()) })

	got := dryRun(c, dbURL, "1s")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `timed out acquiring advisory lock "ptah_schema_apply" on oracle after 1s`)
	c.Assert(countTables(c, conn, "DRY_WIDGETS"), qt.Equals, 0)
}
