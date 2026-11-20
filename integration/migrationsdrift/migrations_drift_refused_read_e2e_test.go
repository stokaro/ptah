//go:build integration

package migrationsdrift_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A MySQL account that may not read mysql.user cannot say which roles exist,
// so a role the applied migrations create is an object the report cannot
// decide on. The server decides that, by refusing the catalog read.

// refusedRoleRead is a MySQL target at the third migration of a history whose
// third migration creates a role, reached through restricted, an account that
// holds every privilege on the target database and none on mysql.user.
type refusedRoleRead struct {
	run        driftRun
	restricted string
	role       string
}

func newRefusedRoleRead(c *qt.C, outOfBand ...string) refusedRoleRead {
	c.Helper()
	run := newDriftRun(c, mysqlDrift, "2")
	// Account and role names are at most 32 characters.
	stamp := time.Now().UnixNano() % 1_000_000_000_000
	read := refusedRoleRead{run: run, role: fmt.Sprintf("pmd_role_%d", stamp)}
	user, password := fmt.Sprintf("pmd_ro_%d", stamp), "drift_ro_password"

	files := map[string]string{
		"0000000003_role.up.sql":   "CREATE ROLE IF NOT EXISTS " + read.role + ";\n",
		"0000000003_role.down.sql": "DROP ROLE IF EXISTS " + read.role + ";\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(run.dir, name), []byte(content), 0o600), qt.IsNil)
	}
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", run.target, "--migrations-dir", run.dir)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", applied.Stderr))

	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, mysqlDrift.admin))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		for _, statement := range []string{
			fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user), "DROP ROLE IF EXISTS " + read.role,
		} {
			_, dropErr := admin.ExecContext(context.Background(), statement)
			c.Check(dropErr, qt.IsNil)
		}
		dbschema.CloseAndWarn(admin)
	})
	for _, statement := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, password),
		fmt.Sprintf("GRANT ALL ON `%s`.* TO '%s'@'%%'", run.targetName, user),
	} {
		_, err := admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	config, err := mysqldriver.ParseDSN(dbtarget.DriverDSN(c, mysqlDrift.admin))
	c.Assert(err, qt.IsNil)
	read.restricted = fmt.Sprintf("mysql://%s:%s@tcp(%s)/%s", user, password, config.Addr, run.targetName)
	run.exec(c, outOfBand...)
	return read
}

func (r refusedRoleRead) report(c *qt.C) (document, clirun.Result) {
	c.Helper()
	reader := r.run
	reader.target = r.restricted
	return reader.report(c)
}

// text runs the report as report does, as the text a person reads.
func (r refusedRoleRead) text(c *qt.C) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"migrations", "drift", "--db-url", r.restricted, "--migrations-dir", r.run.dir, "--shadow-db", r.run.shadow)
}

// undecidedRole is the finding the report gives for the role the read could
// not see.
func (r refusedRoleRead) undecidedRole() migrator.DriftFinding {
	return migrator.DriftFinding{
		Kind: "undecided_object", Object: r.role,
		Message: "undecided role " + r.role + ": the target database does not describe role objects " +
			"because the read was refused the catalog that would have listed them",
	}
}

// Roles are server-wide on MySQL, so the findings name every role the server
// holds; the test asserts the one its history created.
func TestMigrationsDriftE2E_FailurePathCannotDecideWhatTheReadIsRefused(t *testing.T) {
	c := qt.New(t)
	read := newRefusedRoleRead(c)

	got, result := read.report(c)

	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftUnable)
	c.Assert(got.Check.Stage, qt.Equals, "schema-match")
	c.Assert(got.Check.Reason, qt.Equals, "incomplete_coverage")
	c.Assert(got.Check.Findings, qt.Contains, read.undecidedRole())
	c.Assert(got.Undecided, qt.HasLen, 0)
	c.Assert(got.Statements, qt.HasLen, 0)
}

// Drift found beside an object the read could not describe decides the
// outcome, and the report still names the object, so nobody reads the
// statements as the whole of what differs.
func TestMigrationsDriftE2E_FailurePathReportsDriftBesideWhatTheReadIsRefused(t *testing.T) {
	c := qt.New(t)
	read := newRefusedRoleRead(c, "ALTER TABLE drift_users ADD COLUMN nickname VARCHAR(40)")

	got, result := read.report(c)

	c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("%s", result.Stderr))
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
	c.Assert(got.Check.Findings, qt.DeepEquals, []migrator.DriftFinding{{
		Kind: "extra_column", Object: "drift_users.nickname", Table: "drift_users", Column: "nickname",
		Message: "extra column drift_users.nickname",
	}})
	c.Assert(got.Undecided, qt.Contains, read.undecidedRole())
	c.Assert(got.Statements, qt.Not(qt.HasLen), 0)
	c.Assert(read.run.nicknameColumns(c), qt.Equals, 1)

	text := read.text(c)

	c.Assert(text.ExitCode, qt.Equals, 1, qt.Commentf("%s", text.Stderr))
	c.Assert(text.Stdout, qt.Contains, "\nThe database read could not describe these objects the applied migrations create, "+
		"so whether the database holds them is unknown, and the statements below do not cover them:\n")
	c.Assert(text.Stdout, qt.Contains, "\n  - "+read.undecidedRole().Message+"\n")
}
