//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A declared object the comparison could not decide, driven through the
// shipped binary against a live MySQL server (stokaro/ptah#3834).
//
// The account below may change the tables of its own database and may not read
// mysql.user, so the role read is refused and the description records roles as
// not described. A declared role is then withheld: nothing is planned for it,
// because nothing checked whether it already exists. A MySQL account is the
// case because it is the ordinary one -- an application account is rarely
// granted the privilege-table reads a role read needs.
//
// The account is created here rather than borrowed from the suite's URL, so
// what it may do is stated by this test, and a later grant to the shared
// account cannot quietly remove the refusal the test depends on.

// undecidedRoleName is the declared role the restricted account cannot see.
// Nothing creates it: the refusal, not the role, is what the test measures.
const undecidedRoleName = "ptah_undecided_reporter"

// undecidedRoleWarning is the whole of what the native verbs print on standard
// error for the withheld role.
const undecidedRoleWarning = `Warning: role "` + undecidedRoleName + `" is declared by the desired schema` +
	` but no change was planned for it: the database does not describe role objects because` +
	` the read was refused the catalog that would have listed them, so this comparison` +
	` cannot tell it apart from one that already exists, and the creation Ptah renders` +
	` for it cannot safely converge from an unknown current state.
`

// undecidedTableSchema is the part of the desired schema the account can see,
// and already carries.
const undecidedTableSchema = "CREATE TABLE notes (id INT PRIMARY KEY);\n"

// restrictedMySQLTarget creates a database holding the notes table and an
// account that may work on that database and nothing else. It returns the
// database URL spelled with that account, and a working directory holding two
// desired schemas: undecided.sql declares the table and a role, converged.sql
// only the table.
func restrictedMySQLTarget(c *qt.C) (target, workDir string) {
	c.Helper()
	server := newMySQLFamilyServer(c, dbtarget.MySQL)
	database := server.database(c, "undecided")
	ctx := c.Context()

	_, err := server.admin.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s`.notes (id INT PRIMARY KEY)", database))
	c.Assert(err, qt.IsNil)

	account := fmt.Sprintf("ptah_undecided_%d", time.Now().UnixNano()%1_000_000_000)
	password := account + "_pw"
	createMySQLUser(c, ctx, server.admin, account, password)
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), server.admin, account) })
	_, err = server.admin.ExecContext(ctx, fmt.Sprintf(
		"GRANT SELECT, CREATE, DROP, ALTER, INSERT, UPDATE, DELETE ON `%s`.* TO '%s'@'%%'", database, account))
	c.Assert(err, qt.IsNil)

	workDir = c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(workDir, "undecided.sql"),
		[]byte(undecidedTableSchema+"CREATE ROLE "+undecidedRoleName+";\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(workDir, "converged.sql"),
		[]byte(undecidedTableSchema), 0o600), qt.IsNil)

	target = (&url.URL{
		Scheme: "mysql",
		User:   url.UserPassword(account, password),
		Host:   server.config.Addr,
		Path:   "/" + database,
	}).String()
	return target, workDir
}

// TestSchemaCompareReportsAnUndecidedObjectE2E holds `ptah schema compare` to
// both halves of the report: the withheld role is named on standard output
// instead of "No schema differences detected.", and `--exit-code` exits 1 on
// it, because a comparison that could not look is not proof that nothing
// drifted.
//
// The run without `--exit-code` shows the report does not depend on the flag,
// and the converged schema is the control: without it, a compare that exited 1
// on every run against this account would satisfy the first row just as well.
func TestSchemaCompareReportsAnUndecidedObjectE2E(t *testing.T) {
	c := qt.New(t)
	target, workDir := restrictedMySQLTarget(c)

	tests := []struct {
		name       string
		args       []string
		wantExit   int
		wantStdout string
		// notStdout is the report the row must not print: the one the other
		// half of the table prints.
		notStdout  string
		wantStderr string
	}{
		{
			name:     "a declared role the account may not read, gated",
			args:     []string{"--schema-file", "undecided.sql", "--exit-code"},
			wantExit: 1,
			wantStdout: "No differences planned, but 1 declared object could not be decided:\n" +
				`  role "` + undecidedRoleName + `"` + "\n",
			notStdout:  "No schema differences detected.",
			wantStderr: undecidedRoleWarning,
		},
		{
			name:     "a declared role the account may not read, ungated",
			args:     []string{"--schema-file", "undecided.sql"},
			wantExit: 0,
			wantStdout: "No differences planned, but 1 declared object could not be decided:\n" +
				`  role "` + undecidedRoleName + `"` + "\n",
			notStdout:  "No schema differences detected.",
			wantStderr: undecidedRoleWarning,
		},
		{
			name:       "only what the account can read, gated",
			args:       []string{"--schema-file", "converged.sql", "--exit-code"},
			wantExit:   0,
			wantStdout: "No schema differences detected.\n",
			notStdout:  "could not be decided",
			wantStderr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				append([]string{"schema", "compare", "--db-url", target}, test.args...)...)

			c.Assert(got.ExitCode, qt.Equals, test.wantExit,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, test.wantStdout)
			c.Assert(got.Stdout, qt.Not(qt.Contains), test.notStdout)
			c.Assert(got.Stderr, qt.Equals, test.wantStderr)
		})
	}
}

// TestPlanningVerbsNameAnUndecidedObjectE2E holds the verbs that plan from the
// same comparison to the warning `schema compare` prints. Each plans nothing
// for the role, and without the warning each reports a plan that does less than
// the desired schema asks for and says nothing about it.
func TestPlanningVerbsNameAnUndecidedObjectE2E(t *testing.T) {
	c := qt.New(t)
	target, workDir := restrictedMySQLTarget(c)

	tests := []struct {
		name string
		args []string
	}{
		{name: "schema compare", args: []string{"schema", "compare"}},
		{name: "schema apply", args: []string{"schema", "apply", "--dry-run"}},
		{name: "schema plan", args: []string{"schema", "plan", "--dry-run"}},
		{name: "migrations plan", args: []string{"migrations", "plan"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				append(test.args, "--db-url", target, "--schema-file", "undecided.sql")...)

			c.Assert(got.ExitCode, qt.Equals, 0,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, undecidedRoleWarning)
		})
	}
}
