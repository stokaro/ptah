//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
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
	restricted := newRestrictedMySQLTarget(c)
	return restricted.url, restricted.workDir
}

// restrictedMySQL is what [newRestrictedMySQLTarget] set up: the restricted
// URL and working directory, and the administrative server, for a test that
// provisions more on it.
type restrictedMySQL struct {
	url     string
	workDir string
	server  mysqlFamilyServer
}

// newRestrictedMySQLTarget is [restrictedMySQLTarget], keeping the
// administrative server.
func newRestrictedMySQLTarget(c *qt.C) restrictedMySQL {
	c.Helper()
	// The administrative account, because the fixture creates a database and
	// an account.
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
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

	workDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(workDir, "undecided.sql"),
		[]byte(undecidedTableSchema+"CREATE ROLE "+undecidedRoleName+";\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(workDir, "converged.sql"),
		[]byte(undecidedTableSchema), 0o600), qt.IsNil)

	target := (&url.URL{
		Scheme: "mysql",
		User:   url.UserPassword(account, password),
		Host:   server.config.Addr,
		Path:   "/" + database,
	}).String()
	return restrictedMySQL{url: target, workDir: workDir, server: server}
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
		{name: "migrations generate", args: []string{"migrations", "generate", "--migrations-dir", "migrations"}},
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

// TestSchemaDriftReportsAnUndecidedObjectE2E holds `ptah schema drift` to the
// rule `schema compare --exit-code` keeps: a comparison that could not look is
// not a database without drift (stokaro/ptah#3844). The role is no difference,
// so the headline does not say drift was detected, and it is not destructive,
// so the destructive threshold passes. The converged schema is the control.
func TestSchemaDriftReportsAnUndecidedObjectE2E(t *testing.T) {
	c := qt.New(t)
	target, workDir := restrictedMySQLTarget(c)

	tests := []struct {
		name       string
		args       []string
		wantExit   int
		wantStdout string
		wantStderr string
	}{
		{
			name:     "a declared role the account may not read",
			args:     []string{"--schema-file", "undecided.sql"},
			wantExit: 1,
			wantStdout: "No schema drift found, but 1 declared object could not be decided (highest severity: warning).\n" +
				"Failure threshold: all. Failing: true.\n",
			wantStderr: undecidedRoleWarning,
		},
		{
			name:       "the same at the destructive threshold",
			args:       []string{"--schema-file", "undecided.sql", "--severity", "destructive"},
			wantExit:   0,
			wantStdout: "Failure threshold: destructive. Failing: false.\n",
			wantStderr: undecidedRoleWarning,
		},
		{
			name:       "only what the account can read",
			args:       []string{"--schema-file", "converged.sql"},
			wantExit:   0,
			wantStdout: "No schema drift detected.\n",
			wantStderr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				append([]string{"schema", "drift", "--db-url", target}, test.args...)...)

			c.Assert(got.ExitCode, qt.Equals, test.wantExit,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, test.wantStdout)
			c.Assert(got.Stderr, qt.Equals, test.wantStderr)
		})
	}
}

// undecidedDocument is the part of a JSON document these tests read.
type undecidedDocument struct {
	ContractVersion int                    `json:"contract_version"`
	Outcome         string                 `json:"outcome"`
	Drift           bool                   `json:"drift"`
	Failed          bool                   `json:"failed"`
	Undecided       schemadiff.Diagnostics `json:"undecided"`
}

// TestJSONDocumentsCarryAnUndecidedObjectE2E holds each machine-readable
// document to the withheld role: kind, name, and the reason and provenance the
// read gave. A caller that reads only the outcome sees no-changes, and the
// undecided list is what tells it the database is not shown to match.
func TestJSONDocumentsCarryAnUndecidedObjectE2E(t *testing.T) {
	c := qt.New(t)
	target, workDir := restrictedMySQLTarget(c)
	withheld := coverage.Refused(coverage.Role)
	withheld.Name = undecidedRoleName

	tests := []struct {
		name string
		args []string
		want undecidedDocument
	}{
		{
			name: "schema plan",
			args: []string{"schema", "plan", "--dry-run", "--json"},
			want: undecidedDocument{ContractVersion: 2, Outcome: "no-changes", Undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}}},
		},
		{
			name: "schema apply",
			args: []string{"schema", "apply", "--auto-approve", "--json"},
			want: undecidedDocument{ContractVersion: 2, Outcome: "no-changes", Undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}}},
		},
		{
			name: "schema drift",
			args: []string{"schema", "drift", "--format", "json"},
			want: undecidedDocument{ContractVersion: 2, Failed: true, Undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				append(test.args, "--db-url", target, "--schema-file", "undecided.sql")...)

			var document undecidedDocument
			c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(document, qt.DeepEquals, test.want)
			c.Assert(got.Stderr, qt.Contains, undecidedRoleWarning)
		})
	}
}

// undecidedEntities declares the notes table and the role, as Go annotations.
const undecidedEntities = `package entities

//ptah:schema:role name="` + undecidedRoleName + `"
//ptah:schema:table name="notes"
type Note struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int64
}
`

// TestMigrationsBaselineRefusesAnUndecidedObjectE2E holds the entity
// verification of `ptah migrations baseline` to what a baseline claims: the
// database already holds what the migrations create. The role is not shown to
// be there, so the baseline is refused, and --force records it anyway.
//
// The steps run in order because the forced run writes the revision table, and
// a refused run after it would measure that table rather than the refusal.
func TestMigrationsBaselineRefusesAnUndecidedObjectE2E(t *testing.T) {
	c := qt.New(t)
	target, workDir := restrictedMySQLTarget(c)
	entities := filepath.Join(workDir, "entities")
	migrations := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(entities, 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entities, "notes.go"), []byte(undecidedEntities), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_notes.up.sql"),
		[]byte(undecidedTableSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_notes.down.sql"),
		[]byte("DROP TABLE notes;\n"), 0o600), qt.IsNil)
	args := []string{"migrations", "baseline", "--db-url", target, "--migrations-dir", migrations, "--root-dir", entities}

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir}, args...)

	c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, strings.ReplaceAll(undecidedRoleWarning, "the desired schema", "the entities"))
	c.Assert(refused.Stderr, qt.Contains,
		"baseline drift verification failed: 1 declared object could not be decided")

	forced := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir}, append(args, "--force")...)

	c.Assert(forced.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", forced.Stdout, forced.Stderr))
	c.Assert(forced.Stdout, qt.Contains, "Baselined 1 migration(s) through version 1")
}

// TestMigrationsBaselineShadowRefusesAnUndecidedObjectE2E is the shadow half:
// the replay creates a role on the shadow database, the target account cannot
// read roles, and the verification reports the role as an undecided mismatch
// rather than passing on a target that was never checked.
func TestMigrationsBaselineShadowRefusesAnUndecidedObjectE2E(t *testing.T) {
	c := qt.New(t)
	restricted := newRestrictedMySQLTarget(c)
	shadowDatabase := restricted.server.database(c, "undecided_shadow")
	role := fmt.Sprintf("ptah_undecided_shadow_%d", time.Now().UnixNano()%1_000_000_000)
	// Roles belong to the server, so the one the replay creates outlives the
	// shadow database unless it is dropped here.
	c.Cleanup(func() {
		_, err := restricted.server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS `"+role+"`")
		c.Check(err, qt.IsNil)
	})
	migrations := filepath.Join(restricted.workDir, "shadow-migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_notes.up.sql"),
		[]byte(undecidedTableSchema+"CREATE ROLE `"+role+"`;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_notes.down.sql"),
		[]byte("DROP TABLE notes;\nDROP ROLE `"+role+"`;\n"), 0o600), qt.IsNil)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: restricted.workDir},
		"migrations", "baseline", "--db-url", restricted.url, "--migrations-dir", migrations,
		"--shadow-db", restricted.server.url("mysql", shadowDatabase))

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "baseline shadow check failed: undecided role "+role+
		": the target database does not describe role objects because the read was refused"+
		" the catalog that would have listed them")
}
